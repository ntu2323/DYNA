// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "dyna/replica.h"
#include "dyna/stats_util.h"
#include "dyna/wall_clock.h"

#include "lib/assert.h"
#include "lib/message.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <unistd.h>

namespace specpaxos {
namespace dyna {

/** Max log-append/sync steps per durability-loop iteration. */
static constexpr size_t kDurabilityBudgetPerLoop = 256;
static constexpr size_t kDefaultReplicationBudget = 256;
static constexpr size_t kDefaultReplicationBurstBudget = 8192;
static constexpr size_t kDefaultReplicationBacklogThreshold = 1000;
static constexpr uint32_t kDefaultFastReplyShards = 16;

/** Yield this many times before falling back to a short OS sleep. */
static constexpr int kIdleSpinIterations = 32;
/** Short OS sleep used after yield-spin exhausts; far below the prior 50us floor. */
static constexpr std::chrono::microseconds kIdleSleep{5};
/** Cap on time we wait for the next earlyBuffer deadline before re-checking. */
static constexpr uint64_t kReleaseSleepCapUs = 20;

/** Hybrid yield-then-short-sleep used by worker idle paths to avoid 50us floors. */
static inline void
IdleBackoff(int &spinCount)
{
    if (spinCount < kIdleSpinIterations) {
        std::this_thread::yield();
        ++spinCount;
    } else {
        std::this_thread::sleep_for(kIdleSleep);
    }
}

static size_t
ReplicationBudgetNormal()
{
    static const size_t budget = static_cast<size_t>(
        EnvUint64OrDefault("DYNA_REPLICATION_BUDGET", kDefaultReplicationBudget));
    return std::max<size_t>(1, budget);
}

static size_t
ReplicationBudgetBurst()
{
    static const size_t burst = static_cast<size_t>(
        EnvUint64OrDefault("DYNA_REPLICATION_BURST_BUDGET", kDefaultReplicationBurstBudget));
    return std::max(ReplicationBudgetNormal(), burst);
}

static size_t
ReplicationBacklogThreshold()
{
    static const size_t threshold = static_cast<size_t>(
        EnvUint64OrDefault("DYNA_REPLICATION_BACKLOG_THRESHOLD", kDefaultReplicationBacklogThreshold));
    return threshold;
}

static uint32_t
FastReplyShardCountFromEnv()
{
    if (const char *v = std::getenv("DYNA_FAST_REPLY_SHARDS"); v && *v) {
        unsigned long x = std::strtoul(v, nullptr, 10);
        if (x > 0) {
            return static_cast<uint32_t>(x);
        }
    }
    return kDefaultFastReplyShards;
}

static bool
SlowPathDebugEnabled()
{
    static const bool enabled = []() {
        const char *v = std::getenv("DYNA_SLOWPATH_DEBUG");
        return v && *v && std::strcmp(v, "0") != 0;
    }();
    return enabled;
}

static std::string
WalDirFromEnv()
{
    if (const char *v = std::getenv("DYNA_WAL_DIR"); v && *v) {
        return std::string(v);
    }
    return "/tmp/ionia-dyna-wal";
}

namespace {

struct ReplicaRuntimeStats {
    CounterWindow releases;
    CounterWindow slowpathAdmit;
    CounterWindow incomingFull;
    CounterWindow fastReplyFull;
    CounterWindow durabilityFull;
    CounterWindow replicationFull;
    LatencySampleBuffer queueUs;
    LatencySampleBuffer holdUs;
    LatencySampleBuffer fastReplyQueueUs;
};

ReplicaRuntimeStats g_replicaStats;

} // namespace

DynaReplica::DynaReplica(const Configuration &config, int replicaIdx, Transport *transport)
    : config(config),
      numPartitions(static_cast<uint32_t>(std::max(1, config.n))),
      numFastReplyShards(FastReplyShardCountFromEnv()),
      replicaIdx(replicaIdx),
      transport(transport),
      partitions(numPartitions),
      partitionMu_(numPartitions),
      replicationQu_(numPartitions),
      incomingQu_(numPartitions),
      durabilityQu_(numPartitions),
      owdEstimator_(DeadlineConfig::FromEnvironment()),
      walDir_(WalDirFromEnv()),
      replicaStatsEnabled_(EnvFlagEnabled("DYNA_STATS")),
      replicaStatsIntervalUs_(EnvUint64OrDefault("DYNA_STATS_INTERVAL_SEC", 1) * 1000000ULL),
      replicaStatsSampleEvery_(std::max<uint64_t>(1, EnvUint64OrDefault("DYNA_STATS_SAMPLE", 1024)))
{
    fastReplyQu_.resize(numFastReplyShards);
    std::error_code ec;
    std::filesystem::create_directories(walDir_, ec);
    if (ec) {
        Warning("Failed to create DYNA WAL directory %s: %s", walDir_.c_str(), ec.message().c_str());
    }
    transport->Register(this, config, 0, replicaIdx);
    StartWorkers();
}

DynaReplica::~DynaReplica()
{
    StopWorkers();
    if (replicaStatsEnabled_) {
        EmitReplicaStats();
    }
}

void
DynaReplica::UpdateDeadlineCounter(std::atomic<uint64_t> &counter, uint64_t value)
{
    uint64_t cur = counter.load(std::memory_order_relaxed);
    while (value > cur &&
           !counter.compare_exchange_weak(cur, value, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

uint64_t
DynaReplica::OpKeyFromCommand(const std::string &command)
{
    if (command.size() < 25) {
        return 0;
    }
    const std::string key = command.substr(1, 24);
    return static_cast<uint64_t>(std::hash<std::string>()(key));
}

static uint64_t
ReqKeyFromMessage(const proto::RequestMessage &req)
{
    return (static_cast<uint64_t>(req.client_id()) << 32) |
           static_cast<uint64_t>(static_cast<uint32_t>(req.request_id()));
}

DynaReplica::ReleasedRank
DynaReplica::RankOf(const proto::RequestMessage &req)
{
    ReleasedRank rank;
    rank.deadlineUs = req.deadline_us();
    rank.reqKey = ReqKeyFromMessage(req);
    return rank;
}

bool
DynaReplica::RankAfter(const ReleasedRank &incoming, const ReleasedRank &released)
{
    if (incoming.deadlineUs != released.deadlineUs) {
        return incoming.deadlineUs > released.deadlineUs;
    }
    return incoming.reqKey > released.reqKey;
}

bool
DynaReplica::IsOnTimeForOpKey(const PartitionState &state, const proto::RequestMessage &req)
{
    const uint64_t opKey = OpKeyFromCommand(req.command());
    const auto iter = state.lastReleasedByOpKey.find(opKey);
    if (iter == state.lastReleasedByOpKey.end()) {
        return true;
    }
    return RankAfter(RankOf(req), iter->second);
}

void
DynaReplica::RecordReleasedRank(PartitionState &state, const proto::RequestMessage &req)
{
    if (req.is_read()) {
        return;
    }
    state.lastReleasedByOpKey[OpKeyFromCommand(req.command())] = RankOf(req);
}

bool
DynaReplica::EnqueueClientRequest(uint32_t partitionId,
                                    const TransportAddress &remote,
                                    const proto::RequestMessage &req)
{
    const uint32_t pid = partitionId % numPartitions;
    BufferedRequest buffered;
    buffered.request = req;
    buffered.clientAddress = std::shared_ptr<TransportAddress>(remote.clone());
    if (replicaStatsEnabled_) {
        buffered.enqueuedUs = NowMicros();
    }
    if (!incomingQu_[pid].enqueue(std::move(buffered))) {
        if (replicaStatsEnabled_) {
            g_replicaStats.incomingFull.add();
        }
        Warning("DYNA incoming queue full part=%u req=(%lu,%lu)", pid, req.client_id(), req.request_id());
        return false;
    }
    return true;
}

bool
DynaReplica::EnqueueReplication(uint32_t partitionId,
                                InboundKind kind,
                                const TransportAddress &remote,
                                const std::string_view data)
{
    const uint32_t pid = partitionId % numPartitions;
    InboundWork work;
    work.kind = kind;
    work.remote.reset(remote.clone());
    work.payload.assign(data.data(), data.length());
    if (!replicationQu_[pid].enqueue(std::move(work))) {
        if (replicaStatsEnabled_) {
            g_replicaStats.replicationFull.add();
        }
        Warning("DYNA replication queue full part=%u kind=%d", pid, static_cast<int>(kind));
        return false;
    }
    return true;
}

void
DynaReplica::StartWorkers()
{
    workersRunning_.store(true);
    processThreads_.reserve(numPartitions);
    replicationThreads_.reserve(numPartitions);
    durabilityThreads_.reserve(numPartitions);
    fastReplyThreads_.reserve(numFastReplyShards);
    for (uint32_t shard = 0; shard < numFastReplyShards; ++shard) {
        fastReplyThreads_.emplace_back(&DynaReplica::FastReplyThreadLoop, this, shard);
    }
    owdThread_ = std::thread(&DynaReplica::OwdThreadLoop, this);
    for (uint32_t partitionId = 0; partitionId < numPartitions; ++partitionId) {
        processThreads_.emplace_back(&DynaReplica::ProcessPartitionLoop, this, partitionId);
        replicationThreads_.emplace_back(&DynaReplica::ReplicationPartitionLoop, this, partitionId);
        durabilityThreads_.emplace_back(&DynaReplica::DurabilityPartitionLoop, this, partitionId);
    }
    if (replicaStatsEnabled_) {
        statsThread_ = std::thread(&DynaReplica::StatsThreadLoop, this);
    }
}

void
DynaReplica::StopWorkers()
{
    if (!workersRunning_.exchange(false)) {
        return;
    }
    if (statsThread_.joinable()) {
        statsThread_.join();
    }
    for (auto &qu : replicationQu_) {
        InboundWork shutdown;
        shutdown.kind = InboundKind::SHUTDOWN;
        qu.enqueue(std::move(shutdown));
    }
    for (auto &thread : processThreads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    processThreads_.clear();
    for (auto &thread : replicationThreads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    replicationThreads_.clear();
    for (auto &thread : durabilityThreads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    durabilityThreads_.clear();
    for (auto &thread : fastReplyThreads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    fastReplyThreads_.clear();
    if (owdThread_.joinable()) {
        owdThread_.join();
    }
}

void
DynaReplica::ProcessPartitionLoop(uint32_t partitionId)
{
    int spinCount = 0;
    while (workersRunning_.load()) {
        bool didWork = false;
        bool incomingPending = false;
        bool earlyPending = false;
        uint64_t nextDeadlineUs = 0;
        uint64_t nowUs = 0;
        {
            std::lock_guard<std::mutex> partitionLock(partitionMu_[partitionId]);
            PartitionState &state = StateFor(partitionId);

            DrainIncomingBuffered(partitionId);
            nowUs = NowMicros();
            while (EarlyBufferHasDue(state, nowUs)) {
                ProcessDueFastReplies(partitionId);
                nowUs = NowMicros();
                didWork = true;
            }

            if (!state.earlyBuffer.empty()) {
                earlyPending = true;
                nextDeadlineUs = state.earlyBuffer.top().request.deadline_us();
            }
            if (incomingQu_[partitionId].size_approx() > 0) {
                incomingPending = true;
            }
        }

        if (didWork || incomingPending) {
            spinCount = 0;
            continue;
        }

        if (earlyPending && nextDeadlineUs > nowUs) {
            const uint64_t deltaUs = nextDeadlineUs - nowUs;
            const uint64_t sleepUs = deltaUs < kReleaseSleepCapUs ? deltaUs : kReleaseSleepCapUs;
            std::this_thread::sleep_for(std::chrono::microseconds(sleepUs));
            spinCount = 0;
        } else {
            IdleBackoff(spinCount);
        }
    }
}

void
DynaReplica::ReplicationPartitionLoop(uint32_t partitionId)
{
    const size_t normalBudget = ReplicationBudgetNormal();
    const size_t burstBudget = ReplicationBudgetBurst();
    const size_t backlogThreshold = ReplicationBacklogThreshold();
    int spinCount = 0;

    while (workersRunning_.load()) {
        const size_t backlog = replicationQu_[partitionId].size_approx();
        const size_t budget = backlog >= backlogThreshold ? burstBudget : normalBudget;
        bool didWork = false;
        for (size_t i = 0; i < budget; ++i) {
            InboundWork work;
            if (!replicationQu_[partitionId].try_dequeue(work)) {
                break;
            }
            if (work.kind == InboundKind::SHUTDOWN) {
                return;
            }
            DispatchCommitInbound(partitionId, work);
            didWork = true;
        }
        if (didWork) {
            spinCount = 0;
            continue;
        }
        IdleBackoff(spinCount);
    }
}

void
DynaReplica::FastReplyThreadLoop(uint32_t shardId)
{
    int spinCount = 0;
    while (workersRunning_.load()) {
        FastReplySendJob job;
        if (fastReplyQu_[shardId].try_dequeue(job)) {
            if (replicaStatsEnabled_ && job.queuedUs > 0) {
                const uint64_t sendUs = NowMicros();
                if (sendUs > job.queuedUs) {
                    g_replicaStats.fastReplyQueueUs.record(sendUs - job.queuedUs);
                }
            }
            SendFastReplyFromJob(job);
            spinCount = 0;
            continue;
        }
        IdleBackoff(spinCount);
    }
}

void
DynaReplica::StatsThreadLoop()
{
    while (workersRunning_.load()) {
        std::this_thread::sleep_for(std::chrono::microseconds(replicaStatsIntervalUs_));
        if (!workersRunning_.load()) {
            break;
        }
        EmitReplicaStats();
    }
}

void
DynaReplica::EmitReplicaStats()
{
    const double intervalSec =
        static_cast<double>(replicaStatsIntervalUs_) / 1000000.0;
    size_t incomingApprox = 0;
    size_t durabilityApprox = 0;
    size_t replicationApprox = 0;
    size_t fastReplyApprox = 0;
    size_t earlyBufferTotal = 0;
    for (uint32_t pid = 0; pid < numPartitions; ++pid) {
        incomingApprox += incomingQu_[pid].size_approx();
        durabilityApprox += durabilityQu_[pid].size_approx();
        replicationApprox += replicationQu_[pid].size_approx();
        {
            std::lock_guard<std::mutex> partitionLock(partitionMu_[pid]);
            earlyBufferTotal += StateFor(pid).earlyBuffer.size();
        }
    }
    for (uint32_t shard = 0; shard < numFastReplyShards; ++shard) {
        fastReplyApprox += fastReplyQu_[shard].size_approx();
    }

    const uint64_t releases = g_replicaStats.releases.snapshotAndReset();
    const uint64_t slowpath = g_replicaStats.slowpathAdmit.snapshotAndReset();
    const uint64_t incomingFull = g_replicaStats.incomingFull.snapshotAndReset();
    const uint64_t fastReplyFull = g_replicaStats.fastReplyFull.snapshotAndReset();
    const uint64_t durFull = g_replicaStats.durabilityFull.snapshotAndReset();
    const uint64_t replFull = g_replicaStats.replicationFull.snapshotAndReset();

    const auto queueSnap = g_replicaStats.queueUs.snapshotAndClear();
    const auto holdSnap = g_replicaStats.holdUs.snapshotAndClear();
    const auto fastSnap = g_replicaStats.fastReplyQueueUs.snapshotAndClear();

    Notice("DYNA stats rep=%d incoming=%zu early=%zu fastQ=%zu durQ=%zu replQ=%zu "
           "releases=%.0f/s slowpath=%.0f/s incoming_full=%lu fastreply_full=%lu "
           "dur_full=%lu repl_full=%lu",
           replicaIdx,
           incomingApprox,
           earlyBufferTotal,
           fastReplyApprox,
           durabilityApprox,
           replicationApprox,
           intervalSec > 0 ? static_cast<double>(releases) / intervalSec : 0.0,
           intervalSec > 0 ? static_cast<double>(slowpath) / intervalSec : 0.0,
           static_cast<unsigned long>(incomingFull),
           static_cast<unsigned long>(fastReplyFull),
           static_cast<unsigned long>(durFull),
           static_cast<unsigned long>(replFull));

    if (queueSnap.count > 0 || holdSnap.count > 0 || fastSnap.count > 0) {
        Notice("DYNA stats rep=%d latency_us queue_samples=%lu queue_avg=%lu queue_p50=%lu "
               "queue_p99=%lu queue_max=%lu hold_samples=%lu hold_avg=%lu hold_p50=%lu "
               "hold_p99=%lu hold_max=%lu fastQ_samples=%lu fastQ_avg=%lu fastQ_p50=%lu "
               "fastQ_p99=%lu fastQ_max=%lu",
               replicaIdx,
               static_cast<unsigned long>(queueSnap.count),
               static_cast<unsigned long>(queueSnap.avg_us),
               static_cast<unsigned long>(queueSnap.p50_us),
               static_cast<unsigned long>(queueSnap.p99_us),
               static_cast<unsigned long>(queueSnap.max_us),
               static_cast<unsigned long>(holdSnap.count),
               static_cast<unsigned long>(holdSnap.avg_us),
               static_cast<unsigned long>(holdSnap.p50_us),
               static_cast<unsigned long>(holdSnap.p99_us),
               static_cast<unsigned long>(holdSnap.max_us),
               static_cast<unsigned long>(fastSnap.count),
               static_cast<unsigned long>(fastSnap.avg_us),
               static_cast<unsigned long>(fastSnap.p50_us),
               static_cast<unsigned long>(fastSnap.p99_us),
               static_cast<unsigned long>(fastSnap.max_us));
    }
}

void
DynaReplica::OwdThreadLoop()
{
    int spinCount = 0;
    while (workersRunning_.load()) {
        OwdSample sample;
        if (owdQu_.try_dequeue(sample)) {
            std::lock_guard<std::mutex> lock(owdMutex_);
            owdEstimator_.sample(sample.clientId, sample.delayUs);
            spinCount = 0;
            continue;
        }
        IdleBackoff(spinCount);
    }
}

void
DynaReplica::DurabilityPartitionLoop(uint32_t partitionId)
{
    int spinCount = 0;
    while (workersRunning_.load()) {
        bool didWork = false;
        for (size_t i = 0; i < kDurabilityBudgetPerLoop; ++i) {
            BufferedRequest released;
            if (!durabilityQu_[partitionId].try_dequeue(released)) {
                break;
            }

            LogEntry syncEntry;
            bool sendSync = false;
            LogEntry lateEntry;
            uint64_t lateOldDeadline = 0;
            uint64_t lateNewDeadline = 0;
            bool sendLateSync = false;
            uint64_t viewId = 0;

            {
                std::lock_guard<std::mutex> partitionLock(partitionMu_[partitionId]);
                PartitionState &state = StateFor(partitionId);
                const RequestKey keyReq = KeyOf(released.request);

                char op = '\0';
                std::string key;
                std::string value;
                if (!ParseCommand(released.request.command(), op, key, value)) {
                    continue;
                }
                if (state.requestToLogIndex.find(keyReq) != state.requestToLogIndex.end()) {
                    continue;
                }

                viewId = state.viewId.load(std::memory_order_relaxed);
                sendSync = AppendToLogLocked(partitionId, released.request, syncEntry);

                if (!AmLeader(partitionId)) {
                    didWork = true;
                    continue;
                }

                auto lateIt = state.pendingLateSyncByReq.find(keyReq);
                if (lateIt == state.pendingLateSyncByReq.end()) {
                    didWork = true;
                    continue;
                }

                auto logIt = state.requestToLogIndex.find(keyReq);
                if (logIt == state.requestToLogIndex.end() ||
                    logIt->second >= state.logEntries.size() ||
                    !state.logEntries[logIt->second].valid) {
                    state.pendingLateSyncByReq.erase(lateIt);
                    didWork = true;
                    continue;
                }

                lateEntry = state.logEntries[logIt->second];
                lateOldDeadline = lateIt->second.first;
                lateNewDeadline = lateIt->second.second;
                state.pendingLateSyncByReq.erase(lateIt);
                sendLateSync = true;
            }

            if (sendSync) {
                BroadcastSyncForLogEntry(partitionId, viewId, syncEntry);
            }
            if (sendLateSync) {
                BroadcastLateSyncForLogEntry(partitionId,
                                             viewId,
                                             lateEntry,
                                             lateOldDeadline,
                                             lateNewDeadline);
            }
            didWork = true;
        }
        if (didWork) {
            spinCount = 0;
            continue;
        }
        IdleBackoff(spinCount);
    }
}

void
DynaReplica::DrainIncomingBuffered(uint32_t partitionId)
{
    BufferedRequest buffered;
    while (incomingQu_[partitionId].try_dequeue(buffered)) {
        AdmitInboundClientRequest(partitionId, buffered);
    }
}

void
DynaReplica::AdmitInboundClientRequest(uint32_t partitionId, BufferedRequest &buffered)
{
    PartitionState &state = StateFor(partitionId);
    const proto::RequestMessage &req = buffered.request;
    const RequestKey key = KeyOf(req);
    state.requestClients[key] = buffered.clientAddress;
    state.latBuffer[key] = req;

    if (IsOnTimeForOpKey(state, req)) {
        buffered.sequence = state.nextSequence++;
        // Always buffer; ProcessDueFastReplies releases in global deadline order.
        // Releasing here when past deadline would violate ordering (incoming FIFO
        // vs Nezha rank) and spuriously triggers slow-path on same-key ops.
        state.earlyBuffer.push(std::move(buffered));
        return;
    }

    if (AmLeader(partitionId)) {
        if (replicaStatsEnabled_) {
            g_replicaStats.slowpathAdmit.add();
        }
        if (SlowPathDebugEnabled()) {
            const uint64_t opKey = OpKeyFromCommand(req.command());
            ReleasedRank released{0, 0};
            const auto iter = state.lastReleasedByOpKey.find(opKey);
            if (iter != state.lastReleasedByOpKey.end()) {
                released = iter->second;
            }
            Notice("DYNA slowpath req=(%lu,%lu) part=%u deadline=%lu released=(%lu,%lu)",
                   req.client_id(),
                   req.request_id(),
                   partitionId,
                   req.deadline_us(),
                   released.deadlineUs,
                   released.reqKey);
        }
        if (!buffered.clientAddress) {
            Warning("DYNA slowpath leader missing client address part=%u", partitionId);
            return;
        }
        HandleLateRequest(*buffered.clientAddress, req, partitionId);
    }
}

void
DynaReplica::DispatchCommitInbound(uint32_t partitionId, InboundWork &work)
{
    if (!work.remote) {
        Warning("DYNA inbound work missing remote address");
        return;
    }
    const TransportAddress &remote = *work.remote;

    switch (work.kind) {
    case InboundKind::REQUEST: {
        ALLOCATE_MESSAGE(proto::RequestMessage, req);
        if (!req->ParseFromString(work.payload)) {
            Warning("Failed to parse RequestMessage");
            return;
        }
        EnqueueClientRequest(req->partition_id(), remote, *req);
        break;
    }
    case InboundKind::SYNC_BROADCAST: {
        ALLOCATE_MESSAGE(proto::SyncBroadcast, sb);
        if (!sb->ParseFromString(work.payload)) {
            Warning("Failed to parse SyncBroadcast");
            return;
        }
        HandleSyncBroadcast(remote, *sb);
        break;
    }
    case InboundKind::SYNC_ACK: {
        ALLOCATE_MESSAGE(proto::SyncAck, ack);
        if (!ack->ParseFromString(work.payload)) {
            Warning("Failed to parse SyncAck");
            return;
        }
        HandleSyncAck(remote, *ack);
        break;
    }
    case InboundKind::COMMIT_BROADCAST: {
        ALLOCATE_MESSAGE(proto::CommitBroadcast, cb);
        if (!cb->ParseFromString(work.payload)) {
            Warning("Failed to parse CommitBroadcast");
            return;
        }
        HandleCommitBroadcast(remote, *cb);
        break;
    }
    case InboundKind::LATE_SYNC_BROADCAST: {
        ALLOCATE_MESSAGE(proto::LateSyncBroadcast, lsb);
        if (!lsb->ParseFromString(work.payload)) {
            Warning("Failed to parse LateSyncBroadcast");
            return;
        }
        HandleLateSyncBroadcast(remote, *lsb);
        break;
    }
    case InboundKind::FETCH_REQUEST: {
        ALLOCATE_MESSAGE(proto::FetchRequest, fr);
        if (!fr->ParseFromString(work.payload)) {
            Warning("Failed to parse FetchRequest");
            return;
        }
        HandleFetchRequest(remote, *fr);
        break;
    }
    case InboundKind::FETCH_RESPONSE: {
        ALLOCATE_MESSAGE(proto::FetchResponse, fr);
        if (!fr->ParseFromString(work.payload)) {
            Warning("Failed to parse FetchResponse");
            return;
        }
        HandleFetchResponse(remote, *fr);
        break;
    }
    default:
        Panic("Unexpected DYNA inbound work kind");
    }
}

void
DynaReplica::ReceiveMessage(const TransportAddress &remote, const std::string_view type,
                            const std::string_view data, void *meta_data)
{
    (void)meta_data;
    const static auto requestName = proto::RequestMessage::default_instance().GetDescriptor()->full_name();
    const static auto syncBroadcastName = proto::SyncBroadcast::default_instance().GetDescriptor()->full_name();
    const static auto syncAckName = proto::SyncAck::default_instance().GetDescriptor()->full_name();
    const static auto commitBroadcastName = proto::CommitBroadcast::default_instance().GetDescriptor()->full_name();
    const static auto lateSyncName = proto::LateSyncBroadcast::default_instance().GetDescriptor()->full_name();
    const static auto fetchRequestName = proto::FetchRequest::default_instance().GetDescriptor()->full_name();
    const static auto fetchResponseName = proto::FetchResponse::default_instance().GetDescriptor()->full_name();

    if (type == requestName) {
        ALLOCATE_MESSAGE(proto::RequestMessage, req);
        if (!req->ParseFromArray(data.data(), data.size())) {
            return;
        }
        const uint64_t recv_us = NowMicros();
        if (recv_us > req->send_time_us()) {
            OwdSample sample;
            sample.clientId = req->client_id();
            sample.delayUs = recv_us - req->send_time_us();
            owdQu_.enqueue(sample);
        }
        EnqueueClientRequest(req->partition_id(), remote, *req);
        return;
    }
    if (type == syncBroadcastName) {
        ALLOCATE_MESSAGE(proto::SyncBroadcast, sb);
        if (!sb->ParseFromArray(data.data(), data.size())) {
            return;
        }
        EnqueueReplication(sb->partition_id(), InboundKind::SYNC_BROADCAST, remote, data);
    } else if (type == syncAckName) {
        ALLOCATE_MESSAGE(proto::SyncAck, ack);
        if (!ack->ParseFromArray(data.data(), data.size())) {
            return;
        }
        EnqueueReplication(ack->partition_id(), InboundKind::SYNC_ACK, remote, data);
    } else if (type == commitBroadcastName) {
        ALLOCATE_MESSAGE(proto::CommitBroadcast, cb);
        if (!cb->ParseFromArray(data.data(), data.size())) {
            return;
        }
        EnqueueReplication(cb->partition_id(), InboundKind::COMMIT_BROADCAST, remote, data);
    } else if (type == lateSyncName) {
        ALLOCATE_MESSAGE(proto::LateSyncBroadcast, lsb);
        if (!lsb->ParseFromArray(data.data(), data.size())) {
            return;
        }
        EnqueueReplication(lsb->partition_id(), InboundKind::LATE_SYNC_BROADCAST, remote, data);
    } else if (type == fetchRequestName) {
        ALLOCATE_MESSAGE(proto::FetchRequest, fr);
        if (!fr->ParseFromArray(data.data(), data.size())) {
            return;
        }
        EnqueueReplication(fr->partition_id(), InboundKind::FETCH_REQUEST, remote, data);
    } else if (type == fetchResponseName) {
        ALLOCATE_MESSAGE(proto::FetchResponse, fr);
        if (!fr->ParseFromArray(data.data(), data.size())) {
            return;
        }
        EnqueueReplication(fr->partition_id(), InboundKind::FETCH_RESPONSE, remote, data);
    } else {
        Panic("Unexpected DYNA message type: %s", type.data());
    }
}

uint64_t
DynaReplica::NowMicros() const
{
    return WallClockMicros();
}

bool
DynaReplica::AmLeader(uint32_t partitionId) const
{
    return replicaIdx == static_cast<int>(PartitionLeader(partitionId));
}

DynaReplica::RequestKey
DynaReplica::KeyOf(const proto::RequestMessage &req) const
{
    return RequestKey{req.client_id(), req.request_id()};
}

DynaReplica::PartitionState &
DynaReplica::StateFor(uint32_t partitionId)
{
    return partitions[partitionId % numPartitions];
}

const DynaReplica::PartitionState &
DynaReplica::StateFor(uint32_t partitionId) const
{
    return partitions[partitionId % numPartitions];
}

uint32_t
DynaReplica::PartitionLeader(uint32_t partitionId) const
{
    return partitionId % numPartitions;
}

bool
DynaReplica::EarlyBufferHasDue(const PartitionState &state, uint64_t nowUs)
{
    return !state.earlyBuffer.empty() &&
           state.earlyBuffer.top().request.deadline_us() <= nowUs;
}

uint32_t
DynaReplica::FastReplyShardFor(const proto::RequestMessage &req, uint32_t numShards)
{
    if (numShards == 0) {
        return 0;
    }
    return static_cast<uint32_t>(ReqKeyFromMessage(req) % static_cast<uint64_t>(numShards));
}

void
DynaReplica::ReleaseBufferedRequest(PartitionState &state,
                                    uint32_t partitionId,
                                    BufferedRequest &buffered)
{
    RecordReleasedRank(state, buffered.request);

    if (replicaStatsEnabled_) {
        g_replicaStats.releases.add();
        const uint64_t releaseUs = NowMicros();
        const uint64_t sampleIdx =
            replicaStatsSampleCounter_.fetch_add(1, std::memory_order_relaxed);
        if (sampleIdx % replicaStatsSampleEvery_ == 0) {
            if (buffered.request.send_time_us() > 0 && releaseUs > buffered.request.send_time_us()) {
                g_replicaStats.queueUs.record(releaseUs - buffered.request.send_time_us());
            }
            if (releaseUs > buffered.request.deadline_us()) {
                g_replicaStats.holdUs.record(releaseUs - buffered.request.deadline_us());
            }
        }
    }

    if (buffered.clientAddress) {
        FastReplySendJob job;
        job.partitionId = partitionId;
        job.clientAddress = buffered.clientAddress;
        job.clientId = buffered.request.client_id();
        job.requestId = buffered.request.request_id();
        job.deadlineUs = buffered.request.deadline_us();
        job.viewId = state.viewId.load(std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lock(owdMutex_);
            job.owdUs = owdEstimator_.estimate_us(job.clientId);
        }
        if (replicaStatsEnabled_) {
            job.queuedUs = NowMicros();
        }
        const uint32_t shard = FastReplyShardFor(buffered.request, numFastReplyShards);
        if (!fastReplyQu_[shard].enqueue(std::move(job))) {
            if (replicaStatsEnabled_) {
                g_replicaStats.fastReplyFull.add();
            }
            Warning("DYNA fast-reply queue full shard=%u part=%u", shard, partitionId);
        }
    }

    if (!durabilityQu_[partitionId].enqueue(std::move(buffered))) {
        if (replicaStatsEnabled_) {
            g_replicaStats.durabilityFull.add();
        }
        Warning("DYNA durability queue full part=%u", partitionId);
    }
}

void
DynaReplica::ProcessDueFastReplies(uint32_t partitionId)
{
    PartitionState &state = StateFor(partitionId);
    const uint64_t nowUs = NowMicros();
    while (!state.earlyBuffer.empty()) {
        const BufferedRequest &top = state.earlyBuffer.top();
        if (top.request.deadline_us() > nowUs) {
            break;
        }

        BufferedRequest current = top;
        state.earlyBuffer.pop();
        ReleaseBufferedRequest(state, partitionId, current);
    }
}

void
DynaReplica::SendFastReplyFromJob(const FastReplySendJob &job)
{
    if (!job.clientAddress) {
        return;
    }
    proto::FastReply reply;
    reply.set_partition_id(job.partitionId);
    reply.set_request_id(job.requestId);
    reply.set_replica_id(static_cast<uint32_t>(replicaIdx));
    reply.set_view_id(job.viewId);
    reply.set_deadline_us(job.deadlineUs);
    if (job.owdUs > 0) {
        reply.set_owd_us(job.owdUs);
    }
    transport->SendMessage(this, *job.clientAddress, reply);
}

void
DynaReplica::BufferRequestUntilDeadlineLocked(uint32_t partitionId,
                                                const TransportAddress &remote,
                                                const proto::RequestMessage &req)
{
    PartitionState &state = StateFor(partitionId);
    const RequestKey key = KeyOf(req);
    state.requestClients[key] = std::shared_ptr<TransportAddress>(remote.clone());
    state.latBuffer[key] = req;

    BufferedRequest buffered;
    buffered.request = req;
    buffered.clientAddress = state.requestClients[key];
    buffered.sequence = state.nextSequence++;
    state.earlyBuffer.push(std::move(buffered));
}

void
DynaReplica::BufferRequestUntilDeadline(uint32_t partitionId,
                                        const TransportAddress &remote,
                                        const proto::RequestMessage &req)
{
    const uint32_t pid = partitionId % numPartitions;
    std::lock_guard<std::mutex> partitionLock(partitionMu_[pid]);
    BufferRequestUntilDeadlineLocked(partitionId, remote, req);
}

void
DynaReplica::HandleLateRequest(const TransportAddress &remote, const proto::RequestMessage &req, uint32_t partitionId)
{
    PartitionState &state = StateFor(partitionId);
    const RequestKey key = KeyOf(req);
    const uint64_t oldDeadline = req.deadline_us();
    const uint64_t opKey = OpKeyFromCommand(req.command());
    uint64_t revisedBase = 1;
    const auto releasedIt = state.lastReleasedByOpKey.find(opKey);
    if (releasedIt != state.lastReleasedByOpKey.end()) {
        revisedBase = releasedIt->second.deadlineUs + 1;
    }
    const uint64_t newDeadline = std::max(oldDeadline, revisedBase);

    proto::RequestMessage revised = req;
    revised.set_deadline_us(newDeadline);

    auto logIt = state.requestToLogIndex.find(key);
    if (logIt != state.requestToLogIndex.end() &&
        logIt->second < state.logEntries.size()) {
        LogEntry &entry = state.logEntries[logIt->second];
        if (entry.valid && entry.clientId == req.client_id() && entry.requestId == req.request_id()) {
            entry.deadlineUs = newDeadline;
        }
    }

    if (SlowPathDebugEnabled()) {
        Notice("DYNA slowpath leader buffer req=(%lu,%lu) part=%u old_deadline=%lu new_deadline=%lu",
               req.client_id(),
               req.request_id(),
               partitionId,
               oldDeadline,
               newDeadline);
    }

    state.pendingLateSyncByReq[key] = std::make_pair(oldDeadline, newDeadline);
    BufferRequestUntilDeadlineLocked(partitionId, remote, revised);
}

bool
DynaReplica::AppendToLogLocked(uint32_t partitionId, const proto::RequestMessage &req, LogEntry &outEntry)
{
    PartitionState &state = StateFor(partitionId);
    outEntry.valid = true;
    outEntry.committed = false;
    outEntry.applied = false;
    outEntry.logIndex = state.nextLogIndex++;
    outEntry.clientId = req.client_id();
    outEntry.requestId = req.request_id();
    outEntry.command = req.command();
    outEntry.sendTimeUs = req.send_time_us();
    outEntry.deadlineUs = req.deadline_us();
    outEntry.isRead = req.is_read();

    if (state.logEntries.size() <= outEntry.logIndex) {
        state.logEntries.resize(outEntry.logIndex + 1);
    }
    state.logEntries[outEntry.logIndex] = outEntry;
    state.requestToLogIndex[RequestKey{outEntry.clientId, outEntry.requestId}] = outEntry.logIndex;

    if (!AmLeader(partitionId)) {
        return false;
    }

    const uint64_t viewId = state.viewId.load(std::memory_order_relaxed);
    proto::SyncAck selfAck;
    selfAck.set_partition_id(partitionId);
    selfAck.set_view_id(viewId);
    selfAck.set_replica_id(static_cast<uint32_t>(replicaIdx));
    selfAck.set_log_index(outEntry.logIndex);
    selfAck.set_client_id(outEntry.clientId);
    selfAck.set_request_id(outEntry.requestId);
    selfAck.set_deadline_us(outEntry.deadlineUs);
    state.ackMap[outEntry.logIndex][static_cast<uint32_t>(replicaIdx)] = selfAck;
    return true;
}

void
DynaReplica::BroadcastSyncForLogEntry(uint32_t partitionId, uint64_t viewId, const LogEntry &entry)
{
    proto::SyncBroadcast sb;
    sb.set_partition_id(partitionId);
    sb.set_view_id(viewId);
    sb.set_log_index(entry.logIndex);
    sb.set_client_id(entry.clientId);
    sb.set_request_id(entry.requestId);
    sb.set_deadline_us(entry.deadlineUs);
    transport->SendMessageToAll(this, sb);
}

void
DynaReplica::BroadcastLateSyncForLogEntry(uint32_t partitionId,
                                          uint64_t viewId,
                                          const LogEntry &entry,
                                          uint64_t oldDeadlineUs,
                                          uint64_t newDeadlineUs)
{
    proto::LateSyncBroadcast lsb;
    lsb.set_partition_id(partitionId);
    lsb.set_view_id(viewId);
    lsb.set_log_index(entry.logIndex);
    lsb.set_client_id(entry.clientId);
    lsb.set_request_id(entry.requestId);
    lsb.set_old_deadline_us(oldDeadlineUs);
    lsb.set_new_deadline_us(newDeadlineUs);
    transport->SendMessageToAll(this, lsb);
}

void
DynaReplica::HandleSyncBroadcast(const TransportAddress &remote, const proto::SyncBroadcast &sb)
{
    const uint32_t partitionId = sb.partition_id() % numPartitions;
    const uint32_t pid = partitionId % numPartitions;
    proto::SyncAck ack;
    bool sendAck = false;
    {
        std::lock_guard<std::mutex> partitionLock(partitionMu_[pid]);
        PartitionState &state = StateFor(partitionId);
        if (sb.view_id() != state.viewId.load(std::memory_order_relaxed) || AmLeader(partitionId) ||
            !IsValidSyncBroadcast(partitionId, sb)) {
            return;
        }

        EnsureFollowerEntryFromSync(partitionId, sb);

        ack.set_partition_id(partitionId);
        ack.set_view_id(state.viewId.load(std::memory_order_relaxed));
        ack.set_replica_id(static_cast<uint32_t>(replicaIdx));
        ack.set_log_index(sb.log_index());
        ack.set_client_id(sb.client_id());
        ack.set_request_id(sb.request_id());
        ack.set_deadline_us(sb.deadline_us());
        sendAck = true;
    }
    if (sendAck) {
        transport->SendMessage(this, remote, ack);
    }
}

void
DynaReplica::HandleSyncAck(const TransportAddress &remote, const proto::SyncAck &ack)
{
    (void)remote;
    const uint32_t partitionId = ack.partition_id() % numPartitions;
    const uint32_t pid = partitionId % numPartitions;
    {
        std::lock_guard<std::mutex> partitionLock(partitionMu_[pid]);
        const PartitionState &state = StateFor(partitionId);
        if (!AmLeader(partitionId) || ack.view_id() != state.viewId.load(std::memory_order_relaxed) ||
            !IsValidSyncAck(partitionId, ack)) {
            return;
        }
    }
    TryCommitFromAck(partitionId, ack);
}

void
DynaReplica::HandleCommitBroadcast(const TransportAddress &remote, const proto::CommitBroadcast &cb)
{
    (void)remote;
    const uint32_t partitionId = cb.partition_id() % numPartitions;
    const uint32_t pid = partitionId % numPartitions;
    uint64_t upToLogIndex = 0;
    {
        std::lock_guard<std::mutex> partitionLock(partitionMu_[pid]);
        const PartitionState &state = StateFor(partitionId);
        if (cb.view_id() != state.viewId.load(std::memory_order_relaxed)) {
            return;
        }
        if (cb.up_to_log_index() <= state.commitPoint) {
            return;
        }
        upToLogIndex = cb.up_to_log_index();
    }
    if (!PersistCommittedRange(partitionId, upToLogIndex)) {
        return;
    }
    {
        std::lock_guard<std::mutex> partitionLock(partitionMu_[pid]);
        ApplyCommittedUpTo(partitionId, upToLogIndex);
    }
}

void
DynaReplica::HandleLateSyncBroadcast(const TransportAddress &remote, const proto::LateSyncBroadcast &lsb)
{
    (void)remote;
    const uint32_t partitionId = lsb.partition_id() % numPartitions;
    const uint32_t pid = partitionId % numPartitions;
    std::shared_ptr<TransportAddress> clientAddr;
    proto::RequestMessage req;
    {
        std::lock_guard<std::mutex> partitionLock(partitionMu_[pid]);
        PartitionState &state = StateFor(partitionId);
        if (lsb.view_id() != state.viewId.load(std::memory_order_relaxed) || AmLeader(partitionId) ||
            lsb.log_index() < state.commitPoint) {
            return;
        }

        proto::SyncBroadcast sb;
        sb.set_partition_id(partitionId);
        sb.set_view_id(lsb.view_id());
        sb.set_log_index(lsb.log_index());
        sb.set_client_id(lsb.client_id());
        sb.set_request_id(lsb.request_id());
        sb.set_deadline_us(lsb.new_deadline_us());
        EnsureFollowerEntryFromSync(partitionId, sb);
        if (SlowPathDebugEnabled()) {
            Notice("DYNA slowpath follower sync req=(%lu,%lu) part=%u log=%lu old_deadline=%lu new_deadline=%lu",
                   lsb.client_id(),
                   lsb.request_id(),
                   partitionId,
                   lsb.log_index(),
                   lsb.old_deadline_us(),
                   lsb.new_deadline_us());
        }

        const RequestKey key{lsb.client_id(), lsb.request_id()};
        auto addrIt = state.requestClients.find(key);
        if (addrIt == state.requestClients.end() || !addrIt->second) {
            return;
        }
        clientAddr = addrIt->second;

        if (!TryGetRequestMessage(partitionId, key, req)) {
            req.set_partition_id(partitionId);
            req.set_client_id(lsb.client_id());
            req.set_request_id(lsb.request_id());
            req.set_deadline_us(lsb.new_deadline_us());
        } else {
            req.set_deadline_us(lsb.new_deadline_us());
        }
        BufferRequestUntilDeadlineLocked(partitionId, *clientAddr, req);
    }
}

void
DynaReplica::EnsureFollowerEntryFromSync(uint32_t partitionId, const proto::SyncBroadcast &sb)
{
    PartitionState &state = StateFor(partitionId);
    const RequestKey key{sb.client_id(), sb.request_id()};
    state.pendingSyncByReq[key] = std::make_pair(sb.log_index(), sb.deadline_us());
    if (state.logEntries.size() <= sb.log_index()) {
        state.logEntries.resize(sb.log_index() + 1);
    }

    LogEntry &entry = state.logEntries[sb.log_index()];
    if (entry.valid) {
        if (entry.clientId == sb.client_id() && entry.requestId == sb.request_id()) {
            entry.deadlineUs = sb.deadline_us();
            return;
        }
    }

    if (!TryPopulateEntryFromBuffers(partitionId, sb)) {
        RequestFetch(partitionId, sb.client_id(), sb.request_id());
        return;
    }

    auto it = state.latBuffer.find(key);
    if (it == state.latBuffer.end()) {
        RequestFetch(partitionId, sb.client_id(), sb.request_id());
        return;
    }
    const proto::RequestMessage &req = it->second;
    entry.valid = true;
    entry.committed = false;
    entry.applied = false;
    entry.logIndex = sb.log_index();
    entry.clientId = sb.client_id();
    entry.requestId = sb.request_id();
    entry.command = req.command();
    entry.sendTimeUs = req.send_time_us();
    entry.deadlineUs = sb.deadline_us();
    entry.isRead = req.is_read();
    state.requestToLogIndex[key] = sb.log_index();
    state.pendingSyncByReq.erase(key);
}

void
DynaReplica::HandleFetchRequest(const TransportAddress &remote, const proto::FetchRequest &fr)
{
    const uint32_t partitionId = fr.partition_id() % numPartitions;
    const uint32_t pid = partitionId % numPartitions;
    proto::FetchResponse resp;
    bool sendResp = false;
    {
        std::lock_guard<std::mutex> partitionLock(partitionMu_[pid]);
        if (!AmLeader(partitionId)) {
            return;
        }
        const RequestKey key{fr.client_id(), fr.request_id()};

        proto::RequestMessage req;
        if (!TryGetRequestMessage(partitionId, key, req)) {
            return;
        }

        resp.set_partition_id(partitionId);
        resp.set_client_id(fr.client_id());
        resp.set_request_id(fr.request_id());
        resp.set_command(req.command());
        resp.set_send_time_us(req.send_time_us());
        resp.set_is_read(req.is_read());
        sendResp = true;
    }
    if (sendResp) {
        transport->SendMessage(this, remote, resp);
    }
}

void
DynaReplica::HandleFetchResponse(const TransportAddress &remote, const proto::FetchResponse &fr)
{
    (void)remote;
    const uint32_t partitionId = fr.partition_id() % numPartitions;
    const uint32_t pid = partitionId % numPartitions;
    {
        std::lock_guard<std::mutex> partitionLock(partitionMu_[pid]);
        PartitionState &state = StateFor(partitionId);
        RequestKey key{fr.client_id(), fr.request_id()};
        if (state.latBuffer.find(key) != state.latBuffer.end()) {
            return;
        }

        proto::RequestMessage req;
        req.set_partition_id(partitionId);
        req.set_client_id(fr.client_id());
        req.set_request_id(fr.request_id());
        req.set_command(fr.command());
        req.set_send_time_us(fr.send_time_us());
        req.set_is_read(fr.is_read());
        req.set_deadline_us(0);
        state.latBuffer[key] = req;

        auto ps = state.pendingSyncByReq.find(key);
        if (ps != state.pendingSyncByReq.end()) {
            proto::SyncBroadcast sb;
            sb.set_partition_id(partitionId);
            sb.set_view_id(state.viewId.load(std::memory_order_relaxed));
            sb.set_log_index(ps->second.first);
            sb.set_client_id(fr.client_id());
            sb.set_request_id(fr.request_id());
            sb.set_deadline_us(ps->second.second);
            EnsureFollowerEntryFromSync(partitionId, sb);
        }
    }
}

void
DynaReplica::RequestFetch(uint32_t partitionId, uint64_t clientId, uint64_t requestId)
{
    const uint32_t pid = partitionId % numPartitions;
    proto::FetchRequest fr;
    fr.set_partition_id(pid);
    fr.set_client_id(clientId);
    fr.set_request_id(requestId);
    const int leaderReplica = static_cast<int>(PartitionLeader(pid));
    transport->SendMessageToReplica(this, leaderReplica, fr);
}

bool
DynaReplica::TryPopulateEntryFromBuffers(uint32_t partitionId, const proto::SyncBroadcast &sb)
{
    PartitionState &state = StateFor(partitionId);
    RequestKey key{sb.client_id(), sb.request_id()};
    auto it = state.latBuffer.find(key);
    if (it != state.latBuffer.end()) {
        return true;
    }

    proto::RequestMessage req;
    if (!TryGetRequestMessage(partitionId, key, req)) {
        return false;
    }

    req.set_partition_id(partitionId);
    req.set_deadline_us(sb.deadline_us());
    state.latBuffer[key] = req;
    return true;
}

bool
DynaReplica::TryGetRequestMessage(uint32_t partitionId, const RequestKey &key, proto::RequestMessage &out) const
{
    const PartitionState &state = StateFor(partitionId);
    if (auto it = state.latBuffer.find(key); it != state.latBuffer.end()) {
        out = it->second;
        return true;
    }

    for (auto pq = state.earlyBuffer; !pq.empty(); pq.pop()) {
        const auto &req = pq.top().request;
        if (req.client_id() == key.clientId && req.request_id() == key.requestId) {
            out = req;
            return true;
        }
    }

    if (auto idxIt = state.requestToLogIndex.find(key); idxIt != state.requestToLogIndex.end()) {
        const uint64_t idx = idxIt->second;
        if (idx < state.logEntries.size()) {
            const auto &entry = state.logEntries[idx];
            if (entry.valid && entry.clientId == key.clientId && entry.requestId == key.requestId) {
                out.set_partition_id(partitionId);
                out.set_client_id(entry.clientId);
                out.set_request_id(entry.requestId);
                out.set_command(entry.command);
                out.set_send_time_us(entry.sendTimeUs);
                out.set_deadline_us(entry.deadlineUs);
                out.set_is_read(entry.isRead);
                return true;
            }
        }
    }

    return false;
}

bool
DynaReplica::EntryMatchesSync(const LogEntry &entry, const proto::SyncBroadcast &sb) const
{
    return entry.valid &&
           entry.clientId == sb.client_id() &&
           entry.requestId == sb.request_id() &&
           entry.deadlineUs == sb.deadline_us();
}

std::string
DynaReplica::HexEncode(const std::string &data)
{
    static const char *kHex = "0123456789abcdef";
    std::string out;
    out.reserve(data.size() * 2);
    for (unsigned char c : data) {
        out.push_back(kHex[(c >> 4) & 0x0F]);
        out.push_back(kHex[c & 0x0F]);
    }
    return out;
}

std::string
DynaReplica::WalPath(uint32_t partitionId) const
{
    return walDir_ + "/replica-" + std::to_string(replicaIdx) +
           "-partition-" + std::to_string(partitionId) + ".wal";
}

bool
DynaReplica::PersistCommittedRange(uint32_t partitionId, uint64_t upToLogIndex)
{
    const uint32_t pid = partitionId % numPartitions;
    const std::string path = WalPath(pid);
    std::vector<std::string> walLines;
    int64_t targetSynced = -1;

    {
        std::lock_guard<std::mutex> partitionLock(partitionMu_[pid]);
        PartitionState &state = StateFor(partitionId);
        if (state.logEntries.empty()) {
            return true;
        }
        const uint64_t maxIndex = std::min<uint64_t>(upToLogIndex, state.logEntries.size() - 1);
        const uint64_t start = static_cast<uint64_t>(state.walSyncedUpTo + 1);
        if (start > maxIndex) {
            return true;
        }

        walLines.reserve(static_cast<size_t>(maxIndex - start + 1));
        for (uint64_t i = start; i <= maxIndex; ++i) {
            const LogEntry &entry = state.logEntries[i];
            if (!entry.valid) {
                continue;
            }
            const std::string commandHex = HexEncode(entry.command);
            char lineBuf[4096];
            const int n = std::snprintf(lineBuf,
                                        sizeof(lineBuf),
                                        "%u|%lu|%lu|%lu|%lu|%u|%s\n",
                                        pid,
                                        static_cast<unsigned long>(entry.logIndex),
                                        static_cast<unsigned long>(entry.clientId),
                                        static_cast<unsigned long>(entry.requestId),
                                        static_cast<unsigned long>(entry.deadlineUs),
                                        entry.isRead ? 1u : 0u,
                                        commandHex.c_str());
            if (n < 0 || static_cast<size_t>(n) >= sizeof(lineBuf)) {
                Warning("Failed formatting DYNA WAL line for %s", path.c_str());
                return false;
            }
            walLines.emplace_back(lineBuf, static_cast<size_t>(n));
        }
        targetSynced = static_cast<int64_t>(maxIndex);
    }

    if (walLines.empty()) {
        if (targetSynced >= 0) {
            std::lock_guard<std::mutex> partitionLock(partitionMu_[pid]);
            PartitionState &state = StateFor(partitionId);
            state.walSyncedUpTo = std::max(state.walSyncedUpTo, targetSynced);
        }
        return true;
    }

    FILE *fp = std::fopen(path.c_str(), "a");
    if (fp == nullptr) {
        Warning("Failed opening DYNA WAL %s: %s", path.c_str(), std::strerror(errno));
        return false;
    }

    for (const std::string &line : walLines) {
        if (std::fwrite(line.data(), 1, line.size(), fp) != line.size()) {
            Warning("Failed writing DYNA WAL %s", path.c_str());
            std::fclose(fp);
            return false;
        }
    }

    if (std::fflush(fp) != 0) {
        Warning("Failed flushing DYNA WAL %s", path.c_str());
        std::fclose(fp);
        return false;
    }
    if (::fsync(fileno(fp)) != 0) {
        Warning("Failed fsync DYNA WAL %s: %s", path.c_str(), std::strerror(errno));
        std::fclose(fp);
        return false;
    }
    std::fclose(fp);

    {
        std::lock_guard<std::mutex> partitionLock(partitionMu_[pid]);
        PartitionState &state = StateFor(partitionId);
        state.walSyncedUpTo = std::max(state.walSyncedUpTo, targetSynced);
    }
    return true;
}

bool
DynaReplica::LeaderCommitToDiskAndBroadcast(uint32_t partitionId, uint64_t upToLogIndex)
{
    const uint32_t pid = partitionId % numPartitions;
    if (!PersistCommittedRange(partitionId, upToLogIndex)) {
        return false;
    }

    uint64_t viewId = 0;
    {
        std::lock_guard<std::mutex> partitionLock(partitionMu_[pid]);
        PartitionState &state = StateFor(partitionId);
        state.commitPoint = std::max(state.commitPoint, upToLogIndex);
        viewId = state.viewId.load(std::memory_order_relaxed);
        ApplyCommittedUpTo(partitionId, upToLogIndex);
    }
    BroadcastCommit(partitionId, viewId, upToLogIndex);
    return true;
}

void
DynaReplica::TryCommitFromAck(uint32_t partitionId, const proto::SyncAck &ack)
{
    const uint32_t pid = partitionId % numPartitions;
    uint64_t upToCommitPoint = 0;
    bool shouldCommit = false;
    {
        std::lock_guard<std::mutex> partitionLock(partitionMu_[pid]);
        PartitionState &state = StateFor(partitionId);
        if (state.logEntries.size() <= ack.log_index()) {
            return;
        }
        LogEntry &entry = state.logEntries[ack.log_index()];
        if (!entry.valid) {
            return;
        }
        if (entry.clientId != ack.client_id() || entry.requestId != ack.request_id()) {
            return;
        }
        if (entry.deadlineUs != ack.deadline_us()) {
            return;
        }
        if (entry.committed) {
            return;
        }

        state.ackMap[ack.log_index()][ack.replica_id()] = ack;
        const size_t quorum = static_cast<size_t>(config.f + 1);
        if (state.ackMap[ack.log_index()].size() < quorum) {
            return;
        }

        upToCommitPoint = std::max(state.commitPoint, ack.log_index());
        if (upToCommitPoint <= state.commitPoint &&
            state.walSyncedUpTo >= static_cast<int64_t>(upToCommitPoint)) {
            return;
        }
        shouldCommit = true;
    }

    if (!shouldCommit) {
        return;
    }
    LeaderCommitToDiskAndBroadcast(partitionId, upToCommitPoint);
}

void
DynaReplica::ApplyCommittedUpTo(uint32_t partitionId, uint64_t upToLogIndex)
{
    PartitionState &state = StateFor(partitionId);
    if (state.logEntries.empty()) {
        return;
    }
    const uint64_t maxIndex = std::min<uint64_t>(upToLogIndex, state.logEntries.size() - 1);
    for (uint64_t i = 0; i <= maxIndex; ++i) {
        LogEntry &entry = state.logEntries[i];
        if (!entry.valid) {
            continue;
        }
        entry.committed = true;
        if (entry.applied) {
            continue;
        }

        if (entry.isRead) {
            auto key = RequestKey{entry.clientId, entry.requestId};
            auto addrIt = state.requestClients.find(key);
            if (AmLeader(partitionId) && addrIt != state.requestClients.end()) {
                proto::RequestMessage req;
                req.set_partition_id(partitionId);
                req.set_client_id(entry.clientId);
                req.set_request_id(entry.requestId);
                req.set_command(entry.command);
                req.set_send_time_us(entry.sendTimeUs);
                req.set_deadline_us(entry.deadlineUs);
                req.set_is_read(true);
                MaybeSendReadResult(partitionId, *addrIt->second, req);
            }
            UpdateDeadlineCounter(state.deadlineCounter, entry.deadlineUs);
            entry.applied = true;
            continue;
        }

        char op = '\0';
        std::string key;
        std::string value;
        if (!ParseCommand(entry.command, op, key, value)) {
            continue;
        }
        {
            std::lock_guard<std::mutex> lock(kvMutex_);
            kvStore[key] = value;
            appliedWrites.push_back(StoredWrite{partitionId, entry.logIndex, key, value});
        }
        UpdateDeadlineCounter(state.deadlineCounter, entry.deadlineUs);
        entry.applied = true;
    }
}

void
DynaReplica::BroadcastCommit(uint32_t partitionId, uint64_t viewId, uint64_t upToLogIndex)
{
    proto::CommitBroadcast cb;
    cb.set_partition_id(partitionId);
    cb.set_view_id(viewId);
    cb.set_up_to_log_index(upToLogIndex);
    transport->SendMessageToAll(this, cb);
}

void
DynaReplica::SendFastReply(uint32_t partitionId,
                           const TransportAddress &remote,
                           uint64_t clientId,
                           uint64_t requestId,
                           uint64_t deadlineUs)
{
    const PartitionState &state = StateFor(partitionId);
    proto::FastReply reply;
    reply.set_partition_id(partitionId);
    reply.set_request_id(requestId);
    reply.set_replica_id(static_cast<uint32_t>(replicaIdx));
    reply.set_view_id(state.viewId.load(std::memory_order_relaxed));
    reply.set_deadline_us(deadlineUs);
    uint64_t owd = 0;
    {
        std::lock_guard<std::mutex> lock(owdMutex_);
        owd = owdEstimator_.estimate_us(clientId);
    }
    if (owd > 0) {
        reply.set_owd_us(owd);
    }
    transport->SendMessage(this, remote, reply);
}

void
DynaReplica::MaybeSendReadResult(uint32_t partitionId, const TransportAddress &remote, const proto::RequestMessage &req)
{
    PartitionState &state = StateFor(partitionId);
    if (state.repliedReads.find(req.request_id()) != state.repliedReads.end()) {
        return;
    }

    char op = '\0';
    std::string key;
    std::string value;
    if (!ParseCommand(req.command(), op, key, value) || !IsReadOp(op)) {
        return;
    }
    if (!EnsureReadVisibilityBeforeDeadline(partitionId, req.deadline_us())) {
        return;
    }

    std::string result = "NOTFOUND";
    {
        std::lock_guard<std::mutex> lock(kvMutex_);
        if (auto iter = kvStore.find(key); iter != kvStore.end()) {
            result = iter->second;
        }
    }

    proto::ReadResult reply;
    reply.set_partition_id(partitionId);
    reply.set_request_id(req.request_id());
    reply.set_view_id(state.viewId.load(std::memory_order_relaxed));
    reply.set_deadline_us(req.deadline_us());
    reply.set_data(result);
    uint64_t owd = 0;
    {
        std::lock_guard<std::mutex> lock(owdMutex_);
        owd = owdEstimator_.estimate_us(req.client_id());
    }
    if (owd > 0) {
        reply.set_owd_us(owd);
    }
    transport->SendMessage(this, remote, reply);
    state.repliedReads.insert(req.request_id());
}

bool
DynaReplica::ParseCommand(const std::string &command, char &op, std::string &key, std::string &value) const
{
    if (command.size() < 25) {
        return false;
    }
    op = command[0];
    key = command.substr(1, 24);
    value.clear();
    if (!IsReadOp(op) && command.size() > 25) {
        value = command.substr(25);
    }
    return true;
}

bool
DynaReplica::IsReadOp(char op) const
{
    return op == 'r' || op == 'R';
}

bool
DynaReplica::EnsureReadVisibilityBeforeDeadline(uint32_t partitionId, uint64_t readDeadlineUs)
{
    PartitionState &state = StateFor(partitionId);
    for (uint64_t i = 0; i < state.logEntries.size(); ++i) {
        const LogEntry &entry = state.logEntries[i];
        if (!entry.valid || entry.isRead) {
            continue;
        }
        if (entry.deadlineUs >= readDeadlineUs) {
            continue;
        }
        if (!entry.committed || !entry.applied) {
            return false;
        }
    }
    return true;
}

bool
DynaReplica::IsValidSyncBroadcast(uint32_t partitionId, const proto::SyncBroadcast &sb) const
{
    const PartitionState &state = StateFor(partitionId);
    if (sb.log_index() < state.commitPoint) {
        return false;
    }
    return true;
}

bool
DynaReplica::IsValidSyncAck(uint32_t partitionId, const proto::SyncAck &ack) const
{
    const PartitionState &state = StateFor(partitionId);
    if (ack.log_index() < state.commitPoint) {
        return false;
    }
    return true;
}

} // namespace dyna
} // namespace specpaxos

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "dyna/replica.h"

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

DynaReplica::DynaReplica(const Configuration &config, int replicaIdx, Transport *transport)
    : config(config),
      numPartitions(static_cast<uint32_t>(std::max(1, config.n))),
      replicaIdx(replicaIdx),
      transport(transport),
      partitions(numPartitions),
      commitQu_(numPartitions),
      incomingQu_(numPartitions),
      owdEstimator_(DeadlineConfig::FromEnvironment()),
      walDir_(WalDirFromEnv())
{
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
}

void
DynaReplica::UpdateDeadlineCounter(std::atomic<uint64_t> &counter, uint64_t value)
{
    uint64_t cur = counter.load(std::memory_order_relaxed);
    while (value > cur &&
           !counter.compare_exchange_weak(cur, value, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

bool
DynaReplica::EnqueueFast(InboundKind kind,
                         const TransportAddress &remote,
                         const std::string_view data)
{
    InboundWork work;
    work.kind = kind;
    work.remote.reset(remote.clone());
    work.payload.assign(data.data(), data.length());
    if (!fastInboundQu_.enqueue(std::move(work))) {
        Warning("DYNA fast inbound queue full; dropping message kind=%d", static_cast<int>(kind));
        return false;
    }
    return true;
}

bool
DynaReplica::EnqueueCommit(uint32_t partitionId,
                           InboundKind kind,
                           const TransportAddress &remote,
                           const std::string_view data)
{
    const uint32_t pid = partitionId % numPartitions;
    InboundWork work;
    work.kind = kind;
    work.remote.reset(remote.clone());
    work.payload.assign(data.data(), data.length());
    if (!commitQu_[pid].enqueue(std::move(work))) {
        Warning("DYNA commit queue full part=%u kind=%d", pid, static_cast<int>(kind));
        return false;
    }
    return true;
}

void
DynaReplica::StartWorkers()
{
    workersRunning_.store(true);
    fastWorkerThread_ = std::thread(&DynaReplica::FastWorkerLoop, this);
    commitThreads_.reserve(numPartitions);
    for (uint32_t partitionId = 0; partitionId < numPartitions; ++partitionId) {
        commitThreads_.emplace_back(&DynaReplica::CommitPartitionLoop, this, partitionId);
    }
}

void
DynaReplica::StopWorkers()
{
    if (!workersRunning_.exchange(false)) {
        return;
    }
    InboundWork fastShutdown;
    fastShutdown.kind = InboundKind::SHUTDOWN;
    fastInboundQu_.enqueue(std::move(fastShutdown));
    for (auto &qu : commitQu_) {
        InboundWork commitShutdown;
        commitShutdown.kind = InboundKind::SHUTDOWN;
        qu.enqueue(std::move(commitShutdown));
    }
    if (fastWorkerThread_.joinable()) {
        fastWorkerThread_.join();
    }
    for (auto &thread : commitThreads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    commitThreads_.clear();
}

void
DynaReplica::FastWorkerLoop()
{
    while (workersRunning_.load()) {
        InboundWork work;
        bool handledWork = false;
        while (fastInboundQu_.try_dequeue(work)) {
            if (work.kind == InboundKind::SHUTDOWN) {
                return;
            }
            DispatchFastInbound(work);
            handledWork = true;
        }
        if (!handledWork) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
}

void
DynaReplica::CommitPartitionLoop(uint32_t partitionId)
{
    using clock = std::chrono::steady_clock;
    auto lastTick = clock::now();
    const auto tickInterval = std::chrono::milliseconds(1);

    while (workersRunning_.load()) {
        bool handledWork = false;
        DrainIncomingBuffered(partitionId);

        InboundWork work;
        while (commitQu_[partitionId].try_dequeue(work)) {
            if (work.kind == InboundKind::SHUTDOWN) {
                return;
            }
            DispatchCommitInbound(partitionId, work);
            handledWork = true;
        }

        const auto now = clock::now();
        if (now - lastTick >= tickInterval) {
            ProcessDueRequestsTick(partitionId);
            lastTick = now;
        }

        if (!handledWork) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
}

void
DynaReplica::DrainIncomingBuffered(uint32_t partitionId)
{
    PartitionState &state = StateFor(partitionId);
    BufferedRequest buffered;
    while (incomingQu_[partitionId].try_dequeue(buffered)) {
        const RequestKey key = KeyOf(buffered.request);
        state.requestClients[key] = buffered.clientAddress;
        state.latBuffer[key] = buffered.request;
        buffered.sequence = state.nextSequence++;
        state.earlyBuffer.push(std::move(buffered));
    }
}

void
DynaReplica::DispatchFastInbound(InboundWork &work)
{
    if (!work.remote) {
        Warning("DYNA fast inbound work missing remote address");
        return;
    }
    const TransportAddress &remote = *work.remote;

    if (work.kind != InboundKind::REQUEST) {
        Panic("Unexpected DYNA fast inbound work kind");
    }
    ALLOCATE_MESSAGE(proto::RequestMessage, req);
    if (!req->ParseFromString(work.payload)) {
        Warning("Failed to parse RequestMessage");
        return;
    }
    HandleRequestFast(remote, *req);
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
        HandleRequestSlow(remote, *req);
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
        EnqueueFast(InboundKind::REQUEST, remote, data);
        return;
    }
    if (type == syncBroadcastName) {
        ALLOCATE_MESSAGE(proto::SyncBroadcast, sb);
        if (!sb->ParseFromArray(data.data(), data.size())) {
            return;
        }
        EnqueueCommit(sb->partition_id(), InboundKind::SYNC_BROADCAST, remote, data);
    } else if (type == syncAckName) {
        ALLOCATE_MESSAGE(proto::SyncAck, ack);
        if (!ack->ParseFromArray(data.data(), data.size())) {
            return;
        }
        EnqueueCommit(ack->partition_id(), InboundKind::SYNC_ACK, remote, data);
    } else if (type == commitBroadcastName) {
        ALLOCATE_MESSAGE(proto::CommitBroadcast, cb);
        if (!cb->ParseFromArray(data.data(), data.size())) {
            return;
        }
        EnqueueCommit(cb->partition_id(), InboundKind::COMMIT_BROADCAST, remote, data);
    } else if (type == lateSyncName) {
        ALLOCATE_MESSAGE(proto::LateSyncBroadcast, lsb);
        if (!lsb->ParseFromArray(data.data(), data.size())) {
            return;
        }
        EnqueueCommit(lsb->partition_id(), InboundKind::LATE_SYNC_BROADCAST, remote, data);
    } else if (type == fetchRequestName) {
        ALLOCATE_MESSAGE(proto::FetchRequest, fr);
        if (!fr->ParseFromArray(data.data(), data.size())) {
            return;
        }
        EnqueueCommit(fr->partition_id(), InboundKind::FETCH_REQUEST, remote, data);
    } else if (type == fetchResponseName) {
        ALLOCATE_MESSAGE(proto::FetchResponse, fr);
        if (!fr->ParseFromArray(data.data(), data.size())) {
            return;
        }
        EnqueueCommit(fr->partition_id(), InboundKind::FETCH_RESPONSE, remote, data);
    } else {
        Panic("Unexpected DYNA message type: %s", type.data());
    }
}

uint64_t
DynaReplica::NowMicros() const
{
    using namespace std::chrono;
    return duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();
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

void
DynaReplica::ProcessDueRequestsTick(uint32_t partitionId)
{
    ProcessDueRequests(partitionId);
}

void
DynaReplica::HandleRequestFast(const TransportAddress &remote, const proto::RequestMessage &req)
{
    const uint32_t partitionId = req.partition_id() % numPartitions;
    const uint64_t recv_us = NowMicros();
    if (recv_us > req.send_time_us()) {
        std::lock_guard<std::mutex> lock(owdMutex_);
        owdEstimator_.sample(req.client_id(), recv_us - req.send_time_us());
    }

    const PartitionState &state = StateFor(partitionId);
    if (req.deadline_us() <= state.deadlineCounter.load(std::memory_order_acquire)) {
        if (AmLeader(partitionId)) {
            if (SlowPathDebugEnabled()) {
                Notice("DYNA slowpath req=(%lu,%lu) part=%u deadline=%lu counter=%lu",
                       req.client_id(),
                       req.request_id(),
                       partitionId,
                       req.deadline_us(),
                       state.deadlineCounter.load(std::memory_order_relaxed));
            }
            const std::string payload = req.SerializeAsString();
            EnqueueCommit(partitionId, InboundKind::REQUEST, remote, payload);
        }
        return;
    }

    SendFastReply(partitionId, remote, req.client_id(), req.request_id(), req.deadline_us());

    BufferedRequest buffered;
    buffered.request = req;
    buffered.clientAddress = std::shared_ptr<TransportAddress>(remote.clone());
    if (!incomingQu_[partitionId].enqueue(std::move(buffered))) {
        Warning("DYNA incoming queue full part=%u req=(%lu,%lu)", partitionId, req.client_id(), req.request_id());
    }
}

void
DynaReplica::HandleRequestSlow(const TransportAddress &remote, const proto::RequestMessage &req)
{
    const uint32_t partitionId = req.partition_id() % numPartitions;
    PartitionState &state = StateFor(partitionId);
    state.requestClients[KeyOf(req)] = std::shared_ptr<TransportAddress>(remote.clone());
    state.latBuffer[KeyOf(req)] = req;
    HandleLateRequest(remote, req, partitionId);
}

void
DynaReplica::HandleLateRequest(const TransportAddress &remote, const proto::RequestMessage &req, uint32_t partitionId)
{
    PartitionState &state = StateFor(partitionId);
    const RequestKey key = KeyOf(req);
    const uint64_t oldDeadline = req.deadline_us();
    const uint64_t newDeadline =
        std::max(oldDeadline, state.deadlineCounter.load(std::memory_order_relaxed) + 1);

    proto::RequestMessage revised = req;
    revised.set_deadline_us(newDeadline);
    state.latBuffer[key] = revised;
    state.requestClients[key] = std::shared_ptr<TransportAddress>(remote.clone());

    uint64_t logIndex = 0;
    auto logIt = state.requestToLogIndex.find(key);
    if (logIt == state.requestToLogIndex.end()) {
        AppendToLog(partitionId, revised);
        logIt = state.requestToLogIndex.find(key);
        if (logIt == state.requestToLogIndex.end()) {
            return;
        }
    } else {
        if (logIt->second < state.logEntries.size()) {
            LogEntry &entry = state.logEntries[logIt->second];
            if (entry.valid && entry.clientId == req.client_id() && entry.requestId == req.request_id()) {
                entry.deadlineUs = newDeadline;
            }
        }
    }
    logIndex = logIt->second;
    if (logIndex < state.logEntries.size() && state.logEntries[logIndex].valid) {
        if (SlowPathDebugEnabled()) {
            Notice("DYNA slowpath leader rewrite req=(%lu,%lu) part=%u log=%lu old_deadline=%lu new_deadline=%lu",
                   req.client_id(),
                   req.request_id(),
                   partitionId,
                   logIndex,
                   oldDeadline,
                   newDeadline);
        }
        BroadcastLateSyncForLogEntry(partitionId, state.logEntries[logIndex], oldDeadline, newDeadline);
    }
    SendFastReply(partitionId, remote, req.client_id(), req.request_id(), newDeadline);
}

//pops requests on timer
void
DynaReplica::ProcessDueRequests(uint32_t partitionId)
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

        char op = '\0';
        std::string key;
        std::string value;
        if (!ParseCommand(current.request.command(), op, key, value)) {
            continue;
        }

        const auto keyReq = KeyOf(current.request);
        if (state.requestToLogIndex.find(keyReq) == state.requestToLogIndex.end()) {
            AppendToLog(partitionId, current.request);
        }
    }
}

void
DynaReplica::AppendToLog(uint32_t partitionId, const proto::RequestMessage &req)
{
    PartitionState &state = StateFor(partitionId);
    LogEntry entry;
    entry.valid = true;
    entry.committed = false;
    entry.applied = false;
    entry.logIndex = state.nextLogIndex++;
    entry.clientId = req.client_id();
    entry.requestId = req.request_id();
    entry.command = req.command();
    entry.sendTimeUs = req.send_time_us();
    entry.deadlineUs = req.deadline_us();
    entry.isRead = req.is_read();

    if (state.logEntries.size() <= entry.logIndex) {
        state.logEntries.resize(entry.logIndex + 1);
    }
    state.logEntries[entry.logIndex] = entry;
    state.requestToLogIndex[RequestKey{entry.clientId, entry.requestId}] = entry.logIndex;

    if (AmLeader(partitionId)) {
        BroadcastSyncForLogEntry(partitionId, entry);
    }
}

void
DynaReplica::BroadcastSyncForLogEntry(uint32_t partitionId, const LogEntry &entry)
{
    PartitionState &state = StateFor(partitionId);
    proto::SyncBroadcast sb;
    sb.set_partition_id(partitionId);
    sb.set_view_id(state.viewId.load(std::memory_order_relaxed));
    sb.set_log_index(entry.logIndex);
    sb.set_client_id(entry.clientId);
    sb.set_request_id(entry.requestId);
    sb.set_deadline_us(entry.deadlineUs);

    proto::SyncAck selfAck;
    selfAck.set_partition_id(partitionId);
    selfAck.set_view_id(state.viewId.load(std::memory_order_relaxed));
    selfAck.set_replica_id(static_cast<uint32_t>(replicaIdx));
    selfAck.set_log_index(entry.logIndex);
    selfAck.set_client_id(entry.clientId);
    selfAck.set_request_id(entry.requestId);
    selfAck.set_deadline_us(entry.deadlineUs);
    state.ackMap[entry.logIndex][static_cast<uint32_t>(replicaIdx)] = selfAck;

    transport->SendMessageToAll(this, sb);
}

void
DynaReplica::BroadcastLateSyncForLogEntry(uint32_t partitionId,
                                          const LogEntry &entry,
                                          uint64_t oldDeadlineUs,
                                          uint64_t newDeadlineUs)
{
    PartitionState &state = StateFor(partitionId);
    proto::LateSyncBroadcast lsb;
    lsb.set_partition_id(partitionId);
    lsb.set_view_id(state.viewId.load(std::memory_order_relaxed));
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
    PartitionState &state = StateFor(partitionId);
    if (sb.view_id() != state.viewId.load(std::memory_order_relaxed) || AmLeader(partitionId) || !IsValidSyncBroadcast(partitionId, sb)) {
        return;
    }

    EnsureFollowerEntryFromSync(partitionId, sb);

    proto::SyncAck ack;
    ack.set_partition_id(partitionId);
    ack.set_view_id(state.viewId.load(std::memory_order_relaxed));
    ack.set_replica_id(static_cast<uint32_t>(replicaIdx));
    ack.set_log_index(sb.log_index());
    ack.set_client_id(sb.client_id());
    ack.set_request_id(sb.request_id());
    ack.set_deadline_us(sb.deadline_us());
    transport->SendMessage(this, remote, ack);
}

void
DynaReplica::HandleSyncAck(const TransportAddress &remote, const proto::SyncAck &ack)
{
    (void)remote;
    const uint32_t partitionId = ack.partition_id() % numPartitions;
    const PartitionState &state = StateFor(partitionId);
    if (!AmLeader(partitionId) || ack.view_id() != state.viewId.load(std::memory_order_relaxed) || !IsValidSyncAck(partitionId, ack)) {
        return;
    }
    TryCommitFromAck(partitionId, ack);
}

void
DynaReplica::HandleCommitBroadcast(const TransportAddress &remote, const proto::CommitBroadcast &cb)
{
    (void)remote;
    const uint32_t partitionId = cb.partition_id() % numPartitions;
    const PartitionState &state = StateFor(partitionId);
    if (cb.view_id() != state.viewId.load(std::memory_order_relaxed)) {
        return;
    }
    if (cb.up_to_log_index() <= state.commitPoint) {
        return;
    }
    if (!PersistCommittedRange(partitionId, cb.up_to_log_index())) {
        return;
    }
    ApplyCommittedUpTo(partitionId, cb.up_to_log_index());
}

void
DynaReplica::HandleLateSyncBroadcast(const TransportAddress &remote, const proto::LateSyncBroadcast &lsb)
{
    (void)remote;
    const uint32_t partitionId = lsb.partition_id() % numPartitions;
    PartitionState &state = StateFor(partitionId);
    if (lsb.view_id() != state.viewId.load(std::memory_order_relaxed) || AmLeader(partitionId) || lsb.log_index() < state.commitPoint) {
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
    if (addrIt != state.requestClients.end() && addrIt->second) {
        SendFastReply(partitionId, *addrIt->second, lsb.client_id(), lsb.request_id(), lsb.new_deadline_us());
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
    if (!AmLeader(partitionId)) {
        return;
    }
    const RequestKey key{fr.client_id(), fr.request_id()};

    proto::RequestMessage req;
    if (!TryGetRequestMessage(partitionId, key, req)) {
        return;
    }

    proto::FetchResponse resp;
    resp.set_partition_id(partitionId);
    resp.set_client_id(fr.client_id());
    resp.set_request_id(fr.request_id());
    resp.set_command(req.command());
    resp.set_send_time_us(req.send_time_us());
    resp.set_is_read(req.is_read());
    transport->SendMessage(this, remote, resp);
}

void
DynaReplica::HandleFetchResponse(const TransportAddress &remote, const proto::FetchResponse &fr)
{
    (void)remote;
    const uint32_t partitionId = fr.partition_id() % numPartitions;
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
    PartitionState &state = StateFor(partitionId);
    if (state.logEntries.empty()) {
        return true;
    }
    const uint64_t maxIndex = std::min<uint64_t>(upToLogIndex, state.logEntries.size() - 1);
    const uint64_t start = static_cast<uint64_t>(state.walSyncedUpTo + 1);
    if (start > maxIndex) {
        return true;
    }

    const std::string path = WalPath(partitionId);
    FILE *fp = std::fopen(path.c_str(), "a");
    if (fp == nullptr) {
        Warning("Failed opening DYNA WAL %s: %s", path.c_str(), std::strerror(errno));
        return false;
    }

    for (uint64_t i = start; i <= maxIndex; ++i) {
        const LogEntry &entry = state.logEntries[i];
        if (!entry.valid) {
            continue;
        }
        const std::string commandHex = HexEncode(entry.command);
        if (std::fprintf(fp,
                         "%u|%lu|%lu|%lu|%lu|%u|%s\n",
                         partitionId,
                         static_cast<unsigned long>(entry.logIndex),
                         static_cast<unsigned long>(entry.clientId),
                         static_cast<unsigned long>(entry.requestId),
                         static_cast<unsigned long>(entry.deadlineUs),
                         entry.isRead ? 1u : 0u,
                         commandHex.c_str()) < 0) {
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

    state.walSyncedUpTo = static_cast<int64_t>(maxIndex);
    return true;
}

bool
DynaReplica::LeaderCommitToDiskAndBroadcast(uint32_t partitionId, uint64_t upToLogIndex)
{
    if (!PersistCommittedRange(partitionId, upToLogIndex)) {
        return false;
    }
    PartitionState &state = StateFor(partitionId);
    state.commitPoint = std::max(state.commitPoint, upToLogIndex);
    BroadcastCommit(partitionId, upToLogIndex);
    return true;
}

void
DynaReplica::TryCommitFromAck(uint32_t partitionId, const proto::SyncAck &ack)
{
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

    const uint64_t upToCommitPoint = std::max(state.commitPoint, ack.log_index());
    if (upToCommitPoint <= state.commitPoint &&
        state.walSyncedUpTo >= static_cast<int64_t>(upToCommitPoint)) {
        return;
    }

    if (!LeaderCommitToDiskAndBroadcast(partitionId, upToCommitPoint)) {
        return;
    }
    ApplyCommittedUpTo(partitionId, upToCommitPoint);
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
DynaReplica::BroadcastCommit(uint32_t partitionId, uint64_t upToLogIndex)
{
    PartitionState &state = StateFor(partitionId);
    proto::CommitBroadcast cb;
    cb.set_partition_id(partitionId);
    cb.set_view_id(state.viewId.load(std::memory_order_relaxed));
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

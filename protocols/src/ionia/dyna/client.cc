// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

//MAJOR TODOS: Baseline implementations. View change + failure recovery. Multi partition coordinator. 


#include "dyna/client.h"
#include "dyna/stats_util.h"
#include "dyna/wall_clock.h"

#include "lib/assert.h"
#include "lib/message.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>

namespace specpaxos {
namespace dyna {

namespace {

struct ClientRuntimeStats {
    CounterWindow completions;
    CounterWindow deadlineMismatchDrops;
    CounterWindow epochResets;
    std::atomic<uint64_t> maxPendingPeak{0};
    LatencySampleBuffer totalUs;
    LatencySampleBuffer firstReplyUs;
    LatencySampleBuffer quorumWaitUs;

    void notePendingSize(size_t count)
    {
        uint64_t value = static_cast<uint64_t>(count);
        uint64_t cur = maxPendingPeak.load(std::memory_order_relaxed);
        while (value > cur &&
               !maxPendingPeak.compare_exchange_weak(cur, value, std::memory_order_relaxed)) {
        }
    }

    uint64_t snapshotMaxPending()
    {
        return maxPendingPeak.exchange(0, std::memory_order_relaxed);
    }
};

ClientRuntimeStats g_clientStats;

} // namespace

static bool
SlowPathDebugEnabled()
{
    static const bool enabled = []() {
        const char *v = std::getenv("DYNA_SLOWPATH_DEBUG");
        return v && *v && std::strcmp(v, "0") != 0;
    }();
    return enabled;
}

DynaClient::DynaClient(const Configuration &config, Transport *transport, uint64_t clientid)
    : Client(config, transport, clientid),
      lastReqId(0),
      numPartitions(static_cast<uint32_t>(std::max(1, config.n))),
      bound_tracker_(DeadlineConfig::FromEnvironment(), config.n),
      clientStatsEnabled_(EnvFlagEnabled("DYNA_CLIENT_STATS")),
      clientStatsLogEvery_(std::max<uint64_t>(1, EnvUint64OrDefault("DYNA_CLIENT_STATS_LOG_EVERY", 10000)))
{
}

DynaClient::~DynaClient()
{
    if (clientStatsEnabled_) {
        MaybeLogClientStats();
    }
}

void
DynaClient::Invoke(const string &request, continuation_t continuation)
{
    ++lastReqId;
    const uint64_t requestId = lastReqId;
    const uint64_t send_us = NowMicros();
    const uint64_t deadline = ComputeDeadline(send_us);
    const uint32_t partitionId = PartitionForRequest(request);

    PendingRequest pending;
    pending.request = request;
    pending.isRead = IsReadCommand(request);
    pending.partitionId = partitionId;
    pending.requestId = requestId;
    pending.sendUs = send_us;
    pending.deadlineUs = deadline;
    pending.activeDeadlineUs = deadline;
    pending.continuation = std::move(continuation);

    pendingRequests[requestId] = std::move(pending);
    if (clientStatsEnabled_) {
        g_clientStats.notePendingSize(pendingRequests.size());
    }

    proto::RequestMessage req;
    req.set_partition_id(partitionId);
    req.set_client_id(clientid);
    req.set_request_id(requestId);
    req.set_command(request);
    req.set_send_time_us(send_us);
    req.set_deadline_us(deadline);
    req.set_is_read(IsReadCommand(request));

    if (!transport->SendMessageToAll(this, req)) {
        Warning("Failed to multicast DYNA request");
    }
}

void
DynaClient::InvokeUnlogged(int replicaIdx, const string &request, continuation_t continuation,
                           timeout_continuation_t timeoutContinuation, uint32_t timeout)
{
    (void)replicaIdx;
    (void)timeoutContinuation;
    (void)timeout;
    Invoke(request, std::move(continuation));
}

void
DynaClient::ReceiveMessage(const TransportAddress &remote, const std::string_view type,
                           const std::string_view data, void *meta_data)
{
    (void)remote;
    (void)meta_data;
    const static auto fastReplyName = proto::FastReply::default_instance().GetDescriptor()->full_name();
    const static auto readResultName = proto::ReadResult::default_instance().GetDescriptor()->full_name();

    if (type == fastReplyName) {
        ALLOCATE_MESSAGE(proto::FastReply, reply);
        reply->ParseFromArray(data.data(), data.length());
        auto iter = pendingRequests.find(reply->request_id());
        if (iter == pendingRequests.end()) {
            return;
        }
        // TODO: consider making middle ware
        
        // reply should belong to same partition
        PendingRequest &pending = iter->second;
        if (reply->partition_id() != pending.partitionId) {
            return;
        }
        if (reply->view_id() != 0) {
            return;
        }
        if (reply->has_owd_us()) {
            bound_tracker_.on_owd_sample(reply->replica_id(), reply->owd_us());
        }
        const uint32_t leaderId = PartitionLeaderFor(pending.partitionId);
        const bool fromLeader = (reply->replica_id() == leaderId);
        if (fromLeader && reply->deadline_us() != pending.activeDeadlineUs) {
            const uint64_t oldDeadline = pending.activeDeadlineUs;
            // Leader decides epoch for this request 
            // when it shifts restart quorum + collection on the new deadline.
            pending.activeDeadlineUs = reply->deadline_us();
            pending.responders.clear();
            pending.leaderAcked = false;
            pending.readResultReceived = false;
            pending.readResultData.clear();
            if (clientStatsEnabled_) {
                g_clientStats.epochResets.add();
            }
            if (SlowPathDebugEnabled()) {
                Notice("DYNA slowpath client epoch switch req=%lu part=%u old_deadline=%lu new_deadline=%lu",
                       pending.requestId,
                       pending.partitionId,
                       oldDeadline,
                       pending.activeDeadlineUs);
            }
        }
        if (reply->deadline_us() != pending.activeDeadlineUs) {
            if (clientStatsEnabled_) {
                g_clientStats.deadlineMismatchDrops.add();
            }
            return;
        }
        if (clientStatsEnabled_ && pending.firstReplyUs == 0) {
            pending.firstReplyUs = NowMicros();
        }
        pending.responders.insert(reply->replica_id());
        if (fromLeader) {
            pending.leaderAcked = true;
        }
        TryComplete(pending);
    } else if (type == readResultName) {
        ALLOCATE_MESSAGE(proto::ReadResult, result);
        result->ParseFromArray(data.data(), data.length());
        auto iter = pendingRequests.find(result->request_id());
        if (iter == pendingRequests.end()) {
            return;
        }
        PendingRequest &pending = iter->second;
        if (result->partition_id() != pending.partitionId) {
            return;
        }
        if (!pending.isRead) {
            return;
        }
        if (result->deadline_us() != pending.activeDeadlineUs) {
            if (clientStatsEnabled_) {
                g_clientStats.deadlineMismatchDrops.add();
            }
            return;
        }
        if (result->has_owd_us()) {
            bound_tracker_.on_owd_sample(PartitionLeaderFor(pending.partitionId), result->owd_us());
        }
        pending.readResultData = result->data();
        pending.readResultReceived = true;
        TryComplete(pending);
    } else {
        Client::ReceiveMessage(remote, type, data, meta_data);
    }
}

uint64_t
DynaClient::NowMicros() const
{
    return WallClockMicros();
}

uint64_t
DynaClient::ComputeDeadline(uint64_t send_time_us) const
{
    return send_time_us + bound_tracker_.bound_us();
}

bool
DynaClient::IsReadCommand(const std::string &request) const
{
    if (request.empty()) {
        return false;
    }
    const char op = request[0];
    return op == 'r' || op == 'R';
}

uint32_t
DynaClient::PartitionForRequest(const std::string &request) const
{
    if (request.size() < 25) {
        return 0;
    }
    const std::string key = request.substr(1, 24);
    return static_cast<uint32_t>(std::hash<std::string>()(key) % numPartitions);
}

uint32_t
DynaClient::PartitionLeaderFor(uint32_t partitionId) const
{
    return static_cast<uint32_t>(partitionId % static_cast<uint32_t>(config.n));
}

void
DynaClient::RecordClientCompletion(const PendingRequest &pending)
{
    const uint64_t completeUs = NowMicros();
    if (pending.sendUs > 0 && completeUs > pending.sendUs) {
        g_clientStats.totalUs.record(completeUs - pending.sendUs);
    }
    if (pending.firstReplyUs > 0 && pending.firstReplyUs > pending.sendUs) {
        g_clientStats.firstReplyUs.record(pending.firstReplyUs - pending.sendUs);
        if (completeUs > pending.firstReplyUs) {
            g_clientStats.quorumWaitUs.record(completeUs - pending.firstReplyUs);
        }
    }
    g_clientStats.completions.add();
}

void
DynaClient::MaybeLogClientStats()
{
    const uint64_t completions = g_clientStats.completions.snapshotAndReset();
    if (completions == 0) {
        return;
    }
    const uint64_t mismatch = g_clientStats.deadlineMismatchDrops.snapshotAndReset();
    const uint64_t epochs = g_clientStats.epochResets.snapshotAndReset();
    const uint64_t maxPending = g_clientStats.snapshotMaxPending();
    const auto totalSnap = g_clientStats.totalUs.snapshotAndClear();
    const auto firstSnap = g_clientStats.firstReplyUs.snapshotAndClear();
    const auto quorumSnap = g_clientStats.quorumWaitUs.snapshotAndClear();

    Notice("DYNA client stats client=%lu completions=%lu pending_max=%lu "
           "deadline_mismatch=%lu epoch_resets=%lu bound_us=%lu",
           static_cast<unsigned long>(clientid),
           static_cast<unsigned long>(completions),
           static_cast<unsigned long>(maxPending),
           static_cast<unsigned long>(mismatch),
           static_cast<unsigned long>(epochs),
           static_cast<unsigned long>(bound_tracker_.bound_us()));

    if (totalSnap.count > 0) {
        Notice("DYNA client stats client=%lu total_us samples=%lu avg=%lu p50=%lu p99=%lu max=%lu",
               static_cast<unsigned long>(clientid),
               static_cast<unsigned long>(totalSnap.count),
               static_cast<unsigned long>(totalSnap.avg_us),
               static_cast<unsigned long>(totalSnap.p50_us),
               static_cast<unsigned long>(totalSnap.p99_us),
               static_cast<unsigned long>(totalSnap.max_us));
    }
    if (firstSnap.count > 0) {
        Notice("DYNA client stats client=%lu first_reply_us samples=%lu avg=%lu p50=%lu p99=%lu max=%lu",
               static_cast<unsigned long>(clientid),
               static_cast<unsigned long>(firstSnap.count),
               static_cast<unsigned long>(firstSnap.avg_us),
               static_cast<unsigned long>(firstSnap.p50_us),
               static_cast<unsigned long>(firstSnap.p99_us),
               static_cast<unsigned long>(firstSnap.max_us));
    }
    if (quorumSnap.count > 0) {
        Notice("DYNA client stats client=%lu quorum_wait_us samples=%lu avg=%lu p50=%lu p99=%lu max=%lu",
               static_cast<unsigned long>(clientid),
               static_cast<unsigned long>(quorumSnap.count),
               static_cast<unsigned long>(quorumSnap.avg_us),
               static_cast<unsigned long>(quorumSnap.p50_us),
               static_cast<unsigned long>(quorumSnap.p99_us),
               static_cast<unsigned long>(quorumSnap.max_us));
    }
}

void
DynaClient::TryComplete(PendingRequest &pending)
{
    const size_t quorum = static_cast<size_t>(config.f + 1);
    if (!pending.leaderAcked || pending.responders.size() < quorum) {
        return;
    }

    if (!pending.isRead) {
        if (clientStatsEnabled_) {
            RecordClientCompletion(pending);
            if (g_clientStats.completions.load() % clientStatsLogEvery_ == 0) {
                MaybeLogClientStats();
            }
        }
        pending.continuation("", "");
        pendingRequests.erase(pending.requestId);
        return;
    }

    if (!pending.readResultReceived) {
        return;
    }

    if (clientStatsEnabled_) {
        RecordClientCompletion(pending);
        if (g_clientStats.completions.load() % clientStatsLogEvery_ == 0) {
            MaybeLogClientStats();
        }
    }
    pending.continuation("", pending.readResultData);
    pendingRequests.erase(pending.requestId);
}

} // namespace dyna
} // namespace specpaxos

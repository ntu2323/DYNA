// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

//MAJOR TODOS: Baseline implementations. View change + failure recovery. Multi partition coordinator. 


#include "dyna/client.h"
#include "dyna/wall_clock.h"

#include "lib/assert.h"
#include "lib/message.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>

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

DynaClient::DynaClient(const Configuration &config, Transport *transport, uint64_t clientid)
    : Client(config, transport, clientid),
      lastReqId(0),
      numPartitions(static_cast<uint32_t>(std::max(1, config.n))),
      bound_tracker_(DeadlineConfig::FromEnvironment(), config.n)
{
}

DynaClient::~DynaClient() = default;

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
    pending.deadlineUs = deadline;
    pending.activeDeadlineUs = deadline;
    pending.continuation = std::move(continuation);

    pendingRequests[requestId] = std::move(pending);

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
            if (SlowPathDebugEnabled()) {
                Notice("DYNA slowpath client epoch switch req=%lu part=%u old_deadline=%lu new_deadline=%lu",
                       pending.requestId,
                       pending.partitionId,
                       oldDeadline,
                       pending.activeDeadlineUs);
            }
        }
        if (reply->deadline_us() != pending.activeDeadlineUs) {
            return;
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
DynaClient::TryComplete(PendingRequest &pending)
{
    const size_t quorum = static_cast<size_t>(config.f + 1);
    if (!pending.leaderAcked || pending.responders.size() < quorum) {
        return;
    }

    if (!pending.isRead) {
        pending.continuation("", "");
        pendingRequests.erase(pending.requestId);
        return;
    }

    if (!pending.readResultReceived) {
        return;
    }

    pending.continuation("", pending.readResultData);
    pendingRequests.erase(pending.requestId);
}

} // namespace dyna
} // namespace specpaxos

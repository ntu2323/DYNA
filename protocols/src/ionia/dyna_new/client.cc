// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "dyna_new/client.h"

#include "dyna_new/dyna-new-proto.pb.h"
#include "dyna_new/partition_map.h"
#include "dyna_new/wall_clock.h"
#include "lib/message.h"

#include <algorithm>
#include <utility>

namespace specpaxos {
namespace dyna_new {

namespace proto = specpaxos::dyna_new::proto;

DynaNewClient::DynaNewClient(const specpaxos::Configuration &config,
                             Transport *transport,
                             uint64_t clientid)
    : Client(config, transport, clientid),
      num_partitions_(static_cast<uint32_t>(std::max(1, config.n))),
      write_quorum_(static_cast<size_t>(config.f + 1)),
      resend_ms_(DeadlineConfig::EnvU64("DYNA_NEW_RESEND_TIMEOUT_MS", 5)),
      bound_tracker_(DeadlineConfig::FromEnv(), config.n)
{
    if (resend_ms_ == 0) resend_ms_ = 1;
}

DynaNewClient::~DynaNewClient() {}

uint64_t
DynaNewClient::NowMicros() const
{
    return WallClockMicros();
}

bool
DynaNewClient::IsReadCommand(const std::string &request) const
{
    if (request.empty()) return false;
    const char op = request[0];
    return op == 'r' || op == 'R';
}

std::string
DynaNewClient::KeyOf(const std::string &request) const
{
    // [op:1][key:24][value...]
    if (request.size() < 25) return request;
    return request.substr(1, 24);
}

std::string
DynaNewClient::ValueOf(const std::string &request) const
{
    if (request.size() <= 25) return std::string();
    return request.substr(25);
}

void
DynaNewClient::Invoke(const string &request, continuation_t continuation)
{
    ++last_req_id_;
    const uint64_t request_id = last_req_id_;

    const std::string key = KeyOf(request);
    const bool is_read = IsReadCommand(request);
    const uint32_t partition_id = PartitionOf(key, num_partitions_);
    const uint32_t leader_id = LeaderOf(partition_id,
                                        static_cast<uint32_t>(config.n));

    PendingRequest pending{
        request,
        partition_id,
        request_id,
        /*send_us=*/0,
        is_read,
        std::move(continuation),
        CommitQuorum(leader_id, write_quorum_),
        /*resend_timer=*/nullptr,
    };
    auto res = pending_.emplace(request_id, std::move(pending));

    // Arm the retransmission timer (spec §5): if the request has not committed by
    // resend_ms_, resend it (fresh send_time + deadline, same request_id) so a
    // dropped request/reply on best-effort UDP recovers.
    res.first->second.resend_timer = std::make_unique<Timeout>(
        transport, resend_ms_,
        [this, request_id]() { OnResendTimeout(request_id); });
    res.first->second.resend_timer->Start();

    SendRequestProto(request_id);
}

void
DynaNewClient::SendRequestProto(uint64_t request_id)
{
    auto it = pending_.find(request_id);
    if (it == pending_.end()) {
        return;
    }
    PendingRequest &p = it->second;

    const uint64_t send_us = NowMicros();
    const uint64_t deadline = send_us + bound_tracker_.bound_us();  // §2: curr + s + d
    p.send_us = send_us;

    proto::Request req;
    req.set_client_id(clientid);
    req.set_request_id(request_id);
    req.set_type(p.is_read ? proto::READ_ONLY : proto::BLIND_WRITE);
    req.set_key(KeyOf(p.request));
    req.set_command(ValueOf(p.request));
    req.set_send_time(send_us);
    req.set_deadline(deadline);

    if (!transport->SendMessageToAll(this, req)) {
        Warning("Failed to multicast dyna_new request");
    }
}

void
DynaNewClient::OnResendTimeout(uint64_t request_id)
{
    auto it = pending_.find(request_id);
    if (it == pending_.end()) {
        return;   // already committed and erased
    }
    SendRequestProto(request_id);
    if (it->second.resend_timer) {
        it->second.resend_timer->Reset();
    }
}

void
DynaNewClient::InvokeUnlogged(int replicaIdx,
                              const string &request,
                              continuation_t continuation,
                              timeout_continuation_t timeoutContinuation,
                              uint32_t timeout)
{
    (void)replicaIdx;
    (void)timeoutContinuation;
    (void)timeout;
    Invoke(request, std::move(continuation));
}

void
DynaNewClient::ReceiveMessage(const TransportAddress &remote,
                              const std::string_view type,
                              const std::string_view data,
                              void *meta_data)
{
    const static auto replyName =
        proto::Reply::default_instance().GetDescriptor()->full_name();

    if (type != replyName) {
        Client::ReceiveMessage(remote, type, data, meta_data);
        return;
    }

    proto::Reply reply;
    if (!reply.ParseFromArray(data.data(), data.size())) {
        return;
    }

    auto it = pending_.find(reply.request_id());
    if (it == pending_.end()) {
        return;   // unknown or already-committed request
    }
    PendingRequest &p = it->second;
    if (reply.partition_id() != p.partition_id) {
        return;
    }

    // OWD feedback (ignored when 0 = insufficient samples).
    bound_tracker_.on_owd_sample(reply.replica_id(), reply.owd_estimate());

    // Writes agree on the leader's deadline; reads agree on the leader's log
    // position (§5, §6.3). Reads carry the leader's data as the result.
    const uint64_t match = p.is_read ? reply.log_pos() : reply.deadline();
    const bool has_result = reply.is_leader() && reply.has_result();
    const bool committed = p.quorum.AddReply(reply.replica_id(), reply.view_id(),
                                             match, has_result, reply.result());
    if (!committed) {
        return;
    }

    if (p.resend_timer) {
        p.resend_timer->Stop();   // committed: stop retransmitting
    }
    continuation_t cont = std::move(p.continuation);
    std::string result = p.is_read ? p.quorum.result() : std::string();
    pending_.erase(it);
    cont(std::string(), result);
}

} // namespace dyna_new
} // namespace specpaxos

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "dyna_new/client.h"

#include "dyna_new/dyna-new-proto.pb.h"
#include "dyna_new/partition_map.h"
#include "dyna_new/wall_clock.h"
#include "lib/message.h"

#include <algorithm>
#include <cstdio>
#include <string>
#include <utility>
#include <vector>

namespace specpaxos {
namespace dyna_new {

namespace proto = specpaxos::dyna_new::proto;

DynaNewClient::DynaNewClient(const specpaxos::Configuration &config,
                             Transport *transport,
                             uint64_t clientid)
    : Client(config, transport, clientid),
      num_partitions_(static_cast<uint32_t>(std::max(1, config.n))),
      write_quorum_(static_cast<size_t>(config.f + 1)),
      resend_ms_(DeadlineConfig::EnvU64("DYNA_NEW_RESEND_TIMEOUT_MS", 30)),
      bound_tracker_(DeadlineConfig::FromEnv(), config.n)
{
    if (resend_ms_ == 0) resend_ms_ = 1;
    const char *sv = std::getenv("DYNA_NEW_CLIENT_STATS");
    client_stats_enabled_ = (sv != nullptr && *sv != '\0' && *sv != '0');
    client_stats_every_ =
        DeadlineConfig::EnvU64("DYNA_NEW_CLIENT_STATS_LOG_EVERY", 10000);
    if (client_stats_every_ == 0) client_stats_every_ = 10000;
    // Log the deadline config actually read from env, so bound-cap experiments are
    // verifiable (the bound = min(max_owd, max(initial, measured_owd))).
    DeadlineConfig dc = DeadlineConfig::FromEnv();
    Notice("DYNA_NEW client config: initial_bound_us=%lu max_owd_us=%lu owd_pct=%u",
           (unsigned long)dc.initial_bound_us, (unsigned long)dc.max_owd_us,
           dc.owd_percentile_percent);

    // One periodic retransmission sweep for ALL pending requests (spec §5), instead
    // of arming/disarming a libevent timer per request. Sweeps at half the resend
    // interval so an aged request is resent within ~resend_ms_.
    uint64_t sweep_ms = resend_ms_ / 2;
    if (sweep_ms == 0) sweep_ms = 1;
    resend_sweep_timer_ = std::make_unique<Timeout>(
        transport, sweep_ms, [this]() { ResendSweep(); });
    resend_sweep_timer_->Start();
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
        /*first_submit_us=*/NowMicros(),
        /*first_reply_us=*/0,
        /*resends=*/0,
        is_read,
        std::move(continuation),
        CommitQuorum(leader_id, write_quorum_),
    };
    pending_.emplace(request_id, std::move(pending));

    // Retransmission is handled by the periodic ResendSweep (armed in the ctor),
    // so there is no per-request timer to arm here.
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
    if (client_stats_enabled_) {
        stat_send_issue_sum_us_ += NowMicros() - send_us;   // stamp → sent (Step 1)
        stat_send_issue_count_++;
    }
}

void
DynaNewClient::ResendSweep()
{
    // Re-send any pending request whose last send is older than resend_ms_ (spec §5).
    // On a healthy network nothing is aged (resends≈0), so this is a cheap scan of a
    // near-empty map. send_us is µs; resend_ms_ is ms.
    const uint64_t now = NowMicros();
    const uint64_t age_us = resend_ms_ * 1000;
    std::vector<uint64_t> aged;
    for (auto &kv : pending_) {
        if (now > kv.second.send_us && now - kv.second.send_us >= age_us) {
            aged.push_back(kv.first);
        }
    }
    for (uint64_t id : aged) {
        auto it = pending_.find(id);
        if (it != pending_.end()) {
            ++it->second.resends;
            SendRequestProto(id);
        }
    }
    if (resend_sweep_timer_) {
        resend_sweep_timer_->Reset();   // re-arm
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
    if (p.first_reply_us == 0) {
        p.first_reply_us = NowMicros();   // phase split: submit→first reply
        // Effective deadline hold the leader applied (leader deadline − client send).
        if (client_stats_enabled_ && reply.deadline() > p.send_us) {
            stat_eff_bound_us_.push_back(reply.deadline() - p.send_us);
        }
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

    // Committed: erasing from pending_ (below) drops it from the resend sweep — no
    // per-request timer to stop.
    if (client_stats_enabled_) {
        const uint64_t now = NowMicros();
        const uint64_t to_first = (p.first_reply_us > p.first_submit_us)
                                      ? p.first_reply_us - p.first_submit_us : 0;
        RecordCommitStat(now - p.first_submit_us, to_first, p.resends);
    }
    continuation_t cont = std::move(p.continuation);
    std::string result = p.is_read ? p.quorum.result() : std::string();
    pending_.erase(it);
    cont(std::string(), result);
}

void
DynaNewClient::RecordCommitStat(uint64_t total_us, uint64_t to_first_reply_us,
                                uint32_t resends)
{
    stat_latencies_us_.push_back(total_us);
    stat_to_first_us_.push_back(to_first_reply_us);
    stat_resends_.push_back(resends);
    if (stat_latencies_us_.size() < client_stats_every_) {
        return;
    }

    std::vector<uint64_t> lat = stat_latencies_us_;
    std::sort(lat.begin(), lat.end());
    std::vector<uint64_t> tf = stat_to_first_us_;
    std::sort(tf.begin(), tf.end());
    auto pct = [](const std::vector<uint64_t> &v, double p) {
        return v[static_cast<size_t>(p * (v.size() - 1))];
    };
    std::vector<uint64_t> eb = stat_eff_bound_us_;
    std::sort(eb.begin(), eb.end());
    const unsigned long effBoundP50 = eb.empty() ? 0 : eb[eb.size() / 2];
    uint64_t resend_sum = 0, max_resends = 0;
    for (uint32_t r : stat_resends_) {
        resend_sum += r;
        if (r > max_resends) max_resends = r;
    }
    // Phase split: to_first p50 ≈ hold + 1 one-way; (total-to_first) ≈ quorum/
    // correction wait. bound_us is the deadline hold. p50 ≫ target => hold floor.
    const uint64_t send_issue_avg =
        stat_send_issue_count_ ? stat_send_issue_sum_us_ / stat_send_issue_count_ : 0;
    Notice("DYNA_NEW client stats [n=%zu]: total_us p50=%lu p90=%lu p99=%lu max=%lu; "
           "to_first_us p50=%lu p90=%lu; quorum_wait_us p50=%lu; send_issue_us avg=%lu; "
           "effBound_us p50=%lu; resends avg=%.2f max=%lu; bound_us=%lu pending=%zu",
           lat.size(), (unsigned long)pct(lat, 0.5), (unsigned long)pct(lat, 0.9),
           (unsigned long)pct(lat, 0.99), (unsigned long)lat.back(),
           (unsigned long)pct(tf, 0.5), (unsigned long)pct(tf, 0.9),
           (unsigned long)(pct(lat, 0.5) > pct(tf, 0.5) ? pct(lat, 0.5) - pct(tf, 0.5) : 0),
           (unsigned long)send_issue_avg, effBoundP50,
           static_cast<double>(resend_sum) / lat.size(), (unsigned long)max_resends,
           (unsigned long)bound_tracker_.bound_us(), pending_.size());

    stat_latencies_us_.clear();
    stat_to_first_us_.clear();
    stat_eff_bound_us_.clear();
    stat_resends_.clear();
    stat_send_issue_sum_us_ = 0;
    stat_send_issue_count_ = 0;
}

} // namespace dyna_new
} // namespace specpaxos

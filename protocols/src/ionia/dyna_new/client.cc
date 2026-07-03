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
    stat_fwd_by_replica_.resize(std::max(1, config.n));
    const char *sv = std::getenv("DYNA_NEW_CLIENT_STATS");
    client_stats_enabled_ = (sv != nullptr && *sv != '\0' && *sv != '0');
    client_stats_every_ =
        DeadlineConfig::EnvU64("DYNA_NEW_CLIENT_STATS_LOG_EVERY", 10000);
    if (client_stats_every_ == 0) client_stats_every_ = 10000;
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
        /*fwd_us=*/0,
        /*rev_us=*/0,
        /*resends=*/0,
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
    if (client_stats_enabled_) {
        stat_send_issue_sum_us_ += NowMicros() - send_us;   // stamp → sent (Step 1)
        stat_send_issue_count_++;
    }
}

void
DynaNewClient::OnResendTimeout(uint64_t request_id)
{
    auto it = pending_.find(request_id);
    if (it == pending_.end()) {
        return;   // already committed and erased
    }
    ++it->second.resends;
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
    if (p.first_reply_us == 0) {
        p.first_reply_us = NowMicros();   // phase split: submit→first reply
        // PTP-comparable split of to_first into forward (send→replica release) and
        // reverse (release→client recv) legs, using the replica's release stamp.
        const uint64_t rel = reply.release_time();
        p.fwd_us = (rel > p.send_us) ? rel - p.send_us : 0;
        p.rev_us = (rel != 0 && p.first_reply_us > rel) ? p.first_reply_us - rel : 0;
        // NTP 4-timestamp: T1=send, T2=replica recv, T3=replica release, T4=recv.
        // θ = ((T2−T1)+(T3−T4))/2 (replica clock rel. to client), δ = (T4−T1)−(T3−T2).
        if (client_stats_enabled_ && reply.recv_time() != 0 && rel != 0) {
            const int64_t t1 = static_cast<int64_t>(p.send_us);
            const int64_t t2 = static_cast<int64_t>(reply.recv_time());
            const int64_t t3 = static_cast<int64_t>(rel);
            const int64_t t4 = static_cast<int64_t>(p.first_reply_us);
            stat_clock_offset_us_.push_back(((t2 - t1) + (t3 - t4)) / 2);
            stat_net_delay_us_.push_back((t4 - t1) - (t3 - t2));
        }
        if (client_stats_enabled_ && reply.deadline() > p.send_us) {
            stat_eff_bound_us_.push_back(reply.deadline() - p.send_us);
        }
    }
    // Per-replica forward leg for EVERY reply (not just the first): is DYNA fast
    // everywhere, or is one replica the straggler that gates the deadline/commit?
    if (client_stats_enabled_) {
        const uint64_t rel = reply.release_time();
        const uint32_t rid = reply.replica_id();
        if (rel > p.send_us && rid < stat_fwd_by_replica_.size() &&
            stat_fwd_by_replica_[rid].size() < 65536) {
            stat_fwd_by_replica_[rid].push_back(rel - p.send_us);
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

    if (p.resend_timer) {
        p.resend_timer->Stop();   // committed: stop retransmitting
    }
    if (client_stats_enabled_) {
        const uint64_t now = NowMicros();
        const uint64_t to_first = (p.first_reply_us > p.first_submit_us)
                                      ? p.first_reply_us - p.first_submit_us : 0;
        RecordCommitStat(now - p.first_submit_us, to_first, p.fwd_us, p.rev_us,
                         p.resends);
    }
    continuation_t cont = std::move(p.continuation);
    std::string result = p.is_read ? p.quorum.result() : std::string();
    pending_.erase(it);
    cont(std::string(), result);
}

void
DynaNewClient::RecordCommitStat(uint64_t total_us, uint64_t to_first_reply_us,
                                uint64_t fwd_us, uint64_t rev_us, uint32_t resends)
{
    stat_latencies_us_.push_back(total_us);
    stat_to_first_us_.push_back(to_first_reply_us);
    stat_fwd_us_.push_back(fwd_us);
    stat_rev_us_.push_back(rev_us);
    stat_resends_.push_back(resends);
    if (stat_latencies_us_.size() < client_stats_every_) {
        return;
    }

    std::vector<uint64_t> lat = stat_latencies_us_;
    std::sort(lat.begin(), lat.end());
    std::vector<uint64_t> tf = stat_to_first_us_;
    std::sort(tf.begin(), tf.end());
    std::vector<uint64_t> fwd = stat_fwd_us_;
    std::sort(fwd.begin(), fwd.end());
    std::vector<uint64_t> rev = stat_rev_us_;
    std::sort(rev.begin(), rev.end());
    auto pct = [](const std::vector<uint64_t> &v, double p) {
        return v[static_cast<size_t>(p * (v.size() - 1))];
    };
    // Signed median helper for the NTP θ/δ diagnostics (can be negative).
    auto medi = [](std::vector<int64_t> v) -> long {
        if (v.empty()) return 0;
        std::sort(v.begin(), v.end());
        return static_cast<long>(v[v.size() / 2]);
    };
    const long clkOffP50 = medi(stat_clock_offset_us_);
    const long netDelP50 = medi(stat_net_delay_us_);
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
    // Per-replica forward-leg p50 + the client's current per-replica OWD estimate.
    // "fast everywhere?" — a straggler replica shows a high fwd/owd here and, since
    // bound = max OWD, inflates every request's deadline.
    std::string perRep = "perReplica[";
    const std::vector<uint64_t> &owd = bound_tracker_.per_replica_owd();
    for (size_t r = 0; r < stat_fwd_by_replica_.size(); ++r) {
        std::vector<uint64_t> fr = stat_fwd_by_replica_[r];
        uint64_t p50 = 0;
        if (!fr.empty()) { std::sort(fr.begin(), fr.end()); p50 = fr[fr.size() / 2]; }
        char buf[64];
        snprintf(buf, sizeof(buf), "%sr%zu:fwd=%lu,owd=%lu", r ? " " : "", r,
                 (unsigned long)p50, (unsigned long)(r < owd.size() ? owd[r] : 0));
        perRep += buf;
    }
    perRep += "]";
    Notice("DYNA_NEW client stats [n=%zu]: total_us p50=%lu p90=%lu p99=%lu max=%lu; "
           "to_first_us p50=%lu p90=%lu; fwd_us p50=%lu p90=%lu; rev_us p50=%lu p90=%lu; "
           "quorum_wait_us p50=%lu; send_issue_us avg=%lu; "
           "clockOffset_us p50=%ld; netDelay_us p50=%ld; effBound_us p50=%lu; "
           "resends avg=%.2f max=%lu; bound_us=%lu %s pending=%zu",
           lat.size(), (unsigned long)pct(lat, 0.5), (unsigned long)pct(lat, 0.9),
           (unsigned long)pct(lat, 0.99), (unsigned long)lat.back(),
           (unsigned long)pct(tf, 0.5), (unsigned long)pct(tf, 0.9),
           (unsigned long)pct(fwd, 0.5), (unsigned long)pct(fwd, 0.9),
           (unsigned long)pct(rev, 0.5), (unsigned long)pct(rev, 0.9),
           (unsigned long)(pct(lat, 0.5) > pct(tf, 0.5) ? pct(lat, 0.5) - pct(tf, 0.5) : 0),
           (unsigned long)send_issue_avg,
           clkOffP50, netDelP50, effBoundP50,
           static_cast<double>(resend_sum) / lat.size(), (unsigned long)max_resends,
           (unsigned long)bound_tracker_.bound_us(), perRep.c_str(), pending_.size());

    stat_latencies_us_.clear();
    stat_to_first_us_.clear();
    stat_fwd_us_.clear();
    stat_rev_us_.clear();
    for (auto &v : stat_fwd_by_replica_) v.clear();
    stat_clock_offset_us_.clear();
    stat_net_delay_us_.clear();
    stat_eff_bound_us_.clear();
    stat_resends_.clear();
    stat_send_issue_sum_us_ = 0;
    stat_send_issue_count_ = 0;
}

} // namespace dyna_new
} // namespace specpaxos

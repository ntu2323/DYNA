// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/client.cc

#include "dyna/client.h"

#include "dyna/stats.h"
#include "dyna/wall_clock.h"
#include "lib/message.h"

#include <algorithm>
#include <atomic>

#include <string>
#include <utility>
#include <vector>

namespace specpaxos {
namespace dyna {

namespace proto = specpaxos::dyna::proto;

namespace {
// Resend backoff cap: resend_ms_ * 2^5 (30 ms -> ~1 s).
constexpr uint32_t kMaxResendShift = 5;
} // namespace

namespace {
// DYNA_STATS=1: process-wide client counters (the bench runs one DynaClient per thread);
// whichever client's sweep first crosses each 1 s boundary prints the line.
struct ClientStats {
    StatCounter fast, slow_disagree, slow_timeout, commit_via_reply;
    StatCounter resend_write, retry_read;
    StatDist    lat_fast_us, lat_slow_us;
    std::atomic<uint64_t> last_dump_us{0};
};
ClientStats g_stats;
const bool g_stats_on = StatsEnabledFromEnv();

void MaybeDumpClientStats(uint64_t now)
{
    uint64_t last = g_stats.last_dump_us.load(std::memory_order_relaxed);
    if (last == 0) {
        g_stats.last_dump_us.compare_exchange_strong(last, now);
        return;
    }
    if (now < last + 1000000 ||
        !g_stats.last_dump_us.compare_exchange_strong(last, now)) {
        return;
    }
    auto dist = [](StatDist &d) {
        const uint64_t n = d.n.Take(), s = d.sum.Take(), m = d.max.Take();
        return std::make_pair(n ? s / n : 0, m);
    };
    const auto lf = dist(g_stats.lat_fast_us);
    const auto ls = dist(g_stats.lat_slow_us);
    Notice("client stats: fast=%lu slow[disagree=%lu timeout=%lu] viaCommitReply=%lu "
           "resendW=%lu retryR=%lu latFast(avg/max)=%lu/%luus latSlow=%lu/%luus",
           (unsigned long)g_stats.fast.Take(), (unsigned long)g_stats.slow_disagree.Take(),
           (unsigned long)g_stats.slow_timeout.Take(),
           (unsigned long)g_stats.commit_via_reply.Take(),
           (unsigned long)g_stats.resend_write.Take(), (unsigned long)g_stats.retry_read.Take(),
           (unsigned long)lf.first, (unsigned long)lf.second, (unsigned long)ls.first,
           (unsigned long)ls.second);
}
} // namespace

DynaClient::DynaClient(const specpaxos::Configuration &config,
                                 Transport *transport,
                                 uint64_t clientid)
    : Client(config, transport, clientid),
      topo_(static_cast<uint32_t>(std::max(1, config.n))),
      write_quorum_(static_cast<size_t>(config.f + 1)),
      resend_ms_(DeadlineConfig::EnvU64("DYNA_RESEND_TIMEOUT_MS", 30)),
      slow_timeout_us_(DeadlineConfig::EnvU64("DYNA_SLOW_TIMEOUT_US", 2000)),
      bound_tracker_(DeadlineConfig::FromEnv(), config.n)
{
    if (resend_ms_ == 0) resend_ms_ = 1;
    fast_batch_max_ = std::max<uint64_t>(1, DeadlineConfig::EnvU64("DYNA_FAST_BATCH", 32));
    fast_flush_us_ = std::max<uint64_t>(1, DeadlineConfig::EnvU64("DYNA_FAST_FLUSH_US", 200));
    const char *dbg = std::getenv("DYNA_DEBUG");
    debug_ = (dbg != nullptr && *dbg != '\0' && *dbg != '0');
    // Sweep often enough to honor both the resend and the slow-path timeouts.
    uint64_t sweep_ms = std::min<uint64_t>(resend_ms_ / 2, slow_timeout_us_ / 2000);
    if (sweep_ms == 0) sweep_ms = 1;
    resend_sweep_timer_ = std::make_unique<Timeout>(
        transport, sweep_ms, [this]() { ResendSweep(); });
    resend_sweep_timer_->Start();
}

DynaClient::~DynaClient() {}

uint64_t DynaClient::NowMicros() const { return WallClockMicros(); }

bool DynaClient::IsReadCommand(const std::string &request) const
{
    if (request.empty()) return false;
    const char op = request[0];
    return op == 'r' || op == 'R';
}

std::string DynaClient::KeyOf(const std::string &request) const
{
    if (request.size() < 25) return request;   // [op:1][key:24][value...]
    return request.substr(1, 24);
}

std::string DynaClient::ValueOf(const std::string &request) const
{
    if (request.size() <= 25) return std::string();
    return request.substr(25);
}

void DynaClient::Invoke(const string &request, continuation_t continuation)
{
    ++last_req_id_;
    const uint64_t request_id = last_req_id_;

    const std::string key = KeyOf(request);
    const bool is_read = IsReadCommand(request);
    const std::string value = ValueOf(request);

    PendingRequest p;
    p.request = request;
    p.request_id = request_id;
    p.type = is_read ? proto::READ : proto::WRITE;
    p.key = key;
    p.value = value;
    p.phase = Phase::COLLECTING;
    p.owd = bound_tracker_.bound_us();   // READ deadline seed (grows on retry)
    p.invoke_us = NowMicros();
    p.quorum = TxnQuorum(LeadersFor(p), write_quorum_, viewId_);
    p.continuation = std::move(continuation);
    pending_.emplace(request_id, std::move(p));

    SendRequestProto(request_id);
}

void DynaClient::SendRequestProto(uint64_t request_id)
{
    auto it = pending_.find(request_id);
    if (it == pending_.end()) return;
    PendingRequest &p = it->second;

    const uint64_t now = NowMicros();
    if (p.type == proto::READ) {
        // Reads retry with a LARGER timestamp (spec § read path): a fresh, growing
        // deadline each send so a retry can arrive on-time at more replicas.
        if (p.owd == 0) p.owd = bound_tracker_.bound_us();
        p.currentT = now + p.owd;
    } else {
        // Writes retry with the SAME timestamp: fix currentT on the first send.
        if (p.currentT == 0) p.currentT = now + bound_tracker_.bound_us();
    }
    p.send_us = now;

    proto::ClientRequest req;
    proto::Txn *txn = req.mutable_txn();
    txn->set_client_id(clientid);
    txn->set_request_id(request_id);
    txn->set_type(p.type);
    if (p.type == proto::READ) txn->add_r_set(p.key);
    else { proto::KV *kv = txn->add_w_set(); kv->set_key(p.key); kv->set_value(p.value); }
    txn->set_t(p.currentT);
    txn->set_view_id(viewId_);
    txn->set_send_time(now);

    if (!transport->SendMessageToAll(this, req)) {
        Warning("dyna: failed to multicast ClientRequest");
    }
}

std::set<uint32_t> DynaClient::LeadersFor(const PendingRequest &p) const
{
    // Involved leaders of the (single-key) transaction. Multi-key txns would simply
    // produce a larger involved-leader set — the quorum logic is identical.
    proto::Txn probe;
    probe.set_client_id(clientid);
    probe.set_request_id(p.request_id);
    probe.set_type(p.type);
    if (p.type == proto::READ) probe.add_r_set(p.key);
    else { proto::KV *kv = probe.add_w_set(); kv->set_key(p.key); kv->set_value(p.value); }
    probe.set_t(0);
    probe.set_view_id(viewId_);
    return topo_.involvedLeaders(probe);
}

void DynaClient::HandleViewInfo(const proto::ViewInfo &m)
{
    if (m.view_id() <= viewId_) return;
    viewId_ = m.view_id();
    topo_ = topo_.WithAlive(m.alive());
    if (debug_) {
        Notice("client view %lu alive=%#lx: restarting %zu pending", (unsigned long)viewId_,
               (unsigned long)m.alive(), pending_.size());
    }
    std::vector<uint64_t> ids;
    for (const auto &kv : pending_) ids.push_back(kv.first);
    for (uint64_t id : ids) {
        auto it = pending_.find(id);
        if (it == pending_.end()) continue;
        PendingRequest &p = it->second;
        p.phase = Phase::COLLECTING;
        p.resends = 0;   // new view: resend promptly again
        p.quorum = TxnQuorum(LeadersFor(p), write_quorum_, viewId_);
        if (p.type == proto::READ) {
            RetryRead(id);
        } else {
            p.results.clear();
            SendRequestProto(id);   // same t, new view
        }
    }
}

uint64_t DynaClient::SlowTimeoutUs() const
{
    // Leave room for the followers' replies (~ hold + return one-way delay) before
    // deciding they're late; the env value is a floor for low-latency networks.
    return std::max<uint64_t>(slow_timeout_us_, 3 * bound_tracker_.bound_us());
}

void DynaClient::SendSlowCommit(PendingRequest &p)
{
    proto::SlowCommit sc;
    sc.set_client_id(clientid);
    sc.set_request_id(p.request_id);
    sc.set_view_id(viewId_);
    sc.set_agreed_t(p.quorum.slow_agreed_t());
    transport->SendMessageToAll(this, sc);
    if (g_stats_on && p.slow_cause == 0) {
        p.slow_cause = p.quorum.leaders_disagree() ? 1 : 2;
        (p.slow_cause == 1 ? g_stats.slow_disagree : g_stats.slow_timeout).Add();
    }
    p.phase = Phase::SLOW_WAIT;
    p.send_us = NowMicros();
}

void DynaClient::ResendSweep()
{
    const uint64_t now = NowMicros();
    const uint64_t resend_us = resend_ms_ * 1000;
    const uint64_t slow_us = SlowTimeoutUs();
    std::vector<uint64_t> slow, resend;
    for (auto &kv : pending_) {
        const PendingRequest &p = kv.second;
        const uint64_t age = (now > p.send_us) ? now - p.send_us : 0;
        if (p.type == proto::WRITE && p.phase == Phase::COLLECTING &&
            p.quorum.all_leaders_present() && !p.quorum.fast_ready() && age >= slow_us) {
            // Every leader replied but not enough matching replies arrived (followers
            // late or replies lost) → slow path (design doc: "could not get sufficient
            // replica responses"). Late followers adopt from their lateBuffer and
            // answer with CommitReply.
            slow.push_back(kv.first);
        } else if (age >= (resend_us << std::min<uint32_t>(p.resends, kMaxResendShift))) {
            // Exponential backoff per request: a stalled cluster (view change, overload)
            // otherwise gets every pending request re-sent every resend_ms_, a flood
            // that grows with the backlog and starved recovery of its own messages.
            resend.push_back(kv.first);
        }
    }
    for (uint64_t id : slow) {
        auto it = pending_.find(id);
        if (it != pending_.end()) SendSlowCommit(it->second);
    }
    for (uint64_t id : resend) {
        auto it = pending_.find(id);
        if (it == pending_.end()) continue;
        if (g_stats_on) {
            (it->second.type == proto::READ ? g_stats.retry_read : g_stats.resend_write).Add();
        }
        it->second.resends++;
        if (it->second.phase == Phase::SLOW_WAIT) {
            SendSlowCommit(it->second);   // re-drive the slow path
        } else if (it->second.type == proto::READ) {
            RetryRead(id);                // new attempt at a larger t
        } else {
            SendRequestProto(id);         // write: same t (leader reply missing)
        }
    }
    if (g_stats_on) MaybeDumpClientStats(now);
    if (resend_sweep_timer_) resend_sweep_timer_->Reset();
}

uint64_t DynaClient::RetryRead(uint64_t request_id)
{
    auto it = pending_.find(request_id);
    if (it == pending_.end()) return 0;
    PendingRequest p = std::move(it->second);
    pending_.erase(it);

    const uint64_t new_id = ++last_req_id_;
    GrowReadOwd(p);
    p.request_id = new_id;
    p.quorum = TxnQuorum(LeadersFor(p), write_quorum_, viewId_);
    p.results.clear();
    pending_.emplace(new_id, std::move(p));
    SendRequestProto(new_id);
    return new_id;
}

void DynaClient::Commit(std::unordered_map<uint64_t, PendingRequest>::iterator it,
                            const std::string &result)
{
    if (g_stats_on) {
        const PendingRequest &p = it->second;
        const uint64_t lat = NowMicros() - p.invoke_us;
        if (p.slow_cause == 0) {
            g_stats.fast.Add();
            g_stats.lat_fast_us.Observe(lat);
        } else {
            g_stats.lat_slow_us.Observe(lat);
        }
    }
    continuation_t cont = std::move(it->second.continuation);
    pending_.erase(it);
    cont(std::string(), result);
}

void DynaClient::GrowReadOwd(PendingRequest &p)
{
    const uint64_t base = p.owd ? p.owd : bound_tracker_.bound_us();
    p.owd = std::min<uint64_t>(base * 2, 10000000);   // double, cap 10s
}

void DynaClient::InvokeUnlogged(int, const string &request,
                                     continuation_t continuation,
                                     timeout_continuation_t, uint32_t)
{
    Invoke(request, std::move(continuation));
}

void DynaClient::ReceiveMessage(const TransportAddress &remote,
                                     const std::string_view type,
                                     const std::string_view data,
                                     void *meta_data)
{
    const static auto specName =
        proto::SpecReply::default_instance().GetDescriptor()->full_name();
    const static auto commitReplyName =
        proto::CommitReply::default_instance().GetDescriptor()->full_name();
    const static auto viewInfoName =
        proto::ViewInfo::default_instance().GetDescriptor()->full_name();
    const static auto specBatchName =
        proto::SpecReplyBatch::default_instance().GetDescriptor()->full_name();

    if (type == specBatchName) {
        proto::SpecReplyBatch b;
        if (!b.ParseFromArray(data.data(), data.size())) return;
        for (const auto &r : b.replies()) HandleSpecReply(r);
        return;
    }

    if (type == viewInfoName) {
        proto::ViewInfo m;
        if (m.ParseFromArray(data.data(), data.size())) HandleViewInfo(m);
        return;
    }

    if (type == specName) {
        proto::SpecReply r;
        if (r.ParseFromArray(data.data(), data.size())) HandleSpecReply(r);
        return;
    }

    if (type == commitReplyName) {
        proto::CommitReply r;
        if (!r.ParseFromArray(data.data(), data.size())) return;
        auto it = pending_.find(r.request_id());
        if (it == pending_.end()) return;
        PendingRequest &p = it->second;
        if (!p.quorum.AddReply(r.server_id(), r.view_id(), r.t(), r.is_leader())) return;
        if (p.quorum.fast_ready()) {   // write quorum re-formed
            if (g_stats_on) g_stats.commit_via_reply.Add();
            Commit(it, std::string());
        }
        return;
    }

    Client::ReceiveMessage(remote, type, data, meta_data);
}

void DynaClient::HandleSpecReply(const proto::SpecReply &r)
{
    auto it = pending_.find(r.request_id());
    if (it == pending_.end()) return;
    PendingRequest &p = it->second;

    bound_tracker_.on_owd_sample(r.server_id(), r.owd_estimate());
    // Another view's reply doesn't count (see TxnQuorum); a server in a newer view
    // answers our next resend with ViewInfo, and we restart the request there.
    if (!p.quorum.AddReply(r.server_id(), r.view_id(), r.t(), r.is_leader())) return;
    // READ: merge the disk-read values from each involved leader's reply.
    if (p.type == proto::READ && r.is_leader()) {
        for (const auto &kv : r.results()) p.results[kv.key()] = kv.value();
    }
    if (debug_) {
        Notice("client RECV SpecReply s=%u r=%lu t=%lu leader=%d replies=%zu "
               "leaders_present=%d fast=%d disagree=%d", r.server_id(),
               (unsigned long)r.request_id(), (unsigned long)r.t(), r.is_leader() ? 1 : 0,
               p.quorum.reply_count(), p.quorum.all_leaders_present() ? 1 : 0,
               p.quorum.fast_ready() ? 1 : 0, p.quorum.leaders_disagree() ? 1 : 0);
    }

    if (p.phase == Phase::COLLECTING) {
        if (p.quorum.fast_ready()) {
            if (p.type == proto::WRITE) {
                // Writes: durability handshake so servers learn of the commit.
                proto::FastCommit fc;
                fc.set_client_id(clientid);
                fc.set_request_id(p.request_id);
                fc.set_view_id(viewId_);
                fc.set_agreed_t(p.quorum.agreed_t());
                SendFastCommit(fc);
                Commit(it, std::string());
            } else {
                // Reads aren't logged/committed on servers: just return the value
                // read from the involved leaders (no FastCommit).
                auto rit = p.results.find(p.key);
                Commit(it, rit != p.results.end() ? rit->second : std::string());
            }
        } else if (p.type == proto::WRITE && p.quorum.leaders_disagree()) {
            SendSlowCommit(p);
        }
        // Otherwise wait for more replies; the sweep forces the slow path (writes)
        // or retries at a larger t (reads) on timeout.
    } else if (p.quorum.fast_ready()) {
        // SLOW_WAIT: a straggling SpecReply already at agreedT completes the quorum
        // just like a CommitReply would.
        Commit(it, std::string());
    }
}

void DynaClient::SendFastCommit(const proto::FastCommit &fc)
{
    if (fast_batch_max_ <= 1) {
        transport->SendMessageToAll(this, fc);
        return;
    }
    *fast_pending_.add_commits() = fc;
    if (static_cast<size_t>(fast_pending_.commits_size()) >= fast_batch_max_) {
        FlushFastCommits();
    } else if (!fast_timer_armed_) {
        fast_timer_armed_ = true;
        transport->TimerUS(fast_flush_us_, [this]() {
            fast_timer_armed_ = false;
            FlushFastCommits();
        });
    }
}

void DynaClient::FlushFastCommits()
{
    if (fast_pending_.commits_size() == 0) return;
    if (fast_pending_.commits_size() == 1) {
        transport->SendMessageToAll(this, fast_pending_.commits(0));
    } else {
        transport->SendMessageToAll(this, fast_pending_);
    }
    fast_pending_.Clear();
}

} // namespace dyna
} // namespace specpaxos

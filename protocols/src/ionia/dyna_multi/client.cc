// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_multi/client.cc  (Stage 3: write-path commit handshake)

#include "dyna_multi/client.h"

#include "dyna_multi/wall_clock.h"
#include "lib/message.h"

#include <string>
#include <utility>
#include <vector>

namespace specpaxos {
namespace dyna_multi {

namespace proto = specpaxos::dyna_multi::proto;

DynaMultiClient::DynaMultiClient(const specpaxos::Configuration &config,
                                 Transport *transport,
                                 uint64_t clientid)
    : Client(config, transport, clientid),
      topo_(static_cast<uint32_t>(std::max(1, config.n))),
      write_quorum_(static_cast<size_t>(config.f + 1)),
      resend_ms_(DeadlineConfig::EnvU64("DYNA_MULTI_RESEND_TIMEOUT_MS", 30)),
      bound_tracker_(DeadlineConfig::FromEnv(), config.n)
{
    if (resend_ms_ == 0) resend_ms_ = 1;
    const char *dbg = std::getenv("DYNA_MULTI_DEBUG");
    debug_ = (dbg != nullptr && *dbg != '\0' && *dbg != '0');
    uint64_t sweep_ms = resend_ms_ / 2;
    if (sweep_ms == 0) sweep_ms = 1;
    resend_sweep_timer_ = std::make_unique<Timeout>(
        transport, sweep_ms, [this]() { ResendSweep(); });
    resend_sweep_timer_->Start();
}

DynaMultiClient::~DynaMultiClient() {}

uint64_t DynaMultiClient::NowMicros() const { return WallClockMicros(); }

bool DynaMultiClient::IsReadCommand(const std::string &request) const
{
    if (request.empty()) return false;
    const char op = request[0];
    return op == 'r' || op == 'R';
}

std::string DynaMultiClient::KeyOf(const std::string &request) const
{
    if (request.size() < 25) return request;   // [op:1][key:24][value...]
    return request.substr(1, 24);
}

std::string DynaMultiClient::ValueOf(const std::string &request) const
{
    if (request.size() <= 25) return std::string();
    return request.substr(25);
}

void DynaMultiClient::Invoke(const string &request, continuation_t continuation)
{
    ++last_req_id_;
    const uint64_t request_id = last_req_id_;

    const std::string key = KeyOf(request);
    const bool is_read = IsReadCommand(request);
    const std::string value = ValueOf(request);

    // Compute involved leaders from the (single-key) transaction. Multi-key txns would
    // simply produce a larger involved-leader set — the quorum logic is identical.
    proto::Txn probe;
    probe.set_client_id(clientid);
    probe.set_request_id(request_id);
    probe.set_type(is_read ? proto::READ : proto::WRITE);
    if (is_read) probe.add_r_set(key);
    else { proto::KV *kv = probe.add_w_set(); kv->set_key(key); kv->set_value(value); }
    probe.set_t(0);
    probe.set_view_id(viewId_);
    std::set<uint32_t> leaders = topo_.involvedLeaders(probe);

    PendingRequest p;
    p.request = request;
    p.request_id = request_id;
    p.type = is_read ? proto::READ : proto::WRITE;
    p.key = key;
    p.value = value;
    p.phase = Phase::COLLECTING;
    p.owd = bound_tracker_.bound_us();   // READ deadline seed (grows on retry)
    p.quorum = TxnQuorum(std::move(leaders), write_quorum_);
    p.continuation = std::move(continuation);
    pending_.emplace(request_id, std::move(p));

    SendRequestProto(request_id);
}

void DynaMultiClient::SendRequestProto(uint64_t request_id)
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
        Warning("dyna_multi: failed to multicast ClientRequest");
    }
}

void DynaMultiClient::ResendSweep()
{
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
        if (it == pending_.end()) continue;
        if (it->second.phase == Phase::SLOW_WAIT) {
            // Re-drive the slow path: re-broadcast SlowCommit(agreedT).
            proto::SlowCommit sc;
            sc.set_client_id(clientid);
            sc.set_request_id(id);
            sc.set_view_id(viewId_);
            sc.set_agreed_t(it->second.quorum.slow_agreed_t());
            transport->SendMessageToAll(this, sc);
            it->second.send_us = NowMicros();
        } else {
            if (it->second.type == proto::READ) GrowReadOwd(it->second);   // larger t
            SendRequestProto(id);
        }
    }
    if (resend_sweep_timer_) resend_sweep_timer_->Reset();
}

void DynaMultiClient::Commit(std::unordered_map<uint64_t, PendingRequest>::iterator it,
                            const std::string &result)
{
    continuation_t cont = std::move(it->second.continuation);
    pending_.erase(it);
    cont(std::string(), result);
}

void DynaMultiClient::GrowReadOwd(PendingRequest &p)
{
    const uint64_t base = p.owd ? p.owd : bound_tracker_.bound_us();
    p.owd = std::min<uint64_t>(base * 2, 10000000);   // double, cap 10s
}

void DynaMultiClient::InvokeUnlogged(int, const string &request,
                                     continuation_t continuation,
                                     timeout_continuation_t, uint32_t)
{
    Invoke(request, std::move(continuation));
}

void DynaMultiClient::ReceiveMessage(const TransportAddress &remote,
                                     const std::string_view type,
                                     const std::string_view data,
                                     void *meta_data)
{
    const static auto specName =
        proto::SpecReply::default_instance().GetDescriptor()->full_name();
    const static auto commitReplyName =
        proto::CommitReply::default_instance().GetDescriptor()->full_name();
    const static auto rejectName =
        proto::RejectReply::default_instance().GetDescriptor()->full_name();

    if (type == specName) {
        proto::SpecReply r;
        if (!r.ParseFromArray(data.data(), data.size())) return;
        auto it = pending_.find(r.request_id());
        if (it == pending_.end()) return;
        PendingRequest &p = it->second;

        bound_tracker_.on_owd_sample(r.server_id(), r.owd_estimate());
        p.quorum.AddReply(r.server_id(), r.view_id(), r.t(), r.is_leader());
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
                    transport->SendMessageToAll(this, fc);
                    Commit(it, std::string());
                } else {
                    // Reads aren't logged/committed on servers: just return the value
                    // read from the involved leaders (no FastCommit).
                    auto rit = p.results.find(p.key);
                    Commit(it, rit != p.results.end() ? rit->second : std::string());
                }
            } else if (p.type == proto::WRITE && p.quorum.leaders_disagree()) {
                proto::SlowCommit sc;
                sc.set_client_id(clientid);
                sc.set_request_id(p.request_id);
                sc.set_view_id(viewId_);
                sc.set_agreed_t(p.quorum.slow_agreed_t());
                transport->SendMessageToAll(this, sc);
                p.phase = Phase::SLOW_WAIT;
                p.send_us = NowMicros();
            }
            // Reads that aren't fast_ready wait for more replies, or a RejectReply /
            // resend sweep re-issues at a larger t.
        }
        return;
    }

    if (type == rejectName) {
        proto::RejectReply r;
        if (!r.ParseFromArray(data.data(), data.size())) return;
        auto it = pending_.find(r.request_id());
        if (it == pending_.end()) return;
        PendingRequest &p = it->second;
        if (p.type == proto::READ && p.phase == Phase::COLLECTING) {
            GrowReadOwd(p);
            SendRequestProto(p.request_id);   // retry the read at a larger t
        }
        return;
    }

    if (type == commitReplyName) {
        proto::CommitReply r;
        if (!r.ParseFromArray(data.data(), data.size())) return;
        auto it = pending_.find(r.request_id());
        if (it == pending_.end()) return;
        PendingRequest &p = it->second;
        p.quorum.AddReply(r.server_id(), r.view_id(), r.t(), r.is_leader());
        if (p.quorum.fast_ready()) Commit(it, std::string());   // write quorum re-formed
        return;
    }

    Client::ReceiveMessage(remote, type, data, meta_data);
}

} // namespace dyna_multi
} // namespace specpaxos

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_multi/replica.cc  (Stage 3: write-path commit handshake)

#include "dyna_multi/replica.h"

#include "lib/message.h"

#include <cstdlib>
#include <memory>
#include <string>
#include <utility>

namespace specpaxos {
namespace dyna_multi {

namespace proto = specpaxos::dyna_multi::proto;

DynaMultiReplica::DynaMultiReplica(const specpaxos::Configuration &config,
                                   int replicaIdx,
                                   Transport *transport)
    : config(config), replicaIdx(replicaIdx), transport(transport),
      topo_(static_cast<uint32_t>(config.n)),
      owd_(DeadlineConfig::FromEnv())
{
    const char *bp = std::getenv("DYNA_MULTI_BUSY_POLL");
    const bool busyPoll = (bp == nullptr) || (*bp != '0');
    const char *dbg = std::getenv("DYNA_MULTI_DEBUG");
    debug_ = (dbg != nullptr && *dbg != '\0' && *dbg != '0');
    clock_ = std::make_unique<RealClock>(busyPoll);
    backend_ = std::make_unique<InMemoryBackend>(InMemoryBackend::LatencyFromEnv());
    orderer_ = std::make_unique<ServerOrderer>(
        static_cast<uint32_t>(replicaIdx), viewId_, clock_.get(), topo_,
        [this](const TxnEntry &e) { OnSpecReply(e); }, backend_.get(),
        [this](const TxnEntry &e) { OnReject(e); });
    orderer_->Start();

    ts_scan_ms_ = DeadlineConfig::EnvU64("DYNA_MULTI_TS_SCAN_MS", 50);
    if (ts_scan_ms_ == 0) ts_scan_ms_ = 1;
    ts_timeout_us_ = DeadlineConfig::EnvU64("DYNA_MULTI_TS_TIMEOUT_MS", 100) * 1000;

    transport->Register(this, config, 0, replicaIdx);
    // Background ts-agreement scan (spec §3.5). Re-arms itself in TsAgreementTick.
    transport->Timer(ts_scan_ms_, [this]() { TsAgreementTick(); });
    Notice("DYNA_MULTI r%d up: n=%d f=%d busyPoll=%d", replicaIdx, config.n, config.f,
           busyPoll ? 1 : 0);
}

DynaMultiReplica::~DynaMultiReplica()
{
    if (orderer_) orderer_->Stop();
}

void
DynaMultiReplica::ReceiveMessage(const TransportAddress &remote,
                                 const std::string_view type,
                                 const std::string_view data,
                                 void *meta_data)
{
    (void)meta_data;
    const static auto requestName =
        proto::ClientRequest::default_instance().GetDescriptor()->full_name();
    const static auto fastName =
        proto::FastCommit::default_instance().GetDescriptor()->full_name();
    const static auto slowName =
        proto::SlowCommit::default_instance().GetDescriptor()->full_name();
    const static auto fetchReqName =
        proto::FetchRequest::default_instance().GetDescriptor()->full_name();
    const static auto fetchRepName =
        proto::FetchReply::default_instance().GetDescriptor()->full_name();
    const static auto tsAgreeName =
        proto::TsAgree::default_instance().GetDescriptor()->full_name();
    const static auto repPrepName =
        proto::ReplicatePrepare::default_instance().GetDescriptor()->full_name();
    const static auto repPrepReplyName =
        proto::ReplicatePrepareReply::default_instance().GetDescriptor()->full_name();
    const static auto repCommitName =
        proto::ReplicateCommit::default_instance().GetDescriptor()->full_name();

    if (type == requestName) {
        proto::ClientRequest m;
        if (m.ParseFromArray(data.data(), data.size())) HandleClientRequest(remote, m);
        return;
    }
    if (type == fastName) {
        proto::FastCommit m;
        if (m.ParseFromArray(data.data(), data.size())) {
            HandleCommit(remote, TxnId{m.client_id(), m.request_id()}, m.agreed_t(),
                         /*slow=*/false);
        }
        return;
    }
    if (type == slowName) {
        proto::SlowCommit m;
        if (m.ParseFromArray(data.data(), data.size())) {
            HandleCommit(remote, TxnId{m.client_id(), m.request_id()}, m.agreed_t(),
                         /*slow=*/true);
        }
        return;
    }
    if (type == fetchReqName) {
        proto::FetchRequest m;
        if (m.ParseFromArray(data.data(), data.size())) HandleFetchRequest(remote, m);
        return;
    }
    if (type == fetchRepName) {
        proto::FetchReply m;
        if (m.ParseFromArray(data.data(), data.size())) HandleFetchReply(m);
        return;
    }
    if (type == tsAgreeName) {
        proto::TsAgree m;
        if (m.ParseFromArray(data.data(), data.size())) HandleTsAgree(m);
        return;
    }
    if (type == repPrepName) {
        proto::ReplicatePrepare m;
        if (m.ParseFromArray(data.data(), data.size())) HandleReplicatePrepare(remote, m);
        return;
    }
    if (type == repPrepReplyName) {
        proto::ReplicatePrepareReply m;
        if (m.ParseFromArray(data.data(), data.size())) HandleReplicatePrepareReply(m);
        return;
    }
    if (type == repCommitName) {
        proto::ReplicateCommit m;
        if (m.ParseFromArray(data.data(), data.size())) HandleReplicateCommit(m);
        return;
    }
}

void
DynaMultiReplica::HandleReplicatePrepare(const TransportAddress &remote,
                                         const proto::ReplicatePrepare &m)
{
    const TxnId id{m.client_id(), m.request_id()};
    if (!m.has_txn()) return;   // leader always includes the body
    if (!orderer_->HandleReplicatePrepare(id, m.agreed_t(), m.txn())) return;
    proto::ReplicatePrepareReply reply;
    reply.set_client_id(id.clientid);
    reply.set_request_id(id.clientreqid);
    reply.set_view_id(viewId_);
    reply.set_server_id(static_cast<uint32_t>(replicaIdx));
    transport->SendMessage(this, remote, reply);   // ack to the primary leader
}

void
DynaMultiReplica::HandleReplicatePrepareReply(const proto::ReplicatePrepareReply &m)
{
    const TxnId id{m.client_id(), m.request_id()};
    if (!orderer_->RecordPrepareReply(id, m.server_id())) return;   // not yet f acks
    // Reached f acks → COMMITTED. Tell everyone else.
    proto::ReplicateCommit rc;
    rc.set_client_id(id.clientid);
    rc.set_request_id(id.clientreqid);
    rc.set_view_id(viewId_);
    rc.set_leader_id(static_cast<uint32_t>(replicaIdx));
    transport->SendMessageToAll(this, rc);
}

void
DynaMultiReplica::HandleReplicateCommit(const proto::ReplicateCommit &m)
{
    orderer_->MarkCommitted(TxnId{m.client_id(), m.request_id()});
}

void
DynaMultiReplica::SendReplicatePrepare(const ServerOrderer::ReplicateAction &a)
{
    proto::ReplicatePrepare m;
    m.set_client_id(a.id.clientid);
    m.set_request_id(a.id.clientreqid);
    m.set_view_id(viewId_);
    m.set_leader_id(static_cast<uint32_t>(replicaIdx));
    m.set_agreed_t(a.agreed_t);
    *m.mutable_txn() = a.body;
    transport->SendMessageToAll(this, m);   // to all other servers (self ignores)
}

void
DynaMultiReplica::HandleTsAgree(const proto::TsAgree &m)
{
    const TxnId id{m.client_id(), m.request_id()};
    ServerOrderer::TsRecordResult r =
        orderer_->RecordTsProposal(id, m.sender_id(), m.proposed_t());
    if (r.has_action) SendTsAgree(r.action);
    // r.transitioned → COMMITTING; Stage 5 replication drives it to COMMITTED.
}

void
DynaMultiReplica::TsAgreementTick()
{
    std::vector<ServerOrderer::TsAgreeAction> actions;
    std::vector<TxnEntry> committed;
    orderer_->DriveTsAgreement(ts_timeout_us_, &actions, &committed);
    for (const auto &a : actions) SendTsAgree(a);

    // Replication (spec §3.6): drive COMMITTING → COMMITTED for txns we lead. Throttle
    // re-broadcast to once per ts_timeout_us_ so a COMMITTING txn isn't re-flooded every
    // scan (it normally reaches COMMITTED within one round).
    std::vector<ServerOrderer::ReplicateAction> prepares;
    orderer_->DriveReplication(&prepares, ts_timeout_us_);
    for (const auto &p : prepares) SendReplicatePrepare(p);

    // Execution (spec §3.7): apply COMMITTED writes in dependency order, GC from specLog.
    orderer_->ExecuteReady();

    transport->Timer(ts_scan_ms_, [this]() { TsAgreementTick(); });   // re-arm
}

void
DynaMultiReplica::SendTsAgree(const ServerOrderer::TsAgreeAction &action)
{
    for (uint32_t leader : action.to_leaders) {
        proto::TsAgree m;
        m.set_client_id(action.id.clientid);
        m.set_request_id(action.id.clientreqid);
        m.set_view_id(viewId_);
        m.set_sender_id(static_cast<uint32_t>(replicaIdx));
        m.set_proposed_t(action.proposed_t);
        transport->SendMessageToReplica(this, leader, m);
    }
}

void
DynaMultiReplica::HandleClientRequest(const TransportAddress &remote,
                                      const proto::ClientRequest &m)
{
    const proto::Txn &txn = m.txn();
    const TxnId id{txn.client_id(), txn.request_id()};

    // At-most-once (spec §1.4/§5): answer a retry from cache; drop one still in flight
    // so the orderer never holds two entries for the same txnId.
    {
        std::lock_guard<std::mutex> lk(dedup_mu_);
        auto cit = reply_cache_.find(id);
        if (cit != reply_cache_.end()) {
            proto::SpecReply cached = cit->second;
            // (unlock happens at scope exit; send after is fine — retry window is ms)
            transport->SendMessage(this, remote, cached);
            return;
        }
        if (inflight_.find(id) != inflight_.end()) return;   // in flight -> drop retry
        inflight_.insert(id);
    }

    // OWD sample (§2): one-way delay = arrival − send_time (client stamp).
    const uint64_t arrival = WallClockMicros();
    uint64_t owd_est = 0;
    if (txn.has_send_time()) {
        std::lock_guard<std::mutex> lk(owd_mu_);
        if (arrival > txn.send_time()) owd_.sample(txn.client_id(), arrival - txn.send_time());
        owd_est = owd_.estimate_us(txn.client_id());
    }

    TxnEntry e;
    e.txn = txn;
    e.t = txn.t();
    e.owd_estimate_us = owd_est;
    e.arrival_us = arrival;
    e.client_addr = std::shared_ptr<TransportAddress>(remote.clone());
    if (debug_) {
        Notice("r%d RECV req c=%lu r=%lu t=%lu now=%lu wset=%d", replicaIdx,
               (unsigned long)id.clientid, (unsigned long)id.clientreqid,
               (unsigned long)txn.t(), (unsigned long)arrival, txn.w_set_size());
    }
    orderer_->SubmitTxn(std::move(e));
}

proto::SpecReply
DynaMultiReplica::BuildSpecReply(const TxnEntry &e) const
{
    proto::SpecReply r;
    r.set_client_id(e.txn.client_id());
    r.set_request_id(e.txn.request_id());
    r.set_view_id(viewId_);
    r.set_server_id(static_cast<uint32_t>(replicaIdx));
    r.set_t(e.t);
    r.set_is_leader(e.is_leader_for_this);
    r.set_owd_estimate(e.owd_estimate_us);
    // READ (leader): carry the disk-read values for the keys this server leads.
    for (const auto &kv : e.results) {
        proto::KV *out = r.add_results();
        out->set_key(kv.first);
        out->set_value(kv.second);
    }
    return r;
}

void
DynaMultiReplica::OnReject(const TxnEntry &e)
{
    if (e.client_addr) {
        proto::RejectReply r;
        r.set_client_id(e.txn.client_id());
        r.set_request_id(e.txn.request_id());
        r.set_view_id(viewId_);
        r.set_server_id(static_cast<uint32_t>(replicaIdx));
        transport->SendMessage(this, *e.client_addr, r);
    }
    // Clear in-flight so the client's retry (at a larger t) is re-processed, not dropped.
    std::lock_guard<std::mutex> lk(dedup_mu_);
    inflight_.erase(e.Key());
}

void
DynaMultiReplica::OnSpecReply(const TxnEntry &e)
{
    proto::SpecReply reply = BuildSpecReply(e);
    if (debug_) {
        Notice("r%d SEND SpecReply c=%lu r=%lu t=%lu leader=%d addr=%d", replicaIdx,
               (unsigned long)e.txn.client_id(), (unsigned long)e.txn.request_id(),
               (unsigned long)e.t, e.is_leader_for_this ? 1 : 0, e.client_addr ? 1 : 0);
    }
    if (e.client_addr) transport->SendMessage(this, *e.client_addr, reply);
    // Cache for at-most-once AFTER the send (off the release critical path).
    std::lock_guard<std::mutex> lk(dedup_mu_);
    reply_cache_[e.Key()] = std::move(reply);
    inflight_.erase(e.Key());
}

void
DynaMultiReplica::HandleCommit(const TransportAddress &remote, const TxnId &id,
                               uint64_t agreedT, bool slow)
{
    TxnEntry out;
    ServerOrderer::CommitResult r = orderer_->ApplyCommit(id, agreedT, &out);
    if (r == ServerOrderer::CommitResult::kApplied) {
        {
            std::lock_guard<std::mutex> lk(dedup_mu_);
            inflight_.erase(id);
        }
        if (slow) SendCommitReply(remote, out);
        return;
    }

    // kNeedFetch: remember the commit, then broadcast a FetchRequest for the body.
    {
        std::lock_guard<std::mutex> lk(pending_mu_);
        PendingCommit pc;
        pc.agreedT = agreedT;
        pc.slow = slow;
        if (slow) pc.client_addr = std::shared_ptr<TransportAddress>(remote.clone());
        pending_commits_[id] = std::move(pc);
    }
    proto::FetchRequest fr;
    fr.set_client_id(id.clientid);
    fr.set_request_id(id.clientreqid);
    fr.set_view_id(viewId_);
    fr.set_requester_id(static_cast<uint32_t>(replicaIdx));
    transport->SendMessageToAll(this, fr);   // whoever holds the body replies
}

void
DynaMultiReplica::HandleFetchRequest(const TransportAddress &remote,
                                     const proto::FetchRequest &m)
{
    const TxnId id{m.client_id(), m.request_id()};
    proto::Txn body;
    if (!orderer_->TryGetBody(id, &body)) return;   // we don't have it
    proto::FetchReply reply;
    *reply.mutable_txn() = body;
    transport->SendMessage(this, remote, reply);
}

void
DynaMultiReplica::HandleFetchReply(const proto::FetchReply &m)
{
    const proto::Txn &body = m.txn();
    orderer_->InstallFetched(body);
    const TxnId id{body.client_id(), body.request_id()};

    PendingCommit pc;
    bool have = false;
    {
        std::lock_guard<std::mutex> lk(pending_mu_);
        auto it = pending_commits_.find(id);
        if (it != pending_commits_.end()) {
            pc = it->second;
            pending_commits_.erase(it);
            have = true;
        }
    }
    if (!have) return;

    TxnEntry out;
    if (orderer_->ApplyCommit(id, pc.agreedT, &out) ==
        ServerOrderer::CommitResult::kApplied) {
        {
            std::lock_guard<std::mutex> lk(dedup_mu_);
            inflight_.erase(id);
        }
        if (pc.slow && pc.client_addr) SendCommitReply(*pc.client_addr, out);
    }
}

void
DynaMultiReplica::SendCommitReply(const TransportAddress &client, const TxnEntry &e)
{
    proto::CommitReply r;
    r.set_client_id(e.txn.client_id());
    r.set_request_id(e.txn.request_id());
    r.set_view_id(viewId_);
    r.set_server_id(static_cast<uint32_t>(replicaIdx));
    r.set_t(e.t);
    r.set_is_leader(e.is_leader_for_this);
    transport->SendMessage(this, client, r);
}

} // namespace dyna_multi
} // namespace specpaxos

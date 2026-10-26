// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/replica.cc

#include "dyna/replica.h"

#include "dyna/cpu_pin.h"
#include "dyna/db_backend.h"
#include "dyna/in_memory_backend.h"

#include "lib/message.h"

#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <mutex>
#include <shared_mutex>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>
#include <utility>

namespace specpaxos {
namespace dyna {

namespace proto = specpaxos::dyna::proto;

namespace {
// The calling receive thread's own transport (BindThreadTransport); null elsewhere.
thread_local Transport *t_thread_transport = nullptr;
} // namespace

void
DynaReplica::BindThreadTransport(Transport *t)
{
    t_thread_transport = t;
}

Transport *
DynaReplica::Tx() const
{
    return t_thread_transport != nullptr ? t_thread_transport : transport;
}

void
DynaReplica::AddReceiveTransport(Transport *t)
{
    t->Register(this, config, 0, replicaIdx);
}

DynaReplica::DynaReplica(const specpaxos::Configuration &config,
                         int replicaIdx,
                         Transport *transport,
                         DB *db)
    : config(config), replicaIdx(replicaIdx), transport(transport),
      topo_(static_cast<uint32_t>(config.n))
{
    const char *bp = std::getenv("DYNA_BUSY_POLL");
    const bool busyPoll = (bp == nullptr) || (*bp != '0');
    const char *dbg = std::getenv("DYNA_DEBUG");
    debug_ = (dbg != nullptr && *dbg != '\0' && *dbg != '0');
    clock_ = std::make_unique<RealClock>(busyPoll);
    if (db != nullptr) backend_ = std::make_unique<DbBackend>(db);
    else backend_ = std::make_unique<InMemoryBackend>();
    // Ordering threads: on by default in busy-poll mode (DYNA_ORDER_THREAD=0 = handlers
    // run on the receive threads under the ordering lock, as before). DYNA_LANES ordering
    // threads (lanes) split the transactions by TxnId; without ordering threads, 1 lane.
    order_thread_on_ = busyPoll && DeadlineConfig::EnvU64("DYNA_ORDER_THREAD", 1) != 0;
    const size_t n_lanes = order_thread_on_
        ? std::max<size_t>(1, DeadlineConfig::EnvU64("DYNA_LANES", 1)) : 1;
    orderer_ = std::make_unique<ServerOrderer>(
        static_cast<uint32_t>(replicaIdx), viewId_, clock_.get(), topo_,
        [this](const TxnEntry &e) { OnSpecReply(e); }, backend_.get(), n_lanes);
    stats_on_ = StatsEnabledFromEnv();
    last_hb_rx_us_.assign(static_cast<size_t>(config.n), 0);

    // Background batching (replication + ts-agreement): heavy by default — this traffic
    // is off the client's commit path, so it trades a little execution delay for far
    // fewer datagrams. DYNA_REPL_BATCH <= 1 = one message per txn, sent immediately.
    const uint64_t bg_batch = DeadlineConfig::EnvU64("DYNA_REPL_BATCH", 256);
    bg_flush_us_ = std::max<uint64_t>(1, DeadlineConfig::EnvU64("DYNA_REPL_FLUSH_US", 500));
    bg_on_ = bg_batch > 1;
    const size_t n_senders = order_thread_on_ ? DeadlineConfig::EnvU64("DYNA_SEND_THREADS", 1) : 0;
    // DYNA_PRERESERVE: pre-size the never-GC'd per-txn maps (executed set, reply cache)
    // so a long benchmark doesn't pay multi-ms rehashes under their locks mid-run.
    const uint64_t rsv = DeadlineConfig::EnvU64("DYNA_PRERESERVE", 0);
    if (rsv > 0) orderer_->ReserveExecuted(rsv);
    for (size_t i = 0; i < orderer_->lanes(); ++i) {
        auto l = std::make_unique<Lane>();
        l->idx = i;
        l->owd = std::make_unique<ReplicaOwdEstimator>(DeadlineConfig::FromEnv());
        if (bg_on_) {
            l->bg = std::make_unique<BgOutbox>(static_cast<uint32_t>(config.n), bg_batch,
                                               DeadlineConfig::EnvU64("DYNA_REPL_MAX_BYTES", 8000));
        }
        if (rsv > 0) l->reply_cache.reserve(rsv / orderer_->lanes() + rsv / (4 * orderer_->lanes()));
        l->sender = n_senders ? i % n_senders : 0;
        if (stats_on_) {
            l->dedup_mu.SetWaitStats(&dedup_wait_us_);
            l->owd_mu.SetWaitStats(&owd_wait_us_);
        }
        lanes_.push_back(std::move(l));
    }
    if (stats_on_) {
        orderer_->SetStats(&ostats_);
        pending_mu_.SetWaitStats(&pending_wait_us_);
    }
    // Executor passes at most every DYNA_EXEC_MIN_INTERVAL_US (default 2000): each pass
    // takes the ordering lock; at 55k ops/s per-commit passes held it ~14% of the time.
    orderer_->SetExecMinInterval(DeadlineConfig::EnvU64("DYNA_EXEC_MIN_INTERVAL_US", 2000));
    orderer_->Start(/*own_release_thread=*/!order_thread_on_);

    ts_scan_ms_ = DeadlineConfig::EnvU64("DYNA_TS_SCAN_MS", 50);
    if (ts_scan_ms_ == 0) ts_scan_ms_ = 1;
    ts_timeout_us_ = DeadlineConfig::EnvU64("DYNA_TS_TIMEOUT_MS", 100) * 1000;

    // VR layer (recovery). DYNA_FAIL_TIMEOUT_MS=0 disables failure detection.
    ViewCoordinator::Config vcfg;
    vcfg.self = static_cast<uint32_t>(replicaIdx);
    vcfg.n = static_cast<uint32_t>(config.n);
    vcfg.f = static_cast<uint32_t>(config.f);
    vcfg.fail_timeout_us = DeadlineConfig::EnvU64("DYNA_FAIL_TIMEOUT_MS", 1000) * 1000;
    vcfg.grace_us = DeadlineConfig::EnvU64("DYNA_FAIL_GRACE_MS", 10000) * 1000;
    hb_ms_ = DeadlineConfig::EnvU64("DYNA_HB_MS", 20);
    if (hb_ms_ == 0) hb_ms_ = 1;
    vcfg.resend_us = std::max<uint64_t>(ts_scan_ms_, hb_ms_) * 1000;
    snapshot_chunk_ = std::max<uint64_t>(1, DeadlineConfig::EnvU64("DYNA_SNAPSHOT_CHUNK", 512));
    if (const char *d = std::getenv("DYNA_VR_STATE_DIR"); d != nullptr && *d != '\0') {
        vr_state_dir_ = d;
    }
    {
        // Durable VR state: if we were a member before, our in-memory state is gone —
        // rejoin rather than start in view 0.
        std::ifstream in(VrStatePath());
        vcfg.start_joining = in.good();
    }
    vc_ = std::make_unique<ViewCoordinator>(vcfg, WallClockMicros());
    if (vcfg.start_joining) {
        Notice("r%d found %s: restarted, rejoining", replicaIdx, VrStatePath().c_str());
        paused_ = true;
        orderer_->SetPaused(true);
    } else {
        PersistVrState(0, topo_.alive_mask);
    }

    transport->Register(this, config, 0, replicaIdx);
    // Background ts-agreement scan (spec §3.5). Re-arms itself in TsAgreementTick.
    transport->Timer(ts_scan_ms_, [this]() { TsAgreementTick(); });
    transport->Timer(hb_ms_, [this]() { HeartbeatTick(); });

    if (bg_on_) transport->TimerUS(bg_flush_us_, [this]() { BgFlushTick(); });
    if (order_thread_on_) {
        // Initialize the primary transport's lazy address maps from this thread before
        // the lane / sender threads can send through it concurrently.
        proto::Heartbeat hb;
        hb.set_view_id(viewId_);
        hb.set_sender_id(static_cast<uint32_t>(replicaIdx));
        transport->SendMessageToAll(this, hb);
        send_running_.store(true);
        for (size_t i = 0; i < n_senders; ++i) {
            senders_.push_back(std::make_unique<Sender>());
            senders_.back()->th = std::thread(&DynaReplica::SendLoop, this,
                                              senders_.back().get(), i);
        }
        order_running_.store(true);
        for (auto &l : lanes_) l->th = std::thread(&DynaReplica::OrderingLoop, this, l.get());
    }
    Notice("DYNA r%d up: n=%d f=%d busyPoll=%d orderThread=%d lanes=%zu senders=%zu "
           "store=%s failTimeoutMs=%lu replBatch=%lu replFlushUs=%lu", replicaIdx, config.n,
           config.f, busyPoll ? 1 : 0, order_thread_on_ ? 1 : 0, lanes_.size(), senders_.size(),
           db != nullptr ? "harness-db" : "in-memory",
           (unsigned long)(vcfg.fail_timeout_us / 1000), (unsigned long)(bg_on_ ? bg_batch : 1),
           (unsigned long)bg_flush_us_);
}

DynaReplica::~DynaReplica()
{
    order_running_.store(false);
    for (auto &l : lanes_) if (l->th.joinable()) l->th.join();
    send_running_.store(false);
    for (auto &snd : senders_) if (snd->th.joinable()) snd->th.join();
    if (orderer_) orderer_->Stop();
}

DynaReplica::RxType
DynaReplica::ClassifyRx(std::string_view type)
{
    const static auto req = proto::ClientRequest::default_instance().GetDescriptor()->full_name();
    const static auto fast = proto::FastCommit::default_instance().GetDescriptor()->full_name();
    const static auto slow = proto::SlowCommit::default_instance().GetDescriptor()->full_name();
    const static auto prep =
        proto::ReplicatePrepare::default_instance().GetDescriptor()->full_name();
    const static auto prepR =
        proto::ReplicatePrepareReply::default_instance().GetDescriptor()->full_name();
    const static auto commit =
        proto::ReplicateCommit::default_instance().GetDescriptor()->full_name();
    const static auto ts = proto::TsAgree::default_instance().GetDescriptor()->full_name();
    const static auto fetchReq =
        proto::FetchRequest::default_instance().GetDescriptor()->full_name();
    const static auto fetchRep =
        proto::FetchReply::default_instance().GetDescriptor()->full_name();
    const static auto hb = proto::Heartbeat::default_instance().GetDescriptor()->full_name();
    if (type == fetchRep) return kRxFetchRep;
    const static auto bg = proto::BgBatch::default_instance().GetDescriptor()->full_name();
    const static auto fastB =
        proto::FastCommitBatch::default_instance().GetDescriptor()->full_name();
    if (type == req) return kRxRequest;
    if (type == fastB) return kRxFastBatch;
    if (type == bg) return kRxBg;
    if (type == fast) return kRxFast;
    if (type == slow) return kRxSlow;
    if (type == prep) return kRxPrep;
    if (type == prepR) return kRxPrepReply;
    if (type == commit) return kRxCommit;
    if (type == ts) return kRxTsAgree;
    if (type == fetchReq) return kRxFetch;
    if (type == hb) return kRxHeartbeat;
    return kRxOther;
}

void
DynaReplica::ReceiveMessage(const TransportAddress &remote,
                                 const std::string_view type,
                                 const std::string_view data,
                                 void *meta_data)
{
    const RxType kind = ClassifyRx(type);
    const bool hot = IsHot(kind);
    const uint64_t t0 = stats_on_ ? WallClockMicros() : 0;
    // Paused (view change / joining): client-originated messages would be dropped by
    // ProcessHot anyway; drop them here before parsing so a resend flood can't crowd
    // out the recovery traffic. Replication batches still flow (a JOINING server
    // applies them).
    if (paused_.load(std::memory_order_relaxed) &&
        (kind == kRxRequest || kind == kRxFast || kind == kRxSlow || kind == kRxFastBatch)) {
        if (stats_on_) rx_[kind].Add();
        return;
    }
    if (hot && order_thread_on_) {
        // Parse here (receive thread), process on the transaction's lane thread.
        std::unique_ptr<google::protobuf::Message> parsed = ParseHot(kind, data);
        const bool timed = stats_on_;
        if (parsed && kind == kRxRequest) {
            // Build the entry here (txn moved out of the parsed message); the lane thread
            // samples OWD, dedups and submits it.
            auto *req = static_cast<proto::ClientRequest *>(parsed.get());
            auto entry = std::make_shared<TxnEntry>(BuildRequestEntry(
                std::shared_ptr<TransportAddress>(remote.clone()),
                std::move(*req->mutable_txn())));
            if (timed) {
                entry->arrival_us = t0;   // receive-thread time; SubmitRequestEntry overwrites
                if (entry->txn.has_send_time() && t0 > entry->txn.send_time()) {
                    owd_net_hist_.Observe(t0 - entry->txn.send_time());
                }
            }
            PostToLane(orderer_->LaneOf(entry->Key()), [this, entry, timed]() {
                const uint64_t h0 = timed ? WallClockMicros() : 0;
                ProcessRequestEntry(std::move(*entry));
                if (timed) rx_us_[kRxRequest].Add(WallClockMicros() - h0);
            });
        } else if (parsed && kind == kRxBg) {
            // A batch normally belongs to one lane (the sender batches per lane with the
            // same LaneOf); split it per lane otherwise, keeping each lane's entry order.
            auto *b = static_cast<proto::BgBatch *>(parsed.get());
            const size_t nl = lanes_.size();
            std::vector<std::shared_ptr<proto::BgBatch>> per(nl);
            auto slot = [&](uint64_t c, uint64_t r) -> proto::BgBatch & {
                auto &p = per[orderer_->LaneOf(TxnId{c, r})];
                if (!p) { p = std::make_shared<proto::BgBatch>(); p->set_sender_id(b->sender_id()); }
                return *p;
            };
            if (nl == 1) {
                per[0] = std::shared_ptr<proto::BgBatch>(
                    static_cast<proto::BgBatch *>(parsed.release()));
            } else {
                for (auto &m : *b->mutable_prepares()) *slot(m.client_id(), m.request_id()).add_prepares() = std::move(m);
                for (auto &m : *b->mutable_prepare_replies()) *slot(m.client_id(), m.request_id()).add_prepare_replies() = std::move(m);
                for (auto &m : *b->mutable_commits()) *slot(m.client_id(), m.request_id()).add_commits() = std::move(m);
                for (auto &m : *b->mutable_ts_agrees()) *slot(m.client_id(), m.request_id()).add_ts_agrees() = std::move(m);
            }
            for (size_t i = 0; i < nl; ++i) {
                if (!per[i]) continue;
                std::shared_ptr<proto::BgBatch> part = std::move(per[i]);
                PostToLane(i, [this, part, timed]() {
                    const uint64_t h0 = timed ? WallClockMicros() : 0;
                    HandleBgBatch(*part);
                    if (timed) rx_us_[kRxBg].Add(WallClockMicros() - h0);
                });
            }
        } else if (parsed && kind == kRxFastBatch) {
            // One client's FastCommits: route each to its txn's lane (split per lane,
            // keeping each lane's order).
            auto *b = static_cast<proto::FastCommitBatch *>(parsed.get());
            const size_t nl = lanes_.size();
            std::vector<std::shared_ptr<proto::FastCommitBatch>> per(nl);
            if (nl == 1) {
                per[0] = std::shared_ptr<proto::FastCommitBatch>(
                    static_cast<proto::FastCommitBatch *>(parsed.release()));
            } else {
                for (auto &m : *b->mutable_commits()) {
                    auto &p = per[orderer_->LaneOf(TxnId{m.client_id(), m.request_id()})];
                    if (!p) p = std::make_shared<proto::FastCommitBatch>();
                    *p->add_commits() = std::move(m);
                }
            }
            std::shared_ptr<TransportAddress> addr(remote.clone());
            for (size_t i = 0; i < nl; ++i) {
                if (!per[i]) continue;
                std::shared_ptr<proto::FastCommitBatch> part = std::move(per[i]);
                PostToLane(i, [this, part, addr, timed]() {
                    const uint64_t h0 = timed ? WallClockMicros() : 0;
                    ProcessHot(kRxFastBatch, *addr, *part);
                    if (timed) rx_us_[kRxFastBatch].Add(WallClockMicros() - h0);
                });
            }
        } else if (parsed) {
            TxnId id;
            const size_t lane = HotTxnId(kind, *parsed, &id) ? orderer_->LaneOf(id) : 0;
            std::shared_ptr<google::protobuf::Message> msg(std::move(parsed));
            std::shared_ptr<TransportAddress> addr(remote.clone());
            PostToLane(lane, [this, kind, msg, addr, timed]() {
                const uint64_t h0 = timed ? WallClockMicros() : 0;
                ProcessHot(kind, *addr, *msg);
                if (timed) rx_us_[kind].Add(WallClockMicros() - h0);
            });
        }
        if (stats_on_) {
            busy_us_.Add(WallClockMicros() - t0);
            rx_[kind].Add();
        }
        return;
    }
    if (hot) {
        std::shared_lock<RwLock> g(state_lock_);
        ReceiveMessageImpl(remote, type, data, meta_data);
    } else {
        std::unique_lock<RwLock> g(state_lock_);
        ReceiveMessageImpl(remote, type, data, meta_data);
    }
    if (!stats_on_) return;
    const uint64_t d = WallClockMicros() - t0;
    busy_us_.Add(d);
    handler_max_us_.Observe(d);
    rx_[kind].Add();
    rx_us_[kind].Add(d);
}

void
DynaReplica::ReceiveMessageImpl(const TransportAddress &remote,
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
    const static auto hbName =
        proto::Heartbeat::default_instance().GetDescriptor()->full_name();
    const static auto svcName =
        proto::StartViewChange::default_instance().GetDescriptor()->full_name();
    const static auto dvcName =
        proto::DoViewChange::default_instance().GetDescriptor()->full_name();
    const static auto startRecName =
        proto::StartRecovery::default_instance().GetDescriptor()->full_name();
    const static auto recReqName =
        proto::RecoveryRequest::default_instance().GetDescriptor()->full_name();
    const static auto recRepName =
        proto::RecoveryReply::default_instance().GetDescriptor()->full_name();
    const static auto recPrepName =
        proto::RecoveryPrepare::default_instance().GetDescriptor()->full_name();
    const static auto recPrepReplyName =
        proto::RecoveryPrepareReply::default_instance().GetDescriptor()->full_name();
    const static auto recReportName =
        proto::RecoveryReport::default_instance().GetDescriptor()->full_name();
    const static auto startViewName =
        proto::StartView::default_instance().GetDescriptor()->full_name();
    const static auto joinReqName =
        proto::JoinRequest::default_instance().GetDescriptor()->full_name();
    const static auto joinInfoName =
        proto::JoinInfo::default_instance().GetDescriptor()->full_name();
    const static auto joinReadyName =
        proto::JoinReady::default_instance().GetDescriptor()->full_name();
    const static auto snapReqName =
        proto::SnapshotRequest::default_instance().GetDescriptor()->full_name();
    const static auto snapChunkName =
        proto::SnapshotChunk::default_instance().GetDescriptor()->full_name();

    // --- VR layer + recovery (always processed) ---
    ViewCoordinator::Effects fx;
    const uint64_t now = WallClockMicros();
    if (type == hbName) {
        proto::Heartbeat m;
        if (!m.ParseFromArray(data.data(), data.size())) return;
        vc_->OnHeard(m.sender_id(), now);
        if (stats_on_ && m.sender_id() < last_hb_rx_us_.size()) {
            uint64_t &last = last_hb_rx_us_[m.sender_id()];
            if (last != 0) hb_gap_us_.Observe(now - last);
            last = now;
        }
        if (m.view_id() > viewId_.load() && Normal()) {
            // We missed a whole view change: ask (the VR leader answers with StartView).
            proto::StartViewChange q;
            q.set_view_id(m.view_id());
            q.set_sender_id(static_cast<uint32_t>(replicaIdx));
            q.set_alive(vc_->alive());
            Tx()->SendMessageToAll(this, q);
        }
        return;
    }
    if (type == svcName) {
        proto::StartViewChange m;
        if (m.ParseFromArray(data.data(), data.size())) vc_->OnStartViewChange(m, now, &fx);
        ApplyEffects(fx);
        return;
    }
    if (type == dvcName) {
        proto::DoViewChange m;
        if (m.ParseFromArray(data.data(), data.size())) vc_->OnDoViewChange(m, now, &fx);
        ApplyEffects(fx);
        return;
    }
    if (type == startRecName) {
        proto::StartRecovery m;
        if (m.ParseFromArray(data.data(), data.size())) vc_->OnStartRecovery(m, now, &fx);
        ApplyEffects(fx);
        return;
    }
    if (type == recReqName) {
        proto::RecoveryRequest m;
        if (m.ParseFromArray(data.data(), data.size())) HandleRecoveryRequest(m);
        return;
    }
    if (type == recRepName) {
        proto::RecoveryReply m;
        if (m.ParseFromArray(data.data(), data.size())) {
            vc_->OnHeard(m.sender_id(), now);
            vc_->OnRecoveryReply(m, &fx);
        }
        ApplyEffects(fx);
        return;
    }
    if (type == recPrepName) {
        proto::RecoveryPrepare m;
        if (m.ParseFromArray(data.data(), data.size())) vc_->OnRecoveryPrepare(m, now, &fx);
        ApplyEffects(fx);
        return;
    }
    if (type == recPrepReplyName) {
        proto::RecoveryPrepareReply m;
        if (m.ParseFromArray(data.data(), data.size())) {
            vc_->OnHeard(m.sender_id(), now);
            vc_->OnRecoveryPrepareReply(m, &fx);
        }
        ApplyEffects(fx);
        return;
    }
    if (type == recReportName) {
        proto::RecoveryReport m;
        if (m.ParseFromArray(data.data(), data.size())) {
            vc_->OnHeard(m.sender_id(), now);
            vc_->OnRecoveryReport(m, &fx);
        }
        ApplyEffects(fx);
        return;
    }
    if (type == startViewName) {
        proto::StartView m;
        if (m.ParseFromArray(data.data(), data.size())) vc_->OnStartView(m, now, &fx);
        ApplyEffects(fx);
        return;
    }

    // --- rejoin ---
    if (type == joinReqName) {
        proto::JoinRequest m;
        if (m.ParseFromArray(data.data(), data.size())) vc_->OnJoinRequest(m, now, &fx);
        ApplyEffects(fx);
        return;
    }
    if (type == joinInfoName) {
        proto::JoinInfo m;
        if (m.ParseFromArray(data.data(), data.size())) vc_->OnJoinInfo(m, now, &fx);
        ApplyEffects(fx);
        return;
    }
    if (type == joinReadyName) {
        proto::JoinReady m;
        if (m.ParseFromArray(data.data(), data.size())) vc_->OnJoinReady(m, now, &fx);
        ApplyEffects(fx);
        return;
    }
    if (type == snapReqName) {
        proto::SnapshotRequest m;
        if (m.ParseFromArray(data.data(), data.size())) HandleSnapshotRequest(remote, m);
        return;
    }
    if (type == snapChunkName) {
        proto::SnapshotChunk m;
        if (m.ParseFromArray(data.data(), data.size())) HandleSnapshotChunk(m);
        return;
    }

    // Hot messages: parse, then process. With the ordering thread on (DYNA_ORDER_THREAD)
    // ReceiveMessage never gets here for hot types: it parses on the receive thread and
    // posts ProcessHot to the ordering thread instead.
    const RxType kind = ClassifyRx(type);
    if (IsHot(kind)) {
        if (auto m = ParseHot(kind, data)) ProcessHot(kind, remote, *m);
    }
}

std::unique_ptr<google::protobuf::Message>
DynaReplica::ParseHot(RxType kind, std::string_view data)
{
    std::unique_ptr<google::protobuf::Message> m;
    switch (kind) {
    case kRxRequest:   m = std::make_unique<proto::ClientRequest>(); break;
    case kRxFast:      m = std::make_unique<proto::FastCommit>(); break;
    case kRxSlow:      m = std::make_unique<proto::SlowCommit>(); break;
    case kRxPrep:      m = std::make_unique<proto::ReplicatePrepare>(); break;
    case kRxPrepReply: m = std::make_unique<proto::ReplicatePrepareReply>(); break;
    case kRxCommit:    m = std::make_unique<proto::ReplicateCommit>(); break;
    case kRxTsAgree:   m = std::make_unique<proto::TsAgree>(); break;
    case kRxFetch:     m = std::make_unique<proto::FetchRequest>(); break;
    case kRxFetchRep:  m = std::make_unique<proto::FetchReply>(); break;
    case kRxBg:        m = std::make_unique<proto::BgBatch>(); break;
    case kRxFastBatch: m = std::make_unique<proto::FastCommitBatch>(); break;
    default:           return nullptr;
    }
    if (!m->ParseFromArray(data.data(), static_cast<int>(data.size()))) return nullptr;
    return m;
}

bool
DynaReplica::HotTxnId(RxType kind, const google::protobuf::Message &msg, TxnId *id)
{
    switch (kind) {
#define DYNA_TXNID_OF(K, T) \
    case K: { const auto &m = static_cast<const T &>(msg); \
              *id = TxnId{m.client_id(), m.request_id()}; return true; }
    DYNA_TXNID_OF(kRxFast, proto::FastCommit)
    DYNA_TXNID_OF(kRxSlow, proto::SlowCommit)
    DYNA_TXNID_OF(kRxPrep, proto::ReplicatePrepare)
    DYNA_TXNID_OF(kRxPrepReply, proto::ReplicatePrepareReply)
    DYNA_TXNID_OF(kRxCommit, proto::ReplicateCommit)
    DYNA_TXNID_OF(kRxTsAgree, proto::TsAgree)
    DYNA_TXNID_OF(kRxFetch, proto::FetchRequest)
#undef DYNA_TXNID_OF
    case kRxRequest: {
        const auto &t = static_cast<const proto::ClientRequest &>(msg).txn();
        *id = TxnId{t.client_id(), t.request_id()};
        return true;
    }
    case kRxFetchRep: {
        const auto &t = static_cast<const proto::FetchReply &>(msg).txn();
        *id = TxnId{t.client_id(), t.request_id()};
        return true;
    }
    default:
        return false;
    }
}

void
DynaReplica::PostToLane(size_t lane, std::function<void()> fn)
{
    lanes_[lane]->q.enqueue(std::move(fn));
}

void
DynaReplica::ProcessHot(RxType kind, const TransportAddress &remote,
                        const google::protobuf::Message &msg)
{
    if (kind == kRxBg) {
        HandleBgBatch(static_cast<const proto::BgBatch &>(msg));
        return;
    }

    // A JOINING server that adopted the current view applies live replication traffic
    // (without acking: it is not a member yet), so nothing committed while it catches up
    // falls between the snapshot and the view change that re-adds it.
    if (vc_->phase() == ViewCoordinator::Phase::JOINING && vc_->has_view()) {
        const uint64_t jv = viewId_.load();
        if (kind == kRxPrep) {
            const auto &m = static_cast<const proto::ReplicatePrepare &>(msg);
            if (m.view_id() == jv && m.has_txn()) {
                orderer_->HandleReplicatePrepare(TxnId{m.client_id(), m.request_id()},
                                                 m.agreed_t(), m.txn());
            }
        } else if (kind == kRxCommit) {
            const auto &m = static_cast<const proto::ReplicateCommit &>(msg);
            if (m.view_id() == jv) orderer_->MarkCommitted(TxnId{m.client_id(), m.request_id()});
        }
        return;
    }

    // --- normal processing: paused outside NORMAL; other-view messages are dropped
    //     (clients on an old view are told the current one) ---
    if (!Normal()) return;
    const uint64_t view = viewId_.load();

    switch (kind) {
    case kRxRequest: {
        const auto &m = static_cast<const proto::ClientRequest &>(msg);
        if (m.txn().view_id() != view) {
            if (m.txn().view_id() < view) SendViewInfo(remote);
            return;
        }
        HandleClientRequest(remote, m);
        return;
    }
    case kRxFast: {
        const auto &m = static_cast<const proto::FastCommit &>(msg);
        if (m.view_id() != view) {
            // An old-view decision is not adopted: the new view re-derives the txn's t
            // (recovery + leader sync), so a stale agreedT could diverge from it.
            if (m.view_id() < view) SendViewInfo(remote);
            return;
        }
        HandleCommit(remote, TxnId{m.client_id(), m.request_id()}, m.agreed_t(),
                     /*slow=*/false);
        return;
    }
    case kRxFastBatch: {
        bool told = false;   // one ViewInfo per batch is enough
        for (const auto &m : static_cast<const proto::FastCommitBatch &>(msg).commits()) {
            if (m.view_id() != view) {
                if (m.view_id() < view && !told) { SendViewInfo(remote); told = true; }
                continue;
            }
            HandleCommit(remote, TxnId{m.client_id(), m.request_id()}, m.agreed_t(),
                         /*slow=*/false);
        }
        return;
    }
    case kRxSlow: {
        const auto &m = static_cast<const proto::SlowCommit &>(msg);
        if (m.view_id() != view) {
            if (m.view_id() < view) SendViewInfo(remote);
            return;
        }
        HandleCommit(remote, TxnId{m.client_id(), m.request_id()}, m.agreed_t(),
                     /*slow=*/true);
        return;
    }
    case kRxFetch: {
        const auto &m = static_cast<const proto::FetchRequest &>(msg);
        if (m.view_id() == view) HandleFetchRequest(remote, m);
        return;
    }
    case kRxFetchRep:
        HandleFetchReply(static_cast<const proto::FetchReply &>(msg));
        return;
    case kRxTsAgree: {
        const auto &m = static_cast<const proto::TsAgree &>(msg);
        if (m.view_id() == view) HandleTsAgree(m);
        return;
    }
    case kRxPrep: {
        const auto &m = static_cast<const proto::ReplicatePrepare &>(msg);
        if (m.view_id() == view) HandleReplicatePrepare(&remote, m);
        return;
    }
    case kRxPrepReply: {
        const auto &m = static_cast<const proto::ReplicatePrepareReply &>(msg);
        if (m.view_id() == view) HandleReplicatePrepareReply(m);
        return;
    }
    case kRxCommit: {
        const auto &m = static_cast<const proto::ReplicateCommit &>(msg);
        if (m.view_id() == view) HandleReplicateCommit(m);
        return;
    }
    default:
        return;
    }
}

// --- ordering thread (DYNA_ORDER_THREAD) ---------------------------------------------

void
DynaReplica::SendLoop(Sender *snd, size_t idx)
{
    char name[16];
    snprintf(name, sizeof(name), "dyna-send%zu", idx);
    pthread_setname_np(pthread_self(), name);
    PinSelfFromEnv("DYNA_PIN_SEND", idx);
    constexpr size_t kBulk = 64;
    std::function<void()> batch[kBulk];
    ReplyJob jobs[kBulk];
    // DYNA_REPLY_COMBINE (default 1): replies already queued for the same client leave
    // as one SpecReplyBatch. Nothing waits for a batch to fill: batches form only when
    // the sender has a backlog, i.e. exactly when per-packet cost matters.
    const bool combine = DeadlineConfig::EnvU64("DYNA_REPLY_COMBINE", 1) != 0;
    proto::SpecReply r;
    r.set_server_id(static_cast<uint32_t>(replicaIdx));
    proto::SpecReplyBatch rbatch;
    bool grouped[kBulk];
    auto fill = [&](proto::SpecReply &out, const ReplyJob &j) {
        out.set_server_id(static_cast<uint32_t>(replicaIdx));
        out.set_client_id(j.client_id);
        out.set_request_id(j.request_id);
        out.set_view_id(j.view_id);
        out.set_t(j.t);
        out.set_is_leader(j.is_leader);
        out.set_owd_estimate(j.owd);
    };
    while (send_running_.load(std::memory_order_relaxed)) {
        const uint64_t s0 = stats_on_ ? WallClockMicros() : 0;
        const size_t nr = snd->replies.try_dequeue_bulk(jobs, kBulk);
        for (size_t i = 0; i < nr; ++i) grouped[i] = false;
        for (size_t i = 0; i < nr; ++i) {
            if (grouped[i]) continue;
            ReplyJob &j = jobs[i];
            size_t same = 0;
            if (combine) {
                for (size_t k = i + 1; k < nr; ++k) same += (jobs[k].client_id == j.client_id);
            }
            if (same == 0) {
                fill(r, j);
                transport->SendMessage(this, *j.addr, r);
                if (stats_on_) reply_pkts_.Add();
                continue;
            }
            rbatch.Clear();
            for (size_t k = i; k < nr; ++k) {
                if (jobs[k].client_id != j.client_id) continue;
                fill(*rbatch.add_replies(), jobs[k]);
                grouped[k] = true;
            }
            transport->SendMessage(this, *j.addr, rbatch);
            if (stats_on_) reply_pkts_.Add();
        }
        for (size_t i = 0; i < nr; ++i) jobs[i].addr.reset();
        const size_t n = snd->q.try_dequeue_bulk(batch, kBulk);
        for (size_t i = 0; i < n; ++i) {
            batch[i]();
            batch[i] = nullptr;
        }
        if (n == 0 && nr == 0) {
#if defined(__x86_64__) || defined(__i386__)
            _mm_pause();
#endif
        } else if (stats_on_) {
            snd->busy_us.Add(WallClockMicros() - s0);
        }
    }
}

void
DynaReplica::OrderingLoop(Lane *lane)
{
    char name[16];
    snprintf(name, sizeof(name), "dyna-order%zu", lane->idx);
    pthread_setname_np(pthread_self(), name);
    PinSelfFromEnv("DYNA_PIN_ORDER", lane->idx);
    constexpr size_t kBulk = 64;
    std::function<void()> batch[kBulk];
    uint64_t last_release_us = 0;
    while (order_running_.load(std::memory_order_relaxed)) {
        const size_t n = lane->q.try_dequeue_bulk(batch, kBulk);
        const uint64_t t0 = (stats_on_ && n) ? WallClockMicros() : 0;
        if (n > 0) {
            // Same exclusion as the receive threads' hot path: VR / recovery work takes
            // state_lock_ exclusively, so it never interleaves with a hot handler.
            std::shared_lock<RwLock> g(state_lock_);
            for (size_t i = 0; i < n; ++i) {
                const uint64_t h0 = stats_on_ ? WallClockMicros() : 0;
                batch[i]();
                batch[i] = nullptr;
                if (stats_on_) lane->hmax.Observe(WallClockMicros() - h0);
            }
        }
        // Releases run here too (the orderer has no release thread in this mode). The
        // hint makes an idle iteration lock-free; the 100 µs backstop covers deadlines
        // that entered without SubmitTxn (none today) and resumes after a pause.
        const uint64_t now = WallClockMicros();
        if (!orderer_->paused() &&
            (now >= orderer_->next_due_hint(lane->idx) || now >= last_release_us + 100)) {
            last_release_us = now;
            const size_t released = orderer_->ReleaseDue(lane->idx);
            if (stats_on_ && released) {
                lane->busy_us.Add(WallClockMicros() - now);
                lane->hmax.Observe(WallClockMicros() - now);
                lane->released.Add(released);
            }
        }
        if (stats_on_) {
            if (n) {
                lane->busy_us.Add(WallClockMicros() - t0);
                lane->msgs.Add(n);
            }
            lane->qmax.Observe(lane->q.size_approx());
        }
        if (n == 0) {
#if defined(__x86_64__) || defined(__i386__)
            _mm_pause();
#endif
        }
    }
}

void
DynaReplica::HandleReplicatePrepare(const TransportAddress *remote,
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
    BgPrepareReply(m.leader_id(), reply, remote);   // ack to the primary leader
}

void
DynaReplica::HandleReplicatePrepareReply(const proto::ReplicatePrepareReply &m)
{
    const TxnId id{m.client_id(), m.request_id()};
    const ServerOrderer::PrepareReplyResult r = orderer_->RecordPrepareReply(id, m.server_id());
    if (r == ServerOrderer::PrepareReplyResult::kNone) return;   // not yet f acks
    proto::ReplicateCommit rc;
    rc.set_client_id(id.clientid);
    rc.set_request_id(id.clientreqid);
    rc.set_view_id(viewId_);
    rc.set_leader_id(static_cast<uint32_t>(replicaIdx));
    if (r == ServerOrderer::PrepareReplyResult::kNowCommitted) {
        BgCommit(kBgAll, rc);   // reached f acks → COMMITTED; tell everyone
    } else {
        // kResendCommit: this server re-acked because it missed our ReplicateCommit.
        BgCommit(static_cast<int32_t>(m.server_id()), rc);
    }
}

void
DynaReplica::HandleReplicateCommit(const proto::ReplicateCommit &m)
{
    orderer_->MarkCommitted(TxnId{m.client_id(), m.request_id()});
}

void
DynaReplica::SendReplicatePrepare(const ServerOrderer::ReplicateAction &a)
{
    proto::ReplicatePrepare m;
    m.set_client_id(a.id.clientid);
    m.set_request_id(a.id.clientreqid);
    m.set_view_id(viewId_);
    m.set_leader_id(static_cast<uint32_t>(replicaIdx));
    m.set_agreed_t(a.agreed_t);
    *m.mutable_txn() = a.body;
    BgPrepare(m);   // to all other servers
}

void
DynaReplica::HandleTsAgree(const proto::TsAgree &m)
{
    const TxnId id{m.client_id(), m.request_id()};
    ServerOrderer::TsRecordResult r =
        orderer_->RecordTsProposal(id, m.sender_id(), m.proposed_t(), m.reply());
    if (r.has_action) SendTsAgree(r.action);
    if (r.transitioned) {
        LeaderSyncDone(r.entry);
        ReplicateNowIfPrimary(id);   // COMMITTING → start replication now
    }
}

void
DynaReplica::RunTsAgreement()
{
    std::vector<ServerOrderer::TsAgreeAction> actions;
    std::vector<TxnEntry> committed;
    orderer_->DriveTsAgreement(ts_timeout_us_, &actions, &committed);
    for (const auto &a : actions) SendTsAgree(a);
    for (const TxnEntry &e : committed) {
        LeaderSyncDone(e);
        ReplicateNowIfPrimary(e.Key());
    }
}

void
DynaReplica::LeaderSyncDone(const TxnEntry &e)
{
    if (e.txn.type() != proto::WRITE) return;
    std::shared_ptr<TransportAddress> addr = e.client_addr;
    // The txn's own lane first; any lane that heard from this client otherwise.
    for (size_t k = 0; !addr && k < lanes_.size(); ++k) {
        Lane &l = *lanes_[(orderer_->LaneOf(e.Key()) + k) % lanes_.size()];
        std::lock_guard<HybridMutex> lk(l.dedup_mu);
        auto it = l.client_addrs.find(e.txn.client_id());
        if (it != l.client_addrs.end()) addr = it->second;
    }
    if (!addr) return;   // never heard from this client: its own resend reaches the cache
    if (e.txn.view_id() < viewId_) SendViewInfo(*addr);
    ServerOrderer::CommitBrief c;
    c.t = e.t;
    c.status = e.status;
    c.is_leader = e.is_leader_for_this;
    SendCommitReply(*addr, e.Key(), c);
    if (stats_on_) sync_reply_tx_.Add();
}

void
DynaReplica::TsAgreementTick()
{
    std::shared_lock<RwLock> g(state_lock_);   // orderer + sends only, like hot messages
    if (!Normal()) {   // paused during a view change
        transport->Timer(ts_scan_ms_, [this]() { TsAgreementTick(); });
        return;
    }
    const uint64_t scan_t0 = stats_on_ ? WallClockMicros() : 0;
    RunTsAgreement();

    // Replication (spec §3.6): drive COMMITTING → COMMITTED for txns we lead. The first
    // prepare normally goes out immediately (ReplicateNowIfPrimary); this scan covers
    // txns that skipped that path and re-broadcasts lost prepares, throttled to once per
    // ts_timeout_us_.
    std::vector<ServerOrderer::ReplicateAction> prepares;
    orderer_->DriveReplication(&prepares, ts_timeout_us_);
    for (const auto &p : prepares) SendReplicatePrepare(p);

    // A COMMITTING txn whose ReplicateCommit never arrived: re-ack to its primary, which
    // answers with ReplicateCommit (so a lost commit can't block execution forever).
    std::vector<ServerOrderer::ReackAction> reacks;
    orderer_->CollectStaleCommitting(ts_timeout_us_, &reacks);
    if (stats_on_) scan_us_.Observe(WallClockMicros() - scan_t0);
    for (const auto &a : reacks) {
        proto::ReplicatePrepareReply reply;
        reply.set_client_id(a.id.clientid);
        reply.set_request_id(a.id.clientreqid);
        reply.set_view_id(viewId_);
        reply.set_server_id(static_cast<uint32_t>(replicaIdx));
        BgPrepareReply(a.primary, reply, nullptr);
    }

    // Execution (spec §3.7) runs on the orderer's executor thread, woken on commit /
    // leader-read events; kick it here too as a backstop.
    orderer_->KickExecutor();

    if (debug_) {
        const size_t executed = orderer_->executed_count();
        if (executed != last_logged_executed_) {
            last_logged_executed_ = executed;
            const size_t spec = orderer_->speclog_size();
            Notice("r%d progress: executed_writes=%zu speclog=%zu late=%zu%s%s", replicaIdx,
                   executed, spec, orderer_->late_size(), spec > 4 ? " oldest: " : "",
                   spec > 4 ? orderer_->DebugOldestEntry().c_str() : "");
        }
    }

    transport->Timer(ts_scan_ms_, [this]() { TsAgreementTick(); });   // re-arm
}

// --- background batching -------------------------------------------------------------

void
DynaReplica::BgPrepare(const proto::ReplicatePrepare &m)
{
    if (!bg_on_) {
        Tx()->SendMessageToAll(this, m);   // (the self copy is ignored on receipt)
        return;
    }
    Lane &l = LaneFor(TxnId{m.client_id(), m.request_id()});
    for (int r = 0; r < config.n; ++r) {
        if (r != replicaIdx && l.bg->AddPrepare(static_cast<uint32_t>(r), m)) {
            FlushBgTo(l.idx, static_cast<uint32_t>(r));
        }
    }
}

void
DynaReplica::BgPrepareReply(uint32_t to, const proto::ReplicatePrepareReply &m,
                            const TransportAddress *remote)
{
    if (!bg_on_) {
        if (remote) Tx()->SendMessage(this, *remote, m);
        else Tx()->SendMessageToReplica(this, static_cast<int>(to), m);
        return;
    }
    if (static_cast<int>(to) == replicaIdx) return;
    Lane &l = LaneFor(TxnId{m.client_id(), m.request_id()});
    if (l.bg->AddPrepareReply(to, m)) FlushBgTo(l.idx, to);
}

void
DynaReplica::BgCommit(int32_t to, const proto::ReplicateCommit &m)
{
    if (!bg_on_) {
        if (to == kBgAll) Tx()->SendMessageToAll(this, m);
        else Tx()->SendMessageToReplica(this, to, m);
        return;
    }
    Lane &l = LaneFor(TxnId{m.client_id(), m.request_id()});
    for (int r = 0; r < config.n; ++r) {
        if (r == replicaIdx || (to != kBgAll && r != to)) continue;
        if (l.bg->AddCommit(static_cast<uint32_t>(r), m)) FlushBgTo(l.idx, static_cast<uint32_t>(r));
    }
}

void
DynaReplica::BgTsAgree(uint32_t to, const proto::TsAgree &m)
{
    if (!bg_on_) {
        Tx()->SendMessageToReplica(this, static_cast<int>(to), m);
        return;
    }
    if (static_cast<int>(to) == replicaIdx) return;
    Lane &l = LaneFor(TxnId{m.client_id(), m.request_id()});
    if (l.bg->AddTsAgree(to, m)) FlushBgTo(l.idx, to);
}

void
DynaReplica::FlushBgTo(size_t lane, uint32_t to)
{
    Lane &l = *lanes_[lane];
    auto b = std::make_shared<proto::BgBatch>();
    size_t entries = 0;
    if (!l.bg || !l.bg->Take(to, b.get(), &entries)) return;
    Sender *snd = SenderFor(l);
    if (snd != nullptr && send_running_.load(std::memory_order_relaxed)) {
        snd->q.enqueue([this, to, b, entries]() { SendBgBatch(to, *b, entries); });
    } else {
        SendBgBatch(to, *b, entries);
    }
}

void
DynaReplica::SendBgBatch(uint32_t to, proto::BgBatch &b, size_t entries)
{
    b.set_sender_id(static_cast<uint32_t>(replicaIdx));
    Tx()->SendMessageToReplica(this, static_cast<int>(to), b);
    if (stats_on_) bg_tx_entries_.Observe(entries);
}

void
DynaReplica::BgFlushTick()
{
    for (auto &l : lanes_) {
        for (auto &kv : l->bg->TakeAll()) {
            proto::BgBatch &b = kv.second;
            SendBgBatch(kv.first, b, static_cast<size_t>(b.prepares_size() +
                                                          b.prepare_replies_size() +
                                                          b.commits_size() + b.ts_agrees_size()));
        }
    }
    transport->TimerUS(bg_flush_us_, [this]() { BgFlushTick(); });   // re-arm
}

void
DynaReplica::HandleBgBatch(const proto::BgBatch &m)
{
    if (stats_on_) {
        bg_rx_entries_.Observe(static_cast<uint64_t>(m.prepares_size() +
                                                     m.prepare_replies_size() +
                                                     m.commits_size() + m.ts_agrees_size()));
    }
    // JOINING non-member: apply live replication (unacked), exactly like the single-
    // message JOINING path in ReceiveMessageImpl.
    if (vc_->phase() == ViewCoordinator::Phase::JOINING && vc_->has_view()) {
        const uint64_t jv = viewId_.load();
        for (const auto &p : m.prepares()) {
            if (p.view_id() == jv && p.has_txn()) {
                orderer_->HandleReplicatePrepare(TxnId{p.client_id(), p.request_id()},
                                                 p.agreed_t(), p.txn());
            }
        }
        for (const auto &c : m.commits()) {
            if (c.view_id() == jv) orderer_->MarkCommitted(TxnId{c.client_id(), c.request_id()});
        }
        return;
    }
    if (!Normal()) return;
    const uint64_t view = viewId_.load();
    // Prepares before commits: a commit in this batch may be for a prepare in it.
    for (const auto &p : m.prepares()) {
        if (p.view_id() == view) HandleReplicatePrepare(nullptr, p);
    }
    for (const auto &r : m.prepare_replies()) {
        if (r.view_id() == view) HandleReplicatePrepareReply(r);
    }
    for (const auto &c : m.commits()) {
        if (c.view_id() == view) HandleReplicateCommit(c);
    }
    for (const auto &t : m.ts_agrees()) {
        if (t.view_id() == view) HandleTsAgree(t);
    }
}

void
DynaReplica::SendTsAgree(const ServerOrderer::TsAgreeAction &action)
{
    for (uint32_t leader : action.to_leaders) {
        proto::TsAgree m;
        m.set_client_id(action.id.clientid);
        m.set_request_id(action.id.clientreqid);
        m.set_view_id(viewId_);
        m.set_sender_id(static_cast<uint32_t>(replicaIdx));
        m.set_proposed_t(action.proposed_t);
        if (action.is_reply) m.set_reply(true);
        BgTsAgree(leader, m);
    }
}

void
DynaReplica::HandleClientRequest(const TransportAddress &remote,
                                      const proto::ClientRequest &m)
{
    SubmitRequestEntry(BuildRequestEntry(std::shared_ptr<TransportAddress>(remote.clone()),
                                         proto::Txn(m.txn())));
}

TxnEntry
DynaReplica::BuildRequestEntry(std::shared_ptr<TransportAddress> addr, proto::Txn &&txn)
{
    TxnEntry e;
    e.txn = std::move(txn);
    e.t = e.txn.t();
    e.client_addr = std::move(addr);
    return e;
}

void
DynaReplica::ProcessRequestEntry(TxnEntry &&e)
{
    // Same gating as ProcessHot's kRxRequest case (a JOINING server ignores requests).
    if (vc_->phase() == ViewCoordinator::Phase::JOINING && vc_->has_view()) return;
    if (!Normal()) return;
    const uint64_t view = viewId_.load();
    if (e.txn.view_id() != view) {
        if (e.txn.view_id() < view && e.client_addr) SendViewInfo(*e.client_addr);
        return;
    }
    SubmitRequestEntry(std::move(e));
}

void
DynaReplica::SubmitRequestEntry(TxnEntry &&e)
{
    const TxnId id = e.Key();
    // OWD sample (§2): one-way delay = arrival − send_time (client stamp), taken HERE —
    // where the orderer judges lateness — so the learned bound includes any queueing on
    // the way to the ordering thread. (Sampling on the receive thread instead left ~36%
    // of arrivals late at 35k ops/s.)
    Lane &L = LaneFor(id);
    const uint64_t arrival = WallClockMicros();
    if (stats_on_ && e.arrival_us != 0 && arrival >= e.arrival_us) {
        owd_queue_hist_.Observe(arrival - e.arrival_us);
    }
    if (e.txn.has_send_time()) {
        std::lock_guard<HybridMutex> lk(L.owd_mu);
        if (arrival > e.txn.send_time()) {
            L.owd->sample(e.txn.client_id(), arrival - e.txn.send_time());
            if (stats_on_) owd_hist_.Observe(arrival - e.txn.send_time());
        }
        e.owd_estimate_us = L.owd->estimate_us(e.txn.client_id());
        if (stats_on_) owd_est_hist_.Observe(e.owd_estimate_us);
    }
    e.arrival_us = arrival;
    if (debug_) {
        Notice("r%d RECV req c=%lu r=%lu t=%lu now=%lu wset=%d", replicaIdx,
               (unsigned long)id.clientid, (unsigned long)id.clientreqid,
               (unsigned long)e.t, (unsigned long)arrival, e.txn.w_set_size());
    }
    // At-most-once for WRITEs (spec §1.4/§5): answer a retry from cache; drop one still
    // in flight so the orderer never holds two entries for the same txnId. READs skip
    // this: a read retry is a new attempt (fresh request_id, larger t), and a cached
    // reply would carry the old attempt's t.
    if (e.txn.type() != proto::READ) {
        std::lock_guard<HybridMutex> lk(L.dedup_mu);
        if (e.client_addr) L.client_addrs.try_emplace(id.clientid, e.client_addr);
        auto cit = L.reply_cache.find(id);
        if (cit != L.reply_cache.end()) {
            proto::SpecReply cached;
            cached.set_client_id(id.clientid);
            cached.set_request_id(id.clientreqid);
            cached.set_server_id(static_cast<uint32_t>(replicaIdx));
            cached.set_t(cit->second.t);
            cached.set_owd_estimate(cit->second.owd);
            // Answer with the txn's CURRENT state: after a commit or a view change its t
            // and our leadership may differ from the cached release.
            uint64_t live_t = 0;
            bool live_leader = false;
            if (orderer_->LiveState(id, &live_t, &live_leader)) cached.set_t(live_t);
            // Leadership in the CURRENT view (the cached / executed flag may predate a
            // view change).
            bool leads = false;
            for (const auto &kv : e.txn.w_set()) {
                if (topo_.ownerOf(kv.key()) == static_cast<uint32_t>(replicaIdx)) leads = true;
            }
            cached.set_is_leader(leads);
            cached.set_view_id(viewId_);
            // (unlock happens at scope exit; send after is fine — retry window is ms)
            if (e.client_addr) Tx()->SendMessage(this, *e.client_addr, cached);
            return;
        }
        if (L.inflight.find(id) != L.inflight.end()) return;   // in flight -> drop retry
        L.inflight.insert(id);
    }
    // Poke the orderer's release thread only if it has one (the ordering thread polls).
    orderer_->SubmitTxn(std::move(e), /*poke=*/!order_thread_on_);
}

proto::SpecReply
DynaReplica::BuildSpecReply(const TxnEntry &e) const
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
DynaReplica::OnSpecReply(const TxnEntry &e)
{
    if (debug_) {
        Notice("r%d SEND SpecReply c=%lu r=%lu t=%lu leader=%d addr=%d", replicaIdx,
               (unsigned long)e.txn.client_id(), (unsigned long)e.txn.request_id(),
               (unsigned long)e.t, e.is_leader_for_this ? 1 : 0, e.client_addr ? 1 : 0);
    }
    const bool is_read = (e.txn.type() == proto::READ);
    Lane &L = LaneFor(e.Key());
    Sender *snd = SenderFor(L);
    if (e.client_addr) {
        if (!is_read && snd != nullptr && send_running_.load(std::memory_order_relaxed)) {
            // Writes carry no results: hand the sender a plain struct to build + send.
            ReplyJob j;
            j.addr = e.client_addr;
            j.client_id = e.txn.client_id();
            j.request_id = e.txn.request_id();
            j.view_id = viewId_;
            j.t = e.t;
            j.owd = e.owd_estimate_us;
            j.is_leader = e.is_leader_for_this;
            snd->replies.enqueue(std::move(j));
        } else {
            Tx()->SendMessage(this, *e.client_addr, BuildSpecReply(e));
        }
    }
    if (stats_on_) spec_tx_.Add();
    if (is_read) return;   // reads aren't deduped (see SubmitRequestEntry)
    // Cache for at-most-once AFTER the send (off the release critical path).
    std::lock_guard<HybridMutex> lk(L.dedup_mu);
    L.reply_cache[e.Key()] = CachedSpec{e.t, e.owd_estimate_us};
    L.inflight.erase(e.Key());
}

void
DynaReplica::ReplicateNowIfPrimary(const TxnId &id)
{
    ServerOrderer::ReplicateAction a;
    if (orderer_->TakeReplicateNow(id, &a)) SendReplicatePrepare(a);
}

void
DynaReplica::FinishCommit(const TxnId &id, const ServerOrderer::CommitBrief &out,
                          ServerOrderer::CommitResult r, bool slow,
                          const TransportAddress *client)
{
    {
        Lane &L = LaneFor(id);
        std::lock_guard<HybridMutex> lk(L.dedup_mu);
        L.inflight.erase(id);
    }
    // SlowCommit: the client is re-checking the quorum at agreedT (or, if we already
    // executed the txn, at its final t).
    if (slow && client) SendCommitReply(*client, id, out);
    if (r == ServerOrderer::CommitResult::kApplied) ReplicateNowIfPrimary(id);
}

void
DynaReplica::HandleCommit(const TransportAddress &remote, const TxnId &id,
                               uint64_t agreedT, bool slow)
{
    ServerOrderer::CommitBrief out;
    ServerOrderer::CommitResult r = orderer_->ApplyCommitBrief(id, agreedT, &out);
    if (r != ServerOrderer::CommitResult::kNeedFetch) {
        FinishCommit(id, out, r, slow, &remote);
        return;
    }

    // kNeedFetch: remember the commit, then broadcast a FetchRequest for the body.
    {
        std::lock_guard<HybridMutex> lk(pending_mu_);
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
    Tx()->SendMessageToAll(this, fr);   // whoever holds the body replies
}

void
DynaReplica::HandleFetchRequest(const TransportAddress &remote,
                                     const proto::FetchRequest &m)
{
    const TxnId id{m.client_id(), m.request_id()};
    proto::Txn body;
    if (!orderer_->TryGetBody(id, &body)) return;   // we don't have it
    proto::FetchReply reply;
    *reply.mutable_txn() = body;
    Tx()->SendMessage(this, remote, reply);
}

void
DynaReplica::HandleFetchReply(const proto::FetchReply &m)
{
    const proto::Txn &body = m.txn();
    orderer_->InstallFetched(body);
    const TxnId id{body.client_id(), body.request_id()};

    PendingCommit pc;
    bool have = false;
    {
        std::lock_guard<HybridMutex> lk(pending_mu_);
        auto it = pending_commits_.find(id);
        if (it != pending_commits_.end()) {
            pc = it->second;
            pending_commits_.erase(it);
            have = true;
        }
    }
    if (!have) return;

    ServerOrderer::CommitBrief out;
    ServerOrderer::CommitResult r = orderer_->ApplyCommitBrief(id, pc.agreedT, &out);
    if (r != ServerOrderer::CommitResult::kNeedFetch) {
        FinishCommit(id, out, r, pc.slow, pc.client_addr.get());
    }
}

void
DynaReplica::SendCommitReply(const TransportAddress &client, const TxnId &id,
                             const ServerOrderer::CommitBrief &c)
{
    proto::CommitReply r;
    r.set_client_id(id.clientid);
    r.set_request_id(id.clientreqid);
    r.set_view_id(viewId_);
    r.set_server_id(static_cast<uint32_t>(replicaIdx));
    r.set_t(c.t);
    r.set_is_leader(c.is_leader);
    Tx()->SendMessage(this, client, r);
}

// ---------------------------------------------------------------------------------------
// Views + recovery
// ---------------------------------------------------------------------------------------

namespace {
std::string FmtDist(StatDist &d)
{
    const uint64_t n = d.n.Take(), sum = d.sum.Take(), mx = d.max.Take();
    char buf[64];
    snprintf(buf, sizeof(buf), "%lu/%lu/n%lu", (unsigned long)(n ? sum / n : 0),
             (unsigned long)mx, (unsigned long)n);
    return buf;
}
} // namespace

void
DynaReplica::MaybeDumpStats(uint64_t now_us)
{
    if (last_stats_us_ == 0) { last_stats_us_ = now_us; return; }
    const uint64_t elapsed = now_us - last_stats_us_;
    if (elapsed < 1000000) return;
    last_stats_us_ = now_us;
    uint64_t rx[kRxN], rxus[kRxN];
    for (int i = 0; i < kRxN; ++i) {
        rx[i] = rx_[i].Take();
        rxus[i] = rx_us_[i].Take();
    }
    // Average handler µs per message (x10 for one decimal) for the hot types.
    auto per = [&](int k) { return (unsigned long)(rx[k] ? rxus[k] * 10 / rx[k] : 0); };
    // Per-lane ordering-thread busy% and the max queue depth over lanes.
    std::string lane_busy;
    uint64_t qmax = 0;
    for (auto &l : lanes_) {
        if (!lane_busy.empty()) lane_busy += ",";
        const uint64_t lq = l->qmax.Take();
        lane_busy += std::to_string(l->busy_us.Take() * 100 / elapsed) + "%/" +
                     std::to_string(l->msgs.Take()) + "m/" + std::to_string(l->released.Take()) +
                     "r/q" + std::to_string(lq) + "/h" + std::to_string(l->hmax.Take());
        qmax = std::max<uint64_t>(qmax, lq);
    }
    std::string send_busy;
    for (auto &snd : senders_) {
        if (!send_busy.empty()) send_busy += ",";
        send_busy += std::to_string(snd->busy_us.Take() * 100 / elapsed);
    }
    Notice("r%d owd p50/p95/p99 us: owd=%s net=%s queue=%s estimate=%s", replicaIdx,
           owd_hist_.Take().c_str(), owd_net_hist_.Take().c_str(),
           owd_queue_hist_.Take().c_str(), owd_est_hist_.Take().c_str());
    Notice("r%d cost: handlerUs/msg x10 [req=%lu fast=%lu slow=%lu bg=%lu fetch=%lu] "
           "lanes[busy/msgs/released/qmax/hmaxUs]=[%s] orderQmax=%lu sendBusy%%=[%s] "
           "muHoldUs [submit=%s commit=%s repl=%s release=%s]",
           replicaIdx, per(kRxRequest), per(kRxFast), per(kRxSlow), per(kRxBg),
           per(kRxFetch), lane_busy.c_str(),
           (unsigned long)qmax, send_busy.c_str(), FmtDist(ostats_.hold_submit_us).c_str(),
           FmtDist(ostats_.hold_commit_us).c_str(), FmtDist(ostats_.hold_repl_us).c_str(),
           FmtDist(ostats_.hold_release_us).c_str());
    const uint64_t busy = busy_us_.Take();
    // Format: avg/max/nCOUNT for distributions (µs unless noted); counts are per interval.
    // busy% sums all receive threads (can exceed 100 with DYNA_RX_THREADS > 1).
    Notice("r%d stats: busy=%lu%% hmax=%luus rx[req=%lu fast=%lu fastB=%lu slow=%lu prep=%lu "
           "prepR=%lu commit=%lu ts=%lu fetch=%lu hb=%lu other=%lu] specTx=%lu replyPkts=%lu syncTx=%lu "
           "arrive[on=%lu lateL=%lu lateF=%lu dup=%lu] slack=%s late=%s relLag=%s "
           "execLock=%s execBatch=%s execPass=%s execSort=%s execApply=%s scan=%s bgTx=%s bgRx=%s rxBg=%lu submitWait=%s "
           "muWait=%s dedupWait=%s "
           "pendingWait=%s owdWait=%s hbGap=%lu tickGap=%lu "
           "size[spec=%zu early=%zu late=%zu executed=%zu]",
           replicaIdx, (unsigned long)(busy * 100 / elapsed),
           (unsigned long)handler_max_us_.Take(), (unsigned long)rx[kRxRequest],
           (unsigned long)rx[kRxFast], (unsigned long)rx[kRxFastBatch],
           (unsigned long)rx[kRxSlow], (unsigned long)rx[kRxPrep],
           (unsigned long)rx[kRxPrepReply], (unsigned long)rx[kRxCommit],
           (unsigned long)rx[kRxTsAgree], (unsigned long)(rx[kRxFetch] + rx[kRxFetchRep]),
           (unsigned long)rx[kRxHeartbeat],
           (unsigned long)rx[kRxOther], (unsigned long)spec_tx_.Take(),
           (unsigned long)reply_pkts_.Take(),
           (unsigned long)sync_reply_tx_.Take(),
           (unsigned long)ostats_.on_time.Take(), (unsigned long)ostats_.late_leader.Take(),
           (unsigned long)ostats_.late_follower.Take(), (unsigned long)ostats_.duplicate.Take(),
           FmtDist(ostats_.slack_us).c_str(), FmtDist(ostats_.lateness_us).c_str(),
           FmtDist(ostats_.release_lag_us).c_str(), FmtDist(ostats_.exec_locked_us).c_str(),
           FmtDist(ostats_.exec_batch).c_str(), FmtDist(ostats_.exec_pass_us).c_str(),
           FmtDist(ostats_.exec_sort_us).c_str(), FmtDist(ostats_.exec_apply_us).c_str(),
           FmtDist(scan_us_).c_str(),
           FmtDist(bg_tx_entries_).c_str(), FmtDist(bg_rx_entries_).c_str(),
           (unsigned long)rx[kRxBg],
           FmtDist(ostats_.submit_wait_us).c_str(), FmtDist(ostats_.mu_wait_us).c_str(),
           FmtDist(dedup_wait_us_).c_str(), FmtDist(pending_wait_us_).c_str(),
           FmtDist(owd_wait_us_).c_str(),
           (unsigned long)hb_gap_us_.Take(), (unsigned long)tick_gap_us_.Take(),
           orderer_->speclog_size(), orderer_->early_size(), orderer_->late_size(),
           orderer_->executed_count());
}

void
DynaReplica::HeartbeatTick()
{
    std::unique_lock<RwLock> g(state_lock_);   // drives the VR layer (view changes)
    proto::Heartbeat hb;
    hb.set_view_id(viewId_);
    hb.set_sender_id(static_cast<uint32_t>(replicaIdx));
    Tx()->SendMessageToAll(this, hb);

    ViewCoordinator::Effects fx;
    const uint64_t now = WallClockMicros();
    if (stats_on_) {
        if (last_hb_tick_us_ != 0) tick_gap_us_.Observe(now - last_hb_tick_us_);
        last_hb_tick_us_ = now;
        MaybeDumpStats(now);
    }
    vc_->Tick(now, &fx);
    ApplyEffects(fx);
    // Joiner: re-request a lost snapshot chunk.
    if (catchup_.active && now >= catchup_.last_req_us + 4 * ts_scan_ms_ * 1000) {
        SendSnapshotRequest();
    }
    transport->Timer(hb_ms_, [this]() { HeartbeatTick(); });   // re-arm
}

void
DynaReplica::ApplyEffects(ViewCoordinator::Effects &fx)
{
    for (const auto &o : fx.out) {
        if (o.to == ViewCoordinator::kAll) Tx()->SendMessageToAll(this, *o.msg);
        else Tx()->SendMessageToReplica(this, o.to, *o.msg);
    }
    if (!Normal() && !paused_) {
        paused_ = true;
        orderer_->SetPaused(true);
        Notice("r%d view change: pausing (view %lu -> %lu, alive %#lx -> %#lx)", replicaIdx,
               (unsigned long)viewId_.load(), (unsigned long)vc_->target_view(),
               (unsigned long)vc_->alive(), (unsigned long)vc_->target_alive());
    }
    if (fx.excluded) {
        Warning("r%d left out of a started view: resetting state and rejoining", replicaIdx);
        ResetForRejoin();
    }
    if (fx.adopt_view) AdoptViewAsNonMember();
    if (fx.fresh_start) {
        // Whole-cluster restart: nothing survives to rejoin; start over in view 0.
        Warning("r%d every server is rejoining: starting fresh in view 0", replicaIdx);
        catchup_ = CatchUp();
        topo_ = topo_.WithAlive(vc_->alive());
        orderer_->InstallView(topo_, 0, {}, [](const std::string &) { return false; });
        viewId_ = 0;
        PersistVrState(0, vc_->alive());
        paused_ = false;
        orderer_->SetPaused(false);
    }
    if (fx.begin_recovery) BeginRecovery();
    if (fx.install) InstallNewView();
}

void
DynaReplica::BeginRecovery()
{
    // Our own contribution to the M for our delta-p (the keys of the failed range that
    // consistent hashing now assigns to us).
    const uint32_t self = static_cast<uint32_t>(replicaIdx);
    const Topology old_topo = topo_.WithAlive(vc_->old_alive());
    const Topology new_topo = topo_.WithAlive(vc_->target_alive());
    RecoveryData own = orderer_->CollectRecovery([&](const std::string &k) {
        return InDeltaP(old_topo, new_topo, k, self);
    });
    if (debug_) {
        Notice("r%d recovery view %lu: own delta-p txns=%zu kvs=%zu", replicaIdx,
               (unsigned long)vc_->target_view(), own.txns.size(), own.kvs.size());
    }
    ViewCoordinator::Effects fx;
    vc_->OnLocalRecoveryData(std::move(own), &fx);
    ApplyEffects(fx);
}

void
DynaReplica::HandleRecoveryRequest(const proto::RecoveryRequest &m)
{
    ViewCoordinator::Effects fx;
    const uint64_t now = WallClockMicros();
    vc_->OnHeard(m.requester_id(), now);
    const bool ok = vc_->AcceptRecoveryView(m.view_id(), m.alive(), m.old_alive(), now, &fx);
    ApplyEffects(fx);   // may begin our own recovery (and pause) first
    if (!ok || m.requester_id() == static_cast<uint32_t>(replicaIdx)) return;

    const uint32_t pl = m.requester_id();
    const Topology old_topo = topo_.WithAlive(m.old_alive());
    const Topology new_topo = topo_.WithAlive(m.alive());
    RecoveryData d = orderer_->CollectRecovery([&](const std::string &k) {
        return InDeltaP(old_topo, new_topo, k, pl);
    });
    proto::RecoveryReply r;
    r.set_view_id(m.view_id());
    r.set_sender_id(static_cast<uint32_t>(replicaIdx));
    ToProto(d, r.mutable_txns(), r.mutable_kvs());
    Tx()->SendMessageToReplica(this, pl, r);
}

void
DynaReplica::InstallNewView()
{
    ViewCoordinator::Install in = vc_->TakeInstall(WallClockMicros());
    const Topology old_topo = topo_.WithAlive(in.old_alive);
    const Topology new_topo = topo_.WithAlive(in.alive);
    ServerOrderer::InstallStats st = orderer_->InstallView(
        new_topo, in.view, in.ms,
        [&](const std::string &k) { return InFailedRange(old_topo, new_topo, k); });
    topo_ = new_topo;
    viewId_ = in.view;
    {
        // Resends in the new view must reach the orderer (which dedups by id itself):
        // an id stuck "in flight" here (e.g. late-buffered) would otherwise be dropped.
        for (auto &l : lanes_) {
            std::lock_guard<HybridMutex> lk(l->dedup_mu);
            l->inflight.clear();
        }
    }
    {
        std::lock_guard<HybridMutex> lk(pending_mu_);
        pending_commits_.clear();
    }
    catchup_ = CatchUp();   // a joiner is a member from here on
    PersistVrState(in.view, in.alive);
    Notice("r%d view %lu installed: alive=%#lx Ms=%zu txns_installed=%zu kvs_applied=%zu "
           "leftovers=%zu spec_to_sync=%zu", replicaIdx, (unsigned long)in.view,
           (unsigned long)in.alive, in.ms.size(), st.txns_installed, st.kvs_applied,
           st.leftovers, st.spec_to_sync);
    paused_ = false;
    orderer_->SetPaused(false);
    // Leader-sync inherited SPECULATIVE txns now (single-leader ones complete locally) and
    // tell their clients, instead of waiting out the SPEC age timeout.
    RunTsAgreement();
}

void
DynaReplica::SendViewInfo(const TransportAddress &client)
{
    proto::ViewInfo vi;
    vi.set_view_id(viewId_);
    vi.set_alive(topo_.alive_mask);
    Tx()->SendMessage(this, client, vi);
}

// ---------------------------------------------------------------------------------------
// Rejoin
// ---------------------------------------------------------------------------------------

std::string
DynaReplica::VrStatePath() const
{
    return vr_state_dir_ + "/dyna-vr-" + std::to_string(replicaIdx) + ".state";
}

void
DynaReplica::PersistVrState(uint64_t view, uint64_t alive)
{
    // Rare (once per view): write-temp + fsync + rename, so a crash leaves either the old
    // or the new state.
    const std::string path = VrStatePath();
    const std::string tmp = path + ".tmp";
    const std::string body = std::to_string(view) + " " + std::to_string(alive) + "\n";
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        Warning("r%d cannot write VR state %s", replicaIdx, tmp.c_str());
        return;
    }
    const bool ok = ::write(fd, body.data(), body.size()) == static_cast<ssize_t>(body.size());
    ::fsync(fd);
    ::close(fd);
    if (!ok || std::rename(tmp.c_str(), path.c_str()) != 0) {
        Warning("r%d cannot persist VR state %s", replicaIdx, path.c_str());
    }
}

void
DynaReplica::ResetForRejoin()
{
    orderer_->Reset();
    {
        for (auto &l : lanes_) {
            std::lock_guard<HybridMutex> lk(l->dedup_mu);
            l->reply_cache.clear();
            l->inflight.clear();
        }
    }
    {
        std::lock_guard<HybridMutex> lk(pending_mu_);
        pending_commits_.clear();
    }
    catchup_ = CatchUp();
}

void
DynaReplica::AdoptViewAsNonMember()
{
    topo_ = topo_.WithAlive(vc_->alive());
    viewId_ = vc_->view();
    orderer_->InstallView(topo_, vc_->view(), {}, [](const std::string &) { return false; });
    // (Re)start catch-up from this view's VR leader: a new session re-enumerates its keys
    // (anything already applied is skipped by the t-guard).
    catchup_.active = true;
    catchup_.donor = ViewCoordinator::LeaderOf(vc_->alive());
    catchup_.view = vc_->view();
    catchup_.session += 1;
    catchup_.next_offset = 0;
    catchup_.kvs_applied = 0;
    Notice("r%d rejoining: adopted view %lu (alive=%#lx) as non-member; catching up from r%u",
           replicaIdx, (unsigned long)catchup_.view, (unsigned long)vc_->alive(),
           catchup_.donor);
    SendSnapshotRequest();
}

void
DynaReplica::SendSnapshotRequest()
{
    proto::SnapshotRequest m;
    m.set_view_id(catchup_.view);
    m.set_requester_id(static_cast<uint32_t>(replicaIdx));
    m.set_session(catchup_.session);
    m.set_offset(catchup_.next_offset);
    catchup_.last_req_us = WallClockMicros();
    Tx()->SendMessageToReplica(this, catchup_.donor, m);
}

void
DynaReplica::HandleSnapshotRequest(const TransportAddress &remote,
                                   const proto::SnapshotRequest &m)
{
    if (!Normal() || m.view_id() != viewId_.load()) return;   // it re-syncs its view
    DonorSession &sess = donor_sessions_[m.requester_id()];
    if (sess.session != m.session()) {
        sess.session = m.session();
        sess.keys = orderer_->SnapshotKeys();
    }
    const size_t off = static_cast<size_t>(m.offset());
    RecoveryData d;
    d.kvs = orderer_->SnapshotRead(sess.keys, off, snapshot_chunk_);
    const bool done = off + snapshot_chunk_ >= sess.keys.size();
    if (done) d.txns = orderer_->CommittedTxns();

    proto::SnapshotChunk c;
    c.set_view_id(m.view_id());
    c.set_sender_id(static_cast<uint32_t>(replicaIdx));
    c.set_session(m.session());
    c.set_offset(m.offset());
    c.set_total(sess.keys.size());
    c.set_done(done);
    c.set_next_offset(off + snapshot_chunk_);
    ToProto(d, c.mutable_txns(), c.mutable_kvs());
    Tx()->SendMessage(this, remote, c);
}

void
DynaReplica::HandleSnapshotChunk(const proto::SnapshotChunk &m)
{
    if (!catchup_.active || m.session() != catchup_.session ||
        m.offset() != catchup_.next_offset || m.view_id() != catchup_.view) {
        return;   // stale / duplicate
    }
    RecoveryData d = FromProto(m.txns(), m.kvs());
    catchup_.kvs_applied += orderer_->InstallSnapshot(d);
    catchup_.next_offset = m.next_offset();
    if (!m.done()) {
        SendSnapshotRequest();
        return;
    }
    catchup_.active = false;
    Notice("r%d caught up in view %lu: %lu keys from r%u (%zu applied), %d txns; asking to "
           "rejoin", replicaIdx, (unsigned long)catchup_.view, (unsigned long)m.total(),
           catchup_.donor, catchup_.kvs_applied, m.txns_size());
    ViewCoordinator::Effects fx;
    vc_->OnCaughtUp(&fx);
    ApplyEffects(fx);
}

} // namespace dyna
} // namespace specpaxos

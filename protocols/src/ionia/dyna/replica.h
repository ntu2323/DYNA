// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/replica.h
//   DynaReplica — the per-server DYNA replica. Owns ONE ServerOrderer
//   (single per-server ordering domain; spec §1.4) and drives the write path:
//   ClientRequest → SubmitTxn → (release) → SpecReply; FastCommit/SlowCommit →
//   ApplyCommit (with FetchRequest/FetchReply when the body is missing).
//
//   Threading (busy-poll): receive threads parse hot messages and route each to the
//   LANE of its transaction (ServerOrderer::LaneOf(TxnId); every hot message carries
//   the TxnId). Each lane has its own ordering thread, queue, dedup / reply cache, OWD
//   estimator and background outbox, and owns one orderer shard — so lanes order
//   disjoint transactions in parallel and every step of one transaction runs on one
//   thread. DYNA_LANES=1 is the single ordering thread.

#ifndef _DYNA_REPLICA_H_
#define _DYNA_REPLICA_H_

#include "lib/configuration.h"
#include "lib/transport.h"

#include "dyna/bg_outbox.h"
#include "dyna/deadline.h"
#include "dyna/dyna-proto.pb.h"
#include "dyna/rw_lock.h"
#include "dyna/server_orderer.h"
#include "dyna/stats.h"
#include "dyna/storage_backend.h"
#include "dyna/topology.h"
#include "dyna/txn_types.h"
#include "dyna/view_coordinator.h"
#include "dyna/wall_clock.h"

#include <atomic>
#include <functional>
#include <thread>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <mutex>
#include <unordered_set>

class DB;   // lib/db.h

namespace specpaxos {
namespace dyna {

class DynaReplica : public TransportReceiver
{
public:
    // `db`: the bench harness's key-value store (SplinterDB, or lib's InMemoryDB under
    // IONIA_DB_MODE=memory); executed writes and leader reads go through it. nullptr
    // falls back to a private in-memory store.
    DynaReplica(const specpaxos::Configuration &config,
                int replicaIdx,
                Transport *transport,
                DB *db = nullptr);
    ~DynaReplica() override;

    void ReceiveMessage(const TransportAddress &remote,
                        const std::string_view type,
                        const std::string_view data,
                        void *meta_data) override;

    // Multi-threaded receive (bench/replica.cc, DYNA_RX_THREADS > 1): each extra
    // transport binds the same host:port (SO_REUSEPORT) and is run by its own thread,
    // so ReceiveMessage is called concurrently. Hot messages (requests, commits,
    // replication, ts-agreement, fetch) run under the shared side of state_lock_; view
    // change / recovery / join messages and the heartbeat tick take it exclusively, so
    // they stay serialized against normal processing exactly as with one thread.
    void AddReceiveTransport(Transport *t);
    // Call on each extra receive thread before running its transport: sends made on
    // that thread go out through its own transport (each transport's address maps and
    // fragment counter are then touched by one receive thread only).
    static void BindThreadTransport(Transport *t);

private:
    // Transport to send on from the calling thread (its own receive transport, else the
    // primary one — the timer thread, release and executor threads).
    Transport *Tx() const;
    // The real dispatcher; ReceiveMessage wraps it with DYNA_STATS timing.
    void ReceiveMessageImpl(const TransportAddress &remote, const std::string_view type,
                            const std::string_view data, void *meta_data);

    // Message handlers.
    void HandleClientRequest(const TransportAddress &remote, const proto::ClientRequest &m);
    // HandleClientRequest in two halves so the ordering thread does only the ordered
    // part: Build (any thread: entry construction, txn moved in) and Process (ordering:
    // view check, OWD sample, at-most-once dedup, SubmitTxn).
    TxnEntry BuildRequestEntry(std::shared_ptr<TransportAddress> addr, proto::Txn &&txn);
    void ProcessRequestEntry(TxnEntry &&e);
    void SubmitRequestEntry(TxnEntry &&e);
    void HandleCommit(const TransportAddress &remote, const TxnId &id,
                      uint64_t agreedT, bool slow);
    void HandleFetchRequest(const TransportAddress &remote, const proto::FetchRequest &m);
    void HandleFetchReply(const proto::FetchReply &m);
    void HandleTsAgree(const proto::TsAgree &m);

    // Background timestamp agreement (spec §3.5): periodic scan + TsAgree fan-out.
    void TsAgreementTick();
    // One ts-agreement pass (no timer re-arm): send TsAgrees, and for every txn that just
    // became COMMITTING tell its client (LeaderSyncDone) and start replication.
    void RunTsAgreement();
    // Leader sync moved a txn SPECULATIVE → COMMITTING: send the client a CommitReply in
    // case it is still waiting (design doc § Recovery), preceded by a ViewInfo when the
    // txn predates the current view so the client counts it in the right view.
    void LeaderSyncDone(const TxnEntry &e);
    void SendTsAgree(const ServerOrderer::TsAgreeAction &action);

    // Replication (spec §3.6-3.7): COMMITTING → COMMITTED.
    // `remote`: the sender's address for an unbatched prepare (the ack goes straight
    // back), null for a batched one (the ack is queued to the leader).
    void HandleReplicatePrepare(const TransportAddress *remote,
                                const proto::ReplicatePrepare &m);
    void HandleReplicatePrepareReply(const proto::ReplicatePrepareReply &m);
    void HandleReplicateCommit(const proto::ReplicateCommit &m);
    void SendReplicatePrepare(const ServerOrderer::ReplicateAction &a);

    // --- background batching (DYNA_REPL_BATCH > 1): replication + ts-agreement traffic
    //     queues per destination and leaves as one BgBatch per flush (bg_outbox.h). With
    //     batching off these send each message immediately, as before.
    static constexpr int32_t kBgAll = -1;   // every other replica
    void BgPrepare(const proto::ReplicatePrepare &m);
    void BgPrepareReply(uint32_t to, const proto::ReplicatePrepareReply &m,
                        const TransportAddress *remote);
    void BgCommit(int32_t to, const proto::ReplicateCommit &m);
    void BgTsAgree(uint32_t to, const proto::TsAgree &m);
    // Each lane batches its own transactions' entries (so a received batch belongs to a
    // single lane at the receiver too, which uses the same LaneOf).
    void FlushBgTo(size_t lane, uint32_t to);
    void SendBgBatch(uint32_t to, proto::BgBatch &b, size_t entries);
    void BgFlushTick();   // timer: flush every non-empty batch, re-arm
    void HandleBgBatch(const proto::BgBatch &m);

    // Fired by the orderer when an entry releases (SPECULATIVE): send SpecReply (and
    // cache it for WRITEs; read retries carry a fresh request_id, so reads aren't cached).
    void OnSpecReply(const TxnEntry &e);
    // After a txn became COMMITTING here: if we are its primary leader, broadcast its
    // ReplicatePrepare now rather than on the next background scan.
    void ReplicateNowIfPrimary(const TxnId &id);
    // Outcome of a commit message once the body is present (applied / already executed).
    void FinishCommit(const TxnId &id, const ServerOrderer::CommitBrief &out,
                      ServerOrderer::CommitResult r,
                      bool slow, const TransportAddress *client);
    // Build the SpecReply proto for a released entry (writes carry no results).
    proto::SpecReply BuildSpecReply(const TxnEntry &e) const;
    void SendCommitReply(const TransportAddress &client, const TxnId &id,
                         const ServerOrderer::CommitBrief &c);

    // --- views + recovery (design doc § Recovery) ---
    // Normal processing runs only while the VR layer is NORMAL.
    bool Normal() const { return vc_->phase() == ViewCoordinator::Phase::NORMAL; }
    // Heartbeats + VR failure detection / re-sends (own timer, DYNA_HB_MS).
    void HeartbeatTick();
    // Send the coordinator's outbox and act on its events (pause, collect own recovery
    // data, install the new view).
    void ApplyEffects(ViewCoordinator::Effects &fx);
    void BeginRecovery();
    void InstallNewView();
    void HandleRecoveryRequest(const proto::RecoveryRequest &m);
    // A client used an old view: tell it the current one.
    void SendViewInfo(const TransportAddress &client);

    // --- rejoin ---
    // Durable VR state ("disk-based VR"): dyna-vr-<idx>.state in DYNA_VR_STATE_DIR holds
    // the last installed (view, alive). Its presence at startup means this server was a
    // member and lost its in-memory state → it starts JOINING.
    std::string VrStatePath() const;
    void PersistVrState(uint64_t view, uint64_t alive);
    // JOINING: adopt the coordinator's (view, alive) as a non-member and (re)start the
    // chunked catch-up from that view's VR leader.
    void AdoptViewAsNonMember();
    void SendSnapshotRequest();
    void HandleSnapshotRequest(const TransportAddress &remote, const proto::SnapshotRequest &m);
    void HandleSnapshotChunk(const proto::SnapshotChunk &m);
    // Left out of a view: drop ordering state and per-client caches before rejoining.
    void ResetForRejoin();

    specpaxos::Configuration config;
    int replicaIdx;
    Transport *transport;

    std::atomic<uint64_t> viewId_{0};   // read on the release/executor threads (replies)
    bool     debug_ = false;   // DYNA_DEBUG: log request/reply flow
    size_t   last_logged_executed_ = 0;   // DYNA_DEBUG progress line (log on change)
    uint64_t ts_scan_ms_ = 50;      // background scan cadence (ts-agree / replicate / exec)
    uint64_t ts_timeout_us_ = 100000; // SPECULATIVE age before ts-agreement kicks in; also
                                      // the prepare re-broadcast and stale-commit re-ack throttle
    Topology topo_;              // current view's topology (transport thread)
    uint64_t hb_ms_ = 20;        // heartbeat / VR tick cadence
    // Orderer release loop paused (not NORMAL). Atomic: receive threads read it to drop
    // client traffic early during a view change (see ReceiveMessage).
    std::atomic<bool> paused_{false};
    std::unique_ptr<ViewCoordinator> vc_;
    std::string vr_state_dir_ = ".";

    // Joiner catch-up (transport thread).
    struct CatchUp {
        bool     active = false;
        uint32_t donor = 0;
        uint64_t view = 0;
        uint64_t session = 0;
        uint64_t next_offset = 0;
        uint64_t last_req_us = 0;
        size_t   kvs_applied = 0;
    } catchup_;
    uint64_t snapshot_chunk_ = 512;
    // Donor side: per joiner, the sorted key list of its current session.
    struct DonorSession {
        uint64_t session = 0;
        std::vector<std::string> keys;
    };
    std::map<uint32_t, DonorSession> donor_sessions_;
    std::unique_ptr<StorageBackend> backend_;   // KV store for executed writes
    std::unique_ptr<RealClock>    clock_;
    std::unique_ptr<ServerOrderer> orderer_;

    // Compact reply-cache value: a resend is answered with a reply rebuilt from these
    // (its t and leadership are re-derived from live state anyway), so the hot release
    // path doesn't store a protobuf per txn in a multi-million-entry map.
    struct CachedSpec {
        uint64_t t = 0;
        uint64_t owd = 0;
    };

    // One lane: the per-transaction replica state for the txns LaneOf maps here. The
    // locks are taken almost only by the lane's own thread (the timer thread's scans and
    // view changes are the exceptions), so they are uncontended.
    struct Lane {
        size_t idx = 0;
        // Hot handlers posted by the receive threads.
        MultiProducerSingleConsumerQueue<std::function<void()>> q;
        std::thread th;
        // Per-client OWD estimator (§2), sampled where lateness is judged. Each lane
        // samples its share of a client's requests (an equivalent percentile).
        HybridMutex owd_mu;
        std::unique_ptr<ReplicaOwdEstimator> owd;
        // At-most-once reply cache (spec §1.4 CachedReply) + in-flight set, so a retried
        // WRITE ClientRequest is answered from cache and never double-submitted. READs
        // bypass both: every read attempt carries a fresh request_id and a new t.
        HybridMutex dedup_mu;
        TxnMap<CachedSpec> reply_cache;
        TxnSet             inflight;
        // client_id -> address, learned from that client's writes (under dedup_mu).
        // Recovery M carries txn bodies but not client addresses, so a new partition
        // leader finishing an inherited txn finds its client here.
        std::unordered_map<uint64_t, std::shared_ptr<TransportAddress>> client_addrs;
        std::unique_ptr<BgOutbox> bg;   // null = batching off (DYNA_REPL_BATCH <= 1)
        size_t sender = 0;              // which sender thread sends for this lane
        StatCounter busy_us;            // DYNA_STATS: time running handlers + releases
        StatCounter msgs;               // DYNA_STATS: handler closures run
        StatCounter released;           // DYNA_STATS: entries released by ReleaseDue
        StatMax     hmax;               // DYNA_STATS: longest single handler / release, µs
        StatMax     qmax;
    };
    std::vector<std::unique_ptr<Lane>> lanes_;
    Lane &LaneFor(const TxnId &id) { return *lanes_[orderer_->LaneOf(id)]; }

    // A commit that arrived before the body: remember it until the fetch completes.
    struct PendingCommit {
        uint64_t agreedT = 0;
        bool     slow = false;
        std::shared_ptr<TransportAddress> client_addr;
    };
    HybridMutex pending_mu_;
    TxnMap<PendingCommit> pending_commits_;

    // Shared: hot-message handlers; exclusive: VR / recovery / join + heartbeat tick.
    RwLock state_lock_;

    bool     bg_on_ = false;            // background batching (DYNA_REPL_BATCH > 1)
    uint64_t bg_flush_us_ = 500;

    // --- DYNA_STATS=1: per-second counters, printed from HeartbeatTick (observation only)
    enum RxType { kRxRequest, kRxFast, kRxSlow, kRxPrep, kRxPrepReply, kRxCommit,
                  kRxTsAgree, kRxFetch, kRxFetchRep, kRxBg, kRxFastBatch, kRxHeartbeat,
                  kRxOther, kRxN };
    static bool IsHot(RxType k) { return k != kRxHeartbeat && k != kRxOther; }
    // Hot-message pipeline: parse (receive thread) and process (receive thread, or the
    // ordering thread when DYNA_ORDER_THREAD is on).
    static std::unique_ptr<google::protobuf::Message> ParseHot(RxType kind,
                                                               std::string_view data);
    void ProcessHot(RxType kind, const TransportAddress &remote,
                    const google::protobuf::Message &msg);
    // The TxnId a parsed hot message is about (false for a BgBatch: route per entry).
    static bool HotTxnId(RxType kind, const google::protobuf::Message &msg, TxnId *id);
    void PostToLane(size_t lane, std::function<void()> fn);
    // DYNA_ORDER_THREAD: lane threads run every hot handler (posted by the receive
    // threads through lock-free MPSC queues) and drive their orderer shard's releases,
    // so the lane locks are almost never contended. Busy-poll mode only.
    void OrderingLoop(Lane *lane);
    bool order_thread_on_ = false;
    std::atomic<bool> order_running_{false};
    // Sender threads (DYNA_SEND_THREADS, default 1; 0 = each lane sends inline):
    // release-path SpecReplies and full background batches are sent from here, keeping
    // sendto off the lane threads. Lane i uses sender i % S.
    struct ReplyJob {
        std::shared_ptr<TransportAddress> addr;
        uint64_t client_id = 0, request_id = 0, view_id = 0, t = 0, owd = 0;
        bool     is_leader = false;
    };
    struct Sender {
        MultiProducerSingleConsumerQueue<std::function<void()>> q;
        // Release-path SpecReplies as plain structs (no closure allocation; the sender
        // thread builds the protobuf).
        MultiProducerSingleConsumerQueue<ReplyJob> replies;
        std::thread th;
        StatCounter busy_us;   // DYNA_STATS: time spent sending (non-empty iterations)
    };
    void SendLoop(Sender *s, size_t idx);
    std::atomic<bool> send_running_{false};
    std::vector<std::unique_ptr<Sender>> senders_;
    Sender *SenderFor(const Lane &l) {
        return senders_.empty() ? nullptr : senders_[l.sender].get();
    }
    static RxType ClassifyRx(std::string_view type);
    void MaybeDumpStats(uint64_t now_us);
    bool         stats_on_ = false;
    OrdererStats ostats_;
    StatCounter  rx_[kRxN];
    StatCounter  rx_us_[kRxN];      // handler time per message type
    StatCounter  busy_us_;          // transport thread time inside ReceiveMessage
    StatMax      handler_max_us_;   // longest single ReceiveMessage
    StatCounter  spec_tx_;          // SpecReplies sent (release thread)
    StatCounter  reply_pkts_;       // datagrams carrying SpecReplies (batches count once)
    StatCounter  sync_reply_tx_;    // CommitReplies sent after a leader sync
    StatMax      hb_gap_us_;        // longest gap between heartbeats from any one peer
    StatMax      tick_gap_us_;      // longest gap between our own heartbeat ticks
    StatDist     scan_us_;          // TsAgreementTick's locked scans
    StatDist     bg_tx_entries_;    // entries per BgBatch sent
    StatDist     bg_rx_entries_;    // entries per BgBatch received
    // One-way delay of client requests, split (DYNA_STATS): owd = lane arrival - client
    // send stamp; net = receive thread - send stamp; queue = lane - receive thread; and
    // the OWD estimate piggybacked to clients (what their deadline bound follows).
    StatHist     owd_hist_, owd_net_hist_, owd_queue_hist_, owd_est_hist_;
    StatDist     dedup_wait_us_, pending_wait_us_, owd_wait_us_;   // contended waits
    std::vector<uint64_t> last_hb_rx_us_;
    uint64_t     last_hb_tick_us_ = 0;
    uint64_t     last_stats_us_ = 0;
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_REPLICA_H_

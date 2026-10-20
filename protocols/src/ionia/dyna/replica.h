// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/replica.h
//   DynaReplica — the per-server DYNA replica. Owns ONE ServerOrderer
//   (single per-server ordering domain; spec §1.4) and drives the write path:
//   ClientRequest → SubmitTxn → (release) → SpecReply; FastCommit/SlowCommit →
//   ApplyCommit (with FetchRequest/FetchReply when the body is missing).

#ifndef _DYNA_REPLICA_H_
#define _DYNA_REPLICA_H_

#include "lib/configuration.h"
#include "lib/transport.h"

#include "dyna/deadline.h"
#include "dyna/dyna-proto.pb.h"
#include "dyna/server_orderer.h"
#include "dyna/storage_backend.h"
#include "dyna/topology.h"
#include "dyna/txn_types.h"
#include "dyna/view_coordinator.h"
#include "dyna/wall_clock.h"

#include <atomic>
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

private:
    // Message handlers.
    void HandleClientRequest(const TransportAddress &remote, const proto::ClientRequest &m);
    void HandleCommit(const TransportAddress &remote, const TxnId &id,
                      uint64_t agreedT, bool slow);
    void HandleFetchRequest(const TransportAddress &remote, const proto::FetchRequest &m);
    void HandleFetchReply(const proto::FetchReply &m);
    void HandleTsAgree(const proto::TsAgree &m);

    // Background timestamp agreement (spec §3.5): periodic scan + TsAgree fan-out.
    void TsAgreementTick();
    void SendTsAgree(const ServerOrderer::TsAgreeAction &action);

    // Replication (spec §3.6-3.7): COMMITTING → COMMITTED.
    void HandleReplicatePrepare(const TransportAddress &remote,
                                const proto::ReplicatePrepare &m);
    void HandleReplicatePrepareReply(const proto::ReplicatePrepareReply &m);
    void HandleReplicateCommit(const proto::ReplicateCommit &m);
    void SendReplicatePrepare(const ServerOrderer::ReplicateAction &a);

    // Fired by the orderer when an entry releases (SPECULATIVE): send SpecReply (and
    // cache it for WRITEs; read retries carry a fresh request_id, so reads aren't cached).
    void OnSpecReply(const TxnEntry &e);
    // After a txn became COMMITTING here: if we are its primary leader, broadcast its
    // ReplicatePrepare now rather than on the next background scan.
    void ReplicateNowIfPrimary(const TxnId &id);
    // Outcome of a commit message once the body is present (applied / already executed).
    void FinishCommit(const TxnId &id, const TxnEntry &out, ServerOrderer::CommitResult r,
                      bool slow, const TransportAddress *client);
    // Build the SpecReply proto for a released entry (writes carry no results).
    proto::SpecReply BuildSpecReply(const TxnEntry &e) const;
    void SendCommitReply(const TransportAddress &client, const TxnEntry &e);

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
    bool     paused_ = false;    // orderer release loop paused (not NORMAL)
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

    // Per-client OWD estimator (§2); sampled on the transport thread, read on release.
    std::mutex owd_mu_;
    ReplicaOwdEstimator owd_;

    // At-most-once reply cache (spec §1.4 CachedReply) + in-flight set, so a retried
    // WRITE ClientRequest is answered from cache and never double-submitted. READs bypass
    // both: every read attempt carries a fresh request_id and a new t.
    std::mutex dedup_mu_;
    TxnMap<proto::SpecReply>        reply_cache_;
    TxnSet                           inflight_;

    // A commit that arrived before the body: remember it until the fetch completes.
    struct PendingCommit {
        uint64_t agreedT = 0;
        bool     slow = false;
        std::shared_ptr<TransportAddress> client_addr;
    };
    std::mutex pending_mu_;
    TxnMap<PendingCommit> pending_commits_;
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_REPLICA_H_

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
#include "dyna/in_memory_backend.h"
#include "dyna/server_orderer.h"
#include "dyna/topology.h"
#include "dyna/txn_types.h"
#include "dyna/wall_clock.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_set>

namespace specpaxos {
namespace dyna {

class DynaReplica : public TransportReceiver
{
public:
    DynaReplica(const specpaxos::Configuration &config,
                     int replicaIdx,
                     Transport *transport);
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

    specpaxos::Configuration config;
    int replicaIdx;
    Transport *transport;

    uint64_t viewId_ = 0;
    bool     debug_ = false;   // DYNA_DEBUG: log request/reply flow
    uint64_t ts_scan_ms_ = 50;      // background scan cadence (ts-agree / replicate / exec)
    uint64_t ts_timeout_us_ = 100000; // SPECULATIVE age before ts-agreement kicks in; also
                                      // the prepare re-broadcast and stale-commit re-ack throttle
    Topology topo_;
    std::unique_ptr<InMemoryBackend> backend_;   // KV store for executed writes
    std::unique_ptr<RealClock>    clock_;
    std::unique_ptr<ServerOrderer> orderer_;

    // Per-client OWD estimator (§2); sampled on the transport thread, read on release.
    std::mutex owd_mu_;
    ReplicaOwdEstimator owd_;

    // At-most-once reply cache (spec §1.4 CachedReply) + in-flight set, so a retried
    // WRITE ClientRequest is answered from cache and never double-submitted. READs bypass
    // both: every read attempt carries a fresh request_id and a new t.
    std::mutex dedup_mu_;
    HashMap<TxnId, proto::SpecReply> reply_cache_;
    std::unordered_set<TxnId>        inflight_;

    // A commit that arrived before the body: remember it until the fetch completes.
    struct PendingCommit {
        uint64_t agreedT = 0;
        bool     slow = false;
        std::shared_ptr<TransportAddress> client_addr;
    };
    std::mutex pending_mu_;
    HashMap<TxnId, PendingCommit> pending_commits_;
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_REPLICA_H_

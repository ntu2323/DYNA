// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_multi/replica.h
//   DynaMultiReplica — the per-server DYNA-multi replica. Owns ONE ServerOrderer
//   (single per-server ordering domain; spec §1.4) and drives the write path:
//   ClientRequest → SubmitTxn → (release) → SpecReply; FastCommit/SlowCommit →
//   ApplyCommit (with FetchRequest/FetchReply when the body is missing).

#ifndef _DYNA_MULTI_REPLICA_H_
#define _DYNA_MULTI_REPLICA_H_

#include "lib/configuration.h"
#include "lib/transport.h"

#include "dyna_multi/deadline.h"
#include "dyna_multi/dyna-multi-proto.pb.h"
#include "dyna_multi/in_memory_backend.h"
#include "dyna_multi/server_orderer.h"
#include "dyna_multi/topology.h"
#include "dyna_multi/txn_types.h"
#include "dyna_multi/wall_clock.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_set>

namespace specpaxos {
namespace dyna_multi {

class DynaMultiReplica : public TransportReceiver
{
public:
    DynaMultiReplica(const specpaxos::Configuration &config,
                     int replicaIdx,
                     Transport *transport);
    ~DynaMultiReplica() override;

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

    // Fired by the orderer when an entry releases (SPECULATIVE): send + cache SpecReply.
    void OnSpecReply(const TxnEntry &e);
    // Fired for a READ late at a pure follower: send RejectReply, clear in-flight so a
    // retry (at a larger t) re-processes.
    void OnReject(const TxnEntry &e);
    // Build the SpecReply proto for a released entry (writes carry no results).
    proto::SpecReply BuildSpecReply(const TxnEntry &e) const;
    void SendCommitReply(const TransportAddress &client, const TxnEntry &e);

    specpaxos::Configuration config;
    int replicaIdx;
    Transport *transport;

    uint64_t viewId_ = 0;
    bool     debug_ = false;   // DYNA_MULTI_DEBUG: log request/reply flow
    uint64_t ts_scan_ms_ = 50;      // background ts-agreement scan cadence
    uint64_t ts_timeout_us_ = 100000; // SPECULATIVE age before ts-agreement kicks in
    Topology topo_;
    std::unique_ptr<InMemoryBackend> backend_;   // KV store for executed writes
    std::unique_ptr<RealClock>    clock_;
    std::unique_ptr<ServerOrderer> orderer_;

    // Per-client OWD estimator (§2); sampled on the transport thread, read on release.
    std::mutex owd_mu_;
    ReplicaOwdEstimator owd_;

    // At-most-once reply cache (spec §1.4 CachedReply) + in-flight set, so a retried
    // ClientRequest is answered from cache and never double-submitted.
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

} // namespace dyna_multi
} // namespace specpaxos

#endif // _DYNA_MULTI_REPLICA_H_

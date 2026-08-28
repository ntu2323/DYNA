// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_multi/server_orderer.h
//   The per-server ordering engine. Unlike dyna_new's PartitionOrderer (one per
//   partition), DYNA-multi has ONE ServerOrderer per server: a transaction has a
//   single OrderKey and lives in exactly one of earlyBuffer / lateBuffer / specLog
//   even when it spans partitions (spec §1.3-1.4). One lock, one release loop.
//
//   Stage 2 scope: the fast-path release path — earlyBuffer admission (with the
//   always-on counter-veto, DECISION B), the condition-variable release loop, the
//   specLog append + highestReleasedT bump, and the SpecReply callback. lateBuffer /
//   re-timestamp / commit handling arrive in Stage 4.

#ifndef _DYNA_MULTI_SERVER_ORDERER_H_
#define _DYNA_MULTI_SERVER_ORDERER_H_

#include "dyna_multi/indexed_min_pq.h"
#include "dyna_multi/storage_backend.h"
#include "dyna_multi/topology.h"
#include "dyna_multi/txn_types.h"
#include "dyna_multi/wall_clock.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace specpaxos {
namespace dyna_multi {

class ServerOrderer {
public:
    // Fired once per released entry, OUTSIDE the ordering lock (so the replica can
    // serialize + send the SpecReply without contending the release path). Also fired
    // for a leader READ once the executor has disk-read its values (entry.results set).
    using OnSpecReply = std::function<void(const TxnEntry &)>;
    // Fired for a READ that arrives late at a pure follower (spec § read path): the
    // replica sends a RejectReply so the client retries at a larger t.
    using OnReject = std::function<void(const TxnEntry &)>;

    ServerOrderer(uint32_t server_id, uint64_t view_id, Clock *clock,
                  Topology topo, OnSpecReply on_spec_reply,
                  StorageBackend *backend = nullptr, OnReject on_reject = nullptr);
    ~ServerOrderer();

    // Classify a freshly-arrived client transaction (spec § write-path arrival).
    // On-time (clock() <= t AND orderKey > highestReleasedT) => earlyBuffer. Late =>
    // if this server leads a key in the txn, re-timestamp t := clock() (guaranteed
    // > highestReleasedT) and release immediately (fires OnSpecReply at the bumped t);
    // else (pure follower) buffer silently in lateBuffer. The counter-veto lives only
    // on this client-arrival path.
    void SubmitTxn(TxnEntry entry, bool poke = true);

    // Synchronous drain of all due earlyBuffer entries (testable with MockClock).
    // Fires OnSpecReply outside the lock. Returns the number released.
    size_t ReleaseDue();

    // --- commit-message handling (protocol-decided deadlines; NOT vetoed) ---
    enum class CommitResult { kApplied, kNeedFetch };
    // Locate the txn (specLog → earlyBuffer → lateBuffer); on found, adopt
    // t := agreedT (re-heapify / move into specLog), bump highestReleasedT, set
    // status := COMMITTING, and copy the entry to *out. If absent, kNeedFetch (the
    // replica should FetchRequest the body, InstallFetched, then re-apply).
    CommitResult ApplyCommit(const TxnId &id, uint64_t agreedT, TxnEntry *out);

    // Materialize a fetched body into lateBuffer (as if it arrived late) so a
    // subsequent ApplyCommit can adopt it. No-op if already present anywhere.
    void InstallFetched(const proto::Txn &body);

    // --- background timestamp agreement (spec §3.5: SPECULATIVE → COMMITTING) ---
    // One TsAgree to send: proposed_t is our live t; deliver to `to_leaders`.
    struct TsAgreeAction {
        TxnId              id;
        uint64_t           proposed_t = 0;
        std::set<uint32_t> to_leaders;
    };
    // Scan specLog for SPECULATIVE txns we lead that have been stuck ≥ timeout_us:
    // record our own proposal and emit a TsAgree action to the other involved
    // leaders. A txn with a single involved leader (== us) transitions to COMMITTING
    // immediately and is reported in `committed`.
    void DriveTsAgreement(uint64_t timeout_us,
                          std::vector<TsAgreeAction> *actions,
                          std::vector<TxnEntry> *committed);

    // Handle a TsAgree from another involved leader. Records `from`'s proposal,
    // advances our agreement (adopting t := max and marking COMMITTING once every
    // involved leader has proposed), and reports any TsAgree we should now send.
    struct TsRecordResult {
        bool          transitioned = false;   // became COMMITTING
        TxnEntry      entry;                   // the (post-transition) entry copy
        bool          has_action = false;      // we owe other leaders our proposal
        TsAgreeAction action;
    };
    TsRecordResult RecordTsProposal(const TxnId &id, uint32_t from, uint64_t proposedT);

    // --- replication (spec §3.6-3.7: COMMITTING → COMMITTED) ---
    // One ReplicatePrepare to broadcast: the agreed order + body for durability.
    struct ReplicateAction {
        TxnId      id;
        uint64_t   agreed_t = 0;
        proto::Txn body;
    };
    // Scan specLog for COMMITTING txns we are the PRIMARY involved leader for (the
    // min-id involved leader drives replication, avoiding redundant fan-out) and emit
    // a ReplicatePrepare for each whose last broadcast is older than resend_interval_us
    // (0 = always). Throttling avoids flooding while still recovering lost prepares.
    void DriveReplication(std::vector<ReplicateAction> *prepares,
                          uint64_t resend_interval_us = 0);

    // Primary leader: record a ReplicatePrepareReply from `from`; returns true once f
    // distinct other-replica acks are in and the txn transitions to COMMITTED.
    bool RecordPrepareReply(const TxnId &id, uint32_t from);

    // Non-primary server: install/adopt the txn at agreedT as COMMITTING (materializing
    // from `body` if we lack it). Returns true if we now hold it (→ send the ack).
    bool HandleReplicatePrepare(const TxnId &id, uint64_t agreedT, const proto::Txn &body);

    // Mark COMMITTED on a ReplicateCommit from the primary leader.
    void MarkCommitted(const TxnId &id);

    // --- dependency-gated batched execution (spec §3.7) ---
    // Stateless conflict-based pass over an OrderKey-sorted specLog snapshot: apply any
    // COMMITTED write with no smaller un-executed conflicting txn (r-w/w-w/w-r) to the
    // backend (mem_apply + async disk_persist) and GC it from specLog. Returns the
    // TxnIds executed this pass. No-op if no backend was provided.
    std::vector<TxnId> ExecuteReady();

    // Copy out the txn body if this server holds it (for servicing FetchRequest).
    bool TryGetBody(const TxnId &id, proto::Txn *out);

    // Background release loop lifecycle.
    void Start();
    void Stop();

    // --- inspection (locked snapshots) ---
    OrderKey highest_released_t();
    size_t   early_size();
    size_t   late_size();
    size_t   speclog_size();
    bool     speclog_contains(const TxnId &id);
    // Test helper: status + live t of a specLog entry (false if absent).
    bool     speclog_entry(const TxnId &id, Status *status, uint64_t *t);

private:
    void ReleaseLoop();
    // Requires mu_ held. Moves due, non-vetoed entries out of earlyBuffer into
    // specLog and `out`; bumps highestReleasedT. Returns count.
    size_t DrainDueLocked(std::vector<TxnEntry> &out);
    // Requires mu_ held. Earliest earlyBuffer deadline, or Clock::kNever if empty.
    uint64_t NextDeadlineLocked();
    // Fill entry.is_leader_for_this / entry.led_keys from the topology.
    void ComputeLeadership(TxnEntry &e) const;
    // Requires mu_. Records our own proposal for `id`, emits a TsAgree action for
    // any involved leader we haven't told yet, and transitions to COMMITTING once
    // every involved leader has proposed. Outputs a copy of the committed entry.
    void AdvanceTsLocked(const TxnId &id, TsAgreeAction *action, bool *has_action,
                         bool *transitioned, TxnEntry *committed_out);
    // Requires mu_. True iff this server is the min-id involved leader for the txn.
    bool IsPrimaryLeaderLocked(const proto::Txn &txn) const;
    // Requires mu_. Move/materialize the txn into specLog at agreedT as COMMITTING.
    void InstallCommittingLocked(const TxnId &id, uint64_t agreedT, const proto::Txn &body);

    uint32_t        server_id_;
    uint64_t        view_id_;
    uint32_t        quorum_f_;   // f = (n-1)/2; prepare-replies needed for COMMITTED
    Clock          *clock_;
    Topology        topo_;
    OnSpecReply     on_spec_reply_;
    OnReject        on_reject_;
    StorageBackend *backend_;    // KV store for executed writes (nullptr in unit tests)

    std::mutex mu_;
    IndexedMinPQ<TxnId, TxnEntry, OrderKeyLess> early_buffer_;
    HashMap<TxnId, TxnEntry>                     late_buffer_;   // late follower arrivals
    IndexedMinPQ<TxnId, TxnEntry, OrderKeyLess> spec_log_;
    OrderKey highest_released_t_{};   // max OrderKey ever appended to specLog

    std::thread       release_thread_;
    std::atomic<bool> running_{false};
};

} // namespace dyna_multi
} // namespace specpaxos

#endif // _DYNA_MULTI_SERVER_ORDERER_H_

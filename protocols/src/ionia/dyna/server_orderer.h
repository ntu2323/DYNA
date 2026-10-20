// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/server_orderer.h
//   The per-server ordering engine. DYNA has ONE ServerOrderer per server (not one
//   per partition): a transaction has a single OrderKey and lives in exactly one of
//   earlyBuffer / lateBuffer / specLog even when it spans partitions (spec §1.3-1.4).
//   One lock, one release loop.
//
//   Owns earlyBuffer admission + the condition-variable release loop, the late paths
//   (leader re-timestamp, follower lateBuffer), commit adoption, background timestamp
//   agreement, replication bookkeeping, and an event-driven executor thread that applies
//   COMMITTED writes / services leader reads in dependency order.

#ifndef _DYNA_SERVER_ORDERER_H_
#define _DYNA_SERVER_ORDERER_H_

#include "dyna/indexed_min_pq.h"
#include "dyna/recovery.h"
#include "dyna/storage_backend.h"
#include "dyna/topology.h"
#include "dyna/txn_types.h"
#include "dyna/wall_clock.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace specpaxos {
namespace dyna {

class ServerOrderer {
public:
    // Fired once per released entry, OUTSIDE the ordering lock (so the replica can
    // serialize + send the SpecReply without contending the release path). Also fired
    // for a leader READ once the executor has disk-read its values (entry.results set).
    using OnSpecReply = std::function<void(const TxnEntry &)>;

    ServerOrderer(uint32_t server_id, uint64_t view_id, Clock *clock,
                  Topology topo, OnSpecReply on_spec_reply,
                  StorageBackend *backend = nullptr);
    ~ServerOrderer();

    // Classify a freshly-arrived client transaction (design doc § arrival).
    // On-time (clock() <= t) => earlyBuffer. Late => if this server leads a key in the
    // txn, re-timestamp t := clock() (bumped above highestReleasedT) and release
    // immediately (a WRITE fires OnSpecReply at the bumped t); else (pure follower) a
    // WRITE is buffered silently in lateBuffer and a READ is dropped silently (the
    // client retries on timeout). Txns this server already executed are ignored.
    void SubmitTxn(TxnEntry entry, bool poke = true);

    // Synchronous drain of all due earlyBuffer entries (testable with MockClock).
    // Fires OnSpecReply outside the lock. Returns the number released.
    size_t ReleaseDue();

    // --- commit-message handling (protocol-decided deadlines; NOT vetoed) ---
    enum class CommitResult { kApplied, kAlreadyExecuted, kNeedFetch };
    // Locate the txn (specLog → earlyBuffer → lateBuffer); on found, adopt
    // t := agreedT (re-heapify / move into specLog), bump highestReleasedT, set
    // status := COMMITTING, and copy the entry to *out. An entry already COMMITTED is
    // left untouched (never downgraded). A txn this server already executed (and GC'd)
    // returns kAlreadyExecuted with *out carrying its id, final t and leadership. If
    // absent, kNeedFetch (the replica should FetchRequest the body, InstallFetched, then
    // re-apply).
    CommitResult ApplyCommit(const TxnId &id, uint64_t agreedT, TxnEntry *out);

    // Materialize a fetched body into lateBuffer (as if it arrived late) so a
    // subsequent ApplyCommit can adopt it. No-op if already present anywhere.
    void InstallFetched(const proto::Txn &body);

    // --- background timestamp agreement (spec §3.5: SPECULATIVE → COMMITTING) ---
    // One TsAgree to send: proposed_t is our live t; deliver to `to_leaders`. A reply
    // (is_reply) answers a peer's proposal and is never itself answered (no ping-pong).
    struct TsAgreeAction {
        TxnId              id;
        uint64_t           proposed_t = 0;
        std::set<uint32_t> to_leaders;
        bool               is_reply = false;
    };
    // Scan specLog for SPECULATIVE txns we lead that have been stuck ≥ timeout_us:
    // record our own proposal and emit a TsAgree action to every other involved leader
    // whose proposal we still lack (re-sent each scan until answered, so a lost TsAgree
    // can't stall agreement). A txn with a single involved leader (== us) transitions to
    // COMMITTING immediately and is reported in `committed`.
    void DriveTsAgreement(uint64_t timeout_us,
                          std::vector<TsAgreeAction> *actions,
                          std::vector<TxnEntry> *committed);

    // Handle a TsAgree from another involved leader. Records `from`'s proposal and
    // advances our agreement (adopting t := max and marking COMMITTING once every
    // involved leader has proposed). Unless the message was itself a reply, we answer
    // `from` with our own t (whatever our status, including executed), so a peer stuck
    // SPECULATIVE converges even when we already committed via the client.
    struct TsRecordResult {
        bool          transitioned = false;   // became COMMITTING
        TxnEntry      entry;                   // the (post-transition) entry copy
        bool          has_action = false;      // we owe `from` our proposal
        TsAgreeAction action;
    };
    TsRecordResult RecordTsProposal(const TxnId &id, uint32_t from, uint64_t proposedT,
                                    bool is_reply = false);

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

    // Primary leader, right after a txn became COMMITTING (client commit or TsAgree):
    // if we are its primary leader and haven't broadcast a prepare yet, stamp it and
    // return the ReplicatePrepare to send now (instead of waiting for the next scan).
    bool TakeReplicateNow(const TxnId &id, ReplicateAction *out);

    // Primary leader: record a ReplicatePrepareReply from `from`.
    //   kNowCommitted   — this ack reached f and the txn just became COMMITTED
    //                     (caller broadcasts ReplicateCommit);
    //   kResendCommit   — already COMMITTED/executed: `from` evidently missed our
    //                     ReplicateCommit (caller re-sends it to `from` only);
    //   kNone           — nothing to do yet.
    enum class PrepareReplyResult { kNone, kNowCommitted, kResendCommit };
    PrepareReplyResult RecordPrepareReply(const TxnId &id, uint32_t from);

    // Non-primary server: install/adopt the txn at agreedT as COMMITTING (materializing
    // from `body` if we lack it). Returns true if we now hold it — or already executed
    // it — (→ send the ack).
    bool HandleReplicatePrepare(const TxnId &id, uint64_t agreedT, const proto::Txn &body);

    // Mark COMMITTED on a ReplicateCommit from the primary leader.
    void MarkCommitted(const TxnId &id);

    // Non-primary server: COMMITTING entries whose ReplicateCommit hasn't arrived within
    // timeout_us (re-checked at most once per timeout_us each). For each, the caller
    // re-sends a ReplicatePrepareReply to the primary, which answers with ReplicateCommit.
    struct ReackAction {
        TxnId    id;
        uint32_t primary = 0;
    };
    void CollectStaleCommitting(uint64_t timeout_us, std::vector<ReackAction> *out);

    // --- dependency-gated batched execution (spec §3.7) ---
    // Stateless conflict-based pass over an OrderKey-sorted specLog snapshot: apply any
    // COMMITTED write with no smaller un-executed conflicting txn (r-w/w-w/w-r) to the
    // backend (apply, with its commit t) and GC it from specLog. Returns the
    // TxnIds executed this pass. No-op if no backend was provided. Serialized: concurrent
    // callers (executor thread, tests) never interleave applies.
    std::vector<TxnId> ExecuteReady();

    // Wake the executor thread to run ExecuteReady (coalesced). Called internally when a
    // leader READ enters specLog or a txn becomes COMMITTED; the replica's periodic
    // tick also calls it as a backstop.
    void KickExecutor();

    // Copy out the txn body if this server holds it (for servicing FetchRequest).
    bool TryGetBody(const TxnId &id, proto::Txn *out);

    // Background release loop + executor thread lifecycle.
    void Start();
    void Stop();

    // --- views + recovery (design doc § Recovery) ---
    // Pause/resume the release loop (normal processing is paused during a view change;
    // entries due meanwhile release on resume).
    void SetPaused(bool paused);

    // A survivor's contribution to a PL's buffer M: every SPECULATIVE / COMMITTING /
    // COMMITTED write txn in specLog touching a key in delta-p (`in_delta`), plus every
    // executed key-value in delta-p with its commit t (read from the store on the
    // executor thread).
    RecoveryData CollectRecovery(const std::function<bool(const std::string &)> &in_delta);

    // StartView: switch to the new view's topology (recomputing leadership of every held
    // entry) and install the Ms buffered from each PL:
    //   - key-values are applied to the store iff newer than what it holds (t-guard), and
    //     remembered so a later, older execution of the same key can't overwrite them;
    //   - txns are adopted with their M status and t (never downgrading a local copy),
    //     materialized from the body if absent; executed txns are skipped.
    // Entries held here but absent from every M are KEPT (user decision); `leftovers`
    // counts the uncommitted ones in the failed range (they block execution of those keys
    // until their client resends in the new view).
    struct InstallStats {
        size_t txns_installed = 0;
        size_t kvs_applied = 0;
        size_t leftovers = 0;
    };
    InstallStats InstallView(const Topology &topo, uint64_t view_id,
                             const std::vector<RecoveryData> &ms,
                             const std::function<bool(const std::string &)> &in_failed_range);

    // Live (t, leadership) of a txn held in specLog or already executed — used to answer
    // a client's resend with current state rather than a cached reply. False if neither.
    bool LiveState(const TxnId &id, uint64_t *t, bool *is_leader);

    // --- rejoin ---
    // Drop all ordering state (buffers, specLog, executed_) — a server left out of a view
    // rejoins from scratch. The store is kept: catch-up applies newer values t-guarded.
    void Reset();
    // Donor side of catch-up: every key this server has applied (sorted, so a joiner can
    // page through it by offset), and the stored (value, t) of keys[offset, offset+count).
    std::vector<std::string> SnapshotKeys();
    std::vector<RecoveredKV> SnapshotRead(const std::vector<std::string> &keys,
                                          size_t offset, size_t count);
    // COMMITTING / COMMITTED write txns not yet executed (the snapshot's last chunk).
    std::vector<RecoveredTxn> CommittedTxns();
    // Joiner side: install a snapshot chunk (key-values t-guarded, txns as in InstallView).
    // Returns the number of key-values applied.
    size_t InstallSnapshot(const RecoveryData &d);

    // Run `fn` on the thread that owns the store (the executor thread when running, else
    // inline), serialized with ExecuteReady; blocks until it has run.
    void RunOnStoreThread(const std::function<void()> &fn);

    // --- inspection (locked snapshots) ---
    OrderKey highest_released_t();
    size_t   early_size();
    size_t   late_size();
    size_t   speclog_size();
    bool     speclog_contains(const TxnId &id);
    // Test helper: status + live t of a specLog entry (false if absent).
    bool     speclog_entry(const TxnId &id, Status *status, uint64_t *t);
    bool     executed(const TxnId &id);
    size_t   executed_count();
    uint64_t view_id();
    Topology topology();
    // Debug: the smallest-OrderKey specLog entry (what blocks execution), or "".
    std::string DebugOldestEntry();

private:
    void ReleaseLoop();
    void ExecutorLoop();
    // Requires mu_ held. Moves due entries out of earlyBuffer into specLog and `out`;
    // bumps highestReleasedT. Sets *kick if a leader READ entered specLog. Returns count.
    size_t DrainDueLocked(std::vector<TxnEntry> &out, bool *kick);
    // Requires mu_ held. Earliest earlyBuffer deadline, or Clock::kNever if empty.
    uint64_t NextDeadlineLocked();
    // Fill entry.is_leader_for_this / entry.led_keys from the topology.
    void ComputeLeadership(TxnEntry &e) const;
    // Requires mu_. Transitions a SPECULATIVE specLog entry to COMMITTING at the max
    // proposal once every involved leader has proposed. Outputs a copy on transition.
    bool TryCompleteTsLocked(const TxnId &id, TxnEntry *committed_out);
    // Requires mu_. True iff this server is the min-id involved leader for the txn.
    bool IsPrimaryLeaderLocked(const proto::Txn &txn) const;
    // Requires mu_. Move/materialize the txn into specLog at agreedT as COMMITTING.
    void InstallCommittingLocked(const TxnId &id, uint64_t agreedT, const proto::Txn &body);
    // Requires mu_. Adopt one M txn (status precedence, never downgrade). Returns true if
    // it changed local state; sets *kick if it became COMMITTED here.
    bool InstallRecoveredLocked(const RecoveredTxn &r, uint64_t now, bool *kick);
    // Requires exec_run_mu_ (store thread). Apply key-values iff newer than the store's
    // (t-guard) and remember their t in recovered_t_. Returns the number applied.
    size_t ApplyKVsGuardedLocked(const std::vector<RecoveredKV> &kvs);

    uint32_t        server_id_;
    uint64_t        view_id_;
    uint32_t        quorum_f_;   // f = (n-1)/2; prepare-replies needed for COMMITTED
    Clock          *clock_;
    Topology        topo_;
    OnSpecReply     on_spec_reply_;
    StorageBackend *backend_;    // KV store for executed writes (nullptr in unit tests)

    std::mutex mu_;
    IndexedMinPQ<TxnId, TxnEntry, OrderKeyLess, TxnIdHash> early_buffer_;
    TxnMap<TxnEntry>                             late_buffer_;   // late follower arrivals
    IndexedMinPQ<TxnId, TxnEntry, OrderKeyLess, TxnIdHash> spec_log_;
    OrderKey highest_released_t_{};   // max OrderKey ever appended to specLog

    // Writes this server executed and GC'd from specLog: id -> (final t, led by us).
    // Guards against re-inserting (and re-applying) a txn after GC via a late commit,
    // prepare, fetch, or client retry. GC deferred (grows with history).
    struct ExecutedInfo { uint64_t t = 0; bool is_leader = false; };
    TxnMap<ExecutedInfo> executed_;

    std::thread       release_thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> paused_{false};

    // Executor thread (event-driven ExecuteReady) + serialization of ExecuteReady.
    std::mutex              exec_run_mu_;
    std::mutex              exec_mu_;
    std::condition_variable exec_cv_;
    bool                    exec_pending_ = false;
    std::thread             exec_thread_;
    std::vector<std::function<void()>> exec_jobs_;   // RunOnStoreThread (under exec_mu_)

    // Store-side recovery state, guarded by exec_run_mu_ (only the store thread touches
    // the store). written_keys_: every key this server has applied (the store offers no
    // usable scan, so recovery enumerates delta-p keys from here; bounded by the key
    // space, not by history). recovered_t_: per key, the t of a value installed from an
    // M; an older committed write executed later must not overwrite it.
    std::unordered_set<std::string> written_keys_;
    HashMap<std::string, uint64_t>  recovered_t_;
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_SERVER_ORDERER_H_

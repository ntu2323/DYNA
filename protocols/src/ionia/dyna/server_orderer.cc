// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/server_orderer.cc

#include "dyna/server_orderer.h"

#include "dyna/cpu_pin.h"

#include <string_view>
#include <unordered_set>
#include <pthread.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace specpaxos {
namespace dyna {

ServerOrderer::ServerOrderer(uint32_t server_id, uint64_t view_id, Clock *clock,
                             Topology topo, OnSpecReply on_spec_reply,
                             StorageBackend *backend, size_t lanes)
    : server_id_(server_id),
      view_id_(view_id),
      quorum_f_(topo.n > 0 ? (topo.n - 1) / 2 : 0),
      clock_(clock),
      topo_(std::move(topo)),
      on_spec_reply_(std::move(on_spec_reply)),
      backend_(backend)
{
    if (lanes == 0) {
        const char *v = std::getenv("DYNA_LANES");
        lanes = (v != nullptr && *v != '\0') ? std::strtoul(v, nullptr, 10) : 1;
    }
    lanes = std::max<size_t>(1, std::min<size_t>(lanes, 64));
    for (size_t i = 0; i < lanes; ++i) shards_.push_back(std::make_unique<Shard>());
}

void ServerOrderer::BumpHighestLocked(Shard &s, const OrderKey &ok)
{
    s.highest_released = std::max(s.highest_released, ok);
    uint64_t cur = highest_released_tmax_.load(std::memory_order_relaxed);
    while (ok.t > cur &&
           !highest_released_tmax_.compare_exchange_weak(cur, ok.t, std::memory_order_relaxed)) {
    }
}

ServerOrderer::~ServerOrderer()
{
    Stop();
}

namespace {
// DYNA_STATS: time the ordering lock is held by a call site (declare right after the
// lock_guard so it is destroyed — and stops — just before the unlock).
struct HoldTimer {
    StatDist *d;
    uint64_t  t0;
    explicit HoldTimer(StatDist *dist) : d(dist), t0(dist ? WallClockMicros() : 0) {}
    ~HoldTimer() { if (d) d->Observe(WallClockMicros() - t0); }
};

// What the SpecReply callback needs from a released WRITE: ids, type, t, leadership,
// OWD and the client address — not the body / leader bookkeeping. Copying only these
// (and moving the full entry into specLog) keeps a deep TxnEntry copy out of the
// ordering lock on every release.
TxnEntry ReplyCopy(const TxnEntry &e)
{
    TxnEntry r;
    r.txn.set_client_id(e.txn.client_id());
    r.txn.set_request_id(e.txn.request_id());
    r.txn.set_type(e.txn.type());
    r.txn.set_t(e.txn.t());
    r.txn.set_view_id(e.txn.view_id());
    r.t = e.t;
    r.status = e.status;
    r.is_leader_for_this = e.is_leader_for_this;
    r.owd_estimate_us = e.owd_estimate_us;
    r.arrival_us = e.arrival_us;
    r.released_us = e.released_us;
    r.client_addr = e.client_addr;
    return r;
}
} // namespace

void ServerOrderer::ComputeLeadership(TxnEntry &e) const
{
    e.led_keys.clear();
    e.primary = UINT32_MAX;
    for (const auto &k : e.txn.r_set()) {
        const uint32_t o = topo_.ownerOf(k);
        e.primary = std::min(e.primary, o);
        if (o == server_id_) e.led_keys.insert(k);
    }
    for (const auto &kv : e.txn.w_set()) {
        const uint32_t o = topo_.ownerOf(kv.key());
        e.primary = std::min(e.primary, o);
        if (o == server_id_) e.led_keys.insert(kv.key());
    }
    e.is_leader_for_this = !e.led_keys.empty();
}

void ServerOrderer::SubmitTxn(TxnEntry entry, bool poke)
{
    ComputeLeadership(entry);
    // Snapshot the id BEFORE any std::move(entry): reading entry.Key() in the same call
    // as std::move(entry) is unsequenced and would read a moved-from proto.
    const TxnId key = entry.Key();
    const bool  is_read = (entry.txn.type() == proto::READ);
    std::vector<TxnEntry> released;   // late-leader WRITE immediate replies
    bool kick = false;
    Shard &s = ShardOf(key);
    {
        const uint64_t wait_t0 = stats_ ? WallClockMicros() : 0;
        std::lock_guard<HybridMutex> lk(s.mu);
        HoldTimer ht(stats_ ? &stats_->hold_submit_us : nullptr);
        if (stats_) stats_->submit_wait_us.Observe(WallClockMicros() - wait_t0);
        // A delayed / retried request for a txn we already executed and GC'd must not
        // be re-admitted (it would be re-applied on top of newer writes) — but a WRITE
        // resend is answered with its final t: its client may still be collecting a
        // quorum (e.g. this server missed the original while paused for a view change
        // and the txn committed via leader sync), and silence would stall it.
        auto xit = s.executed.find(key);
        if (xit != s.executed.end()) {
            if (!is_read) {
                entry.t = xit->second.t;
                entry.status = Status::COMMITTED;
                released.push_back(std::move(entry));
            } else {
                return;
            }
        }

        // Already held (a resend the replica's dedup didn't catch, e.g. after a view
        // change installed it from M): never hold two entries for one id. specLog →
        // answer with the live entry; earlyBuffer → it will reply on release; lateBuffer
        // → re-classify the fresh arrival (we may lead it now).
        bool duplicate = (xit != s.executed.end());
        if (duplicate) {
            // answered above
        } else if (TxnEntry *cur = s.spec_log.find(key)) {
            if (!is_read) {
                if (!cur->client_addr) cur->client_addr = entry.client_addr;
                released.push_back(*cur);
            }
            duplicate = true;
        } else if (s.early_buffer.contains(key)) {
            duplicate = true;
        } else {
            s.late_buffer.erase(key);
        }

        const uint64_t now = clock_->Now();
        if (stats_) {
            if (duplicate) {
                stats_->duplicate.Add();
            } else if (now <= entry.t) {
                stats_->on_time.Add();
                stats_->slack_us.Observe(entry.t - now);
            } else {
                (entry.is_leader_for_this ? stats_->late_leader : stats_->late_follower).Add();
                stats_->lateness_us.Observe(now - entry.t);
            }
        }
        if (duplicate) {
            // handled above
        } else if (now <= entry.t) {
            // On-time (design doc: arrives before T.t) → earlyBuffer.
            if (entry.t < s.next_due.load(std::memory_order_relaxed)) {
                s.next_due.store(entry.t, std::memory_order_release);
            }
            s.early_buffer.push(key, std::move(entry));
        } else if (entry.is_leader_for_this) {
            // Late-leader re-timestamp: bump t := clock(), kept above everything already
            // released (in any lane: the floor is server-wide), and release at the new
            // t. A WRITE replies immediately; a leader READ is held in specLog for the
            // executor (disk-read).
            uint64_t new_t = now;
            const uint64_t floor = highest_released_tmax_.load(std::memory_order_relaxed);
            if (new_t <= floor) new_t = floor + 1;
            entry.t = new_t;
            entry.status = Status::SPECULATIVE;
            entry.released_us = now;
            BumpHighestLocked(s, entry.Order());
            if (is_read) {
                s.spec_log.push(key, std::move(entry));
                kick = true;
            } else {
                released.push_back(ReplyCopy(entry));
                s.spec_log.push(key, std::move(entry));
            }
        } else if (!is_read) {
            // WRITE late at a pure follower: silently buffer.
            s.late_buffer[key] = std::move(entry);
        }
        // READ late at a pure follower: drop silently (the client retries on timeout
        // at a larger t).
    }
    for (const auto &e : released) if (on_spec_reply_) on_spec_reply_(e);
    if (kick) KickExecutor();
    if (poke) clock_->Interrupt();
}

template <class Emit>
ServerOrderer::CommitResult
ServerOrderer::ApplyCommitLocked(Shard &s, const TxnId &id, uint64_t agreedT, Emit &&emit)
{
    const uint64_t now = clock_->Now();

    // Already executed and GC'd: report its final t (so a SlowCommit can still be
    // answered) instead of fetching + re-applying it.
    auto xit = s.executed.find(id);
    if (xit != s.executed.end()) {
        TxnEntry done;
        done.txn.set_client_id(id.clientid);
        done.txn.set_request_id(id.clientreqid);
        done.t = xit->second.t;
        done.status = Status::COMMITTED;
        done.is_leader_for_this = xit->second.is_leader;
        emit(done);
        return CommitResult::kAlreadyExecuted;
    }

    // Already released: adopt agreedT in place (re-heapify). Never downgrade an entry
    // that replication already marked COMMITTED (it holds the same agreed t).
    if (TxnEntry *e = s.spec_log.find(id)) {
        if (e->status != Status::COMMITTED) {
            e->t = agreedT;
            if (e->status != Status::COMMITTING) e->committing_us = now;
            e->status = Status::COMMITTING;
            BumpHighestLocked(s, e->Order());
        }
        emit(*e);                    // report BEFORE update_key (pointer stays valid, but
        s.spec_log.update_key(id);    // keep the order explicit)
        return CommitResult::kApplied;
    }
    // Admitted but not yet released: move earlyBuffer → specLog at agreedT.
    if (TxnEntry *pending = s.early_buffer.find(id)) {
        TxnEntry e = std::move(*pending);   // values live out of line: moving is safe
        s.early_buffer.erase(id);
        e.t = agreedT;
        e.status = Status::COMMITTING;
        e.committing_us = now;
        BumpHighestLocked(s, e.Order());
        emit(e);
        s.spec_log.push(id, std::move(e));
        return CommitResult::kApplied;
    }
    // Buffered late (follower): move lateBuffer → specLog at agreedT.
    auto it = s.late_buffer.find(id);
    if (it != s.late_buffer.end()) {
        TxnEntry e = std::move(it->second);
        s.late_buffer.erase(it);
        e.t = agreedT;
        e.status = Status::COMMITTING;
        e.committing_us = now;
        BumpHighestLocked(s, e.Order());
        emit(e);
        s.spec_log.push(id, std::move(e));
        return CommitResult::kApplied;
    }
    return CommitResult::kNeedFetch;
}

ServerOrderer::CommitResult
ServerOrderer::ApplyCommit(const TxnId &id, uint64_t agreedT, TxnEntry *out)
{
    Shard &s = ShardOf(id);
    std::lock_guard<HybridMutex> lk(s.mu);
    HoldTimer ht(stats_ ? &stats_->hold_commit_us : nullptr);
    return ApplyCommitLocked(s, id, agreedT, [out](const TxnEntry &e) { if (out) *out = e; });
}

ServerOrderer::CommitResult
ServerOrderer::ApplyCommitBrief(const TxnId &id, uint64_t agreedT, CommitBrief *out)
{
    Shard &s = ShardOf(id);
    std::lock_guard<HybridMutex> lk(s.mu);
    HoldTimer ht(stats_ ? &stats_->hold_commit_us : nullptr);
    return ApplyCommitLocked(s, id, agreedT, [out](const TxnEntry &e) {
        if (!out) return;
        out->t = e.t;
        out->status = e.status;
        out->is_leader = e.is_leader_for_this;
    });
}

void ServerOrderer::InstallFetched(const proto::Txn &body)
{
    TxnEntry e;
    e.txn = body;
    e.t = body.t();
    e.status = Status::SPECULATIVE;
    ComputeLeadership(e);
    const TxnId id = e.Key();
    Shard &s = ShardOf(id);
    std::lock_guard<HybridMutex> lk(s.mu);
    if (s.executed.find(id) != s.executed.end() || s.spec_log.contains(id) ||
        s.early_buffer.contains(id) || s.late_buffer.find(id) != s.late_buffer.end()) {
        return;   // already have it (or already executed it)
    }
    s.late_buffer[id] = std::move(e);   // a following ApplyCommit will adopt it
}

bool ServerOrderer::TryGetBody(const TxnId &id, proto::Txn *out)
{
    Shard &s = ShardOf(id);
    std::lock_guard<HybridMutex> lk(s.mu);
    if (const TxnEntry *e = s.spec_log.find(id)) { if (out) *out = e->txn; return true; }
    if (const TxnEntry *e = s.early_buffer.find(id)) { if (out) *out = e->txn; return true; }
    auto it = s.late_buffer.find(id);
    if (it != s.late_buffer.end()) { if (out) *out = it->second.txn; return true; }
    return false;
}

bool ServerOrderer::TryCompleteTsLocked(Shard &s, const TxnId &id, TxnEntry *committed_out)
{
    TxnEntry *e = s.spec_log.find(id);
    if (!e || !e->is_leader_for_this || e->status != Status::SPECULATIVE) return false;
    for (uint32_t L : topo_.involvedLeaders(e->txn)) {
        if (e->ts_proposals.find(L) == e->ts_proposals.end()) return false;
    }
    // Every involved leader has proposed → adopt the max, COMMITTING.
    uint64_t mx = 0;
    for (const auto &kv : e->ts_proposals) mx = std::max(mx, kv.second);
    e->t = mx;
    e->status = Status::COMMITTING;
    e->committing_us = clock_->Now();
    BumpHighestLocked(s, e->Order());
    if (committed_out) *committed_out = *e;   // copy BEFORE the re-heapify
    s.spec_log.update_key(id);                // `e` is invalid past this point
    return true;
}

void ServerOrderer::DriveTsAgreement(uint64_t timeout_us,
                                     std::vector<TsAgreeAction> *actions,
                                     std::vector<TxnEntry> *committed)
{
  for (auto &shp : shards_) {   // one lane at a time
    Shard &s = *shp;
    std::lock_guard<HybridMutex> lk(s.mu);
    const uint64_t now = clock_->Now();

    std::vector<TxnId> stuck;
    s.spec_log.for_each([&](const TxnId &id, const TxnEntry &e) {
        if (e.status == Status::SPECULATIVE && e.is_leader_for_this &&
            (e.sync_now || now >= e.released_us + timeout_us)) {
            stuck.push_back(id);
        }
    });

    for (const TxnId &id : stuck) {
        TxnEntry *e = s.spec_log.find(id);
        e->ts_proposals[server_id_] = e->t;   // our own proposal: our live t
        TxnEntry done;
        if (TryCompleteTsLocked(s, id, &done)) {
            if (committed) committed->push_back(std::move(done));
            continue;
        }
        // Not complete: (re)send our proposal to every involved leader we still lack a
        // proposal from. Re-sent each scan until answered, so a lost TsAgree (or one
        // that reached a peer before it held the txn) can't stall agreement.
        TsAgreeAction a;
        for (uint32_t L : topo_.involvedLeaders(e->txn)) {
            if (L != server_id_ && e->ts_proposals.find(L) == e->ts_proposals.end())
                a.to_leaders.insert(L);
        }
        if (!a.to_leaders.empty() && actions) {
            a.id = id;
            a.proposed_t = e->t;
            actions->push_back(std::move(a));
        }
    }
  }
}

ServerOrderer::TsRecordResult
ServerOrderer::RecordTsProposal(const TxnId &id, uint32_t from, uint64_t proposedT,
                                bool is_reply)
{
    TsRecordResult r;
    Shard &s = ShardOf(id);
    std::lock_guard<HybridMutex> lk(s.mu);

    uint64_t our_t = 0;
    bool     have_t = false;
    auto xit = s.executed.find(id);
    if (xit != s.executed.end()) {
        // Already executed: our final t is authoritative.
        our_t = xit->second.t;
        have_t = true;
    } else if (TxnEntry *e = s.spec_log.find(id)) {
        if (e->is_leader_for_this) {
            e->ts_proposals[from] = proposedT;
            our_t = e->t;   // our live t (the agreed t once COMMITTING/COMMITTED)
            have_t = true;
            if (e->status == Status::SPECULATIVE) {
                e->ts_proposals[server_id_] = e->t;
                r.transitioned = TryCompleteTsLocked(s, id, &r.entry);
            }
        }
    } else if (TxnEntry *pending = s.early_buffer.find(id)) {
        // Not yet released here (our clock lags): keep the proposal on the entry (it
        // moves into specLog with it) and answer with the t it will release at.
        if (pending->is_leader_for_this) {
            pending->ts_proposals[from] = proposedT;
            our_t = pending->t;
            have_t = true;
        }
    }
    // Absent here: drop; the sender re-sends on its next scan.

    if (have_t && !is_reply) {
        r.has_action = true;
        r.action.id = id;
        r.action.proposed_t = our_t;
        r.action.to_leaders = {from};
        r.action.is_reply = true;
    }
    return r;
}

void ServerOrderer::InstallCommittingLocked(Shard &s, const TxnId &id, uint64_t agreedT,
                                            const proto::Txn &body)
{
    const uint64_t now = clock_->Now();
    if (TxnEntry *e = s.spec_log.find(id)) {
        if (e->status != Status::COMMITTED) {
            e->t = agreedT;
            if (e->status != Status::COMMITTING) e->committing_us = now;
            e->status = Status::COMMITTING;
            BumpHighestLocked(s, e->Order());
            s.spec_log.update_key(id);
        }
        return;
    }
    TxnEntry e;
    if (s.early_buffer.contains(id)) {
        e = *s.early_buffer.find(id);
        s.early_buffer.erase(id);
    } else if (auto it = s.late_buffer.find(id); it != s.late_buffer.end()) {
        e = std::move(it->second);
        s.late_buffer.erase(it);
    } else {
        // Not held anywhere: materialize from the body the leader included.
        e.txn = body;
        ComputeLeadership(e);
    }
    e.t = agreedT;
    e.status = Status::COMMITTING;
    e.committing_us = now;
    BumpHighestLocked(s, e.Order());
    s.spec_log.push(id, std::move(e));
}

void ServerOrderer::DriveReplication(std::vector<ReplicateAction> *prepares,
                                     uint64_t resend_interval_us)
{
  for (auto &shp : shards_) {   // one lane at a time
    Shard &s = *shp;
    std::lock_guard<HybridMutex> lk(s.mu);
    const uint64_t now = clock_->Now();
    std::vector<TxnId> ids;
    s.spec_log.for_each([&](const TxnId &id, const TxnEntry &e) {
        // Cheap fields first: this runs over the whole specLog every tick.
        if (e.status == Status::COMMITTING &&
            (e.last_replicate_us == 0 || now >= e.last_replicate_us + resend_interval_us) &&
            e.primary == server_id_) {
            ids.push_back(id);
        }
    });
    for (const TxnId &id : ids) {
        TxnEntry *e = s.spec_log.find(id);
        e->last_replicate_us = std::max<uint64_t>(now, 1);
        ReplicateAction a;
        a.id = id;
        a.agreed_t = e->t;
        a.body = e->txn;
        prepares->push_back(std::move(a));
    }
  }
}

bool ServerOrderer::TakeReplicateNow(const TxnId &id, ReplicateAction *out)
{
    Shard &s = ShardOf(id);
    std::lock_guard<HybridMutex> lk(s.mu);
    HoldTimer ht(stats_ ? &stats_->hold_repl_us : nullptr);
    TxnEntry *e = s.spec_log.find(id);
    if (!e || e->status != Status::COMMITTING || e->last_replicate_us != 0 ||
        e->primary != server_id_) {
        return false;
    }
    e->last_replicate_us = std::max<uint64_t>(clock_->Now(), 1);
    out->id = id;
    out->agreed_t = e->t;
    out->body = e->txn;
    return true;
}

ServerOrderer::PrepareReplyResult
ServerOrderer::RecordPrepareReply(const TxnId &id, uint32_t from)
{
    PrepareReplyResult res = PrepareReplyResult::kNone;
    {
        Shard &s = ShardOf(id);
        std::lock_guard<HybridMutex> lk(s.mu);
        HoldTimer ht(stats_ ? &stats_->hold_repl_us : nullptr);
        if (from == server_id_) return PrepareReplyResult::kNone;
        if (s.executed.find(id) != s.executed.end()) {
            // We only execute COMMITTED txns: `from` missed the ReplicateCommit.
            return PrepareReplyResult::kResendCommit;
        }
        TxnEntry *e = s.spec_log.find(id);
        if (!e || e->primary != server_id_) return PrepareReplyResult::kNone;
        if (e->status == Status::COMMITTED) return PrepareReplyResult::kResendCommit;
        e->prepare_replies.insert(from);
        if (e->status == Status::COMMITTING && e->prepare_replies.size() >= quorum_f_) {
            e->status = Status::COMMITTED;
            res = PrepareReplyResult::kNowCommitted;   // caller broadcasts ReplicateCommit
        }
    }
    if (res == PrepareReplyResult::kNowCommitted) KickExecutor();
    return res;
}

bool ServerOrderer::HandleReplicatePrepare(const TxnId &id, uint64_t agreedT,
                                           const proto::Txn &body)
{
    Shard &s = ShardOf(id);
    std::lock_guard<HybridMutex> lk(s.mu);
    HoldTimer ht(stats_ ? &stats_->hold_repl_us : nullptr);
    if (s.executed.find(id) != s.executed.end()) return true;   // ack; never re-install
    InstallCommittingLocked(s, id, agreedT, body);
    return s.spec_log.contains(id);
}

void ServerOrderer::MarkCommitted(const TxnId &id)
{
    bool kick = false;
    {
        Shard &s = ShardOf(id);
        std::lock_guard<HybridMutex> lk(s.mu);
        HoldTimer ht(stats_ ? &stats_->hold_repl_us : nullptr);
        if (TxnEntry *e = s.spec_log.find(id)) {
            if (e->status != Status::COMMITTED) {
                e->status = Status::COMMITTED;
                kick = true;
            }
        }
    }
    if (kick) KickExecutor();
}

void ServerOrderer::CollectStaleCommitting(uint64_t timeout_us, std::vector<ReackAction> *out)
{
  for (auto &shp : shards_) {   // one lane at a time
    Shard &s = *shp;
    std::lock_guard<HybridMutex> lk(s.mu);
    const uint64_t now = clock_->Now();
    std::vector<TxnId> ids;
    s.spec_log.for_each([&](const TxnId &id, const TxnEntry &e) {
        if (e.status == Status::COMMITTING && now >= e.committing_us + timeout_us &&
            (e.last_replicate_us == 0 || now >= e.last_replicate_us + timeout_us) &&
            e.primary != server_id_) {
            ids.push_back(id);
        }
    });
    for (const TxnId &id : ids) {
        TxnEntry *e = s.spec_log.find(id);
        e->last_replicate_us = std::max<uint64_t>(now, 1);
        if (e->primary == UINT32_MAX) continue;   // no keys (can't happen for a write)
        out->push_back(ReackAction{id, e->primary});
    }
  }
}

std::vector<TxnId> ServerOrderer::ExecuteReady()
{
    // One pass at a time: two interleaved passes could apply a smaller and a larger
    // conflicting write out of order (each erases its batch from specLog under the locks).
    std::lock_guard<std::mutex> run_lk(exec_run_mu_);
    const uint64_t pass_t0 = stats_ ? WallClockMicros() : 0;

    std::vector<TxnId> executed;

    // 1. One consistent snapshot of every lane's specLog (conflicting txns can live in
    //    different lanes, and per-key execution order is by OrderKey across all of them):
    //    all lane locks, but only for a copy of each entry's mutable fields + a pointer.
    //    The pointer stays valid after unlocking: specLog entries live out of line and
    //    only this (serialized) pass or Reset (which takes exec_run_mu_) removes them, and
    //    the txn body (keys, type) is never modified in place.
    struct Snap {
        OrderKey        ok;
        const TxnEntry *e;
        bool            executable;   // COMMITTED write, or a read
        bool            leader;
    };
    std::vector<Snap> snap;
    size_t candidates = 0;
    {
        AllLanesLock locks(shards_);
        const uint64_t lock_t0 = stats_ ? WallClockMicros() : 0;
        size_t total = 0;
        for (auto &shp : shards_) total += shp->spec_log.size();
        snap.reserve(total);
        for (auto &shp : shards_) {
            shp->spec_log.for_each([&](const TxnId &, const TxnEntry &e) {
                const bool ex = e.txn.type() != proto::WRITE || e.status == Status::COMMITTED;
                candidates += ex;
                snap.push_back(Snap{e.Order(), &e, ex, e.is_leader_for_this});
            });
        }
        if (stats_) stats_->exec_locked_us.Observe(WallClockMicros() - lock_t0);
    }
    // Most kicks find nothing executable (e.g. a commit at a follower waiting for its
    // smaller conflicting writes).
    if (candidates == 0) {
        if (stats_) stats_->exec_batch.Observe(0);
        return executed;
    }

    // 2. Decide, with no lock held. Single ascending pass = the transitive
    //    conflict-dependency rule: a blocked smaller txn propagates its block (via
    //    blocked_* keys) to larger conflicting txns. Keys are views into the bodies.
    const uint64_t sort_t0 = stats_ ? WallClockMicros() : 0;
    std::sort(snap.begin(), snap.end(), [](const Snap &a, const Snap &b) { return a.ok < b.ok; });
    // Executable entries in OrderKey order: writes to apply and leader reads to service.
    // Kept as one ordered list so a read observes smaller writes but not larger ones.
    std::vector<std::pair<const Snap *, bool>> picked;   // (entry, run it) in OrderKey order
    {
        // Open addressing (no per-insert allocation): this scan runs every pass over
        // every not-yet-executable entry.
        ankerl::unordered_dense::set<std::string_view> blocked_w, blocked_r;
        blocked_w.reserve(snap.size() * 2);
        blocked_r.reserve(snap.size());
        for (const Snap &sn : snap) {
            // Past the last executable entry nothing more can run; the blocked sets
            // only matter for larger entries, so the rest of the scan is moot.
            if (candidates == 0) break;
            const TxnEntry *e = sn.e;
            const bool cand = sn.executable;
            if (cand) --candidates;
            bool conflict = false;
            for (const auto &kv : e->txn.w_set()) {       // w-w or w-r
                if (blocked_w.count(kv.key()) || blocked_r.count(kv.key())) {
                    conflict = true;
                    break;
                }
            }
            if (!conflict) {
                for (const auto &k : e->txn.r_set()) {    // r-w
                    if (blocked_w.count(k)) { conflict = true; break; }
                }
            }
            const bool is_write = (e->txn.type() == proto::WRITE);
            if (cand && !conflict && is_write) {
                picked.emplace_back(&sn, true);
            } else if (cand && !conflict) {
                // A READ whose conflicting smaller writes are all executed is ready: a
                // leader read is serviced (disk-read + reply); any read is GC'd so it
                // never blocks a later conflicting write.
                picked.emplace_back(&sn, sn.leader);
            } else {
                // Not executable → block larger conflicting txns.
                for (const auto &kv : e->txn.w_set()) blocked_w.insert(kv.key());
                for (const auto &k : e->txn.r_set()) blocked_r.insert(k);
            }
        }
    }

    if (stats_) stats_->exec_sort_us.Observe(WallClockMicros() - sort_t0);
    // 3. Per lane: take ownership of the picked entries out of specLog (no copies or
    //    moves of the entries themselves) and record executed writes. A picked entry
    //    can't have changed meanwhile: a COMMITTED write is final, and reads aren't
    //    re-timestamped. Picked entries we don't run (follower reads) are freed below.
    std::vector<std::unique_ptr<TxnEntry>> owned;
    owned.reserve(picked.size());
    {
        std::vector<std::vector<const Snap *>> by_lane(shards_.size());
        for (const auto &p : picked) by_lane[LaneOf(p.first->e->Key())].push_back(p.first);
        for (size_t lane = 0; lane < shards_.size(); ++lane) {
            if (by_lane[lane].empty()) continue;
            Shard &s = *shards_[lane];
            std::lock_guard<HybridMutex> lk(s.mu);
            for (const Snap *sn : by_lane[lane]) {
                const TxnId id = sn->e->Key();
                if (sn->e->txn.type() == proto::WRITE) {
                    s.executed[id] = ExecutedInfo{sn->ok.t, sn->leader};
                }
                owned.push_back(s.spec_log.erase_ptr(id));   // == sn->e, now ours
            }
        }
    }
    for (const auto &p : picked) {   // executed writes, in OrderKey order (return value)
        if (p.first->e->txn.type() == proto::WRITE) executed.push_back(p.first->e->Key());
    }
    if (stats_) stats_->exec_batch.Observe(executed.size());

    const uint64_t apply_t0 = stats_ ? WallClockMicros() : 0;
    // Run outside the lane locks, in OrderKey order. Each write is applied to the
    // store with its commit timestamp; it is readable immediately. This is after commit,
    // so it never sits on the commit path (invariant 2), and there is no fsync: a
    // committed txn already lives on f+1 replicas.
    for (const auto &[sn, run] : picked) {
        if (!run) continue;   // a follower's read: just GC'd
        // We own the entry now (`owned`); it is no longer in any specLog.
        TxnEntry &e = *const_cast<TxnEntry *>(sn->e);
        if (e.txn.type() == proto::WRITE) {
            for (const auto &kv : e.txn.w_set()) {
                if (!backend_) continue;
                // A value installed by recovery may be newer than this (leftover) write.
                if (!recovered_t_.empty()) {
                    auto rit = recovered_t_.find(kv.key());
                    if (rit != recovered_t_.end() && e.t < rit->second) continue;
                }
                backend_->apply(kv.key(), kv.value(), e.t);
                written_keys_.insert(kv.key());
            }
        } else {
            // Leader READ: read the keys THIS server leads; the client merges results
            // across involved leaders (spec § read path).
            for (const auto &k : e.txn.r_set()) {
                if (e.led_keys.count(k) && backend_) {
                    auto v = backend_->read(k);
                    e.results[k] = v ? v->value : std::string();
                }
            }
            if (on_spec_reply_) on_spec_reply_(e);   // SpecReply carrying the read results
        }
    }
    if (stats_) {
        const uint64_t end = WallClockMicros();
        stats_->exec_apply_us.Observe(end - apply_t0);
        stats_->exec_pass_us.Observe(end - pass_t0);
    }
    return executed;
}

void ServerOrderer::KickExecutor()
{
    if (exec_kicked_.exchange(true, std::memory_order_acq_rel)) return;   // already pending
    {
        std::lock_guard<std::mutex> lk(exec_mu_);
        exec_pending_ = true;
    }
    exec_cv_.notify_one();
}

void ServerOrderer::ExecutorLoop()
{
    // The executor is the only thread that touches the store in a running replica
    // (stores like SplinterDB require per-thread registration).
    if (backend_) backend_->RegisterThread();
    pthread_setname_np(pthread_self(), "dyna-exec");
    PinSelfFromEnv("DYNA_PIN_EXEC");
    uint64_t last_pass_us = 0;
    while (true) {
        std::vector<std::function<void()>> jobs;
        {
            std::unique_lock<std::mutex> lk(exec_mu_);
            exec_cv_.wait(lk, [this] {
                return exec_pending_ || !exec_jobs_.empty() || !running_.load();
            });
            jobs.swap(exec_jobs_);
            if (!running_.load() && jobs.empty()) break;
            exec_pending_ = false;   // coalesce: kicks during the pass trigger one more
            // Re-arm the lock-free kick BEFORE the pass, so a commit that lands during it
            // takes the slow path above and schedules one more pass.
            exec_kicked_.store(false, std::memory_order_release);
        }
        for (auto &job : jobs) job();   // RunOnStoreThread (each wrapper serializes)
        if (!running_.load()) break;
        if (exec_min_interval_us_ > 0 && jobs.empty()) {
            // Let commits accumulate: one pass per interval at most. (Store jobs from
            // recovery / rejoin run immediately above and are not delayed.)
            const uint64_t now = WallClockMicros();
            if (now < last_pass_us + exec_min_interval_us_) {
                std::this_thread::sleep_for(
                    std::chrono::microseconds(last_pass_us + exec_min_interval_us_ - now));
            }
            last_pass_us = WallClockMicros();
        }
        ExecuteReady();
    }
    if (backend_) backend_->UnregisterThread();
}

size_t ServerOrderer::DrainDueLocked(Shard &s, std::vector<TxnEntry> &out, bool *kick)
{
    const uint64_t now = clock_->Now();
    size_t n = 0;
    while (!s.early_buffer.empty()) {
        const OrderKey ok = s.early_buffer.peek_min().Order();
        if (ok.t > now) break;                       // earliest not yet due
        const TxnId id = s.early_buffer.peek_min_id();
        // Transfer the entry's allocation from earlyBuffer to specLog (no realloc).
        std::unique_ptr<TxnEntry> ep = s.early_buffer.pop_min_ptr();
        TxnEntry &e = *ep;
        e.status = Status::SPECULATIVE;
        e.released_us = now;
        if (stats_) stats_->release_lag_us.Observe(now - ok.t);
        // max, not assign: a protocol-decided insert (commit / prepare / TsAgree) may
        // already have pushed highestReleasedT past this entry.
        BumpHighestLocked(s, ok);

        const bool is_read = (e.txn.type() == proto::READ);
        if (is_read && !e.is_leader_for_this) {
            // Follower READ endorsement (spec § read path): reply immediately, do NOT
            // log it (reads aren't ordered/executed on followers).
            out.push_back(std::move(e));
        } else if (is_read) {
            // Leader READ: log it; the executor services it (disk-read) once its
            // dependent writes have executed.
            s.spec_log.push_ptr(id, std::move(ep));
            *kick = true;
        } else {
            // WRITE (endorse/lead): log it and reply now.
            out.push_back(ReplyCopy(e));
            s.spec_log.push_ptr(id, std::move(ep));
        }
        ++n;
    }
    s.next_due.store(NextDeadlineLocked(s), std::memory_order_release);
    return n;
}

uint64_t ServerOrderer::NextDeadlineLocked(Shard &s)
{
    if (s.early_buffer.empty()) return Clock::kNever;
    return s.early_buffer.peek_min().Order().t;
}

size_t ServerOrderer::ReleaseDue()
{
    size_t n = 0;
    for (size_t lane = 0; lane < shards_.size(); ++lane) n += ReleaseDue(lane);
    return n;
}

size_t ServerOrderer::ReleaseDue(size_t lane)
{
    std::vector<TxnEntry> released;
    bool kick = false;
    size_t n;
    {
        Shard &s = *shards_[lane];
        std::lock_guard<HybridMutex> lk(s.mu);
        HoldTimer ht(stats_ ? &stats_->hold_release_us : nullptr);
        n = DrainDueLocked(s, released, &kick);
    }
    for (const auto &e : released) {
        if (on_spec_reply_) on_spec_reply_(e);
    }
    if (kick) KickExecutor();
    return n;
}

void ServerOrderer::ReleaseLoop()
{
    pthread_setname_np(pthread_self(), "dyna-rel");
    while (running_.load()) {
        std::vector<TxnEntry> released;
        bool kick = false;
        uint64_t next = Clock::kNever;   // parked until SetPaused(false) interrupts us
        if (!paused_.load()) {
            for (auto &shp : shards_) {
                Shard &s = *shp;
                std::lock_guard<HybridMutex> lk(s.mu);
                HoldTimer ht(stats_ ? &stats_->hold_release_us : nullptr);
                DrainDueLocked(s, released, &kick);
                next = std::min(next, NextDeadlineLocked(s));
            }
        }
        for (const auto &e : released) {
            if (on_spec_reply_) on_spec_reply_(e);
        }
        if (kick) KickExecutor();
        if (!running_.load()) break;
        clock_->SleepUntil(next);   // parks until next deadline or Interrupt()
    }
}

void ServerOrderer::Start(bool own_release_thread)
{
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return;  // already running
    if (own_release_thread) release_thread_ = std::thread(&ServerOrderer::ReleaseLoop, this);
    exec_thread_ = std::thread(&ServerOrderer::ExecutorLoop, this);
}

void ServerOrderer::Stop()
{
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false)) return; // not running
    clock_->Interrupt();
    {
        std::lock_guard<std::mutex> lk(exec_mu_);   // pair with the wait predicate
    }
    exec_cv_.notify_all();
    if (release_thread_.joinable()) release_thread_.join();
    if (exec_thread_.joinable()) exec_thread_.join();
}

void ServerOrderer::SetPaused(bool paused)
{
    paused_.store(paused);
    clock_->Interrupt();   // resume: drain whatever came due meanwhile
}

void ServerOrderer::RunOnStoreThread(const std::function<void()> &fn)
{
    if (!running_.load() || !exec_thread_.joinable() ||
        std::this_thread::get_id() == exec_thread_.get_id()) {
        std::lock_guard<std::mutex> run_lk(exec_run_mu_);
        fn();
        return;
    }
    std::mutex done_mu;
    std::condition_variable done_cv;
    bool done = false;
    {
        std::lock_guard<std::mutex> lk(exec_mu_);
        exec_jobs_.push_back([&]() {
            {
                std::lock_guard<std::mutex> run_lk(exec_run_mu_);
                fn();
            }
            std::lock_guard<std::mutex> dl(done_mu);
            done = true;
            done_cv.notify_all();
        });
    }
    exec_cv_.notify_one();
    std::unique_lock<std::mutex> dl(done_mu);
    done_cv.wait(dl, [&] { return done; });
}

RecoveryData
ServerOrderer::CollectRecovery(const std::function<bool(const std::string &)> &in_delta)
{
    RecoveryData d;
    {
        // Txns first: an entry the executor erases from specLog after this snapshot is
        // applied to the store before the store scan below runs (same serialization).
        AllLanesLock locks(shards_);
        for (auto &shp : shards_) {
            shp->spec_log.for_each([&](const TxnId &, const TxnEntry &e) {
                if (e.txn.type() != proto::WRITE) return;   // reads hold no state
                if (!TxnTouches(e.txn, in_delta)) return;
                d.txns.push_back(RecoveredTxn{e.txn, e.t, e.status});
            });
        }
    }
    RunOnStoreThread([&]() {
        if (!backend_) return;
        for (const std::string &key : written_keys_) {
            if (!in_delta(key)) continue;
            auto v = backend_->read(key);
            if (v) d.kvs.push_back(RecoveredKV{key, v->value, v->t});
        }
    });
    return d;
}

bool ServerOrderer::InstallRecoveredLocked(Shard &s, const RecoveredTxn &r, uint64_t now,
                                           bool *kick)
{
    const TxnId id = r.Key();
    if (s.executed.find(id) != s.executed.end()) return false;

    if (TxnEntry *e = s.spec_log.find(id)) {
        if (StatusRank(r.status) <= StatusRank(e->status)) return false;   // never downgrade
        if (e->status == Status::SPECULATIVE) e->committing_us = now;
        e->t = r.t;
        e->status = r.status;
        BumpHighestLocked(s, e->Order());
        s.spec_log.update_key(id);   // `e` is invalid past this point
        if (r.status == Status::COMMITTED) *kick = true;
        return true;
    }

    TxnEntry e;
    if (s.early_buffer.contains(id)) {
        // Not yet released here: it releases on its own at the client's t, which is what
        // a SPECULATIVE copy in M carries anyway.
        if (r.status == Status::SPECULATIVE) return false;
        e = *s.early_buffer.find(id);
        s.early_buffer.erase(id);
    } else if (auto it = s.late_buffer.find(id); it != s.late_buffer.end()) {
        e = std::move(it->second);
        s.late_buffer.erase(it);
    } else {
        e.txn = r.txn;
        ComputeLeadership(e);
    }
    e.t = r.t;
    e.status = r.status;
    e.released_us = now;
    if (r.status != Status::SPECULATIVE) e.committing_us = now;
    BumpHighestLocked(s, e.Order());
    s.spec_log.push(id, std::move(e));
    if (r.status == Status::COMMITTED) *kick = true;
    return true;
}

ServerOrderer::InstallStats
ServerOrderer::InstallView(const Topology &topo, uint64_t view_id,
                           const std::vector<RecoveryData> &ms,
                           const std::function<bool(const std::string &)> &in_failed_range)
{
    InstallStats st;

    // 1. Executed key-values → store (t-guarded), on the store thread.
    RunOnStoreThread([&]() {
        for (const RecoveryData &m : ms) st.kvs_applied += ApplyKVsGuardedLocked(m.kvs);
    });

    bool kick = false;
    {
        AllLanesLock locks(shards_);
        const uint64_t now = clock_->Now();

        // 2. New view: every held entry's leadership follows the new owner map. Primary
        //    replication may have moved too, so let the (new) primary prepare promptly.
        topo_ = topo;
        view_id_ = view_id;
        std::vector<TxnId> ids;
        for (auto &shp : shards_) {
            Shard &s = *shp;
            ids.clear();
            s.spec_log.for_each([&](const TxnId &id, const TxnEntry &) { ids.push_back(id); });
            for (const TxnId &id : ids) {
                TxnEntry *e = s.spec_log.find(id);
                ComputeLeadership(*e);   // leadership doesn't affect the OrderKey
                e->last_replicate_us = 0;
                e->prepare_replies.clear();
            }
            ids.clear();
            s.early_buffer.for_each([&](const TxnId &id, const TxnEntry &) { ids.push_back(id); });
            for (const TxnId &id : ids) ComputeLeadership(*s.early_buffer.find(id));
            for (auto &kv : s.late_buffer) ComputeLeadership(kv.second);
        }

        // 3. M txns.
        TxnSet in_m;
        for (const RecoveryData &m : ms) {
            for (const RecoveredTxn &r : m.txns) {
                in_m.insert(r.Key());
                if (InstallRecoveredLocked(ShardOf(r.Key()), r, now, &kick)) {
                    ++st.txns_installed;
                }
            }
        }

        // 4. Leftovers: uncommitted entries in the failed range that no M carried. Kept;
        //    counted so they show up in testing.
        auto leftover = [&](const TxnEntry &e) {
            return e.status != Status::COMMITTED && e.txn.type() == proto::WRITE &&
                   in_m.find(e.Key()) == in_m.end() && TxnTouches(e.txn, in_failed_range);
        };
        for (auto &shp : shards_) {
            shp->spec_log.for_each([&](const TxnId &, const TxnEntry &e) {
                if (leftover(e)) ++st.leftovers;
            });
            for (const auto &kv : shp->late_buffer) if (leftover(kv.second)) ++st.leftovers;
        }

        // 5. Inherited SPECULATIVE writes we now lead: leader-sync them on the next
        //    ts-agreement pass (the replica runs one right after install) rather than
        //    after the SPEC age timeout. A single-leader txn completes locally.
        for (auto &shp : shards_) {
            Shard &s = *shp;
            ids.clear();
            s.spec_log.for_each([&](const TxnId &id, const TxnEntry &e) {
                if (e.status == Status::SPECULATIVE && e.is_leader_for_this &&
                    e.txn.type() == proto::WRITE) {
                    ids.push_back(id);
                }
            });
            for (const TxnId &id : ids) s.spec_log.find(id)->sync_now = true;
            st.spec_to_sync += ids.size();
        }
    }
    if (kick) KickExecutor();
    clock_->Interrupt();
    return st;
}

size_t ServerOrderer::ApplyKVsGuardedLocked(const std::vector<RecoveredKV> &kvs)
{
    size_t applied = 0;
    for (const RecoveredKV &kv : kvs) {
        uint64_t &rt = recovered_t_[kv.key];
        rt = std::max(rt, kv.t);
        if (!backend_) continue;
        auto cur = backend_->read(kv.key);
        if (cur && cur->t >= kv.t) continue;
        backend_->apply(kv.key, kv.value, kv.t);
        written_keys_.insert(kv.key);
        ++applied;
    }
    return applied;
}

void ServerOrderer::Reset()
{
    std::lock_guard<std::mutex> run_lk(exec_run_mu_);
    AllLanesLock locks(shards_);
    for (auto &shp : shards_) {
        Shard &s = *shp;
        s.early_buffer = decltype(s.early_buffer)();
        s.spec_log = decltype(s.spec_log)();
        s.late_buffer.clear();
        s.executed.clear();
        s.highest_released = OrderKey{};
        s.next_due.store(Clock::kNever, std::memory_order_release);
    }
    highest_released_tmax_.store(0, std::memory_order_relaxed);
    // written_keys_ / recovered_t_ describe the store, which is kept.
}

std::vector<std::string> ServerOrderer::SnapshotKeys()
{
    std::vector<std::string> keys;
    RunOnStoreThread([&]() { keys.assign(written_keys_.begin(), written_keys_.end()); });
    std::sort(keys.begin(), keys.end());
    return keys;
}

std::vector<RecoveredKV> ServerOrderer::SnapshotRead(const std::vector<std::string> &keys,
                                                     size_t offset, size_t count)
{
    std::vector<RecoveredKV> out;
    RunOnStoreThread([&]() {
        if (!backend_) return;
        for (size_t i = offset; i < keys.size() && i < offset + count; ++i) {
            auto v = backend_->read(keys[i]);
            if (v) out.push_back(RecoveredKV{keys[i], v->value, v->t});
        }
    });
    return out;
}

std::vector<RecoveredTxn> ServerOrderer::CommittedTxns()
{
    std::vector<RecoveredTxn> out;
    for (auto &shp : shards_) {
        std::lock_guard<HybridMutex> lk(shp->mu);
        shp->spec_log.for_each([&](const TxnId &, const TxnEntry &e) {
            if (e.txn.type() == proto::WRITE && e.status != Status::SPECULATIVE) {
                out.push_back(RecoveredTxn{e.txn, e.t, e.status});
            }
        });
    }
    return out;
}

size_t ServerOrderer::InstallSnapshot(const RecoveryData &d)
{
    size_t applied = 0;
    if (!d.kvs.empty()) {
        RunOnStoreThread([&]() { applied = ApplyKVsGuardedLocked(d.kvs); });
    }
    bool kick = false;
    {
        AllLanesLock locks(shards_);
        const uint64_t now = clock_->Now();
        for (const RecoveredTxn &r : d.txns) {
            InstallRecoveredLocked(ShardOf(r.Key()), r, now, &kick);
        }
    }
    if (kick) KickExecutor();
    return applied;
}

bool ServerOrderer::LiveState(const TxnId &id, uint64_t *t, bool *is_leader)
{
    Shard &s = ShardOf(id);
    std::lock_guard<HybridMutex> lk(s.mu);
    auto xit = s.executed.find(id);
    if (xit != s.executed.end()) {
        *t = xit->second.t;
        *is_leader = xit->second.is_leader;
        return true;
    }
    if (const TxnEntry *e = s.spec_log.find(id)) {
        *t = e->t;
        *is_leader = e->is_leader_for_this;
        return true;
    }
    return false;
}

std::string ServerOrderer::DebugOldestEntry()
{
    AllLanesLock locks(shards_);
    const TxnEntry *best = nullptr;
    for (auto &shp : shards_) {
        shp->spec_log.for_each([&](const TxnId &, const TxnEntry &e) {
            if (!best || e.Order() < best->Order()) best = &e;
        });
    }
    if (!best) return std::string();
    static const char *kStatus[] = {"SPEC", "COMMITTING", "COMMITTED"};
    std::string keys;
    for (const auto &kv : best->txn.w_set()) keys += kv.key().substr(0, 8) + ",";
    for (const auto &k : best->txn.r_set()) keys += "r:" + k.substr(0, 8) + ",";
    char buf[256];
    snprintf(buf, sizeof(buf), "c=%lu r=%lu t=%lu %s leader=%d keys=%s",
             (unsigned long)best->txn.client_id(), (unsigned long)best->txn.request_id(),
             (unsigned long)best->t, kStatus[StatusRank(best->status)],
             best->is_leader_for_this ? 1 : 0, keys.c_str());
    return buf;
}

uint64_t ServerOrderer::view_id()
{
    std::lock_guard<HybridMutex> lk(shards_[0]->mu);   // any lane lock guards topo_ / view
    return view_id_;
}

Topology ServerOrderer::topology()
{
    std::lock_guard<HybridMutex> lk(shards_[0]->mu);
    return topo_;
}

OrderKey ServerOrderer::highest_released_t()
{
    OrderKey m{};
    for (auto &shp : shards_) {
        std::lock_guard<HybridMutex> lk(shp->mu);
        m = std::max(m, shp->highest_released);
    }
    return m;
}

size_t ServerOrderer::early_size()
{
    size_t n = 0;
    for (auto &shp : shards_) {
        std::lock_guard<HybridMutex> lk(shp->mu);
        n += shp->early_buffer.size();
    }
    return n;
}

size_t ServerOrderer::late_size()
{
    size_t n = 0;
    for (auto &shp : shards_) {
        std::lock_guard<HybridMutex> lk(shp->mu);
        n += shp->late_buffer.size();
    }
    return n;
}

size_t ServerOrderer::speclog_size()
{
    size_t n = 0;
    for (auto &shp : shards_) {
        std::lock_guard<HybridMutex> lk(shp->mu);
        n += shp->spec_log.size();
    }
    return n;
}

bool ServerOrderer::speclog_contains(const TxnId &id)
{
    Shard &s = ShardOf(id);
    std::lock_guard<HybridMutex> lk(s.mu);
    return s.spec_log.contains(id);
}

bool ServerOrderer::speclog_entry(const TxnId &id, Status *status, uint64_t *t)
{
    Shard &s = ShardOf(id);
    std::lock_guard<HybridMutex> lk(s.mu);
    const TxnEntry *e = s.spec_log.find(id);
    if (!e) return false;
    if (status) *status = e->status;
    if (t) *t = e->t;
    return true;
}

bool ServerOrderer::executed(const TxnId &id)
{
    Shard &s = ShardOf(id);
    std::lock_guard<HybridMutex> lk(s.mu);
    return s.executed.find(id) != s.executed.end();
}

void ServerOrderer::ReserveExecuted(size_t n)
{
    // Lanes split txns evenly; leave headroom for imbalance.
    const size_t per = shards_.size() == 1 ? n : n / shards_.size() + n / (4 * shards_.size());
    for (auto &shp : shards_) {
        std::lock_guard<HybridMutex> lk(shp->mu);
        shp->executed.reserve(per);
    }
}

size_t ServerOrderer::executed_count()
{
    size_t n = 0;
    for (auto &shp : shards_) {
        std::lock_guard<HybridMutex> lk(shp->mu);
        n += shp->executed.size();
    }
    return n;
}

} // namespace dyna
} // namespace specpaxos

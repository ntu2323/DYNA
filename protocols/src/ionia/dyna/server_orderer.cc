// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/server_orderer.cc

#include "dyna/server_orderer.h"

#include <string_view>
#include <unordered_set>
#include <algorithm>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace specpaxos {
namespace dyna {

ServerOrderer::ServerOrderer(uint32_t server_id, uint64_t view_id, Clock *clock,
                             Topology topo, OnSpecReply on_spec_reply,
                             StorageBackend *backend)
    : server_id_(server_id),
      view_id_(view_id),
      quorum_f_(topo.n > 0 ? (topo.n - 1) / 2 : 0),
      clock_(clock),
      topo_(std::move(topo)),
      on_spec_reply_(std::move(on_spec_reply)),
      backend_(backend)
{
}

ServerOrderer::~ServerOrderer()
{
    Stop();
}

namespace {
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
    for (const auto &k : e.txn.r_set()) {
        if (topo_.ownerOf(k) == server_id_) e.led_keys.insert(k);
    }
    for (const auto &kv : e.txn.w_set()) {
        if (topo_.ownerOf(kv.key()) == server_id_) e.led_keys.insert(kv.key());
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
    {
        const uint64_t wait_t0 = stats_ ? WallClockMicros() : 0;
        std::lock_guard<HybridMutex> lk(mu_);
        if (stats_) stats_->submit_wait_us.Observe(WallClockMicros() - wait_t0);
        // A delayed / retried request for a txn we already executed and GC'd must not
        // be re-admitted (it would be re-applied on top of newer writes) — but a WRITE
        // resend is answered with its final t: its client may still be collecting a
        // quorum (e.g. this server missed the original while paused for a view change
        // and the txn committed via leader sync), and silence would stall it.
        auto xit = executed_.find(key);
        if (xit != executed_.end()) {
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
        bool duplicate = (xit != executed_.end());
        if (duplicate) {
            // answered above
        } else if (TxnEntry *cur = spec_log_.find(key)) {
            if (!is_read) {
                if (!cur->client_addr) cur->client_addr = entry.client_addr;
                released.push_back(*cur);
            }
            duplicate = true;
        } else if (early_buffer_.contains(key)) {
            duplicate = true;
        } else {
            late_buffer_.erase(key);
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
            early_buffer_.push(key, std::move(entry));
        } else if (entry.is_leader_for_this) {
            // Late-leader re-timestamp: bump t := clock(), kept above everything already
            // released, and release at the new t. A WRITE replies immediately; a leader
            // READ is held in specLog for the executor (disk-read).
            uint64_t new_t = now;
            if (new_t <= highest_released_t_.t) new_t = highest_released_t_.t + 1;
            entry.t = new_t;
            entry.status = Status::SPECULATIVE;
            entry.released_us = now;
            highest_released_t_ = std::max(highest_released_t_, entry.Order());
            if (is_read) {
                spec_log_.push(key, std::move(entry));
                kick = true;
            } else {
                released.push_back(ReplyCopy(entry));
                spec_log_.push(key, std::move(entry));
            }
        } else if (!is_read) {
            // WRITE late at a pure follower: silently buffer.
            late_buffer_[key] = std::move(entry);
        }
        // READ late at a pure follower: drop silently (the client retries on timeout
        // at a larger t).
    }
    for (const auto &e : released) if (on_spec_reply_) on_spec_reply_(e);
    if (kick) KickExecutor();
    if (poke) clock_->Interrupt();
}

ServerOrderer::CommitResult
ServerOrderer::ApplyCommit(const TxnId &id, uint64_t agreedT, TxnEntry *out)
{
    std::lock_guard<HybridMutex> lk(mu_);
    const uint64_t now = clock_->Now();

    // Already executed and GC'd: report its final t (so a SlowCommit can still be
    // answered) instead of fetching + re-applying it.
    auto xit = executed_.find(id);
    if (xit != executed_.end()) {
        if (out) {
            out->txn.set_client_id(id.clientid);
            out->txn.set_request_id(id.clientreqid);
            out->t = xit->second.t;
            out->status = Status::COMMITTED;
            out->is_leader_for_this = xit->second.is_leader;
        }
        return CommitResult::kAlreadyExecuted;
    }

    // Already released: adopt agreedT in place (re-heapify). Never downgrade an entry
    // that replication already marked COMMITTED (it holds the same agreed t).
    if (TxnEntry *e = spec_log_.find(id)) {
        if (e->status != Status::COMMITTED) {
            e->t = agreedT;
            if (e->status != Status::COMMITTING) e->committing_us = now;
            e->status = Status::COMMITTING;
            highest_released_t_ = std::max(highest_released_t_, e->Order());
        }
        if (out) *out = *e;          // copy BEFORE update_key: `e` is invalid after it
        spec_log_.update_key(id);
        return CommitResult::kApplied;
    }
    // Admitted but not yet released: move earlyBuffer → specLog at agreedT.
    if (early_buffer_.contains(id)) {
        TxnEntry e = *early_buffer_.find(id);
        early_buffer_.erase(id);
        e.t = agreedT;
        e.status = Status::COMMITTING;
        e.committing_us = now;
        highest_released_t_ = std::max(highest_released_t_, e.Order());
        if (out) *out = e;
        spec_log_.push(id, std::move(e));
        return CommitResult::kApplied;
    }
    // Buffered late (follower): move lateBuffer → specLog at agreedT.
    auto it = late_buffer_.find(id);
    if (it != late_buffer_.end()) {
        TxnEntry e = std::move(it->second);
        late_buffer_.erase(it);
        e.t = agreedT;
        e.status = Status::COMMITTING;
        e.committing_us = now;
        highest_released_t_ = std::max(highest_released_t_, e.Order());
        if (out) *out = e;
        spec_log_.push(id, std::move(e));
        return CommitResult::kApplied;
    }
    return CommitResult::kNeedFetch;
}

void ServerOrderer::InstallFetched(const proto::Txn &body)
{
    TxnEntry e;
    e.txn = body;
    e.t = body.t();
    e.status = Status::SPECULATIVE;
    ComputeLeadership(e);
    const TxnId id = e.Key();
    std::lock_guard<HybridMutex> lk(mu_);
    if (executed_.find(id) != executed_.end() || spec_log_.contains(id) ||
        early_buffer_.contains(id) || late_buffer_.find(id) != late_buffer_.end()) {
        return;   // already have it (or already executed it)
    }
    late_buffer_[id] = std::move(e);   // a following ApplyCommit will adopt it
}

bool ServerOrderer::TryGetBody(const TxnId &id, proto::Txn *out)
{
    std::lock_guard<HybridMutex> lk(mu_);
    if (const TxnEntry *e = spec_log_.find(id)) { if (out) *out = e->txn; return true; }
    if (const TxnEntry *e = early_buffer_.find(id)) { if (out) *out = e->txn; return true; }
    auto it = late_buffer_.find(id);
    if (it != late_buffer_.end()) { if (out) *out = it->second.txn; return true; }
    return false;
}

bool ServerOrderer::TryCompleteTsLocked(const TxnId &id, TxnEntry *committed_out)
{
    TxnEntry *e = spec_log_.find(id);
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
    highest_released_t_ = std::max(highest_released_t_, e->Order());
    if (committed_out) *committed_out = *e;   // copy BEFORE the re-heapify
    spec_log_.update_key(id);                 // `e` is invalid past this point
    return true;
}

void ServerOrderer::DriveTsAgreement(uint64_t timeout_us,
                                     std::vector<TsAgreeAction> *actions,
                                     std::vector<TxnEntry> *committed)
{
    std::lock_guard<HybridMutex> lk(mu_);
    const uint64_t now = clock_->Now();

    std::vector<TxnId> stuck;
    spec_log_.for_each([&](const TxnId &id, const TxnEntry &e) {
        if (e.status == Status::SPECULATIVE && e.is_leader_for_this &&
            now >= e.released_us + timeout_us) {
            stuck.push_back(id);
        }
    });

    for (const TxnId &id : stuck) {
        TxnEntry *e = spec_log_.find(id);
        e->ts_proposals[server_id_] = e->t;   // our own proposal: our live t
        TxnEntry done;
        if (TryCompleteTsLocked(id, &done)) {
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

ServerOrderer::TsRecordResult
ServerOrderer::RecordTsProposal(const TxnId &id, uint32_t from, uint64_t proposedT,
                                bool is_reply)
{
    TsRecordResult r;
    std::lock_guard<HybridMutex> lk(mu_);

    uint64_t our_t = 0;
    bool     have_t = false;
    auto xit = executed_.find(id);
    if (xit != executed_.end()) {
        // Already executed: our final t is authoritative.
        our_t = xit->second.t;
        have_t = true;
    } else if (TxnEntry *e = spec_log_.find(id)) {
        if (e->is_leader_for_this) {
            e->ts_proposals[from] = proposedT;
            our_t = e->t;   // our live t (the agreed t once COMMITTING/COMMITTED)
            have_t = true;
            if (e->status == Status::SPECULATIVE) {
                e->ts_proposals[server_id_] = e->t;
                r.transitioned = TryCompleteTsLocked(id, &r.entry);
            }
        }
    } else if (TxnEntry *pending = early_buffer_.find(id)) {
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

bool ServerOrderer::IsPrimaryLeaderLocked(const proto::Txn &txn) const
{
    const std::set<uint32_t> leaders = topo_.involvedLeaders(txn);
    if (leaders.empty()) return false;
    return *leaders.begin() == server_id_;   // std::set is ordered → front is the min
}

void ServerOrderer::InstallCommittingLocked(const TxnId &id, uint64_t agreedT,
                                            const proto::Txn &body)
{
    const uint64_t now = clock_->Now();
    if (TxnEntry *e = spec_log_.find(id)) {
        if (e->status != Status::COMMITTED) {
            e->t = agreedT;
            if (e->status != Status::COMMITTING) e->committing_us = now;
            e->status = Status::COMMITTING;
            highest_released_t_ = std::max(highest_released_t_, e->Order());
            spec_log_.update_key(id);
        }
        return;
    }
    TxnEntry e;
    if (early_buffer_.contains(id)) {
        e = *early_buffer_.find(id);
        early_buffer_.erase(id);
    } else if (auto it = late_buffer_.find(id); it != late_buffer_.end()) {
        e = std::move(it->second);
        late_buffer_.erase(it);
    } else {
        // Not held anywhere: materialize from the body the leader included.
        e.txn = body;
        ComputeLeadership(e);
    }
    e.t = agreedT;
    e.status = Status::COMMITTING;
    e.committing_us = now;
    highest_released_t_ = std::max(highest_released_t_, e.Order());
    spec_log_.push(id, std::move(e));
}

void ServerOrderer::DriveReplication(std::vector<ReplicateAction> *prepares,
                                     uint64_t resend_interval_us)
{
    std::lock_guard<HybridMutex> lk(mu_);
    const uint64_t now = clock_->Now();
    std::vector<TxnId> ids;
    spec_log_.for_each([&](const TxnId &id, const TxnEntry &e) {
        if (e.status == Status::COMMITTING && IsPrimaryLeaderLocked(e.txn) &&
            (e.last_replicate_us == 0 || now >= e.last_replicate_us + resend_interval_us)) {
            ids.push_back(id);
        }
    });
    for (const TxnId &id : ids) {
        TxnEntry *e = spec_log_.find(id);
        e->last_replicate_us = std::max<uint64_t>(now, 1);
        ReplicateAction a;
        a.id = id;
        a.agreed_t = e->t;
        a.body = e->txn;
        prepares->push_back(std::move(a));
    }
}

bool ServerOrderer::TakeReplicateNow(const TxnId &id, ReplicateAction *out)
{
    std::lock_guard<HybridMutex> lk(mu_);
    TxnEntry *e = spec_log_.find(id);
    if (!e || e->status != Status::COMMITTING || e->last_replicate_us != 0 ||
        !IsPrimaryLeaderLocked(e->txn)) {
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
        std::lock_guard<HybridMutex> lk(mu_);
        if (from == server_id_) return PrepareReplyResult::kNone;
        if (executed_.find(id) != executed_.end()) {
            // We only execute COMMITTED txns: `from` missed the ReplicateCommit.
            return PrepareReplyResult::kResendCommit;
        }
        TxnEntry *e = spec_log_.find(id);
        if (!e || !IsPrimaryLeaderLocked(e->txn)) return PrepareReplyResult::kNone;
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
    std::lock_guard<HybridMutex> lk(mu_);
    if (executed_.find(id) != executed_.end()) return true;   // ack; never re-install
    InstallCommittingLocked(id, agreedT, body);
    return spec_log_.contains(id);
}

void ServerOrderer::MarkCommitted(const TxnId &id)
{
    bool kick = false;
    {
        std::lock_guard<HybridMutex> lk(mu_);
        if (TxnEntry *e = spec_log_.find(id)) {
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
    std::lock_guard<HybridMutex> lk(mu_);
    const uint64_t now = clock_->Now();
    std::vector<TxnId> ids;
    spec_log_.for_each([&](const TxnId &id, const TxnEntry &e) {
        if (e.status == Status::COMMITTING && !IsPrimaryLeaderLocked(e.txn) &&
            now >= e.committing_us + timeout_us &&
            (e.last_replicate_us == 0 || now >= e.last_replicate_us + timeout_us)) {
            ids.push_back(id);
        }
    });
    for (const TxnId &id : ids) {
        TxnEntry *e = spec_log_.find(id);
        e->last_replicate_us = std::max<uint64_t>(now, 1);
        const std::set<uint32_t> leaders = topo_.involvedLeaders(e->txn);
        if (leaders.empty()) continue;
        out->push_back(ReackAction{id, *leaders.begin()});
    }
}

std::vector<TxnId> ServerOrderer::ExecuteReady()
{
    // One pass at a time: two interleaved passes could apply a smaller and a larger
    // conflicting write out of order (each erases its batch from specLog under mu_).
    std::lock_guard<std::mutex> run_lk(exec_run_mu_);

    std::vector<TxnId> executed;
    // Executable entries in OrderKey order: writes to apply and leader reads to service.
    // Kept as one ordered list so a read observes smaller writes but not larger ones.
    std::vector<TxnEntry> to_run;
    {
        std::lock_guard<HybridMutex> lk(mu_);
        const uint64_t lock_t0 = stats_ ? WallClockMicros() : 0;

        // Only COMMITTED writes and reads can run in a pass. Most kicks find the log
        // with nothing executable (e.g. a commit at a follower waiting for its smaller
        // conflicting writes), so check that before paying for the snapshot + sort —
        // this pass holds mu_, which the transport and release threads also need.
        auto executable = [](const TxnEntry &e) {
            return e.txn.type() != proto::WRITE || e.status == Status::COMMITTED;
        };
        size_t candidates = 0;
        spec_log_.for_each([&](const TxnId &, const TxnEntry &e) {
            if (executable(e)) ++candidates;
        });

        std::vector<std::pair<TxnId, bool>> picked;   // (id, run it) in OrderKey order
        if (candidates > 0) {
            // Snapshot specLog pointers (stable while we hold mu_) and sort by OrderKey.
            std::vector<const TxnEntry *> snap;
            snap.reserve(spec_log_.size());
            spec_log_.for_each([&](const TxnId &, const TxnEntry &e) { snap.push_back(&e); });
            std::sort(snap.begin(), snap.end(), [](const TxnEntry *a, const TxnEntry *b) {
                return a->Order() < b->Order();
            });

            // Single ascending pass = the transitive conflict-dependency rule. A blocked
            // smaller txn propagates its block (via blocked_* keys) to larger conflicting
            // txns. Keys are views into the entries' protos (alive while we hold mu_).
            std::unordered_set<std::string_view> blocked_w, blocked_r;
            blocked_w.reserve(snap.size() * 2);
            blocked_r.reserve(snap.size());
            for (const TxnEntry *e : snap) {
                // Past the last executable entry nothing more can run; the blocked sets
                // only matter for larger entries, so the rest of the scan is moot.
                if (candidates == 0) break;
                const bool cand = executable(*e);
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
                    picked.emplace_back(e->Key(), true);
                    executed.push_back(e->Key());
                    executed_[e->Key()] = ExecutedInfo{e->t, e->is_leader_for_this};
                } else if (cand && !conflict) {
                    // A READ whose conflicting smaller writes are all executed is ready: a
                    // leader read is serviced (disk-read + reply); any read is GC'd so it
                    // never blocks a later conflicting write.
                    picked.emplace_back(e->Key(), e->is_leader_for_this);
                } else {
                    // Not executable → block larger conflicting txns.
                    for (const auto &kv : e->txn.w_set()) blocked_w.insert(kv.key());
                    for (const auto &k : e->txn.r_set()) blocked_r.insert(k);
                }
            }
        }
        // Move the picked entries out (no proto copies) and GC them from specLog.
        to_run.reserve(picked.size());
        for (const auto &[id, run] : picked) {
            if (run) {
                if (TxnEntry *m = spec_log_.find(id)) to_run.push_back(std::move(*m));
            }
            spec_log_.erase(id);
        }
        if (stats_) {
            stats_->exec_locked_us.Observe(WallClockMicros() - lock_t0);
            stats_->exec_batch.Observe(executed.size());
        }
    }

    // Run outside the ordering lock, in OrderKey order. Each write is applied to the
    // store with its commit timestamp; it is readable immediately. This is after commit,
    // so it never sits on the commit path (invariant 2), and there is no fsync: a
    // committed txn already lives on f+1 replicas.
    for (TxnEntry &e : to_run) {
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
    return executed;
}

void ServerOrderer::KickExecutor()
{
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
        }
        for (auto &job : jobs) job();   // RunOnStoreThread (each wrapper serializes)
        if (!running_.load()) break;
        ExecuteReady();
    }
    if (backend_) backend_->UnregisterThread();
}

size_t ServerOrderer::DrainDueLocked(std::vector<TxnEntry> &out, bool *kick)
{
    const uint64_t now = clock_->Now();
    size_t n = 0;
    while (!early_buffer_.empty()) {
        const OrderKey ok = early_buffer_.peek_min().Order();
        if (ok.t > now) break;                       // earliest not yet due
        const TxnId id = early_buffer_.peek_min_id();
        TxnEntry e = early_buffer_.pop_min();
        e.status = Status::SPECULATIVE;
        e.released_us = now;
        if (stats_) stats_->release_lag_us.Observe(now - ok.t);
        // max, not assign: a protocol-decided insert (commit / prepare / TsAgree) may
        // already have pushed highestReleasedT past this entry.
        highest_released_t_ = std::max(highest_released_t_, ok);

        const bool is_read = (e.txn.type() == proto::READ);
        if (is_read && !e.is_leader_for_this) {
            // Follower READ endorsement (spec § read path): reply immediately, do NOT
            // log it (reads aren't ordered/executed on followers).
            out.push_back(std::move(e));
        } else if (is_read) {
            // Leader READ: log it; the executor services it (disk-read) once its
            // dependent writes have executed.
            spec_log_.push(id, std::move(e));
            *kick = true;
        } else {
            // WRITE (endorse/lead): log it and reply now.
            out.push_back(ReplyCopy(e));
            spec_log_.push(id, std::move(e));
        }
        ++n;
    }
    return n;
}

uint64_t ServerOrderer::NextDeadlineLocked()
{
    if (early_buffer_.empty()) return Clock::kNever;
    return early_buffer_.peek_min().Order().t;
}

size_t ServerOrderer::ReleaseDue()
{
    std::vector<TxnEntry> released;
    bool kick = false;
    size_t n;
    {
        std::lock_guard<HybridMutex> lk(mu_);
        n = DrainDueLocked(released, &kick);
    }
    for (const auto &e : released) {
        if (on_spec_reply_) on_spec_reply_(e);
    }
    if (kick) KickExecutor();
    return n;
}

void ServerOrderer::ReleaseLoop()
{
    while (running_.load()) {
        std::vector<TxnEntry> released;
        bool kick = false;
        uint64_t next;
        {
            std::lock_guard<HybridMutex> lk(mu_);
            if (paused_.load()) {
                next = Clock::kNever;   // parked until SetPaused(false) interrupts us
            } else {
                DrainDueLocked(released, &kick);
                next = NextDeadlineLocked();
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

void ServerOrderer::Start()
{
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return;  // already running
    release_thread_ = std::thread(&ServerOrderer::ReleaseLoop, this);
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
        std::lock_guard<HybridMutex> lk(mu_);
        spec_log_.for_each([&](const TxnId &, const TxnEntry &e) {
            if (e.txn.type() != proto::WRITE) return;   // reads hold no state
            if (!TxnTouches(e.txn, in_delta)) return;
            d.txns.push_back(RecoveredTxn{e.txn, e.t, e.status});
        });
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

bool ServerOrderer::InstallRecoveredLocked(const RecoveredTxn &r, uint64_t now, bool *kick)
{
    const TxnId id = r.Key();
    if (executed_.find(id) != executed_.end()) return false;

    if (TxnEntry *e = spec_log_.find(id)) {
        if (StatusRank(r.status) <= StatusRank(e->status)) return false;   // never downgrade
        if (e->status == Status::SPECULATIVE) e->committing_us = now;
        e->t = r.t;
        e->status = r.status;
        highest_released_t_ = std::max(highest_released_t_, e->Order());
        spec_log_.update_key(id);   // `e` is invalid past this point
        if (r.status == Status::COMMITTED) *kick = true;
        return true;
    }

    TxnEntry e;
    if (early_buffer_.contains(id)) {
        // Not yet released here: it releases on its own at the client's t, which is what
        // a SPECULATIVE copy in M carries anyway.
        if (r.status == Status::SPECULATIVE) return false;
        e = *early_buffer_.find(id);
        early_buffer_.erase(id);
    } else if (auto it = late_buffer_.find(id); it != late_buffer_.end()) {
        e = std::move(it->second);
        late_buffer_.erase(it);
    } else {
        e.txn = r.txn;
        ComputeLeadership(e);
    }
    e.t = r.t;
    e.status = r.status;
    e.released_us = now;
    if (r.status != Status::SPECULATIVE) e.committing_us = now;
    highest_released_t_ = std::max(highest_released_t_, e.Order());
    spec_log_.push(id, std::move(e));
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
        std::lock_guard<HybridMutex> lk(mu_);
        const uint64_t now = clock_->Now();

        // 2. New view: every held entry's leadership follows the new owner map. Primary
        //    replication may have moved too, so let the (new) primary prepare promptly.
        topo_ = topo;
        view_id_ = view_id;
        std::vector<TxnId> ids;
        spec_log_.for_each([&](const TxnId &id, const TxnEntry &) { ids.push_back(id); });
        for (const TxnId &id : ids) {
            TxnEntry *e = spec_log_.find(id);
            ComputeLeadership(*e);   // leadership doesn't affect the OrderKey
            e->last_replicate_us = 0;
            e->prepare_replies.clear();
        }
        ids.clear();
        early_buffer_.for_each([&](const TxnId &id, const TxnEntry &) { ids.push_back(id); });
        for (const TxnId &id : ids) ComputeLeadership(*early_buffer_.find(id));
        for (auto &kv : late_buffer_) ComputeLeadership(kv.second);

        // 3. M txns.
        TxnSet in_m;
        for (const RecoveryData &m : ms) {
            for (const RecoveredTxn &r : m.txns) {
                in_m.insert(r.Key());
                if (InstallRecoveredLocked(r, now, &kick)) ++st.txns_installed;
            }
        }

        // 4. Leftovers: uncommitted entries in the failed range that no M carried. Kept;
        //    counted so they show up in testing.
        auto leftover = [&](const TxnEntry &e) {
            return e.status != Status::COMMITTED && e.txn.type() == proto::WRITE &&
                   in_m.find(e.Key()) == in_m.end() && TxnTouches(e.txn, in_failed_range);
        };
        spec_log_.for_each([&](const TxnId &, const TxnEntry &e) {
            if (leftover(e)) ++st.leftovers;
        });
        for (const auto &kv : late_buffer_) if (leftover(kv.second)) ++st.leftovers;
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
    std::lock_guard<HybridMutex> lk(mu_);
    early_buffer_ = decltype(early_buffer_)();
    spec_log_ = decltype(spec_log_)();
    late_buffer_.clear();
    executed_.clear();
    highest_released_t_ = OrderKey{};
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
    std::lock_guard<HybridMutex> lk(mu_);
    spec_log_.for_each([&](const TxnId &, const TxnEntry &e) {
        if (e.txn.type() == proto::WRITE && e.status != Status::SPECULATIVE) {
            out.push_back(RecoveredTxn{e.txn, e.t, e.status});
        }
    });
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
        std::lock_guard<HybridMutex> lk(mu_);
        const uint64_t now = clock_->Now();
        for (const RecoveredTxn &r : d.txns) InstallRecoveredLocked(r, now, &kick);
    }
    if (kick) KickExecutor();
    return applied;
}

bool ServerOrderer::LiveState(const TxnId &id, uint64_t *t, bool *is_leader)
{
    std::lock_guard<HybridMutex> lk(mu_);
    auto xit = executed_.find(id);
    if (xit != executed_.end()) {
        *t = xit->second.t;
        *is_leader = xit->second.is_leader;
        return true;
    }
    if (const TxnEntry *e = spec_log_.find(id)) {
        *t = e->t;
        *is_leader = e->is_leader_for_this;
        return true;
    }
    return false;
}

std::string ServerOrderer::DebugOldestEntry()
{
    std::lock_guard<HybridMutex> lk(mu_);
    const TxnEntry *best = nullptr;
    spec_log_.for_each([&](const TxnId &, const TxnEntry &e) {
        if (!best || e.Order() < best->Order()) best = &e;
    });
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
    std::lock_guard<HybridMutex> lk(mu_);
    return view_id_;
}

Topology ServerOrderer::topology()
{
    std::lock_guard<HybridMutex> lk(mu_);
    return topo_;
}

OrderKey ServerOrderer::highest_released_t()
{
    std::lock_guard<HybridMutex> lk(mu_);
    return highest_released_t_;
}

size_t ServerOrderer::early_size()
{
    std::lock_guard<HybridMutex> lk(mu_);
    return early_buffer_.size();
}

size_t ServerOrderer::late_size()
{
    std::lock_guard<HybridMutex> lk(mu_);
    return late_buffer_.size();
}

size_t ServerOrderer::speclog_size()
{
    std::lock_guard<HybridMutex> lk(mu_);
    return spec_log_.size();
}

bool ServerOrderer::speclog_contains(const TxnId &id)
{
    std::lock_guard<HybridMutex> lk(mu_);
    return spec_log_.contains(id);
}

bool ServerOrderer::speclog_entry(const TxnId &id, Status *status, uint64_t *t)
{
    std::lock_guard<HybridMutex> lk(mu_);
    const TxnEntry *e = spec_log_.find(id);
    if (!e) return false;
    if (status) *status = e->status;
    if (t) *t = e->t;
    return true;
}

bool ServerOrderer::executed(const TxnId &id)
{
    std::lock_guard<HybridMutex> lk(mu_);
    return executed_.find(id) != executed_.end();
}

void ServerOrderer::ReserveExecuted(size_t n)
{
    std::lock_guard<HybridMutex> lk(mu_);
    executed_.reserve(n);
}

size_t ServerOrderer::executed_count()
{
    std::lock_guard<HybridMutex> lk(mu_);
    return executed_.size();
}

} // namespace dyna
} // namespace specpaxos

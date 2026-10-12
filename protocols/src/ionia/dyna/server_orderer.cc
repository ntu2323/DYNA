// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/server_orderer.cc

#include "dyna/server_orderer.h"

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

void ServerOrderer::ComputeLeadership(TxnEntry &e) const
{
    e.led_keys.clear();
    for (const auto &k : e.txn.r_set()) {
        if (topo_.leaderOf(topo_.partitionOf(k)) == server_id_) e.led_keys.insert(k);
    }
    for (const auto &kv : e.txn.w_set()) {
        if (topo_.leaderOf(topo_.partitionOf(kv.key())) == server_id_)
            e.led_keys.insert(kv.key());
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
        std::lock_guard<std::mutex> lk(mu_);
        // A delayed / retried request for a txn we already executed and GC'd must not
        // be re-admitted (it would be re-applied on top of newer writes).
        if (executed_.find(key) != executed_.end()) return;

        const uint64_t now = clock_->Now();
        if (now <= entry.t) {
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
            spec_log_.push(key, entry);
            if (is_read) kick = true;
            else released.push_back(std::move(entry));
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
    std::lock_guard<std::mutex> lk(mu_);
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
    std::lock_guard<std::mutex> lk(mu_);
    if (executed_.find(id) != executed_.end() || spec_log_.contains(id) ||
        early_buffer_.contains(id) || late_buffer_.find(id) != late_buffer_.end()) {
        return;   // already have it (or already executed it)
    }
    late_buffer_[id] = std::move(e);   // a following ApplyCommit will adopt it
}

bool ServerOrderer::TryGetBody(const TxnId &id, proto::Txn *out)
{
    std::lock_guard<std::mutex> lk(mu_);
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
    std::lock_guard<std::mutex> lk(mu_);
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
    std::lock_guard<std::mutex> lk(mu_);

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
    std::lock_guard<std::mutex> lk(mu_);
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
    std::lock_guard<std::mutex> lk(mu_);
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
        std::lock_guard<std::mutex> lk(mu_);
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
    std::lock_guard<std::mutex> lk(mu_);
    if (executed_.find(id) != executed_.end()) return true;   // ack; never re-install
    InstallCommittingLocked(id, agreedT, body);
    return spec_log_.contains(id);
}

void ServerOrderer::MarkCommitted(const TxnId &id)
{
    bool kick = false;
    {
        std::lock_guard<std::mutex> lk(mu_);
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
    std::lock_guard<std::mutex> lk(mu_);
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
        std::lock_guard<std::mutex> lk(mu_);

        // Snapshot specLog pointers (stable while we hold mu_) and sort by OrderKey.
        std::vector<const TxnEntry *> snap;
        snap.reserve(spec_log_.size());
        spec_log_.for_each([&](const TxnId &, const TxnEntry &e) { snap.push_back(&e); });
        std::sort(snap.begin(), snap.end(),
                  [](const TxnEntry *a, const TxnEntry *b) { return a->Order() < b->Order(); });

        // Single ascending pass = the transitive conflict-dependency rule. A blocked
        // smaller txn propagates its block (via blocked_* keys) to larger conflicting txns.
        std::set<std::string> blocked_w, blocked_r;
        std::vector<TxnId> to_erase;
        for (const TxnEntry *e : snap) {
            bool conflict = false;
            for (const auto &kv : e->txn.w_set()) {       // w-w or w-r
                if (blocked_w.count(kv.key()) || blocked_r.count(kv.key())) { conflict = true; break; }
            }
            if (!conflict) {
                for (const auto &k : e->txn.r_set()) {    // r-w
                    if (blocked_w.count(k)) { conflict = true; break; }
                }
            }
            const bool is_write = (e->txn.type() == proto::WRITE);
            if (is_write && e->status == Status::COMMITTED && !conflict) {
                to_run.push_back(*e);
                to_erase.push_back(e->Key());
                executed.push_back(e->Key());
                executed_[e->Key()] = ExecutedInfo{e->t, e->is_leader_for_this};
            } else if (!is_write && !conflict) {
                // A READ whose conflicting smaller writes are all executed is ready: a
                // leader read is serviced (disk-read + reply); any read is GC'd so it
                // never blocks a later conflicting write.
                if (e->is_leader_for_this) to_run.push_back(*e);
                to_erase.push_back(e->Key());
            } else {
                // Not executable → block larger conflicting txns.
                for (const auto &kv : e->txn.w_set()) blocked_w.insert(kv.key());
                for (const auto &k : e->txn.r_set()) blocked_r.insert(k);
            }
        }
        for (const TxnId &id : to_erase) spec_log_.erase(id);
    }

    // Run outside the ordering lock, in OrderKey order. mem_apply is authoritative
    // (visible immediately); disk_persist is async / off the critical path (invariant 2).
    for (TxnEntry &e : to_run) {
        if (e.txn.type() == proto::WRITE) {
            for (const auto &kv : e.txn.w_set()) {
                if (backend_) {
                    backend_->mem_apply(kv.key(), kv.value());
                    backend_->disk_persist(kv.key(), kv.value());
                }
            }
        } else {
            // Leader READ: disk-read the keys THIS server leads; the client merges
            // results across involved leaders (spec § read path).
            for (const auto &k : e.txn.r_set()) {
                if (e.led_keys.count(k) && backend_) {
                    auto v = backend_->mem_read(k);
                    e.results[k] = v.value_or(std::string());
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
    while (true) {
        {
            std::unique_lock<std::mutex> lk(exec_mu_);
            exec_cv_.wait(lk, [this] { return exec_pending_ || !running_.load(); });
            if (!running_.load()) break;
            exec_pending_ = false;   // coalesce: kicks during the pass trigger one more
        }
        ExecuteReady();
    }
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
            spec_log_.push(id, e);
            out.push_back(std::move(e));
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
        std::lock_guard<std::mutex> lk(mu_);
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
            std::lock_guard<std::mutex> lk(mu_);
            DrainDueLocked(released, &kick);
            next = NextDeadlineLocked();
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

OrderKey ServerOrderer::highest_released_t()
{
    std::lock_guard<std::mutex> lk(mu_);
    return highest_released_t_;
}

size_t ServerOrderer::early_size()
{
    std::lock_guard<std::mutex> lk(mu_);
    return early_buffer_.size();
}

size_t ServerOrderer::late_size()
{
    std::lock_guard<std::mutex> lk(mu_);
    return late_buffer_.size();
}

size_t ServerOrderer::speclog_size()
{
    std::lock_guard<std::mutex> lk(mu_);
    return spec_log_.size();
}

bool ServerOrderer::speclog_contains(const TxnId &id)
{
    std::lock_guard<std::mutex> lk(mu_);
    return spec_log_.contains(id);
}

bool ServerOrderer::speclog_entry(const TxnId &id, Status *status, uint64_t *t)
{
    std::lock_guard<std::mutex> lk(mu_);
    const TxnEntry *e = spec_log_.find(id);
    if (!e) return false;
    if (status) *status = e->status;
    if (t) *t = e->t;
    return true;
}

bool ServerOrderer::executed(const TxnId &id)
{
    std::lock_guard<std::mutex> lk(mu_);
    return executed_.find(id) != executed_.end();
}

} // namespace dyna
} // namespace specpaxos

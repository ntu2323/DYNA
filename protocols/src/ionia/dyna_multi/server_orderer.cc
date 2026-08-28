// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_multi/server_orderer.cc  (Stage 2: fast-path release)

#include "dyna_multi/server_orderer.h"

#include <algorithm>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace specpaxos {
namespace dyna_multi {

ServerOrderer::ServerOrderer(uint32_t server_id, uint64_t view_id, Clock *clock,
                             Topology topo, OnSpecReply on_spec_reply,
                             StorageBackend *backend, OnReject on_reject)
    : server_id_(server_id),
      view_id_(view_id),
      quorum_f_(topo.n > 0 ? (topo.n - 1) / 2 : 0),
      clock_(clock),
      topo_(std::move(topo)),
      on_spec_reply_(std::move(on_spec_reply)),
      on_reject_(std::move(on_reject)),
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
    // Snapshot the id/order BEFORE any std::move(entry): reading entry.Key() in the
    // same call as std::move(entry) is unsequenced and would read a moved-from proto.
    const TxnId    key = entry.Key();
    const OrderKey ok  = entry.Order();
    const bool     is_read = (entry.txn.type() == proto::READ);
    std::vector<TxnEntry> released;   // late-leader WRITE immediate replies
    std::vector<TxnEntry> rejected;   // late-follower READ RejectReplies
    {
        std::lock_guard<std::mutex> lk(mu_);
        const uint64_t now = clock_->Now();

        // On-time iff BOTH: wall clock has not passed the deadline AND the order is
        // ahead of everything already released (the always-on counter-veto, DECISION
        // B). The veto is scoped to this client-arrival path; protocol-decided
        // deadlines bypass earlyBuffer entirely.
        const bool on_time = (now <= entry.t) && (ok > highest_released_t_);
        if (on_time) {
            early_buffer_.push(key, std::move(entry));
        } else if (entry.is_leader_for_this) {
            // Late-leader re-timestamp (spec § write/read path): bump t := clock(),
            // guaranteed > highestReleasedT, and release at the new t. A WRITE replies
            // immediately; a leader READ is held in specLog for the executor (disk-read).
            uint64_t new_t = now;
            if (new_t <= highest_released_t_.t) new_t = highest_released_t_.t + 1;
            entry.t = new_t;
            entry.status = Status::SPECULATIVE;
            entry.released_us = now;
            highest_released_t_ = entry.Order();
            const bool reply_now = !is_read;
            spec_log_.push(key, entry);
            if (reply_now) released.push_back(std::move(entry));
        } else if (is_read) {
            // READ late at a pure follower (spec § read path): RejectReply, don't buffer.
            rejected.push_back(std::move(entry));
        } else {
            // WRITE late at a pure follower: silently buffer (spec: writes silent).
            late_buffer_[key] = std::move(entry);
        }
    }
    for (const auto &e : released) if (on_spec_reply_) on_spec_reply_(e);
    for (const auto &e : rejected) if (on_reject_) on_reject_(e);
    if (poke) clock_->Interrupt();
}

ServerOrderer::CommitResult
ServerOrderer::ApplyCommit(const TxnId &id, uint64_t agreedT, TxnEntry *out)
{
    std::lock_guard<std::mutex> lk(mu_);

    // Already released: adopt agreedT in place (re-heapify) — protocol-decided, so
    // NOT subject to the counter-veto even if agreedT <= highestReleasedT.
    if (TxnEntry *e = spec_log_.find(id)) {
        e->t = agreedT;
        spec_log_.update_key(id);
        e->status = Status::COMMITTING;
        highest_released_t_ = std::max(highest_released_t_, e->Order());
        if (out) *out = *e;
        return CommitResult::kApplied;
    }
    // Admitted but not yet released: move earlyBuffer → specLog at agreedT.
    if (early_buffer_.contains(id)) {
        TxnEntry e = *early_buffer_.find(id);
        early_buffer_.erase(id);
        e.t = agreedT;
        e.status = Status::COMMITTING;
        highest_released_t_ = std::max(highest_released_t_, e.Order());
        spec_log_.push(id, e);
        if (out) *out = e;
        return CommitResult::kApplied;
    }
    // Buffered late (follower): move lateBuffer → specLog at agreedT.
    auto it = late_buffer_.find(id);
    if (it != late_buffer_.end()) {
        TxnEntry e = std::move(it->second);
        late_buffer_.erase(it);
        e.t = agreedT;
        e.status = Status::COMMITTING;
        highest_released_t_ = std::max(highest_released_t_, e.Order());
        spec_log_.push(id, e);
        if (out) *out = e;
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
    if (spec_log_.contains(id) || early_buffer_.contains(id) ||
        late_buffer_.find(id) != late_buffer_.end()) {
        return;   // already have it
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

void ServerOrderer::AdvanceTsLocked(const TxnId &id, TsAgreeAction *action,
                                    bool *has_action, bool *transitioned,
                                    TxnEntry *committed_out)
{
    *has_action = false;
    *transitioned = false;
    TxnEntry *e = spec_log_.find(id);
    if (!e || !e->is_leader_for_this) return;

    // Record our own proposal (our current live t).
    e->ts_proposals[server_id_] = e->t;
    const std::set<uint32_t> leaders = topo_.involvedLeaders(e->txn);

    // Send our proposal to any involved leader we haven't told yet.
    std::set<uint32_t> to_send;
    for (uint32_t L : leaders) {
        if (L != server_id_ && e->ts_sent_to.find(L) == e->ts_sent_to.end()) to_send.insert(L);
    }
    if (!to_send.empty()) {
        for (uint32_t L : to_send) e->ts_sent_to.insert(L);
        action->id = id;
        action->proposed_t = e->t;
        action->to_leaders = std::move(to_send);
        *has_action = true;
    }

    // Complete once every involved leader has a proposal → adopt max, COMMITTING.
    bool complete = true;
    for (uint32_t L : leaders) {
        if (e->ts_proposals.find(L) == e->ts_proposals.end()) { complete = false; break; }
    }
    if (complete && e->status == Status::SPECULATIVE) {
        uint64_t mx = 0;
        for (const auto &kv : e->ts_proposals) mx = std::max(mx, kv.second);
        e->t = mx;
        e->status = Status::COMMITTING;
        highest_released_t_ = std::max(highest_released_t_, e->Order());
        if (committed_out) *committed_out = *e;   // copy BEFORE the re-heapify
        spec_log_.update_key(id);                 // `e` is invalid past this point
        *transitioned = true;
    }
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
        TsAgreeAction action;
        bool has_action = false, transitioned = false;
        TxnEntry committed_entry;
        AdvanceTsLocked(id, &action, &has_action, &transitioned, &committed_entry);
        if (transitioned && committed) committed->push_back(std::move(committed_entry));
        if (has_action && actions) actions->push_back(std::move(action));
    }
}

ServerOrderer::TsRecordResult
ServerOrderer::RecordTsProposal(const TxnId &id, uint32_t from, uint64_t proposedT)
{
    TsRecordResult r;
    std::lock_guard<std::mutex> lk(mu_);
    TxnEntry *e = spec_log_.find(id);
    if (!e || !e->is_leader_for_this) return r;   // not led here / absent → ignore
    e->ts_proposals[from] = proposedT;
    AdvanceTsLocked(id, &r.action, &r.has_action, &r.transitioned, &r.entry);
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
    if (TxnEntry *e = spec_log_.find(id)) {
        if (e->status != Status::COMMITTED) {
            e->t = agreedT;
            e->status = Status::COMMITTING;
            highest_released_t_ = std::max(highest_released_t_, e->Order());
            spec_log_.update_key(id);
        }
        return;
    }
    if (early_buffer_.contains(id)) {
        TxnEntry e = *early_buffer_.find(id);
        early_buffer_.erase(id);
        e.t = agreedT;
        e.status = Status::COMMITTING;
        highest_released_t_ = std::max(highest_released_t_, e.Order());
        spec_log_.push(id, e);
        return;
    }
    auto it = late_buffer_.find(id);
    if (it != late_buffer_.end()) {
        TxnEntry e = std::move(it->second);
        late_buffer_.erase(it);
        e.t = agreedT;
        e.status = Status::COMMITTING;
        highest_released_t_ = std::max(highest_released_t_, e.Order());
        spec_log_.push(id, e);
        return;
    }
    // Not held anywhere: materialize from the body the leader included.
    TxnEntry e;
    e.txn = body;
    e.t = agreedT;
    e.status = Status::COMMITTING;
    ComputeLeadership(e);
    highest_released_t_ = std::max(highest_released_t_, e.Order());
    spec_log_.push(id, e);
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
        e->last_replicate_us = now;
        ReplicateAction a;
        a.id = id;
        a.agreed_t = e->t;
        a.body = e->txn;
        prepares->push_back(std::move(a));
    }
}

bool ServerOrderer::RecordPrepareReply(const TxnId &id, uint32_t from)
{
    std::lock_guard<std::mutex> lk(mu_);
    TxnEntry *e = spec_log_.find(id);
    if (!e || !IsPrimaryLeaderLocked(e->txn)) return false;
    if (from == server_id_) return false;
    e->prepare_replies.insert(from);
    if (e->status == Status::COMMITTING && e->prepare_replies.size() >= quorum_f_) {
        e->status = Status::COMMITTED;
        return true;   // caller broadcasts ReplicateCommit
    }
    return false;
}

bool ServerOrderer::HandleReplicatePrepare(const TxnId &id, uint64_t agreedT,
                                           const proto::Txn &body)
{
    std::lock_guard<std::mutex> lk(mu_);
    InstallCommittingLocked(id, agreedT, body);
    return spec_log_.contains(id);
}

void ServerOrderer::MarkCommitted(const TxnId &id)
{
    std::lock_guard<std::mutex> lk(mu_);
    if (TxnEntry *e = spec_log_.find(id)) e->status = Status::COMMITTED;
}

std::vector<TxnId> ServerOrderer::ExecuteReady()
{
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

size_t ServerOrderer::DrainDueLocked(std::vector<TxnEntry> &out)
{
    const uint64_t now = clock_->Now();
    size_t n = 0;
    while (!early_buffer_.empty()) {
        const OrderKey ok = early_buffer_.peek_min().Order();
        if (ok.t > now) break;                       // earliest not yet due
        const TxnId id = early_buffer_.peek_min_id();
        TxnEntry e = early_buffer_.pop_min();
        if (!(ok > highest_released_t_)) {
            // Release-time counter-veto: a protocol-inserted higher order passed
            // this entry. Drop (a leader would re-timestamp; handled elsewhere).
            continue;
        }
        e.status = Status::SPECULATIVE;
        e.released_us = now;
        highest_released_t_ = ok;                     // released in order => new max

        const bool is_read = (e.txn.type() == proto::READ);
        if (is_read && !e.is_leader_for_this) {
            // Follower READ endorsement (spec § read path): reply immediately, do NOT
            // log it (reads aren't ordered/executed on followers).
            out.push_back(std::move(e));
        } else {
            // WRITE (endorse/lead) or leader READ: log it. A WRITE replies now; a leader
            // READ is serviced later by the executor (disk-read once dependents execute).
            const bool reply_now = !is_read;
            spec_log_.push(id, e);
            if (reply_now) out.push_back(std::move(e));
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
    {
        std::lock_guard<std::mutex> lk(mu_);
        DrainDueLocked(released);
    }
    for (const auto &e : released) {
        if (on_spec_reply_) on_spec_reply_(e);
    }
    return released.size();
}

void ServerOrderer::ReleaseLoop()
{
    while (running_.load()) {
        std::vector<TxnEntry> released;
        uint64_t next;
        {
            std::lock_guard<std::mutex> lk(mu_);
            DrainDueLocked(released);
            next = NextDeadlineLocked();
        }
        for (const auto &e : released) {
            if (on_spec_reply_) on_spec_reply_(e);
        }
        if (!running_.load()) break;
        clock_->SleepUntil(next);   // parks until next deadline or Interrupt()
    }
}

void ServerOrderer::Start()
{
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return;  // already running
    release_thread_ = std::thread(&ServerOrderer::ReleaseLoop, this);
}

void ServerOrderer::Stop()
{
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false)) return; // not running
    clock_->Interrupt();
    if (release_thread_.joinable()) release_thread_.join();
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

} // namespace dyna_multi
} // namespace specpaxos

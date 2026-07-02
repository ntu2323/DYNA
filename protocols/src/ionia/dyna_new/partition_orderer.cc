// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "dyna_new/partition_orderer.h"

#include <algorithm>
#include <utility>

namespace specpaxos {
namespace dyna_new {

PartitionOrderer::PartitionOrderer(uint32_t partition_id, uint64_t view_id,
                                   StorageBackend *backend, Clock *clock,
                                   Role role, OnRelease on_release,
                                   uint32_t commit_quorum,
                                   uint64_t recon_interval_us,
                                   OnMaintenance on_maintenance,
                                   OnPostDrain on_post_drain)
    : partition_id_(partition_id), view_id_(view_id),
      backend_(backend), clock_(clock), role_(role),
      on_release_(std::move(on_release)),
      commit_quorum_(commit_quorum == 0 ? 1 : commit_quorum),
      recon_interval_us_(recon_interval_us),
      on_maintenance_(std::move(on_maintenance)),
      on_post_drain_(std::move(on_post_drain))
{
    state_.view_id = view_id;
}

PartitionOrderer::~PartitionOrderer()
{
    Stop();
}

void
PartitionOrderer::Submit(LogEntry entry)
{
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (role_ == Role::kFollower) {
            // Remember the body so reconciliation against the leader's sync can
            // place it into synced_log without searching the priority queue.
            known_[entry.Key()] = entry;
        }
        if (entry.deadline > state_.deadline_counter) {
            // On time: ordered by deadline in the early buffer (§6.1).
            fast_submits_.fetch_add(1, std::memory_order_relaxed);
            state_.early_buffer.push(std::move(entry));
        } else {
            slow_submits_.fetch_add(1, std::memory_order_relaxed);
            // Slow path (§6.2): late relative to the counter. Stage in the late
            // buffer, then reassign a deadline strictly past the counter (and
            // not in the past) so it is appended after already-released entries
            // rather than dropped.
            ClientAndReqId key = entry.Key();
            state_.late_buffer[key] = entry;
            entry.deadline = std::max(clock_->Now(), state_.deadline_counter + 1);
            state_.late_buffer.erase(key);
            state_.early_buffer.push(std::move(entry));
        }
    }
    // Wake the release loop so it re-evaluates the next deadline.
    clock_->Interrupt();
}

size_t
PartitionOrderer::ReleaseDue()
{
    std::vector<LogEntry> released;
    {
        std::lock_guard<std::mutex> lk(mu_);
        DrainDueLocked(released);
    }
    for (const LogEntry &e : released) {
        if (on_release_) on_release_(e);
    }
    if (!released.empty() && on_post_drain_) on_post_drain_();
    return released.size();
}

void
PartitionOrderer::DrainDueLocked(std::vector<LogEntry> &out)
{
    const uint64_t now = clock_->Now();
    while (!state_.early_buffer.empty() &&
           state_.early_buffer.top().deadline <= now) {
        LogEntry e = state_.early_buffer.top();
        state_.early_buffer.pop();
        state_.deadline_counter = e.deadline;   // monotonically non-decreasing

        if (role_ == Role::kFollower &&
            synced_index_.find(e.Key()) != synced_index_.end()) {
            // The leader's sync already reconciled this entry into synced_log
            // (it arrived before our speculative release). Skip the speculative
            // reply — reconciliation already replied with the leader's deadline.
            continue;
        }

        ReleaseEntryLocked(e);
        out.push_back(std::move(e));
    }
}

void
PartitionOrderer::ReleaseEntryLocked(LogEntry &e)
{
    if (e.type == proto::READ_ONLY) {
        // Reads are served off the release path and never logged or synced
        // (spec §6.3). Their position is the number of writes ordered before
        // them, so the leader and followers agree on log_pos without persisting
        // the read. Writes ordered before this read have already released (we
        // release in deadline order), so the read sees them in memory.
        if (role_ == Role::kLeader) {
            e.log_pos = state_.synced_log.size();
            auto v = backend_->mem_read(e.key);
            e.result = v.value_or(std::string());
            e.has_result = true;
        } else {
            e.log_pos = state_.unsynced_log.size();
        }
        return;
    }

    if (role_ == Role::kLeader) {
        // Leader is the source of truth: appended entries go straight to
        // synced_log and writes hit authoritative memory (§3.3, §6.1).
        e.log_pos = state_.synced_log.size();
        backend_->mem_apply(e.key, e.command);
        state_.synced_log.push_back(e);
        synced_index_[e.Key()] = e.log_pos;
        state_.sync_point = state_.synced_log.size();

        // Start the §7 confirmation tally: the leader's own append is the first
        // (implicit) confirmation. With a single replica (quorum 1) the write is
        // already committed, so persist it now; otherwise wait for sync_confirms.
        ConfirmState &cs = confirm_state_[e.Key()];
        cs.deadline = e.deadline;
        cs.released_us = clock_->Now();
        if (commit_quorum_ <= 1) {
            cs.committed = true;
            CommitLocked(e.Key());
            confirm_state_.erase(e.Key());
        }
    } else {
        // Follower: speculative release. Stage in unsynced_log without applying
        // to authoritative memory; reconciliation against the leader's sync
        // promotes/repositions and applies.
        e.log_pos = state_.unsynced_log.size();
        state_.unsynced_log.push_back(e);
        replied_deadline_[e.Key()] = e.deadline;   // we just replied at this deadline
    }
}

std::vector<ReconAction>
PartitionOrderer::HandleSync(const SyncIn &sync)
{
    std::vector<ReconAction> actions;
    std::lock_guard<std::mutex> lk(mu_);
    if (role_ != Role::kFollower) {
        return actions;   // leader is the source of truth; ignore (incl. self-sync)
    }

    if (sync.log_id < state_.sync_point) {
        // Already applied — re-confirm idempotently.
        ReconAction a;
        a.kind = ReconAction::Kind::kConfirm;
        a.entry.client_id = sync.client_id;
        a.entry.request_id = sync.request_id;
        a.log_id = sync.log_id;
        a.deadline = sync.deadline;
        actions.push_back(std::move(a));
        return actions;
    }

    // Buffer by log_id, then apply contiguously from sync_point (tolerates
    // reordered syncs and the gap left by a sync-miss awaiting a fetch).
    pending_syncs_[sync.log_id] = sync;
    DrainSyncsLocked(actions);
    return actions;
}

std::vector<ReconAction>
PartitionOrderer::HandleFetchResp(const LogEntry &body)
{
    std::vector<ReconAction> actions;
    std::lock_guard<std::mutex> lk(mu_);
    if (role_ != Role::kFollower) {
        return actions;
    }
    known_[body.Key()] = body;     // body now available; resume draining the gap
    DrainSyncsLocked(actions);
    return actions;
}

void
PartitionOrderer::DrainSyncsLocked(std::vector<ReconAction> &out)
{
    for (;;) {
        auto sit = pending_syncs_.find(state_.sync_point);
        if (sit == pending_syncs_.end()) {
            break;
        }
        const SyncIn sync = sit->second;
        const ClientAndReqId key{sync.client_id, sync.request_id};

        auto bit = known_.find(key);
        if (bit == known_.end()) {
            // Body missing (dropped/delayed): fetch it from the leader, then
            // stop — synced_log cannot advance past this gap (§6.4 not-found).
            if (fetch_inflight_.insert(sync.log_id).second) {
                ReconAction a;
                a.kind = ReconAction::Kind::kFetch;
                a.entry.client_id = sync.client_id;
                a.entry.request_id = sync.request_id;
                a.log_id = sync.log_id;
                out.push_back(std::move(a));
            }
            break;
        }

        LogEntry body = bit->second;     // copy (preserves client_addr)
        const bool replied =
            (replied_deadline_.find(key) != replied_deadline_.end());
        const bool changed = replied && replied_deadline_[key] != sync.deadline;

        body.deadline = sync.deadline;   // adopt the leader's authoritative deadline
        body.log_pos = sync.log_id;
        if (body.type != proto::READ_ONLY) {
            backend_->mem_apply(body.key, body.command);  // apply in leader order
        }
        state_.synced_log.push_back(body);
        synced_index_[key] = sync.log_id;
        state_.sync_point = state_.synced_log.size();

        fetch_inflight_.erase(sync.log_id);
        pending_syncs_.erase(sit);

        // A Commit may have raced ahead of this sync; apply it now (§7).
        if (pending_commits_.erase(key) > 0) {
            CommitLocked(key);
        }

        // Make sure the client can count this replica on the leader's deadline.
        // (The replica drops the reply if there is no client address, e.g. for a
        // fetched entry we never received directly — the client commits via the
        // other replicas in that case.)
        if (!replied) {
            ReconAction a;
            a.kind = ReconAction::Kind::kReply;           // first (endorsement) reply
            a.entry = body;
            out.push_back(std::move(a));
        } else if (changed) {
            ReconAction a;
            a.kind = ReconAction::Kind::kCorrectedReply;  // supersede stale reply
            a.entry = body;
            out.push_back(std::move(a));
        }
        replied_deadline_[key] = sync.deadline;

        ReconAction c;
        c.kind = ReconAction::Kind::kConfirm;
        c.entry.client_id = sync.client_id;
        c.entry.request_id = sync.request_id;
        c.log_id = sync.log_id;
        c.deadline = sync.deadline;
        out.push_back(std::move(c));
    }
}

bool
PartitionOrderer::LeaderFetch(uint64_t client_id, uint64_t request_id,
                             LogEntry &out) const
{
    std::lock_guard<std::mutex> lk(mu_);
    auto it = synced_index_.find(ClientAndReqId{client_id, request_id});
    if (it == synced_index_.end()) {
        return false;
    }
    out = state_.synced_log[it->second];
    return true;
}

bool
PartitionOrderer::HandleSyncConfirm(uint32_t replica_id, uint64_t client_id,
                                    uint64_t request_id, uint64_t deadline)
{
    std::lock_guard<std::mutex> lk(mu_);
    if (role_ != Role::kLeader) {
        return false;
    }
    const ClientAndReqId key{client_id, request_id};
    auto it = confirm_state_.find(key);
    if (it == confirm_state_.end()) {
        // Either already committed (and erased) or never released here. Ignore.
        return false;
    }
    ConfirmState &cs = it->second;
    if (cs.committed) {
        return false;
    }
    if (deadline != cs.deadline) {
        // A confirmation only counts when it agrees on the leader's deadline
        // (§7). A follower that had to change the deadline (slow path) will send
        // a confirm at the leader's value once reconciled.
        return false;
    }
    cs.confirmers.insert(replica_id);
    // +1 for the leader's own append (§7).
    if (1u + cs.confirmers.size() < commit_quorum_) {
        return false;
    }
    cs.committed = true;
    CommitLocked(key);
    confirm_state_.erase(it);
    return true;   // caller broadcasts Commit
}

void
PartitionOrderer::HandleCommit(uint64_t client_id, uint64_t request_id)
{
    std::lock_guard<std::mutex> lk(mu_);
    if (role_ != Role::kFollower) {
        return;
    }
    const ClientAndReqId key{client_id, request_id};
    if (synced_index_.find(key) == synced_index_.end()) {
        // Commit raced ahead of the sync; apply once reconciliation catches up.
        pending_commits_[key] = true;
        return;
    }
    CommitLocked(key);
}

void
PartitionOrderer::CommitLocked(const ClientAndReqId &key)
{
    auto it = synced_index_.find(key);
    if (it == synced_index_.end()) {
        return;
    }
    commit_count_.fetch_add(1, std::memory_order_relaxed);
    const uint64_t pos = it->second;
    const LogEntry &e = state_.synced_log[pos];
    if (e.type != proto::READ_ONLY) {
        // Off the critical path: enqueue the durable write (invariant 2). Reads
        // are never logged, so this is always a write.
        backend_->disk_persist(e.key, e.command);
    }
    // Advance commit_point over the contiguous committed prefix (§3.3).
    committed_positions_.insert(pos);
    while (committed_positions_.erase(state_.commit_point) > 0) {
        state_.commit_point++;
    }
}

void
PartitionOrderer::Start()
{
    bool expected = false;
    if (running_.compare_exchange_strong(expected, true)) {
        thread_ = std::thread(&PartitionOrderer::ReleaseLoop, this);
    }
}

void
PartitionOrderer::Stop()
{
    if (running_.exchange(false)) {
        clock_->Interrupt();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
}

void
PartitionOrderer::ReleaseLoop()
{
    while (running_.load()) {
        loop_wakeups_.fetch_add(1, std::memory_order_relaxed);

        std::vector<LogEntry> released;
        uint64_t next;
        {
            std::lock_guard<std::mutex> lk(mu_);
            DrainDueLocked(released);
            next = state_.early_buffer.empty()
                       ? Clock::kNever
                       : state_.early_buffer.top().deadline;
        }
        for (const LogEntry &e : released) {
            if (on_release_) on_release_(e);
        }
        if (!released.empty() && on_post_drain_) on_post_drain_();

        // Periodic self-healing: re-send Sync/Fetch so a lost control message
        // can't leave a permanent reconciliation gap. Off when interval is 0.
        if (on_maintenance_ && recon_interval_us_ > 0) {
            const uint64_t now = clock_->Now();
            if (now - last_maint_us_ >= recon_interval_us_) {
                last_maint_us_ = now;
                std::vector<LogEntry> resync;
                std::vector<ReconAction> fetches;
                {
                    std::lock_guard<std::mutex> lk(mu_);
                    CollectMaintenanceLocked(resync, fetches);
                }
                if (!resync.empty() || !fetches.empty()) {
                    on_maintenance_(resync, fetches);
                }
            }
            // Wake at least once per interval so healing keeps making progress.
            const uint64_t maint_next = last_maint_us_ + recon_interval_us_;
            if (maint_next < next) next = maint_next;
        }

        if (!running_.load()) {
            break;
        }
        // Blocks until the clock reaches `next` or Submit/Stop interrupts us.
        clock_->SleepUntil(next);
    }
}

void
PartitionOrderer::CollectMaintenanceLocked(std::vector<LogEntry> &resync,
                                           std::vector<ReconAction> &fetches)
{
    // Bound per-tick work; leftovers are picked up on subsequent ticks.
    static constexpr size_t kMaxPerTick = 256;
    const uint64_t now = clock_->Now();

    if (role_ == Role::kLeader) {
        // Re-sync ONLY the head-of-line unconfirmed entries. A follower drains
        // syncs contiguously, so it blocks at the lowest missing log_id; re-sending
        // later entries can't unblock it and just floods pending_syncs_ (that blind
        // re-sync-everything caused a healing storm — millions of re-syncs/s). The
        // uncommitted head sits at commit_point, so re-send a small window from
        // there, each entry at most once per backoff. Genuinely-stuck entries heal
        // in a few attempts; healthy in-flight entries are never touched.
        static constexpr uint64_t kHeadWindow = 16;
        static constexpr uint64_t kResyncBackoffUs = 20000;   // 20ms per entry
        const uint64_t end =
            std::min<uint64_t>(state_.sync_point, state_.commit_point + kHeadWindow);
        for (uint64_t pos = state_.commit_point; pos < end; ++pos) {
            const ClientAndReqId key = state_.synced_log[pos].Key();
            auto it = confirm_state_.find(key);
            if (it == confirm_state_.end() || it->second.committed) continue;
            if (now - it->second.released_us < recon_interval_us_) continue;
            if (it->second.last_resync_us != 0 &&
                now - it->second.last_resync_us < kResyncBackoffUs) continue;
            it->second.last_resync_us = now;
            resync.push_back(state_.synced_log[pos]);
            resync_count_.fetch_add(1, std::memory_order_relaxed);
        }
    } else {
        // Re-fetch bodies we are still blocked on (a fetch_resp was lost).
        size_t budget = kMaxPerTick;
        for (uint64_t log_id : fetch_inflight_) {
            if (budget == 0) break;
            auto sit = pending_syncs_.find(log_id);
            if (sit == pending_syncs_.end()) continue;
            ReconAction a;
            a.kind = ReconAction::Kind::kFetch;
            a.entry.client_id = sit->second.client_id;
            a.entry.request_id = sit->second.request_id;
            a.log_id = log_id;
            fetches.push_back(std::move(a));
            refetch_count_.fetch_add(1, std::memory_order_relaxed);
            --budget;
        }
    }
}

uint64_t
PartitionOrderer::deadline_counter() const
{
    std::lock_guard<std::mutex> lk(mu_);
    return state_.deadline_counter;
}

uint64_t
PartitionOrderer::sync_point() const
{
    std::lock_guard<std::mutex> lk(mu_);
    return state_.sync_point;
}

uint64_t
PartitionOrderer::commit_point() const
{
    std::lock_guard<std::mutex> lk(mu_);
    return state_.commit_point;
}

size_t
PartitionOrderer::synced_log_size() const
{
    std::lock_guard<std::mutex> lk(mu_);
    return state_.synced_log.size();
}

LogEntry
PartitionOrderer::synced_log_at(size_t i) const
{
    std::lock_guard<std::mutex> lk(mu_);
    return state_.synced_log.at(i);
}

size_t
PartitionOrderer::unsynced_log_size() const
{
    std::lock_guard<std::mutex> lk(mu_);
    return state_.unsynced_log.size();
}

LogEntry
PartitionOrderer::unsynced_log_at(size_t i) const
{
    std::lock_guard<std::mutex> lk(mu_);
    return state_.unsynced_log.at(i);
}

size_t
PartitionOrderer::early_buffer_size() const
{
    std::lock_guard<std::mutex> lk(mu_);
    return state_.early_buffer.size();
}

size_t
PartitionOrderer::late_buffer_size() const
{
    std::lock_guard<std::mutex> lk(mu_);
    return state_.late_buffer.size();
}

size_t
PartitionOrderer::confirm_state_size() const
{
    std::lock_guard<std::mutex> lk(mu_);
    return confirm_state_.size();
}

size_t
PartitionOrderer::pending_syncs_size() const
{
    std::lock_guard<std::mutex> lk(mu_);
    return pending_syncs_.size();
}

} // namespace dyna_new
} // namespace specpaxos

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "dyna_new/partition_orderer.h"

#include <algorithm>
#include <utility>

namespace specpaxos {
namespace dyna_new {

PartitionOrderer::PartitionOrderer(uint32_t partition_id, uint64_t view_id,
                                   StorageBackend *backend, Clock *clock,
                                   Role role, OnRelease on_release)
    : partition_id_(partition_id), view_id_(view_id),
      backend_(backend), clock_(clock), role_(role),
      on_release_(std::move(on_release))
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
            state_.early_buffer.push(std::move(entry));
        } else {
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
    if (role_ == Role::kLeader) {
        // Leader is the source of truth: appended entries go straight to
        // synced_log and writes hit authoritative memory (§3.3, §6.1).
        e.log_pos = state_.synced_log.size();
        if (e.type != proto::READ_ONLY) {
            backend_->mem_apply(e.key, e.command);
        }
        state_.synced_log.push_back(e);
        synced_index_[e.Key()] = e.log_pos;
        state_.sync_point = state_.synced_log.size();
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

        if (!running_.load()) {
            break;
        }
        // Blocks until the clock reaches `next` or Submit/Stop interrupts us.
        clock_->SleepUntil(next);
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

} // namespace dyna_new
} // namespace specpaxos

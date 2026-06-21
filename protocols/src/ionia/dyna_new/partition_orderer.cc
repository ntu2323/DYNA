// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "dyna_new/partition_orderer.h"

#include <algorithm>
#include <utility>

namespace specpaxos {
namespace dyna_new {

PartitionOrderer::PartitionOrderer(uint32_t partition_id, uint64_t view_id,
                                   StorageBackend *backend, Clock *clock)
    : partition_id_(partition_id), view_id_(view_id),
      backend_(backend), clock_(clock)
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
        if (entry.deadline > state_.deadline_counter) {
            // On time: ordered by deadline in the early buffer (§6.1).
            state_.early_buffer.push(std::move(entry));
        } else {
            // Slow path (§6.2): late relative to the counter. Stage in the late
            // buffer, then reassign a deadline that is strictly past the counter
            // (and not in the past) so it is appended after already-released
            // entries rather than dropped.
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
    std::lock_guard<std::mutex> lk(mu_);
    size_t before = state_.synced_log.size();
    ReleaseDueLocked();
    return state_.synced_log.size() - before;
}

void
PartitionOrderer::ReleaseDueLocked()
{
    const uint64_t now = clock_->Now();
    while (!state_.early_buffer.empty() &&
           state_.early_buffer.top().deadline <= now) {
        LogEntry e = state_.early_buffer.top();
        state_.early_buffer.pop();
        ReleaseEntryLocked(e);
    }
}

void
PartitionOrderer::ReleaseEntryLocked(LogEntry &e)
{
    e.log_pos = state_.synced_log.size();
    state_.deadline_counter = e.deadline;        // monotonically non-decreasing

    // Leader is the source of truth: appended entries go straight to synced_log
    // (the synced/unsynced split matters only for followers, added later).
    if (e.type != proto::READ_ONLY) {
        backend_->mem_apply(e.key, e.command);   // authoritative memory state
    }
    state_.synced_log.push_back(e);
    state_.sync_point = state_.synced_log.size();
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

        uint64_t next;
        {
            std::lock_guard<std::mutex> lk(mu_);
            ReleaseDueLocked();
            next = state_.early_buffer.empty()
                       ? Clock::kNever
                       : state_.early_buffer.top().deadline;
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

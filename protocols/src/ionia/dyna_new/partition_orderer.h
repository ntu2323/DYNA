// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/partition_orderer.h
//   Stage 3: the deadline-ordering mechanism for ONE partition, with this
//   replica acting as the partition leader, in isolation (no followers, no sync,
//   no client quorum yet). This is the heart of the protocol (spec §2, §3.2,
//   §6.1 release, §6.2 slow path).
//
//   Design for testability + no busy-wait:
//     * The pure release step (ReleaseDue) is synchronous and runs under the
//       partition lock, so correctness tests drive it deterministically with a
//       MockClock — no threads, no races.
//     * A thin background thread (Start/Stop) wraps it: it releases what is due,
//       then blocks in clock->SleepUntil(next_deadline). It wakes only when the
//       clock reaches that deadline or Submit/Stop calls Interrupt() — never
//       spins. loop_wakeups() lets a test assert the idle loop is quiescent.

#ifndef _DYNA_NEW_PARTITION_ORDERER_H_
#define _DYNA_NEW_PARTITION_ORDERER_H_

#include "dyna_new/partition_state.h"
#include "dyna_new/storage_backend.h"
#include "dyna_new/wall_clock.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <thread>

namespace specpaxos {
namespace dyna_new {

class PartitionOrderer {
public:
    PartitionOrderer(uint32_t partition_id, uint64_t view_id,
                     StorageBackend *backend, Clock *clock);
    ~PartitionOrderer();

    PartitionOrderer(const PartitionOrderer &) = delete;
    PartitionOrderer &operator=(const PartitionOrderer &) = delete;

    // Classify and buffer a request, then wake the release loop. On time
    // (deadline > deadline_counter) -> early_buffer; otherwise the slow path
    // (§6.2): stage in late_buffer, reassign deadline = max(now, counter+1),
    // move to early_buffer. Thread-safe.
    void Submit(LogEntry entry);

    // Release every entry whose deadline <= clock->Now(), in deadline order
    // (advance deadline_counter, append to synced_log, mem_apply writes).
    // Returns the number released. Synchronous; used by the loop and by tests.
    size_t ReleaseDue();

    // Background release-loop lifecycle (idempotent).
    void Start();
    void Stop();

    // --- inspection accessors (locked snapshots) ---
    uint64_t deadline_counter() const;
    uint64_t sync_point() const;
    size_t synced_log_size() const;
    LogEntry synced_log_at(size_t i) const;
    size_t early_buffer_size() const;
    size_t late_buffer_size() const;
    uint64_t loop_wakeups() const { return loop_wakeups_.load(); }

private:
    void ReleaseLoop();
    void ReleaseDueLocked();             // requires mu_ held
    void ReleaseEntryLocked(LogEntry &e); // requires mu_ held

    const uint32_t partition_id_;
    const uint64_t view_id_;
    StorageBackend *backend_;
    Clock *clock_;

    mutable std::mutex mu_;
    PartitionState state_;

    std::atomic<bool> running_{false};
    std::atomic<uint64_t> loop_wakeups_{0};
    std::thread thread_;
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_PARTITION_ORDERER_H_

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
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace specpaxos {
namespace dyna_new {

// A leader's sync message, as seen by a follower (spec §4, §6.4).
struct SyncIn {
    uint64_t view_id = 0;
    uint64_t log_id = 0;
    uint64_t client_id = 0;
    uint64_t request_id = 0;
    uint64_t deadline = 0;
};

// What the replica should send after a follower processes a sync / fetch_resp.
struct ReconAction {
    enum class Kind { kConfirm, kReply, kCorrectedReply, kFetch };
    Kind kind;
    LogEntry entry;        // body for replies (deadline set to the leader's); for
                           // confirm/fetch only client_id/request_id are used
    uint64_t log_id = 0;   // confirm
    uint64_t deadline = 0; // confirm
};

class PartitionOrderer {
public:
    // LEADER for the partition this replica owns; FOLLOWER for partitions it
    // merely tracks. The leader appends released entries to synced_log and
    // applies writes to memory; a follower stages them in unsynced_log (§3.3,
    // §6.1) and reconciles them into synced_log against the leader's syncs.
    enum class Role { kLeader, kFollower };

    // Invoked once per released entry, AFTER it is appended to the log, with the
    // partition lock released. The replica uses it to send the reply.
    using OnRelease = std::function<void(const LogEntry &)>;

    PartitionOrderer(uint32_t partition_id, uint64_t view_id,
                     StorageBackend *backend, Clock *clock,
                     Role role = Role::kLeader, OnRelease on_release = nullptr);
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

    // --- follower reconciliation (spec §6.4) ---
    // Process a leader sync. Builds synced_log in leader log_id order (buffering
    // out-of-order syncs); returns the messages the replica must send (confirm,
    // corrected/endorsement reply, or fetch when the body is missing). No-op for
    // a leader-role partition.
    std::vector<ReconAction> HandleSync(const SyncIn &sync);
    // Supply a fetched request body for a sync-miss, then resume draining.
    std::vector<ReconAction> HandleFetchResp(const LogEntry &body);

    // --- leader fetch service (spec §6.4 not-found case) ---
    // Look up a synced entry by ⟨client,req⟩ so the leader can answer a fetch.
    bool LeaderFetch(uint64_t client_id, uint64_t request_id, LogEntry &out) const;

    // --- inspection accessors (locked snapshots) ---
    uint64_t deadline_counter() const;
    uint64_t sync_point() const;
    size_t synced_log_size() const;
    LogEntry synced_log_at(size_t i) const;
    size_t unsynced_log_size() const;
    LogEntry unsynced_log_at(size_t i) const;
    size_t early_buffer_size() const;
    size_t late_buffer_size() const;
    uint64_t loop_wakeups() const { return loop_wakeups_.load(); }

private:
    void ReleaseLoop();
    // Releases all due entries into `out` (appends to the log + mem_apply under
    // the lock); the caller fires on_release for each after unlocking.
    void DrainDueLocked(std::vector<LogEntry> &out);
    void ReleaseEntryLocked(LogEntry &e);
    // Apply contiguous buffered syncs starting at sync_point (requires mu_).
    void DrainSyncsLocked(std::vector<ReconAction> &out);

    const uint32_t partition_id_;
    const uint64_t view_id_;
    StorageBackend *backend_;
    Clock *clock_;
    const Role role_;
    OnRelease on_release_;

    mutable std::mutex mu_;
    PartitionState state_;

    // Index over synced_log by ⟨client,req⟩ -> log_id. Leader: built on release
    // (answers fetches). Follower: built on reconciliation (de-dups speculative
    // release once an entry is synced).
    HashMap<ClientAndReqId, uint64_t> synced_index_;

    // Follower-only reconciliation state.
    HashMap<ClientAndReqId, LogEntry> known_;          // received request bodies
    HashMap<ClientAndReqId, uint64_t> replied_deadline_; // deadline last replied to client
    std::unordered_map<uint64_t, SyncIn> pending_syncs_; // out-of-order syncs by log_id
    std::unordered_set<uint64_t> fetch_inflight_;        // log_ids awaiting a fetch_resp

    std::atomic<bool> running_{false};
    std::atomic<uint64_t> loop_wakeups_{0};
    std::thread thread_;
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_PARTITION_ORDERER_H_

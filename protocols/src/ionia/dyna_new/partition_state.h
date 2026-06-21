// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/partition_state.h
//   Stage 1 (inert data types): the per-partition ordering state block from the
//   DYNA normal-operation spec (§3.2-3.3). Data structures only — no protocol
//   logic. A replica owns one PartitionState per partition (one it leads plus
//   2f it follows; §3).

#ifndef _DYNA_NEW_PARTITION_STATE_H_
#define _DYNA_NEW_PARTITION_STATE_H_

#include "common/log.h"                  // ClientAndReqId + std::hash<ClientAndReqId>
#include "lib/data_structure.h"          // HashMap (ankerl::unordered_dense::map)
#include "lib/transport.h"               // TransportAddress
#include "dyna_new/dyna-new-proto.pb.h"  // proto::RequestType

#include <cstdint>
#include <memory>
#include <queue>
#include <string>
#include <vector>

namespace specpaxos {
namespace dyna_new {

// One ordered request as it lives in a partition's buffers and logs.
//
// Deliberately POD-ish and trivially movable: no locks, atomics, or pointers to
// other entries. This is what lets a request later be moved cheaply through
// moodycamel queues when multithreading is introduced (Stage 8) without any
// lifetime-tracking machinery — unlike Nezha's heap-pointer entries.
struct LogEntry {
    uint64_t client_id  = 0;
    uint64_t request_id = 0;
    proto::RequestType type = proto::BLIND_WRITE;
    std::string key;
    std::string command;        // value for writes; empty for READ_ONLY
    uint64_t send_time = 0;      // µs, client's synchronized clock at send
    uint64_t deadline  = 0;      // µs; leader-authoritative once released/synced
    uint64_t log_pos   = 0;      // position in synced_log once appended

    std::string result;         // populated for RMW / read on the leader
    bool has_result = false;

    // Where to send this request's reply (set by the replica from the inbound
    // message; null for entries that need no client reply, e.g. sync-fetched).
    // shared_ptr keeps LogEntry cheaply movable through queues later.
    std::shared_ptr<TransportAddress> client_addr;

    ClientAndReqId Key() const { return ClientAndReqId{client_id, request_id}; }
};

// Deterministic early-buffer ordering, IDENTICAL across all replicas:
//   (deadline, client_id, request_id).
//
// This tie-break is load-bearing: the per-request commit rule is only sound if
// every replica orders ties the same way (spec §3 invariant 1, §9). It is
// deliberately NOT old dyna's comparator, which tied by a local arrival
// `sequence` — a per-replica value that is NOT identical across replicas.
//
// std::priority_queue is a max-heap, so we invert the comparison to put the
// smallest (earliest-deadline) entry on top, i.e. released first.
struct DeadlineGreater {
    bool operator()(const LogEntry &a, const LogEntry &b) const {
        if (a.deadline != b.deadline)   return a.deadline   > b.deadline;
        if (a.client_id != b.client_id) return a.client_id  > b.client_id;
        return a.request_id > b.request_id;
    }
};

using EarlyBuffer =
    std::priority_queue<LogEntry, std::vector<LogEntry>, DeadlineGreater>;

// All ordering state for a single partition (spec §3.2-3.3).
//
// Plain STL, intended to be guarded by a single per-partition lock once
// multithreading lands (Stage 8). The buffers themselves are NOT concurrent
// collections — the concurrent queues sit in front of this state, feeding it;
// this state is the heavy, lock-protected side.
struct PartitionState {
    // §3.1: per-partition view; bumped only by recovery (out of scope), but the
    // field exists so normal-operation matching logic is recovery-ready.
    uint64_t view_id = 0;

    // §3.2: arrived "on time" (deadline > deadline_counter), ordered by deadline.
    EarlyBuffer early_buffer;
    // §3.2: arrived "late" (deadline <= deadline_counter), keyed by request id.
    HashMap<ClientAndReqId, LogEntry> late_buffer;
    // §3.2: deadline of the last request released; monotonically non-decreasing.
    uint64_t deadline_counter = 0;

    // §3.3: synced/unsynced split. Leader appends straight to synced_log;
    // followers stage speculative releases in unsynced_log until reconciled.
    std::vector<LogEntry> synced_log;
    std::vector<LogEntry> unsynced_log;
    // §3.3: log position confirmed equal to the leader's.
    uint64_t sync_point = 0;
    // §3.3: log entries up to here are committed/stable (bounds background
    // persistence; later used by recovery).
    uint64_t commit_point = 0;
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_PARTITION_STATE_H_

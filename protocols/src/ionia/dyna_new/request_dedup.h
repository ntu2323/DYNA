// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/request_dedup.h
//   At-most-once request handling for the replica (spec §1.6, §6.1). Composes
//   two pieces of state per ⟨client_id, request_id⟩:
//     * an in-flight set  — requests admitted to the orderer but not yet replied;
//     * a reply cache (DedupTable) — the last reply we sent for a request.
//
//   This distinction matters once the client retransmits (spec §5): a retry that
//   arrives BEFORE the original releases must be dropped, not re-admitted, or the
//   orderer would hold two entries for the same request (different deadlines) and
//   execute it twice. A retry that arrives AFTER we replied is answered from the
//   cache without re-execution.
//
//   Pure logic, no internal locking (matches dedup_table.h) — the caller holds
//   the owning partition's lock.

#ifndef _DYNA_NEW_REQUEST_DEDUP_H_
#define _DYNA_NEW_REQUEST_DEDUP_H_

#include "common/log.h"                  // ClientAndReqId + std::hash<ClientAndReqId>
#include "dyna_new/dedup_table.h"        // DedupTable (reply cache)
#include "dyna_new/dyna-new-proto.pb.h"  // proto::Reply

#include <cstddef>
#include <deque>
#include <unordered_set>

namespace specpaxos {
namespace dyna_new {

class RequestDedup {
public:
    enum class Decision {
        kSubmit,  // genuinely new: admit to the orderer
        kDrop,    // duplicate of an in-flight request: ignore
        kResend,  // duplicate of an already-answered request: resend cached reply
    };

    // Classify an inbound request. On kSubmit the key is now marked in-flight.
    Decision OnReceive(const ClientAndReqId &key) {
        if (cache_.Contains(key)) {
            return Decision::kResend;
        }
        if (inflight_.find(key) != inflight_.end()) {
            return Decision::kDrop;
        }
        inflight_.insert(key);
        return Decision::kSubmit;
    }

    // Cached reply for a kResend decision (nullptr if none). The pointer is
    // invalidated by the next OnReply().
    const proto::Reply *CachedReply(const ClientAndReqId &key) const {
        return cache_.Lookup(key);
    }

    // Record the reply we just sent. Overwrites any prior reply, so a corrected
    // reply (§6.4) supersedes the speculative one in the cache. Clears in-flight.
    // The cache is LRU-bounded (cap_ > 0): the oldest reply is evicted once the
    // cap is exceeded. cap must exceed the client's retry window so a retried
    // request is never evicted before its (rare) retransmit (else it re-executes).
    void OnReply(const ClientAndReqId &key, const proto::Reply &reply) {
        const bool is_new = !cache_.Contains(key);
        cache_.Record(key, reply);
        inflight_.erase(key);
        if (is_new && cap_ > 0) {
            fifo_.push_back(key);   // each key inserted once (request_ids unique)
            while (cache_.Size() > cap_ && !fifo_.empty()) {
                cache_.Erase(fifo_.front());
                fifo_.pop_front();
            }
        }
    }

    // 0 = unbounded (default, for tests). Bounded in production to keep the cache
    // from growing with total ops (a latency-drift driver at scale).
    void set_cap(size_t cap) { cap_ = cap; }

    size_t inflight_size() const { return inflight_.size(); }
    size_t cached_size() const { return cache_.Size(); }

private:
    std::unordered_set<ClientAndReqId> inflight_;
    DedupTable cache_;
    std::deque<ClientAndReqId> fifo_;   // insertion order for LRU eviction
    size_t cap_ = 0;
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_REQUEST_DEDUP_H_

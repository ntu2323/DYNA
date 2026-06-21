// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/dedup_table.h
//   Stage 2: the at-most-once dedup table (spec §1.6, §6.1). Maps each
//   ⟨client_id, request_id⟩ to the reply previously sent for it, so a retried
//   request is answered from cache without re-execution.
//
//   Data structure only — no internal locking and no protocol wiring yet. In
//   later stages it lives inside a partition's state and is accessed under that
//   partition's lock, so synchronization is the caller's responsibility.

#ifndef _DYNA_NEW_DEDUP_TABLE_H_
#define _DYNA_NEW_DEDUP_TABLE_H_

#include "common/log.h"                  // ClientAndReqId + std::hash<ClientAndReqId>
#include "lib/data_structure.h"          // HashMap
#include "dyna_new/dyna-new-proto.pb.h"  // proto::Reply

#include <cstddef>

namespace specpaxos {
namespace dyna_new {

class DedupTable {
public:
    // Returns the cached reply for key, or nullptr if this request is new.
    // The pointer is invalidated by the next Record() call.
    const proto::Reply *Lookup(const ClientAndReqId &key) const {
        auto it = table_.find(key);
        return it == table_.end() ? nullptr : &it->second;
    }

    bool Contains(const ClientAndReqId &key) const {
        return table_.find(key) != table_.end();
    }

    // Cache (or overwrite) the reply for key.
    void Record(const ClientAndReqId &key, const proto::Reply &reply) {
        table_[key] = reply;
    }

    size_t Size() const { return table_.size(); }

private:
    HashMap<ClientAndReqId, proto::Reply> table_;
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_DEDUP_TABLE_H_

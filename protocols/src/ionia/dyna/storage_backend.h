// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/storage_backend.h
//   Abstract storage interface for executed writes. The orderer's executor thread
//   applies COMMITTED writes in dependency order (design doc: "disk-write threads
//   execute these write txns in batches and write them to disk") and services leader
//   reads from the same store, so an applied write is visible to reads immediately.
//
//   Durability model: commit never waits on the store (invariant 2) — a committed txn
//   is already held by f+1 replicas in memory, and a replica that loses unsynced store
//   state recovers it from its peers. So the store is NOT fsynced per write (same as
//   the IONIA baseline: SplinterDB with use_log=0).
//
//   Every value is stored with the commit timestamp `t` of the write that produced it
//   (recovery merges key-values by timestamp and installs them t-guarded).

#ifndef _DYNA_STORAGE_BACKEND_H_
#define _DYNA_STORAGE_BACKEND_H_

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace specpaxos {
namespace dyna {

struct VersionedValue {
    std::string value;
    uint64_t    t = 0;   // commit timestamp of the write that produced `value`
};

class StorageBackend {
public:
    virtual ~StorageBackend() = default;

    // Apply an executed write: key := value, committed at timestamp t. Visible to read()
    // as soon as it returns.
    virtual void apply(std::string_view key, std::string_view value, uint64_t t) = 0;
    // Current value of `key` and the commit timestamp that wrote it. Empty if absent.
    virtual std::optional<VersionedValue> read(std::string_view key) = 0;

    // Per-thread setup/teardown for stores that require it (SplinterDB registers each
    // thread that touches it). Called by the thread that will call apply()/read().
    virtual void RegisterThread() {}
    virtual void UnregisterThread() {}
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_STORAGE_BACKEND_H_

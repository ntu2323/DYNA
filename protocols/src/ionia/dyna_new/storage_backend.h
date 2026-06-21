// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/storage_backend.h
//   Stage 1 (inert data types): the abstract storage interface from the DYNA
//   normal-operation spec (§3.4). Interface only — no implementation.
//
//   Each replica layers two stores (spec §1.7):
//     * Memory KV  — authoritative committed state; a write becomes visible on
//                    release from the early buffer (mem_apply).
//     * Disk KV    — durable backing that lags memory; committed writes are
//                    persisted asynchronously, off the critical path (§7).
//
//   Stage 2 will provide concrete implementations. The intended reuse is to
//   wrap lib/db.h's `DB` interface rather than invent a KV engine:
//     * InMemoryBackend  -> lib/db.h InMemoryDB (with optional simulated
//                           write latency for disk_persist), and
//     * SplinterDBBackend -> lib/db.h SplinterDB,
//   so DYNA and the VR/IONIA baselines all run over the same backend for
//   apples-to-apples measurement (build plan Stage 9).

#ifndef _DYNA_NEW_STORAGE_BACKEND_H_
#define _DYNA_NEW_STORAGE_BACKEND_H_

#include <optional>
#include <string>
#include <string_view>

namespace specpaxos {
namespace dyna_new {

class StorageBackend {
public:
    virtual ~StorageBackend() = default;

    // Apply a committed write to the authoritative memory KV (release path).
    virtual void mem_apply(std::string_view key, std::string_view value) = 0;

    // Read from the memory KV (endorsement / fast read). Empty if absent.
    virtual std::optional<std::string> mem_read(std::string_view key) = 0;

    // Durably persist an entry. MUST be asynchronous / off the critical path
    // (spec §7, invariant 2): the calling thread must not block on disk I/O.
    virtual void disk_persist(std::string_view key, std::string_view value) = 0;

    // Read from the durable store (leader read path / recovery). Empty if absent.
    virtual std::optional<std::string> disk_read(std::string_view key) = 0;
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_STORAGE_BACKEND_H_

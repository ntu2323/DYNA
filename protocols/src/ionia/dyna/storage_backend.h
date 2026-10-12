// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/storage_backend.h
//   Abstract storage interface (spec §1.7 / §3.4). Two layers:
//     * Memory KV — authoritative committed state; a write becomes visible when it is
//                   executed in dependency order (mem_apply).
//     * Disk KV   — durable backing that lags memory; committed writes are persisted
//                   asynchronously, off the critical path (invariant 2).

#ifndef _DYNA_STORAGE_BACKEND_H_
#define _DYNA_STORAGE_BACKEND_H_

#include <optional>
#include <string>
#include <string_view>

namespace specpaxos {
namespace dyna {

class StorageBackend {
public:
    virtual ~StorageBackend() = default;

    // Apply an executed write to the authoritative memory KV.
    virtual void mem_apply(std::string_view key, std::string_view value) = 0;
    // Read from the memory KV (leader read path). Empty if absent.
    virtual std::optional<std::string> mem_read(std::string_view key) = 0;
    // Durably persist an entry. MUST be asynchronous / off the critical path.
    virtual void disk_persist(std::string_view key, std::string_view value) = 0;
    // Read from the durable store (recovery). Empty if absent.
    virtual std::optional<std::string> disk_read(std::string_view key) = 0;
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_STORAGE_BACKEND_H_

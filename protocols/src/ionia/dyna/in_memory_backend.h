// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/in_memory_backend.h
//   A StorageBackend backed by an in-memory map. Used by the unit tests (and as the
//   replica's fallback when no harness DB is supplied).

#ifndef _DYNA_IN_MEMORY_BACKEND_H_
#define _DYNA_IN_MEMORY_BACKEND_H_

#include "dyna/storage_backend.h"

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace specpaxos {
namespace dyna {

class InMemoryBackend : public StorageBackend {
public:
    void apply(std::string_view key, std::string_view value, uint64_t t) override;
    std::optional<VersionedValue> read(std::string_view key) override;

    // Convenience for tests: just the value ("" if absent).
    std::string value_or_empty(std::string_view key);
    uint64_t apply_count() const;

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, VersionedValue> kv_;
    uint64_t apply_count_ = 0;
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_IN_MEMORY_BACKEND_H_

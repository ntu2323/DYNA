// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/db_backend.h
//   StorageBackend over the bench harness's key-value store (lib/db.h): SplinterDB on
//   real hardware (IONIA_DB_MODE=splinter, the default) or lib's InMemoryDB
//   (IONIA_DB_MODE=memory). Sharing the harness DB gives DYNA exactly the IONIA
//   baseline's storage configuration (same cache/disk sizes, use_log=0, no fsync).
//
//   apply() is a synchronous insert — into SplinterDB's memtable, so it is readable
//   immediately and persisted by SplinterDB's own background flushing. It runs on the
//   orderer's executor thread, after commit, so it never sits on the commit path.

#ifndef _DYNA_DB_BACKEND_H_
#define _DYNA_DB_BACKEND_H_

#include "dyna/storage_backend.h"

class DB;   // lib/db.h (global namespace)

namespace specpaxos {
namespace dyna {

class DbBackend : public StorageBackend {
public:
    explicit DbBackend(DB *db) : db_(db) {}

    void apply(std::string_view key, std::string_view value, uint64_t t) override;
    std::optional<VersionedValue> read(std::string_view key) override;
    void RegisterThread() override;
    void UnregisterThread() override;

private:
    DB *db_;   // not owned (the harness AppReplica owns it)
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_DB_BACKEND_H_

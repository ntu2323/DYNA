// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "dyna/db_backend.h"

#include "dyna/value_codec.h"
#include "lib/db.h"
#include "lib/message.h"

namespace specpaxos {
namespace dyna {

void
DbBackend::apply(std::string_view key, std::string_view value, uint64_t t)
{
    const std::string stored = EncodeVersioned(value, t);
    if (db_->Insert(DB_TABLE, key, std::string_view(stored)) != DB::kOK) {
        Panic("dyna: store insert failed for key of %zu bytes", key.size());
    }
}

std::optional<VersionedValue>
DbBackend::read(std::string_view key)
{
    // lib/db.h's miss handling differs per store: InMemoryDB::Read returns kOK with an
    // EMPTY result on a miss (so the generic DB::Read(table, key) would index result[0]
    // and crash), while SplinterDB's 4-arg Read asserts on a miss but its
    // Read(table, key) handles one. Use whichever form is safe for this store.
    if (dynamic_cast<InMemoryDB *>(db_) != nullptr) {
        std::vector<DB::KVPair> result;
        if (db_->Read(DB_TABLE, std::string(key), nullptr, result) != DB::kOK ||
            result.empty()) {
            return std::nullopt;
        }
        return DecodeVersioned(result[0].second);
    }
    std::optional<std::string> stored = db_->Read(DB_TABLE, key);
    if (!stored) return std::nullopt;
    return DecodeVersioned(*stored);
}

void
DbBackend::RegisterThread()
{
    db_->Init();    // SplinterDB: splinterdb_register_thread (no-op for InMemoryDB)
}

void
DbBackend::UnregisterThread()
{
    db_->Close();   // SplinterDB: splinterdb_deregister_thread
}

} // namespace dyna
} // namespace specpaxos

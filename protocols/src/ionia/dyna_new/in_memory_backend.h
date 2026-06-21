// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/in_memory_backend.h
//   Stage 2: a StorageBackend backed by in-memory maps, for correctness
//   bring-up (spec §3.4). The memory KV is the authoritative state; the disk
//   KV lags it and is written asynchronously off the critical path (§7,
//   invariant 2). disk_persist enqueues to a background worker and returns
//   immediately; an optional simulated write latency models slow disk so tests
//   can confirm commit latency is decoupled from persistence (build plan §7).
//
//   SplinterDBBackend (later, build plan Stage 9) will wrap lib/db.h's
//   SplinterDB behind this same interface.

#ifndef _DYNA_NEW_IN_MEMORY_BACKEND_H_
#define _DYNA_NEW_IN_MEMORY_BACKEND_H_

#include "dyna_new/storage_backend.h"

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace specpaxos {
namespace dyna_new {

class InMemoryBackend : public StorageBackend {
public:
    // simulated_disk_latency_us: artificial delay applied to each background
    // disk write (0 = none). Use LatencyFromEnv() to honor the
    // DYNA_SIM_DISK_LATENCY_US environment variable.
    explicit InMemoryBackend(uint64_t simulated_disk_latency_us = 0);
    ~InMemoryBackend() override;

    void mem_apply(std::string_view key, std::string_view value) override;
    std::optional<std::string> mem_read(std::string_view key) override;
    void disk_persist(std::string_view key, std::string_view value) override;
    std::optional<std::string> disk_read(std::string_view key) override;

    // Reads DYNA_SIM_DISK_LATENCY_US (microseconds); 0 if unset/invalid.
    static uint64_t LatencyFromEnv();

    // Test helper: number of writes still queued for the disk worker.
    size_t disk_backlog() const;

private:
    void DiskWorkerLoop();

    struct WriteOp {
        std::string key;
        std::string value;
    };

    mutable std::mutex mem_mu_;
    std::unordered_map<std::string, std::string> mem_;

    mutable std::mutex disk_mu_;
    std::unordered_map<std::string, std::string> disk_;

    const uint64_t sim_latency_us_;

    // Background disk-write pipeline.
    mutable std::mutex q_mu_;
    std::condition_variable q_cv_;
    std::deque<WriteOp> queue_;
    bool stop_ = false;
    std::thread worker_;
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_IN_MEMORY_BACKEND_H_

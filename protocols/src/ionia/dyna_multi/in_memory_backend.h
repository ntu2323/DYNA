// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_multi/in_memory_backend.h
//   A StorageBackend backed by in-memory maps (copied from dyna_new). The memory KV
//   is authoritative; the disk KV lags it and is written asynchronously off the
//   critical path (invariant 2): disk_persist enqueues to a background worker and
//   returns immediately, with an optional simulated write latency.

#ifndef _DYNA_MULTI_IN_MEMORY_BACKEND_H_
#define _DYNA_MULTI_IN_MEMORY_BACKEND_H_

#include "dyna_multi/storage_backend.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace specpaxos {
namespace dyna_multi {

class InMemoryBackend : public StorageBackend {
public:
    explicit InMemoryBackend(uint64_t simulated_disk_latency_us = 0);
    ~InMemoryBackend() override;

    void mem_apply(std::string_view key, std::string_view value) override;
    std::optional<std::string> mem_read(std::string_view key) override;
    void disk_persist(std::string_view key, std::string_view value) override;
    std::optional<std::string> disk_read(std::string_view key) override;

    // Reads DYNA_SIM_DISK_LATENCY_US (microseconds); 0 if unset/invalid.
    static uint64_t LatencyFromEnv();

    size_t disk_backlog() const;
    uint64_t disk_persist_count() const { return disk_persist_count_.load(); }

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

    mutable std::mutex q_mu_;
    std::condition_variable q_cv_;
    std::deque<WriteOp> queue_;
    bool stop_ = false;
    std::thread worker_;

    std::atomic<uint64_t> disk_persist_count_{0};
};

} // namespace dyna_multi
} // namespace specpaxos

#endif // _DYNA_MULTI_IN_MEMORY_BACKEND_H_

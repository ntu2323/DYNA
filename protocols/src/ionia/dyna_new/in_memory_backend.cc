// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "dyna_new/in_memory_backend.h"

#include <chrono>
#include <cstdlib>

namespace specpaxos {
namespace dyna_new {

InMemoryBackend::InMemoryBackend(uint64_t simulated_disk_latency_us)
    : sim_latency_us_(simulated_disk_latency_us)
{
    worker_ = std::thread(&InMemoryBackend::DiskWorkerLoop, this);
}

InMemoryBackend::~InMemoryBackend()
{
    {
        std::lock_guard<std::mutex> lk(q_mu_);
        stop_ = true;
    }
    q_cv_.notify_all();
    if (worker_.joinable()) {
        worker_.join();
    }
}

void
InMemoryBackend::mem_apply(std::string_view key, std::string_view value)
{
    std::lock_guard<std::mutex> lk(mem_mu_);
    mem_[std::string(key)] = std::string(value);
}

std::optional<std::string>
InMemoryBackend::mem_read(std::string_view key)
{
    std::lock_guard<std::mutex> lk(mem_mu_);
    auto it = mem_.find(std::string(key));
    if (it == mem_.end()) {
        return std::nullopt;
    }
    return it->second;
}

void
InMemoryBackend::disk_persist(std::string_view key, std::string_view value)
{
    // Off the critical path: enqueue and return immediately. The calling
    // thread must never block on disk I/O (invariant 2).
    {
        std::lock_guard<std::mutex> lk(q_mu_);
        queue_.push_back(WriteOp{std::string(key), std::string(value)});
    }
    q_cv_.notify_one();
}

std::optional<std::string>
InMemoryBackend::disk_read(std::string_view key)
{
    std::lock_guard<std::mutex> lk(disk_mu_);
    auto it = disk_.find(std::string(key));
    if (it == disk_.end()) {
        return std::nullopt;
    }
    return it->second;
}

uint64_t
InMemoryBackend::LatencyFromEnv()
{
    const char *env = std::getenv("DYNA_SIM_DISK_LATENCY_US");
    if (env == nullptr) {
        return 0;
    }
    char *end = nullptr;
    unsigned long long v = std::strtoull(env, &end, 10);
    if (end == env || *end != '\0') {
        return 0;
    }
    return static_cast<uint64_t>(v);
}

size_t
InMemoryBackend::disk_backlog() const
{
    std::lock_guard<std::mutex> lk(q_mu_);
    return queue_.size();
}

void
InMemoryBackend::DiskWorkerLoop()
{
    for (;;) {
        WriteOp op;
        {
            std::unique_lock<std::mutex> lk(q_mu_);
            q_cv_.wait(lk, [this] { return stop_ || !queue_.empty(); });
            if (queue_.empty()) {
                // Only reached when stop_ is set and nothing remains: drain done.
                return;
            }
            op = std::move(queue_.front());
            queue_.pop_front();
        }

        // Simulate slow durable storage off the lock; the producer is unaffected.
        if (sim_latency_us_ > 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(sim_latency_us_));
        }

        {
            std::lock_guard<std::mutex> lk(disk_mu_);
            disk_[op.key] = op.value;
        }
    }
}

} // namespace dyna_new
} // namespace specpaxos

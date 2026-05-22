// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _DYNA_STATS_UTIL_H_
#define _DYNA_STATS_UTIL_H_

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <vector>

namespace specpaxos {
namespace dyna {

inline bool
EnvFlagEnabled(const char *name)
{
    const char *v = std::getenv(name);
    return v && *v && std::strcmp(v, "0") != 0;
}

inline uint64_t
EnvUint64OrDefault(const char *name, uint64_t defaultValue)
{
    const char *v = std::getenv(name);
    if (!v || !*v) {
        return defaultValue;
    }
    return static_cast<uint64_t>(std::strtoull(v, nullptr, 10));
}

/** Thread-safe latency samples; snapshot sorts and clears. */
struct LatencySampleBuffer {
    void record(uint64_t us)
    {
        std::lock_guard<std::mutex> lock(mu_);
        samples_.push_back(us);
    }

    struct Snapshot {
        uint64_t count = 0;
        uint64_t avg_us = 0;
        uint64_t p50_us = 0;
        uint64_t p99_us = 0;
        uint64_t max_us = 0;
    };

    Snapshot snapshotAndClear()
    {
        std::vector<uint64_t> local;
        {
            std::lock_guard<std::mutex> lock(mu_);
            local.swap(samples_);
        }
        Snapshot out;
        if (local.empty()) {
            return out;
        }
        out.count = local.size();
        uint64_t sum = 0;
        uint64_t maxVal = 0;
        for (uint64_t v : local) {
            sum += v;
            maxVal = std::max(maxVal, v);
        }
        out.avg_us = sum / out.count;
        out.max_us = maxVal;
        std::sort(local.begin(), local.end());
        out.p50_us = local[(local.size() - 1) / 2];
        out.p99_us = local[(local.size() * 99) / 100];
        return out;
    }

private:
    std::mutex mu_;
    std::vector<uint64_t> samples_;
};

struct CounterWindow {
    void add(uint64_t delta = 1) { value_.fetch_add(delta, std::memory_order_relaxed); }

    uint64_t snapshotAndReset()
    {
        return value_.exchange(0, std::memory_order_relaxed);
    }

    uint64_t load() const { return value_.load(std::memory_order_relaxed); }

private:
    std::atomic<uint64_t> value_{0};
};

} // namespace dyna
} // namespace specpaxos

#endif /* _DYNA_STATS_UTIL_H_ */

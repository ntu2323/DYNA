// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/stats.h
//   Opt-in (DYNA_STATS=1) per-second performance counters for the replica and the
//   client. Pure observation: lock-free relaxed atomics, no effect on protocol behavior.
//   Each Take() returns the value accumulated since the previous Take() and resets it,
//   so a once-per-second dump prints per-second rates / maxima.

#ifndef _DYNA_STATS_H_
#define _DYNA_STATS_H_

#include <atomic>
#include <string>
#include <cstdint>
#include <cstdlib>

namespace specpaxos {
namespace dyna {

inline bool StatsEnabledFromEnv()
{
    const char *s = std::getenv("DYNA_STATS");
    return s != nullptr && *s != '\0' && *s != '0';
}

struct StatCounter {
    std::atomic<uint64_t> v{0};
    void Add(uint64_t x = 1) { v.fetch_add(x, std::memory_order_relaxed); }
    uint64_t Take() { return v.exchange(0, std::memory_order_relaxed); }
};

struct StatMax {
    std::atomic<uint64_t> v{0};
    void Observe(uint64_t x) {
        uint64_t cur = v.load(std::memory_order_relaxed);
        while (x > cur && !v.compare_exchange_weak(cur, x, std::memory_order_relaxed)) {}
    }
    uint64_t Take() { return v.exchange(0, std::memory_order_relaxed); }
};

// Sum + count + max of a sampled quantity (e.g. µs).
struct StatDist {
    StatCounter sum, n;
    StatMax     max;
    void Observe(uint64_t x) { sum.Add(x); n.Add(); max.Observe(x); }
};

// Latency histogram (µs) for percentiles: 25 µs buckets up to 3.2 ms, then overflow.
struct StatHist {
    static constexpr uint64_t kBucketUs = 25;
    static constexpr size_t   kBuckets = 128;
    std::atomic<uint64_t> b[kBuckets + 1] = {};
    void Observe(uint64_t us) {
        const size_t i = static_cast<size_t>(us / kBucketUs);
        b[i < kBuckets ? i : kBuckets].fetch_add(1, std::memory_order_relaxed);
    }
    // "p50/p95/p99" (bucket upper bounds, µs; ">3200" past the last bucket), resets.
    std::string Take() {
        uint64_t c[kBuckets + 1], n = 0;
        for (size_t i = 0; i <= kBuckets; ++i) n += (c[i] = b[i].exchange(0, std::memory_order_relaxed));
        if (n == 0) return "-";
        auto pct = [&](double q) -> std::string {
            const uint64_t want = static_cast<uint64_t>(q * n);
            uint64_t acc = 0;
            for (size_t i = 0; i <= kBuckets; ++i) {
                acc += c[i];
                if (acc > want) {
                    return i == kBuckets ? std::string(">3200")
                                         : std::to_string((i + 1) * kBucketUs);
                }
            }
            return std::string(">3200");
        };
        return pct(0.50) + "/" + pct(0.95) + "/" + pct(0.99);
    }
};

// Orderer-side counters (filled by ServerOrderer when a stats sink is attached).
struct OrdererStats {
    StatCounter on_time;          // arrivals before their deadline -> earlyBuffer
    StatCounter late_leader;      // late at a leader -> re-timestamped
    StatCounter late_follower;    // late at a pure follower -> lateBuffer (silent)
    StatCounter duplicate;        // resend of a txn already held / executed
    StatDist    slack_us;         // on-time arrivals: deadline - arrival
    StatDist    lateness_us;      // late arrivals: arrival - deadline
    StatDist    release_lag_us;   // release time - deadline (release-loop timeliness)
    StatDist    exec_locked_us;   // ExecuteReady: time holding the ordering lock
    StatDist    exec_batch;       // ExecuteReady: txns executed per pass
    StatDist    exec_pass_us;       // executor pass wall time (snapshot..apply), µs
    StatDist    exec_apply_us;      // of which: applying writes / serving reads, µs
    StatDist    exec_sort_us;       // of which: sort + conflict scan (unlocked), µs
    StatDist    scan_locked_us;   // background scans (ts-agree / replicate / re-ack)
    StatDist    submit_wait_us;   // SubmitTxn: time waiting to acquire the ordering lock
    StatDist    mu_wait_us;       // every contended ordering-lock acquisition (any thread)
    // Ordering-lock hold time per call site class.
    StatDist    hold_submit_us;   // SubmitTxn
    StatDist    hold_commit_us;   // ApplyCommit (Fast/SlowCommit)
    StatDist    hold_repl_us;     // replication: prepare / ack / commit / take-prepare
    StatDist    hold_release_us;  // release loop drain
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_STATS_H_

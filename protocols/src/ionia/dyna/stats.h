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
    StatDist    scan_locked_us;   // background scans (ts-agree / replicate / re-ack)
    StatDist    submit_wait_us;   // SubmitTxn: time waiting to acquire the ordering lock
    StatDist    mu_wait_us;       // every contended ordering-lock acquisition (any thread)
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_STATS_H_

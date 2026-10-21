// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/hybrid_mutex.h
//   Spin-then-block mutex for the replica's short, hot critical sections (the ordering
//   lock, the dedup / pending / OWD maps). glibc's std::mutex goes straight to a futex
//   sleep on contention, and the sleep + wake-up costs far more than the few µs these
//   sections are held: on CloudLab the transport thread spent ~45% of its time blocked
//   in __lll_lock_wait while only ~10k ops/s flowed. Spinning briefly first keeps the
//   waiter on-CPU through a typical hold. DYNA_LOCK_SPIN sets the spin budget (pause
//   iterations; 0 = plain blocking mutex; default 2000: holds are ~1 µs, and spinning cut
//   the average contended wait 15 -> 5 µs at 35k ops/s, replica busy 103% -> 73%,
//   median 1.06 -> 0.55 ms). With a StatDist attached (DYNA_STATS) every
//   contended acquisition records its wait (µs).

#ifndef _DYNA_HYBRID_MUTEX_H_
#define _DYNA_HYBRID_MUTEX_H_

#include "dyna/stats.h"
#include "dyna/wall_clock.h"

#include <cstdlib>
#include <mutex>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

namespace specpaxos {
namespace dyna {

inline uint32_t LockSpinFromEnv()
{
    static const uint32_t spin = []() -> uint32_t {
        const char *v = std::getenv("DYNA_LOCK_SPIN");
        if (v == nullptr || *v == '\0') return 2000;
        return static_cast<uint32_t>(std::strtoul(v, nullptr, 10));
    }();
    return spin;
}

class HybridMutex {
public:
    void lock() {
        if (m_.try_lock()) return;
        const uint64_t t0 = wait_stats_ ? WallClockMicros() : 0;
        const uint32_t spin = LockSpinFromEnv();
        for (uint32_t i = 0; i < spin; ++i) {
            CpuRelax();
            if (m_.try_lock()) { Record(t0); return; }
        }
        m_.lock();
        Record(t0);
    }
    bool try_lock() { return m_.try_lock(); }
    void unlock() { m_.unlock(); }

    // Optional (DYNA_STATS): record contended waits. Set before concurrent use.
    void SetWaitStats(StatDist *d) { wait_stats_ = d; }

private:
    static void CpuRelax() {
#if defined(__x86_64__) || defined(__i386__)
        _mm_pause();
#endif
    }
    void Record(uint64_t t0) {
        if (wait_stats_) wait_stats_->Observe(WallClockMicros() - t0);
    }

    std::mutex m_;
    StatDist *wait_stats_ = nullptr;
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_HYBRID_MUTEX_H_

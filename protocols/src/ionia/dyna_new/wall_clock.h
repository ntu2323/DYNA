// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _DYNA_NEW_WALL_CLOCK_H_
#define _DYNA_NEW_WALL_CLOCK_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>

namespace specpaxos {
namespace dyna_new {

// Microsecond epoch from std::chrono::system_clock (NTP-comparable across hosts).
inline uint64_t WallClockMicros()
{
    using namespace std::chrono;
    return duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
}

// Clock seam for the deadline release loop (Stage 3).
//
// Beyond reading the time, the loop must *wait* until the next deadline without
// busy-waiting, and tests must be able to drive that wait deterministically with
// virtual time. So the clock owns the wait primitive:
//   - SleepUntil(t) blocks until Now() >= t OR Interrupt() is called.
//   - Interrupt() wakes a blocked (or about-to-block) SleepUntil; it is sticky,
//     so a notification issued just before SleepUntil is not lost.
// RealClock waits against real time; MockClock only advances when a test calls
// Set()/Advance(), which wake any SleepUntil — making "released exactly at the
// deadline" and "idle ⇒ no CPU" both testable.
class Clock {
public:
    static constexpr uint64_t kNever = UINT64_MAX;

    virtual ~Clock() = default;
    virtual uint64_t Now() = 0;             // current time, microseconds
    virtual void SleepUntil(uint64_t until_us) = 0;
    virtual void Interrupt() = 0;
};

class RealClock : public Clock {
public:
    // busy_poll: spin to a finite deadline instead of cv.wait_until, for
    // µs-precise release. Once the transport busy-polls, cv/scheduler wakeup
    // latency (~100s of µs on virtualized hosts) dominates commit latency.
    explicit RealClock(bool busy_poll = false) : busy_poll_(busy_poll) {}

    uint64_t Now() override { return WallClockMicros(); }

    void SleepUntil(uint64_t until_us) override {
        if (busy_poll_) {
            // Spin (cooperatively) until the deadline — and, crucially, keep
            // spinning when idle (kNever) too, until Submit/Stop interrupts.
            // At low load the early buffer is empty between requests, so parking
            // on the CV in the idle case would make EVERY request pay ~100µs+
            // cv/scheduler wakeup latency — exactly what busy-poll exists to
            // avoid, and the dominant term in the commit-latency tail. Spinning
            // keeps the idle→release wakeup at ~µs. (Costs a hot core per idle
            // partition; acceptable on the 8-vCPU perf boxes, matching the
            // transport's own busy-poll.)
            while (until_us == kNever || WallClockMicros() < until_us) {
                if (interrupted_.exchange(false)) return;  // Submit/Stop woke us
                std::this_thread::yield();                 // cooperative spin
            }
            interrupted_.store(false);
            return;
        }
        // Non-busy: park on the condition variable.
        std::unique_lock<std::mutex> lk(mu_);
        if (until_us == kNever) {
            cv_.wait(lk, [this] { return interrupted_.load(); });
        } else {
            std::chrono::system_clock::time_point tp{
                std::chrono::microseconds(until_us)};
            cv_.wait_until(lk, tp, [this] { return interrupted_.load(); });
        }
        interrupted_.store(false);
    }

    void Interrupt() override {
        interrupted_.store(true);
        { std::lock_guard<std::mutex> lk(mu_); }   // barrier vs a cv waiter
        cv_.notify_all();
    }

private:
    std::mutex mu_;
    std::condition_variable cv_;
    std::atomic<bool> interrupted_{false};
    bool busy_poll_ = false;
};

// Controllable clock for tests: virtual time only moves when told to, and every
// move wakes any waiter so the release loop re-evaluates deterministically.
class MockClock : public Clock {
public:
    explicit MockClock(uint64_t now_us = 0) : now_us_(now_us) {}

    uint64_t Now() override {
        std::lock_guard<std::mutex> lk(mu_);
        return now_us_;
    }

    void SleepUntil(uint64_t until_us) override {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [this, until_us] { return interrupted_ || now_us_ >= until_us; });
        interrupted_ = false;
    }

    void Interrupt() override {
        {
            std::lock_guard<std::mutex> lk(mu_);
            interrupted_ = true;
        }
        cv_.notify_all();
    }

    void Set(uint64_t now_us) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            now_us_ = now_us;
        }
        cv_.notify_all();
    }

    void Advance(uint64_t delta_us) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            now_us_ += delta_us;
        }
        cv_.notify_all();
    }

private:
    std::mutex mu_;
    std::condition_variable cv_;
    uint64_t now_us_;
    bool interrupted_ = false;
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_WALL_CLOCK_H_

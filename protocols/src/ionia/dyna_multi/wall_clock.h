// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_multi/wall_clock.h
//   Clock seam for the single per-server release loop. Copied from dyna_new (the
//   pattern is unchanged): a Clock owns both "read the time" and "wait until the
//   next deadline without busy-waiting", so tests can drive the wait deterministically
//   with virtual time (MockClock).
//     - SleepUntil(t) blocks until Now() >= t OR Interrupt() is called.
//     - Interrupt() wakes a blocked (or about-to-block) SleepUntil; it is sticky.

#ifndef _DYNA_MULTI_WALL_CLOCK_H_
#define _DYNA_MULTI_WALL_CLOCK_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>

namespace specpaxos {
namespace dyna_multi {

// Microsecond epoch from std::chrono::system_clock (NTP-comparable across hosts).
inline uint64_t WallClockMicros()
{
    using namespace std::chrono;
    return duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
}

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
    // busy_poll: spin to the deadline instead of cv.wait_until, for µs-precise
    // release once the transport already busy-polls (cv/scheduler wakeup latency
    // then dominates commit latency).
    explicit RealClock(bool busy_poll = false) : busy_poll_(busy_poll) {}

    uint64_t Now() override { return WallClockMicros(); }

    void SleepUntil(uint64_t until_us) override {
        if (busy_poll_) {
            while (until_us == kNever || WallClockMicros() < until_us) {
                if (interrupted_.exchange(false)) return;   // Submit/Stop woke us
                std::this_thread::yield();                  // cooperative spin
            }
            interrupted_.store(false);
            return;
        }
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

} // namespace dyna_multi
} // namespace specpaxos

#endif // _DYNA_MULTI_WALL_CLOCK_H_

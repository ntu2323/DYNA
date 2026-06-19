// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _DYNA_NEW_WALL_CLOCK_H_
#define _DYNA_NEW_WALL_CLOCK_H_

#include <chrono>
#include <cstdint>

namespace specpaxos {
namespace dyna_new {

// Microsecond epoch from std::chrono::system_clock (NTP-comparable across hosts).
// Wrap in a lambda or mock wrapper in unit tests that need a controllable clock.
inline uint64_t WallClockMicros()
{
    using namespace std::chrono;
    return duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
}

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_WALL_CLOCK_H_

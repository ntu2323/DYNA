// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _DYNA_WALL_CLOCK_H_
#define _DYNA_WALL_CLOCK_H_

#include <chrono>
#include <cstdint>

namespace specpaxos {
namespace dyna {

/** Microsecond epoch from std::chrono::system_clock (Nezha / GCP NTP-comparable). */
inline uint64_t WallClockMicros()
{
    using namespace std::chrono;
    return duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
}

} // namespace dyna
} // namespace specpaxos

#endif /* _DYNA_WALL_CLOCK_H_ */

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/cpu_pin.h
//   Optional CPU pinning for the replica's busy threads, from env:
//     DYNA_PIN_RX="a,b"  main receive loop + extra receive threads (in order)
//     DYNA_PIN_ORDER=c   ordering thread        DYNA_PIN_SEND=d   sender thread
//     DYNA_PIN_EXEC=e    executor thread
//   Unset = not pinned. Why: several threads busy-poll; one landing on the hyperthread
//   sibling of another spinning thread runs at roughly half speed, which made the
//   ordering thread's capacity swing ~20% run to run on CloudLab (65k ops/s collapsing
//   in one run, 75k clean in the next). start_dyna_replica.sh fills these with distinct
//   physical cores on the NIC's NUMA node.

#ifndef _DYNA_CPU_PIN_H_
#define _DYNA_CPU_PIN_H_

#include <pthread.h>
#include <sched.h>

#include <cstdlib>
#include <string>
#include <vector>

namespace specpaxos {
namespace dyna {

// The idx-th CPU in env var `name` ("3,5,7"), or -1 if unset / too short.
inline int CpuFromEnv(const char *name, size_t idx = 0)
{
    const char *v = std::getenv(name);
    if (v == nullptr || *v == '\0') return -1;
    std::vector<int> cpus;
    std::string s(v);
    size_t start = 0;
    while (start <= s.size()) {
        const size_t comma = s.find(',', start);
        const std::string tok = s.substr(start, comma == std::string::npos ? std::string::npos
                                                                           : comma - start);
        if (!tok.empty()) cpus.push_back(std::atoi(tok.c_str()));
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    return idx < cpus.size() ? cpus[idx] : -1;
}

// Pin the calling thread to CPU `cpu` (no-op if cpu < 0). Returns success.
inline bool PinSelf(int cpu)
{
    if (cpu < 0) return false;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
}

inline bool PinSelfFromEnv(const char *name, size_t idx = 0)
{
    return PinSelf(CpuFromEnv(name, idx));
}

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_CPU_PIN_H_

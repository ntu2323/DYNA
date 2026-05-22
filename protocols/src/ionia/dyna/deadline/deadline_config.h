// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _DYNA_DEADLINE_CONFIG_H_
#define _DYNA_DEADLINE_CONFIG_H_

#include <cstdint>

namespace specpaxos {
namespace dyna {

struct DeadlineConfig {
    /** Bound used until replicas publish usable OWD (microseconds). */
    uint64_t initial_bound_us = 500;
    /** Clamp per-replica OWD and aggregated bound (microseconds). */
    uint64_t max_owd_us = 10000;
    /** Recompute client bound after this many inbound replies carrying OWD > 0. */
    uint32_t recompute_every_k = 4;
    /** Sliding-window length per client_id on each replica. */
    uint32_t owd_window_len = 100;
    /** Percentile in [0, 100] over sorted window (50 ~= median). */
    uint32_t owd_percentile_percent = 50;

    /** Populate fields from DYNA_* environment variables (optional overrides). */
    static DeadlineConfig FromEnvironment();
};

} // namespace dyna
} // namespace specpaxos

#endif /* _DYNA_DEADLINE_CONFIG_H_ */

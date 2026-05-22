// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "dyna/deadline/deadline_config.h"

#include <cstdlib>

namespace specpaxos {
namespace dyna {

static uint64_t
ParseU64(const char *v, uint64_t fallback)
{
    if (!v || !*v) {
        return fallback;
    }
    unsigned long long x = std::strtoull(v, nullptr, 10);
    return static_cast<uint64_t>(x);
}

static uint32_t
ParseU32(const char *v, uint32_t fallback)
{
    if (!v || !*v) {
        return fallback;
    }
    unsigned long x = std::strtoul(v, nullptr, 10);
    return static_cast<uint32_t>(x);
}

DeadlineConfig
DeadlineConfig::FromEnvironment()
{
    DeadlineConfig c;
    const char *initial_env = std::getenv("DYNA_INITIAL_BOUND_US");
    if (!initial_env || !*initial_env) {
        initial_env = std::getenv("DYNA_DEADLINE_SLACK_US");
    }
    if (initial_env && *initial_env) {
        c.initial_bound_us = ParseU64(initial_env, c.initial_bound_us);
        if (c.initial_bound_us == 0) {
            c.initial_bound_us = 500;
        }
    }
    if (const char *v = std::getenv("DYNA_MAX_OWD_US"); v && *v) {
        c.max_owd_us = ParseU64(v, c.max_owd_us);
        if (c.max_owd_us == 0) {
            c.max_owd_us = 10000;
        }
    }
    if (const char *v = std::getenv("DYNA_BOUND_RECOMPUTE_K"); v && *v) {
        c.recompute_every_k = ParseU32(v, c.recompute_every_k);
        if (c.recompute_every_k == 0) {
            c.recompute_every_k = 1;
        }
    }
    if (const char *v = std::getenv("DYNA_OWD_WINDOW_LEN"); v && *v) {
        c.owd_window_len = ParseU32(v, c.owd_window_len);
        if (c.owd_window_len == 0) {
            c.owd_window_len = 100;
        }
    }
    if (const char *v = std::getenv("DYNA_OWD_PERCENTILE"); v && *v) {
        c.owd_percentile_percent = ParseU32(v, c.owd_percentile_percent);
        if (c.owd_percentile_percent > 100) {
            c.owd_percentile_percent = 100;
        }
    }
    return c;
}

} // namespace dyna
} // namespace specpaxos

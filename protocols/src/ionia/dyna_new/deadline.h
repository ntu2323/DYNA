// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/deadline.h
//   Stage 4: deadline / OWD-feedback machinery (spec §2). Self-contained
//   reimplementation (dyna/ is reference only, not a base). Nezha-style:
//     * ReplicaOwdEstimator (replica side): per-client sliding window, reports a
//       percentile of observed one-way delays.
//     * ClientBoundTracker (client side): bound = max over the most recent
//       per-replica OWD reports, clamped to [initial, max]; the client's
//       deadline is send_time + bound (folds the spec's s + d).
//   Tunables come from DYNA_* env vars (shared names with the old module).

#ifndef _DYNA_NEW_DEADLINE_H_
#define _DYNA_NEW_DEADLINE_H_

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <unordered_map>
#include <vector>

namespace specpaxos {
namespace dyna_new {

struct DeadlineConfig {
    uint64_t initial_bound_us = 500;     // cold-start bound before OWD is known
    uint64_t max_owd_us = 10000;         // clamp for per-replica OWD and bound
    uint32_t owd_window_len = 100;       // per-client sliding window length
    uint32_t owd_percentile_percent = 50; // percentile over the window (50 = median)

    static uint64_t EnvU64(const char *name, uint64_t fallback) {
        const char *v = std::getenv(name);
        if (v == nullptr || *v == '\0') return fallback;
        char *end = nullptr;
        unsigned long long parsed = std::strtoull(v, &end, 10);
        if (end == v || *end != '\0') return fallback;
        return static_cast<uint64_t>(parsed);
    }

    static DeadlineConfig FromEnv() {
        DeadlineConfig c;
        c.initial_bound_us = EnvU64("DYNA_INITIAL_BOUND_US", c.initial_bound_us);
        c.max_owd_us = EnvU64("DYNA_MAX_OWD_US", c.max_owd_us);
        c.owd_window_len =
            static_cast<uint32_t>(EnvU64("DYNA_OWD_WINDOW_LEN", c.owd_window_len));
        c.owd_percentile_percent =
            static_cast<uint32_t>(EnvU64("DYNA_OWD_PERCENTILE", c.owd_percentile_percent));
        if (c.owd_window_len == 0) c.owd_window_len = 1;
        if (c.owd_percentile_percent > 100) c.owd_percentile_percent = 100;
        return c;
    }
};

// Replica-side per-client OWD estimator (spec §2 "learning d").
class ReplicaOwdEstimator {
public:
    explicit ReplicaOwdEstimator(DeadlineConfig cfg) : cfg_(cfg) {}

    void sample(uint64_t client_id, uint64_t delay_us) {
        uint64_t clamped = std::min(delay_us, cfg_.max_owd_us);
        auto &w = windows_[client_id];
        if (w.size() < cfg_.owd_window_len) {
            w.push_back(clamped);
        } else {
            w[next_[client_id] % cfg_.owd_window_len] = clamped;
        }
        ++next_[client_id];
    }

    // Worst per-client OWD estimate (this is what drives a client's max-over-
    // replicas bound). Diagnostic only.
    uint64_t max_estimate_us() const {
        uint64_t m = 0;
        for (const auto &kv : windows_) {
            if (kv.second.empty()) continue;
            std::vector<uint64_t> sorted(kv.second);
            std::sort(sorted.begin(), sorted.end());
            size_t idx = (static_cast<size_t>(cfg_.owd_percentile_percent) *
                          (sorted.size() - 1)) / 100;
            if (sorted[idx] > m) m = sorted[idx];
        }
        return m;
    }

    // 0 means "not enough samples yet" (matches the protobuf default sentinel).
    uint64_t estimate_us(uint64_t client_id) const {
        auto it = windows_.find(client_id);
        if (it == windows_.end() || it->second.empty()) return 0;
        std::vector<uint64_t> sorted(it->second);
        std::sort(sorted.begin(), sorted.end());
        size_t idx = (static_cast<size_t>(cfg_.owd_percentile_percent) *
                      (sorted.size() - 1)) / 100;
        return sorted[idx];
    }

private:
    DeadlineConfig cfg_;
    std::unordered_map<uint64_t, std::vector<uint64_t>> windows_;
    std::unordered_map<uint64_t, uint64_t> next_;
};

// Client-side deadline bound: max over the latest per-replica OWD, clamped.
class ClientBoundTracker {
public:
    ClientBoundTracker(DeadlineConfig cfg, int num_replicas)
        : cfg_(cfg), last_owd_(std::max(1, num_replicas), 0),
          bound_us_(cfg.initial_bound_us) {}

    uint64_t bound_us() const { return bound_us_; }

    // Ignores 0 (unknown). replica_id must be within range.
    void on_owd_sample(uint32_t replica_id, uint64_t owd_us) {
        if (owd_us == 0 || replica_id >= last_owd_.size()) return;
        last_owd_[replica_id] = std::min(owd_us, cfg_.max_owd_us);
        recompute();
    }

private:
    void recompute() {
        uint64_t m = 0;
        for (uint64_t o : last_owd_) m = std::max(m, o);
        if (m == 0) {
            bound_us_ = cfg_.initial_bound_us;
        } else {
            // Floor by the cold-start bound (absorbs sync error s + jitter),
            // cap by max_owd.
            bound_us_ = std::min(cfg_.max_owd_us, std::max(cfg_.initial_bound_us, m));
        }
    }

    DeadlineConfig cfg_;
    std::vector<uint64_t> last_owd_;
    uint64_t bound_us_;
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_DEADLINE_H_

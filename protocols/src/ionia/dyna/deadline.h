// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/deadline.h
//   Deadline / OWD-feedback machinery (spec §2).
//     * ReplicaOwdEstimator (replica side): per-client sliding window, reports a
//       percentile of observed one-way delays.
//     * ClientBoundTracker (client side): bound = max over the most recent per-replica
//       OWD reports, clamped to [initial, max]; the client's deadline is
//       send_time + bound. Tunables from DYNA_* env vars (shared names).

#ifndef _DYNA_DEADLINE_H_
#define _DYNA_DEADLINE_H_

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <unordered_map>
#include <vector>

namespace specpaxos {
namespace dyna {

struct DeadlineConfig {
    uint64_t initial_bound_us = 500;
    uint64_t max_owd_us = 10000;
    uint32_t owd_window_len = 100;
    uint32_t owd_percentile_percent = 50;

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
        const uint64_t n = ++next_[client_id];
        static constexpr uint64_t kRecomputeEvery = 16;
        if (w.size() < cfg_.owd_window_len || (n % kRecomputeEvery) == 0 ||
            cache_.find(client_id) == cache_.end()) {
            cache_[client_id] = PercentileOf(w);
        }
    }

    uint64_t max_estimate_us() const {
        uint64_t m = 0;
        for (const auto &kv : cache_) if (kv.second > m) m = kv.second;
        return m;
    }

    uint64_t estimate_us(uint64_t client_id) const {
        auto it = cache_.find(client_id);
        return it == cache_.end() ? 0 : it->second;
    }

private:
    uint64_t PercentileOf(const std::vector<uint64_t> &w) const {
        if (w.empty()) return 0;
        std::vector<uint64_t> sorted(w);
        std::sort(sorted.begin(), sorted.end());
        size_t idx = (static_cast<size_t>(cfg_.owd_percentile_percent) *
                      (sorted.size() - 1)) / 100;
        return sorted[idx];
    }

    DeadlineConfig cfg_;
    std::unordered_map<uint64_t, std::vector<uint64_t>> windows_;
    std::unordered_map<uint64_t, uint64_t> next_;
    std::unordered_map<uint64_t, uint64_t> cache_;
};

// Client-side deadline bound: max over the latest per-replica OWD, clamped.
class ClientBoundTracker {
public:
    ClientBoundTracker(DeadlineConfig cfg, int num_replicas)
        : cfg_(cfg), last_owd_(std::max(1, num_replicas), 0),
          bound_us_(cfg.initial_bound_us) {}

    uint64_t bound_us() const { return bound_us_; }

    void on_owd_sample(uint32_t replica_id, uint64_t owd_us) {
        if (owd_us == 0 || replica_id >= last_owd_.size()) return;
        last_owd_[replica_id] = std::min(owd_us, cfg_.max_owd_us);
        recompute();
    }

    void on_observed_hold(uint64_t send_us, uint64_t reply_deadline_us) {
        if (reply_deadline_us <= send_us) return;
        const uint64_t hold_us = reply_deadline_us - send_us;
        if (hold_us > bound_us_) {
            const uint64_t capped = std::min(hold_us, kCongestionMaxUs);
            if (capped > congestion_bound_us_) congestion_bound_us_ = capped;
        } else if (congestion_bound_us_ > 0) {
            congestion_bound_us_ -= (congestion_bound_us_ / 32) + 1;
        }
        recompute();
    }

private:
    static constexpr uint64_t kCongestionMaxUs = 20000;

    void recompute() {
        uint64_t m = 0;
        for (uint64_t o : last_owd_) m = std::max(m, o);
        uint64_t owd_bound = (m == 0)
            ? cfg_.initial_bound_us
            : std::min(cfg_.max_owd_us, std::max(cfg_.initial_bound_us, m));
        bound_us_ = std::max(owd_bound, congestion_bound_us_);
    }

    DeadlineConfig cfg_;
    std::vector<uint64_t> last_owd_;
    uint64_t bound_us_;
    uint64_t congestion_bound_us_ = 0;
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_DEADLINE_H_

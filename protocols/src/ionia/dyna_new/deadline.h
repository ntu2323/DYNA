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
        const uint64_t n = ++next_[client_id];
        // Refresh the cached percentile periodically instead of sorting on every
        // estimate_us() call (the reply hot path — one sort per reply was a top
        // latency cost). Every sample while warming up (small window, cheap, keeps
        // tests exact), then every kRecomputeEvery once the window is full.
        static constexpr uint64_t kRecomputeEvery = 16;
        if (w.size() < cfg_.owd_window_len || (n % kRecomputeEvery) == 0 ||
            cache_.find(client_id) == cache_.end()) {
            cache_[client_id] = PercentileOf(w);
        }
    }

    // Worst per-client OWD estimate (this is what drives a client's max-over-
    // replicas bound). Diagnostic only.
    uint64_t max_estimate_us() const {
        uint64_t m = 0;
        for (const auto &kv : cache_) if (kv.second > m) m = kv.second;
        return m;
    }

    // O(1) cache lookup (0 = not enough samples yet, the protobuf default sentinel).
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
    std::unordered_map<uint64_t, uint64_t> cache_;   // client -> cached percentile
};

// Client-side deadline bound: max over the latest per-replica OWD, clamped.
class ClientBoundTracker {
public:
    ClientBoundTracker(DeadlineConfig cfg, int num_replicas)
        : cfg_(cfg), last_owd_(std::max(1, num_replicas), 0),
          bound_us_(cfg.initial_bound_us) {}

    uint64_t bound_us() const { return bound_us_; }

    // Diagnostic: latest per-replica OWD (index = replica_id). Exposes the "fast
    // everywhere?" question — bound = max over these, so one slow replica inflates
    // every request's deadline.
    const std::vector<uint64_t> &per_replica_owd() const { return last_owd_; }

    // Ignores 0 (unknown). replica_id must be within range.
    void on_owd_sample(uint32_t replica_id, uint64_t owd_us) {
        if (owd_us == 0 || replica_id >= last_owd_.size()) return;
        last_owd_[replica_id] = std::min(owd_us, cfg_.max_owd_us);
        recompute();
    }

    // Congestion feedback (§6.2): the leader's ACTUAL hold for a committed request =
    // reply.deadline − send. On the fast path this is ≤ our bound (the leader honored
    // our deadline). If it EXCEEDS our bound, the request arrived after its deadline
    // and the leader had to reassign it (slow path) — a direct signal that the bound
    // is too tight for the current load. We raise a separate `congestion_bound_us_`
    // toward that observed hold (bounded by kCongestionMaxUs) so future deadlines are
    // generous enough to stay on the fast path, and decay it back when the network
    // clears. This keeps the OWD-driven bound tight at low load (preserving the
    // low-latency floor) while preventing the slow-path/healing-storm collapse under
    // load. See the collapse analysis: bound=40 vs OWD=2000 → 88% slow path.
    void on_observed_hold(uint64_t send_us, uint64_t reply_deadline_us) {
        if (reply_deadline_us <= send_us) return;
        const uint64_t hold_us = reply_deadline_us - send_us;
        if (hold_us > bound_us_) {
            const uint64_t capped = std::min(hold_us, kCongestionMaxUs);
            if (capped > congestion_bound_us_) congestion_bound_us_ = capped;
        } else if (congestion_bound_us_ > 0) {
            // Healthy (fast path): relax the congestion allowance back toward 0.
            congestion_bound_us_ -= (congestion_bound_us_ / 32) + 1;
        }
        recompute();
    }

private:
    static constexpr uint64_t kCongestionMaxUs = 20000;  // 20ms ceiling on the hold

    void recompute() {
        uint64_t m = 0;
        for (uint64_t o : last_owd_) m = std::max(m, o);
        // OWD-driven bound: tight, capped by max_owd (keeps the low-load floor).
        uint64_t owd_bound = (m == 0)
            ? cfg_.initial_bound_us
            : std::min(cfg_.max_owd_us, std::max(cfg_.initial_bound_us, m));
        // Under congestion the observed slow-path hold can exceed max_owd; let it.
        bound_us_ = std::max(owd_bound, congestion_bound_us_);
    }

    DeadlineConfig cfg_;
    std::vector<uint64_t> last_owd_;
    uint64_t bound_us_;
    uint64_t congestion_bound_us_ = 0;   // slow-path-driven headroom above max_owd
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_DEADLINE_H_

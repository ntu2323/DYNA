// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "dyna/deadline/client_bound_tracker.h"

#include <algorithm>

namespace specpaxos {
namespace dyna {

ClientBoundTracker::ClientBoundTracker(const DeadlineConfig &cfg, int numReplicas)
    : cfg_(cfg),
      numReplicas_(numReplicas > 0 ? numReplicas : 1),
      last_owd_(static_cast<size_t>(numReplicas_), cfg.initial_bound_us),
      bound_us_(cfg.initial_bound_us)
{
}

void
ClientBoundTracker::recompute_bound()
{
    uint64_t mx = 0;
    for (uint64_t v : last_owd_) {
        mx = std::max(mx, v);
    }
    bound_us_ = std::min(mx, cfg_.max_owd_us);
    if (bound_us_ == 0) {
        bound_us_ = cfg_.initial_bound_us;
    }
}

void
ClientBoundTracker::on_owd_sample(uint32_t replica_id, uint64_t owd_us)
{
    if (owd_us == 0 || replica_id >= static_cast<uint32_t>(numReplicas_)) {
        return;
    }
    owd_us = std::min(owd_us, cfg_.max_owd_us);
    last_owd_[replica_id] = owd_us;

    replies_since_recompute_++;
    if (replies_since_recompute_ >= cfg_.recompute_every_k) {
        replies_since_recompute_ = 0;
        recompute_bound();
    }
}

} // namespace dyna
} // namespace specpaxos

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _DYNA_CLIENT_BOUND_TRACKER_H_
#define _DYNA_CLIENT_BOUND_TRACKER_H_

#include "dyna/deadline/deadline_config.h"

#include <cstdint>
#include <vector>

namespace specpaxos {
namespace dyna {

/** Event-driven Nezha-style bound: max over last known per-replica OWD, clamped. */
class ClientBoundTracker
{
public:
    ClientBoundTracker(const DeadlineConfig &cfg, int numReplicas);

    uint64_t bound_us() const { return bound_us_; }

    /** Ignore owd_us == 0 (unknown). Replica index must be < numReplicas. */
    void on_owd_sample(uint32_t replica_id, uint64_t owd_us);

private:
    void recompute_bound();

    DeadlineConfig cfg_;
    int numReplicas_;
    std::vector<uint64_t> last_owd_;
    uint64_t bound_us_;
    uint32_t replies_since_recompute_ = 0;
};

} // namespace dyna
} // namespace specpaxos

#endif /* _DYNA_CLIENT_BOUND_TRACKER_H_ */

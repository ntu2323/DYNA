// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _DYNA_REPLICA_OWD_ESTIMATOR_H_
#define _DYNA_REPLICA_OWD_ESTIMATOR_H_

#include "dyna/deadline/deadline_config.h"

#include <cstdint>
#include <unordered_map>
#include <vector>

namespace specpaxos {
namespace dyna {

/** Per-client sliding-window percentile delay estimate (Nezha-style, replica-side). */
class ReplicaOwdEstimator
{
public:
    explicit ReplicaOwdEstimator(DeadlineConfig cfg);

    void sample(uint64_t client_id, uint64_t delay_us);

    /** 0 means insufficient samples yet (like Nezha protobuf default). */
    uint64_t estimate_us(uint64_t client_id) const;

private:
    struct ClientState {
        std::vector<uint64_t> ring;
        uint64_t sample_index = 0;
        uint64_t estimate_us = 0;
    };

    DeadlineConfig cfg_;
    std::unordered_map<uint64_t, ClientState> clients_;
};

} // namespace dyna
} // namespace specpaxos

#endif /* _DYNA_REPLICA_OWD_ESTIMATOR_H_ */

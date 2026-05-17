// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "dyna/deadline/replica_owd_estimator.h"

#include <algorithm>

namespace specpaxos {
namespace dyna {

ReplicaOwdEstimator::ReplicaOwdEstimator(DeadlineConfig cfg)
    : cfg_(cfg)
{
    if (cfg_.owd_window_len == 0) {
        cfg_.owd_window_len = 100;
    }
}

void
ReplicaOwdEstimator::sample(uint64_t client_id, uint64_t delay_us)
{
    const uint32_t wlen = cfg_.owd_window_len;
    ClientState &st = clients_[client_id];
    if (st.ring.size() < static_cast<size_t>(wlen)) {
        st.ring.push_back(delay_us);
    } else {
        st.ring[static_cast<size_t>(st.sample_index % wlen)] = delay_us;
    }
    st.sample_index++;

    if (st.sample_index < static_cast<uint64_t>(wlen)) {
        return;
    }

    std::vector<uint64_t> tmp = st.ring;
    std::sort(tmp.begin(), tmp.end());
    size_t idx = (tmp.size() * static_cast<size_t>(cfg_.owd_percentile_percent)) / 100;
    if (idx >= tmp.size()) {
        idx = tmp.size() > 0 ? tmp.size() - 1 : 0;
    }
    uint64_t est = tmp.empty() ? 0 : tmp[idx];
    if (est > cfg_.max_owd_us) {
        est = cfg_.max_owd_us;
    }
    st.estimate_us = est;
}

uint64_t
ReplicaOwdEstimator::estimate_us(uint64_t client_id) const
{
    auto it = clients_.find(client_id);
    if (it == clients_.end()) {
        return 0;
    }
    const ClientState &st = it->second;
    if (st.sample_index < static_cast<uint64_t>(cfg_.owd_window_len)) {
        return 0;
    }
    return st.estimate_us;
}

} // namespace dyna
} // namespace specpaxos

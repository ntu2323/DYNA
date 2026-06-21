// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/partition_map.h
//   Static key -> partition -> leader mapping (spec §1.2). The key space is
//   split into `num_partitions` (= number of replicas, 2f+1) disjoint ranges;
//   replica i is the leader of partition i. Shared verbatim by client and
//   replica so routing and leadership agree.

#ifndef _DYNA_NEW_PARTITION_MAP_H_
#define _DYNA_NEW_PARTITION_MAP_H_

#include <cstdint>
#include <functional>
#include <string>

namespace specpaxos {
namespace dyna_new {

inline uint32_t PartitionOf(const std::string &key, uint32_t num_partitions) {
    if (num_partitions == 0) return 0;
    return static_cast<uint32_t>(std::hash<std::string>()(key) % num_partitions);
}

inline uint32_t LeaderOf(uint32_t partition_id, uint32_t num_replicas) {
    if (num_replicas == 0) return 0;
    return partition_id % num_replicas;
}

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_PARTITION_MAP_H_

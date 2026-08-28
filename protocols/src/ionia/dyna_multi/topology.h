// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_multi/topology.h
//   Static, config-derived leadership map (spec §0.1-0.2, §1.6). Shared-replica
//   model: N = 2f+1 servers, the key range split into N partitions, server i leads
//   partition i and follows the rest. Identical on every node, so involvedLeaders()
//   is computed the same way on client and server.

#ifndef _DYNA_MULTI_TOPOLOGY_H_
#define _DYNA_MULTI_TOPOLOGY_H_

#include "dyna_multi/dyna-multi-proto.pb.h"

#include <cstdint>
#include <functional>
#include <set>
#include <string>

namespace specpaxos {
namespace dyna_multi {

struct Topology {
    uint32_t n = 1;   // number of servers == number of partitions

    explicit Topology(uint32_t n_ = 1) : n(n_) {}

    // Deterministic key -> partition mapping, known to all (spec §1.4).
    static uint32_t PartitionOf(const std::string &key, uint32_t n) {
        return static_cast<uint32_t>(std::hash<std::string>()(key) % n);
    }
    uint32_t partitionOf(const std::string &key) const { return PartitionOf(key, n); }

    // Normal operation: partition i is led by server i (identity; spec §0.1).
    static uint32_t leaderOf(uint32_t partition) { return partition; }

    // §1.6 involved partitions/leaders of a transaction: over rSet ∪ keys(wSet).
    std::set<uint32_t> involvedPartitions(const proto::Txn &t) const {
        std::set<uint32_t> ps;
        for (const auto &k : t.r_set()) ps.insert(partitionOf(k));
        for (const auto &kv : t.w_set()) ps.insert(partitionOf(kv.key()));
        return ps;
    }
    std::set<uint32_t> involvedLeaders(const proto::Txn &t) const {
        std::set<uint32_t> ls;
        for (uint32_t p : involvedPartitions(t)) ls.insert(leaderOf(p));
        return ls;
    }
};

} // namespace dyna_multi
} // namespace specpaxos

#endif // _DYNA_MULTI_TOPOLOGY_H_

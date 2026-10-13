// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/topology.h
//   Leadership map for one view (spec §0.1-0.2, §1.6). Shared-replica model: N = 2f+1
//   servers, the key range split into N home partitions, server i leads partition i and
//   follows the rest. Identical on every node in the same view, so involvedLeaders() is
//   computed the same way on client and server.
//
//   Views (design doc § Recovery): a view carries the set of ALIVE servers. A key whose
//   home leader is alive keeps it. A key whose home leader failed is re-assigned by
//   consistent hashing with virtual nodes over the alive servers: each server owns
//   kVnodesPerServer points on a hash ring and the key goes to the first alive point
//   clockwise of its own hash. So survivors keep their ranges and the failed range is
//   split (roughly evenly) among them; each survivor's share is its "delta-p".

#ifndef _DYNA_TOPOLOGY_H_
#define _DYNA_TOPOLOGY_H_

#include "dyna/dyna-proto.pb.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace specpaxos {
namespace dyna {

struct Topology {
    static constexpr uint32_t kVnodesPerServer = 16;

    uint32_t n = 1;            // number of servers == number of home partitions
    uint64_t alive_mask = 1;   // bit i set ⇔ server i is alive in this view (n ≤ 64)

    explicit Topology(uint32_t n_ = 1)
        : n(n_), alive_mask(AllAlive(n_)), ring_(BuildRing(n_)) {}

    static uint64_t AllAlive(uint32_t n) {
        return n >= 64 ? ~0ULL : ((1ULL << n) - 1);
    }

    // Same servers, different alive set (a new view).
    Topology WithAlive(uint64_t mask) const {
        Topology t = *this;
        t.alive_mask = mask & AllAlive(n);
        return t;
    }

    bool alive(uint32_t s) const { return s < 64 && ((alive_mask >> s) & 1ULL) != 0; }
    uint32_t alive_count() const { return static_cast<uint32_t>(__builtin_popcountll(alive_mask)); }

    // Deterministic key -> home partition mapping, known to all (spec §1.4).
    static uint32_t PartitionOf(const std::string &key, uint32_t n) {
        return static_cast<uint32_t>(std::hash<std::string>()(key) % n);
    }
    uint32_t partitionOf(const std::string &key) const { return PartitionOf(key, n); }

    // Normal operation: home partition i is led by server i (identity; spec §0.1).
    static uint32_t leaderOf(uint32_t partition) { return partition; }

    // The server that leads `key` in this view: its home leader if alive, otherwise
    // the consistent-hash successor among the alive servers.
    uint32_t ownerOf(const std::string &key) const {
        const uint32_t home = leaderOf(partitionOf(key));
        if (alive(home) || alive_mask == 0) return home;
        const uint64_t h = Fnv1a(key);
        const auto &ring = *ring_;
        auto it = std::lower_bound(ring.begin(), ring.end(), std::make_pair(h, 0u));
        for (size_t i = 0; i < ring.size(); ++i, ++it) {
            if (it == ring.end()) it = ring.begin();
            if (alive(it->second)) return it->second;
        }
        return home;   // unreachable while alive_mask != 0
    }

    // §1.6 involved partitions/leaders of a transaction: over rSet ∪ keys(wSet).
    std::set<uint32_t> involvedPartitions(const proto::Txn &t) const {
        std::set<uint32_t> ps;
        for (const auto &k : t.r_set()) ps.insert(partitionOf(k));
        for (const auto &kv : t.w_set()) ps.insert(partitionOf(kv.key()));
        return ps;
    }
    std::set<uint32_t> involvedLeaders(const proto::Txn &t) const {
        std::set<uint32_t> ls;
        for (const auto &k : t.r_set()) ls.insert(ownerOf(k));
        for (const auto &kv : t.w_set()) ls.insert(ownerOf(kv.key()));
        return ls;
    }

    static uint64_t Fnv1a(const std::string &s) {
        uint64_t h = 1469598103934665603ULL;
        for (unsigned char c : s) { h ^= c; h *= 1099511628211ULL; }
        return h;
    }

private:
    using Ring = std::vector<std::pair<uint64_t, uint32_t>>;   // (point, server), sorted

    static std::shared_ptr<const Ring> BuildRing(uint32_t n) {
        auto ring = std::make_shared<Ring>();
        for (uint32_t s = 0; s < n; ++s) {
            for (uint32_t v = 0; v < kVnodesPerServer; ++v) {
                ring->emplace_back(Fnv1a("dyna-vnode-" + std::to_string(s) + "-" +
                                         std::to_string(v)), s);
            }
        }
        std::sort(ring->begin(), ring->end());
        return ring;
    }

    std::shared_ptr<const Ring> ring_;   // shared: views copy Topology freely
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_TOPOLOGY_H_

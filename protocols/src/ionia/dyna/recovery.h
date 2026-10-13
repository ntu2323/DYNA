// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/recovery.h
//   Data model + pure helpers for partition-leader recovery (design doc § Recovery).
//
//   After a server fails, every survivor becomes Partition Leader (PL) for its delta-p:
//   the keys of the failed range that consistent hashing now assigns to it. The PL
//   collects, from f+1 servers (itself included), every speculative / committing /
//   committed txn touching delta-p and every executed key-value in delta-p, and merges
//   them into the buffer M (MergeRecovery). M is then prepared on f+1 servers and, at
//   StartView, installed everywhere.
//
//   Why this is safe: followers only ever hold a txn SPECULATIVE at the client's original
//   t, so a txn fast-committed with its leader dead has ≥1 SPECULATIVE copy at that t
//   among any f+1 of the 2f survivors; a slow-committed txn has ≥1 COMMITTING copy at
//   agreedT among them. Merge precedence keeps the most-decided copy.

#ifndef _DYNA_RECOVERY_H_
#define _DYNA_RECOVERY_H_

#include "dyna/dyna-proto.pb.h"
#include "dyna/topology.h"
#include "dyna/txn_types.h"

#include <cstdint>
#include <string>
#include <vector>

namespace specpaxos {
namespace dyna {

struct RecoveredTxn {
    proto::Txn txn;
    uint64_t   t = 0;
    Status     status = Status::SPECULATIVE;

    TxnId Key() const { return TxnId{txn.client_id(), txn.request_id()}; }
};

struct RecoveredKV {
    std::string key;
    std::string value;
    uint64_t    t = 0;
};

struct RecoveryData {
    std::vector<RecoveredTxn> txns;
    std::vector<RecoveredKV>  kvs;

    bool empty() const { return txns.empty() && kvs.empty(); }
};

// Is `key` part of PL `pl`'s delta-p for the change old → new view? (It moved to `pl`.)
inline bool InDeltaP(const Topology &old_topo, const Topology &new_topo,
                     const std::string &key, uint32_t pl)
{
    return new_topo.ownerOf(key) == pl && old_topo.ownerOf(key) != pl;
}

// Was `key` led by a server that is gone in the new view? (Any PL's delta-p.)
inline bool InFailedRange(const Topology &old_topo, const Topology &new_topo,
                          const std::string &key)
{
    return !new_topo.alive(old_topo.ownerOf(key));
}

// Does the txn touch any key satisfying `pred`?
template <typename Pred>
bool TxnTouches(const proto::Txn &txn, Pred pred)
{
    for (const auto &k : txn.r_set()) if (pred(k)) return true;
    for (const auto &kv : txn.w_set()) if (pred(kv.key())) return true;
    return false;
}

// Merge the f+1 replies into M.
//   Txns, per TxnId: COMMITTED > COMMITTING > SPECULATIVE; within the same status the
//   larger t (COMMITTING/COMMITTED copies all carry the one agreed t; SPECULATIVE copies
//   at followers all carry the client's t, a surviving co-leader's may be bumped — the
//   post-recovery leader sync resolves those to the max anyway).
//   Key-values, per key: the largest t.
RecoveryData MergeRecovery(const std::vector<RecoveryData> &replies);

// Status rank used by the merge (and by install: never downgrade).
inline int StatusRank(Status s)
{
    switch (s) {
    case Status::SPECULATIVE: return 0;
    case Status::COMMITTING:  return 1;
    case Status::COMMITTED:   return 2;
    }
    return 0;
}

// Proto conversions.
void ToProto(const RecoveryData &d, google::protobuf::RepeatedPtrField<proto::RecTxn> *txns,
             google::protobuf::RepeatedPtrField<proto::RecKV> *kvs);
RecoveryData FromProto(const google::protobuf::RepeatedPtrField<proto::RecTxn> &txns,
                       const google::protobuf::RepeatedPtrField<proto::RecKV> &kvs);

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_RECOVERY_H_

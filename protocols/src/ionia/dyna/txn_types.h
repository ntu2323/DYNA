// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/txn_types.h
//   Stage 1 (inert data types): the server-side transaction model from the DYNA
//   algorithm doc + spec §1. A multi-key, multi-partition transaction has a single
//   OrderKey and therefore lives in exactly one server-wide buffer at a time.
//   Data structures only — no protocol logic.

#ifndef _DYNA_TXN_TYPES_H_
#define _DYNA_TXN_TYPES_H_

#include "common/log.h"                     // ClientAndReqId + std::hash<ClientAndReqId>
#include "lib/data_structure.h"             // HashMap (ankerl::unordered_dense::map)
#include "lib/transport.h"                  // TransportAddress
#include "dyna/dyna-proto.pb.h" // proto::Txn, proto::TxnType

#include <cstdint>
#include <memory>
#include <set>
#include <string>

namespace specpaxos {
namespace dyna {

// Globally-unique transaction identifier: (client_id, request_id). Identical to
// the framework's ClientAndReqId (two uint64s).
using TxnId = ClientAndReqId;

// Hash for TxnId-keyed containers. Do NOT use the framework's std::hash<ClientAndReqId>
// (common/log.h): it discards hash_combine's return value and always yields 0, so every
// lookup degenerates into a linear scan that grows with the map (measured on CloudLab:
// throughput collapsed below 1k ops/s and kept falling). Always key DYNA containers with
// TxnMap / TxnSet / TxnIdHash.
struct TxnIdHash {
    using is_avalanching = void;   // ankerl: output is already well mixed
    uint64_t operator()(const TxnId &x) const noexcept {
        uint64_t h = x.clientid * 0x9E3779B97F4A7C15ULL ^ x.clientreqid;
        h ^= h >> 30; h *= 0xBF58476D1CE4E5B9ULL;   // splitmix64 finalizer
        h ^= h >> 27; h *= 0x94D049BB133111EBULL;
        return h ^ (h >> 31);
    }
};

template <class V>
using TxnMap = ankerl::unordered_dense::map<TxnId, V, TxnIdHash>;
using TxnSet = ankerl::unordered_dense::set<TxnId, TxnIdHash>;

inline TxnId MakeTxnId(uint64_t client_id, uint64_t request_id) {
    return TxnId{client_id, request_id};
}

enum class Status { SPECULATIVE, COMMITTING, COMMITTED };

// OrderKey — the total order everything sorts by (spec §1.1):
//   (t physical, client_id, request_id), compared lexicographically.
// Globally unique, totally ordered, and identical on every server. Re-timestamp
// changes only `t`; (client_id, request_id) are preserved, so uniqueness survives
// a bump. The C++20 defaulted operator<=> gives exactly the lexicographic order.
struct OrderKey {
    uint64_t t          = 0;
    uint64_t client_id  = 0;
    uint64_t request_id = 0;

    // Lexicographic (t, client_id, request_id). Hand-written because the build is
    // -std=c++17 (no defaulted operator<=>).
    bool operator<(const OrderKey &o) const {
        if (t != o.t)                 return t < o.t;
        if (client_id != o.client_id) return client_id < o.client_id;
        return request_id < o.request_id;
    }
    bool operator==(const OrderKey &o) const {
        return t == o.t && client_id == o.client_id && request_id == o.request_id;
    }
    bool operator!=(const OrderKey &o) const { return !(*this == o); }
    bool operator>(const OrderKey &o)  const { return o < *this; }
    bool operator<=(const OrderKey &o) const { return !(o < *this); }
    bool operator>=(const OrderKey &o) const { return !(*this < o); }
};

inline OrderKey MakeOrderKey(uint64_t t, const TxnId &id) {
    return OrderKey{t, id.clientid, id.clientreqid};
}

// TxnEntry — server-side stored form (spec §1.3). Its `t` is the authoritative,
// live timestamp (may differ from the original wire `t` after a bump); it orders
// the entry. Entries live in exactly one of earlyBuffer / lateBuffer / specLog at
// a time (they move between them; see the ServerOrderer handlers).
struct TxnEntry {
    proto::Txn txn;                          // the body (may be fetched later)
    uint64_t   t      = 0;                    // LIVE timestamp; orders the entry
    Status     status = Status::SPECULATIVE;

    // Does this server lead any key in txn.wSet ∪ txn.rSet? And which keys?
    bool                  is_leader_for_this = false;
    std::set<std::string> led_keys;

    // READ + leader only: values produced by the disk-read.
    HashMap<std::string, std::string> results;

    // Leader-only bookkeeping.
    HashMap<uint32_t, uint64_t> ts_proposals;   // serverId -> proposed t (§3.5 TsAgree)
    std::set<uint32_t>          prepare_replies; // servers that ack'd ReplicatePrepare

    // Piggybacked OWD feedback snapshotted at receive, and the replica clock at
    // ReceiveMessage (diagnostics / skew-immune internal latency).
    uint64_t owd_estimate_us = 0;
    uint64_t arrival_us      = 0;
    uint64_t released_us     = 0;   // clock() when it entered specLog SPECULATIVE (stuck detection)
    uint64_t committing_us   = 0;   // clock() when it became COMMITTING (stale-commit detection)
    uint64_t last_replicate_us = 0; // primary: last ReplicatePrepare broadcast; others: last
                                    // re-ack to the primary (both throttles)

    // Where to send this txn's client reply (null for peer-fetched bodies).
    std::shared_ptr<TransportAddress> client_addr;

    TxnId Key() const {
        return TxnId{txn.client_id(), txn.request_id()};
    }
    OrderKey Order() const {
        return OrderKey{t, txn.client_id(), txn.request_id()};
    }
};

// Min-heap ordering for earlyBuffer / specLog: smaller OrderKey first.
struct OrderKeyLess {
    bool operator()(const TxnEntry &a, const TxnEntry &b) const {
        return a.Order() < b.Order();
    }
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_TXN_TYPES_H_

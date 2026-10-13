// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/recovery.cc

#include "dyna/recovery.h"

#include <map>

namespace specpaxos {
namespace dyna {

RecoveryData
MergeRecovery(const std::vector<RecoveryData> &replies)
{
    // Ordered maps keep M deterministic (same bytes from the same replies).
    std::map<std::pair<uint64_t, uint64_t>, RecoveredTxn> txns;
    std::map<std::string, RecoveredKV> kvs;
    for (const RecoveryData &r : replies) {
        for (const RecoveredTxn &c : r.txns) {
            const auto id = std::make_pair(c.txn.client_id(), c.txn.request_id());
            auto it = txns.find(id);
            if (it == txns.end()) {
                txns.emplace(id, c);
                continue;
            }
            RecoveredTxn &cur = it->second;
            const int rc = StatusRank(c.status), rk = StatusRank(cur.status);
            if (rc > rk || (rc == rk && c.t > cur.t)) cur = c;
        }
        for (const RecoveredKV &kv : r.kvs) {
            auto it = kvs.find(kv.key);
            if (it == kvs.end() || kv.t > it->second.t) kvs[kv.key] = kv;
        }
    }
    RecoveryData m;
    m.txns.reserve(txns.size());
    for (auto &kv : txns) m.txns.push_back(std::move(kv.second));
    m.kvs.reserve(kvs.size());
    for (auto &kv : kvs) m.kvs.push_back(std::move(kv.second));
    return m;
}

void
ToProto(const RecoveryData &d, google::protobuf::RepeatedPtrField<proto::RecTxn> *txns,
        google::protobuf::RepeatedPtrField<proto::RecKV> *kvs)
{
    for (const RecoveredTxn &c : d.txns) {
        proto::RecTxn *p = txns->Add();
        *p->mutable_txn() = c.txn;
        p->set_t(c.t);
        p->set_status(static_cast<uint32_t>(StatusRank(c.status)));
    }
    for (const RecoveredKV &kv : d.kvs) {
        proto::RecKV *p = kvs->Add();
        p->set_key(kv.key);
        p->set_value(kv.value);
        p->set_t(kv.t);
    }
}

RecoveryData
FromProto(const google::protobuf::RepeatedPtrField<proto::RecTxn> &txns,
          const google::protobuf::RepeatedPtrField<proto::RecKV> &kvs)
{
    RecoveryData d;
    d.txns.reserve(txns.size());
    for (const proto::RecTxn &p : txns) {
        RecoveredTxn c;
        c.txn = p.txn();
        c.t = p.t();
        c.status = p.status() >= 2 ? Status::COMMITTED
                 : p.status() == 1 ? Status::COMMITTING : Status::SPECULATIVE;
        d.txns.push_back(std::move(c));
    }
    d.kvs.reserve(kvs.size());
    for (const proto::RecKV &p : kvs) d.kvs.push_back(RecoveredKV{p.key(), p.value(), p.t()});
    return d;
}

} // namespace dyna
} // namespace specpaxos

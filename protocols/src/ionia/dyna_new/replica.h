// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _DYNA_NEW_REPLICA_H_
#define _DYNA_NEW_REPLICA_H_

#include "lib/configuration.h"
#include "lib/transport.h"

#include "dyna_new/storage_backend.h"

#include <memory>

namespace specpaxos {
namespace dyna_new {

class DynaNewReplica : public TransportReceiver
{
public:
    DynaNewReplica(const specpaxos::Configuration &config,
                   int replicaIdx,
                   Transport *transport);
    ~DynaNewReplica() override;

    void ReceiveMessage(const TransportAddress &remote,
                        const std::string_view type,
                        const std::string_view data,
                        void *meta_data) override;

private:
    specpaxos::Configuration config;
    int replicaIdx;
    Transport *transport;

    // Authoritative memory KV + lagging disk KV (spec §1.7, §3.4). Not yet
    // exercised — protocol logic that calls into it arrives in later stages.
    std::unique_ptr<StorageBackend> backend_;
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_REPLICA_H_

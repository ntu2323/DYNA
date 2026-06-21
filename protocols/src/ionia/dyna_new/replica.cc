// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "dyna_new/replica.h"

#include "dyna_new/in_memory_backend.h"
#include "lib/message.h"

namespace specpaxos {
namespace dyna_new {

DynaNewReplica::DynaNewReplica(const specpaxos::Configuration &config,
                               int replicaIdx,
                               Transport *transport)
    : config(config), replicaIdx(replicaIdx), transport(transport),
      backend_(std::make_unique<InMemoryBackend>(
                   InMemoryBackend::LatencyFromEnv()))
{
    transport->Register(this, config, 0, replicaIdx);
}

DynaNewReplica::~DynaNewReplica() {}

void
DynaNewReplica::ReceiveMessage(const TransportAddress &remote,
                               const std::string_view type,
                               const std::string_view data,
                               void *meta_data)
{
    // Stage 0 stub: drop all messages silently.
    (void)remote; (void)type; (void)data; (void)meta_data;
}

} // namespace dyna_new
} // namespace specpaxos

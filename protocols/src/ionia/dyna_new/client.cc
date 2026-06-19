// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "dyna_new/client.h"

#include "lib/message.h"

namespace specpaxos {
namespace dyna_new {

DynaNewClient::DynaNewClient(const specpaxos::Configuration &config,
                             Transport *transport,
                             uint64_t clientid)
    : Client(config, transport, clientid)
{
}

DynaNewClient::~DynaNewClient() {}

void
DynaNewClient::Invoke(const string &request, continuation_t continuation)
{
    // Stage 0 stub: drop silently.
    (void)request; (void)continuation;
}

void
DynaNewClient::InvokeUnlogged(int replicaIdx,
                              const string &request,
                              continuation_t continuation,
                              timeout_continuation_t timeoutContinuation,
                              uint32_t timeout)
{
    // Stage 0 stub: drop silently.
    (void)replicaIdx; (void)request; (void)continuation;
    (void)timeoutContinuation; (void)timeout;
}

void
DynaNewClient::ReceiveMessage(const TransportAddress &remote,
                              const std::string_view type,
                              const std::string_view data,
                              void *meta_data)
{
    // Stage 0 stub: drop silently.
    (void)remote; (void)type; (void)data; (void)meta_data;
}

} // namespace dyna_new
} // namespace specpaxos

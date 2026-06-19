// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _DYNA_NEW_CLIENT_H_
#define _DYNA_NEW_CLIENT_H_

#include "common/client.h"

namespace specpaxos {
namespace dyna_new {

class DynaNewClient : public Client
{
public:
    DynaNewClient(const specpaxos::Configuration &config,
                  Transport *transport,
                  uint64_t clientid = 0);
    ~DynaNewClient() override;

    void Invoke(const string &request, continuation_t continuation) override;
    void InvokeUnlogged(int replicaIdx,
                        const string &request,
                        continuation_t continuation,
                        timeout_continuation_t timeoutContinuation = nullptr,
                        uint32_t timeout = DEFAULT_UNLOGGED_OP_TIMEOUT) override;
    void ReceiveMessage(const TransportAddress &remote,
                        const std::string_view type,
                        const std::string_view data,
                        void *meta_data) override;
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_CLIENT_H_

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _DYNA_NEW_CLIENT_H_
#define _DYNA_NEW_CLIENT_H_

#include "common/client.h"
#include "dyna_new/deadline.h"
#include "dyna_new/commit_quorum.h"

#include <cstdint>
#include <string>
#include <unordered_map>

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

private:
    struct PendingRequest {
        std::string request;
        uint32_t partition_id = 0;
        uint64_t request_id = 0;
        uint64_t send_us = 0;
        bool is_read = false;
        continuation_t continuation;
        CommitQuorum quorum;
    };

    uint64_t NowMicros() const;
    bool IsReadCommand(const std::string &request) const;
    std::string KeyOf(const std::string &request) const;
    std::string ValueOf(const std::string &request) const;

    uint64_t last_req_id_ = 0;
    uint32_t num_partitions_;
    size_t write_quorum_;
    ClientBoundTracker bound_tracker_;
    std::unordered_map<uint64_t, PendingRequest> pending_;
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_CLIENT_H_

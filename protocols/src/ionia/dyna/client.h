// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _DYNA_CLIENT_H_
#define _DYNA_CLIENT_H_

#include "common/client.h"
#include "dyna/deadline/client_bound_tracker.h"
#include "dyna/dyna-proto.pb.h"

#include <unordered_map>
#include <unordered_set>

namespace specpaxos {
namespace dyna {

class DynaClient : public Client
{
public:
    DynaClient(const Configuration &config, Transport *transport, uint64_t clientid = 0);
    ~DynaClient() override;

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
        string request;
        bool isRead = false;
        uint32_t partitionId = 0;
        uint64_t requestId = 0;
        uint64_t deadlineUs = 0;       // original client-stamped deadline
        uint64_t activeDeadlineUs = 0; // leader-authoritative epoch for quorum matching
        continuation_t continuation;
        bool leaderAcked = false;
        bool readResultReceived = false;
        std::unordered_set<uint32_t> responders;
        std::string readResultData;
    };

    uint64_t lastReqId;
    uint32_t numPartitions;
    std::unordered_map<uint64_t, PendingRequest> pendingRequests;
    ClientBoundTracker bound_tracker_;

    uint64_t NowMicros() const;
    uint64_t ComputeDeadline(uint64_t send_time_us) const;
    bool IsReadCommand(const std::string &request) const;
    uint32_t PartitionForRequest(const std::string &request) const;
    uint32_t PartitionLeaderFor(uint32_t partitionId) const;
    void TryComplete(PendingRequest &pending);
};

} // namespace dyna
} // namespace specpaxos

#endif  /* _DYNA_CLIENT_H_ */

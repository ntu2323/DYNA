// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _VR_CLIENT_H_
#define _VR_CLIENT_H_

#include "common/client.h"
#include "lib/configuration.h"
#include "vr/vr-proto.pb.h"

#include <unordered_map>

namespace specpaxos {
namespace vr {

class VRClient : public Client
{
public:
    VRClient(const Configuration &config,
             Transport *transport,
             uint64_t clientid = 0);
    virtual ~VRClient();
    virtual void Invoke(const string &request,
                        continuation_t continuation) override;
    virtual void InvokeUnlogged(int replicaIdx,
                                const string &request,
                                continuation_t continuation,
                                timeout_continuation_t timeoutContinuation = nullptr,
                                uint32_t timeout = DEFAULT_UNLOGGED_OP_TIMEOUT) override;
    virtual void ReceiveMessage(const TransportAddress &remote,
                                const string &type, const string &data,
                                void *meta_data) override;

protected:
    int view;
    int opnumber;
    uint64_t lastReqId;
    bool isRead;

    struct PendingRequest
    {
        string request;
        uint64_t clientReqId;
        continuation_t continuation;
        timeout_continuation_t timeoutContinuation;
        inline PendingRequest(string request, uint64_t clientReqId,
                              continuation_t continuation)
            : request(request), clientReqId(clientReqId),
              continuation(continuation) { }
    };
    std::unordered_map<uint64_t, PendingRequest *> pendingRequests_;
    PendingRequest *pendingUnloggedRequest;
    Timeout *requestTimeout;
    Timeout *unloggedRequestTimeout;

    void SendRequestFor(PendingRequest *req);
    void ResendRequest();
    void HandleReply(const TransportAddress &remote,
                     const proto::ReplyMessage &msg);
    void HandleUnloggedReply(const TransportAddress &remote,
                             const proto::UnloggedReplyMessage &msg);
    void UnloggedRequestTimeoutCallback();
};

} // namespace vr
} // namespace specpaxos

#endif  /* _VR_CLIENT_H_ */

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "vr/client.h"

#include "lib/assert.h"
#include "lib/message.h"

namespace specpaxos {
namespace vr {

VRClient::VRClient(const Configuration &config,
                   Transport *transport,
                   uint64_t clientid)
    : Client(config, transport, clientid)
{
    pendingUnloggedRequest = NULL;
    lastReqId = 0;

    requestTimeout = new Timeout(transport, 7000, [this]() {
            ResendRequest();
        });
    unloggedRequestTimeout = new Timeout(transport, 1000, [this]() {
            UnloggedRequestTimeoutCallback();
        });
}

VRClient::~VRClient()
{
    for (auto &kv : pendingRequests_) {
        delete kv.second;
    }
    pendingRequests_.clear();
    if (pendingUnloggedRequest) {
        delete pendingUnloggedRequest;
    }
    delete requestTimeout;
    delete unloggedRequestTimeout;
}

void
VRClient::Invoke(const string &request,
                 continuation_t continuation)
{
    ++lastReqId;
    const uint64_t reqId = lastReqId;
    PendingRequest *pending = new PendingRequest(request, reqId, continuation);
    pendingRequests_[reqId] = pending;

    SendRequestFor(pending);
    requestTimeout->Reset();
}

void
VRClient::InvokeUnlogged(int replicaIdx,
                         const string &request,
                         continuation_t continuation,
                         timeout_continuation_t timeoutContinuation,
                         uint32_t timeout)
{
    if (pendingUnloggedRequest != NULL) {
        Panic("Client only supports one pending unlogged request");
    }

    ++lastReqId;
    uint64_t reqId = lastReqId;

    pendingUnloggedRequest = new PendingRequest(request, reqId, continuation);
    pendingUnloggedRequest->timeoutContinuation = timeoutContinuation;

    proto::UnloggedRequestMessage reqMsg;
    reqMsg.mutable_req()->set_op(pendingUnloggedRequest->request);
    reqMsg.mutable_req()->set_clientid(clientid);
    reqMsg.mutable_req()->set_clientreqid(pendingUnloggedRequest->clientReqId);

    ASSERT(!unloggedRequestTimeout->Active());
    unloggedRequestTimeout->SetTimeout(timeout);
    unloggedRequestTimeout->Start();

    transport->SendMessageToReplica(this, replicaIdx, reqMsg);
}

void
VRClient::SendRequestFor(PendingRequest *pending)
{
    if (pending->request.c_str()[0] == 'r' || pending->request.c_str()[0] == 'R') {
        proto::UnloggedRequestMessage reqMsg;
        reqMsg.mutable_req()->set_op(pending->request);
        reqMsg.mutable_req()->set_clientid(clientid);
        reqMsg.mutable_req()->set_clientreqid(pending->clientReqId);
        transport->SendMessageToAll(this, reqMsg);
    } else {
        proto::RequestMessage reqMsg;
        reqMsg.mutable_req()->set_op(pending->request);
        reqMsg.mutable_req()->set_clientid(clientid);
        reqMsg.mutable_req()->set_clientreqid(pending->clientReqId);
        transport->SendMessageToAll(this, reqMsg);
    }
}

void
VRClient::ResendRequest()
{
    if (pendingRequests_.empty()) {
        return;
    }
    Warning("Client timeout; resending %zu pending request(s)", pendingRequests_.size());
    for (auto &kv : pendingRequests_) {
        SendRequestFor(kv.second);
    }
}

void
VRClient::ReceiveMessage(const TransportAddress &remote,
                         const string &type,
                         const string &data,
                         void *meta_data)
{
    static proto::ReplyMessage reply;
    static proto::UnloggedReplyMessage unloggedReply;

    if (type == reply.GetTypeName()) {
        reply.ParseFromString(data);
        HandleReply(remote, reply);
    } else if (type == unloggedReply.GetTypeName()) {
        unloggedReply.ParseFromString(data);
        HandleUnloggedReply(remote, unloggedReply);
    } else {
        Client::ReceiveMessage(remote, type, data, NULL);
    }
}

void
VRClient::HandleReply(const TransportAddress &remote,
                      const proto::ReplyMessage &msg)
{
    (void)remote;
    auto it = pendingRequests_.find(msg.clientreqid());
    if (it == pendingRequests_.end()) {
        Warning("Received reply for unknown request %lu", msg.clientreqid());
        return;
    }

    Debug2("Client received reply");

    PendingRequest *req = it->second;
    pendingRequests_.erase(it);
    if (pendingRequests_.empty()) {
        requestTimeout->Stop();
    }

    req->continuation(req->request, msg.reply());
    delete req;
}

void
VRClient::HandleUnloggedReply(const TransportAddress &remote,
                              const proto::UnloggedReplyMessage &msg)
{
    (void)remote;
    if (pendingUnloggedRequest == NULL) {
        Warning("Received unlogged reply when no request was pending");
        return;
    }
    if (msg.clientreqid() != pendingUnloggedRequest->clientReqId) {
        Warning("Received unlogged reply for a different request");
        return;
    }

    Debug2("Client received unlogged reply");

    unloggedRequestTimeout->Stop();

    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;

    req->continuation(req->request, msg.reply());
    delete req;
}

void
VRClient::UnloggedRequestTimeoutCallback()
{
    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;

    Warning("Unlogged request timed out");

    unloggedRequestTimeout->Stop();

    req->timeoutContinuation(req->request);
    delete req;
}

} // namespace vr
} // namespace specpaxos

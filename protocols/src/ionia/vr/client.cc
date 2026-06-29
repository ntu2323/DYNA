// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * vr/client.cc:
 *   Skyros client
 * 
 * Copyright 2021 Aishwarya Ganesan and Ramnatthan Alagappan
 *
 * Small changes made to the code to implement Skyros
 *
 * *************************************************************
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include "common/client.h"
#include "common/request.pb.h"
#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "vr/client.h"
#include "vr/vr-proto.pb.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

namespace specpaxos {
namespace vr {

namespace {

bool
IoniaFastQuorumEnabled()
{
    static const bool enabled = []() {
        const char *v = std::getenv("IONIA_FAST_QUORUM");
        return v != nullptr && *v != '\0' && std::strcmp(v, "0") != 0;
    }();
    return enabled;
}

// IONIA_CLIENT_TIMING: split each completed write into
//   leader  = leader-ack arrival - send
//   quorum  = quorum-completing reply - send  (end-to-end)
//   tail    = quorum - leader  (extra time waiting on the slowest follower)
// A large `tail` means normal quorum is bound by the follower tail; a small
// `tail` (quorum ~ leader) means it is bound by the leader itself.
bool
ClientTimingEnabled()
{
    static const bool enabled = []() {
        const char *v = std::getenv("IONIA_CLIENT_TIMING");
        return v != nullptr && *v != '\0' && std::strcmp(v, "0") != 0;
    }();
    return enabled;
}

inline uint64_t
NowUs()
{
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

constexpr int kTimingBuckets = 32;  // log2 microseconds: bucket b => [2^b, 2^(b+1)) us

struct TimingHist
{
    std::array<std::atomic<uint64_t>, kTimingBuckets> buckets{};
    std::atomic<uint64_t> count{0};
    std::atomic<uint64_t> sum_us{0};
};

TimingHist g_leader_hist;
TimingHist g_quorum_hist;
TimingHist g_tail_hist;
std::atomic<bool> g_timing_started{false};

inline int
TimingBucket(uint64_t us)
{
    if (us == 0) {
        return 0;
    }
    int b = 63 - __builtin_clzll(us);
    if (b >= kTimingBuckets) {
        b = kTimingBuckets - 1;
    }
    return b;
}

inline void
RecordTiming(TimingHist &h, uint64_t us)
{
    h.buckets[TimingBucket(us)].fetch_add(1, std::memory_order_relaxed);
    h.count.fetch_add(1, std::memory_order_relaxed);
    h.sum_us.fetch_add(us, std::memory_order_relaxed);
}

// Approximate percentile (lower edge of the bucket) from a delta snapshot.
uint64_t
PercentileFromDelta(const std::array<uint64_t, kTimingBuckets> &delta,
                    uint64_t total, double pct)
{
    if (total == 0) {
        return 0;
    }
    const uint64_t target = static_cast<uint64_t>(pct * total);
    uint64_t cum = 0;
    for (int b = 0; b < kTimingBuckets; b++) {
        cum += delta[b];
        if (cum >= target) {
            return uint64_t{1} << b;  // lower edge, microseconds
        }
    }
    return uint64_t{1} << (kTimingBuckets - 1);
}

void
MaybeStartTimingPrinter()
{
    bool expected = false;
    if (!g_timing_started.compare_exchange_strong(expected, true)) {
        return;
    }
    std::thread([]() {
        struct Snap {
            std::array<uint64_t, kTimingBuckets> buckets{};
            uint64_t count = 0;
            uint64_t sum_us = 0;
        };
        Snap last_leader, last_quorum, last_tail;
        auto snapshot = [](TimingHist &h, Snap &last, const char *label) {
            std::array<uint64_t, kTimingBuckets> delta{};
            uint64_t cur_count = h.count.load(std::memory_order_relaxed);
            uint64_t cur_sum = h.sum_us.load(std::memory_order_relaxed);
            uint64_t dcount = cur_count - last.count;
            uint64_t dsum = cur_sum - last.sum_us;
            for (int b = 0; b < kTimingBuckets; b++) {
                uint64_t cur = h.buckets[b].load(std::memory_order_relaxed);
                delta[b] = cur - last.buckets[b];
                last.buckets[b] = cur;
            }
            last.count = cur_count;
            last.sum_us = cur_sum;
            const uint64_t med = PercentileFromDelta(delta, dcount, 0.50);
            const uint64_t p99 = PercentileFromDelta(delta, dcount, 0.99);
            const uint64_t avg = dcount ? dsum / dcount : 0;
            Notice("IONIA_CLIENT_TIMING %-6s n=%lu med~%luus p99~%luus avg=%luus",
                   label, static_cast<unsigned long>(dcount),
                   static_cast<unsigned long>(med),
                   static_cast<unsigned long>(p99),
                   static_cast<unsigned long>(avg));
        };
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(2));
            snapshot(g_leader_hist, last_leader, "leader");
            snapshot(g_quorum_hist, last_quorum, "quorum");
            snapshot(g_tail_hist, last_tail, "tail");
        }
    }).detach();
}

}  // namespace

VRClient::VRClient(const Configuration &config,
                   Transport *transport,
                   uint64_t clientid)
    : Client(config, transport, clientid)
{
    pendingRequest = NULL;
    pendingUnloggedRequest = NULL;
    lastReqId = 0;
    quorum = 0;

    if (ClientTimingEnabled()) {
        MaybeStartTimingPrinter();
    }

    /*
    requestTimeout = new Timeout(transport, 7000, [this]() {
            ResendRequest();
        });
    unloggedRequestTimeout = new Timeout(transport, 1000, [this]() {
            UnloggedRequestTimeoutCallback();
        });
    */
}

VRClient::~VRClient()
{
    if (pendingRequest) {
        delete pendingRequest;
    }
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
    // XXX Can only handle one pending request for now
    if (pendingRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    const bool fast_quorum = IoniaFastQuorumEnabled();
    if (fast_quorum) {
        static std::atomic<bool> logged{false};
        if (!logged.exchange(true, std::memory_order_relaxed)) {
            Notice("IONIA_FAST_QUORUM=1: bench writes use quorum=1 and "
                   "SendMessageToReplica(leader)");
        }
    }

    quorum = config.FastQuorumSize();
    // Notice("Sending request %s", request.c_str());
    if(request.c_str()[0] == 'r' || request.c_str()[0] == 'R'
        || request.c_str()[0] == 'e' || request.c_str()[0] == 'E') {
        // only one response expected for reads and non-nilext writes
        quorum = 1;
    }

    if (IMMEDIATE_WRITE_RESPOND || fast_quorum)
    {
        // make writes and reads quorum 1
        quorum = 1;        
    }

    ++lastReqId;
    uint64_t reqId = lastReqId;
    responses[lastReqId] = 0;
    leader_acked[lastReqId] = 0;

    auto& pending_request = pending_requests[reqId];
    pending_request.clientReqId = reqId;
    pending_request.continuation = std::move(continuation);
    if (ClientTimingEnabled()) {
        pending_request.t_send_us = NowUs();
        pending_request.t_leader_us = 0;
    }

    proto::RequestMessage request_message;
    request_message.mutable_req()->set_op(std::move(request));
    request_message.mutable_req()->set_clientid(clientid);
    request_message.mutable_req()->set_clientreqid(reqId);
    if (request.c_str()[0] == 'r' || request.c_str()[0] == 'R')
    {
        if (!(transport->SendMessageToReplica(this,
                                              config.GetLeaderIndex(0),
                                              request_message)))
        {
            Warning("Failed to send read message to leader");
        }
    }
    else
    {
        if (IMMEDIATE_WRITE_RESPOND || fast_quorum)
        {
            if (!(transport->SendMessageToReplica(this,
                                                config.GetLeaderIndex(0),
                                                request_message)))
            {
                Warning("Failed to send message to leader");
            }
        }
        else
        {
            transport->SendMessageToAll(this, request_message);
        }
    }
}

void
VRClient::InvokeUnlogged(int replicaIdx,
                         const string &request,
                         continuation_t continuation,
                         timeout_continuation_t timeoutContinuation,
                         uint32_t timeout)
{
    // XXX Can only handle one pending request for now
    if (pendingUnloggedRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    ++lastReqId;
    uint64_t reqId = lastReqId;

    pendingUnloggedRequest = new PendingRequest(request, reqId, std::move(continuation));
    pendingUnloggedRequest->timeoutContinuation = std::move(timeoutContinuation);

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
VRClient::SendRequest()
{
    Panic("Deprecated function");
    thread_local proto::RequestMessage reqMsg;
    reqMsg.mutable_req()->set_op(std::move(pendingRequest->request));
    reqMsg.mutable_req()->set_clientid(clientid);
    reqMsg.mutable_req()->set_clientreqid(pendingRequest->clientReqId);
    transport->SendMessageToAll(this, reqMsg);

    //requestTimeout->Reset();
}

void
VRClient::ResendRequest()
{
    Warning("Client timeout; resending request");
    SendRequest();
}


void
VRClient::ReceiveMessage(const TransportAddress &remote,
                         const std::string_view type,
                         const std::string_view data,
                         void *meta_data)
{
    const static auto replyName = proto::ReplyMessage::default_instance().GetDescriptor()->full_name();
    const static auto unloggedReplyName = proto::UnloggedReplyMessage::default_instance().GetDescriptor()->full_name();

    if (type == replyName) {
        ALLOCATE_MESSAGE(proto::ReplyMessage, reply);
        reply->ParseFromArray(data.data(), data.length());
        HandleReply(remote, *reply);
    } else if (type == unloggedReplyName) {
        ALLOCATE_MESSAGE(proto::UnloggedReplyMessage, unloggedReply);
        unloggedReply->ParseFromArray(data.data(), data.length());
        HandleUnloggedReply(remote, *unloggedReply);
    } else {
        Client::ReceiveMessage(remote, type, data, NULL);
    }
}

void
VRClient::HandleReply(const TransportAddress &remote,
                      const proto::ReplyMessage &msg)
{
    auto clientreqid = msg.clientreqid();
    responses[clientreqid]++;

    // proxy to check whether the responses are from the same view.
    assert(msg.view() == 0); 

    const bool from_leader =
        (uint64_t) config.GetLeaderIndex(msg.view()) == msg.replicaidx();
    if (from_leader) {
        leader_acked[clientreqid] = 1;
    }

    auto& pending_request = pending_requests[clientreqid];

    if (clientreqid != pending_request.clientReqId) {
        Warning("Received reply for a different request %d != %d", clientreqid, pending_request.clientReqId);
        return;
    }

    const bool timing = ClientTimingEnabled();
    if (timing && from_leader && pending_request.t_leader_us == 0) {
        pending_request.t_leader_us = NowUs();
    }

    Debug2("Client received reply");

    if(responses[clientreqid] >= quorum
        && leader_acked[clientreqid]) {
        //requestTimeout->Stop();
        //pending_request.continuation(pending_request.request, msg.reply());

        if (timing && pending_request.t_send_us != 0) {
            const uint64_t now = NowUs();
            const uint64_t d_quorum = now - pending_request.t_send_us;
            RecordTiming(g_quorum_hist, d_quorum);
            if (pending_request.t_leader_us != 0) {
                const uint64_t d_leader =
                    pending_request.t_leader_us - pending_request.t_send_us;
                RecordTiming(g_leader_hist, d_leader);
                const uint64_t d_tail = now >= pending_request.t_leader_us
                                            ? now - pending_request.t_leader_us
                                            : 0;
                RecordTiming(g_tail_hist, d_tail);
            }
        }

        // return nothing since the client doesn't use the parameters anyways...
        pending_request.continuation("", "");
        responses.erase(clientreqid);
        leader_acked.erase(clientreqid);
        pending_requests.erase(clientreqid);
        // Notice("Got response: %s", msg.reply().c_str());
    }
}

void
VRClient::HandleUnloggedReply(const TransportAddress &remote,
                              const proto::UnloggedReplyMessage &msg)
{
    if (pendingUnloggedRequest == NULL) {
        Warning("Received unloggedReply when no request was pending");
        return;
    }

    Debug2("Client received unloggedReply");

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
}

} // namespace vr
} // namespace specpaxos

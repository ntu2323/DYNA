// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/client.h
//   DynaClient — drives a transaction through MULTICASTING → COLLECTING →
//   SLOW_WAIT → DONE. Collects SpecReplies into a multi-leader TxnQuorum (DECISION A);
//   on a fast quorum emits FastCommit and returns. A WRITE takes the slow path —
//   SlowCommit(max leader t), then wait for a CommitReply quorum at agreedT — when the
//   involved leaders disagree, or when every leader replied but not enough matching
//   replies arrived within the slow-path timeout (e.g. followers got it late). A READ
//   that doesn't reach a quorum retries as a fresh attempt at a larger t.

#ifndef _DYNA_CLIENT_H_
#define _DYNA_CLIENT_H_

#include "common/client.h"
#include "lib/transport.h"

#include "dyna/commit_quorum.h"
#include "dyna/deadline.h"
#include "dyna/dyna-proto.pb.h"
#include "dyna/topology.h"

#include <cstdint>
#include <map>
#include <set>
#include <memory>
#include <string>
#include <unordered_map>

namespace specpaxos {
namespace dyna {

class DynaClient : public Client
{
public:
    DynaClient(const specpaxos::Configuration &config,
                    Transport *transport,
                    uint64_t clientid = 0);
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
    enum class Phase { COLLECTING, SLOW_WAIT };

    struct PendingRequest {
        std::string request;
        uint64_t    request_id = 0;
        proto::TxnType type = proto::WRITE;
        std::string key;
        std::string value;
        uint64_t    currentT = 0;   // fixed for a write across resends (spec: same t)
        uint64_t    owd = 0;        // READ only: grows each retry (spec: larger t)
        uint64_t    send_us = 0;
        Phase       phase = Phase::COLLECTING;
        TxnQuorum   quorum;
        std::map<std::string, std::string> results;   // READ: merged from involved leaders
        continuation_t continuation;
    };

    uint64_t NowMicros() const;
    bool     IsReadCommand(const std::string &request) const;
    std::string KeyOf(const std::string &request) const;
    std::string ValueOf(const std::string &request) const;
    void SendRequestProto(uint64_t request_id);   // (re)multicast with a fresh send_time
    void ResendSweep();
    void GrowReadOwd(PendingRequest &p);   // enlarge a READ's deadline for the next retry
    // Re-issue a READ as a new attempt: fresh request_id (servers never answer it from a
    // stale cache, and replies to the old attempt are ignored), reset quorum/results,
    // larger t. Returns the new request_id.
    uint64_t RetryRead(uint64_t request_id);
    void SendSlowCommit(PendingRequest &p);
    // Involved leaders of a pending request under the current view's topology.
    std::set<uint32_t> LeadersFor(const PendingRequest &p) const;
    // A replica told us about a newer view: adopt its topology and restart every pending
    // request there (writes resend at the same t; reads retry as fresh attempts).
    void HandleViewInfo(const proto::ViewInfo &m);
    uint64_t SlowTimeoutUs() const;
    void Commit(std::unordered_map<uint64_t, PendingRequest>::iterator it,
                const std::string &result);

    uint64_t last_req_id_ = 0;
    uint64_t viewId_ = 0;
    bool     debug_ = false;   // DYNA_DEBUG: log reply/quorum flow
    Topology topo_;
    size_t   write_quorum_;
    uint64_t resend_ms_;
    uint64_t slow_timeout_us_;   // min wait (all leaders in) before forcing the slow path
    ClientBoundTracker bound_tracker_;
    std::unordered_map<uint64_t, PendingRequest> pending_;
    std::unique_ptr<Timeout> resend_sweep_timer_;
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_CLIENT_H_

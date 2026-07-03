// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _DYNA_NEW_CLIENT_H_
#define _DYNA_NEW_CLIENT_H_

#include "common/client.h"
#include "lib/transport.h"
#include "dyna_new/deadline.h"
#include "dyna_new/commit_quorum.h"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

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
        uint64_t first_submit_us = 0;   // for end-to-end commit latency (stats)
        uint64_t first_reply_us = 0;    // when the first reply arrived (phase split)
        uint64_t fwd_us = 0;            // send→replica-release (PTP-comparable leg)
        uint64_t rev_us = 0;            // replica-release→first-reply-recv leg
        uint32_t resends = 0;           // retransmit count for this request (stats)
        bool is_read = false;
        continuation_t continuation;
        CommitQuorum quorum;
        // Retransmission timer (spec §5): fires if the request has not committed.
        std::unique_ptr<Timeout> resend_timer;
    };

    uint64_t NowMicros() const;
    bool IsReadCommand(const std::string &request) const;
    std::string KeyOf(const std::string &request) const;
    std::string ValueOf(const std::string &request) const;
    // (Re)build and multicast the request proto for a pending request, with a
    // fresh send_time and recomputed deadline; keeps the same request_id.
    void SendRequestProto(uint64_t request_id);
    // Resend timer callback: resend + reschedule while the request is pending.
    void OnResendTimeout(uint64_t request_id);
    // DYNA_NEW_CLIENT_STATS: record a commit and periodically log percentiles.
    // total = submit→commit; to_first_reply = submit→first reply (hold + 1 one-way).
    void RecordCommitStat(uint64_t total_us, uint64_t to_first_reply_us,
                          uint64_t fwd_us, uint64_t rev_us, uint32_t resends);

    uint64_t last_req_id_ = 0;
    uint32_t num_partitions_;
    size_t write_quorum_;
    uint64_t resend_ms_;
    ClientBoundTracker bound_tracker_;
    std::unordered_map<uint64_t, PendingRequest> pending_;

    // Client-side diagnostics (off unless DYNA_NEW_CLIENT_STATS is set).
    bool client_stats_enabled_ = false;
    uint64_t client_stats_every_ = 10000;
    std::vector<uint64_t> stat_latencies_us_;   // total commit latency this window
    std::vector<uint64_t> stat_to_first_us_;    // submit→first-reply this window
    std::vector<uint64_t> stat_fwd_us_;         // send→replica-release leg this window
    std::vector<uint64_t> stat_rev_us_;         // replica-release→recv leg this window
    std::vector<uint32_t> stat_resends_;        // resends per commit this window
    // Client send-issue cost: send_time stamp → after SendMessageToAll (3x
    // serialize+sendto). Cumulative; a hop-latency breakdown (Step 1).
    uint64_t stat_send_issue_sum_us_ = 0;
    uint64_t stat_send_issue_count_ = 0;
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_CLIENT_H_

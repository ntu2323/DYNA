// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _DYNA_NEW_REPLICA_H_
#define _DYNA_NEW_REPLICA_H_

#include "lib/configuration.h"
#include "lib/transport.h"

#include "dyna_new/deadline.h"
#include "dyna_new/partition_orderer.h"
#include "dyna_new/partition_state.h"
#include "dyna_new/request_dedup.h"
#include "dyna_new/storage_backend.h"
#include "dyna_new/wall_clock.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

namespace specpaxos {
namespace dyna_new {

class DynaNewReplica : public TransportReceiver
{
public:
    DynaNewReplica(const specpaxos::Configuration &config,
                   int replicaIdx,
                   Transport *transport);
    ~DynaNewReplica() override;

    void ReceiveMessage(const TransportAddress &remote,
                        const std::string_view type,
                        const std::string_view data,
                        void *meta_data) override;

private:
    // Fired by partition `pid`'s orderer when an entry releases: sends the client
    // reply and, for the leader, broadcasts the sync to followers.
    void OnRelease(uint32_t pid, bool is_leader, const LogEntry &e);
    // Self-healing: re-broadcast Sync for the leader's unconfirmed entries and
    // resend the follower's pending fetches (recovers lost control messages).
    void OnMaintenance(uint32_t pid,
                       const std::vector<LogEntry> &resync,
                       const std::vector<ReconAction> &fetches);
    // Flush the SyncBatch accumulated during a release drain (leader, §9 batching).
    void OnPostDrain(uint32_t pid);
    void SendReply(uint32_t pid, bool is_leader, const LogEntry &e);
    // One SyncBatch / CommitBatch for many entries (cuts per-op transport load).
    void BroadcastSyncBatch(uint32_t pid, const std::vector<LogEntry> &entries);
    void BroadcastCommitBatch(uint32_t pid,
                              const std::vector<ClientAndReqId> &committed);
    // DYNA_NEW_STATS: periodic per-partition stats dump (re-arms itself).
    void StatsDump();
    // Executes the messages a follower's reconciliation asked us to send; batches
    // the sync_confirms into a single SyncConfirmBatch.
    void ExecuteReconActions(uint32_t pid, const std::vector<ReconAction> &actions);

    specpaxos::Configuration config;
    int replicaIdx;
    Transport *transport;

    std::unique_ptr<StorageBackend> backend_;

    uint32_t numPartitions_;
    uint64_t reconIntervalUs_;   // self-healing re-sync/re-fetch cadence

    // DYNA_NEW_STATS diagnostics (off unless the env var is set).
    bool statsEnabled_ = false;
    uint64_t statsIntervalMs_ = 1000;
    std::atomic<uint64_t> reqIn_{0}, syncIn_{0}, confirmIn_{0}, commitIn_{0},
        fetchReqIn_{0}, fetchRespIn_{0};
    std::atomic<uint64_t> dedupSubmit_{0}, dedupDrop_{0}, dedupResend_{0};
    // Hop-latency breakdown (Step 1): replica receive-processing (arrival→post-dedup)
    // and reply-send (OnRelease→after SendMessage), to localize the ~85µs/hop.
    std::atomic<uint64_t> procDelaySumUs_{0}, procDelayCount_{0};
    std::atomic<uint64_t> replySendSumUs_{0}, replySendCount_{0}, replySendMaxUs_{0};
    // Finer split: just the transport->SendMessage call inside SendReply (serialize
    // + sendto). replySend − replyXmit ≈ our own pre-send work + deschedule.
    std::atomic<uint64_t> replyXmitSumUs_{0}, replyXmitCount_{0};
    // How late releases fire vs their deadline (release_time − deadline). ~0 means
    // the release loop is prompt; large means it woke late (cv-park at idle).
    std::atomic<uint64_t> releaseLateSumUs_{0}, releaseLateCount_{0},
        releaseLateMaxUs_{0};
    // p50/p90 samplers (averages are tail-corrupted here — a fat 30ms tail hides
    // the median). residence = arrival→release; late = release−deadline.
    struct LatSampler {
        std::mutex mu;
        std::vector<uint64_t> v;
        void add(uint64_t x) {
            std::lock_guard<std::mutex> l(mu);
            if (v.size() < 16384) v.push_back(x);
        }
    };
    LatSampler residenceSampler_;   // arrival_us → release_time (client-visible)
    LatSampler lateSampler_;        // release − deadline (clamped ≥0)
    LatSampler presendSampler_;     // time inside SendReply before the send stamp
    LatSampler rawOwdSampler_;      // raw per-request send→arrival (cf client fwd_us)
    // Cumulative pump-busy time (µs) inside ReceiveMessage — request vs §7 control.
    // Diff two 1s stat lines: if pumpCtrl/sec is a large fraction of 1e6, the single
    // transport thread is saturated by §7 and blocks request pickup.
    std::atomic<uint64_t> pumpReqBusyUs_{0}, pumpCtrlBusyUs_{0};
    // One clock + orderer per partition (leader of partition==replicaIdx,
    // follower of the rest). Separate clocks so a Submit only wakes its own
    // partition's release loop.
    std::vector<std::unique_ptr<RealClock>> clocks_;
    std::vector<std::unique_ptr<PartitionOrderer>> orderers_;

    // Per-client OWD estimator, shared across partitions; guarded since it is
    // sampled on the transport thread and read on orderer release threads.
    std::mutex owd_mu_;
    ReplicaOwdEstimator owd_;

    // At-most-once dedup (spec §1.6). One per partition — a ⟨client,req⟩ always
    // routes to the same partition by key, so this needs no global lock. Written
    // on the transport thread (OnReceive) and on release threads (OnReply).
    struct PartitionDedup {
        std::mutex mu;
        RequestDedup dedup;
    };
    std::vector<std::unique_ptr<PartitionDedup>> dedup_;

    // Per-partition Sync accumulator: the leader appends released writes in
    // OnRelease and flushes one SyncBatch in OnPostDrain. Touched only by that
    // partition's single release thread, so it needs no lock.
    std::vector<std::vector<LogEntry>> pendingSync_;
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_REPLICA_H_

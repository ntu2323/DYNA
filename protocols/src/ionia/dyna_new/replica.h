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

#include <algorithm>
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

    // One backend per partition: keys are partitioned, so per-partition backends
    // avoid a global mem_mu_ contended by all release threads (a per-write cost).
    std::vector<std::unique_ptr<StorageBackend>> backends_;

    uint32_t numPartitions_;
    uint64_t reconIntervalUs_;   // self-healing re-sync/re-fetch cadence
    bool eagerEnabled_ = false;  // eager inline reply on receive thread (off: neutral)
    uint64_t eagerSpinUs_ = 0;   // spin budget in the eager path (0 = due-only, no spin)
    std::atomic<uint64_t> eagerReleases_{0};  // entries released by the eager path

    // DYNA_NEW_STATS diagnostics (off unless the env var is set).
    bool statsEnabled_ = false;
    uint64_t statsIntervalMs_ = 1000;
    std::atomic<uint64_t> reqIn_{0}, syncIn_{0}, confirmIn_{0}, commitIn_{0},
        fetchReqIn_{0}, fetchRespIn_{0};
    std::atomic<uint64_t> dedupSubmit_{0}, dedupDrop_{0}, dedupResend_{0};
    // Skew-immune replica-internal latency split (all deltas on ONE replica clock).
    // pumpProc = receive→submit (pump per-request cost); replicaInternal =
    // receive→reply-sent (pump + hold + release + reply-build). Compared to the
    // client's to_first (157µs) and IONIA (54µs) these localize DYNA's ~53µs excess
    // to pump / release / outside-the-replica. p50 (not avg) — a fat tail corrupts avg.
    struct LatSampler {
        std::mutex mu;
        std::vector<uint64_t> v;
        void add(uint64_t x) {
            std::lock_guard<std::mutex> l(mu);
            if (v.size() < 16384) v.push_back(x);
        }
        uint64_t drain_p50() {
            std::lock_guard<std::mutex> l(mu);
            if (v.empty()) return 0;
            std::sort(v.begin(), v.end());
            uint64_t p = v[v.size() / 2];
            v.clear();
            return p;
        }
    };
    LatSampler pumpProcSampler_;        // receive → submit (pump per-request)
    LatSampler replicaInternalSampler_; // receive → reply-sent (whole replica path)
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

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _DYNA_NEW_REPLICA_H_
#define _DYNA_NEW_REPLICA_H_

#include "lib/configuration.h"
#include "lib/transport.h"

#include "dyna_new/deadline.h"
#include "dyna_new/partition_orderer.h"
#include "dyna_new/partition_state.h"
#include "dyna_new/storage_backend.h"
#include "dyna_new/wall_clock.h"

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
    void SendReply(uint32_t pid, bool is_leader, const LogEntry &e);
    void BroadcastSync(uint32_t pid, const LogEntry &e);
    void BroadcastCommit(uint32_t pid, uint64_t client_id, uint64_t request_id);
    // Executes the messages a follower's reconciliation asked us to send.
    void ExecuteReconActions(uint32_t pid, const std::vector<ReconAction> &actions);

    specpaxos::Configuration config;
    int replicaIdx;
    Transport *transport;

    std::unique_ptr<StorageBackend> backend_;

    uint32_t numPartitions_;
    // One clock + orderer per partition (leader of partition==replicaIdx,
    // follower of the rest). Separate clocks so a Submit only wakes its own
    // partition's release loop.
    std::vector<std::unique_ptr<RealClock>> clocks_;
    std::vector<std::unique_ptr<PartitionOrderer>> orderers_;

    // Per-client OWD estimator, shared across partitions; guarded since it is
    // sampled on the transport thread and read on orderer release threads.
    std::mutex owd_mu_;
    ReplicaOwdEstimator owd_;
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_REPLICA_H_

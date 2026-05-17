// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _DYNA_REPLICA_H_
#define _DYNA_REPLICA_H_

#include "dyna/dyna-proto.pb.h"
#include "dyna/deadline/deadline_config.h"
#include "dyna/deadline/replica_owd_estimator.h"
#include "lib/configuration.h"
#include "lib/transport.h"
#include "third_party/concurrentqueue/concurrentqueue.h"

#include <atomic>
#include <chrono>
#include <deque>
#include <memory>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace specpaxos {
namespace dyna {

class DynaReplica : public TransportReceiver
{
public:
    DynaReplica(const Configuration &config, int replicaIdx, Transport *transport);
    ~DynaReplica() override;

    void ReceiveMessage(const TransportAddress &remote,
                        const std::string_view type,
                        const std::string_view data,
                        void *meta_data) override;

private:
    enum class InboundKind {
        SHUTDOWN,
        REQUEST,
        SYNC_BROADCAST,
        SYNC_ACK,
        COMMIT_BROADCAST,
        LATE_SYNC_BROADCAST,
        FETCH_REQUEST,
        FETCH_RESPONSE,
    };

    struct InboundWork {
        InboundKind kind = InboundKind::SHUTDOWN;
        std::unique_ptr<TransportAddress> remote;
        std::string payload;
    };

    struct RequestKey {
        uint64_t clientId = 0;
        uint64_t requestId = 0;

        bool operator==(const RequestKey &other) const {
            return clientId == other.clientId && requestId == other.requestId;
        }
    };

    struct RequestKeyHash {
        size_t operator()(const RequestKey &k) const {
            return std::hash<uint64_t>()(k.clientId) ^ (std::hash<uint64_t>()(k.requestId) << 1);
        }
    };

    struct BufferedRequest {
        proto::RequestMessage request;
        std::shared_ptr<TransportAddress> clientAddress;
        uint64_t sequence = 0;
    };

    struct DeadlineCompare {
        bool operator()(const BufferedRequest &a, const BufferedRequest &b) const {
            if (a.request.deadline_us() != b.request.deadline_us()) {
                return a.request.deadline_us() > b.request.deadline_us();
            }
            return a.sequence > b.sequence;
        }
    };

    struct LogEntry {
        bool valid = false;
        bool committed = false;
        bool applied = false;
        uint64_t logIndex = 0;
        uint64_t clientId = 0;
        uint64_t requestId = 0;
        std::string command;
        uint64_t sendTimeUs = 0;
        uint64_t deadlineUs = 0;
        bool isRead = false;
    };

    struct StoredWrite {
        uint32_t partitionId = 0;
        uint64_t logIndex = 0;
        std::string key;
        std::string value;
    };

    struct PartitionState {
        uint64_t viewId = 0;
        uint64_t deadlineCounter = 0;
        uint64_t nextSequence = 0;
        uint64_t nextLogIndex = 0;
        uint64_t commitPoint = 0;
        int64_t walSyncedUpTo = -1;
        std::priority_queue<BufferedRequest, std::vector<BufferedRequest>, DeadlineCompare> earlyBuffer;
        std::unordered_map<RequestKey, proto::RequestMessage, RequestKeyHash> latBuffer;
        std::unordered_map<RequestKey, std::shared_ptr<TransportAddress>, RequestKeyHash> requestClients;
        std::unordered_map<RequestKey, uint64_t, RequestKeyHash> requestToLogIndex;
        std::vector<LogEntry> logEntries;
        std::unordered_map<uint64_t, std::unordered_map<uint32_t, proto::SyncAck>> ackMap;
        std::unordered_set<uint64_t> repliedReads;
        std::unordered_map<RequestKey, std::pair<uint64_t, uint64_t>, RequestKeyHash> pendingSyncByReq;
    };

    Configuration config;
    uint32_t numPartitions;
    int replicaIdx;
    Transport *transport;
    std::vector<PartitionState> partitions;
    std::deque<StoredWrite> appliedWrites;
    std::unordered_map<std::string, std::string> kvStore;
    ReplicaOwdEstimator owdEstimator_;
    std::string walDir_;

    moodycamel::ConcurrentQueue<InboundWork> inboundQu_;
    std::thread workerThread_;
    std::atomic<bool> workerRunning_{false};

    void StartWorker();
    void StopWorker();
    void WorkerLoop();
    void DispatchInbound(InboundWork &work);
    bool EnqueueInbound(InboundKind kind,
                        const TransportAddress &remote,
                        const std::string_view type,
                        const std::string_view data);

    uint64_t NowMicros() const;
    uint32_t PartitionLeader(uint32_t partitionId) const;
    bool AmLeader(uint32_t partitionId) const;
    RequestKey KeyOf(const proto::RequestMessage &req) const;
    PartitionState &StateFor(uint32_t partitionId);
    const PartitionState &StateFor(uint32_t partitionId) const;
    void Tick();
    void HandleRequest(const TransportAddress &remote, const proto::RequestMessage &req);
    void HandleLateRequest(const TransportAddress &remote, const proto::RequestMessage &req, uint32_t partitionId);
    void HandleLateSyncBroadcast(const TransportAddress &remote, const proto::LateSyncBroadcast &lsb);
    void HandleSyncBroadcast(const TransportAddress &remote, const proto::SyncBroadcast &sb);
    void HandleSyncAck(const TransportAddress &remote, const proto::SyncAck &ack);
    void HandleCommitBroadcast(const TransportAddress &remote, const proto::CommitBroadcast &cb);
    void HandleFetchRequest(const TransportAddress &remote, const proto::FetchRequest &fr);
    void HandleFetchResponse(const TransportAddress &remote, const proto::FetchResponse &fr);
    void ProcessDueRequests(uint32_t partitionId);
    void AppendToLog(uint32_t partitionId, const proto::RequestMessage &req);
    void BroadcastSyncForLogEntry(uint32_t partitionId, const LogEntry &entry);
    void BroadcastLateSyncForLogEntry(uint32_t partitionId,
                                      const LogEntry &entry,
                                      uint64_t oldDeadlineUs,
                                      uint64_t newDeadlineUs);
    void EnsureFollowerEntryFromSync(uint32_t partitionId, const proto::SyncBroadcast &sb);
    void TryCommitFromAck(uint32_t partitionId, const proto::SyncAck &ack);
    /** Leader: WAL fsync through upToLogIndex, then CommitBroadcast (same commit step). */
    bool LeaderCommitToDiskAndBroadcast(uint32_t partitionId, uint64_t upToLogIndex);
    void ApplyCommittedUpTo(uint32_t partitionId, uint64_t upToLogIndex);
    void BroadcastCommit(uint32_t partitionId, uint64_t upToLogIndex);
    bool EntryMatchesSync(const LogEntry &entry, const proto::SyncBroadcast &sb) const;
    void SendFastReply(uint32_t partitionId,
                       const TransportAddress &remote,
                       uint64_t clientId,
                       uint64_t requestId,
                       uint64_t deadlineUs);
    void MaybeSendReadResult(uint32_t partitionId, const TransportAddress &remote, const proto::RequestMessage &req);
    void RequestFetch(uint32_t partitionId, uint64_t clientId, uint64_t requestId);
    bool TryPopulateEntryFromBuffers(uint32_t partitionId, const proto::SyncBroadcast &sb);
    bool PersistCommittedRange(uint32_t partitionId, uint64_t upToLogIndex);
    std::string WalPath(uint32_t partitionId) const;
    bool EnsureReadVisibilityBeforeDeadline(uint32_t partitionId, uint64_t readDeadlineUs);
    bool IsValidSyncBroadcast(uint32_t partitionId, const proto::SyncBroadcast &sb) const;
    bool IsValidSyncAck(uint32_t partitionId, const proto::SyncAck &ack) const;
    bool TryGetRequestMessage(uint32_t partitionId, const RequestKey &key, proto::RequestMessage &out) const;
    bool ParseCommand(const std::string &command, char &op, std::string &key, std::string &value) const;
    static std::string HexEncode(const std::string &data);
    bool IsReadOp(char op) const;
};

} // namespace dyna
} // namespace specpaxos

#endif  /* _DYNA_REPLICA_H_ */

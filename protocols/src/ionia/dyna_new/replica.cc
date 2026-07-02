// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "dyna_new/replica.h"

#include "dyna_new/dyna-new-proto.pb.h"
#include "dyna_new/in_memory_backend.h"
#include "dyna_new/partition_map.h"
#include "lib/message.h"

#include <memory>
#include <utility>

namespace specpaxos {
namespace dyna_new {

namespace proto = specpaxos::dyna_new::proto;

DynaNewReplica::DynaNewReplica(const specpaxos::Configuration &config,
                               int replicaIdx,
                               Transport *transport)
    : config(config), replicaIdx(replicaIdx), transport(transport),
      backend_(std::make_unique<InMemoryBackend>(InMemoryBackend::LatencyFromEnv())),
      numPartitions_(static_cast<uint32_t>(config.n)),
      reconIntervalUs_(DeadlineConfig::EnvU64("DYNA_NEW_RECON_INTERVAL_MS", 5) * 1000),
      owd_(DeadlineConfig::FromEnv())
{
    // One orderer per partition: leader of our own range, follower of the rest
    // (spec §1.2, §3). Each gets its own clock so a Submit only wakes that
    // partition's release loop.
    clocks_.reserve(numPartitions_);
    orderers_.reserve(numPartitions_);
    dedup_.reserve(numPartitions_);
    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
        clocks_.push_back(std::make_unique<RealClock>());
        dedup_.push_back(std::make_unique<PartitionDedup>());
        const bool is_leader = (pid == static_cast<uint32_t>(replicaIdx));
        auto role = is_leader ? PartitionOrderer::Role::kLeader
                              : PartitionOrderer::Role::kFollower;
        auto orderer = std::make_unique<PartitionOrderer>(
            pid, /*view=*/0, backend_.get(), clocks_[pid].get(), role,
            [this, pid, is_leader](const LogEntry &e) {
                OnRelease(pid, is_leader, e);
            },
            /*commit_quorum=*/static_cast<uint32_t>(config.f) + 1,
            /*recon_interval_us=*/reconIntervalUs_,
            [this, pid](const std::vector<LogEntry> &resync,
                        const std::vector<ReconAction> &fetches) {
                OnMaintenance(pid, resync, fetches);
            });
        orderer->Start();
        orderers_.push_back(std::move(orderer));
    }

    transport->Register(this, config, 0, replicaIdx);

    const char *sv = std::getenv("DYNA_NEW_STATS");
    statsEnabled_ = (sv != nullptr && *sv != '\0' && *sv != '0');
    statsIntervalMs_ = DeadlineConfig::EnvU64("DYNA_NEW_STATS_INTERVAL_MS", 1000);
    if (statsIntervalMs_ == 0) statsIntervalMs_ = 1000;
    if (statsEnabled_) {
        transport->Timer(statsIntervalMs_, [this]() { StatsDump(); });
    }
}

DynaNewReplica::~DynaNewReplica()
{
    for (auto &o : orderers_) {
        o->Stop();
    }
}

void
DynaNewReplica::ReceiveMessage(const TransportAddress &remote,
                               const std::string_view type,
                               const std::string_view data,
                               void *meta_data)
{
    (void)meta_data;
    const static auto requestName =
        proto::Request::default_instance().GetDescriptor()->full_name();
    const static auto syncName =
        proto::Sync::default_instance().GetDescriptor()->full_name();
    const static auto syncConfirmName =
        proto::SyncConfirm::default_instance().GetDescriptor()->full_name();
    const static auto commitName =
        proto::Commit::default_instance().GetDescriptor()->full_name();
    const static auto fetchReqName =
        proto::FetchRequest::default_instance().GetDescriptor()->full_name();
    const static auto fetchRespName =
        proto::FetchResponse::default_instance().GetDescriptor()->full_name();

    if (type == requestName) {
        proto::Request req;
        if (!req.ParseFromArray(data.data(), data.size())) {
            return;
        }
        reqIn_.fetch_add(1, std::memory_order_relaxed);

        const uint32_t pid = PartitionOf(req.key(), numPartitions_);
        if (pid >= orderers_.size()) {
            return;
        }

        // At-most-once dedup (spec §1.6/§6.1): a retried request (spec §5) that
        // already released is answered from cache; one still in flight is dropped
        // so the orderer never holds two entries for the same ⟨client,req⟩.
        const ClientAndReqId key{req.client_id(), req.request_id()};
        {
            std::unique_lock<std::mutex> dlk(dedup_[pid]->mu);
            switch (dedup_[pid]->dedup.OnReceive(key)) {
            case RequestDedup::Decision::kResend: {
                proto::Reply cached = *dedup_[pid]->dedup.CachedReply(key);
                dlk.unlock();
                dedupResend_.fetch_add(1, std::memory_order_relaxed);
                transport->SendMessage(this, remote, cached);
                return;
            }
            case RequestDedup::Decision::kDrop:
                dedupDrop_.fetch_add(1, std::memory_order_relaxed);
                return;
            case RequestDedup::Decision::kSubmit:
                dedupSubmit_.fetch_add(1, std::memory_order_relaxed);
                break;
            }
        }

        // OWD sample (§2): one-way delay = receive_time - send_time. Only sampled
        // for genuinely-new requests so retries don't skew the estimate.
        const uint64_t now = WallClockMicros();
        if (now > req.send_time()) {
            std::lock_guard<std::mutex> lk(owd_mu_);
            owd_.sample(req.client_id(), now - req.send_time());
        }

        LogEntry e;
        e.client_id = req.client_id();
        e.request_id = req.request_id();
        e.type = req.type();
        e.key = req.key();
        e.command = req.command();
        e.send_time = req.send_time();
        e.deadline = req.deadline();
        e.client_addr = std::shared_ptr<TransportAddress>(remote.clone());

        orderers_[pid]->Submit(std::move(e));
        return;
    }

    if (type == syncName) {
        proto::Sync s;
        if (!s.ParseFromArray(data.data(), data.size())) return;
        syncIn_.fetch_add(1, std::memory_order_relaxed);
        const uint32_t pid = s.partition_id();
        if (pid >= orderers_.size()) return;
        SyncIn in;
        in.view_id = s.view_id();
        in.log_id = s.log_id();
        in.client_id = s.client_id();
        in.request_id = s.request_id();
        in.deadline = s.deadline();
        ExecuteReconActions(pid, orderers_[pid]->HandleSync(in));
        return;
    }

    if (type == syncConfirmName) {
        // A follower reports log agreement. The leader tallies confirmations and,
        // on f+1 (including its own append), persists off-path and broadcasts a
        // Commit (§7).
        proto::SyncConfirm c;
        if (!c.ParseFromArray(data.data(), data.size())) return;
        confirmIn_.fetch_add(1, std::memory_order_relaxed);
        const uint32_t pid = c.partition_id();
        if (pid >= orderers_.size()) return;
        if (orderers_[pid]->HandleSyncConfirm(c.replica_id(), c.client_id(),
                                              c.request_id(), c.deadline())) {
            BroadcastCommit(pid, c.client_id(), c.request_id());
        }
        return;
    }

    if (type == commitName) {
        // Background commit from the partition leader: persist our own copy (§7).
        proto::Commit cm;
        if (!cm.ParseFromArray(data.data(), data.size())) return;
        commitIn_.fetch_add(1, std::memory_order_relaxed);
        const uint32_t pid = cm.partition_id();
        if (pid >= orderers_.size()) return;
        orderers_[pid]->HandleCommit(cm.client_id(), cm.request_id());
        return;
    }

    if (type == fetchReqName) {
        proto::FetchRequest fr;
        if (!fr.ParseFromArray(data.data(), data.size())) return;
        fetchReqIn_.fetch_add(1, std::memory_order_relaxed);
        const uint32_t pid = fr.partition_id();
        if (pid >= orderers_.size()) return;
        LogEntry body;
        if (orderers_[pid]->LeaderFetch(fr.client_id(), fr.request_id(), body)) {
            proto::FetchResponse resp;
            resp.set_partition_id(pid);
            resp.set_client_id(body.client_id);
            resp.set_request_id(body.request_id);
            resp.set_key(body.key);
            resp.set_command(body.command);
            resp.set_send_time(body.send_time);
            resp.set_type(body.type);
            transport->SendMessage(this, remote, resp);
        }
        return;
    }

    if (type == fetchRespName) {
        proto::FetchResponse resp;
        if (!resp.ParseFromArray(data.data(), data.size())) return;
        fetchRespIn_.fetch_add(1, std::memory_order_relaxed);
        const uint32_t pid = resp.partition_id();
        if (pid >= orderers_.size()) return;
        LogEntry body;
        body.client_id = resp.client_id();
        body.request_id = resp.request_id();
        body.type = resp.type();
        body.key = resp.key();
        body.command = resp.command();
        body.send_time = resp.send_time();
        // client_addr stays null: we never received this request from the client,
        // so we owe it no reply (the client commits via the other replicas).
        ExecuteReconActions(pid, orderers_[pid]->HandleFetchResp(body));
        return;
    }
}

void
DynaNewReplica::OnRelease(uint32_t pid, bool is_leader, const LogEntry &e)
{
    SendReply(pid, is_leader, e);
    // Reads are not logged or synced (§6.3); only writes are reconciled.
    if (is_leader && e.type != proto::READ_ONLY) {
        BroadcastSync(pid, e);   // followers reconcile against this (§6.4)
    }
}

void
DynaNewReplica::OnMaintenance(uint32_t pid,
                              const std::vector<LogEntry> &resync,
                              const std::vector<ReconAction> &fetches)
{
    // Leader: re-broadcast Sync for entries still awaiting confirmations (a fresh
    // Sync is idempotent and re-elicits a lost SyncConfirm). Follower: resend the
    // pending fetches (ExecuteReconActions handles kFetch).
    for (const LogEntry &e : resync) {
        BroadcastSync(pid, e);
    }
    if (!fetches.empty()) {
        ExecuteReconActions(pid, fetches);
    }
}

void
DynaNewReplica::BroadcastSync(uint32_t pid, const LogEntry &e)
{
    proto::Sync sync;
    sync.set_view_id(0);
    sync.set_partition_id(pid);
    sync.set_log_id(e.log_pos);
    sync.set_client_id(e.client_id);
    sync.set_request_id(e.request_id);
    sync.set_deadline(e.deadline);   // possibly reassigned by the slow path (§6.2)
    transport->SendMessageToAll(this, sync);   // self ignores (leader-role HandleSync)
}

void
DynaNewReplica::BroadcastCommit(uint32_t pid, uint64_t client_id,
                                uint64_t request_id)
{
    proto::Commit commit;
    commit.set_view_id(0);
    commit.set_partition_id(pid);
    commit.set_client_id(client_id);
    commit.set_request_id(request_id);
    transport->SendMessageToAll(this, commit);   // self ignores (leader-role HandleCommit)
}

void
DynaNewReplica::ExecuteReconActions(uint32_t pid,
                                    const std::vector<ReconAction> &actions)
{
    const uint32_t leader = LeaderOf(pid, numPartitions_);
    for (const ReconAction &a : actions) {
        switch (a.kind) {
        case ReconAction::Kind::kReply:
        case ReconAction::Kind::kCorrectedReply:
            SendReply(pid, /*is_leader=*/false, a.entry);
            break;
        case ReconAction::Kind::kConfirm: {
            proto::SyncConfirm c;
            c.set_view_id(0);
            c.set_replica_id(static_cast<uint32_t>(replicaIdx));
            c.set_partition_id(pid);
            c.set_client_id(a.entry.client_id);
            c.set_request_id(a.entry.request_id);
            c.set_deadline(a.deadline);
            transport->SendMessageToReplica(this, leader, c);
            break;
        }
        case ReconAction::Kind::kFetch: {
            proto::FetchRequest fr;
            fr.set_partition_id(pid);
            fr.set_client_id(a.entry.client_id);
            fr.set_request_id(a.entry.request_id);
            transport->SendMessageToReplica(this, leader, fr);
            break;
        }
        }
    }
}

void
DynaNewReplica::SendReply(uint32_t pid, bool is_leader, const LogEntry &e)
{
    if (!e.client_addr) {
        return;
    }

    proto::Reply reply;
    reply.set_view_id(0);
    reply.set_replica_id(static_cast<uint32_t>(replicaIdx));
    reply.set_partition_id(pid);
    reply.set_client_id(e.client_id);
    reply.set_request_id(e.request_id);
    reply.set_deadline(e.deadline);
    reply.set_is_leader(is_leader);

    if (e.type == proto::READ_ONLY) {
        // Read commit matches on log position; the leader carries the data (§6.3).
        reply.set_log_pos(e.log_pos);
        if (e.has_result) {
            reply.set_result(e.result);
        }
    }

    {
        std::lock_guard<std::mutex> lk(owd_mu_);
        reply.set_owd_estimate(owd_.estimate_us(e.client_id));
    }

    // Cache for at-most-once (spec §6.1): a later retry is answered from here.
    // A corrected reply (§6.4) overwrites the speculative one. Also clears the
    // in-flight mark set at receive time.
    if (pid < dedup_.size()) {
        const ClientAndReqId key{e.client_id, e.request_id};
        std::lock_guard<std::mutex> dlk(dedup_[pid]->mu);
        dedup_[pid]->dedup.OnReply(key, reply);
    }

    transport->SendMessage(this, *e.client_addr, reply);
}

void
DynaNewReplica::StatsDump()
{
    uint64_t fast = 0, slow = 0, resync = 0, refetch = 0, commits = 0;
    size_t confirmBacklog = 0, syncGaps = 0, earlyBuf = 0;
    for (auto &o : orderers_) {
        fast += o->fast_submits();
        slow += o->slow_submits();
        resync += o->resync_count();
        refetch += o->refetch_count();
        commits += o->commit_count();
        confirmBacklog += o->confirm_state_size();
        syncGaps += o->pending_syncs_size();
        earlyBuf += o->early_buffer_size();
    }
    // Cumulative counters (diff consecutive lines for rates) + instantaneous
    // gauges. slow/(fast+slow) = slow-path ratio; resync/refetch = healing (loss);
    // confirmBacklog/syncGaps = commit/reconciliation backlog behind the tail.
    Notice("DYNA_NEW stats r%d: in[req=%lu sync=%lu conf=%lu commit=%lu fReq=%lu fResp=%lu] "
           "dedup[sub=%lu drop=%lu resend=%lu] release[fast=%lu slow=%lu] "
           "heal[resync=%lu refetch=%lu] commits=%lu "
           "gauge[confirmBacklog=%zu syncGaps=%zu earlyBuf=%zu]",
           replicaIdx,
           (unsigned long)reqIn_.load(), (unsigned long)syncIn_.load(),
           (unsigned long)confirmIn_.load(), (unsigned long)commitIn_.load(),
           (unsigned long)fetchReqIn_.load(), (unsigned long)fetchRespIn_.load(),
           (unsigned long)dedupSubmit_.load(), (unsigned long)dedupDrop_.load(),
           (unsigned long)dedupResend_.load(),
           (unsigned long)fast, (unsigned long)slow,
           (unsigned long)resync, (unsigned long)refetch, (unsigned long)commits,
           confirmBacklog, syncGaps, earlyBuf);

    if (statsEnabled_) {
        transport->Timer(statsIntervalMs_, [this]() { StatsDump(); });
    }
}

} // namespace dyna_new
} // namespace specpaxos

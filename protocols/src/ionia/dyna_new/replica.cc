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
    // Busy-poll the release loop (spin to deadline) when the transport busy-polls,
    // so cv/scheduler wakeup latency doesn't dominate commit latency.
    const char *bp = std::getenv("DYNA_NEW_BUSY_POLL");
    const bool busyPoll = (bp == nullptr) || (*bp != '0');

    clocks_.reserve(numPartitions_);
    orderers_.reserve(numPartitions_);
    dedup_.reserve(numPartitions_);
    pendingSync_.resize(numPartitions_);
    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
        clocks_.push_back(std::make_unique<RealClock>(busyPoll));
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
            },
            [this, pid]() { OnPostDrain(pid); });
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
        proto::SyncBatch::default_instance().GetDescriptor()->full_name();
    const static auto syncConfirmName =
        proto::SyncConfirmBatch::default_instance().GetDescriptor()->full_name();
    const static auto commitName =
        proto::CommitBatch::default_instance().GetDescriptor()->full_name();
    const static auto syncResendName =
        proto::SyncResendRequest::default_instance().GetDescriptor()->full_name();
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
        proto::SyncBatch s;
        if (!s.ParseFromArray(data.data(), data.size())) return;
        const uint32_t pid = s.partition_id();
        if (pid >= orderers_.size()) return;
        syncIn_.fetch_add(s.entries_size(), std::memory_order_relaxed);
        // Feed entries in log order to the per-entry handler; batch the resulting
        // confirms/replies into single messages via ExecuteReconActions.
        std::vector<ReconAction> actions;
        for (const proto::Sync &e : s.entries()) {
            SyncIn in;
            in.view_id = e.view_id();
            in.log_id = e.log_id();
            in.client_id = e.client_id();
            in.request_id = e.request_id();
            in.deadline = e.deadline();
            auto a = orderers_[pid]->HandleSync(in);
            actions.insert(actions.end(), a.begin(), a.end());
        }
        ExecuteReconActions(pid, actions);
        return;
    }

    if (type == syncConfirmName) {
        // A follower reports log agreement (batched). The leader tallies each and,
        // on f+1 (including its own append), persists off-path (§7); committed
        // entries are broadcast in one CommitBatch.
        proto::SyncConfirmBatch cb;
        if (!cb.ParseFromArray(data.data(), data.size())) return;
        const uint32_t pid = cb.partition_id();
        if (pid >= orderers_.size()) return;
        confirmIn_.fetch_add(cb.entries_size(), std::memory_order_relaxed);
        std::vector<ClientAndReqId> committed;
        for (const proto::SyncConfirm &c : cb.entries()) {
            if (orderers_[pid]->HandleSyncConfirm(c.replica_id(), c.client_id(),
                                                  c.request_id(), c.deadline())) {
                committed.push_back(ClientAndReqId{c.client_id(), c.request_id()});
            }
        }
        BroadcastCommitBatch(pid, committed);
        return;
    }

    if (type == commitName) {
        // Background commit from the partition leader (batched): persist our copies.
        proto::CommitBatch cb;
        if (!cb.ParseFromArray(data.data(), data.size())) return;
        const uint32_t pid = cb.partition_id();
        if (pid >= orderers_.size()) return;
        commitIn_.fetch_add(cb.entries_size(), std::memory_order_relaxed);
        for (const proto::Commit &cm : cb.entries()) {
            orderers_[pid]->HandleCommit(cm.client_id(), cm.request_id());
        }
        return;
    }

    if (type == syncResendName) {
        // A follower is stuck at a missing Sync; re-send Syncs from there to it
        // only (targeted gap recovery). Leader-role orderer answers; others no-op.
        proto::SyncResendRequest rr;
        if (!rr.ParseFromArray(data.data(), data.size())) return;
        const uint32_t pid = rr.partition_id();
        if (pid >= orderers_.size()) return;
        std::vector<LogEntry> entries;
        orderers_[pid]->LeaderCollectResync(rr.from_log_id(), /*max=*/64, entries);
        if (!entries.empty()) {
            proto::SyncBatch batch;
            batch.set_partition_id(pid);
            for (const LogEntry &e : entries) {
                proto::Sync *s = batch.add_entries();
                s->set_view_id(0);
                s->set_partition_id(pid);
                s->set_log_id(e.log_pos);
                s->set_client_id(e.client_id);
                s->set_request_id(e.request_id);
                s->set_deadline(e.deadline);
            }
            transport->SendMessageToReplica(this, rr.replica_id(), batch);
        }
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
    // Reads are not logged or synced (§6.3); only writes are reconciled. Accumulate
    // the sync; OnPostDrain flushes one SyncBatch for the whole drain (§9 batching,
    // zero added latency). Runs on this partition's release thread only.
    if (is_leader && e.type != proto::READ_ONLY) {
        pendingSync_[pid].push_back(e);
    }
}

void
DynaNewReplica::OnPostDrain(uint32_t pid)
{
    if (pendingSync_[pid].empty()) {
        return;
    }
    BroadcastSyncBatch(pid, pendingSync_[pid]);
    pendingSync_[pid].clear();
}

void
DynaNewReplica::OnMaintenance(uint32_t pid,
                              const std::vector<LogEntry> &resync,
                              const std::vector<ReconAction> &fetches)
{
    // Leader: re-broadcast Sync (one batch) for entries still awaiting confirmations
    // — idempotent + re-elicits a lost SyncConfirm. Follower: resend pending fetches.
    if (!resync.empty()) {
        BroadcastSyncBatch(pid, resync);
    }
    if (!fetches.empty()) {
        ExecuteReconActions(pid, fetches);
    }
}

void
DynaNewReplica::BroadcastSyncBatch(uint32_t pid, const std::vector<LogEntry> &entries)
{
    proto::SyncBatch batch;
    batch.set_partition_id(pid);
    for (const LogEntry &e : entries) {
        proto::Sync *sync = batch.add_entries();
        sync->set_view_id(0);
        sync->set_partition_id(pid);
        sync->set_log_id(e.log_pos);
        sync->set_client_id(e.client_id);
        sync->set_request_id(e.request_id);
        sync->set_deadline(e.deadline);   // possibly reassigned by the slow path (§6.2)
    }
    transport->SendMessageToAll(this, batch);   // self ignores (leader-role HandleSync)
}

void
DynaNewReplica::BroadcastCommitBatch(uint32_t pid,
                                     const std::vector<ClientAndReqId> &committed)
{
    if (committed.empty()) return;
    proto::CommitBatch batch;
    batch.set_partition_id(pid);
    for (const ClientAndReqId &k : committed) {
        proto::Commit *commit = batch.add_entries();
        commit->set_view_id(0);
        commit->set_partition_id(pid);
        commit->set_client_id(k.clientid);
        commit->set_request_id(k.clientreqid);
    }
    transport->SendMessageToAll(this, batch);   // self ignores (leader-role HandleCommit)
}

void
DynaNewReplica::ExecuteReconActions(uint32_t pid,
                                    const std::vector<ReconAction> &actions)
{
    const uint32_t leader = LeaderOf(pid, numPartitions_);
    proto::SyncConfirmBatch confirms;   // batched to the leader (§9)
    confirms.set_partition_id(pid);
    for (const ReconAction &a : actions) {
        switch (a.kind) {
        case ReconAction::Kind::kReply:
        case ReconAction::Kind::kCorrectedReply:
            SendReply(pid, /*is_leader=*/false, a.entry);
            break;
        case ReconAction::Kind::kConfirm: {
            proto::SyncConfirm *c = confirms.add_entries();
            c->set_view_id(0);
            c->set_replica_id(static_cast<uint32_t>(replicaIdx));
            c->set_partition_id(pid);
            c->set_client_id(a.entry.client_id);
            c->set_request_id(a.entry.request_id);
            c->set_deadline(a.deadline);
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
        case ReconAction::Kind::kSyncResend: {
            proto::SyncResendRequest rr;
            rr.set_partition_id(pid);
            rr.set_replica_id(static_cast<uint32_t>(replicaIdx));
            rr.set_from_log_id(a.log_id);
            transport->SendMessageToReplica(this, leader, rr);
            break;
        }
        }
    }
    if (confirms.entries_size() > 0) {
        transport->SendMessageToReplica(this, leader, confirms);
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

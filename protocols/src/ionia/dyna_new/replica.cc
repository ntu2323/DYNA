// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "dyna_new/replica.h"

#include "dyna_new/dyna-new-proto.pb.h"
#include "dyna_new/in_memory_backend.h"
#include "dyna_new/partition_map.h"
#include "lib/message.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

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
    // LRU cap for the per-partition dedup reply-cache (bounds a latency-drift
    // driver at scale). Must exceed the client retry window; 0 = unbounded.
    // Per-partition cap (each partition sees ~1/N of traffic). At a few hundred
    // ops/s/partition, 8192 ≈ tens of seconds of history — far longer than the
    // client retry window, so a retried request is never evicted early.
    const size_t dedupCap =
        static_cast<size_t>(DeadlineConfig::EnvU64("DYNA_NEW_DEDUP_CAP", 8192));
    // Trailing-window log GC: keep this many entries below commit_point in memory,
    // drop older (durably-committed) ones. Must exceed any healthy follower's lag.
    const uint64_t logKeep = DeadlineConfig::EnvU64("DYNA_NEW_LOG_KEEP", 4096);

    clocks_.reserve(numPartitions_);
    orderers_.reserve(numPartitions_);
    dedup_.reserve(numPartitions_);
    pendingSync_.resize(numPartitions_);
    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
        clocks_.push_back(std::make_unique<RealClock>(busyPoll));
        dedup_.push_back(std::make_unique<PartitionDedup>());
        dedup_.back()->dedup.set_cap(dedupCap);
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
            [this, pid]() { OnPostDrain(pid); },
            logKeep);
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
        const uint64_t recv_arrival = WallClockMicros();   // stamp at arrival (§Step1)
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

        // OWD sample (§2): one-way delay = arrival_time - send_time (stamped at the
        // top, before parse/dedup, so it reflects the wire+pickup, not our own
        // processing). Only sampled for genuinely-new requests so retries don't skew.
        // proc_delay = how long our own receive-path processing (parse/PartitionOf/
        // dedup-lock) took before this point — a hop-latency breakdown (Step 1).
        const uint64_t now = WallClockMicros();
        procDelaySumUs_.fetch_add(now - recv_arrival, std::memory_order_relaxed);
        procDelayCount_.fetch_add(1, std::memory_order_relaxed);
        if (recv_arrival > req.send_time()) {
            std::lock_guard<std::mutex> lk(owd_mu_);
            owd_.sample(req.client_id(), recv_arrival - req.send_time());
        }

        LogEntry e;
        e.client_id = req.client_id();
        e.request_id = req.request_id();
        e.type = req.type();
        e.key = req.key();
        e.command = req.command();
        e.send_time = req.send_time();
        e.deadline = req.deadline();
        e.arrival_us = recv_arrival;   // for arrival→release residence (diagnostic)
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
    const uint64_t t0 = WallClockMicros();
    // How late the release fires vs the entry's deadline. If the release loop is
    // prompt this is ~0; a large value means the loop woke late (e.g. cv-park at
    // idle). Clamped at 0 (an early release / clock skew shows as 0).
    const uint64_t late = (t0 > e.deadline) ? t0 - e.deadline : 0;
    if (late > 0) {
        releaseLateSumUs_.fetch_add(late, std::memory_order_relaxed);
        for (uint64_t cur = releaseLateMaxUs_.load(std::memory_order_relaxed);
             late > cur && !releaseLateMaxUs_.compare_exchange_weak(cur, late); ) {}
    }
    releaseLateCount_.fetch_add(1, std::memory_order_relaxed);
    lateSampler_.add(late);
    SendReply(pid, is_leader, e);
    const uint64_t dt = WallClockMicros() - t0;   // reply-send cost (Step 1)
    replySendSumUs_.fetch_add(dt, std::memory_order_relaxed);
    replySendCount_.fetch_add(1, std::memory_order_relaxed);
    for (uint64_t cur = replySendMaxUs_.load(std::memory_order_relaxed);
         dt > cur && !replySendMaxUs_.compare_exchange_weak(cur, dt); ) {}
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

    const uint64_t sr_start = WallClockMicros();   // SendReply entry (presend probe)
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

    const uint64_t xt0 = WallClockMicros();
    // PTP-comparable release stamp so the client can split to_first into a true
    // forward leg (send→release) and reverse leg (release→client-recv).
    reply.set_release_time(xt0);
    // residence = arrival→release_time (what the client's fwd leg actually sees,
    // skew-immune replica-only). presend = time spent in SendReply BEFORE this
    // stamp (reply build + owd_mu_ + dedup OnReply locks) — isolates whether the
    // fwd cost is here vs the wire.
    if (e.arrival_us != 0 && xt0 > e.arrival_us) {
        residenceSampler_.add(xt0 - e.arrival_us);
    }
    presendSampler_.add(xt0 - sr_start);
    transport->SendMessage(this, *e.client_addr, reply);
    const uint64_t xdt = WallClockMicros() - xt0;
    replyXmitSumUs_.fetch_add(xdt, std::memory_order_relaxed);
    replyXmitCount_.fetch_add(1, std::memory_order_relaxed);
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
    uint64_t owdMax = 0;
    {
        std::lock_guard<std::mutex> lk(owd_mu_);
        owdMax = owd_.max_estimate_us();
    }
    const uint64_t pc = procDelayCount_.load(std::memory_order_relaxed);
    const uint64_t procAvg = pc ? procDelaySumUs_.load() / pc : 0;
    const uint64_t rc = replySendCount_.load(std::memory_order_relaxed);
    const uint64_t replyAvg = rc ? replySendSumUs_.load() / rc : 0;
    const uint64_t xc = replyXmitCount_.load(std::memory_order_relaxed);
    const uint64_t xmitAvg = xc ? replyXmitSumUs_.load() / xc : 0;
    const uint64_t lc = releaseLateCount_.load(std::memory_order_relaxed);
    const uint64_t lateAvg = lc ? releaseLateSumUs_.load() / lc : 0;
    // Drain the p50/p90 samplers (avgs above are tail-corrupted; these are the
    // real medians for residence = arrival→release and late = release−deadline).
    auto drain = [](LatSampler &s, uint64_t &p50, uint64_t &p90) {
        std::vector<uint64_t> v;
        { std::lock_guard<std::mutex> l(s.mu); v.swap(s.v); }
        p50 = p90 = 0;
        if (v.empty()) return;
        std::sort(v.begin(), v.end());
        p50 = v[v.size() / 2];
        p90 = v[(v.size() * 9) / 10];
    };
    uint64_t resP50, resP90, lateP50, lateP90, preP50, preP90;
    drain(residenceSampler_, resP50, resP90);
    drain(lateSampler_, lateP50, lateP90);
    drain(presendSampler_, preP50, preP90);
    Notice("DYNA_NEW stats r%d: in[req=%lu sync=%lu conf=%lu commit=%lu fReq=%lu fResp=%lu] "
           "dedup[sub=%lu drop=%lu resend=%lu] release[fast=%lu slow=%lu] "
           "heal[resync=%lu refetch=%lu] commits=%lu owd_max=%lu "
           "procDelay_us[avg=%lu] replySend_us[avg=%lu max=%lu] replyXmit_us[avg=%lu] "
           "releaseLate_us[avg=%lu max=%lu] "
           "residence_us[p50=%lu p90=%lu] presend_us[p50=%lu p90=%lu] late_us[p50=%lu p90=%lu] "
           "gauge[confirmBacklog=%zu syncGaps=%zu earlyBuf=%zu]",
           replicaIdx,
           (unsigned long)reqIn_.load(), (unsigned long)syncIn_.load(),
           (unsigned long)confirmIn_.load(), (unsigned long)commitIn_.load(),
           (unsigned long)fetchReqIn_.load(), (unsigned long)fetchRespIn_.load(),
           (unsigned long)dedupSubmit_.load(), (unsigned long)dedupDrop_.load(),
           (unsigned long)dedupResend_.load(),
           (unsigned long)fast, (unsigned long)slow,
           (unsigned long)resync, (unsigned long)refetch, (unsigned long)commits,
           (unsigned long)owdMax,
           (unsigned long)procAvg, (unsigned long)replyAvg,
           (unsigned long)replySendMaxUs_.load(std::memory_order_relaxed),
           (unsigned long)xmitAvg,
           (unsigned long)lateAvg,
           (unsigned long)releaseLateMaxUs_.load(std::memory_order_relaxed),
           (unsigned long)resP50, (unsigned long)resP90,
           (unsigned long)preP50, (unsigned long)preP90,
           (unsigned long)lateP50, (unsigned long)lateP90,
           confirmBacklog, syncGaps, earlyBuf);

    if (statsEnabled_) {
        transport->Timer(statsIntervalMs_, [this]() { StatsDump(); });
    }
}

} // namespace dyna_new
} // namespace specpaxos

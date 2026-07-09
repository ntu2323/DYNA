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
    // Release-loop spin, independent of the transport busy-poll. Defaults to the
    // transport setting, but DYNA_NEW_RELEASE_SPIN=0 parks the release loops on a
    // cv (leaving only the transport pump spinning). Diagnostic: if the forward
    // leg / pickup latency drops with release spinning OFF, the per-partition
    // release spinners were starving the single transport pump for CPU.
    const char *rs = std::getenv("DYNA_NEW_RELEASE_SPIN");
    const bool releaseSpin = (rs == nullptr) ? busyPoll : (*rs != '0');
    // Eager fast reply on the receive thread. Default OFF: the local repro showed
    // the release loop already releases at deadline+~10µs, so eager isn't needed,
    // and doing release work on the pump busies it → inflates measured OWD → inflates
    // the deadline-hold bound (the dominant latency term). DYNA_NEW_EAGER=1 to A/B.
    const char *eg = std::getenv("DYNA_NEW_EAGER");
    eagerEnabled_ = (eg != nullptr) && (*eg != '0');
    // Max time the eager path will spin to a near-due deadline before drain+reply.
    // Deadlines are ~a few µs out at Submit, so a small budget covers the common
    // case; far-future entries fall through to the release loop.
    eagerSpinUs_ = DeadlineConfig::EnvU64("DYNA_NEW_EAGER_SPIN_US", 0);
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
        clocks_.push_back(std::make_unique<RealClock>(releaseSpin));
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

    // Make the release-clock spin mode visible: if this logs releaseSpin=0 the
    // release loop is cv-parking (≈100µs wakeup per release) — the prime suspect
    // for the Submit→release latency. Env can override the default (spin on).
    Notice("DYNA_NEW r%d config: busyPoll=%d releaseSpin=%d eager=%d eagerSpinUs=%lu reconIntervalUs=%lu",
           replicaIdx, busyPoll ? 1 : 0, releaseSpin ? 1 : 0, eagerEnabled_ ? 1 : 0,
           (unsigned long)eagerSpinUs_, (unsigned long)reconIntervalUs_);

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
    // Pump-busy probe: total wall time this single transport thread spends inside
    // ReceiveMessage, split request vs control (§7 sync/confirm/commit). If control
    // dominates, request PICKUP is blocked behind §7 processing — the likely cause
    // of the asymmetric forward leg (fwd 232µs ≫ rev 62µs) with residence≈0.
    const uint64_t pump_t0 = WallClockMicros();
    bool pump_is_request = false;
    struct PumpTimer {
        DynaNewReplica *self; const uint64_t &t0; const bool &is_req;
        ~PumpTimer() {
            const uint64_t d = WallClockMicros() - t0;
            if (is_req) self->pumpReqBusyUs_.fetch_add(d, std::memory_order_relaxed);
            else        self->pumpCtrlBusyUs_.fetch_add(d, std::memory_order_relaxed);
        }
    } pump_timer{this, pump_t0, pump_is_request};

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
        pump_is_request = true;   // this ReceiveMessage is a client request
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
        uint64_t owd_est = 0;
        {
            std::lock_guard<std::mutex> lk(owd_mu_);
            if (recv_arrival > req.send_time()) {
                owd_.sample(req.client_id(), recv_arrival - req.send_time());
            }
            // Snapshot the estimate here so SendReply needn't take owd_mu_ (a global
            // lock) on the release critical path.
            owd_est = owd_.estimate_us(req.client_id());
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
        e.recv_us = recv_arrival;      // T2 for the client's NTP clock-offset calc
        e.owd_estimate_us = owd_est;   // snapshot so SendReply skips the global owd_mu_
        e.client_addr = std::shared_ptr<TransportAddress>(remote.clone());

        // Eager path submits without poking the release loop, so the release loop
        // can't race the pump for the just-submitted entry during the eager spin;
        // the pump pokes it afterwards for anything it left behind.
        orderers_[pid]->Submit(std::move(e), /*poke=*/!eagerEnabled_);

        // Eager fast path (§ collapse the relay): if the request is already due,
        // release + reply INLINE on this receive thread instead of paying the
        // hand-off to the per-partition release thread. Most requests are due at
        // Submit (deadline ≈ now once pump processing is done), so this is the
        // common case. Not-yet-due entries stay in the buffer for the release loop.
        // The pump already sends replies inline (dedup-resend path), so this is a
        // proven-safe transport use; DrainDueEager pops under mu_ so the release
        // loop never double-releases.
        if (eagerEnabled_) {
            // The entry is typically a few µs short of due at Submit (submit≈send+67,
            // deadline≈send+70). Spin the small remaining time to its deadline, then
            // drain+reply inline. Bounded by eagerSpinUs_ so far-future deadlines are
            // left to the release loop and the pump is never blocked long.
            const uint64_t dl = req.deadline();
            const uint64_t now0 = WallClockMicros();
            if (dl <= now0 + eagerSpinUs_) {
                while (WallClockMicros() < dl) { /* brief spin to the deadline */ }
            }
            std::vector<LogEntry> eager;
            orderers_[pid]->DrainDueEager(eager);
            eagerReleases_.fetch_add(eager.size(), std::memory_order_relaxed);
            if (!eager.empty()) {
                const bool is_leader = (pid == static_cast<uint32_t>(replicaIdx));
                std::vector<LogEntry> writes;
                for (const LogEntry &re : eager) {
                    SendReply(pid, is_leader, re);
                    if (is_leader && re.type != proto::READ_ONLY) {
                        writes.push_back(re);
                    }
                }
                if (is_leader && !writes.empty()) {
                    // Sends may reorder vs the release loop's broadcasts, but
                    // followers reconcile by log_id (DrainSyncsLocked), so §7 order
                    // is preserved. Local batch — does not touch pendingSync_.
                    BroadcastSyncBatch(pid, writes);
                }
            }
            // Now let the release loop handle any not-yet-due entries left behind.
            orderers_[pid]->PokeReleaseLoop();
        }
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
    SendReply(pid, is_leader, e, /*onrelease_us=*/t0);
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
DynaNewReplica::SendReply(uint32_t pid, bool is_leader, const LogEntry &e,
                          uint64_t onrelease_us)
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

    // Use the estimate snapshotted at receive — no owd_mu_ (global lock) here.
    reply.set_owd_estimate(e.owd_estimate_us);

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
    reply.set_recv_time(e.recv_us);      // T2 for the client's NTP clock-offset calc
    reply.set_submit_time(e.arrival_us); // Submit stamp — splits the replica hold
    reply.set_drain_time(e.drain_us);    // release-loop drain stamp — splits rLoop
    reply.set_onrelease_time(onrelease_us ? onrelease_us : sr_start);
    // residence = arrival→release_time (what the client's fwd leg actually sees,
    // skew-immune replica-only). presend = time spent in SendReply BEFORE this
    // stamp (reply build + owd_mu_ + dedup OnReply locks) — isolates whether the
    // fwd cost is here vs the wire.
    // residence = recv→release (T3−T2): the TRUE replica hold, offset-immune (both
    // stamps are the replica clock). Uses recv_us (stamped in ReceiveMessage), which
    // provably survives to here since the NTP recv_time relies on it — unlike the
    // Submit-stamped arrival_us, whose sampler read empty.
    if (e.recv_us != 0 && xt0 > e.recv_us) {
        residenceSampler_.add(xt0 - e.recv_us);
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
    uint64_t lockWaitSum = 0, lockWaitCnt = 0, sendWorkSum = 0, sendWorkCnt = 0;
    uint64_t submitLockWaitSum = 0, submitInternalSum = 0, submitCnt = 0;
    for (auto &o : orderers_) {
        fast += o->fast_submits();
        slow += o->slow_submits();
        resync += o->resync_count();
        refetch += o->refetch_count();
        commits += o->commit_count();
        confirmBacklog += o->confirm_state_size();
        syncGaps += o->pending_syncs_size();
        earlyBuf += o->early_buffer_size();
        lockWaitSum += o->lock_wait_sum_us();
        lockWaitCnt += o->lock_wait_count();
        sendWorkSum += o->send_work_sum_us();
        sendWorkCnt += o->send_work_count();
        submitLockWaitSum += o->submit_lock_wait_sum_us();
        submitInternalSum += o->submit_internal_sum_us();
        submitCnt += o->submit_count();
    }
    const uint64_t sendWorkAvg = sendWorkCnt ? sendWorkSum / sendWorkCnt : 0;
    const uint64_t submitLockWaitAvg = submitCnt ? submitLockWaitSum / submitCnt : 0;
    const uint64_t submitInternalAvg = submitCnt ? submitInternalSum / submitCnt : 0;
    const uint64_t lockWaitAvg = lockWaitCnt ? lockWaitSum / lockWaitCnt : 0;
    // Interrupt→wake latency of the busy-poll release spin. If large, the spin is
    // slow to notice a Submit's Interrupt (e.g. sched_yield not rescheduling) —
    // the systematic Submit→release cost with lockWait≈0.
    uint64_t wakeLatSum = 0, wakeLatCnt = 0;
    for (auto &c : clocks_) {
        wakeLatSum += c->wake_lat_sum_us();
        wakeLatCnt += c->wake_lat_count();
    }
    const uint64_t wakeLatAvg = wakeLatCnt ? wakeLatSum / wakeLatCnt : 0;
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
    uint64_t resP50, resP90, lateP50, lateP90, preP50, preP90, rawP50, rawP90;
    drain(residenceSampler_, resP50, resP90);
    drain(lateSampler_, lateP50, lateP90);
    drain(presendSampler_, preP50, preP90);
    drain(rawOwdSampler_, rawP50, rawP90);
    const uint64_t pumpReq = pumpReqBusyUs_.load(std::memory_order_relaxed);
    const uint64_t pumpCtrl = pumpCtrlBusyUs_.load(std::memory_order_relaxed);
    Notice("DYNA_NEW stats r%d: in[req=%lu sync=%lu conf=%lu commit=%lu fReq=%lu fResp=%lu] "
           "dedup[sub=%lu drop=%lu resend=%lu] release[fast=%lu slow=%lu] "
           "heal[resync=%lu refetch=%lu] commits=%lu owd_max=%lu "
           "procDelay_us[avg=%lu] replySend_us[avg=%lu max=%lu] replyXmit_us[avg=%lu] "
           "releaseLate_us[avg=%lu max=%lu] "
           "rawOwd_us[p50=%lu p90=%lu] "
           "residence_us[p50=%lu p90=%lu] presend_us[p50=%lu p90=%lu] late_us[p50=%lu p90=%lu] "
           "pumpBusy_us[req=%lu ctrl=%lu] lockWait_us[avg=%lu] wakeLat_us[avg=%lu] "
           "sendWork_us[avg=%lu] submitLockWait_us[avg=%lu] submitInternal_us[avg=%lu] "
           "eagerRel=%lu "
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
           (unsigned long)rawP50, (unsigned long)rawP90,
           (unsigned long)resP50, (unsigned long)resP90,
           (unsigned long)preP50, (unsigned long)preP90,
           (unsigned long)lateP50, (unsigned long)lateP90,
           (unsigned long)pumpReq, (unsigned long)pumpCtrl,
           (unsigned long)lockWaitAvg, (unsigned long)wakeLatAvg,
           (unsigned long)sendWorkAvg,
           (unsigned long)submitLockWaitAvg, (unsigned long)submitInternalAvg,
           (unsigned long)eagerReleases_.load(std::memory_order_relaxed),
           confirmBacklog, syncGaps, earlyBuf);

    if (statsEnabled_) {
        transport->Timer(statsIntervalMs_, [this]() { StatsDump(); });
    }
}

} // namespace dyna_new
} // namespace specpaxos

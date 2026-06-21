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
      owd_(DeadlineConfig::FromEnv())
{
    // One orderer per partition: leader of our own range, follower of the rest
    // (spec §1.2, §3). Each gets its own clock so a Submit only wakes that
    // partition's release loop.
    clocks_.reserve(numPartitions_);
    orderers_.reserve(numPartitions_);
    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
        clocks_.push_back(std::make_unique<RealClock>());
        const bool is_leader = (pid == static_cast<uint32_t>(replicaIdx));
        auto role = is_leader ? PartitionOrderer::Role::kLeader
                              : PartitionOrderer::Role::kFollower;
        auto orderer = std::make_unique<PartitionOrderer>(
            pid, /*view=*/0, backend_.get(), clocks_[pid].get(), role,
            [this, pid, is_leader](const LogEntry &e) {
                OnRelease(pid, is_leader, e);
            });
        orderer->Start();
        orderers_.push_back(std::move(orderer));
    }

    transport->Register(this, config, 0, replicaIdx);
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
    const static auto fetchReqName =
        proto::FetchRequest::default_instance().GetDescriptor()->full_name();
    const static auto fetchRespName =
        proto::FetchResponse::default_instance().GetDescriptor()->full_name();

    if (type == requestName) {
        proto::Request req;
        if (!req.ParseFromArray(data.data(), data.size())) {
            return;
        }

        // OWD sample (§2): one-way delay = receive_time - send_time.
        const uint64_t now = WallClockMicros();
        if (now > req.send_time()) {
            std::lock_guard<std::mutex> lk(owd_mu_);
            owd_.sample(req.client_id(), now - req.send_time());
        }

        const uint32_t pid = PartitionOf(req.key(), numPartitions_);
        if (pid >= orderers_.size()) {
            return;
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
        // Followers report log agreement; the leader consumes these for
        // background persistence in a later stage (§7). Accept and ignore now.
        return;
    }

    if (type == fetchReqName) {
        proto::FetchRequest fr;
        if (!fr.ParseFromArray(data.data(), data.size())) return;
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
    if (is_leader) {
        BroadcastSync(pid, e);   // followers reconcile against this (§6.4)
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

    {
        std::lock_guard<std::mutex> lk(owd_mu_);
        reply.set_owd_estimate(owd_.estimate_us(e.client_id));
    }

    transport->SendMessage(this, *e.client_addr, reply);
}

} // namespace dyna_new
} // namespace specpaxos

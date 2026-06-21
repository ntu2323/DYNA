// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/tests/recon_test.cc
//   Stage 5 acceptance tests for follower reconciliation (spec §6.4): match ->
//   promote, deadline-differs -> corrected reply, sync-miss -> fetch, and the
//   "all synced_logs identical after commit" consistency property.

#include "dyna_new/partition_orderer.h"
#include "dyna_new/in_memory_backend.h"
#include "dyna_new/partition_state.h"
#include "dyna_new/wall_clock.h"
#include "dyna_new/dyna-new-proto.pb.h"

#include <gtest/gtest.h>

#include <vector>

using namespace specpaxos::dyna_new;
namespace proto = specpaxos::dyna_new::proto;

namespace {

LogEntry Write(uint64_t client, uint64_t req, uint64_t deadline,
               const std::string &key = "k", const std::string &val = "v")
{
    LogEntry e;
    e.client_id = client;
    e.request_id = req;
    e.type = proto::BLIND_WRITE;
    e.key = key;
    e.command = val;
    e.deadline = deadline;
    return e;
}

SyncIn Sync(uint64_t log_id, uint64_t client, uint64_t req, uint64_t deadline)
{
    SyncIn s;
    s.view_id = 0;
    s.log_id = log_id;
    s.client_id = client;
    s.request_id = req;
    s.deadline = deadline;
    return s;
}

int CountKind(const std::vector<ReconAction> &a, ReconAction::Kind k)
{
    int n = 0;
    for (const auto &x : a) if (x.kind == k) ++n;
    return n;
}

} // namespace

// §6.4 match: a follower reconciles syncs (same deadline) into synced_log in the
// leader's log_id order; no corrected reply needed.
TEST(Reconciliation, MatchBuildsSyncedLogInOrder)
{
    InMemoryBackend backend;
    MockClock clock(0);
    PartitionOrderer fol(0, 0, &backend, &clock, PartitionOrderer::Role::kFollower);

    fol.Submit(Write(1, 1, 100));
    fol.Submit(Write(1, 2, 200));
    clock.Set(200);
    fol.ReleaseDue();   // speculative release at the original deadlines

    auto a0 = fol.HandleSync(Sync(0, 1, 1, 100));
    auto a1 = fol.HandleSync(Sync(1, 1, 2, 200));

    ASSERT_EQ(fol.synced_log_size(), 2u);
    EXPECT_EQ(fol.synced_log_at(0).request_id, 1u);
    EXPECT_EQ(fol.synced_log_at(1).request_id, 2u);
    EXPECT_EQ(fol.sync_point(), 2u);
    // Deadlines already matched the speculative replies -> confirm only.
    EXPECT_EQ(CountKind(a0, ReconAction::Kind::kConfirm), 1);
    EXPECT_EQ(CountKind(a0, ReconAction::Kind::kCorrectedReply), 0);
    EXPECT_EQ(CountKind(a1, ReconAction::Kind::kConfirm), 1);
}

// §6.4 deadline differs: the leader reassigned (slow path); the follower adopts
// the leader's deadline and emits a corrected reply.
TEST(Reconciliation, DeadlineDiffersEmitsCorrectedReply)
{
    InMemoryBackend backend;
    MockClock clock(0);
    PartitionOrderer fol(0, 0, &backend, &clock, PartitionOrderer::Role::kFollower);

    fol.Submit(Write(1, 1, 100));
    clock.Set(100);
    fol.ReleaseDue();   // speculative reply on deadline 100

    auto a = fol.HandleSync(Sync(0, 1, 1, /*leader deadline=*/150));

    ASSERT_EQ(fol.synced_log_size(), 1u);
    EXPECT_EQ(fol.synced_log_at(0).deadline, 150u);   // adopted leader's deadline
    EXPECT_EQ(CountKind(a, ReconAction::Kind::kCorrectedReply), 1);
    EXPECT_EQ(CountKind(a, ReconAction::Kind::kConfirm), 1);
}

// §6.4 not-found: the follower never received the request (dropped). A sync for
// it triggers a fetch; once the body arrives the log reconciles.
TEST(Reconciliation, SyncMissFetchesThenReconciles)
{
    InMemoryBackend backend;
    MockClock clock(0);
    PartitionOrderer fol(0, 0, &backend, &clock, PartitionOrderer::Role::kFollower);

    // No Submit for req 1 — it was dropped at this follower.
    auto a = fol.HandleSync(Sync(0, 1, 1, 100));
    EXPECT_EQ(CountKind(a, ReconAction::Kind::kFetch), 1);
    EXPECT_EQ(fol.synced_log_size(), 0u);    // stuck at the gap
    EXPECT_EQ(fol.sync_point(), 0u);

    // Leader answers the fetch with the body.
    LogEntry body = Write(1, 1, 100);        // deadline here is irrelevant; sync wins
    auto b = fol.HandleFetchResp(body);

    ASSERT_EQ(fol.synced_log_size(), 1u);
    EXPECT_EQ(fol.synced_log_at(0).request_id, 1u);
    EXPECT_EQ(fol.synced_log_at(0).deadline, 100u);
    EXPECT_EQ(fol.sync_point(), 1u);
    EXPECT_EQ(CountKind(b, ReconAction::Kind::kConfirm), 1);
}

// Out-of-order syncs buffer until contiguous; a missing middle entry holds the
// log until its fetch completes, then later entries drain.
TEST(Reconciliation, OutOfOrderSyncsBufferUntilContiguous)
{
    InMemoryBackend backend;
    MockClock clock(0);
    PartitionOrderer fol(0, 0, &backend, &clock, PartitionOrderer::Role::kFollower);

    fol.Submit(Write(1, 2, 200));   // have body for log_id 1 only

    // log_id 1 arrives first: buffered, nothing applied (gap at 0).
    auto a1 = fol.HandleSync(Sync(1, 1, 2, 200));
    EXPECT_EQ(fol.synced_log_size(), 0u);
    EXPECT_EQ(CountKind(a1, ReconAction::Kind::kConfirm), 0);

    // log_id 0 arrives but its body is missing -> fetch, still stuck.
    auto a0 = fol.HandleSync(Sync(0, 1, 1, 100));
    EXPECT_EQ(CountKind(a0, ReconAction::Kind::kFetch), 1);
    EXPECT_EQ(fol.synced_log_size(), 0u);

    // Fetch completes for log_id 0 -> both 0 and the buffered 1 drain in order.
    auto b = fol.HandleFetchResp(Write(1, 1, 100));
    ASSERT_EQ(fol.synced_log_size(), 2u);
    EXPECT_EQ(fol.synced_log_at(0).request_id, 1u);
    EXPECT_EQ(fol.synced_log_at(1).request_id, 2u);
    EXPECT_EQ(CountKind(b, ReconAction::Kind::kConfirm), 2);
}

// After a write commits, every replica's synced_log is identical (content+order).
TEST(Reconciliation, AllSyncedLogsIdenticalAfterReconcile)
{
    InMemoryBackend be_l, be_f1, be_f2;
    MockClock clk_l(0);
    PartitionOrderer leader(0, 0, &be_l, &clk_l, PartitionOrderer::Role::kLeader);
    MockClock clk1(0), clk2(0);
    PartitionOrderer f1(0, 0, &be_f1, &clk1, PartitionOrderer::Role::kFollower);
    PartitionOrderer f2(0, 0, &be_f2, &clk2, PartitionOrderer::Role::kFollower);

    // Same requests reach all three, in scrambled submit order.
    const std::vector<LogEntry> reqs = {
        Write(2, 7, 300, "kc", "vc"),
        Write(1, 4, 100, "ka", "va"),
        Write(1, 9, 200, "kb", "vb"),
    };
    for (const auto &r : reqs) {
        leader.Submit(r);
        f1.Submit(r);
        f2.Submit(r);
    }

    clk_l.Set(300);
    leader.ReleaseDue();   // leader builds the authoritative order
    ASSERT_EQ(leader.synced_log_size(), 3u);

    // Drive followers from the leader's synced order.
    for (size_t i = 0; i < leader.synced_log_size(); ++i) {
        LogEntry e = leader.synced_log_at(i);
        f1.HandleSync(Sync(i, e.client_id, e.request_id, e.deadline));
        f2.HandleSync(Sync(i, e.client_id, e.request_id, e.deadline));
    }

    ASSERT_EQ(f1.synced_log_size(), 3u);
    ASSERT_EQ(f2.synced_log_size(), 3u);
    for (size_t i = 0; i < 3; ++i) {
        LogEntry l = leader.synced_log_at(i);
        LogEntry a = f1.synced_log_at(i);
        LogEntry b = f2.synced_log_at(i);
        EXPECT_EQ(a.client_id, l.client_id);
        EXPECT_EQ(a.request_id, l.request_id);
        EXPECT_EQ(a.deadline, l.deadline);
        EXPECT_EQ(b.client_id, l.client_id);
        EXPECT_EQ(b.request_id, l.request_id);
        EXPECT_EQ(b.deadline, l.deadline);
    }
}

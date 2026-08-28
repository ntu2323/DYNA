// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_multi/tests/orderer_commit_test.cc
//   Stage 3 acceptance tests for the ServerOrderer commit / late paths (MockClock):
//   late-leader re-timestamp (immediate release at a bumped t), pure-follower silent
//   lateBuffer, and ApplyCommit adopting agreedT from specLog / earlyBuffer /
//   lateBuffer, plus the fetch→install→apply flow for a commit that races the body.

#include "dyna_multi/server_orderer.h"
#include "dyna_multi/topology.h"
#include "dyna_multi/txn_types.h"
#include "dyna_multi/wall_clock.h"

#include <gtest/gtest.h>

#include <mutex>
#include <string>
#include <vector>

using namespace specpaxos::dyna_multi;
namespace proto = specpaxos::dyna_multi::proto;

namespace {

// A key whose partition is `target` under n partitions (brute-forced; deterministic).
std::string KeyForPartition(uint32_t target, uint32_t n)
{
    for (uint64_t i = 0;; ++i) {
        std::string k = "k" + std::to_string(i);
        if (Topology::PartitionOf(k, n) == target) return k;
    }
}

TxnEntry MakeWrite(uint64_t client, uint64_t req, uint64_t t, const std::string &key)
{
    TxnEntry e;
    e.txn.set_client_id(client);
    e.txn.set_request_id(req);
    e.txn.set_type(proto::WRITE);
    proto::KV *kv = e.txn.add_w_set();
    kv->set_key(key);
    kv->set_value("v");
    e.txn.set_t(t);
    e.txn.set_view_id(0);
    e.t = t;
    return e;
}

struct Collector {
    std::mutex mu;
    std::vector<std::pair<TxnId, uint64_t>> released;   // (id, released t)
    ServerOrderer::OnSpecReply cb() {
        return [this](const TxnEntry &e) {
            std::lock_guard<std::mutex> lk(mu);
            released.emplace_back(e.Key(), e.t);
        };
    }
    size_t count() { std::lock_guard<std::mutex> lk(mu); return released.size(); }
};

} // namespace

// Late arrival at a leader: bump t := clock() and release immediately at the new t.
TEST(OrdererCommit, LateLeaderReTimestampsAndReleases)
{
    MockClock clk(1000);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(1), c.cb());   // Topology(1): server 0 leads all

    o.SubmitTxn(MakeWrite(1, 1, /*t=*/500, "k"), /*poke=*/false);  // 500 < now => late

    ASSERT_EQ(c.count(), 1u);
    EXPECT_EQ(c.released[0].first, MakeTxnId(1, 1));
    EXPECT_EQ(c.released[0].second, 1000u);            // bumped to clock()
    EXPECT_EQ(o.early_size(), 0u);
    EXPECT_EQ(o.late_size(), 0u);
    Status st; uint64_t t = 0;
    ASSERT_TRUE(o.speclog_entry(MakeTxnId(1, 1), &st, &t));
    EXPECT_EQ(st, Status::SPECULATIVE);
    EXPECT_EQ(t, 1000u);
    EXPECT_EQ(o.highest_released_t().t, 1000u);
}

// DECISION B, leader side: a write that is wall-clock on-time (now <= t) but whose
// OrderKey is behind highestReleasedT must NOT release at that stale order — the leader
// re-timestamps strictly above highestReleasedT (via the +1 guard) and releases there.
TEST(OrdererCommit, CounterVetoLeaderReTimestampsAboveHighestReleased)
{
    MockClock clk(100);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(1), c.cb());

    o.SubmitTxn(MakeWrite(5, 5, /*t=*/100, "k"), false);   // on-time
    ASSERT_EQ(o.ReleaseDue(), 1u);
    EXPECT_EQ(o.highest_released_t().t, 100u);

    // B: wall-clock on-time (now=100 <= t=100) but OrderKey (100,1,1) < (100,5,5).
    o.SubmitTxn(MakeWrite(1, 1, /*t=*/100, "k"), false);
    ASSERT_EQ(c.count(), 2u);                              // leader released it (not dropped)
    Status st; uint64_t t = 0;
    ASSERT_TRUE(o.speclog_entry(MakeTxnId(1, 1), &st, &t));
    EXPECT_EQ(st, Status::SPECULATIVE);
    EXPECT_EQ(t, 101u);                                    // bumped strictly above 100
    EXPECT_EQ(o.highest_released_t().t, 101u);
    EXPECT_EQ(c.released[1].second, 101u);                // released at the bumped t
}

// Late arrival at a pure follower: buffered silently, no reply, no specLog entry.
TEST(OrdererCommit, LateFollowerBuffersSilently)
{
    MockClock clk(1000);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(3), c.cb());   // 3 partitions; server 0 follows 1,2
    const std::string key = KeyForPartition(/*target=*/1, 3);  // led by server 1, not 0

    o.SubmitTxn(MakeWrite(1, 1, /*t=*/500, key), false);       // late

    EXPECT_EQ(c.count(), 0u);            // silent
    EXPECT_EQ(o.late_size(), 1u);
    EXPECT_EQ(o.speclog_size(), 0u);
    EXPECT_EQ(o.early_size(), 0u);
}

TEST(OrdererCommit, ApplyCommitAdoptsAgreedTFromSpecLog)
{
    MockClock clk(1000);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(1), c.cb());

    o.SubmitTxn(MakeWrite(1, 1, /*t=*/1500, "k"), false);  // on-time
    clk.Set(1500);
    ASSERT_EQ(o.ReleaseDue(), 1u);                         // SPECULATIVE at t=1500

    TxnEntry out;
    ASSERT_EQ(o.ApplyCommit(MakeTxnId(1, 1), /*agreedT=*/1600, &out),
              ServerOrderer::CommitResult::kApplied);
    EXPECT_EQ(out.t, 1600u);
    EXPECT_EQ(out.status, Status::COMMITTING);
    Status st; uint64_t t = 0;
    ASSERT_TRUE(o.speclog_entry(MakeTxnId(1, 1), &st, &t));
    EXPECT_EQ(st, Status::COMMITTING);
    EXPECT_EQ(t, 1600u);
    EXPECT_EQ(o.highest_released_t().t, 1600u);            // bumped by the adopt
}

TEST(OrdererCommit, ApplyCommitMovesEarlyBufferToSpecLog)
{
    MockClock clk(1000);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(1), c.cb());

    o.SubmitTxn(MakeWrite(1, 1, /*t=*/2000, "k"), false);  // on-time, NOT yet released
    ASSERT_EQ(o.early_size(), 1u);

    TxnEntry out;
    ASSERT_EQ(o.ApplyCommit(MakeTxnId(1, 1), 2100, &out),
              ServerOrderer::CommitResult::kApplied);       // applied before release
    EXPECT_EQ(out.status, Status::COMMITTING);
    EXPECT_EQ(o.early_size(), 0u);
    EXPECT_EQ(o.speclog_size(), 1u);
    Status st; uint64_t t = 0;
    ASSERT_TRUE(o.speclog_entry(MakeTxnId(1, 1), &st, &t));
    EXPECT_EQ(t, 2100u);
}

TEST(OrdererCommit, ApplyCommitMovesLateBufferToSpecLog)
{
    MockClock clk(1000);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(3), c.cb());
    const std::string key = KeyForPartition(1, 3);          // follower for this key

    o.SubmitTxn(MakeWrite(1, 1, /*t=*/500, key), false);    // late => lateBuffer
    ASSERT_EQ(o.late_size(), 1u);

    TxnEntry out;
    ASSERT_EQ(o.ApplyCommit(MakeTxnId(1, 1), /*agreedT=*/2000, &out),
              ServerOrderer::CommitResult::kApplied);
    EXPECT_EQ(out.t, 2000u);
    EXPECT_EQ(out.status, Status::COMMITTING);
    EXPECT_EQ(o.late_size(), 0u);
    EXPECT_EQ(o.speclog_size(), 1u);
}

// Commit races ahead of the body: kNeedFetch → InstallFetched → re-apply.
TEST(OrdererCommit, CommitNeedsFetchThenInstallThenApply)
{
    MockClock clk(1000);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(3), c.cb());
    const std::string key = KeyForPartition(1, 3);

    TxnEntry out;
    EXPECT_EQ(o.ApplyCommit(MakeTxnId(9, 9), 2000, &out),
              ServerOrderer::CommitResult::kNeedFetch);      // don't have the body

    proto::Txn body;
    body.set_client_id(9);
    body.set_request_id(9);
    body.set_type(proto::WRITE);
    proto::KV *kv = body.add_w_set();
    kv->set_key(key);
    kv->set_value("v");
    body.set_t(500);
    body.set_view_id(0);
    o.InstallFetched(body);
    EXPECT_EQ(o.late_size(), 1u);

    ASSERT_EQ(o.ApplyCommit(MakeTxnId(9, 9), 2000, &out),
              ServerOrderer::CommitResult::kApplied);
    EXPECT_EQ(out.t, 2000u);
    EXPECT_EQ(out.status, Status::COMMITTING);
    EXPECT_TRUE(o.speclog_contains(MakeTxnId(9, 9)));

    // Servicing a FetchRequest: the body is now retrievable.
    proto::Txn got;
    ASSERT_TRUE(o.TryGetBody(MakeTxnId(9, 9), &got));
    EXPECT_EQ(got.client_id(), 9u);
    EXPECT_EQ(got.w_set(0).key(), key);
}

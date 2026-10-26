// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/tests/orderer_release_test.cc
//   Stage 2 acceptance tests for ServerOrderer's fast-path release, driven by
//   MockClock (deterministic virtual time, no busy-wait):
//     - a write releases exactly at its deadline, appends to specLog, bumps
//       highestReleasedT, and fires the SpecReply callback;
//     - due entries release in OrderKey order;
//     - the always-on counter-veto (DECISION B) refuses a client txn whose
//       OrderKey <= highestReleasedT even when the wall clock says on-time;
//     - the background release loop releases on a clock Advance and stops cleanly.

#include "dyna/server_orderer.h"
#include "dyna/topology.h"
#include "dyna/txn_types.h"
#include "dyna/wall_clock.h"

#include <gtest/gtest.h>

#include <condition_variable>
#include <mutex>
#include <vector>

using namespace specpaxos::dyna;
namespace proto = specpaxos::dyna::proto;

namespace {

// A key whose partition is `target` under n partitions (brute-forced; deterministic).
std::string KeyForPartition(uint32_t target, uint32_t n)
{
    for (uint64_t i = 0;; ++i) {
        std::string k = "k" + std::to_string(i);
        if (Topology::PartitionOf(k, n) == target) return k;
    }
}

// Build a single-key WRITE txn entry with live t = deadline.
TxnEntry MakeWrite(uint64_t client, uint64_t req, uint64_t t,
                   const std::string &key = "k")
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

// Collects released TxnIds (thread-safe) for assertions.
struct Collector {
    std::mutex mu;
    std::condition_variable cv;
    std::vector<TxnId> ids;

    ServerOrderer::OnSpecReply cb() {
        return [this](const TxnEntry &e) {
            std::lock_guard<std::mutex> lk(mu);
            ids.push_back(e.Key());
            cv.notify_all();
        };
    }
    size_t count() {
        std::lock_guard<std::mutex> lk(mu);
        return ids.size();
    }
    bool wait_for(size_t n, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lk(mu);
        return cv.wait_for(lk, timeout, [&] { return ids.size() >= n; });
    }
};

} // namespace

TEST(ServerOrderer, ReleasesAtDeadline)
{
    MockClock clk(1000);
    Collector c;
    ServerOrderer o(0, /*view=*/0, &clk, Topology(1), c.cb());

    o.SubmitTxn(MakeWrite(1, 1, /*t=*/1500), /*poke=*/false);
    EXPECT_EQ(o.early_size(), 1u);

    // Not yet due: now (1000) < deadline (1500).
    EXPECT_EQ(o.ReleaseDue(), 0u);
    EXPECT_EQ(c.count(), 0u);

    clk.Set(1500);
    EXPECT_EQ(o.ReleaseDue(), 1u);
    EXPECT_EQ(c.count(), 1u);
    EXPECT_EQ(o.early_size(), 0u);
    EXPECT_EQ(o.speclog_size(), 1u);
    EXPECT_TRUE(o.speclog_contains(MakeTxnId(1, 1)));
    EXPECT_EQ(o.highest_released_t().t, 1500u);
}

TEST(ServerOrderer, ReleasesInOrderKeyOrder)
{
    MockClock clk(50);   // all deadlines in the future => all admitted on-time
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(1), c.cb());

    o.SubmitTxn(MakeWrite(1, 1, 300), false);
    o.SubmitTxn(MakeWrite(1, 1 + 1000, 100), false);  // distinct req, deadline 100
    o.SubmitTxn(MakeWrite(2, 1, 200), false);
    EXPECT_EQ(o.early_size(), 3u);

    clk.Set(1000);   // now past all deadlines
    EXPECT_EQ(o.ReleaseDue(), 3u);

    ASSERT_EQ(c.ids.size(), 3u);
    // Released smallest-OrderKey first: t=100, then 200, then 300.
    EXPECT_EQ(c.ids[0], MakeTxnId(1, 1001));
    EXPECT_EQ(c.ids[1], MakeTxnId(2, 1));
    EXPECT_EQ(c.ids[2], MakeTxnId(1, 1));
    EXPECT_EQ(o.highest_released_t().t, 300u);
}

// No counter-veto (design doc: on-time ⇔ arrives before T.t): a txn on-time by the
// wall clock is admitted and released even if its OrderKey is behind highestReleasedT
// (same timestamp, smaller (client,request)); highestReleasedT never decreases.
TEST(ServerOrderer, OnTimeTxnBehindHighestReleasedStillReleases)
{
    MockClock clk(100);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(3), c.cb());   // server 0 follows partitions 1,2
    const std::string followerKey = KeyForPartition(/*target=*/1, 3);

    // Release A at t=100 with a LARGE tie-break id => highestReleasedT=(100,5,5).
    o.SubmitTxn(MakeWrite(5, 5, 100, followerKey), false);   // on-time (leadership N/A)
    EXPECT_EQ(o.ReleaseDue(), 1u);
    EXPECT_EQ(o.highest_released_t(), (OrderKey{100, 5, 5}));

    // B: same t=100 (now<=t, wall-clock on-time) but smaller (client,request), so
    // OrderKey(B)=(100,1,1) < highestReleasedT. Still on-time → admitted and released.
    o.SubmitTxn(MakeWrite(1, 1, 100, followerKey), false);
    EXPECT_EQ(o.early_size(), 1u);
    EXPECT_EQ(o.ReleaseDue(), 1u);
    EXPECT_EQ(c.count(), 2u);
    EXPECT_EQ(o.late_size(), 0u);
    EXPECT_TRUE(o.speclog_contains(MakeTxnId(1, 1)));
    EXPECT_EQ(o.highest_released_t(), (OrderKey{100, 5, 5}));   // not lowered
}

TEST(ServerOrderer, BackgroundLoopReleasesAndStops)
{
    MockClock clk(1000);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(1), c.cb());

    o.Start();
    o.SubmitTxn(MakeWrite(7, 7, 2000));   // poke=true wakes the loop
    // Still in the future: no release yet.
    EXPECT_FALSE(c.wait_for(1, std::chrono::milliseconds(100)));

    clk.Set(2000);                        // advancing wakes SleepUntil
    EXPECT_TRUE(c.wait_for(1, std::chrono::milliseconds(1000)));
    EXPECT_EQ(c.count(), 1u);

    o.Stop();   // must not hang
    EXPECT_EQ(o.speclog_size(), 1u);
}

// External release driver (the replica's ordering thread): Start(false) starts no release
// thread; next_due_hint() tracks the earliest on-time deadline so the driver can poll
// without the lock, and ReleaseDue() releases + refreshes it.
TEST(OrdererRelease, ExternalDriverHintTracksEarliestDeadline)
{
    MockClock clk(50);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(1), c.cb());
    o.Start(/*own_release_thread=*/false);

    EXPECT_EQ(o.next_due_hint(), Clock::kNever);
    o.SubmitTxn(MakeWrite(1, 1, 300), /*poke=*/false);
    EXPECT_EQ(o.next_due_hint(), 300u);
    o.SubmitTxn(MakeWrite(1, 2, 200), /*poke=*/false);   // earlier deadline lowers the hint
    EXPECT_EQ(o.next_due_hint(), 200u);

    // No release thread: advancing the clock alone releases nothing.
    clk.Set(250);
    EXPECT_FALSE(c.wait_for(1, std::chrono::milliseconds(50)));

    EXPECT_EQ(o.ReleaseDue(), 1u);                        // the driver releases t=200
    EXPECT_EQ(c.count(), 1u);
    EXPECT_EQ(o.next_due_hint(), 300u);                   // refreshed to the next one
    clk.Set(300);
    EXPECT_EQ(o.ReleaseDue(), 1u);
    EXPECT_EQ(o.next_due_hint(), Clock::kNever);
    o.Stop();
}

// Lanes: the late-leader re-timestamp floor is server-wide. A commit in one lane pushes
// highestReleasedT to 5000; a late txn this server leads, arriving in ANOTHER lane, is
// re-timestamped above it (not just above its own lane's releases).
TEST(OrdererLanes, LateLeaderRetimestampAboveOtherLanes)
{
    MockClock clk(100);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(1), c.cb(), nullptr, /*lanes=*/4);
    const TxnId a = MakeTxnId(1, 1);
    uint64_t rb = 1;
    while (o.LaneOf(MakeTxnId(2, rb)) == o.LaneOf(a)) ++rb;
    const TxnId b = MakeTxnId(2, rb);

    o.SubmitTxn(MakeWrite(1, 1, 200), false);
    clk.Set(200);
    o.ReleaseDue();
    TxnEntry out;
    ASSERT_EQ(o.ApplyCommit(a, 5000, &out), ServerOrderer::CommitResult::kApplied);
    EXPECT_EQ(o.highest_released_t().t, 5000u);

    clk.Set(1000);
    o.SubmitTxn(MakeWrite(2, rb, /*t=*/300), false);   // late; server 0 leads every key
    Status st;
    uint64_t t = 0;
    ASSERT_TRUE(o.speclog_entry(b, &st, &t));
    EXPECT_EQ(st, Status::SPECULATIVE);
    EXPECT_EQ(t, 5001u);
}

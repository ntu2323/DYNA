// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_multi/tests/orderer_release_test.cc
//   Stage 2 acceptance tests for ServerOrderer's fast-path release, driven by
//   MockClock (deterministic virtual time, no busy-wait):
//     - a write releases exactly at its deadline, appends to specLog, bumps
//       highestReleasedT, and fires the SpecReply callback;
//     - due entries release in OrderKey order;
//     - the always-on counter-veto (DECISION B) refuses a client txn whose
//       OrderKey <= highestReleasedT even when the wall clock says on-time;
//     - the background release loop releases on a clock Advance and stops cleanly.

#include "dyna_multi/server_orderer.h"
#include "dyna_multi/topology.h"
#include "dyna_multi/txn_types.h"
#include "dyna_multi/wall_clock.h"

#include <gtest/gtest.h>

#include <condition_variable>
#include <mutex>
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

// DECISION B, follower side: on-time by the wall clock (now <= t) but
// OrderKey <= highestReleasedT (same timestamp, smaller (client,request)) => the
// counter-veto refuses to admit/endorse it; a pure follower buffers it silently.
TEST(ServerOrderer, CounterVetoSilentlyBuffersFollowerTxnBehindHighestReleased)
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
    // OrderKey(B)=(100,1,1) < highestReleasedT. Server 0 is a FOLLOWER for this key =>
    // silent lateBuffer, never released.
    o.SubmitTxn(MakeWrite(1, 1, 100, followerKey), false);
    EXPECT_EQ(o.early_size(), 0u);          // not admitted to earlyBuffer
    EXPECT_EQ(o.ReleaseDue(), 0u);          // never released
    EXPECT_EQ(c.count(), 1u);               // only A ever released
    EXPECT_EQ(o.late_size(), 1u);           // B sits silently in lateBuffer
    EXPECT_FALSE(o.speclog_contains(MakeTxnId(1, 1)));
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

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/tests/read_test.cc
//   Stage 7 acceptance tests for the read path (spec § read path):
//   a leader read returns the committed value after its conflicting write executes;
//   a read is held (not replied) until that write executes; a late read at a pure
//   follower is dropped silently; an on-time follower read endorses immediately
//   (no results, not logged); an on-time leader read is held until the executor
//   services it.

#include "dyna/in_memory_backend.h"
#include "dyna/server_orderer.h"
#include "dyna/topology.h"
#include "dyna/txn_types.h"
#include "dyna/wall_clock.h"

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

using namespace specpaxos::dyna;
namespace proto = specpaxos::dyna::proto;

namespace {

std::string KeyForPartition(uint32_t target, uint32_t n)
{
    for (uint64_t i = 0;; ++i) {
        std::string k = "k" + std::to_string(i);
        if (Topology::PartitionOf(k, n) == target) return k;
    }
}

TxnEntry MakeWrite(uint64_t c, uint64_t r, uint64_t t, const std::string &k, const std::string &v)
{
    TxnEntry e;
    e.txn.set_client_id(c); e.txn.set_request_id(r); e.txn.set_type(proto::WRITE);
    proto::KV *kv = e.txn.add_w_set(); kv->set_key(k); kv->set_value(v);
    e.txn.set_t(t); e.txn.set_view_id(0); e.t = t;
    return e;
}

TxnEntry MakeRead(uint64_t c, uint64_t r, uint64_t t, const std::string &k)
{
    TxnEntry e;
    e.txn.set_client_id(c); e.txn.set_request_id(r); e.txn.set_type(proto::READ);
    e.txn.add_r_set(k);
    e.txn.set_t(t); e.txn.set_view_id(0); e.t = t;
    return e;
}

struct Reply {
    TxnId id;
    bool is_leader;
    std::map<std::string, std::string> results;
};

struct Collector {
    std::vector<Reply> replies;
    ServerOrderer::OnSpecReply spec_cb() {
        return [this](const TxnEntry &e) {
            Reply r; r.id = e.Key(); r.is_leader = e.is_leader_for_this;
            for (const auto &kv : e.results) r.results[kv.first] = kv.second;
            replies.push_back(std::move(r));
        };
    }
};

} // namespace

TEST(Read, LeaderReadReturnsCommittedValueAfterWriteExecutes)
{
    InMemoryBackend backend;
    MockClock clk(50);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(1), c.spec_cb(), &backend);

    // Write K=v1 and execute it.
    o.SubmitTxn(MakeWrite(1, 1, 100, "K", "v1"), false);
    clk.Set(100);
    o.ReleaseDue();
    o.MarkCommitted(MakeTxnId(1, 1));
    o.ExecuteReady();
    ASSERT_EQ(backend.value_or_empty("K"), "v1");
    c.replies.clear();   // drop the write's own reply

    // Read K: released (leader read) but NOT replied until the executor services it.
    o.SubmitTxn(MakeRead(1, 2, 200, "K"), false);
    clk.Set(200);
    o.ReleaseDue();
    EXPECT_TRUE(c.replies.empty());        // held for the executor

    o.ExecuteReady();
    ASSERT_EQ(c.replies.size(), 1u);
    EXPECT_EQ(c.replies[0].id, MakeTxnId(1, 2));
    EXPECT_TRUE(c.replies[0].is_leader);
    EXPECT_EQ(c.replies[0].results["K"], "v1");
    EXPECT_FALSE(o.speclog_contains(MakeTxnId(1, 2)));   // read GC'd after servicing
}

TEST(Read, ReadBlockedUntilConflictingWriteCommitsAndExecutes)
{
    InMemoryBackend backend;
    MockClock clk(50);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(1), c.spec_cb(), &backend);

    // Smaller write W(K) released but NOT committed; larger read R(K).
    o.SubmitTxn(MakeWrite(1, 1, 100, "K", "v"), false);
    o.SubmitTxn(MakeRead(1, 2, 200, "K"), false);
    clk.Set(200);
    o.ReleaseDue();
    c.replies.clear();   // drop the write endorsement reply

    // R is blocked by the un-committed conflicting smaller write.
    o.ExecuteReady();
    EXPECT_TRUE(c.replies.empty());

    // Commit the write → it executes, then the read is serviced with the value.
    o.MarkCommitted(MakeTxnId(1, 1));
    o.ExecuteReady();
    ASSERT_EQ(c.replies.size(), 1u);
    EXPECT_EQ(c.replies[0].results["K"], "v");
}

TEST(Read, LateFollowerReadDroppedSilently)
{
    InMemoryBackend backend;
    MockClock clk(1000);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(3), c.spec_cb(), &backend);
    const std::string key = KeyForPartition(1, 3);   // led by server 1; server 0 follows

    o.SubmitTxn(MakeRead(1, 1, /*t=*/500, key), false);   // late (now=1000 > 500)

    EXPECT_TRUE(c.replies.empty());      // no reply of any kind: client retries on timeout
    EXPECT_EQ(o.late_size(), 0u);        // not buffered
    EXPECT_EQ(o.speclog_size(), 0u);
    EXPECT_EQ(o.early_size(), 0u);
}

TEST(Read, OnTimeFollowerReadEndorsesWithoutLogging)
{
    InMemoryBackend backend;
    MockClock clk(50);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(3), c.spec_cb(), &backend);
    const std::string key = KeyForPartition(1, 3);   // server 0 is a follower

    o.SubmitTxn(MakeRead(1, 1, 100, key), false);
    clk.Set(100);
    o.ReleaseDue();

    ASSERT_EQ(c.replies.size(), 1u);
    EXPECT_FALSE(c.replies[0].is_leader);       // endorsement, no results
    EXPECT_TRUE(c.replies[0].results.empty());
    EXPECT_EQ(o.speclog_size(), 0u);            // follower reads aren't logged
}

TEST(Read, OnTimeLeaderReadHeldUntilExecuted)
{
    InMemoryBackend backend;
    MockClock clk(50);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(1), c.spec_cb(), &backend);

    o.SubmitTxn(MakeRead(1, 1, 100, "K"), false);
    clk.Set(100);
    o.ReleaseDue();
    EXPECT_TRUE(c.replies.empty());             // leader read held, not replied at release
    EXPECT_TRUE(o.speclog_contains(MakeTxnId(1, 1)));

    o.ExecuteReady();
    ASSERT_EQ(c.replies.size(), 1u);
    EXPECT_TRUE(c.replies[0].is_leader);        // empty value: K never written
    EXPECT_EQ(c.replies[0].results["K"], "");
}

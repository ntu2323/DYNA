// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/tests/exec_test.cc
//   Stage 6 acceptance tests for dependency-gated batched disk execution (spec §3.7):
//   a COMMITTED write executes and is applied + GC'd; a w-w conflict executes in
//   OrderKey order (larger blocked until the smaller commits); a non-conflicting txn
//   executes even while an unrelated smaller txn is un-committed; and an applied write
//   is readable immediately and stored with its commit timestamp.

#include "dyna/in_memory_backend.h"
#include "dyna/server_orderer.h"
#include "dyna/topology.h"
#include "dyna/txn_types.h"
#include "dyna/wall_clock.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace specpaxos::dyna;
namespace proto = specpaxos::dyna::proto;

namespace {

TxnEntry MakeWrite(uint64_t client, uint64_t req, uint64_t t,
                   const std::string &key, const std::string &value)
{
    TxnEntry e;
    e.txn.set_client_id(client);
    e.txn.set_request_id(req);
    e.txn.set_type(proto::WRITE);
    proto::KV *kv = e.txn.add_w_set();
    kv->set_key(key);
    kv->set_value(value);
    e.txn.set_t(t);
    e.txn.set_view_id(0);
    e.t = t;
    return e;
}

ServerOrderer::OnSpecReply Noop() { return [](const TxnEntry &) {}; }

// Release an on-time write and force it to COMMITTED (skips the commit/replication
// handshake — this test targets ExecuteReady only).
void ReleaseAndCommit(ServerOrderer &o, MockClock &clk, TxnEntry entry, uint64_t at)
{
    o.SubmitTxn(std::move(entry), false);
    clk.Set(at);
    o.ReleaseDue();
}

} // namespace

TEST(Exec, CommittedWriteExecutesAndApplies)
{
    InMemoryBackend backend;
    MockClock clk(50);
    ServerOrderer o(0, 0, &clk, Topology(1), Noop(), &backend);
    const TxnId id = MakeTxnId(1, 1);

    ReleaseAndCommit(o, clk, MakeWrite(1, 1, 100, "k", "v1"), 100);
    o.MarkCommitted(id);   // simulate replication reaching COMMITTED

    std::vector<TxnId> done = o.ExecuteReady();
    ASSERT_EQ(done.size(), 1u);
    EXPECT_EQ(done[0], id);
    EXPECT_EQ(backend.value_or_empty("k"), "v1");
    EXPECT_EQ(o.speclog_size(), 0u);            // GC'd from memory
    EXPECT_FALSE(o.speclog_contains(id));
}

TEST(Exec, WWConflictExecutesInOrderKeyOrder)
{
    InMemoryBackend backend;
    MockClock clk(50);
    ServerOrderer o(0, 0, &clk, Topology(1), Noop(), &backend);
    const TxnId t1 = MakeTxnId(1, 1), t2 = MakeTxnId(1, 2);

    ReleaseAndCommit(o, clk, MakeWrite(1, 1, 100, "K", "a"), 100);   // smaller OrderKey
    ReleaseAndCommit(o, clk, MakeWrite(1, 2, 200, "K", "b"), 200);   // larger, same key

    // Only the larger is COMMITTED → blocked by the un-committed smaller conflicting one.
    o.MarkCommitted(t2);
    EXPECT_TRUE(o.ExecuteReady().empty());
    EXPECT_FALSE(backend.read("K").has_value());

    // Commit the smaller → both execute in OrderKey order; final value is the larger's.
    o.MarkCommitted(t1);
    std::vector<TxnId> done = o.ExecuteReady();
    ASSERT_EQ(done.size(), 2u);
    EXPECT_EQ(done[0], t1);
    EXPECT_EQ(done[1], t2);
    EXPECT_EQ(backend.value_or_empty("K"), "b");
    EXPECT_EQ(o.speclog_size(), 0u);
}

TEST(Exec, NonConflictingExecutesDespiteUncommittedSmaller)
{
    InMemoryBackend backend;
    MockClock clk(50);
    ServerOrderer o(0, 0, &clk, Topology(1), Noop(), &backend);
    const TxnId small = MakeTxnId(1, 1), big = MakeTxnId(1, 2);

    ReleaseAndCommit(o, clk, MakeWrite(1, 1, 100, "A", "x"), 100);   // stays un-committed
    ReleaseAndCommit(o, clk, MakeWrite(1, 2, 200, "B", "y"), 200);   // different key
    o.MarkCommitted(big);   // only the larger, non-conflicting one is committed

    std::vector<TxnId> done = o.ExecuteReady();
    ASSERT_EQ(done.size(), 1u);
    EXPECT_EQ(done[0], big);
    EXPECT_EQ(backend.value_or_empty("B"), "y");
    EXPECT_FALSE(backend.read("A").has_value());   // smaller not executed
    EXPECT_TRUE(o.speclog_contains(small));            // still buffered
    EXPECT_FALSE(o.speclog_contains(big));             // executed + GC'd
}

TEST(Exec, AppliedWithCommitTimestamp)
{
    InMemoryBackend backend;
    MockClock clk(50);
    ServerOrderer o(0, 0, &clk, Topology(1), Noop(), &backend);
    const TxnId id = MakeTxnId(1, 1);

    ReleaseAndCommit(o, clk, MakeWrite(1, 1, 100, "k", "v"), 100);
    o.MarkCommitted(id);
    ASSERT_EQ(o.ExecuteReady().size(), 1u);

    // Applied once, readable immediately, stored with its (agreed) commit timestamp.
    auto v = backend.read("k");
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(v->value, "v");
    EXPECT_EQ(v->t, 100u);
    EXPECT_EQ(backend.apply_count(), 1u);
}

// ExecuteReady skips the scan when nothing is executable and stops it after the last
// executable entry; the dependency rule must be unchanged by either shortcut.
TEST(Exec, ShortcutsPreserveDependencyRule)
{
    InMemoryBackend backend;
    MockClock clk(50);
    ServerOrderer o(0, 0, &clk, Topology(1), Noop(), &backend);
    const TxnId a = MakeTxnId(1, 1), b = MakeTxnId(1, 2), c = MakeTxnId(1, 3),
                d = MakeTxnId(1, 4);

    // a (K, smallest) stays SPECULATIVE; b (K) and c (J) commit; d (K, largest) stays
    // SPECULATIVE.
    ReleaseAndCommit(o, clk, MakeWrite(1, 1, 100, "K", "a"), 100);
    ReleaseAndCommit(o, clk, MakeWrite(1, 2, 110, "K", "b"), 110);
    ReleaseAndCommit(o, clk, MakeWrite(1, 3, 120, "J", "c"), 120);
    ReleaseAndCommit(o, clk, MakeWrite(1, 4, 130, "K", "d"), 130);

    // Nothing committed yet: an empty pass that leaves everything in place.
    EXPECT_TRUE(o.ExecuteReady().empty());
    EXPECT_EQ(o.speclog_size(), 4u);

    o.MarkCommitted(b);
    o.MarkCommitted(c);
    // b is blocked behind the un-committed smaller a (same key); c is independent.
    std::vector<TxnId> done = o.ExecuteReady();
    ASSERT_EQ(done.size(), 1u);
    EXPECT_EQ(done[0], c);
    EXPECT_EQ(backend.value_or_empty("J"), "c");
    EXPECT_EQ(backend.value_or_empty("K"), "");

    // a commits: a then b run in OrderKey order; d (un-committed) stays.
    o.MarkCommitted(a);
    done = o.ExecuteReady();
    ASSERT_EQ(done.size(), 2u);
    EXPECT_EQ(done[0], a);
    EXPECT_EQ(done[1], b);
    EXPECT_EQ(backend.value_or_empty("K"), "b");
    EXPECT_TRUE(o.speclog_contains(d));
    EXPECT_EQ(o.speclog_size(), 1u);
}

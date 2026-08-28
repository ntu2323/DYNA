// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_multi/tests/slowpath_test.cc
//   Slow-path correctness (spec § write path, DECISION A): a multi-partition write whose
//   involved leaders assign DIFFERENT timestamps (one on-time, one late-bumped) must not
//   fast-commit. The client resolves to SlowCommit(max), the servers adopt the agreed max
//   (COMMITTING), and the CommitReply quorum re-forms at that max — the whole slow-path
//   loop tied together with real ServerOrderers + the real TxnQuorum. Also: a follower
//   that silently buffered the late write adopts the agreed max on SlowCommit.

#include "dyna_multi/commit_quorum.h"
#include "dyna_multi/server_orderer.h"
#include "dyna_multi/topology.h"
#include "dyna_multi/txn_types.h"
#include "dyna_multi/wall_clock.h"

#include <gtest/gtest.h>

#include <string>

using namespace specpaxos::dyna_multi;
namespace proto = specpaxos::dyna_multi::proto;

namespace {

std::string KeyForPartition(uint32_t target, uint32_t n)
{
    for (uint64_t i = 0;; ++i) {
        std::string k = "k" + std::to_string(i);
        if (Topology::PartitionOf(k, n) == target) return k;
    }
}

TxnEntry MakeWrite(uint64_t c, uint64_t r, uint64_t t,
                   const std::string &k1, const std::string &k2 = "")
{
    TxnEntry e;
    e.txn.set_client_id(c); e.txn.set_request_id(r); e.txn.set_type(proto::WRITE);
    { proto::KV *kv = e.txn.add_w_set(); kv->set_key(k1); kv->set_value("v"); }
    if (!k2.empty()) { proto::KV *kv = e.txn.add_w_set(); kv->set_key(k2); kv->set_value("v"); }
    e.txn.set_t(t); e.txn.set_view_id(0); e.t = t;
    return e;
}

ServerOrderer::OnSpecReply Noop() { return [](const TxnEntry &) {}; }
Status StatusOf(ServerOrderer &o, const TxnId &id) {
    Status s = Status::SPECULATIVE; uint64_t t = 0;
    EXPECT_TRUE(o.speclog_entry(id, &s, &t));
    return s;
}
uint64_t TsOf(ServerOrderer &o, const TxnId &id) {
    Status s = Status::SPECULATIVE; uint64_t t = 0;
    EXPECT_TRUE(o.speclog_entry(id, &s, &t));
    return t;
}

} // namespace

// The end-to-end slow path: leaders disagree on t → SlowCommit(max) → both adopt →
// CommitReply quorum re-forms at the max.
TEST(SlowPath, LeaderDisagreementResolvesToMaxAndCommits)
{
    const std::string kA = KeyForPartition(0, 3);   // led by server 0
    const std::string kB = KeyForPartition(1, 3);   // led by server 1
    const TxnId id = MakeTxnId(1, 1);

    // Leader 0: on-time → releases at t=100.
    MockClock c0(50);
    ServerOrderer o0(0, 0, &c0, Topology(3), Noop());
    o0.SubmitTxn(MakeWrite(1, 1, 100, kA, kB), false);
    c0.Set(100);
    ASSERT_EQ(o0.ReleaseDue(), 1u);
    // Leader 1: arrives late (now=200 > t=100) → re-timestamps to 200.
    MockClock c1(200);
    ServerOrderer o1(1, 0, &c1, Topology(3), Noop());
    o1.SubmitTxn(MakeWrite(1, 1, 100, kA, kB), false);
    ASSERT_EQ(TsOf(o0, id), 100u);
    ASSERT_EQ(TsOf(o1, id), 200u);

    // Client collects the two leader SpecReplies → NOT fast; leaders disagree; max=200.
    TxnQuorum q({0, 1}, /*quorum=*/2);
    q.AddReply(0, /*view=*/0, /*t=*/100, /*is_leader=*/true);
    q.AddReply(1, 0, 200, true);
    EXPECT_FALSE(q.fast_ready());
    EXPECT_TRUE(q.leaders_disagree());
    EXPECT_EQ(q.slow_agreed_t(), 200u);

    // Client sends SlowCommit(200); each server adopts it → COMMITTING at 200.
    TxnEntry out0, out1;
    ASSERT_EQ(o0.ApplyCommit(id, 200, &out0), ServerOrderer::CommitResult::kApplied);
    ASSERT_EQ(o1.ApplyCommit(id, 200, &out1), ServerOrderer::CommitResult::kApplied);
    EXPECT_EQ(StatusOf(o0, id), Status::COMMITTING);
    EXPECT_EQ(StatusOf(o1, id), Status::COMMITTING);
    EXPECT_EQ(TsOf(o0, id), 200u);
    EXPECT_EQ(TsOf(o1, id), 200u);

    // Servers send CommitReply(200); the quorum re-forms at the agreed max.
    q.AddReply(0, 0, 200, true);
    q.AddReply(1, 0, 200, true);
    EXPECT_TRUE(q.fast_ready());
    EXPECT_EQ(q.agreed_t(), 200u);   // client commits at 200
}

// A pure follower that silently buffered the late write adopts the agreed max on
// SlowCommit, moving it into specLog COMMITTING so its CommitReply counts.
TEST(SlowPath, FollowerAdoptsAgreedTFromLateBuffer)
{
    const std::string kA = KeyForPartition(0, 3);   // led by server 0; server 2 follows
    const TxnId id = MakeTxnId(1, 1);
    MockClock c2(1000);
    ServerOrderer o2(2, 0, &c2, Topology(3), Noop());

    o2.SubmitTxn(MakeWrite(1, 1, /*t=*/500, kA), false);   // late at follower → lateBuffer
    ASSERT_EQ(o2.late_size(), 1u);
    ASSERT_EQ(o2.speclog_size(), 0u);

    TxnEntry out;
    ASSERT_EQ(o2.ApplyCommit(id, /*agreedT=*/700, &out), ServerOrderer::CommitResult::kApplied);
    EXPECT_EQ(o2.late_size(), 0u);
    EXPECT_EQ(StatusOf(o2, id), Status::COMMITTING);
    EXPECT_EQ(TsOf(o2, id), 700u);
}

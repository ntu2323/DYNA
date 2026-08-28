// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_multi/tests/tsagree_test.cc
//   Stage 4 acceptance tests for background timestamp agreement (spec §3.5,
//   SPECULATIVE → COMMITTING):
//     - two involved leaders that released a txn at different t converge to the max
//       via a TsAgree exchange, with no client commit message;
//     - a single-involved-leader txn transitions to COMMITTING immediately (no msg);
//     - a freshly-released txn is not "stuck" until it ages past the timeout.

#include "dyna_multi/server_orderer.h"
#include "dyna_multi/topology.h"
#include "dyna_multi/txn_types.h"
#include "dyna_multi/wall_clock.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

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

// WRITE txn touching one or two keys (→ one or two involved partitions/leaders).
TxnEntry MakeWrite(uint64_t client, uint64_t req, uint64_t t,
                   const std::string &k1, const std::string &k2 = "")
{
    TxnEntry e;
    e.txn.set_client_id(client);
    e.txn.set_request_id(req);
    e.txn.set_type(proto::WRITE);
    { proto::KV *kv = e.txn.add_w_set(); kv->set_key(k1); kv->set_value("v"); }
    if (!k2.empty()) { proto::KV *kv = e.txn.add_w_set(); kv->set_key(k2); kv->set_value("v"); }
    e.txn.set_t(t);
    e.txn.set_view_id(0);
    e.t = t;
    return e;
}

ServerOrderer::OnSpecReply NoopReply() { return [](const TxnEntry &) {}; }

} // namespace

// Two involved leaders (0 and 1) that released the txn at different t (0 at 100, 1
// late-bumped to 200) converge to max=200, COMMITTING, purely via TsAgree.
TEST(TsAgree, TwoLeadersConvergeToMax)
{
    const std::string kA = KeyForPartition(0, 3);   // led by server 0
    const std::string kB = KeyForPartition(1, 3);   // led by server 1
    const TxnId id = MakeTxnId(1, 1);

    MockClock c0(50), c1(200);
    ServerOrderer o0(0, 0, &c0, Topology(3), NoopReply());
    ServerOrderer o1(1, 0, &c1, Topology(3), NoopReply());

    // Server 0: on-time → release at t=100.
    o0.SubmitTxn(MakeWrite(1, 1, 100, kA, kB), false);
    c0.Set(100);
    ASSERT_EQ(o0.ReleaseDue(), 1u);
    // Server 1: arrives late (now=200 > t=100) → late-leader re-timestamp to 200.
    o1.SubmitTxn(MakeWrite(1, 1, 100, kA, kB), false);

    {   // sanity: both SPECULATIVE, different t
        Status s; uint64_t t = 0;
        ASSERT_TRUE(o0.speclog_entry(id, &s, &t)); EXPECT_EQ(s, Status::SPECULATIVE); EXPECT_EQ(t, 100u);
        ASSERT_TRUE(o1.speclog_entry(id, &s, &t)); EXPECT_EQ(s, Status::SPECULATIVE); EXPECT_EQ(t, 200u);
    }

    // Each leader scans (timeout 0 => immediately stuck) and emits a TsAgree action.
    std::vector<ServerOrderer::TsAgreeAction> a0, a1;
    std::vector<TxnEntry> c0done, c1done;
    o0.DriveTsAgreement(0, &a0, &c0done);
    o1.DriveTsAgreement(0, &a1, &c1done);
    ASSERT_EQ(a0.size(), 1u); EXPECT_EQ(a0[0].proposed_t, 100u);
    ASSERT_EQ(a0[0].to_leaders.count(1), 1u);
    ASSERT_EQ(a1.size(), 1u); EXPECT_EQ(a1[0].proposed_t, 200u);
    ASSERT_EQ(a1[0].to_leaders.count(0), 1u);
    EXPECT_TRUE(c0done.empty());   // not complete yet
    EXPECT_TRUE(c1done.empty());

    // Deliver each leader's proposal to the other.
    auto r1 = o1.RecordTsProposal(id, /*from=*/0, a0[0].proposed_t);   // 1 learns 0's t=100
    EXPECT_TRUE(r1.transitioned);
    auto r0 = o0.RecordTsProposal(id, /*from=*/1, a1[0].proposed_t);   // 0 learns 1's t=200
    EXPECT_TRUE(r0.transitioned);

    // Both converge: COMMITTING at max(100,200)=200.
    Status s; uint64_t t = 0;
    ASSERT_TRUE(o0.speclog_entry(id, &s, &t)); EXPECT_EQ(s, Status::COMMITTING); EXPECT_EQ(t, 200u);
    ASSERT_TRUE(o1.speclog_entry(id, &s, &t)); EXPECT_EQ(s, Status::COMMITTING); EXPECT_EQ(t, 200u);
    EXPECT_EQ(o0.highest_released_t().t, 200u);
}

// Single involved leader: transitions to COMMITTING immediately, emits no TsAgree.
TEST(TsAgree, SingleLeaderCommitsImmediately)
{
    MockClock clk(50);
    ServerOrderer o(0, 0, &clk, Topology(1), NoopReply());   // server 0 leads everything

    o.SubmitTxn(MakeWrite(1, 1, 100, "k"), false);
    clk.Set(100);
    ASSERT_EQ(o.ReleaseDue(), 1u);

    std::vector<ServerOrderer::TsAgreeAction> actions;
    std::vector<TxnEntry> committed;
    o.DriveTsAgreement(0, &actions, &committed);

    EXPECT_TRUE(actions.empty());          // nobody else to agree with
    ASSERT_EQ(committed.size(), 1u);
    Status s; uint64_t t = 0;
    ASSERT_TRUE(o.speclog_entry(MakeTxnId(1, 1), &s, &t));
    EXPECT_EQ(s, Status::COMMITTING);
    EXPECT_EQ(t, 100u);                    // unchanged (max over {own})
}

// Not stuck until it ages past the timeout.
TEST(TsAgree, NotStuckBeforeTimeout)
{
    MockClock clk(1000);
    ServerOrderer o(0, 0, &clk, Topology(1), NoopReply());
    o.SubmitTxn(MakeWrite(1, 1, 1000, "k"), false);
    ASSERT_EQ(o.ReleaseDue(), 1u);         // released_us = 1000

    std::vector<ServerOrderer::TsAgreeAction> actions;
    std::vector<TxnEntry> committed;
    o.DriveTsAgreement(/*timeout_us=*/1000000, &actions, &committed);   // 1s timeout
    EXPECT_TRUE(actions.empty());
    EXPECT_TRUE(committed.empty());
    Status s; uint64_t t = 0;
    ASSERT_TRUE(o.speclog_entry(MakeTxnId(1, 1), &s, nullptr));
    EXPECT_EQ(s, Status::SPECULATIVE);     // still speculative

    clk.Set(1000 + 1000000);               // now aged past the timeout
    o.DriveTsAgreement(1000000, &actions, &committed);
    ASSERT_EQ(committed.size(), 1u);       // now transitions
    ASSERT_TRUE(o.speclog_entry(MakeTxnId(1, 1), &s, nullptr));
    EXPECT_EQ(s, Status::COMMITTING);
}

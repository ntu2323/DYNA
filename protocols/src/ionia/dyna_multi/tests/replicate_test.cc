// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_multi/tests/replicate_test.cc
//   Stage 5 acceptance tests for replication (spec §3.6-3.7, COMMITTING → COMMITTED):
//   the primary leader drives ReplicatePrepare, a follower materializes the txn from
//   the body and acks, the leader COMMITs after f acks and broadcasts ReplicateCommit,
//   and followers mark COMMITTED. Also: only the min-id involved leader drives, and a
//   ReplicatePrepare adopts a txn the server already holds late-buffered.

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

ServerOrderer::OnSpecReply Noop() { return [](const TxnEntry &) {}; }
Status StatusOf(ServerOrderer &o, const TxnId &id) {
    Status s = Status::SPECULATIVE;
    EXPECT_TRUE(o.speclog_entry(id, &s, nullptr));
    return s;
}

} // namespace

TEST(Replicate, PrimaryLeaderDrivesCommittedAcrossFollower)
{
    const std::string kA = KeyForPartition(0, 3);   // led by server 0 (primary)
    const TxnId id = MakeTxnId(1, 1);

    MockClock c0(50), c1(50);
    ServerOrderer o0(0, 0, &c0, Topology(3), Noop());   // leader
    ServerOrderer o1(1, 0, &c1, Topology(3), Noop());   // follower

    // Server 0: release then commit (via a FastCommit-style ApplyCommit) → COMMITTING.
    o0.SubmitTxn(MakeWrite(1, 1, 100, kA), false);
    c0.Set(100);
    ASSERT_EQ(o0.ReleaseDue(), 1u);
    TxnEntry out;
    ASSERT_EQ(o0.ApplyCommit(id, 100, &out), ServerOrderer::CommitResult::kApplied);
    ASSERT_EQ(StatusOf(o0, id), Status::COMMITTING);

    // Primary drives ReplicatePrepare.
    std::vector<ServerOrderer::ReplicateAction> prep;
    o0.DriveReplication(&prep);
    ASSERT_EQ(prep.size(), 1u);
    EXPECT_EQ(prep[0].id, id);
    EXPECT_EQ(prep[0].agreed_t, 100u);

    // Follower materializes from the body and acks.
    ASSERT_TRUE(o1.HandleReplicatePrepare(id, prep[0].agreed_t, prep[0].body));
    ASSERT_EQ(StatusOf(o1, id), Status::COMMITTING);

    // Leader: a self-ack doesn't count; the follower's ack reaches f=1 → COMMITTED.
    EXPECT_FALSE(o0.RecordPrepareReply(id, /*from=*/0));
    EXPECT_EQ(StatusOf(o0, id), Status::COMMITTING);
    EXPECT_TRUE(o0.RecordPrepareReply(id, /*from=*/1));
    EXPECT_EQ(StatusOf(o0, id), Status::COMMITTED);

    // ReplicateCommit → follower marks COMMITTED.
    o1.MarkCommitted(id);
    EXPECT_EQ(StatusOf(o1, id), Status::COMMITTED);
}

// Only the min-id involved leader drives replication (no redundant fan-out).
TEST(Replicate, OnlyPrimaryLeaderDrives)
{
    const std::string kA = KeyForPartition(0, 3);   // partition 0
    const std::string kB = KeyForPartition(1, 3);   // partition 1
    const TxnId id = MakeTxnId(1, 1);

    MockClock c0(50), c1(50);
    ServerOrderer o0(0, 0, &c0, Topology(3), Noop());   // involved leader 0 (primary)
    ServerOrderer o1(1, 0, &c1, Topology(3), Noop());   // involved leader 1 (not primary)

    o0.SubmitTxn(MakeWrite(1, 1, 100, kA, kB), false);
    c0.Set(100); ASSERT_EQ(o0.ReleaseDue(), 1u);
    TxnEntry o0out; ASSERT_EQ(o0.ApplyCommit(id, 100, &o0out), ServerOrderer::CommitResult::kApplied);

    o1.SubmitTxn(MakeWrite(1, 1, 100, kA, kB), false);
    c1.Set(100); ASSERT_EQ(o1.ReleaseDue(), 1u);
    TxnEntry o1out; ASSERT_EQ(o1.ApplyCommit(id, 100, &o1out), ServerOrderer::CommitResult::kApplied);

    std::vector<ServerOrderer::ReplicateAction> p0, p1;
    o0.DriveReplication(&p0);
    o1.DriveReplication(&p1);
    EXPECT_EQ(p0.size(), 1u);   // server 0 = min(involved leaders {0,1}) → primary
    EXPECT_EQ(p1.size(), 0u);   // server 1 is involved but not primary
}

// ReplicatePrepare adopts a txn the server already holds (late-buffered follower copy).
TEST(Replicate, PrepareAdoptsHeldLateBufferEntry)
{
    const std::string kA = KeyForPartition(0, 3);   // led by server 0; server 2 is a follower
    const TxnId id = MakeTxnId(1, 1);

    MockClock c2(1000);
    ServerOrderer o2(2, 0, &c2, Topology(3), Noop());

    o2.SubmitTxn(MakeWrite(1, 1, /*t=*/500, kA), false);   // late at follower → lateBuffer
    ASSERT_EQ(o2.late_size(), 1u);

    proto::Txn body = MakeWrite(1, 1, 500, kA).txn;
    ASSERT_TRUE(o2.HandleReplicatePrepare(id, /*agreedT=*/700, body));
    EXPECT_EQ(o2.late_size(), 0u);
    Status s; uint64_t t = 0;
    ASSERT_TRUE(o2.speclog_entry(id, &s, &t));
    EXPECT_EQ(s, Status::COMMITTING);
    EXPECT_EQ(t, 700u);
}

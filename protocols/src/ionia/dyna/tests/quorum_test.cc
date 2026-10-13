// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/tests/quorum_test.cc
//   Stage 3 acceptance tests for TxnQuorum — the multi-leader DECISION-A commit rule:
//   single-partition (m=1) fast path, multi-partition (m>1) fast path, leader-timestamp
//   disagreement → slow path, and the slow-path adoption that re-forms the quorum at
//   agreedT through the same accumulator.

#include "dyna/commit_quorum.h"

#include <gtest/gtest.h>

using namespace specpaxos::dyna;

TEST(TxnQuorum, SinglePartitionFastPath)
{
    // f=1 => quorum=2, one involved leader (server 0).
    TxnQuorum q({0}, /*quorum=*/2);
    q.AddReply(0, /*view=*/0, /*t=*/100, /*is_leader=*/true);
    EXPECT_FALSE(q.fast_ready());              // only the leader so far (1 < 2)
    q.AddReply(1, 0, 100, false);             // a follower agrees
    EXPECT_TRUE(q.fast_ready());
    EXPECT_EQ(q.agreed_t(), 100u);
}

TEST(TxnQuorum, MultiPartitionFastPathAllLeaders)
{
    // Two involved leaders (0 and 1); both agree on (view 0, t 200). f=1 => quorum 2,
    // and the two leaders alone satisfy it.
    TxnQuorum q({0, 1}, 2);
    q.AddReply(0, 0, 200, true);
    EXPECT_FALSE(q.fast_ready());              // leader 1 still missing
    EXPECT_FALSE(q.all_leaders_present());
    q.AddReply(1, 0, 200, true);
    EXPECT_TRUE(q.all_leaders_present());
    EXPECT_TRUE(q.fast_ready());
    EXPECT_EQ(q.agreed_t(), 200u);
}

TEST(TxnQuorum, MissingLeaderBlocksCommit)
{
    // Followers agree but an involved leader never replies => never fast.
    TxnQuorum q({0, 1}, 2);
    q.AddReply(0, 0, 200, true);
    q.AddReply(2, 0, 200, false);
    q.AddReply(3, 0, 200, false);             // plenty of matching replies, but...
    EXPECT_FALSE(q.all_leaders_present());    // ...leader 1 is absent
    EXPECT_FALSE(q.fast_ready());
    EXPECT_FALSE(q.leaders_disagree());       // not "disagree" — just incomplete
}

TEST(TxnQuorum, LeaderDisagreementTriggersSlowPath)
{
    TxnQuorum q({0, 1}, 2);
    q.AddReply(0, 0, 100, true);
    q.AddReply(1, 0, 150, true);              // leaders disagree on t
    EXPECT_TRUE(q.all_leaders_present());
    EXPECT_FALSE(q.fast_ready());
    EXPECT_TRUE(q.leaders_disagree());
    EXPECT_EQ(q.slow_agreed_t(), 150u);       // max over leaders
}

TEST(TxnQuorum, SlowPathAdoptionReformsQuorumAtAgreedT)
{
    TxnQuorum q({0, 1}, 2);
    q.AddReply(0, 0, 100, true);
    q.AddReply(1, 0, 150, true);
    ASSERT_TRUE(q.leaders_disagree());
    const uint64_t agreed = q.slow_agreed_t();   // 150
    EXPECT_EQ(agreed, 150u);

    // Servers adopt agreedT and send CommitReply(150). Latest reply per server wins.
    q.AddReply(0, 0, agreed, true);           // leader 0 adopts 150 (overwrites its 100)
    q.AddReply(1, 0, agreed, true);           // leader 1 already 150
    EXPECT_TRUE(q.fast_ready());              // both leaders at 150, 2 >= quorum
    EXPECT_EQ(q.agreed_t(), 150u);
}

TEST(TxnQuorum, StaleFollowerReplyStopsMatching)
{
    // A follower's original speculative t stops counting once the leader's epoch is
    // known and differs; it must re-report agreedT to count.
    TxnQuorum q({0, 1}, 3);
    q.AddReply(0, 0, 150, true);
    q.AddReply(1, 0, 150, true);
    q.AddReply(2, 0, 100, false);             // stale follower at original t=100
    EXPECT_FALSE(q.fast_ready());             // only 2 at t=150 (< 3)
    q.AddReply(2, 0, 150, false);             // follower re-reports agreedT
    EXPECT_TRUE(q.fast_ready());              // now 3 at t=150
}

// A client in view 1 must not count replies from view 2: there the keys may be led by a
// different server, so two view-2 followers would look like "leader + follower" under the
// client's view-1 leader set and fake a fast commit.
TEST(TxnQuorum, RepliesFromAnotherViewIgnored)
{
    TxnQuorum q({0}, /*quorum=*/2, /*view=*/1);
    EXPECT_FALSE(q.AddReply(0, /*view=*/2, 100, false));
    EXPECT_FALSE(q.AddReply(1, 2, 100, false));
    EXPECT_FALSE(q.fast_ready());
    EXPECT_EQ(q.reply_count(), 0u);
    EXPECT_TRUE(q.AddReply(0, 1, 100, true));
    EXPECT_TRUE(q.AddReply(1, 1, 100, false));
    EXPECT_TRUE(q.fast_ready());
}

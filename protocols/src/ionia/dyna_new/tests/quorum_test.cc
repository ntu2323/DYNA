// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/tests/quorum_test.cc
//   Stage 4 acceptance tests for the write commit rule (spec §5), exercised
//   through the pure WriteQuorum accumulator.

#include "dyna_new/write_quorum.h"

#include <gtest/gtest.h>

using namespace specpaxos::dyna_new;

namespace {
constexpr uint32_t kLeader = 0;
constexpr uint64_t kView = 0;
constexpr uint64_t kDeadline = 100;
} // namespace

// f=1: a blind write commits with the leader + exactly one follower agreeing on
// the leader's deadline.
TEST(WriteQuorum, CommitsWithLeaderPlusOneFollower)
{
    WriteQuorum q(kLeader, /*write_quorum=*/2, kDeadline);

    EXPECT_FALSE(q.AddReply(kLeader, kView, kDeadline));   // leader alone: 1/2
    EXPECT_FALSE(q.committed());

    EXPECT_TRUE(q.AddReply(1, kView, kDeadline));          // +1 follower: 2/2
    EXPECT_TRUE(q.committed());
    EXPECT_EQ(q.responders(), 2u);
}

// A write where only followers (not the leader) reply does NOT commit — the
// leader's reply is mandatory.
TEST(WriteQuorum, FollowerOnlyQuorumDoesNotCommit)
{
    WriteQuorum q(kLeader, /*write_quorum=*/2, kDeadline);

    EXPECT_FALSE(q.AddReply(1, kView, kDeadline));
    EXPECT_FALSE(q.AddReply(2, kView, kDeadline));
    EXPECT_FALSE(q.committed());
    EXPECT_FALSE(q.leader_acked());
    EXPECT_EQ(q.responders(), 2u);   // two followers tallied, but no leader

    // The leader's confirming reply (same deadline) then completes the quorum
    // without discarding the matched followers.
    EXPECT_TRUE(q.AddReply(kLeader, kView, kDeadline));
    EXPECT_TRUE(q.committed());
}

// Mismatched deadlines do not commit until f+1 agree on the LEADER's deadline.
// Models the slow path (§6.2): the leader reassigns, follower stale replies must
// be superseded by corrected replies (§6.4).
TEST(WriteQuorum, NoCommitUntilDeadlinesAgreeOnLeader)
{
    WriteQuorum q(kLeader, /*write_quorum=*/2, /*initial=*/100);

    // A follower speculatively replies on the original (stamped) deadline.
    EXPECT_FALSE(q.AddReply(1, kView, 100));

    // The leader took the slow path and reassigned the deadline to 105. This
    // switches the epoch and discards the stale tally.
    EXPECT_FALSE(q.AddReply(kLeader, kView, 105));
    EXPECT_EQ(q.active_deadline(), 105u);
    EXPECT_EQ(q.responders(), 1u);   // only the leader now

    // A stale follower reply on the old deadline is ignored.
    EXPECT_FALSE(q.AddReply(2, kView, 100));
    EXPECT_EQ(q.responders(), 1u);

    // The corrected follower reply on the leader's deadline commits.
    EXPECT_TRUE(q.AddReply(1, kView, 105));
    EXPECT_TRUE(q.committed());
}

// The quorum size is a parameter, not a hard-coded literal: with write_quorum=3
// the leader + 1 follower is insufficient.
TEST(WriteQuorum, RespectsConfiguredQuorumSize)
{
    WriteQuorum q(kLeader, /*write_quorum=*/3, kDeadline);

    EXPECT_FALSE(q.AddReply(kLeader, kView, kDeadline));
    EXPECT_FALSE(q.AddReply(1, kView, kDeadline));   // 2/3 — not yet
    EXPECT_FALSE(q.committed());
    EXPECT_TRUE(q.AddReply(2, kView, kDeadline));     // 3/3
    EXPECT_TRUE(q.committed());
}

// A view mismatch does not count toward the leader's quorum.
TEST(WriteQuorum, IgnoresRepliesFromOtherViews)
{
    WriteQuorum q(kLeader, /*write_quorum=*/2, kDeadline);

    EXPECT_FALSE(q.AddReply(kLeader, /*view=*/1, kDeadline));   // leader in view 1
    EXPECT_EQ(q.active_deadline(), kDeadline);
    // Follower stuck in view 0 must not count.
    EXPECT_FALSE(q.AddReply(1, /*view=*/0, kDeadline));
    EXPECT_EQ(q.responders(), 1u);
    // Follower in the leader's view commits.
    EXPECT_TRUE(q.AddReply(1, /*view=*/1, kDeadline));
    EXPECT_TRUE(q.committed());
}

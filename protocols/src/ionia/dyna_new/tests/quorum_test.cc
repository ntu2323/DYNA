// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/tests/quorum_test.cc
//   Stage 4/6 acceptance tests for the unified commit rule (spec §5, §6.3),
//   exercised through the pure CommitQuorum accumulator. Writes agree on the
//   leader's deadline; reads agree on the leader's log position and return the
//   leader's data.

#include "dyna_new/commit_quorum.h"

#include <gtest/gtest.h>

using namespace specpaxos::dyna_new;

namespace {
constexpr uint32_t kLeader = 0;
constexpr uint64_t kView = 0;
constexpr uint64_t kMatch = 100;   // a deadline (writes) or log_pos (reads)
} // namespace

// --- write commit rule (match value = leader deadline) ---

TEST(CommitQuorum, CommitsWithLeaderPlusOneFollower)
{
    CommitQuorum q(kLeader, /*quorum=*/2);
    EXPECT_FALSE(q.AddReply(kLeader, kView, kMatch));   // leader alone: 1/2
    EXPECT_FALSE(q.committed());
    EXPECT_TRUE(q.AddReply(1, kView, kMatch));          // +1 follower: 2/2
    EXPECT_TRUE(q.committed());
    EXPECT_EQ(q.matching(), 2u);
}

TEST(CommitQuorum, FollowerOnlyQuorumDoesNotCommit)
{
    CommitQuorum q(kLeader, /*quorum=*/2);
    EXPECT_FALSE(q.AddReply(1, kView, kMatch));
    EXPECT_FALSE(q.AddReply(2, kView, kMatch));
    EXPECT_FALSE(q.committed());
    EXPECT_FALSE(q.leader_acked());

    // The leader's confirming reply completes the quorum (the matched followers
    // are retained, since the re-evaluation counts them against the leader).
    EXPECT_TRUE(q.AddReply(kLeader, kView, kMatch));
    EXPECT_TRUE(q.committed());
}

TEST(CommitQuorum, NoCommitUntilMatchAgreesOnLeader)
{
    CommitQuorum q(kLeader, /*quorum=*/2);

    // A follower speculatively replies on the original (stamped) deadline.
    EXPECT_FALSE(q.AddReply(1, kView, 100));
    // The leader took the slow path and reassigned to 105.
    EXPECT_FALSE(q.AddReply(kLeader, kView, 105));
    EXPECT_EQ(q.active_match(), 105u);
    EXPECT_EQ(q.matching(), 1u);   // only the leader matches the new epoch
    // A stale follower reply (old deadline) does not match.
    EXPECT_FALSE(q.AddReply(2, kView, 100));
    EXPECT_EQ(q.matching(), 1u);
    // The corrected follower reply on the leader's deadline commits.
    EXPECT_TRUE(q.AddReply(1, kView, 105));
    EXPECT_TRUE(q.committed());
}

TEST(CommitQuorum, RespectsConfiguredQuorumSize)
{
    CommitQuorum q(kLeader, /*quorum=*/3);
    EXPECT_FALSE(q.AddReply(kLeader, kView, kMatch));
    EXPECT_FALSE(q.AddReply(1, kView, kMatch));   // 2/3
    EXPECT_FALSE(q.committed());
    EXPECT_TRUE(q.AddReply(2, kView, kMatch));     // 3/3
    EXPECT_TRUE(q.committed());
}

TEST(CommitQuorum, IgnoresRepliesFromOtherViews)
{
    CommitQuorum q(kLeader, /*quorum=*/2);
    EXPECT_FALSE(q.AddReply(kLeader, /*view=*/1, kMatch));
    EXPECT_FALSE(q.AddReply(1, /*view=*/0, kMatch));   // wrong view
    EXPECT_EQ(q.matching(), 1u);
    EXPECT_TRUE(q.AddReply(1, /*view=*/1, kMatch));     // leader's view
    EXPECT_TRUE(q.committed());
}

// --- read commit rule (match value = leader log_pos; leader carries data) ---

TEST(CommitQuorum, ReadCommitsOnLogPosAndReturnsLeaderData)
{
    CommitQuorum q(kLeader, /*quorum=*/2);

    // Leader data reply: log_pos=3, result="val".
    EXPECT_FALSE(q.AddReply(kLeader, kView, /*log_pos=*/3, /*has_result=*/true, "val"));
    EXPECT_EQ(q.result(), "val");
    // One endorsement on a different log_pos must not commit.
    EXPECT_FALSE(q.AddReply(1, kView, /*log_pos=*/2));
    EXPECT_FALSE(q.committed());
    // An endorsement agreeing on the leader's log_pos commits.
    EXPECT_TRUE(q.AddReply(1, kView, /*log_pos=*/3));
    EXPECT_TRUE(q.committed());
    EXPECT_EQ(q.result(), "val");
}

TEST(CommitQuorum, ReadWithoutLeaderDoesNotCommit)
{
    CommitQuorum q(kLeader, /*quorum=*/2);
    EXPECT_FALSE(q.AddReply(1, kView, /*log_pos=*/3));
    EXPECT_FALSE(q.AddReply(2, kView, /*log_pos=*/3));
    EXPECT_FALSE(q.committed());   // endorsements alone never commit a read
}

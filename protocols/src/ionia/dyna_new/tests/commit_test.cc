// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/tests/commit_test.cc
//   Stage 7 acceptance tests for background ordering / persistence (spec §7,
//   §3.3 commit_point). The leader tallies sync_confirms and, on f+1 (including
//   its own append), persists off-path and signals a Commit; followers persist
//   on Commit. Disk persistence is asynchronous, so disk_read is checked via a
//   short poll.

#include "dyna_new/partition_orderer.h"
#include "dyna_new/in_memory_backend.h"
#include "dyna_new/partition_state.h"
#include "dyna_new/wall_clock.h"
#include "dyna_new/dyna-new-proto.pb.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

using namespace specpaxos::dyna_new;
namespace proto = specpaxos::dyna_new::proto;

namespace {

LogEntry Write(uint64_t client, uint64_t req, uint64_t deadline,
               const std::string &key = "k", const std::string &val = "v")
{
    LogEntry e;
    e.client_id = client;
    e.request_id = req;
    e.type = proto::BLIND_WRITE;
    e.key = key;
    e.command = val;
    e.deadline = deadline;
    return e;
}

SyncIn Sync(uint64_t log_id, uint64_t client, uint64_t req, uint64_t deadline)
{
    SyncIn s;
    s.view_id = 0;
    s.log_id = log_id;
    s.client_id = client;
    s.request_id = req;
    s.deadline = deadline;
    return s;
}

template <typename F>
bool WaitFor(F pred, int max_ms = 2000)
{
    for (int i = 0; i < max_ms; ++i) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return pred();
}

} // namespace

// §7: leader commits a write once it holds f+1 confirmations (its own append +
// f followers) agreeing on the leader's deadline; then it persists and advances
// commit_point.
TEST(Commit, LeaderCommitsOnQuorumConfirm)
{
    InMemoryBackend backend;          // f=1 -> quorum 2
    MockClock clock(0);
    PartitionOrderer leader(0, 0, &backend, &clock,
                            PartitionOrderer::Role::kLeader, nullptr,
                            /*commit_quorum=*/2);

    leader.Submit(Write(1, 1, 100, "k", "v1"));
    clock.Set(100);
    ASSERT_EQ(leader.ReleaseDue(), 1u);
    // Leader append alone is not yet a quorum; nothing committed/persisted.
    EXPECT_EQ(leader.commit_point(), 0u);
    EXPECT_EQ(backend.disk_persist_count(), 0u);

    // One follower confirmation reaches f+1 -> commit.
    EXPECT_TRUE(leader.HandleSyncConfirm(/*replica=*/1, 1, 1, /*deadline=*/100));
    EXPECT_EQ(leader.commit_point(), 1u);
    EXPECT_TRUE(WaitFor([&] { return backend.disk_read("k") == "v1"; }));

    // A later confirmation for the same request does not re-commit.
    EXPECT_FALSE(leader.HandleSyncConfirm(/*replica=*/2, 1, 1, 100));
    EXPECT_EQ(leader.commit_point(), 1u);
}

// §7: a confirmation that disagrees on the leader's deadline does not count.
TEST(Commit, MismatchedDeadlineDoesNotCommit)
{
    InMemoryBackend backend;
    MockClock clock(0);
    PartitionOrderer leader(0, 0, &backend, &clock,
                            PartitionOrderer::Role::kLeader, nullptr, 2);

    leader.Submit(Write(1, 1, 100));
    clock.Set(100);
    ASSERT_EQ(leader.ReleaseDue(), 1u);

    EXPECT_FALSE(leader.HandleSyncConfirm(1, 1, 1, /*deadline=*/999));
    EXPECT_EQ(leader.commit_point(), 0u);
    EXPECT_EQ(backend.disk_persist_count(), 0u);
}

// §7: confirmations are de-duplicated per replica; the same follower twice is
// one vote.
TEST(Commit, DuplicateConfirmerDoesNotOverCount)
{
    InMemoryBackend backend;
    MockClock clock(0);
    PartitionOrderer leader(0, 0, &backend, &clock,
                            PartitionOrderer::Role::kLeader, nullptr,
                            /*commit_quorum=*/3); // f=2

    leader.Submit(Write(1, 1, 100));
    clock.Set(100);
    ASSERT_EQ(leader.ReleaseDue(), 1u);

    EXPECT_FALSE(leader.HandleSyncConfirm(1, 1, 1, 100)); // 1 (leader) + {1}
    EXPECT_FALSE(leader.HandleSyncConfirm(1, 1, 1, 100)); // still {1}
    EXPECT_EQ(leader.commit_point(), 0u);
    EXPECT_TRUE(leader.HandleSyncConfirm(2, 1, 1, 100));  // {1,2} -> 3
    EXPECT_EQ(leader.commit_point(), 1u);
}

// §7: a single replica (quorum 1) commits on its own append at release time.
TEST(Commit, SingleReplicaSelfCommits)
{
    InMemoryBackend backend;
    MockClock clock(0);
    PartitionOrderer leader(0, 0, &backend, &clock,
                            PartitionOrderer::Role::kLeader, nullptr,
                            /*commit_quorum=*/1);

    leader.Submit(Write(1, 1, 100, "k", "solo"));
    clock.Set(100);
    ASSERT_EQ(leader.ReleaseDue(), 1u);
    EXPECT_EQ(leader.commit_point(), 1u);
    EXPECT_TRUE(WaitFor([&] { return backend.disk_read("k") == "solo"; }));
}

// §7: a follower persists its own copy when the leader's Commit arrives.
TEST(Commit, FollowerPersistsOnCommit)
{
    InMemoryBackend backend;
    MockClock clock(0);
    PartitionOrderer fol(0, 0, &backend, &clock,
                         PartitionOrderer::Role::kFollower);

    fol.Submit(Write(1, 1, 100, "k", "fv")); // body known
    fol.HandleSync(Sync(/*log_id=*/0, 1, 1, /*deadline=*/100));
    ASSERT_EQ(fol.sync_point(), 1u);
    EXPECT_EQ(fol.commit_point(), 0u);
    EXPECT_EQ(backend.disk_persist_count(), 0u);

    fol.HandleCommit(1, 1);
    EXPECT_EQ(fol.commit_point(), 1u);
    EXPECT_TRUE(WaitFor([&] { return backend.disk_read("k") == "fv"; }));
}

// §7: a Commit that races ahead of the Sync is buffered and applied when
// reconciliation catches up.
TEST(Commit, FollowerBuffersCommitBeforeSync)
{
    InMemoryBackend backend;
    MockClock clock(0);
    PartitionOrderer fol(0, 0, &backend, &clock,
                         PartitionOrderer::Role::kFollower);

    fol.HandleCommit(1, 1);              // arrives before we know the entry
    EXPECT_EQ(fol.commit_point(), 0u);
    EXPECT_EQ(backend.disk_persist_count(), 0u);

    fol.Submit(Write(1, 1, 100, "k", "late"));
    fol.HandleSync(Sync(0, 1, 1, 100));  // drains -> applies buffered commit
    EXPECT_EQ(fol.commit_point(), 1u);
    EXPECT_TRUE(WaitFor([&] { return backend.disk_read("k") == "late"; }));
}

// §3.3: commit_point advances only over the contiguous committed prefix.
TEST(Commit, CommitPointAdvancesContiguously)
{
    InMemoryBackend backend;
    MockClock clock(0);
    PartitionOrderer leader(0, 0, &backend, &clock,
                            PartitionOrderer::Role::kLeader, nullptr, 2);

    leader.Submit(Write(1, 1, 100, "a", "va")); // log pos 0
    leader.Submit(Write(1, 2, 200, "b", "vb")); // log pos 1
    clock.Set(200);
    ASSERT_EQ(leader.ReleaseDue(), 2u);

    // Commit the second entry first: prefix is not contiguous, so commit_point
    // stays at 0.
    EXPECT_TRUE(leader.HandleSyncConfirm(1, 1, 2, 200));
    EXPECT_EQ(leader.commit_point(), 0u);

    // Now the first entry commits: commit_point jumps over both.
    EXPECT_TRUE(leader.HandleSyncConfirm(1, 1, 1, 100));
    EXPECT_EQ(leader.commit_point(), 2u);
}

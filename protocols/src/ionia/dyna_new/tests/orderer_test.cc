// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/tests/orderer_test.cc
//   Stage 3 acceptance tests for the deadline mechanism (spec §2, §3.2, §6.1,
//   §6.2). Correctness tests use the synchronous ReleaseDue() with a MockClock
//   for full determinism; two tests start the background loop to exercise the
//   timer/condition-variable path and the no-busy-wait property.

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

// Poll until pred() or timeout; returns pred()'s final value.
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

// §6.1: out-of-order arrival, distinct deadlines -> log comes out deadline-ordered.
TEST(PartitionOrderer, OutOfOrderArrivalReleasesInDeadlineOrder)
{
    InMemoryBackend backend;
    MockClock clock(0);
    PartitionOrderer orderer(/*pid=*/0, /*view=*/0, &backend, &clock);

    orderer.Submit(Write(1, 1, 300));
    orderer.Submit(Write(1, 2, 100));
    orderer.Submit(Write(1, 3, 200));

    clock.Set(99);
    EXPECT_EQ(orderer.ReleaseDue(), 0u);   // nothing due yet

    clock.Set(300);
    EXPECT_EQ(orderer.ReleaseDue(), 3u);

    ASSERT_EQ(orderer.synced_log_size(), 3u);
    EXPECT_EQ(orderer.synced_log_at(0).deadline, 100u);
    EXPECT_EQ(orderer.synced_log_at(1).deadline, 200u);
    EXPECT_EQ(orderer.synced_log_at(2).deadline, 300u);
    EXPECT_EQ(orderer.synced_log_at(0).log_pos, 0u);
    EXPECT_EQ(orderer.synced_log_at(2).log_pos, 2u);
    EXPECT_EQ(orderer.deadline_counter(), 300u);
    EXPECT_EQ(orderer.sync_point(), 3u);
}

// §6.2: a late request is reassigned past the counter and appended, not dropped.
TEST(PartitionOrderer, LateRequestIsReassignedAndAppended)
{
    InMemoryBackend backend;
    MockClock clock(0);
    PartitionOrderer orderer(0, 0, &backend, &clock);

    orderer.Submit(Write(1, 1, 100));
    clock.Set(100);
    EXPECT_EQ(orderer.ReleaseDue(), 1u);
    EXPECT_EQ(orderer.deadline_counter(), 100u);

    // Arrives late: deadline 50 <= counter 100. Reassigned to max(now=100,
    // counter+1=101) = 101, then appended after the first entry.
    orderer.Submit(Write(1, 2, 50));
    EXPECT_EQ(orderer.late_buffer_size(), 0u);   // staged then moved out
    EXPECT_EQ(orderer.early_buffer_size(), 1u);

    clock.Set(101);
    EXPECT_EQ(orderer.ReleaseDue(), 1u);

    ASSERT_EQ(orderer.synced_log_size(), 2u);
    EXPECT_EQ(orderer.synced_log_at(1).request_id, 2u);
    EXPECT_EQ(orderer.synced_log_at(1).deadline, 101u);   // reassigned, > 100
    EXPECT_GT(orderer.synced_log_at(1).deadline, orderer.synced_log_at(0).deadline);
    EXPECT_EQ(orderer.deadline_counter(), 101u);
}

// §3 invariant 1: equal deadlines break ties deterministically by
// (client_id, request_id) through the release path.
TEST(PartitionOrderer, EqualDeadlineTieBreaksByClientThenRequest)
{
    InMemoryBackend backend;
    MockClock clock(0);
    PartitionOrderer orderer(0, 0, &backend, &clock);

    orderer.Submit(Write(2, 5, 100));
    orderer.Submit(Write(1, 9, 100));
    orderer.Submit(Write(1, 4, 100));
    orderer.Submit(Write(2, 1, 100));

    clock.Set(100);
    EXPECT_EQ(orderer.ReleaseDue(), 4u);

    ASSERT_EQ(orderer.synced_log_size(), 4u);
    EXPECT_EQ(orderer.synced_log_at(0).client_id, 1u);
    EXPECT_EQ(orderer.synced_log_at(0).request_id, 4u);
    EXPECT_EQ(orderer.synced_log_at(1).client_id, 1u);
    EXPECT_EQ(orderer.synced_log_at(1).request_id, 9u);
    EXPECT_EQ(orderer.synced_log_at(2).client_id, 2u);
    EXPECT_EQ(orderer.synced_log_at(2).request_id, 1u);
    EXPECT_EQ(orderer.synced_log_at(3).client_id, 2u);
    EXPECT_EQ(orderer.synced_log_at(3).request_id, 5u);
}

// §6.1: a future-deadline entry is not released early and is released exactly
// when the clock reaches its deadline (deterministic via MockClock).
TEST(PartitionOrderer, FutureDeadlineReleasedExactlyAtDeadline)
{
    InMemoryBackend backend;
    MockClock clock(0);
    PartitionOrderer orderer(0, 0, &backend, &clock);

    orderer.Submit(Write(1, 1, 1000));

    clock.Set(999);
    EXPECT_EQ(orderer.ReleaseDue(), 0u);          // not early
    EXPECT_EQ(orderer.synced_log_size(), 0u);

    clock.Set(1000);
    EXPECT_EQ(orderer.ReleaseDue(), 1u);          // exactly at deadline (<=)
    EXPECT_EQ(orderer.synced_log_size(), 1u);
}

// §6.1: writes are applied to authoritative memory on release.
TEST(PartitionOrderer, MemApplyOnRelease)
{
    InMemoryBackend backend;
    MockClock clock(0);
    PartitionOrderer orderer(0, 0, &backend, &clock);

    EXPECT_FALSE(backend.mem_read("k").has_value());
    orderer.Submit(Write(1, 1, 100, "k", "v"));

    clock.Set(99);
    orderer.ReleaseDue();
    EXPECT_FALSE(backend.mem_read("k").has_value());   // not applied before release

    clock.Set(100);
    orderer.ReleaseDue();
    auto v = backend.mem_read("k");
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, "v");
}

// Background loop: releases at the deadline, driven by the cv (not polling).
TEST(PartitionOrderer, BackgroundLoopReleasesAtDeadline)
{
    InMemoryBackend backend;
    MockClock clock(0);
    PartitionOrderer orderer(0, 0, &backend, &clock);
    orderer.Start();

    orderer.Submit(Write(1, 1, 1000, "bk", "bv"));

    // Give the loop time to process the submit; it must NOT release early.
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_EQ(orderer.synced_log_size(), 0u);

    clock.Set(1000);   // wakes the loop; release should follow
    EXPECT_TRUE(WaitFor([&] { return orderer.synced_log_size() == 1; }));
    EXPECT_EQ(*backend.mem_read("bk"), "bv");

    orderer.Stop();
}

// Submit wakes a loop blocked on an empty buffer, without advancing the clock.
TEST(PartitionOrderer, SubmitWakesIdleLoop)
{
    InMemoryBackend backend;
    MockClock clock(500);
    PartitionOrderer orderer(0, 0, &backend, &clock);
    orderer.Start();

    std::this_thread::sleep_for(std::chrono::milliseconds(20));   // reach idle wait
    orderer.Submit(Write(1, 1, 500));   // deadline == now -> immediately due

    EXPECT_TRUE(WaitFor([&] { return orderer.synced_log_size() == 1; }));
    orderer.Stop();
}

// No busy-wait: an idle loop (empty buffer) does not spin.
TEST(PartitionOrderer, IdleLoopDoesNotBusyWait)
{
    InMemoryBackend backend;
    MockClock clock(0);
    PartitionOrderer orderer(0, 0, &backend, &clock);
    orderer.Start();

    std::this_thread::sleep_for(std::chrono::milliseconds(50));   // settle into wait
    uint64_t w1 = orderer.loop_wakeups();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    uint64_t w2 = orderer.loop_wakeups();

    EXPECT_EQ(w1, w2);          // no iterations while idle
    EXPECT_LE(w2, 5u);          // settled almost immediately

    orderer.Stop();
}

// §6.1: the leader fires the on-release callback per released entry (used by the
// replica to send the leader reply), after appending to synced_log.
TEST(PartitionOrderer, LeaderFiresReleaseCallback)
{
    InMemoryBackend backend;
    MockClock clock(0);
    std::vector<LogEntry> released;
    PartitionOrderer orderer(0, 0, &backend, &clock,
                             PartitionOrderer::Role::kLeader,
                             [&](const LogEntry &e) { released.push_back(e); });

    orderer.Submit(Write(1, 1, 100));
    clock.Set(100);
    EXPECT_EQ(orderer.ReleaseDue(), 1u);

    ASSERT_EQ(released.size(), 1u);
    EXPECT_EQ(released[0].request_id, 1u);
    EXPECT_EQ(orderer.synced_log_size(), 1u);
}

// §6.1 follower speculative release: stage in unsynced_log, do NOT apply to
// memory, and still fire the callback (for the is_leader=false reply).
TEST(PartitionOrderer, FollowerStagesUnsyncedWithoutApply)
{
    InMemoryBackend backend;
    MockClock clock(0);
    std::vector<LogEntry> released;
    PartitionOrderer orderer(0, 0, &backend, &clock,
                             PartitionOrderer::Role::kFollower,
                             [&](const LogEntry &e) { released.push_back(e); });

    orderer.Submit(Write(1, 1, 100, "k", "v"));
    clock.Set(100);
    EXPECT_EQ(orderer.ReleaseDue(), 1u);

    EXPECT_EQ(orderer.synced_log_size(), 0u);          // not on the synced log
    ASSERT_EQ(orderer.unsynced_log_size(), 1u);
    EXPECT_EQ(orderer.unsynced_log_at(0).request_id, 1u);
    EXPECT_FALSE(backend.mem_read("k").has_value());    // no speculative apply
    ASSERT_EQ(released.size(), 1u);                      // endorsement callback fired
}

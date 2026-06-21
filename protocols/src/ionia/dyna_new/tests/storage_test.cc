// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/tests/storage_test.cc
//   Stage 2 acceptance tests: InMemoryBackend memory read/write, asynchronous
//   (non-blocking) disk_persist with simulated latency, and the dedup table.

#include "dyna_new/in_memory_backend.h"
#include "dyna_new/dedup_table.h"
#include "dyna_new/dyna-new-proto.pb.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

using namespace specpaxos::dyna_new;
namespace proto = specpaxos::dyna_new::proto;

// ---------------------------------------------------------------------------
// Memory KV: write-then-read returns the value (spec §3.4).
// ---------------------------------------------------------------------------

TEST(InMemoryBackend, MemWriteThenRead)
{
    InMemoryBackend be;
    EXPECT_FALSE(be.mem_read("absent").has_value());

    be.mem_apply("k", "v1");
    auto got = be.mem_read("k");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, "v1");

    be.mem_apply("k", "v2");   // overwrite
    EXPECT_EQ(*be.mem_read("k"), "v2");
}

// ---------------------------------------------------------------------------
// disk_persist is asynchronous: the caller is not blocked by the simulated
// write latency, yet the value is eventually durable (build plan §7 / inv. 2).
// ---------------------------------------------------------------------------

TEST(InMemoryBackend, DiskPersistIsAsyncAndEventuallyReadable)
{
    const uint64_t latency_us = 200000;   // 200ms simulated disk write
    InMemoryBackend be(latency_us);

    auto t0 = std::chrono::steady_clock::now();
    be.disk_persist("dk", "dv");
    auto call_us = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - t0).count();

    // The enqueue must return far faster than the simulated write latency.
    EXPECT_LT(call_us, static_cast<long long>(latency_us / 2));

    // Not yet durable (worker is still "writing").
    EXPECT_FALSE(be.disk_read("dk").has_value());

    // Eventually becomes readable once the background write completes.
    std::optional<std::string> got;
    for (int i = 0; i < 200; ++i) {   // up to ~2s
        got = be.disk_read("dk");
        if (got.has_value()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, "dv");
}

TEST(InMemoryBackend, DiskPersistNoLatencyStillWorks)
{
    InMemoryBackend be;   // 0 latency
    be.disk_persist("a", "1");
    be.disk_persist("b", "2");

    std::optional<std::string> a, b;
    for (int i = 0; i < 200; ++i) {
        a = be.disk_read("a");
        b = be.disk_read("b");
        if (a.has_value() && b.has_value()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(*a, "1");
    EXPECT_EQ(*b, "2");
}

// ---------------------------------------------------------------------------
// Dedup table: ⟨client_id, request_id⟩ -> cached reply (spec §1.6, §6.1).
// ---------------------------------------------------------------------------

TEST(DedupTable, RecordLookupContains)
{
    DedupTable dt;
    ClientAndReqId k1{7, 42};
    ClientAndReqId k2{7, 43};

    EXPECT_FALSE(dt.Contains(k1));
    EXPECT_EQ(dt.Lookup(k1), nullptr);
    EXPECT_EQ(dt.Size(), 0u);

    proto::Reply r;
    r.set_view_id(1);
    r.set_replica_id(0);
    r.set_partition_id(0);
    r.set_client_id(7);
    r.set_request_id(42);
    r.set_deadline(1234);
    r.set_is_leader(true);
    r.set_owd_estimate(100);
    dt.Record(k1, r);

    EXPECT_TRUE(dt.Contains(k1));
    EXPECT_EQ(dt.Size(), 1u);
    const proto::Reply *cached = dt.Lookup(k1);
    ASSERT_NE(cached, nullptr);
    EXPECT_EQ(cached->request_id(), 42u);
    EXPECT_EQ(cached->deadline(), 1234u);
    EXPECT_TRUE(cached->is_leader());

    // Different request id is still unknown.
    EXPECT_FALSE(dt.Contains(k2));
    EXPECT_EQ(dt.Lookup(k2), nullptr);
}

TEST(DedupTable, RecordOverwrites)
{
    DedupTable dt;
    ClientAndReqId k{1, 1};

    proto::Reply r1;
    r1.set_view_id(1); r1.set_replica_id(0); r1.set_partition_id(0);
    r1.set_client_id(1); r1.set_request_id(1); r1.set_deadline(10);
    r1.set_is_leader(false); r1.set_owd_estimate(1);
    dt.Record(k, r1);

    proto::Reply r2 = r1;
    r2.set_deadline(20);   // corrected deadline
    dt.Record(k, r2);

    EXPECT_EQ(dt.Size(), 1u);
    EXPECT_EQ(dt.Lookup(k)->deadline(), 20u);
}

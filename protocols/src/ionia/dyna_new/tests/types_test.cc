// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/tests/types_test.cc
//   Stage 1 acceptance tests: protobuf round-trip for every wire message (§4),
//   per-partition state-block initial values (§3.2-3.3), and the load-bearing
//   early-buffer tie-break ordering (§3 invariant 1).

#include "dyna_new/dyna-new-proto.pb.h"
#include "dyna_new/partition_state.h"
#include "dyna_new/wall_clock.h"

#include <gtest/gtest.h>

using namespace specpaxos::dyna_new;
namespace proto = specpaxos::dyna_new::proto;

// ---------------------------------------------------------------------------
// §4 message formats — serialize -> deserialize -> assert field equality.
// Serialization is provided entirely by protobuf (the transport calls
// SerializeAsString()/ParseFromArray() itself), so these tests exercise the
// real wire encoding without any custom serializer.
// ---------------------------------------------------------------------------

TEST(Serialization, RequestRoundTrip)
{
    proto::Request in;
    in.set_client_id(7);
    in.set_request_id(42);
    in.set_type(proto::RMW);
    in.set_key("k1");
    in.set_command("v1");
    in.set_send_time(1000);
    in.set_deadline(1500);

    std::string bytes = in.SerializeAsString();
    proto::Request out;
    ASSERT_TRUE(out.ParseFromString(bytes));

    EXPECT_EQ(out.client_id(), 7u);
    EXPECT_EQ(out.request_id(), 42u);
    EXPECT_EQ(out.type(), proto::RMW);
    EXPECT_EQ(out.key(), "k1");
    EXPECT_EQ(out.command(), "v1");
    EXPECT_EQ(out.send_time(), 1000u);
    EXPECT_EQ(out.deadline(), 1500u);
}

TEST(Serialization, ReplyRoundTrip)
{
    proto::Reply in;
    in.set_view_id(3);
    in.set_replica_id(2);
    in.set_partition_id(1);
    in.set_client_id(7);
    in.set_request_id(42);
    in.set_deadline(1500);
    in.set_is_leader(true);
    in.set_result("readval");
    in.set_owd_estimate(250);
    in.set_log_pos(9);

    std::string bytes = in.SerializeAsString();
    proto::Reply out;
    ASSERT_TRUE(out.ParseFromString(bytes));

    EXPECT_EQ(out.view_id(), 3u);
    EXPECT_EQ(out.replica_id(), 2u);
    EXPECT_EQ(out.partition_id(), 1u);
    EXPECT_EQ(out.client_id(), 7u);
    EXPECT_EQ(out.request_id(), 42u);
    EXPECT_EQ(out.deadline(), 1500u);
    EXPECT_TRUE(out.is_leader());
    EXPECT_TRUE(out.has_result());
    EXPECT_EQ(out.result(), "readval");
    EXPECT_EQ(out.owd_estimate(), 250u);
    EXPECT_TRUE(out.has_log_pos());
    EXPECT_EQ(out.log_pos(), 9u);
}

TEST(Serialization, ReplyOptionalFieldsAbsent)
{
    // result and log_pos are optional (null in follower / blind-write replies).
    proto::Reply in;
    in.set_view_id(0);
    in.set_replica_id(1);
    in.set_partition_id(0);
    in.set_client_id(7);
    in.set_request_id(42);
    in.set_deadline(1500);
    in.set_is_leader(false);
    in.set_owd_estimate(100);

    std::string bytes = in.SerializeAsString();
    proto::Reply out;
    ASSERT_TRUE(out.ParseFromString(bytes));

    EXPECT_FALSE(out.has_result());
    EXPECT_FALSE(out.has_log_pos());
    EXPECT_FALSE(out.is_leader());
}

TEST(Serialization, SyncRoundTrip)
{
    proto::Sync in;
    in.set_view_id(1);
    in.set_partition_id(2);
    in.set_log_id(10);
    in.set_client_id(7);
    in.set_request_id(42);
    in.set_deadline(1600);

    std::string bytes = in.SerializeAsString();
    proto::Sync out;
    ASSERT_TRUE(out.ParseFromString(bytes));

    EXPECT_EQ(out.view_id(), 1u);
    EXPECT_EQ(out.partition_id(), 2u);
    EXPECT_EQ(out.log_id(), 10u);
    EXPECT_EQ(out.client_id(), 7u);
    EXPECT_EQ(out.request_id(), 42u);
    EXPECT_EQ(out.deadline(), 1600u);
}

TEST(Serialization, SyncConfirmRoundTrip)
{
    proto::SyncConfirm in;
    in.set_view_id(1);
    in.set_replica_id(2);
    in.set_partition_id(2);
    in.set_client_id(7);
    in.set_request_id(42);
    in.set_deadline(1600);

    std::string bytes = in.SerializeAsString();
    proto::SyncConfirm out;
    ASSERT_TRUE(out.ParseFromString(bytes));

    EXPECT_EQ(out.view_id(), 1u);
    EXPECT_EQ(out.replica_id(), 2u);
    EXPECT_EQ(out.partition_id(), 2u);
    EXPECT_EQ(out.client_id(), 7u);
    EXPECT_EQ(out.request_id(), 42u);
    EXPECT_EQ(out.deadline(), 1600u);
}

TEST(Serialization, CommitRoundTrip)
{
    proto::Commit in;
    in.set_view_id(1);
    in.set_partition_id(2);
    in.set_client_id(7);
    in.set_request_id(42);

    std::string bytes = in.SerializeAsString();
    proto::Commit out;
    ASSERT_TRUE(out.ParseFromString(bytes));

    EXPECT_EQ(out.view_id(), 1u);
    EXPECT_EQ(out.partition_id(), 2u);
    EXPECT_EQ(out.client_id(), 7u);
    EXPECT_EQ(out.request_id(), 42u);
}

TEST(Serialization, FetchRequestRoundTrip)
{
    proto::FetchRequest in;
    in.set_partition_id(2);
    in.set_client_id(7);
    in.set_request_id(42);

    std::string bytes = in.SerializeAsString();
    proto::FetchRequest out;
    ASSERT_TRUE(out.ParseFromString(bytes));

    EXPECT_EQ(out.partition_id(), 2u);
    EXPECT_EQ(out.client_id(), 7u);
    EXPECT_EQ(out.request_id(), 42u);
}

TEST(Serialization, FetchResponseRoundTrip)
{
    proto::FetchResponse in;
    in.set_partition_id(2);
    in.set_client_id(7);
    in.set_request_id(42);
    in.set_key("k1");
    in.set_command("v1");
    in.set_send_time(1000);
    in.set_type(proto::BLIND_WRITE);

    std::string bytes = in.SerializeAsString();
    proto::FetchResponse out;
    ASSERT_TRUE(out.ParseFromString(bytes));

    EXPECT_EQ(out.partition_id(), 2u);
    EXPECT_EQ(out.client_id(), 7u);
    EXPECT_EQ(out.request_id(), 42u);
    EXPECT_EQ(out.key(), "k1");
    EXPECT_EQ(out.command(), "v1");
    EXPECT_EQ(out.send_time(), 1000u);
    EXPECT_EQ(out.type(), proto::BLIND_WRITE);
}

// ---------------------------------------------------------------------------
// §3.2-3.3 per-partition state block — initial values.
// ---------------------------------------------------------------------------

TEST(PartitionState, InitialValues)
{
    PartitionState st;

    EXPECT_EQ(st.view_id, 0u);
    EXPECT_EQ(st.deadline_counter, 0u);
    EXPECT_EQ(st.sync_point, 0u);
    EXPECT_EQ(st.commit_point, 0u);

    EXPECT_TRUE(st.early_buffer.empty());
    EXPECT_TRUE(st.late_buffer.empty());
    EXPECT_TRUE(st.synced_log.empty());
    EXPECT_TRUE(st.unsynced_log.empty());
}

// ---------------------------------------------------------------------------
// §3 invariant 1 — early-buffer ordering is (deadline, client_id, request_id),
// identical across replicas. The release order must be deterministic on ties.
// ---------------------------------------------------------------------------

static LogEntry MakeEntry(uint64_t deadline, uint64_t client, uint64_t req)
{
    LogEntry e;
    e.deadline = deadline;
    e.client_id = client;
    e.request_id = req;
    return e;
}

TEST(EarlyBuffer, ReleasesInDeadlineOrder)
{
    EarlyBuffer pq;
    pq.push(MakeEntry(300, 1, 1));
    pq.push(MakeEntry(100, 1, 1));
    pq.push(MakeEntry(200, 1, 1));

    EXPECT_EQ(pq.top().deadline, 100u); pq.pop();
    EXPECT_EQ(pq.top().deadline, 200u); pq.pop();
    EXPECT_EQ(pq.top().deadline, 300u); pq.pop();
    EXPECT_TRUE(pq.empty());
}

TEST(EarlyBuffer, TieBreaksByClientThenRequest)
{
    EarlyBuffer pq;
    // All equal deadline; insert in a scrambled order.
    pq.push(MakeEntry(100, 2, 5));
    pq.push(MakeEntry(100, 1, 9));
    pq.push(MakeEntry(100, 1, 4));
    pq.push(MakeEntry(100, 2, 1));

    // Expected order: client_id asc, then request_id asc.
    auto e0 = pq.top(); pq.pop();
    EXPECT_EQ(e0.client_id, 1u); EXPECT_EQ(e0.request_id, 4u);
    auto e1 = pq.top(); pq.pop();
    EXPECT_EQ(e1.client_id, 1u); EXPECT_EQ(e1.request_id, 9u);
    auto e2 = pq.top(); pq.pop();
    EXPECT_EQ(e2.client_id, 2u); EXPECT_EQ(e2.request_id, 1u);
    auto e3 = pq.top(); pq.pop();
    EXPECT_EQ(e3.client_id, 2u); EXPECT_EQ(e3.request_id, 5u);
}

// ---------------------------------------------------------------------------
// Clock seam — MockClock advances only when told (Stage 3 release-loop testing).
// ---------------------------------------------------------------------------

TEST(Clock, MockClockIsControllable)
{
    MockClock clk(1000);
    EXPECT_EQ(clk.Now(), 1000u);
    clk.Advance(250);
    EXPECT_EQ(clk.Now(), 1250u);
    clk.Set(42);
    EXPECT_EQ(clk.Now(), 42u);
}

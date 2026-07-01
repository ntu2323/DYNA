// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/tests/dedup_flow_test.cc
//   At-most-once request handling (spec §1.6, §6.1) under client retransmission
//   (spec §5): a new request is admitted, an in-flight duplicate is dropped, and
//   an already-answered request is resent from cache.

#include "dyna_new/request_dedup.h"
#include "dyna_new/dyna-new-proto.pb.h"

#include <gtest/gtest.h>

using namespace specpaxos::dyna_new;
namespace proto = specpaxos::dyna_new::proto;

using D = RequestDedup::Decision;

// First sight of a request is admitted to the orderer.
TEST(RequestDedup, NewRequestSubmits)
{
    RequestDedup d;
    ClientAndReqId k{7, 1};
    EXPECT_EQ(d.OnReceive(k), D::kSubmit);
    EXPECT_EQ(d.inflight_size(), 1u);
}

// A retry that arrives while the original is still in flight is dropped, so the
// orderer never holds two entries for the same request.
TEST(RequestDedup, InflightDuplicateDrops)
{
    RequestDedup d;
    ClientAndReqId k{7, 1};
    ASSERT_EQ(d.OnReceive(k), D::kSubmit);
    EXPECT_EQ(d.OnReceive(k), D::kDrop);
    EXPECT_EQ(d.OnReceive(k), D::kDrop);
    EXPECT_EQ(d.inflight_size(), 1u);
    EXPECT_EQ(d.cached_size(), 0u);
}

// After we reply, the request leaves the in-flight set and is cached; a later
// retry is answered from the cache (resend) without re-execution.
TEST(RequestDedup, AnsweredRequestResendsFromCache)
{
    RequestDedup d;
    ClientAndReqId k{7, 1};
    ASSERT_EQ(d.OnReceive(k), D::kSubmit);

    proto::Reply r;
    r.set_client_id(7);
    r.set_request_id(1);
    r.set_deadline(1234);
    d.OnReply(k, r);

    EXPECT_EQ(d.inflight_size(), 0u);
    EXPECT_EQ(d.cached_size(), 1u);

    EXPECT_EQ(d.OnReceive(k), D::kResend);
    const proto::Reply *cached = d.CachedReply(k);
    ASSERT_NE(cached, nullptr);
    EXPECT_EQ(cached->deadline(), 1234u);
}

// A corrected reply (§6.4) overwrites the cached one, so a later retry sees the
// leader's authoritative deadline.
TEST(RequestDedup, CorrectedReplyOverwritesCache)
{
    RequestDedup d;
    ClientAndReqId k{7, 1};
    ASSERT_EQ(d.OnReceive(k), D::kSubmit);

    proto::Reply speculative;
    speculative.set_deadline(100);
    d.OnReply(k, speculative);

    proto::Reply corrected;
    corrected.set_deadline(200);
    d.OnReply(k, corrected);

    ASSERT_EQ(d.OnReceive(k), D::kResend);
    EXPECT_EQ(d.CachedReply(k)->deadline(), 200u);
}

// Distinct requests are tracked independently.
TEST(RequestDedup, DistinctKeysIndependent)
{
    RequestDedup d;
    ClientAndReqId a{7, 1};
    ClientAndReqId b{7, 2};
    EXPECT_EQ(d.OnReceive(a), D::kSubmit);
    EXPECT_EQ(d.OnReceive(b), D::kSubmit);
    EXPECT_EQ(d.OnReceive(a), D::kDrop);
    EXPECT_EQ(d.OnReceive(b), D::kDrop);
}

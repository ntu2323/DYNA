// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/tests/types_test.cc
//   Stage 1 acceptance tests: OrderKey lexicographic ordering + global uniqueness
//   (spec §1.1, the basis of the total-order / Invariant-1 proof), protobuf
//   round-trips for the wire messages (§2), and TxnEntry Key()/Order() derivation.

#include "dyna/bg_outbox.h"
#include "dyna/dyna-proto.pb.h"
#include "dyna/txn_types.h"
#include "dyna/wall_clock.h"

#include <gtest/gtest.h>

#include <set>

using namespace specpaxos::dyna;
namespace proto = specpaxos::dyna::proto;

// ---------------------------------------------------------------------------
// §1.1 OrderKey — (t, client_id, request_id) compared lexicographically.
// ---------------------------------------------------------------------------

TEST(OrderKey, LexicographicByTimestampFirst)
{
    OrderKey a{100, 9, 9};
    OrderKey b{200, 1, 1};
    EXPECT_LT(a, b);                 // smaller t wins regardless of id
    EXPECT_GT(b, a);
    EXPECT_NE(a, b);
}

TEST(OrderKey, TieBreaksByClientThenRequest)
{
    OrderKey base{100, 2, 5};
    EXPECT_LT((OrderKey{100, 1, 9}), base);   // client 1 < client 2
    EXPECT_LT((OrderKey{100, 2, 1}), base);   // same client, request 1 < 5
    EXPECT_GT((OrderKey{100, 2, 6}), base);   // same client, request 6 > 5
}

TEST(OrderKey, EqualityAndComparators)
{
    OrderKey a{100, 1, 1};
    OrderKey b{100, 1, 1};
    EXPECT_EQ(a, b);
    EXPECT_LE(a, b);
    EXPECT_GE(a, b);
    EXPECT_FALSE(a < b);
    EXPECT_FALSE(a > b);
}

// Re-timestamp changes only `t`; (client_id, request_id) are preserved, so two
// DISTINCT transactions can never collide on an OrderKey even at the same `t`.
TEST(OrderKey, GloballyUniqueAcrossDistinctTxns)
{
    std::set<std::pair<std::pair<uint64_t, uint64_t>, uint64_t>> seen;
    // Many distinct (client, request) at an identical timestamp t = 500.
    for (uint64_t c = 0; c < 20; ++c) {
        for (uint64_t r = 0; r < 20; ++r) {
            OrderKey k = MakeOrderKey(500, MakeTxnId(c, r));
            auto tag = std::make_pair(std::make_pair(k.client_id, k.request_id), k.t);
            EXPECT_TRUE(seen.insert(tag).second) << "duplicate OrderKey";
        }
    }
    EXPECT_EQ(seen.size(), 400u);
}

TEST(OrderKey, BumpPreservesIdentity)
{
    TxnId id = MakeTxnId(7, 42);
    OrderKey before = MakeOrderKey(100, id);
    OrderKey after  = MakeOrderKey(999, id);   // late-leader bump: only t changes
    EXPECT_GT(after, before);
    EXPECT_EQ(after.client_id, before.client_id);
    EXPECT_EQ(after.request_id, before.request_id);
}

// ---------------------------------------------------------------------------
// §1.3 TxnEntry — Key()/Order() derive from the body + live t.
// ---------------------------------------------------------------------------

TEST(TxnEntry, KeyAndOrderDeriveFromBody)
{
    TxnEntry e;
    e.txn.set_client_id(7);
    e.txn.set_request_id(42);
    e.txn.set_type(proto::WRITE);
    e.txn.set_t(100);
    e.txn.set_view_id(0);
    e.t = 250;   // live t differs from the wire t after a bump

    EXPECT_EQ(e.Key().clientid, 7u);
    EXPECT_EQ(e.Key().clientreqid, 42u);
    OrderKey k = e.Order();
    EXPECT_EQ(k.t, 250u);            // Order() uses the LIVE t, not txn.t()
    EXPECT_EQ(k.client_id, 7u);
    EXPECT_EQ(k.request_id, 42u);
}

// ---------------------------------------------------------------------------
// §2 message formats — serialize -> deserialize -> assert field equality.
// ---------------------------------------------------------------------------

TEST(Serialization, ClientRequestRoundTrip)
{
    proto::ClientRequest in;
    proto::Txn *t = in.mutable_txn();
    t->set_client_id(7);
    t->set_request_id(42);
    t->set_type(proto::WRITE);
    t->add_r_set("rk1");
    proto::KV *kv = t->add_w_set();
    kv->set_key("wk1");
    kv->set_value("v1");
    t->set_t(1500);
    t->set_view_id(3);

    std::string bytes = in.SerializeAsString();
    proto::ClientRequest out;
    ASSERT_TRUE(out.ParseFromString(bytes));

    EXPECT_EQ(out.txn().client_id(), 7u);
    EXPECT_EQ(out.txn().request_id(), 42u);
    EXPECT_EQ(out.txn().type(), proto::WRITE);
    ASSERT_EQ(out.txn().r_set_size(), 1);
    EXPECT_EQ(out.txn().r_set(0), "rk1");
    ASSERT_EQ(out.txn().w_set_size(), 1);
    EXPECT_EQ(out.txn().w_set(0).key(), "wk1");
    EXPECT_EQ(out.txn().w_set(0).value(), "v1");
    EXPECT_EQ(out.txn().t(), 1500u);
    EXPECT_EQ(out.txn().view_id(), 3u);
}

TEST(Serialization, SpecReplyRoundTrip)
{
    proto::SpecReply in;
    in.set_client_id(7);
    in.set_request_id(42);
    in.set_view_id(1);
    in.set_server_id(2);
    in.set_t(1600);
    in.set_is_leader(true);
    proto::KV *kv = in.add_results();
    kv->set_key("k");
    kv->set_value("readval");
    in.set_owd_estimate(250);

    std::string bytes = in.SerializeAsString();
    proto::SpecReply out;
    ASSERT_TRUE(out.ParseFromString(bytes));

    EXPECT_EQ(out.client_id(), 7u);
    EXPECT_EQ(out.request_id(), 42u);
    EXPECT_EQ(out.server_id(), 2u);
    EXPECT_EQ(out.t(), 1600u);
    EXPECT_TRUE(out.is_leader());
    ASSERT_EQ(out.results_size(), 1);
    EXPECT_EQ(out.results(0).value(), "readval");
    EXPECT_EQ(out.owd_estimate(), 250u);
}

TEST(Serialization, FastAndSlowCommitRoundTrip)
{
    proto::FastCommit fc;
    fc.set_client_id(7);
    fc.set_request_id(42);
    fc.set_view_id(1);
    fc.set_agreed_t(1700);
    proto::FastCommit fc_out;
    ASSERT_TRUE(fc_out.ParseFromString(fc.SerializeAsString()));
    EXPECT_EQ(fc_out.agreed_t(), 1700u);

    proto::SlowCommit sc;
    sc.set_client_id(7);
    sc.set_request_id(42);
    sc.set_view_id(1);
    sc.set_agreed_t(1800);
    proto::SlowCommit sc_out;
    ASSERT_TRUE(sc_out.ParseFromString(sc.SerializeAsString()));
    EXPECT_EQ(sc_out.agreed_t(), 1800u);
}

TEST(Serialization, ReplicatePrepareOptionalBody)
{
    proto::ReplicatePrepare in;
    in.set_client_id(7);
    in.set_request_id(42);
    in.set_view_id(1);
    in.set_leader_id(0);
    in.set_agreed_t(1900);
    // body omitted (optional)

    proto::ReplicatePrepare out;
    ASSERT_TRUE(out.ParseFromString(in.SerializeAsString()));
    EXPECT_FALSE(out.has_txn());
    EXPECT_EQ(out.agreed_t(), 1900u);

    in.mutable_txn()->set_client_id(7);
    in.mutable_txn()->set_request_id(42);
    in.mutable_txn()->set_type(proto::WRITE);
    in.mutable_txn()->set_t(1900);
    in.mutable_txn()->set_view_id(1);
    proto::ReplicatePrepare out2;
    ASSERT_TRUE(out2.ParseFromString(in.SerializeAsString()));
    EXPECT_TRUE(out2.has_txn());
    EXPECT_EQ(out2.txn().client_id(), 7u);
}

// ---------------------------------------------------------------------------
// Clock seam — MockClock advances only when told (Stage 2 release-loop testing).
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

// ---------------------------------------------------------------------------
// TxnIdHash — must spread ids (the framework's std::hash<ClientAndReqId> returns 0
// for every id, which turned every TxnId-keyed lookup into a linear scan).
// ---------------------------------------------------------------------------

TEST(TxnIdHash, DistinctForSequentialIds)
{
    TxnIdHash h;
    std::set<uint64_t> seen;
    for (uint64_t c = 1; c <= 24; c++) {
        for (uint64_t r = 1; r <= 1000; r++) seen.insert(h(TxnId{c, r}));
    }
    EXPECT_EQ(seen.size(), 24u * 1000u);
    EXPECT_NE(h(TxnId{1, 2}), h(TxnId{2, 1}));   // not symmetric in (client, req)
}

TEST(TxnIdHash, TxnMapLookupsWork)
{
    TxnMap<int> m;
    TxnSet s;
    for (uint64_t r = 1; r <= 5000; r++) {
        m[TxnId{7, r}] = static_cast<int>(r);
        s.insert(TxnId{7, r});
    }
    EXPECT_EQ(m.size(), 5000u);
    EXPECT_EQ(m.at(TxnId{7, 4321}), 4321);
    EXPECT_TRUE(s.contains(TxnId{7, 1}));
    EXPECT_FALSE(s.contains(TxnId{8, 1}));
}

// ---------------------------------------------------------------------------
// BgOutbox — per-destination batching of background messages (bg_outbox.h).
// ---------------------------------------------------------------------------

namespace {
proto::ReplicatePrepareReply Ack(uint64_t req)
{
    proto::ReplicatePrepareReply a;
    a.set_client_id(7);
    a.set_request_id(req);
    a.set_view_id(0);
    a.set_server_id(1);
    return a;
}
} // namespace

TEST(BgOutbox, FullByEntryCountAndTakeEmpties)
{
    BgOutbox box(3, /*max_entries=*/3, /*max_bytes=*/0);
    EXPECT_FALSE(box.AddPrepareReply(1, Ack(1)));
    EXPECT_FALSE(box.AddPrepareReply(1, Ack(2)));
    EXPECT_FALSE(box.AddPrepareReply(2, Ack(3)));   // other destination: own count
    EXPECT_TRUE(box.AddPrepareReply(1, Ack(4)));    // 3rd entry for r1 -> full

    proto::BgBatch b;
    size_t n = 0;
    ASSERT_TRUE(box.Take(1, &b, &n));
    EXPECT_EQ(n, 3u);
    ASSERT_EQ(b.prepare_replies_size(), 3);
    EXPECT_EQ(b.prepare_replies(0).request_id(), 1u);   // per-txn entries kept in order
    EXPECT_EQ(b.prepare_replies(2).request_id(), 4u);
    EXPECT_EQ(box.pending(1), 0u);
    EXPECT_FALSE(box.Take(1, &b));                       // now empty
    EXPECT_EQ(box.pending(2), 1u);
}

TEST(BgOutbox, FullByBytesAndTakeAllMixesKinds)
{
    BgOutbox box(3, /*max_entries=*/1000, /*max_bytes=*/200);
    proto::ReplicateCommit c;
    c.set_client_id(1);
    c.set_request_id(1);
    c.set_view_id(0);
    c.set_leader_id(0);
    EXPECT_FALSE(box.AddCommit(2, c));                   // small: not full
    EXPECT_FALSE(box.AddCommit(0, c));
    EXPECT_FALSE(box.AddCommit(9, c));                   // out of range: ignored

    proto::ReplicatePrepare p;
    p.set_client_id(1);
    p.set_request_id(2);
    p.set_view_id(0);
    p.set_leader_id(0);
    p.set_agreed_t(100);
    proto::KV *kv = p.mutable_txn()->add_w_set();
    kv->set_key(std::string(24, 'k'));
    kv->set_value(std::string(300, 'v'));
    EXPECT_TRUE(box.AddPrepare(2, p));                   // the big body crosses 200 bytes

    auto all = box.TakeAll();
    ASSERT_EQ(all.size(), 2u);
    EXPECT_EQ(all[0].first, 0u);
    EXPECT_EQ(all[0].second.commits_size(), 1);
    EXPECT_EQ(all[1].first, 2u);
    EXPECT_EQ(all[1].second.commits_size(), 1);          // kinds share one batch
    ASSERT_EQ(all[1].second.prepares_size(), 1);
    EXPECT_EQ(all[1].second.prepares(0).txn().w_set(0).value(), std::string(300, 'v'));
    EXPECT_TRUE(box.TakeAll().empty());
}

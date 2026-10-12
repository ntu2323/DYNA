// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/tests/robust_test.cc
//   Regression tests for robustness fixes (MockClock):
//     - no counter-veto: a buffered client txn overtaken by a protocol-decided insert
//       is still released at its deadline (was silently dropped → hung at its leader);
//     - commit adoption never downgrades COMMITTED, re-heapify doesn't corrupt the
//       adopted entry, and an executed+GC'd txn is never re-installed or re-applied;
//     - background messages are retransmitted: a lost TsAgree or ReplicateCommit
//       still converges;
//     - the executor thread services a leader read after a committed write without any
//       periodic tick (event-driven), and the primary replicates immediately.

#include "dyna/in_memory_backend.h"
#include "dyna/server_orderer.h"
#include "dyna/topology.h"
#include "dyna/txn_types.h"
#include "dyna/wall_clock.h"

#include <gtest/gtest.h>

#include <chrono>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace specpaxos::dyna;
namespace proto = specpaxos::dyna::proto;

namespace {

std::string KeyForPartition(uint32_t target, uint32_t n)
{
    for (uint64_t i = 0;; ++i) {
        std::string k = "k" + std::to_string(i);
        if (Topology::PartitionOf(k, n) == target) return k;
    }
}

TxnEntry MakeWrite(uint64_t c, uint64_t r, uint64_t t, const std::string &k1,
                   const std::string &v = "v", const std::string &k2 = "")
{
    TxnEntry e;
    e.txn.set_client_id(c); e.txn.set_request_id(r); e.txn.set_type(proto::WRITE);
    { proto::KV *kv = e.txn.add_w_set(); kv->set_key(k1); kv->set_value(v); }
    if (!k2.empty()) { proto::KV *kv = e.txn.add_w_set(); kv->set_key(k2); kv->set_value(v); }
    e.txn.set_t(t); e.txn.set_view_id(0); e.t = t;
    return e;
}

TxnEntry MakeRead(uint64_t c, uint64_t r, uint64_t t, const std::string &k)
{
    TxnEntry e;
    e.txn.set_client_id(c); e.txn.set_request_id(r); e.txn.set_type(proto::READ);
    e.txn.add_r_set(k);
    e.txn.set_t(t); e.txn.set_view_id(0); e.t = t;
    return e;
}

// Thread-safe reply collector (the executor thread fires callbacks in C1 tests).
struct Collector {
    struct Reply { TxnId id; uint64_t t; std::map<std::string, std::string> results; };
    std::mutex mu;
    std::vector<Reply> replies;
    ServerOrderer::OnSpecReply cb() {
        return [this](const TxnEntry &e) {
            Reply r{e.Key(), e.t, {}};
            for (const auto &kv : e.results) r.results[kv.first] = kv.second;
            std::lock_guard<std::mutex> lk(mu);
            replies.push_back(std::move(r));
        };
    }
    size_t count() { std::lock_guard<std::mutex> lk(mu); return replies.size(); }
    bool has(const TxnId &id, Reply *out = nullptr) {
        std::lock_guard<std::mutex> lk(mu);
        for (const auto &r : replies) if (r.id == id) { if (out) *out = r; return true; }
        return false;
    }
};

Status StatusOf(ServerOrderer &o, const TxnId &id, uint64_t *t = nullptr)
{
    Status s = Status::SPECULATIVE;
    EXPECT_TRUE(o.speclog_entry(id, &s, t));
    return s;
}

template <class Pred>
bool WaitFor(Pred pred, int ms = 3000)
{
    for (int i = 0; i < ms; ++i) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return pred();
}

} // namespace

// --- no counter-veto (B2 / A4) ----------------------------------------------------

// A commit inserts Y at t=150 while X (t=100) still waits in earlyBuffer (this
// server's clock lags). X must still release at its deadline — it used to be vetoed
// and silently dropped, hanging the write at its leader.
TEST(Robust, BufferedTxnOvertakenByCommitStillReleases)
{
    MockClock clk(90);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(1), c.cb());   // server 0 leads everything

    o.SubmitTxn(MakeWrite(1, 1, 100, "a"), false);
    o.SubmitTxn(MakeWrite(1, 2, 150, "b"), false);
    TxnEntry out;
    ASSERT_EQ(o.ApplyCommit(MakeTxnId(1, 2), 150, &out), ServerOrderer::CommitResult::kApplied);
    EXPECT_EQ(o.highest_released_t().t, 150u);

    clk.Set(100);
    EXPECT_EQ(o.ReleaseDue(), 1u);
    Collector::Reply r;
    ASSERT_TRUE(c.has(MakeTxnId(1, 1), &r));
    EXPECT_EQ(r.t, 100u);
    EXPECT_EQ(StatusOf(o, MakeTxnId(1, 1)), Status::SPECULATIVE);
    EXPECT_EQ(o.highest_released_t().t, 150u);   // never lowered
}

// --- commit adoption safety (B3) --------------------------------------------------

// A slow-path agreedT moves the entry within the heap. The adopted entry (and only it)
// must become COMMITTING, and *out must describe it — not whichever entry the stale
// pointer landed on after re-heapify.
TEST(Robust, ApplyCommitReheapAdoptsTheRightEntry)
{
    MockClock clk(50);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(1), c.cb());
    o.SubmitTxn(MakeWrite(1, 1, 100, "a"), false);
    o.SubmitTxn(MakeWrite(1, 2, 200, "b"), false);
    o.SubmitTxn(MakeWrite(1, 3, 300, "c"), false);
    clk.Set(300);
    ASSERT_EQ(o.ReleaseDue(), 3u);

    TxnEntry out;
    ASSERT_EQ(o.ApplyCommit(MakeTxnId(1, 1), 400, &out), ServerOrderer::CommitResult::kApplied);
    EXPECT_EQ(out.Key(), MakeTxnId(1, 1));
    EXPECT_EQ(out.t, 400u);
    uint64_t t = 0;
    EXPECT_EQ(StatusOf(o, MakeTxnId(1, 1), &t), Status::COMMITTING);
    EXPECT_EQ(t, 400u);
    EXPECT_EQ(StatusOf(o, MakeTxnId(1, 2)), Status::SPECULATIVE);
    EXPECT_EQ(StatusOf(o, MakeTxnId(1, 3)), Status::SPECULATIVE);
}

// A re-sent SlowCommit after replication finished must not downgrade COMMITTED.
TEST(Robust, ApplyCommitNeverDowngradesCommitted)
{
    MockClock clk(50);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(1), c.cb());
    const TxnId id = MakeTxnId(1, 1);
    o.SubmitTxn(MakeWrite(1, 1, 100, "a"), false);
    clk.Set(100);
    ASSERT_EQ(o.ReleaseDue(), 1u);
    TxnEntry out;
    ASSERT_EQ(o.ApplyCommit(id, 100, &out), ServerOrderer::CommitResult::kApplied);
    o.MarkCommitted(id);

    ASSERT_EQ(o.ApplyCommit(id, 100, &out), ServerOrderer::CommitResult::kApplied);
    EXPECT_EQ(out.status, Status::COMMITTED);
    EXPECT_EQ(StatusOf(o, id), Status::COMMITTED);
}

// After W1 executes (and is GC'd) and a newer W2 overwrites the key, no late message
// for W1 — commit, fetched body, prepare, or client retry — may re-install or
// re-apply it.
TEST(Robust, ExecutedTxnIsNeverReinstalledOrReapplied)
{
    InMemoryBackend backend;
    MockClock clk(50);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(1), c.cb(), &backend);
    const TxnId w1 = MakeTxnId(1, 1), w2 = MakeTxnId(1, 2);

    o.SubmitTxn(MakeWrite(1, 1, 100, "K", "v1"), false);
    o.SubmitTxn(MakeWrite(1, 2, 200, "K", "v2"), false);
    clk.Set(200);
    ASSERT_EQ(o.ReleaseDue(), 2u);
    o.MarkCommitted(w1);
    o.MarkCommitted(w2);
    ASSERT_EQ(o.ExecuteReady().size(), 2u);
    ASSERT_EQ(backend.mem_read("K").value_or(""), "v2");
    ASSERT_TRUE(o.executed(w1));

    TxnEntry out;
    EXPECT_EQ(o.ApplyCommit(w1, 100, &out), ServerOrderer::CommitResult::kAlreadyExecuted);
    EXPECT_EQ(out.t, 100u);                       // CommitReply can carry the final t
    EXPECT_EQ(out.Key(), w1);
    o.InstallFetched(MakeWrite(1, 1, 100, "K", "v1").txn);
    EXPECT_TRUE(o.HandleReplicatePrepare(w1, 100, MakeWrite(1, 1, 100, "K", "v1").txn));
    o.SubmitTxn(MakeWrite(1, 1, 100, "K", "v1"), false);   // delayed client retry

    EXPECT_EQ(o.late_size(), 0u);
    EXPECT_EQ(o.early_size(), 0u);
    EXPECT_FALSE(o.speclog_contains(w1));
    EXPECT_TRUE(o.ExecuteReady().empty());
    EXPECT_EQ(backend.mem_read("K").value_or(""), "v2");
}

// --- retransmission (B4) ----------------------------------------------------------

// Both leaders' first TsAgree messages are lost. Leader 1 keeps re-sending on each
// scan; leader 0 (already complete) answers with a reply; replies aren't answered.
TEST(Robust, LostTsAgreeStillConverges)
{
    const std::string kA = KeyForPartition(0, 3), kB = KeyForPartition(1, 3);
    const TxnId id = MakeTxnId(1, 1);
    MockClock c0(50), c1(200);
    Collector col0, col1;
    ServerOrderer o0(0, 0, &c0, Topology(3), col0.cb());
    ServerOrderer o1(1, 0, &c1, Topology(3), col1.cb());

    o0.SubmitTxn(MakeWrite(1, 1, 100, kA, "v", kB), false);
    c0.Set(100);
    ASSERT_EQ(o0.ReleaseDue(), 1u);                         // leader 0 at t=100
    o1.SubmitTxn(MakeWrite(1, 1, 100, kA, "v", kB), false); // late → leader 1 at t=200

    std::vector<ServerOrderer::TsAgreeAction> a0, a1;
    std::vector<TxnEntry> d0, d1;
    o0.DriveTsAgreement(0, &a0, &d0);                       // 0 → 1 : LOST
    ASSERT_EQ(a0.size(), 1u);
    o1.DriveTsAgreement(0, &a1, &d1);
    ASSERT_EQ(a1.size(), 1u);
    auto r0 = o0.RecordTsProposal(id, 1, a1[0].proposed_t, a1[0].is_reply);
    EXPECT_TRUE(r0.transitioned);                           // 0 has both → COMMITTING@200
    ASSERT_TRUE(r0.has_action);                             // 0's reply to 1 : LOST
    EXPECT_TRUE(r0.action.is_reply);

    // Next scan: leader 1 still lacks 0's proposal → re-sends.
    a1.clear();
    o1.DriveTsAgreement(0, &a1, &d1);
    ASSERT_EQ(a1.size(), 1u);
    EXPECT_EQ(a1[0].to_leaders.count(0), 1u);
    auto r0b = o0.RecordTsProposal(id, 1, a1[0].proposed_t, a1[0].is_reply);
    ASSERT_TRUE(r0b.has_action);                            // answered even though complete
    auto r1 = o1.RecordTsProposal(id, 0, r0b.action.proposed_t, r0b.action.is_reply);
    EXPECT_TRUE(r1.transitioned);
    EXPECT_FALSE(r1.has_action);                            // a reply is never answered

    uint64_t t0 = 0, t1 = 0;
    EXPECT_EQ(StatusOf(o0, id, &t0), Status::COMMITTING);
    EXPECT_EQ(StatusOf(o1, id, &t1), Status::COMMITTING);
    EXPECT_EQ(t0, 200u);
    EXPECT_EQ(t1, 200u);
}

// Leader 0 already adopted the client's commit (COMMITTING@200); leader 1 missed it
// and is stuck SPECULATIVE. Its TsAgree is answered with 0's agreed t → converges.
TEST(Robust, StuckLeaderConvergesWithPeerAlreadyCommitted)
{
    const std::string kA = KeyForPartition(0, 3), kB = KeyForPartition(1, 3);
    const TxnId id = MakeTxnId(1, 1);
    MockClock c0(50), c1(50);
    Collector col0, col1;
    ServerOrderer o0(0, 0, &c0, Topology(3), col0.cb());
    ServerOrderer o1(1, 0, &c1, Topology(3), col1.cb());

    for (auto *p : {&o0, &o1}) p->SubmitTxn(MakeWrite(1, 1, 100, kA, "v", kB), false);
    c0.Set(100); c1.Set(100);
    ASSERT_EQ(o0.ReleaseDue(), 1u);
    ASSERT_EQ(o1.ReleaseDue(), 1u);
    TxnEntry out;
    ASSERT_EQ(o0.ApplyCommit(id, 100, &out), ServerOrderer::CommitResult::kApplied);

    std::vector<ServerOrderer::TsAgreeAction> a1;
    std::vector<TxnEntry> d1;
    o1.DriveTsAgreement(0, &a1, &d1);
    ASSERT_EQ(a1.size(), 1u);
    auto r0 = o0.RecordTsProposal(id, 1, a1[0].proposed_t, a1[0].is_reply);
    EXPECT_FALSE(r0.transitioned);                          // already COMMITTING
    ASSERT_TRUE(r0.has_action);
    EXPECT_EQ(r0.action.proposed_t, 100u);
    auto r1 = o1.RecordTsProposal(id, 0, r0.action.proposed_t, r0.action.is_reply);
    EXPECT_TRUE(r1.transitioned);
    uint64_t t1 = 0;
    EXPECT_EQ(StatusOf(o1, id, &t1), Status::COMMITTING);
    EXPECT_EQ(t1, 100u);
}

// The primary's ReplicateCommit to a follower is lost. After the timeout the follower
// re-acks; the primary (COMMITTED, and later executed) answers with ReplicateCommit.
// The first prepare goes out immediately via TakeReplicateNow (once, primary only).
TEST(Robust, LostReplicateCommitRecoveredByReack)
{
    const std::string kA = KeyForPartition(0, 3);           // primary = server 0
    const TxnId id = MakeTxnId(1, 1);
    MockClock c0(50), c1(50);
    Collector col0, col1;
    ServerOrderer o0(0, 0, &c0, Topology(3), col0.cb());
    ServerOrderer o1(1, 0, &c1, Topology(3), col1.cb());

    o0.SubmitTxn(MakeWrite(1, 1, 100, kA), false);
    c0.Set(100);
    ASSERT_EQ(o0.ReleaseDue(), 1u);
    TxnEntry out;
    ASSERT_EQ(o0.ApplyCommit(id, 100, &out), ServerOrderer::CommitResult::kApplied);

    ServerOrderer::ReplicateAction prep;
    ASSERT_TRUE(o0.TakeReplicateNow(id, &prep));             // immediate first prepare
    EXPECT_FALSE(o0.TakeReplicateNow(id, &prep));            // only once
    ASSERT_TRUE(o1.HandleReplicatePrepare(id, prep.agreed_t, prep.body));
    EXPECT_FALSE(o1.TakeReplicateNow(id, &prep));            // follower never drives
    ASSERT_EQ(o0.RecordPrepareReply(id, 1), ServerOrderer::PrepareReplyResult::kNowCommitted);
    // ... ReplicateCommit to server 1 is LOST.

    std::vector<ServerOrderer::ReackAction> reacks;
    c1.Set(100 + 500);
    o1.CollectStaleCommitting(/*timeout_us=*/1000, &reacks);
    EXPECT_TRUE(reacks.empty());                             // not stale yet
    c1.Set(100 + 2000);
    o1.CollectStaleCommitting(1000, &reacks);
    ASSERT_EQ(reacks.size(), 1u);
    EXPECT_EQ(reacks[0].primary, 0u);
    reacks.clear();
    o1.CollectStaleCommitting(1000, &reacks);
    EXPECT_TRUE(reacks.empty());                             // throttled

    EXPECT_EQ(o0.RecordPrepareReply(id, 1), ServerOrderer::PrepareReplyResult::kResendCommit);
    o0.ExecuteReady();                                       // primary executes + GCs
    ASSERT_TRUE(o0.executed(id));
    EXPECT_EQ(o0.RecordPrepareReply(id, 1), ServerOrderer::PrepareReplyResult::kResendCommit);

    o1.MarkCommitted(id);                                    // the re-sent ReplicateCommit
    EXPECT_EQ(StatusOf(o1, id), Status::COMMITTED);
}

// --- event-driven execution (C1) --------------------------------------------------

// With the release + executor threads running, a committed write is applied and a
// later leader read is serviced with its value — no ExecuteReady() call, no tick.
TEST(Robust, ExecutorServicesReadAfterCommitWithoutTick)
{
    InMemoryBackend backend;
    MockClock clk(50);
    Collector c;
    ServerOrderer o(0, 0, &clk, Topology(1), c.cb(), &backend);
    o.Start();

    const TxnId w = MakeTxnId(1, 1), r = MakeTxnId(1, 2);
    o.SubmitTxn(MakeWrite(1, 1, 100, "K", "v1"));
    clk.Set(100);
    ASSERT_TRUE(WaitFor([&] { return c.has(w); }));         // write released + replied
    TxnEntry out;
    ASSERT_EQ(o.ApplyCommit(w, 100, &out), ServerOrderer::CommitResult::kApplied);
    o.MarkCommitted(w);                                      // kicks the executor
    ASSERT_TRUE(WaitFor([&] { return backend.mem_read("K").value_or("") == "v1"; }));

    o.SubmitTxn(MakeRead(1, 2, 200, "K"));
    clk.Set(200);                                            // release → kick → serve
    Collector::Reply rr;
    ASSERT_TRUE(WaitFor([&] { return c.has(r, &rr); }));
    EXPECT_EQ(rr.results["K"], "v1");
    o.Stop();
}

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/tests/recovery_test.cc
//   Partition-leader recovery (design doc § Recovery), MockClock-driven:
//     - consistent hashing: survivors keep their ranges, the failed range is split;
//     - the M merge rule (COMMITTED > COMMITTING > SPECULATIVE, max t; kv max t);
//     - end-to-end over orderers: a fast-committed write whose leader died survives at
//       its t and is leader-synced to COMMITTING by the new PL; a slow-committed write
//       survives at agreedT; executed key-values reach a server that lacked them;
//     - install: inherited SPEC writes the PL now leads are leader-synced immediately;
//       t-guarded store apply, leftovers kept + counted, a recovered value is
//       not overwritten by an older execution, a resend after install isn't duplicated;
//     - the VR layer over a fake (lossy) network: survivors converge on the new view with
//       every PL's M, whether a follower or the VR leader died; a fenced node is excluded.

#include "dyna/in_memory_backend.h"
#include "dyna/recovery.h"
#include "dyna/server_orderer.h"
#include "dyna/topology.h"
#include "dyna/txn_types.h"
#include "dyna/view_coordinator.h"
#include "dyna/wall_clock.h"

#include <gtest/gtest.h>

#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <vector>

using namespace specpaxos::dyna;
namespace proto = specpaxos::dyna::proto;

namespace {

constexpr uint32_t kN = 3;
constexpr uint64_t kAll3 = 0b111;

std::string KeyForPartition(uint32_t target, uint32_t n, int skip = 0)
{
    for (uint64_t i = 0;; ++i) {
        std::string k = "k" + std::to_string(i);
        if (Topology::PartitionOf(k, n) == target && skip-- == 0) return k;
    }
}

TxnEntry MakeWrite(uint64_t c, uint64_t r, uint64_t t, const std::string &k,
                   const std::string &v = "v")
{
    TxnEntry e;
    e.txn.set_client_id(c); e.txn.set_request_id(r); e.txn.set_type(proto::WRITE);
    proto::KV *kv = e.txn.add_w_set(); kv->set_key(k); kv->set_value(v);
    e.txn.set_t(t); e.txn.set_view_id(0); e.t = t;
    return e;
}

struct Collector {
    std::mutex mu;
    std::vector<std::pair<TxnId, uint64_t>> replies;
    ServerOrderer::OnSpecReply cb() {
        return [this](const TxnEntry &e) {
            std::lock_guard<std::mutex> lk(mu);
            replies.emplace_back(e.Key(), e.t);
        };
    }
};

// Three servers' orderers on one MockClock, server 1 about to fail.
struct Cluster {
    MockClock clock{0};
    InMemoryBackend store[kN];
    Collector col[kN];
    std::unique_ptr<ServerOrderer> o[kN];
    Topology old_topo{kN};
    Topology new_topo = Topology(kN).WithAlive(0b101);   // server 1 died

    Cluster() {
        for (uint32_t s = 0; s < kN; ++s) {
            o[s] = std::make_unique<ServerOrderer>(s, 0, &clock, Topology(kN), col[s].cb(),
                                                   &store[s]);
        }
    }
    // Release `e` (on time) at the given servers.
    void Release(const TxnEntry &e, std::initializer_list<uint32_t> at) {
        clock.Set(e.t - 10);
        for (uint32_t s : at) o[s]->SubmitTxn(e, false);
        clock.Set(e.t);
        for (uint32_t s : at) o[s]->ReleaseDue();
    }
    // Recovery among the survivors {0, 2}: each PL merges its own + the other's data,
    // then both install every M.
    std::vector<ServerOrderer::InstallStats> Recover() {
        std::vector<RecoveryData> ms;
        for (uint32_t pl : {0u, 2u}) {
            auto in_delta = [&](const std::string &k) {
                return InDeltaP(old_topo, new_topo, k, pl);
            };
            std::vector<RecoveryData> replies = {o[0]->CollectRecovery(in_delta),
                                                 o[2]->CollectRecovery(in_delta)};
            ms.push_back(MergeRecovery(replies));
        }
        auto failed = [&](const std::string &k) {
            return InFailedRange(old_topo, new_topo, k);
        };
        std::vector<ServerOrderer::InstallStats> st;
        for (uint32_t s : {0u, 2u}) st.push_back(o[s]->InstallView(new_topo, 1, ms, failed));
        return st;
    }
};

} // namespace

// ---------------------------------------------------------------------------------------
// Topology + merge
// ---------------------------------------------------------------------------------------

TEST(RecoveryTopology, SurvivorsKeepRangesFailedRangeSplit)
{
    const Topology all(kN);
    const Topology t = all.WithAlive(0b101);
    std::map<uint32_t, int> took;
    for (int i = 0; i < 3000; ++i) {
        const std::string k = "key" + std::to_string(i);
        const uint32_t home = all.ownerOf(k);
        EXPECT_EQ(home, Topology::PartitionOf(k, kN));
        if (home != 1) {
            EXPECT_EQ(t.ownerOf(k), home);   // survivors keep their range
        } else {
            const uint32_t s = t.ownerOf(k);
            EXPECT_TRUE(s == 0 || s == 2);
            took[s]++;
            EXPECT_TRUE(InFailedRange(all, t, k));
            EXPECT_TRUE(InDeltaP(all, t, k, s));
        }
    }
    EXPECT_GT(took[0], 100);   // split between both survivors
    EXPECT_GT(took[2], 100);
}

TEST(RecoveryMerge, PrecedenceThenMaxT)
{
    auto rt = [](uint64_t r, uint64_t t, Status s) {
        RecoveredTxn x;
        x.txn.set_client_id(1); x.txn.set_request_id(r); x.txn.set_type(proto::WRITE);
        x.txn.set_t(t); x.txn.set_view_id(0);
        x.t = t; x.status = s;
        return x;
    };
    RecoveryData a, b;
    a.txns = {rt(1, 100, Status::SPECULATIVE), rt(2, 100, Status::SPECULATIVE),
              rt(3, 100, Status::COMMITTED)};
    b.txns = {rt(1, 150, Status::COMMITTING), rt(2, 120, Status::SPECULATIVE),
              rt(3, 300, Status::SPECULATIVE)};
    a.kvs = {RecoveredKV{"x", "old", 5}, RecoveredKV{"y", "y1", 9}};
    b.kvs = {RecoveredKV{"x", "new", 7}};
    RecoveryData m = MergeRecovery({a, b});
    ASSERT_EQ(m.txns.size(), 3u);
    std::map<uint64_t, RecoveredTxn> by;
    for (const auto &x : m.txns) by[x.txn.request_id()] = x;
    EXPECT_EQ(by[1].status, Status::COMMITTING); EXPECT_EQ(by[1].t, 150u);
    EXPECT_EQ(by[2].status, Status::SPECULATIVE); EXPECT_EQ(by[2].t, 120u);
    EXPECT_EQ(by[3].status, Status::COMMITTED);  EXPECT_EQ(by[3].t, 100u);
    ASSERT_EQ(m.kvs.size(), 2u);
    EXPECT_EQ(m.kvs[0].key, "x"); EXPECT_EQ(m.kvs[0].value, "new"); EXPECT_EQ(m.kvs[0].t, 7u);

    // Proto round trip.
    proto::RecoveryPrepare p;
    ToProto(m, p.mutable_txns(), p.mutable_kvs());
    RecoveryData back = FromProto(p.txns(), p.kvs());
    ASSERT_EQ(back.txns.size(), 3u);
    ASSERT_EQ(back.kvs.size(), 2u);
    EXPECT_EQ(back.txns[0].status, m.txns[0].status);
}

// ---------------------------------------------------------------------------------------
// Orderer-level recovery
// ---------------------------------------------------------------------------------------

// A key of the failed partition whose new owner is `pl`.
std::string FailedKeyOwnedBy(uint32_t pl)
{
    const Topology t = Topology(kN).WithAlive(0b101);
    for (int skip = 0;; ++skip) {
        std::string k = KeyForPartition(1, kN, skip);
        if (t.ownerOf(k) == pl) return k;
    }
}

// Fast path, leader dead: L1 + one follower endorsed at t (the client fast-committed);
// the other follower never got it. Recovery keeps it at t, and the new PL's leader sync
// commits it at that t. Run with the endorser being the new PL itself (its existing
// follower copy must become a leader copy) and being the other survivor.
class FastCommittedLeaderDead : public ::testing::TestWithParam<bool> {};

TEST_P(FastCommittedLeaderDead, SurvivesAtT)
{
    Cluster c;
    const bool endorser_is_pl = GetParam();
    const uint32_t pl = 2;
    const std::string k = FailedKeyOwnedBy(pl);
    const TxnEntry w = MakeWrite(7, 1, 1000, k);
    c.Release(w, {1u, endorser_is_pl ? pl : 0u});

    auto st = c.Recover();
    EXPECT_EQ(st[0].leftovers + st[1].leftovers, 0u);

    for (uint32_t s : {0u, 2u}) {
        Status status; uint64_t t = 0;
        ASSERT_TRUE(c.o[s]->speclog_entry(w.Key(), &status, &t)) << "server " << s;
        EXPECT_EQ(status, Status::SPECULATIVE);
        EXPECT_EQ(t, 1000u);
    }
    // Leader sync at the new PL (single involved leader → its own t).
    c.clock.Set(1000 + 200000);
    std::vector<ServerOrderer::TsAgreeAction> actions;
    std::vector<TxnEntry> committed;
    c.o[pl]->DriveTsAgreement(100000, &actions, &committed);
    ASSERT_EQ(committed.size(), 1u);
    EXPECT_EQ(committed[0].t, 1000u);
    EXPECT_TRUE(committed[0].is_leader_for_this);
    ServerOrderer::ReplicateAction a;
    EXPECT_TRUE(c.o[pl]->TakeReplicateNow(w.Key(), &a));   // new primary replicates
}

INSTANTIATE_TEST_SUITE_P(RecoveryOrderer, FastCommittedLeaderDead, ::testing::Bool());

// Inherited SPECULATIVE writes are leader-synced right after install, not after the SPEC
// age timeout: the client of a fast-committed txn won't send another commit (design doc
// § Recovery cases 2/3). Single involved leader → completes locally at its t. Only the
// new PL marks it; a write released after install still waits out the timeout.
TEST(RecoveryOrderer, InheritedSpecSyncedImmediately)
{
    Cluster c;
    const uint32_t pl = 2;
    const std::string k = FailedKeyOwnedBy(pl);
    const TxnEntry w = MakeWrite(7, 1, 1000, k);
    c.Release(w, {0u, 1u, 2u});

    auto st = c.Recover();
    EXPECT_EQ(st[1].spec_to_sync, 1u);   // server 2 (the PL)
    EXPECT_EQ(st[0].spec_to_sync, 0u);   // server 0 is a follower for k

    // No time passes: still well inside the 100 ms SPEC age timeout.
    std::vector<ServerOrderer::TsAgreeAction> actions;
    std::vector<TxnEntry> committed;
    c.o[pl]->DriveTsAgreement(100000, &actions, &committed);
    ASSERT_EQ(committed.size(), 1u);
    EXPECT_EQ(committed[0].Key(), w.Key());
    EXPECT_EQ(committed[0].t, 1000u);
    EXPECT_EQ(committed[0].status, Status::COMMITTING);
    EXPECT_TRUE(actions.empty());

    committed.clear();
    c.o[0]->DriveTsAgreement(100000, &actions, &committed);   // not a leader of it
    EXPECT_TRUE(committed.empty());

    // A fresh write in view 1, led by the PL: normal timeout applies.
    TxnEntry w2 = MakeWrite(7, 2, 5000, FailedKeyOwnedBy(pl));
    w2.txn.set_view_id(1);
    c.Release(w2, {0u, 2u});
    committed.clear();
    c.o[pl]->DriveTsAgreement(100000, &actions, &committed);
    EXPECT_TRUE(committed.empty());
}

// Multi-leader inherited SPEC txn (failed key now led by 2, plus a key of partition 0):
// both leaders emit TsAgree right after install and converge on the max.
TEST(RecoveryOrderer, InheritedMultiLeaderSpecSyncsImmediately)
{
    Cluster c;
    const std::string kf = FailedKeyOwnedBy(2);
    const std::string k0 = KeyForPartition(0, kN);
    TxnEntry w = MakeWrite(7, 3, 1000, kf);
    proto::KV *kv = w.txn.add_w_set(); kv->set_key(k0); kv->set_value("v0");
    c.Release(w, {0u, 1u, 2u});

    auto st = c.Recover();
    EXPECT_EQ(st[0].spec_to_sync, 1u);
    EXPECT_EQ(st[1].spec_to_sync, 1u);

    std::vector<ServerOrderer::TsAgreeAction> a0, a2;
    std::vector<TxnEntry> d0, d2;
    c.o[0]->DriveTsAgreement(100000, &a0, &d0);
    c.o[2]->DriveTsAgreement(100000, &a2, &d2);
    ASSERT_EQ(a0.size(), 1u);
    ASSERT_EQ(a2.size(), 1u);
    EXPECT_TRUE(d0.empty());
    auto r2 = c.o[2]->RecordTsProposal(w.Key(), 0, a0[0].proposed_t);
    auto r0 = c.o[0]->RecordTsProposal(w.Key(), 2, a2[0].proposed_t);
    EXPECT_TRUE(r2.transitioned);
    EXPECT_TRUE(r0.transitioned);
    EXPECT_EQ(r0.entry.t, 1000u);
    EXPECT_EQ(r2.entry.t, 1000u);
}

// Install never downgrades: a local COMMITTED copy stays COMMITTED at its t even if an M
// carries a SPECULATIVE copy (e.g. this server wasn't among the PL's f+1 responders).
TEST(RecoveryOrderer, InstallNeverDowngrades)
{
    Cluster c;
    const std::string k = KeyForPartition(1, kN);
    const TxnEntry w = MakeWrite(7, 8, 1000, k);
    c.Release(w, {0});
    TxnEntry out;
    c.o[0]->ApplyCommit(w.Key(), 1500, &out);
    c.o[0]->MarkCommitted(w.Key());

    RecoveryData m;
    m.txns.push_back(RecoveredTxn{w.txn, 1000, Status::SPECULATIVE});
    c.o[0]->InstallView(c.new_topo, 1, {m}, [](const std::string &) { return true; });
    Status status; uint64_t t = 0;
    ASSERT_TRUE(c.o[0]->speclog_entry(w.Key(), &status, &t));
    EXPECT_EQ(status, Status::COMMITTED);
    EXPECT_EQ(t, 1500u);
}

// Slow path: f+1 = {1, 0} adopted agreedT; 2 holds the original t. M carries agreedT.
TEST(RecoveryOrderer, SlowCommittedSurvivesAtAgreedT)
{
    Cluster c;
    const std::string k = KeyForPartition(1, kN);
    const TxnEntry w = MakeWrite(7, 2, 1000, k);
    c.Release(w, {0, 1, 2});
    TxnEntry out;
    ASSERT_EQ(c.o[0]->ApplyCommit(w.Key(), 1500, &out), ServerOrderer::CommitResult::kApplied);
    ASSERT_EQ(c.o[1]->ApplyCommit(w.Key(), 1500, &out), ServerOrderer::CommitResult::kApplied);

    c.Recover();
    for (uint32_t s : {0u, 2u}) {
        Status status; uint64_t t = 0;
        ASSERT_TRUE(c.o[s]->speclog_entry(w.Key(), &status, &t));
        EXPECT_EQ(status, Status::COMMITTING) << "server " << s;
        EXPECT_EQ(t, 1500u) << "server " << s;
    }
}

// A committed write executed at 0 (and GC'd) but never seen by 2 reaches 2's store via
// M's key-values; a newer value already in a store is not overwritten.
TEST(RecoveryOrderer, ExecutedKeyValuesRecoveredWithTGuard)
{
    Cluster c;
    const std::string k = KeyForPartition(1, kN);
    const std::string k2 = KeyForPartition(1, kN, 1);
    const TxnEntry w = MakeWrite(7, 3, 1000, k, "val");
    const TxnEntry w2 = MakeWrite(7, 4, 1100, k2, "old");
    for (const TxnEntry *e : {&w, &w2}) {
        c.Release(*e, {0});
        TxnEntry out;
        c.o[0]->ApplyCommit(e->Key(), e->t, &out);
        c.o[0]->MarkCommitted(e->Key());
    }
    c.o[0]->ExecuteReady();
    ASSERT_EQ(c.store[0].value_or_empty(k), "val");
    c.store[2].apply(k2, "newer", 5000);   // 2 already holds a newer value of k2

    auto st = c.Recover();
    EXPECT_EQ(c.store[2].value_or_empty(k), "val");
    EXPECT_EQ(c.store[2].read(k)->t, 1000u);
    EXPECT_EQ(c.store[2].value_or_empty(k2), "newer");   // t-guard
    EXPECT_EQ(st[1].kvs_applied, 1u);
    EXPECT_EQ(st[0].kvs_applied, 0u);   // 0 already had both
}

// An uncommitted copy in the failed range that no PL collected (it was only in 2's
// lateBuffer) is kept and counted as a leftover.
TEST(RecoveryOrderer, LeftoversKeptAndCounted)
{
    Cluster c;
    const std::string k = KeyForPartition(1, kN);
    TxnEntry w = MakeWrite(7, 5, 1000, k);
    c.clock.Set(2000);
    c.o[2]->SubmitTxn(w, false);   // late at a pure follower → lateBuffer
    ASSERT_EQ(c.o[2]->late_size(), 1u);

    auto st = c.Recover();
    EXPECT_EQ(st[1].leftovers, 1u);
    EXPECT_EQ(c.o[2]->late_size(), 1u);   // kept
}

// A value installed from M is newer than a leftover committed write executed later on
// the same key: the older write must not overwrite it.
TEST(RecoveryOrderer, RecoveredValueNotOverwrittenByOlderExecution)
{
    Cluster c;
    const std::string k = KeyForPartition(1, kN);
    RecoveryData m;
    m.kvs.push_back(RecoveredKV{k, "recovered", 2000});
    c.o[2]->InstallView(c.new_topo, 1, {m}, [](const std::string &) { return false; });
    ASSERT_EQ(c.store[2].value_or_empty(k), "recovered");

    const TxnEntry old = MakeWrite(9, 1, 1500, k, "stale");
    TxnEntry out;
    c.clock.Set(1600);
    c.o[2]->InstallFetched(old.txn);
    c.o[2]->ApplyCommit(old.Key(), 1500, &out);
    c.o[2]->MarkCommitted(old.Key());
    c.o[2]->ExecuteReady();
    EXPECT_TRUE(c.o[2]->executed(old.Key()));
    EXPECT_EQ(c.store[2].value_or_empty(k), "recovered");
}

// A txn installed from M (no reply cache entry at this server) and then resent by its
// client must not be admitted twice; the resend is answered with the live entry.
TEST(RecoveryOrderer, ResendAfterInstallNotDuplicated)
{
    Cluster c;
    const std::string k = KeyForPartition(1, kN);
    const TxnEntry w = MakeWrite(7, 6, 1000, k);
    c.Release(w, {0, 1});
    c.Recover();
    const uint32_t pl = c.new_topo.ownerOf(k);
    const size_t before = c.o[pl]->speclog_size();
    c.col[pl].replies.clear();

    c.clock.Set(9000);   // the resend is now late at the (new) leader
    c.o[pl]->SubmitTxn(w, false);
    EXPECT_EQ(c.o[pl]->speclog_size(), before);
    ASSERT_EQ(c.col[pl].replies.size(), 1u);
    EXPECT_EQ(c.col[pl].replies[0].second, 1000u);   // live t, not re-timestamped
}

// ---------------------------------------------------------------------------------------
// VR layer over a fake network
// ---------------------------------------------------------------------------------------

namespace {

struct FakeNet {
    struct Msg { int from; int to; std::shared_ptr<google::protobuf::Message> m; };
    std::vector<std::unique_ptr<ViewCoordinator>> vc;
    std::vector<bool> up;
    std::deque<Msg> q;
    std::mt19937 rng{42};
    double drop = 0.0;
    uint64_t now = 0;
    std::function<bool(const Msg &)> filter;   // false = drop this delivery
    struct Installed { bool done = false; uint64_t view = 0; uint64_t alive = 0;
                       uint64_t old_alive = 0; std::vector<std::string> m_keys; };
    std::vector<Installed> installed;

    std::vector<int> adopted, fresh;   // per server: # adopt_view / fresh_start effects

    static ViewCoordinator::Config Cfg(uint32_t s, uint32_t n, bool joining) {
        ViewCoordinator::Config cfg;
        cfg.self = s; cfg.n = n; cfg.f = (n - 1) / 2;
        cfg.fail_timeout_us = 100000; cfg.grace_us = 1000000; cfg.resend_us = 30000;
        cfg.start_joining = joining;
        return cfg;
    }
    explicit FakeNet(uint32_t n, bool all_joining = false)
        : up(n, true), installed(n), adopted(n, 0), fresh(n, 0) {
        for (uint32_t s = 0; s < n; ++s) {
            vc.push_back(std::make_unique<ViewCoordinator>(Cfg(s, n, all_joining), 0));
        }
    }
    // The process restarts: in-memory VR state lost, the persisted file says rejoin.
    void Restart(uint32_t s) {
        vc[s] = std::make_unique<ViewCoordinator>(Cfg(s, vc.size(), true), now);
        up[s] = true;
        installed[s] = Installed();
    }
    // Each server's recovery contribution: one key-value naming it.
    RecoveryData DataOf(int s) {
        RecoveryData d;
        d.kvs.push_back(RecoveredKV{"from" + std::to_string(s), "v", 1});
        return d;
    }
    void Emit(int from, ViewCoordinator::Effects &fx) {
        for (const auto &o : fx.out) {
            if (o.to == ViewCoordinator::kAll) {
                for (int r = 0; r < (int)vc.size(); ++r) if (r != from) q.push_back({from, r, o.msg});
            } else {
                q.push_back({from, o.to, o.msg});
            }
        }
        if (fx.begin_recovery) {
            ViewCoordinator::Effects fx2;
            vc[from]->OnLocalRecoveryData(DataOf(from), &fx2);
            Emit(from, fx2);
        }
        if (fx.adopt_view) {
            // Catch-up is the replica's job; here it completes instantly.
            adopted[from]++;
            ViewCoordinator::Effects fx2;
            vc[from]->OnCaughtUp(&fx2);
            Emit(from, fx2);
        }
        if (fx.fresh_start) fresh[from]++;
        if (fx.install) {
            ViewCoordinator::Install in = vc[from]->TakeInstall(now);
            Installed &i = installed[from];
            i.done = true; i.view = in.view; i.alive = in.alive; i.old_alive = in.old_alive;
            i.m_keys.clear();
            for (const auto &m : in.ms) for (const auto &kv : m.kvs) i.m_keys.push_back(kv.key);
        }
    }
    void Deliver(const Msg &msg) {
        if (!up[msg.to] || !up[msg.from]) return;
        if (filter && !filter(msg)) return;
        if (drop > 0 && std::uniform_real_distribution<double>(0, 1)(rng) < drop) return;
        ViewCoordinator &v = *vc[msg.to];
        ViewCoordinator::Effects fx;
        const google::protobuf::Message *m = msg.m.get();
        if (auto *x = dynamic_cast<const proto::Heartbeat *>(m)) {
            v.OnHeard(x->sender_id(), now);
        } else if (auto *x = dynamic_cast<const proto::StartViewChange *>(m)) {
            v.OnStartViewChange(*x, now, &fx);
        } else if (auto *x = dynamic_cast<const proto::DoViewChange *>(m)) {
            v.OnDoViewChange(*x, now, &fx);
        } else if (auto *x = dynamic_cast<const proto::StartRecovery *>(m)) {
            v.OnStartRecovery(*x, now, &fx);
        } else if (auto *x = dynamic_cast<const proto::RecoveryRequest *>(m)) {
            v.OnHeard(x->requester_id(), now);
            if (v.AcceptRecoveryView(x->view_id(), x->alive(), x->old_alive(), now, &fx)) {
                auto r = std::make_shared<proto::RecoveryReply>();
                r->set_view_id(x->view_id());
                r->set_sender_id(msg.to);
                RecoveryData d = DataOf(msg.to);
                ToProto(d, r->mutable_txns(), r->mutable_kvs());
                fx.out.push_back({static_cast<int32_t>(x->requester_id()), r});
            }
        } else if (auto *x = dynamic_cast<const proto::RecoveryReply *>(m)) {
            v.OnRecoveryReply(*x, &fx);
        } else if (auto *x = dynamic_cast<const proto::RecoveryPrepare *>(m)) {
            v.OnRecoveryPrepare(*x, now, &fx);
        } else if (auto *x = dynamic_cast<const proto::RecoveryPrepareReply *>(m)) {
            v.OnRecoveryPrepareReply(*x, &fx);
        } else if (auto *x = dynamic_cast<const proto::RecoveryReport *>(m)) {
            v.OnRecoveryReport(*x, &fx);
        } else if (auto *x = dynamic_cast<const proto::StartView *>(m)) {
            v.OnStartView(*x, now, &fx);
        } else if (auto *x = dynamic_cast<const proto::JoinRequest *>(m)) {
            v.OnJoinRequest(*x, now, &fx);
        } else if (auto *x = dynamic_cast<const proto::JoinInfo *>(m)) {
            v.OnJoinInfo(*x, now, &fx);
        } else if (auto *x = dynamic_cast<const proto::JoinReady *>(m)) {
            v.OnJoinReady(*x, now, &fx);
        }
        Emit(msg.to, fx);
    }
    // 10 ms of simulated time: heartbeats + ticks, then drain the network.
    void Step() {
        now += 10000;
        for (int s = 0; s < (int)vc.size(); ++s) {
            if (!up[s]) continue;
            auto hb = std::make_shared<proto::Heartbeat>();
            hb->set_view_id(vc[s]->view()); hb->set_sender_id(s);
            for (int r = 0; r < (int)vc.size(); ++r) if (r != s) q.push_back({s, r, hb});
            ViewCoordinator::Effects fx;
            vc[s]->Tick(now, &fx);
            Emit(s, fx);
        }
        for (size_t guard = 0; !q.empty() && guard < 100000; ++guard) {
            Msg m = q.front(); q.pop_front();
            Deliver(m);
        }
    }
    bool AllSurvivorsInstalled(uint64_t view) {
        for (size_t s = 0; s < vc.size(); ++s) {
            if (!up[s]) continue;
            if (vc[s]->phase() != ViewCoordinator::Phase::NORMAL || vc[s]->view() != view)
                return false;
        }
        return true;
    }
};

void ExpectRecoveredWithout(uint32_t dead, double drop)
{
    FakeNet net(kN);
    for (int i = 0; i < 20; ++i) net.Step();   // everyone hears everyone
    for (uint32_t s = 0; s < kN; ++s) {
        EXPECT_EQ(net.vc[s]->phase(), ViewCoordinator::Phase::NORMAL);
        EXPECT_EQ(net.vc[s]->view(), 0u);
    }
    net.up[dead] = false;
    net.drop = drop;
    int steps = 0;
    while (!net.AllSurvivorsInstalled(1) && steps < 500) { net.Step(); ++steps; }
    ASSERT_TRUE(net.AllSurvivorsInstalled(1)) << "no convergence after " << steps << " steps";

    const uint64_t want = kAll3 & ~(1ULL << dead);
    for (uint32_t s = 0; s < kN; ++s) {
        if (s == dead) continue;
        EXPECT_EQ(net.vc[s]->alive(), want);
        ASSERT_TRUE(net.installed[s].done);
        EXPECT_EQ(net.installed[s].alive, want);
        // The VR leader saw every PL's report, so each PL's M reached f+1 servers; with
        // f = 1 and 2 survivors, both hold both Ms (each built from both servers' data).
        if (drop == 0) {
            EXPECT_EQ(net.installed[s].m_keys.size(), 4u) << "server " << s;
        }
    }
}

} // namespace

TEST(RecoveryView, FollowerDiesSurvivorsInstallNewView)
{
    ExpectRecoveredWithout(1, 0.0);
}

TEST(RecoveryView, VrLeaderDiesNewLeaderCoordinates)
{
    ExpectRecoveredWithout(0, 0.0);
}

TEST(RecoveryView, ConvergesUnderMessageLoss)
{
    ExpectRecoveredWithout(2, 0.25);
    ExpectRecoveredWithout(0, 0.25);
}

TEST(RecoveryView, NodeLeftOutOfStartedViewResetsAndJoinsAsNonMember)
{
    ViewCoordinator::Config cfg;
    cfg.self = 1; cfg.n = kN; cfg.f = 1;
    ViewCoordinator v(cfg, 0);
    proto::StartView sv;
    sv.set_view_id(1); sv.set_sender_id(0); sv.set_alive(0b101);
    ViewCoordinator::Effects fx;
    v.OnStartView(sv, 10, &fx);
    EXPECT_TRUE(fx.excluded);      // replica drops its state
    EXPECT_TRUE(fx.adopt_view);    // and catches up in view 1 as a non-member
    EXPECT_EQ(v.phase(), ViewCoordinator::Phase::JOINING);
    EXPECT_EQ(v.view(), 1u);
    EXPECT_EQ(v.alive(), 0b101u);
}

TEST(RecoveryView, NoFailureDetectionWhenDisabled)
{
    ViewCoordinator::Config cfg;
    cfg.self = 0; cfg.n = kN; cfg.f = 1; cfg.fail_timeout_us = 0;
    ViewCoordinator v(cfg, 0);
    ViewCoordinator::Effects fx;
    v.Tick(100000000, &fx);
    EXPECT_EQ(v.phase(), ViewCoordinator::Phase::NORMAL);
    EXPECT_TRUE(fx.out.empty());
}

// n = 5 (f = 2): two crashes one after the other → views 1 and 2, each among the
// survivors; the second failure is the new view's VR leader.
TEST(RecoveryView, FiveServersTwoSequentialFailures)
{
    FakeNet net(5);
    for (int i = 0; i < 20; ++i) net.Step();
    net.up[3] = false;
    int steps = 0;
    while (!net.AllSurvivorsInstalled(1) && steps < 500) { net.Step(); ++steps; }
    ASSERT_TRUE(net.AllSurvivorsInstalled(1));
    for (uint32_t s : {0u, 1u, 2u, 4u}) EXPECT_EQ(net.vc[s]->alive(), 0b10111u);

    net.up[0] = false;   // the VR leader of view 1
    net.drop = 0.1;
    steps = 0;
    while (!net.AllSurvivorsInstalled(2) && steps < 1000) { net.Step(); ++steps; }
    ASSERT_TRUE(net.AllSurvivorsInstalled(2));
    for (uint32_t s : {1u, 2u, 4u}) EXPECT_EQ(net.vc[s]->alive(), 0b10110u);
}

// The VR leader of view 1 crashes right after its StartView reached only server 1. Server 1
// is NORMAL in view 1 (and, as min alive, VR leader of view 2); servers 2 and 3 never
// installed view 1. Recovery of view 2 must start from view 0 — the oldest last-NORMAL view
// among the participants — so delta-p also re-covers the keys that moved in view 1.
TEST(RecoveryView, LeaderDiesAfterPartialStartView)
{
    FakeNet net(5);
    for (int i = 0; i < 20; ++i) net.Step();
    bool start_view_sent = false;
    net.filter = [&](const FakeNet::Msg &m) {
        if (m.from == 0 && dynamic_cast<const proto::StartView *>(m.m.get()) != nullptr) {
            start_view_sent = true;
            return m.to == 1;
        }
        return true;
    };
    net.up[4] = false;
    int steps = 0;
    while (!start_view_sent && steps < 500) { net.Step(); ++steps; }
    ASSERT_TRUE(start_view_sent);
    net.up[0] = false;   // dies right after StartView
    EXPECT_EQ(net.vc[1]->phase(), ViewCoordinator::Phase::NORMAL);
    EXPECT_EQ(net.vc[1]->view(), 1u);
    for (uint32_t s : {2u, 3u}) {
        EXPECT_EQ(net.vc[s]->phase(), ViewCoordinator::Phase::RECOVERING);
        EXPECT_EQ(net.vc[s]->view(), 0u);
    }

    steps = 0;
    while (!net.AllSurvivorsInstalled(2) && steps < 1000) { net.Step(); ++steps; }
    ASSERT_TRUE(net.AllSurvivorsInstalled(2));
    for (uint32_t s : {1u, 2u, 3u}) {
        EXPECT_EQ(net.installed[s].view, 2u);
        EXPECT_EQ(net.installed[s].alive, 0b01110u);
        EXPECT_EQ(net.installed[s].old_alive, 0b11111u) << "server " << s;
    }
}

// ---------------------------------------------------------------------------------------
// Rejoin
// ---------------------------------------------------------------------------------------

namespace {

void RunUntil(FakeNet &net, const std::function<bool()> &done, int max_steps = 1000)
{
    for (int i = 0; i < max_steps && !done(); ++i) net.Step();
}

bool AllNormalIn(FakeNet &net, uint64_t view, uint64_t alive)
{
    for (size_t s = 0; s < net.vc.size(); ++s) {
        if (!net.up[s]) continue;
        if (net.vc[s]->phase() != ViewCoordinator::Phase::NORMAL ||
            net.vc[s]->view() != view || net.vc[s]->alive() != alive) {
            return false;
        }
    }
    return true;
}

} // namespace

// Crash → view 1 without it → restart → it adopts view 1 as a non-member, catches up,
// asks to rejoin → view 2 with everyone; its home range comes back (old_alive = view 1's).
TEST(Rejoin, RestartedServerRejoins)
{
    for (uint32_t dead : {1u, 2u, 0u}) {   // 0: the joiner is VR leader of the join view
        FakeNet net(kN);
        for (int i = 0; i < 20; ++i) net.Step();
        net.up[dead] = false;
        const uint64_t without = kAll3 & ~(1ULL << dead);
        RunUntil(net, [&] { return AllNormalIn(net, 1, without); });
        ASSERT_TRUE(AllNormalIn(net, 1, without)) << "dead " << dead;

        net.Restart(dead);
        EXPECT_EQ(net.vc[dead]->phase(), ViewCoordinator::Phase::JOINING);
        RunUntil(net, [&] { return AllNormalIn(net, 2, kAll3); });
        ASSERT_TRUE(AllNormalIn(net, 2, kAll3)) << "dead " << dead;
        EXPECT_EQ(net.adopted[dead], 1);
        for (uint32_t s = 0; s < kN; ++s) {
            EXPECT_EQ(net.installed[s].view, 2u);
            EXPECT_EQ(net.installed[s].old_alive, without) << "server " << s;
        }
    }
}

// Restarted faster than failure detection: members still count it alive, so its
// JoinRequest first triggers a view change without it, then it rejoins.
TEST(Rejoin, RestartBeforeDetection)
{
    FakeNet net(kN);
    for (int i = 0; i < 20; ++i) net.Step();
    net.Restart(1);
    RunUntil(net, [&] { return AllNormalIn(net, 2, kAll3); });
    ASSERT_TRUE(AllNormalIn(net, 2, kAll3));
    EXPECT_EQ(net.installed[0].old_alive, 0b101u);   // view 1 had excluded it
}

// A live server falsely suspected (its heartbeats lost for a while) is left out of view 1;
// it resets and rejoins on its own once it is heard again.
TEST(Rejoin, FalselyExcludedServerRejoins)
{
    FakeNet net(kN);
    for (int i = 0; i < 20; ++i) net.Step();
    bool mute = true;
    net.filter = [&](const FakeNet::Msg &m) {
        return !(mute && m.from == 2 && dynamic_cast<const proto::Heartbeat *>(m.m.get()));
    };
    for (int i = 0; i < 50; ++i) net.Step();   // 500 ms muted
    EXPECT_GE(net.adopted[2], 1);   // it was left out, reset, and caught up as a non-member
    mute = false;
    auto rejoined = [&] {
        const uint64_t v = net.vc[0]->view();
        return v >= 2 && AllNormalIn(net, v, kAll3);
    };
    RunUntil(net, rejoined);
    ASSERT_TRUE(rejoined());
}

// Every server restarted (> f failures): no member to rejoin, so after the grace period
// they start fresh in view 0.
TEST(Rejoin, WholeClusterRestartStartsFresh)
{
    FakeNet net(kN, /*all_joining=*/true);
    RunUntil(net, [&] { return AllNormalIn(net, 0, kAll3); }, 300);
    ASSERT_TRUE(AllNormalIn(net, 0, kAll3));
    for (uint32_t s = 0; s < kN; ++s) EXPECT_EQ(net.fresh[s], 1);
}

// A joiner never votes: a recovery round that leaves it out does not involve it, and a
// StartViewChange that adds it is ignored until it has caught up.
TEST(Rejoin, JoinerIgnoresViewChangesUntilCaughtUp)
{
    ViewCoordinator::Config cfg = FakeNet::Cfg(2, kN, true);
    ViewCoordinator v(cfg, 0);
    ViewCoordinator::Effects fx;
    proto::JoinInfo info;
    info.set_view_id(1); info.set_sender_id(0); info.set_alive(0b011);
    v.OnJoinInfo(info, 10, &fx);
    ASSERT_TRUE(fx.adopt_view);
    proto::StartViewChange svc;
    svc.set_view_id(2); svc.set_sender_id(0); svc.set_alive(0b111);
    ViewCoordinator::Effects fx2;
    v.OnStartViewChange(svc, 20, &fx2);
    EXPECT_EQ(v.phase(), ViewCoordinator::Phase::JOINING);   // not caught up yet
    EXPECT_FALSE(v.AcceptRecoveryView(2, 0b011, 0b011, 30, &fx2));
    v.OnCaughtUp(&fx2);
    ASSERT_EQ(fx2.out.size(), 1u);   // JoinReady to the VR leader
    EXPECT_EQ(fx2.out[0].to, 0);
    v.OnStartViewChange(svc, 40, &fx2);
    EXPECT_EQ(v.phase(), ViewCoordinator::Phase::VIEW_CHANGE);
}

// Orderer: the donor pages its executed state out by sorted key; the joiner installs it
// t-guarded, plus the donor's committed-but-unexecuted txns.
TEST(Rejoin, SnapshotPagesAndInstalls)
{
    Cluster c;
    std::vector<std::string> keys;
    for (int i = 0; i < 5; ++i) {
        const std::string k = "snap" + std::to_string(i);
        keys.push_back(k);
        const TxnEntry w = MakeWrite(3, i + 1, 1000 + i, k, "v" + std::to_string(i));
        c.Release(w, {0});
        TxnEntry out;
        c.o[0]->ApplyCommit(w.Key(), w.t, &out);
        c.o[0]->MarkCommitted(w.Key());
    }
    c.o[0]->ExecuteReady();
    const TxnEntry pending = MakeWrite(3, 99, 5000, "snapP", "p");
    c.Release(pending, {0});
    TxnEntry out;
    c.o[0]->ApplyCommit(pending.Key(), 5000, &out);   // COMMITTING, not executed

    c.store[2].apply("snap0", "newer", 9999);          // joiner already has a newer value
    const std::vector<std::string> sk = c.o[0]->SnapshotKeys();
    ASSERT_EQ(sk, keys);   // sorted
    size_t applied = 0;
    for (size_t off = 0; off < sk.size(); off += 2) {
        RecoveryData d;
        d.kvs = c.o[0]->SnapshotRead(sk, off, 2);
        if (off + 2 >= sk.size()) d.txns = c.o[0]->CommittedTxns();
        applied += c.o[2]->InstallSnapshot(d);
    }
    EXPECT_EQ(applied, 4u);
    EXPECT_EQ(c.store[2].value_or_empty("snap0"), "newer");
    EXPECT_EQ(c.store[2].value_or_empty("snap4"), "v4");
    Status st; uint64_t t = 0;
    ASSERT_TRUE(c.o[2]->speclog_entry(pending.Key(), &st, &t));
    EXPECT_EQ(st, Status::COMMITTING);
    EXPECT_EQ(t, 5000u);

    c.o[2]->Reset();
    EXPECT_EQ(c.o[2]->speclog_size(), 0u);
    EXPECT_EQ(c.store[2].value_or_empty("snap4"), "v4");   // store kept
}

// Orderer-level handback: server 1 dies (view 1: its keys led by 0/2), a write on one of
// its keys is fast-committed in view 1; server 1 restarts empty, catches up from 0, and in
// the join recovery (view 1 → 2) becomes PL of its home range again — holding the write
// at its t as leader, and the executed value.
TEST(Rejoin, HomeRangeHandedBackWithState)
{
    Cluster c;
    const std::string kexec = KeyForPartition(1, kN, 0);
    const std::string kspec = KeyForPartition(1, kN, 1);
    // Executed before the crash, on all three.
    const TxnEntry w0 = MakeWrite(4, 1, 1000, kexec, "exec");
    c.Release(w0, {0, 1, 2});
    for (uint32_t s = 0; s < kN; ++s) {
        TxnEntry out;
        c.o[s]->ApplyCommit(w0.Key(), 1000, &out);
        c.o[s]->MarkCommitted(w0.Key());
        c.o[s]->ExecuteReady();
    }
    c.Recover();   // view 1, alive {0, 2}

    // In view 1: a write on kspec released at both survivors (fast-committed).
    const TxnEntry w1 = MakeWrite(4, 2, 3000, kspec, "spec");
    c.Release(w1, {0, 2});

    // Server 1 restarts with nothing.
    InMemoryBackend fresh_store;
    Collector fresh_col;
    ServerOrderer joiner(1, 0, &c.clock, Topology(kN), fresh_col.cb(), &fresh_store);
    joiner.InstallView(c.new_topo, 1, {}, [](const std::string &) { return false; });
    RecoveryData snap;
    snap.kvs = c.o[0]->SnapshotRead(c.o[0]->SnapshotKeys(), 0, 100);
    snap.txns = c.o[0]->CommittedTxns();
    joiner.InstallSnapshot(snap);
    EXPECT_EQ(fresh_store.value_or_empty(kexec), "exec");

    // Join recovery view 1 → 2: PL 1's delta-p is its home range.
    const Topology v1 = c.new_topo, v2 = Topology(kN);
    ServerOrderer *srv[kN] = {c.o[0].get(), &joiner, c.o[2].get()};
    std::vector<RecoveryData> ms;
    for (uint32_t pl = 0; pl < kN; ++pl) {
        auto in_delta = [&](const std::string &k) { return InDeltaP(v1, v2, k, pl); };
        if (pl != 1) {
            EXPECT_TRUE(srv[pl]->CollectRecovery(in_delta).empty()) << "pl " << pl;
            continue;
        }
        std::vector<RecoveryData> replies = {joiner.CollectRecovery(in_delta),
                                             c.o[0]->CollectRecovery(in_delta)};
        ms.push_back(MergeRecovery(replies));
    }
    for (ServerOrderer *o : srv) {
        o->InstallView(v2, 2, ms, [](const std::string &) { return false; });
    }
    Status st; uint64_t t = 0;
    ASSERT_TRUE(joiner.speclog_entry(w1.Key(), &st, &t));
    EXPECT_EQ(st, Status::SPECULATIVE);
    EXPECT_EQ(t, 3000u);
    c.clock.Set(3000 + 200000);
    std::vector<ServerOrderer::TsAgreeAction> actions;
    std::vector<TxnEntry> committed;
    joiner.DriveTsAgreement(100000, &actions, &committed);   // leader sync at the joiner
    ASSERT_EQ(committed.size(), 1u);
    EXPECT_EQ(committed[0].t, 3000u);
    EXPECT_TRUE(committed[0].is_leader_for_this);
}

// Whole-cluster restart with staggered restarts: two servers start fresh first; the
// third's JoinRequest then reaches fresh-started members, which must answer with view 0
// (it starts fresh too) instead of treating it as a restarted member and excluding it.
TEST(Rejoin, StaggeredWholeClusterRestartNoExclusion)
{
    FakeNet net(kN, /*all_joining=*/true);
    for (int i = 0; i < 50; ++i) net.Step();
    net.Restart(2);   // its grace period starts 500 ms later than the others'
    RunUntil(net, [&] { return AllNormalIn(net, 0, kAll3); }, 300);
    ASSERT_TRUE(AllNormalIn(net, 0, kAll3));
    for (int i = 0; i < 50; ++i) net.Step();
    EXPECT_TRUE(AllNormalIn(net, 0, kAll3));   // and nobody got excluded afterwards
}

// A server that missed a write (paused for a view change) and later executed it via the
// leader's replication must still answer the client's resend — with the final t —
// rather than drop it, or the client never reaches f+1 replies.
TEST(Rejoin, ResendOfExecutedWriteAnsweredWithFinalT)
{
    Cluster c;
    const TxnEntry w = MakeWrite(5, 1, 1000, "kx");
    c.clock.Set(1500);
    ASSERT_TRUE(c.o[1]->HandleReplicatePrepare(w.Key(), 1200, w.txn));   // never saw the request
    c.o[1]->MarkCommitted(w.Key());
    c.o[1]->ExecuteReady();
    ASSERT_TRUE(c.o[1]->executed(w.Key()));
    c.col[1].replies.clear();

    c.o[1]->SubmitTxn(w, false);   // the client's resend
    ASSERT_EQ(c.col[1].replies.size(), 1u);
    EXPECT_EQ(c.col[1].replies[0].second, 1200u);
    EXPECT_FALSE(c.o[1]->speclog_contains(w.Key()));   // not re-admitted
}

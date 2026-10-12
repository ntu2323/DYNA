// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/tests/indexed_pq_test.cc
//   Stage 1 acceptance tests for IndexedMinPQ: push / peek_min / pop_min /
//   contains / erase, the update_key re-heapify that specLog re-timestamp needs,
//   and the load-bearing invariant that pos_ mirrors the heap across many random
//   operations (validated via CheckInvariants()).

#include "dyna/indexed_min_pq.h"
#include "dyna/txn_types.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <map>
#include <random>
#include <vector>

using namespace specpaxos::dyna;

// A minimal ordered value for the generic heap tests: ordered by `key` ascending.
struct IntVal {
    uint64_t key = 0;
};
struct IntValLess {
    bool operator()(const IntVal &a, const IntVal &b) const { return a.key < b.key; }
};
using IntPQ = IndexedMinPQ<uint64_t, IntVal, IntValLess>;

TEST(IndexedMinPQ, PushPeekPopOrder)
{
    IntPQ pq;
    EXPECT_TRUE(pq.empty());
    pq.push(1, IntVal{300});
    pq.push(2, IntVal{100});
    pq.push(3, IntVal{200});
    EXPECT_EQ(pq.size(), 3u);
    EXPECT_TRUE(pq.CheckInvariants());

    EXPECT_EQ(pq.peek_min().key, 100u);
    EXPECT_EQ(pq.peek_min_id(), 2u);
    EXPECT_EQ(pq.pop_min().key, 100u);
    EXPECT_EQ(pq.pop_min().key, 200u);
    EXPECT_EQ(pq.pop_min().key, 300u);
    EXPECT_TRUE(pq.empty());
}

TEST(IndexedMinPQ, DuplicatePushRejected)
{
    IntPQ pq;
    EXPECT_TRUE(pq.push(1, IntVal{10}));
    EXPECT_FALSE(pq.push(1, IntVal{5}));   // same id -> no-op
    EXPECT_EQ(pq.size(), 1u);
    EXPECT_EQ(pq.peek_min().key, 10u);     // original value retained
}

TEST(IndexedMinPQ, ContainsAndFind)
{
    IntPQ pq;
    pq.push(5, IntVal{50});
    EXPECT_TRUE(pq.contains(5));
    EXPECT_FALSE(pq.contains(6));
    ASSERT_NE(pq.find(5), nullptr);
    EXPECT_EQ(pq.find(5)->key, 50u);
    EXPECT_EQ(pq.find(6), nullptr);
}

TEST(IndexedMinPQ, EraseArbitrary)
{
    IntPQ pq;
    for (uint64_t i = 0; i < 10; ++i) pq.push(i, IntVal{i * 10});
    EXPECT_TRUE(pq.erase(4));
    EXPECT_FALSE(pq.contains(4));
    EXPECT_FALSE(pq.erase(4));   // already gone
    EXPECT_EQ(pq.size(), 9u);
    EXPECT_TRUE(pq.CheckInvariants());

    // Remaining pop in ascending order, skipping the erased key*10 = 40.
    uint64_t prev = 0;
    while (!pq.empty()) {
        uint64_t k = pq.pop_min().key;
        EXPECT_NE(k, 40u);
        EXPECT_GE(k, prev);
        prev = k;
    }
}

// update_key must re-heapify after the caller mutates an entry in place — this is
// exactly the specLog re-timestamp path (late-leader bump / SlowCommit adopt-max).
TEST(IndexedMinPQ, UpdateKeyReheapifiesDown)
{
    IntPQ pq;
    pq.push(1, IntVal{10});   // currently the min
    pq.push(2, IntVal{20});
    pq.push(3, IntVal{30});
    ASSERT_EQ(pq.peek_min_id(), 1u);

    pq.find(1)->key = 100;    // bump id 1 past everyone
    EXPECT_TRUE(pq.update_key(1));
    EXPECT_TRUE(pq.CheckInvariants());

    EXPECT_EQ(pq.peek_min_id(), 2u);      // 20 now smallest
    EXPECT_EQ(pq.pop_min().key, 20u);
    EXPECT_EQ(pq.pop_min().key, 30u);
    EXPECT_EQ(pq.pop_min().key, 100u);    // bumped entry released last
}

TEST(IndexedMinPQ, UpdateKeyReheapifiesUp)
{
    IntPQ pq;
    pq.push(1, IntVal{10});
    pq.push(2, IntVal{20});
    pq.push(3, IntVal{30});

    pq.find(3)->key = 1;      // lower id 3 below everyone
    EXPECT_TRUE(pq.update_key(3));
    EXPECT_TRUE(pq.CheckInvariants());
    EXPECT_EQ(pq.peek_min_id(), 3u);
    EXPECT_EQ(pq.pop_min().key, 1u);
}

TEST(IndexedMinPQ, UpdateKeyMissingReturnsFalse)
{
    IntPQ pq;
    pq.push(1, IntVal{10});
    EXPECT_FALSE(pq.update_key(99));
}

// Randomized differential test against a std::multimap oracle: interleave push,
// pop_min, erase, and update_key; after every op assert CheckInvariants() and that
// the min matches the oracle. This is the real guard on pos_ staying consistent.
TEST(IndexedMinPQ, RandomizedMatchesOracle)
{
    std::mt19937_64 rng(12345);
    IntPQ pq;
    std::map<uint64_t, uint64_t> oracle;   // id -> key (unique ids)
    uint64_t next_id = 1;

    auto oracle_min = [&]() -> uint64_t {
        uint64_t best = UINT64_MAX;
        for (auto &kv : oracle) best = std::min(best, kv.second);
        return best;
    };

    for (int step = 0; step < 5000; ++step) {
        int op = rng() % 4;
        if (op == 0 || oracle.empty()) {
            uint64_t id = next_id++;
            uint64_t key = rng() % 1000;
            pq.push(id, IntVal{key});
            oracle[id] = key;
        } else if (op == 1) {
            uint64_t expect = oracle_min();
            uint64_t got = pq.peek_min().key;
            EXPECT_EQ(got, expect);
            uint64_t popped_id = pq.peek_min_id();
            pq.pop_min();
            oracle.erase(popped_id);
        } else if (op == 2) {
            // erase a random existing id
            auto it = oracle.begin();
            std::advance(it, rng() % oracle.size());
            uint64_t id = it->first;
            EXPECT_TRUE(pq.erase(id));
            oracle.erase(id);
        } else {
            // update a random existing id to a new key
            auto it = oracle.begin();
            std::advance(it, rng() % oracle.size());
            uint64_t id = it->first;
            uint64_t key = rng() % 1000;
            pq.find(id)->key = key;
            EXPECT_TRUE(pq.update_key(id));
            oracle[id] = key;
        }
        ASSERT_TRUE(pq.CheckInvariants()) << "at step " << step;
        ASSERT_EQ(pq.size(), oracle.size());
        if (!oracle.empty()) EXPECT_EQ(pq.peek_min().key, oracle_min());
    }
}

// The real use: IndexedMinPQ<TxnId, TxnEntry> ordered by OrderKey — a re-timestamp
// bumps the live t and re-heapifies (specLog behavior).
TEST(IndexedMinPQ, TxnEntryByOrderKey)
{
    IndexedMinPQ<TxnId, TxnEntry, OrderKeyLess> spec;

    auto make = [](uint64_t c, uint64_t r, uint64_t t) {
        TxnEntry e;
        e.txn.set_client_id(c);
        e.txn.set_request_id(r);
        e.txn.set_type(proto::WRITE);
        e.txn.set_t(t);
        e.txn.set_view_id(0);
        e.t = t;
        return e;
    };

    spec.push(MakeTxnId(1, 1), make(1, 1, 300));
    spec.push(MakeTxnId(1, 2), make(1, 2, 100));
    spec.push(MakeTxnId(2, 1), make(2, 1, 200));
    ASSERT_TRUE(spec.CheckInvariants());
    EXPECT_EQ(spec.peek_min().t, 100u);   // (t=100) is min

    // Re-timestamp (1,2) to t=999 (late-leader style bump) and re-heapify.
    spec.find(MakeTxnId(1, 2))->t = 999;
    EXPECT_TRUE(spec.update_key(MakeTxnId(1, 2)));
    ASSERT_TRUE(spec.CheckInvariants());

    EXPECT_EQ(spec.pop_min().t, 200u);    // now (t=200) is smallest
    EXPECT_EQ(spec.pop_min().t, 300u);
    EXPECT_EQ(spec.pop_min().t, 999u);    // bumped entry last
}

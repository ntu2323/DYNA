// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/tests/storage_test.cc
//   The on-store value encoding ([t : 8 bytes LE][value]) and DbBackend over the
//   harness DB interface (lib's InMemoryDB here; SplinterDB in the bench): applied
//   writes round-trip with their commit timestamp and overwrite in place.

#include "dyna/db_backend.h"
#include "dyna/value_codec.h"
#include "lib/db.h"

#include <gtest/gtest.h>

#include <limits>
#include <string>

using namespace specpaxos::dyna;

TEST(ValueCodec, RoundTripsValueAndTimestamp)
{
    for (uint64_t t : {uint64_t{0}, uint64_t{1}, uint64_t{1791838758056582},
                       std::numeric_limits<uint64_t>::max()}) {
        for (std::string v : {std::string(), std::string("x"), std::string(100, 'z'),
                              std::string("a\0b", 3)}) {
            const std::string enc = EncodeVersioned(v, t);
            ASSERT_EQ(enc.size(), kValueHeaderBytes + v.size());
            const VersionedValue dec = DecodeVersioned(enc);
            EXPECT_EQ(dec.t, t);
            EXPECT_EQ(dec.value, v);
        }
    }
}

TEST(ValueCodec, ShortForeignBlobDecodesAsUnversioned)
{
    const VersionedValue dec = DecodeVersioned("abc");
    EXPECT_EQ(dec.t, 0u);
    EXPECT_EQ(dec.value, "abc");
}

TEST(DbBackend, AppliesAndReadsVersionedValues)
{
    Properties props;
    InMemoryDB db(props, /*preloaded=*/false);
    DbBackend backend(&db);
    backend.RegisterThread();

    EXPECT_FALSE(backend.read("k").has_value());
    backend.apply("k", "v1", 100);
    auto r1 = backend.read("k");
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->value, "v1");
    EXPECT_EQ(r1->t, 100u);

    backend.apply("k", "v2", 200);              // overwrite in place
    auto r2 = backend.read("k");
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->value, "v2");
    EXPECT_EQ(r2->t, 200u);
    backend.UnregisterThread();
}

// --- end-to-end through real SplinterDB -----------------------------------------
//
// The same path a running replica uses: the orderer's executor thread (registered with
// SplinterDB) applies a COMMITTED write into SplinterDB via DbBackend, then services a
// leader read from it. The bench never issues reads, so this is where read-after-write
// against the real store is checked.

#include "dyna/server_orderer.h"
#include "dyna/topology.h"
#include "dyna/wall_clock.h"

#include <chrono>
#include <cstdio>
#include <mutex>
#include <thread>
#include <vector>

namespace {

TxnEntry StorageWrite(uint64_t r, uint64_t t, const std::string &k, const std::string &v)
{
    TxnEntry e;
    e.txn.set_client_id(1); e.txn.set_request_id(r);
    e.txn.set_type(specpaxos::dyna::proto::WRITE);
    auto *kv = e.txn.add_w_set(); kv->set_key(k); kv->set_value(v);
    e.txn.set_t(t); e.txn.set_view_id(0); e.t = t;
    return e;
}

TxnEntry StorageRead(uint64_t r, uint64_t t, const std::string &k)
{
    TxnEntry e;
    e.txn.set_client_id(1); e.txn.set_request_id(r);
    e.txn.set_type(specpaxos::dyna::proto::READ);
    e.txn.add_r_set(k);
    e.txn.set_t(t); e.txn.set_view_id(0); e.t = t;
    return e;
}

} // namespace

TEST(DbBackend, SplinterDbWriteThenLeaderReadThroughExecutor)
{
    const std::string path = "/tmp/dyna-storage-test-splinterdb.db";
    std::remove(path.c_str());
    Properties props;   // must outlive the DB (it holds a pointer to the filename)
    for (const auto &[k, v] : DB_PROPS) props.SetProperty(k, v);
    props.SetProperty("splinterdb.filename", path);
    props.SetProperty("splinterdb.cache_size_mb", "64");
    props.SetProperty("splinterdb.disk_size_gb", "1");
    {
        SplinterDB db(props, /*preloaded=*/false);
        DbBackend backend(&db);
        MockClock clk(50);
        std::mutex mu;
        std::vector<std::pair<TxnId, std::string>> replies;   // (id, value read for "K")
        ServerOrderer o(0, 0, &clk, Topology(1),
                        [&](const TxnEntry &e) {
                            std::lock_guard<std::mutex> lk(mu);
                            auto it = e.results.find("K");
                            replies.emplace_back(e.Key(), it == e.results.end() ? "" : it->second);
                        },
                        &backend);
        o.Start();   // release loop + executor (registers with SplinterDB)
        auto wait_for = [&](const TxnId &id, std::string *val) {
            for (int i = 0; i < 5000; ++i) {
                {
                    std::lock_guard<std::mutex> lk(mu);
                    for (const auto &r : replies) if (r.first == id) { *val = r.second; return true; }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            return false;
        };
        std::string got;

        // Write K=v1 (t=100), commit, and let the executor apply it to SplinterDB.
        const TxnId w = MakeTxnId(1, 1);
        o.SubmitTxn(StorageWrite(1, 100, "K", "v1"));
        clk.Set(100);
        ASSERT_TRUE(wait_for(w, &got));                     // write's SpecReply
        TxnEntry out;
        ASSERT_EQ(o.ApplyCommit(w, 100, &out), ServerOrderer::CommitResult::kApplied);
        o.MarkCommitted(w);
        for (int i = 0; i < 5000 && !o.executed(w); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        ASSERT_TRUE(o.executed(w));

        // Leader read (t=200): served by the executor from SplinterDB.
        const TxnId r = MakeTxnId(1, 2);
        o.SubmitTxn(StorageRead(2, 200, "K"));
        clk.Set(200);
        ASSERT_TRUE(wait_for(r, &got));
        EXPECT_EQ(got, "v1");

        // A read of a never-written key is a clean miss (empty), not a crash.
        const TxnId miss = MakeTxnId(1, 3);
        auto missing = StorageRead(3, 300, "never-written");
        o.SubmitTxn(std::move(missing));
        clk.Set(300);
        ASSERT_TRUE(wait_for(miss, &got));
        EXPECT_EQ(got, "");
        o.Stop();

        // The stored record carries the commit timestamp. (This thread created the
        // SplinterDB, which registers it implicitly — registering again would assert.)
        auto stored = backend.read("K");
        ASSERT_TRUE(stored.has_value());
        EXPECT_EQ(stored->value, "v1");
        EXPECT_EQ(stored->t, 100u);
    }
    std::remove(path.c_str());
}

// release_repro_test.cc
//   Local reproduction of the ~120µs Submit→release cost seen on the cluster.
//   Drives the REAL PartitionOrderer + background release loop (RealClock,
//   busy_poll) under synthetic load from a "pump" thread, and prints the actual
//   Submit→release latency (rLoop) distribution + a per-request hop timeline.
//   Remote aggregate stats said every sub-stage is tiny yet rLoop≈120µs — this
//   isolates whether that's the orderer/release-loop itself or the environment.
//
//   Tunables (env): REPRO_BOUND_US (deadline hold), REPRO_INTER_US (inter-submit
//   gap), REPRO_N (#requests), REPRO_POST_DRAIN_US (simulate the §7 broadcast that
//   the release thread does inline).

#include "dyna_new/partition_orderer.h"
#include "dyna_new/in_memory_backend.h"
#include "dyna_new/partition_state.h"
#include "dyna_new/wall_clock.h"
#include "dyna_new/dyna-new-proto.pb.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

namespace specpaxos {
namespace dyna_new {
namespace {

uint64_t EnvU64(const char *k, uint64_t d) {
    const char *v = std::getenv(k);
    return (v && *v) ? std::strtoull(v, nullptr, 10) : d;
}

TEST(ReleaseRepro, SubmitToReleaseTimeline) {
    const uint64_t BOUND = EnvU64("REPRO_BOUND_US", 70);
    const uint64_t INTER = EnvU64("REPRO_INTER_US", 2400);  // ~140/s per partition
    const uint64_t N = EnvU64("REPRO_N", 4000);
    const uint64_t POST_DRAIN = EnvU64("REPRO_POST_DRAIN_US", 0);

    InMemoryBackend backend;
    RealClock clock(/*busy_poll=*/true);

    std::vector<uint64_t> rloop(N, 0);   // release - submit, per request_id
    std::vector<uint64_t> late(N, 0);    // release - deadline (clamped)
    std::vector<uint64_t> submit_at(N, 0);   // per-id submit stamp (test-local)
    std::atomic<uint64_t> released_count{0};

    // on_release runs on the release loop thread — record the hop stamps.
    auto on_release = [&](const LogEntry &e) {
        const uint64_t now = WallClockMicros();
        const uint64_t id = e.request_id;
        if (id < N) {
            rloop[id] = (now > submit_at[id]) ? now - submit_at[id] : 0;
            late[id] = (now > e.deadline) ? now - e.deadline : 0;
        }
        released_count.fetch_add(1, std::memory_order_relaxed);
    };
    // on_post_drain simulates the inline §7 broadcast the leader does on the
    // release thread (spin to burn ~POST_DRAIN µs of wall time).
    auto on_post_drain = [&]() {
        if (POST_DRAIN == 0) return;
        const uint64_t until = WallClockMicros() + POST_DRAIN;
        while (WallClockMicros() < until) { /* burn */ }
    };

    PartitionOrderer orderer(0, 0, &backend, &clock,
                             PartitionOrderer::Role::kLeader, on_release,
                             /*commit_quorum=*/1, /*recon_interval_us=*/5000,
                             /*on_maintenance=*/nullptr, on_post_drain);
    orderer.Start();

    // Pump thread: submit N requests spaced by INTER, deadline = submit + BOUND.
    for (uint64_t i = 0; i < N; ++i) {
        const uint64_t t = WallClockMicros();
        LogEntry e;
        e.client_id = 1;
        e.request_id = i;
        e.type = proto::BLIND_WRITE;
        e.key = "k";
        e.command = "v";
        submit_at[i] = t;          // submit stamp (test-local)
        e.deadline = t + BOUND;    // client deadline
        orderer.Submit(std::move(e));
        // Busy-wait the inter-submit gap (mimics arrivals; avoids sleep jitter).
        const uint64_t next = t + INTER;
        while (WallClockMicros() < next) { /* spacing */ }
    }
    // Let the tail drain.
    while (released_count.load() < N) {
        std::this_thread::yield();
    }
    orderer.Stop();

    std::vector<uint64_t> rl = rloop;
    std::sort(rl.begin(), rl.end());
    std::vector<uint64_t> lt = late;
    std::sort(lt.begin(), lt.end());
    auto pct = [](const std::vector<uint64_t> &v, double p) {
        return v[static_cast<size_t>(p * (v.size() - 1))];
    };
    std::printf("\n=== ReleaseRepro: BOUND=%llu INTER=%llu N=%llu POST_DRAIN=%llu ===\n",
                (unsigned long long)BOUND, (unsigned long long)INTER,
                (unsigned long long)N, (unsigned long long)POST_DRAIN);
    std::printf("rLoop_us  min=%llu p50=%llu p90=%llu p99=%llu max=%llu\n",
                (unsigned long long)rl.front(), (unsigned long long)pct(rl, 0.5),
                (unsigned long long)pct(rl, 0.9), (unsigned long long)pct(rl, 0.99),
                (unsigned long long)rl.back());
    std::printf("late_us   min=%llu p50=%llu p90=%llu p99=%llu max=%llu (release - deadline)\n",
                (unsigned long long)lt.front(), (unsigned long long)pct(lt, 0.5),
                (unsigned long long)pct(lt, 0.9), (unsigned long long)pct(lt, 0.99),
                (unsigned long long)lt.back());
    std::printf("first 8 requests rLoop: ");
    for (uint64_t i = 0; i < 8 && i < N; ++i)
        std::printf("%llu ", (unsigned long long)rloop[i]);
    std::printf("\n");
    // Expectation: rLoop ≈ BOUND (released at the deadline). Anything far above
    // BOUND is release-loop overhead. Loose bound so the test never fails the CI.
    EXPECT_EQ(released_count.load(), N);
}

} // namespace
} // namespace dyna_new
} // namespace specpaxos

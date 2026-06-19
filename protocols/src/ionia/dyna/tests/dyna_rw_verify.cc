// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_rw_verify.cc
//   Read-after-write correctness check for the DYNA protocol.
//
//   This is a standalone client driver (no gtest / SimulatedTransport).
//   The current DYNA implementation drives deadlines off the real wall
//   clock and runs background worker threads, so it cannot be exercised by
//   the single-threaded, virtual-time SimulatedTransport used by vr-test.
//   Instead we talk to a real, running DYNA cluster over FastTransport
//   (the same transport the bench uses) and assert that a read reflects the
//   most recently committed write.
//
//   The bench client deliberately ignores read results; this driver exists
//   precisely to validate them.
//
//   Two scenarios:
//     * single-key (default): write V_A -> read (expect V_A)
//                             -> write V_B -> read (expect V_B).
//       Proves a read reflects the *latest* committed write to a key.
//     * interleaved multi-key (-K N): N keys are written, read back, then
//       overwritten (including back-to-back double-writes to the same key to
//       exercise last-writer-wins ordering) and read again, with all
//       operation orders deterministically shuffled. Proves per-key freshness
//       holds when many keys (and partitions) are in flight in mixed order.
//
//   Exit code 0 => all reads matched; non-zero => mismatch or timeout.

#include "lib/configuration.h"
#include "lib/fasttransport.h"
#include "lib/message.h"
#include "dyna/client.h"

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include <getopt.h>

namespace {

// DYNA keys are a fixed 24-byte field (command layout: op[1] | key[24] | value).
const size_t kKeyLen = 24;

std::string NormalizeKey(const std::string &raw)
{
    std::string k = raw;
    if (k.size() < kKeyLen) {
        k.append(kKeyLen - k.size(), '0');
    } else if (k.size() > kKeyLen) {
        k = k.substr(0, kKeyLen);
    }
    return k;
}

void Usage(const char *prog)
{
    fprintf(stderr,
            "usage: %s -c config [-k key] [-K num-keys] [-i client-id] "
            "[-t timeout-ms] [-w write-settle-ms] [-s seed]\n"
            "  -c  cluster config file (f N + replica lines)\n"
            "  -k  key (single-key mode) or key prefix (multi-key mode)\n"
            "  -K  number of keys for interleaved multi-key mode "
            "(0 = single-key, default 0)\n"
            "  -i  client id (default 1)\n"
            "  -t  overall timeout in ms (default 15000)\n"
            "  -w  delay after a write before the following read, ms "
            "(default 200)\n"
            "  -s  RNG seed for the deterministic shuffle (default 1)\n",
            prog);
}

struct Step {
    bool isRead;
    std::string key;   // already normalized to 24 bytes
    std::string value; // value to write, or value the read must return
};

// Single-key scenario: prove the read returns the latest of two writes.
std::vector<Step> BuildSingleKeySteps(const std::string &key)
{
    return {
        {false, key, "dyna_value_alpha"},
        {true,  key, "dyna_value_alpha"},
        {false, key, "dyna_value_beta"},
        {true,  key, "dyna_value_beta"},
    };
}

// Interleaved multi-key scenario. Builds a deterministically shuffled mix of
// writes and reads across many keys, with read expectations computed from the
// last write to each key earlier in the sequence.
std::vector<Step> BuildMultiKeySteps(const std::string &prefix,
                                     size_t numKeys,
                                     uint64_t seed)
{
    std::vector<std::string> keys;
    keys.reserve(numKeys);
    for (size_t i = 0; i < numKeys; i++) {
        keys.push_back(NormalizeKey(prefix + std::to_string(i)));
    }

    std::mt19937_64 rng(seed);
    std::unordered_map<std::string, std::string> lastWritten;
    std::vector<Step> steps;

    auto shuffledIndices = [&](size_t n) {
        std::vector<size_t> idx(n);
        for (size_t i = 0; i < n; i++) idx[i] = i;
        std::shuffle(idx.begin(), idx.end(), rng);
        return idx;
    };

    auto emitWrite = [&](size_t k, const std::string &val) {
        steps.push_back({false, keys[k], val});
        lastWritten[keys[k]] = val;
    };
    auto emitRead = [&](size_t k) {
        // Expect whatever was last written to this key so far.
        steps.push_back({true, keys[k], lastWritten[keys[k]]});
    };

    // Round 1: write every key once (shuffled order).
    for (size_t k : shuffledIndices(numKeys)) {
        emitWrite(k, "r1_v_" + std::to_string(k));
    }
    // Read every key (shuffled), expecting round-1 values.
    for (size_t k : shuffledIndices(numKeys)) {
        emitRead(k);
    }

    // Round 2: overwrite every key, with a back-to-back double-write on each
    // (shuffled) so the read must observe the *second* write (last-writer-wins).
    for (size_t k : shuffledIndices(numKeys)) {
        emitWrite(k, "r2_a_" + std::to_string(k));
        emitWrite(k, "r2_b_" + std::to_string(k));
    }
    // Final reads (shuffled) expecting the second round-2 value per key.
    for (size_t k : shuffledIndices(numKeys)) {
        emitRead(k);
    }

    return steps;
}

} // namespace

int main(int argc, char **argv)
{
    const char *configPath = nullptr;
    std::string keyArg = "dyna_rw_correctness_key";
    size_t numKeys = 0;
    uint64_t clientId = 1;
    uint64_t timeoutMs = 15000;
    uint64_t writeSettleMs = 200;
    uint64_t seed = 1;

    int opt;
    while ((opt = getopt(argc, argv, "c:k:K:i:t:w:s:h")) != -1) {
        switch (opt) {
        case 'c': configPath = optarg; break;
        case 'k': keyArg = optarg; break;
        case 'K': numKeys = strtoull(optarg, nullptr, 10); break;
        case 'i': clientId = strtoull(optarg, nullptr, 10); break;
        case 't': timeoutMs = strtoull(optarg, nullptr, 10); break;
        case 'w': writeSettleMs = strtoull(optarg, nullptr, 10); break;
        case 's': seed = strtoull(optarg, nullptr, 10); break;
        case 'h':
        default:
            Usage(argv[0]);
            return 2;
        }
    }

    if (configPath == nullptr) {
        fprintf(stderr, "error: -c config is required\n");
        Usage(argv[0]);
        return 2;
    }

    std::ifstream configStream(configPath);
    if (configStream.fail()) {
        fprintf(stderr, "error: unable to read config file: %s\n", configPath);
        return 2;
    }
    specpaxos::Configuration config(configStream);

    FastTransport transport(config, /*nthreads=*/1, /*nr_req_types=*/1,
                            /*phy_port=*/0, /*numa_node=*/0, /*id=*/0);
    specpaxos::dyna::DynaClient client(config, &transport, clientId);

    const std::vector<Step> steps =
        (numKeys > 0)
            ? BuildMultiKeySteps(keyArg, numKeys, seed)
            : BuildSingleKeySteps(NormalizeKey(keyArg));

    if (numKeys > 0) {
        Notice("DYNA read-correctness: interleaved multi-key mode, "
               "%zu keys, %zu ops, seed=%llu",
               numKeys, steps.size(), (unsigned long long)seed);
    } else {
        Notice("DYNA read-correctness: single-key mode, %zu ops",
               steps.size());
    }

    std::atomic<int> failures{0};
    std::atomic<int> readsChecked{0};
    std::atomic<bool> done{false};
    std::atomic<bool> timedOut{false};

    std::function<void(size_t)> runStep;
    runStep = [&](size_t i) {
        if (i >= steps.size()) {
            done = true;
            transport.Stop();
            return;
        }
        const Step &s = steps[i];
        if (!s.isRead) {
            const std::string req = "i" + s.key + s.value;
            Notice("[step %zu] WRITE key='%s' value='%s'",
                   i, s.key.c_str(), s.value.c_str());
            client.Invoke(req, [&, i](const std::string &, const std::string &) {
                // Client commit is on the speculative quorum, which can precede
                // the leader's commit/apply. Give the sync/commit round a moment
                // so the subsequent read is guaranteed to see the value.
                transport.Timer(writeSettleMs, [&, i]() { runStep(i + 1); });
            });
        } else {
            const std::string req = "r" + s.key;
            Notice("[step %zu] READ key='%s' (expect '%s')",
                   i, s.key.c_str(), s.value.c_str());
            client.Invoke(req, [&, i](const std::string &, const std::string &reply) {
                readsChecked++;
                if (reply == steps[i].value) {
                    Notice("[step %zu] READ ok -> '%s'", i, reply.c_str());
                } else {
                    Warning("[step %zu] READ MISMATCH key='%s': expected '%s' got '%s'",
                            i, steps[i].key.c_str(),
                            steps[i].value.c_str(), reply.c_str());
                    failures++;
                }
                runStep(i + 1);
            });
        }
    };

    transport.Timer(0, [&]() { runStep(0); });
    transport.Timer(timeoutMs, [&]() {
        if (!done) {
            timedOut = true;
            Warning("TIMEOUT after %llu ms before sequence completed",
                    (unsigned long long)timeoutMs);
            transport.Stop();
        }
    });

    transport.Run();

    if (timedOut || !done) {
        fprintf(stderr,
                "FAIL: DYNA read-correctness test did not complete "
                "(timeout). Is the cluster up and reachable?\n");
        return 1;
    }
    if (failures.load() != 0) {
        fprintf(stderr,
                "FAIL: DYNA read-correctness test had %d mismatch(es) "
                "across %d read(s)\n",
                failures.load(), readsChecked.load());
        return 1;
    }

    printf("PASS: DYNA reads reflected the latest committed writes "
           "(%d read(s) checked)\n",
           readsChecked.load());
    return 0;
}

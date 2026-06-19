// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _BENCHMARK_H_
#define _BENCHMARK_H_

#include "common/client.h"
#include "lib/latency.h"
#include "lib/transport.h"
#include <chrono>
#include <cstdint>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

namespace specpaxos {

class BenchmarkClient
{
public:
    static constexpr int kOpenLoopIntervalMs = 10;

    BenchmarkClient(Client &client, Transport &transport,
                    int numRequests, uint64_t delay,
                    int warmupSec,
                    int tputInterval,
                    string traceFile,
                    int experimentDuration,
                    bool logLatency,
                    string latencyFilename = "",
                    bool openLoop = false,
                    int poissonRate = 0,
                    uint64_t poissonSeed = 1,
                    double offeredOpsPerSec = 0.0,
                    size_t maxInflight = 0);
    void Start();
    void OnReply(const string &request, const string &reply);
    struct Latency_t latency;
    bool started;
    bool done;
    bool cooldownDone;
    int tputInterval;
    bool logLatency = true;
    std::vector<uint64_t> latencies;
    std::vector<char> opcodes;
    std::vector<std::string> messages;

private:
    void SendNext();
    void SubmitOne();
    void OpenLoopTick();
    void OnReplyOpen(uint64_t submitId, const string &request, const string &reply);
    void MaybeFinishExperiment();
    void Finish();
    void WarmupDone();
    void CooldownDone();
    void TimeInterval();
    bool ExperimentElapsed() const;
    bool MaySubmitMore() const;
    bool InflightCapReached() const;

    Client &client;
    Transport &transport;
    int numRequests;
    uint64_t delay;
    int n;
    int warmupSec;
    struct timeval startTime;
    struct timeval endTime;
    string traceFile;
    int experimentDuration;
    string latencyFilename;
    int msSinceStart;
    int opLastInterval;
    std::chrono::time_point<std::chrono::high_resolution_clock> expStartTime;

    bool openLoop_;
    int poissonRate_;
    double offeredOpsPerSec_;
    size_t maxInflight_;
    uint64_t submitSeq_;
    uint64_t totalSubmitted_;
    size_t poissonIdx_;
    std::vector<uint32_t> poissonTrace_;
    std::unordered_map<uint64_t, uint64_t> inflightStartNs_;
    // Per-client RNG for random key selection (seeded from poissonSeed so each
    // client thread draws a distinct, reproducible stream). Keeps the bench from
    // walking the keyspace sequentially without affecting offered throughput.
    std::mt19937_64 keyRng_;
};

} // namespace specpaxos

#endif /* _BENCHMARK_H_ */

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "bench/benchmark.h"

#include "common/client.h"
#include "lib/assert.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "lib/timeval.h"

#include <fcntl.h>
#include <random>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <sstream>
#include <string>

#define OPCODE_SIZE 1
#define KEY_SIZE 24
#define VAL_SIZE 100

namespace specpaxos {

DEFINE_LATENCY(op);

BenchmarkClient::BenchmarkClient(Client &client, Transport &transport,
                                 int numRequests, uint64_t delay,
                                 int warmupSec,
                                 int tputInterval,
                                 string traceFile,
                                 int experimentDuration,
                                 string latencyFilename,
                                 bool openLoop,
                                 int poissonRate,
                                 uint64_t poissonSeed)
    : tputInterval(tputInterval),
      client(client),
      transport(transport),
      numRequests(numRequests),
      delay(delay),
      warmupSec(warmupSec),
      traceFile(traceFile),
      experimentDuration(experimentDuration),
      latencyFilename(latencyFilename),
      requestInFlight(false),
      openLoop_(openLoop),
      poissonRate_(poissonRate),
      submitSeq_(0),
      totalSubmitted_(0),
      poissonIdx_(0)
{
    if (openLoop_) {
        if (poissonRate_ <= 0) {
            Panic("Open-loop mode requires poisson rate > 0 (-R)");
        }
        poissonTrace_.resize(1000, 0);
        std::default_random_engine generator(static_cast<uint32_t>(poissonSeed));
        std::poisson_distribution<int> distribution(poissonRate_);
        for (size_t i = 0; i < poissonTrace_.size(); ++i) {
            const int reqNum = distribution(generator);
            poissonTrace_[i] = reqNum > 0 ? static_cast<uint32_t>(reqNum) : 0;
        }
        const double offeredPerSec =
            (static_cast<double>(poissonRate_) * 1000.0) / kOpenLoopIntervalMs;
        Notice("Open-loop client: poisson rate %d reqs/%dms (~%.0f offered ops/s)",
               poissonRate_,
               kOpenLoopIntervalMs,
               offeredPerSec);
    } else if (delay != 0) {
        Notice("Closed-loop client: delay between requests %lu ms", delay);
    } else {
        Notice("Closed-loop client: send next request after each completion");
    }

    started = false;
    done = false;
    cooldownDone = false;
    _Latency_Init(&latency, "op");
    latencies.reserve(openLoop_ ? 100000 : static_cast<size_t>(numRequests));
    opcodes.reserve(numRequests);

    Notice("Using tracefile: %s", traceFile.c_str());
    int traceFd = open(traceFile.c_str(), O_RDONLY);
    assert(traceFd);
    struct stat stat_buf;
    int rc = 0;
    assert(!(rc = stat(traceFile.c_str(), &stat_buf)));
    void *vaddr = mmap(NULL, stat_buf.st_size, PROT_READ, MAP_SHARED, traceFd, 0);
    size_t off = 0;
    char *curr = (char *)vaddr;
    while (off < (uint32_t)stat_buf.st_size) {
        char *op_ptr = curr;
        curr += OPCODE_SIZE + 1;
        off += OPCODE_SIZE + 1;

        char *key_ptr = curr;
        curr += KEY_SIZE + 1;
        off += KEY_SIZE + 1;

        string op = std::string(op_ptr, OPCODE_SIZE);
        string key = std::string(key_ptr, KEY_SIZE);
        assert(op.length() == OPCODE_SIZE);
        assert(key.length() == KEY_SIZE);
        if (op[0] == 'r' || op[0] == 'R') {
            continue;
        }
        operations.push_back(std::make_pair(op, key));

        if (operations.size() >= (uint32_t)numRequests + 100) {
            break;
        }
    }
    munmap(vaddr, stat_buf.st_size);
    close(traceFd);

    if (operations.empty()) {
        Panic("No write operations in tracefile %s", traceFile.c_str());
    }

    Notice("Loaded %lu trace ops (numrequests = %d, cycles trace) from tracefile: %s",
           operations.size(),
           numRequests,
           traceFile.c_str());
}

string
BenchmarkClient::BuildRequest(size_t opIdx) const
{
    const auto &opPair = operations[opIdx % operations.size()];
    const char op = opPair.first[0];
    const bool isRead = op == 'r' || op == 'R';
    const bool isUpdate = op == 'u' || op == 'U';
    const bool nonNilext = op == 'e' || op == 'E';

    std::ostringstream full;
    full << opPair.first << opPair.second;
    if (!isRead) {
        if (isUpdate || nonNilext) {
            full << string(VAL_SIZE, 'x');
        } else {
            full << string(VAL_SIZE, 'v');
        }
    }
    return full.str();
}

bool
BenchmarkClient::ExperimentElapsed() const
{
    if (!started) {
        return false;
    }
    const auto now = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::seconds>(now - expStartTime).count() >=
           experimentDuration;
}

bool
BenchmarkClient::MaySubmitMore() const
{
    if (numRequests <= 0) {
        return true;
    }
    return totalSubmitted_ < static_cast<uint64_t>(numRequests);
}

void
BenchmarkClient::MaybeFinishExperiment()
{
    if (!started || done || cooldownDone) {
        return;
    }
    if (ExperimentElapsed()) {
        Notice("Experiment duration elapsed (submitted=%lu completed=%d).",
               static_cast<unsigned long>(totalSubmitted_),
               n);
        Finish();
    }
}

void
BenchmarkClient::Start()
{
    n = 0;
    totalSubmitted_ = 0;
    submitSeq_ = 0;
    poissonIdx_ = 0;
    inflightStartNs_.clear();
    requestInFlight = false;

    transport.Timer(warmupSec * 1000, std::bind(&BenchmarkClient::WarmupDone, this));

    if (tputInterval > 0) {
        msSinceStart = 0;
        opLastInterval = n;
        transport.Timer(tputInterval, std::bind(&BenchmarkClient::TimeInterval, this));
    }
    expStartTime = std::chrono::high_resolution_clock::now();

    if (openLoop_) {
        OpenLoopTick();
    } else {
        SendNext();
    }
}

void
BenchmarkClient::TimeInterval()
{
    if (done) {
        return;
    }

    struct timeval tv;
    gettimeofday(&tv, NULL);
    msSinceStart += tputInterval;
    Notice("Completed %d requests at %lu ms", n - opLastInterval, (((tv.tv_sec * 1000000 + tv.tv_usec) / 1000) / 10) * 10);
    opLastInterval = n;
    transport.Timer(tputInterval, std::bind(&BenchmarkClient::TimeInterval, this));
}

void
BenchmarkClient::WarmupDone()
{
    started = true;
    Notice("Completed warmup period of %d seconds with %d completions (%lu submitted)",
           warmupSec,
           n,
           static_cast<unsigned long>(totalSubmitted_));
    gettimeofday(&startTime, NULL);
    n = 0;
    opLastInterval = 0;
    expStartTime = std::chrono::high_resolution_clock::now();

    if (!openLoop_ && !requestInFlight) {
        SendNext();
    }
}

void
BenchmarkClient::CooldownDone()
{

    char buf[1024];
    cooldownDone = true;
    Notice("Finished cooldown period.");
    std::vector<uint64_t> sorted = latencies;
    std::sort(sorted.begin(), sorted.end());

    if (sorted.empty()) {
        Notice("No latency samples recorded.");
        return;
    }

    uint64_t ns = sorted[sorted.size() / 2];
    LatencyFmtNS(ns, buf);
    Notice("Median latency is %ld ns (%s)", ns, buf);

    ns = 0;
    for (auto latency : sorted) {
        ns += latency;
    }
    ns = ns / sorted.size();
    LatencyFmtNS(ns, buf);
    Notice("Average latency is %ld ns (%s)", ns, buf);

    ns = sorted[sorted.size() * 90 / 100];
    LatencyFmtNS(ns, buf);
    Notice("90th percentile latency is %ld ns (%s)", ns, buf);

    ns = sorted[sorted.size() * 95 / 100];
    LatencyFmtNS(ns, buf);
    Notice("95th percentile latency is %ld ns (%s)", ns, buf);

    ns = sorted[sorted.size() * 99 / 100];
    LatencyFmtNS(ns, buf);
    Notice("99th percentile latency is %ld ns (%s)", ns, buf);
}

void
BenchmarkClient::SubmitOne()
{
    if (operations.empty() || !MaySubmitMore()) {
        return;
    }
    if (!openLoop_ && requestInFlight) {
        return;
    }

    const size_t opIdx = static_cast<size_t>(totalSubmitted_);
    const string request = BuildRequest(opIdx);
    const char op = request[0];
    const uint64_t submitId = ++submitSeq_;
    totalSubmitted_++;

    opcodes.push_back(op);

    if (openLoop_) {
        const auto now = std::chrono::steady_clock::now();
        inflightStartNs_[submitId] =
            std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
        client.Invoke(request,
                      [this, submitId](const string &req, const string &reply) {
                          OnReplyOpen(submitId, req, reply);
                      });
    } else {
        Latency_Start(&latency);
        requestInFlight = true;
        client.Invoke(request,
                      std::bind(&BenchmarkClient::OnReply,
                                this,
                                std::placeholders::_1,
                                std::placeholders::_2));
    }
}

void
BenchmarkClient::OpenLoopTick()
{
    if (done || cooldownDone) {
        return;
    }

    const uint32_t reqNum = poissonTrace_[poissonIdx_ % poissonTrace_.size()];
    poissonIdx_++;

    for (uint32_t j = 0; j < reqNum; ++j) {
        if (!MaySubmitMore()) {
            break;
        }
        SubmitOne();
    }

    MaybeFinishExperiment();
    if (!done) {
        transport.Timer(kOpenLoopIntervalMs,
                        std::bind(&BenchmarkClient::OpenLoopTick, this));
    }
}

void
BenchmarkClient::SendNext()
{
    if (operations.empty()) {
        return;
    }
    if (started && n >= numRequests) {
        return;
    }
    SubmitOne();
}

void
BenchmarkClient::OnReplyOpen(uint64_t submitId, const string &request, const string &reply)
{
    (void)request;
    (void)reply;
    if (cooldownDone) {
        return;
    }

    auto it = inflightStartNs_.find(submitId);
    if (it != inflightStartNs_.end()) {
        const auto now = std::chrono::steady_clock::now();
        const uint64_t endNs =
            std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
        latencies.push_back(endNs - it->second);
        inflightStartNs_.erase(it);
    }

    n++;
    if (started && !done) {
        MaybeFinishExperiment();
    }
}

void
BenchmarkClient::OnReply(const string &request, const string &reply)
{
    (void)request;
    (void)reply;
    if (cooldownDone) {
        return;
    }

    requestInFlight = false;
    n++;
    if ((started) && (!done) && (n != 0)) {
        uint64_t ns = Latency_End(&latency);
        latencies.push_back(ns);
        if (n >= numRequests) {
            Finish();
        }

        MaybeFinishExperiment();
        if (done) {
            return;
        }
    }

    if (delay == 0) {
        SendNext();
    } else {
        uint64_t rdelay = rand() % delay * 2;
        transport.Timer(rdelay, std::bind(&BenchmarkClient::SendNext, this));
    }
}

void
BenchmarkClient::Finish()
{
    if (done) {
        return;
    }
    gettimeofday(&endTime, NULL);

    struct timeval diff = timeval_sub(endTime, startTime);

    Notice("Completed %d requests in " FMT_TIMEVAL_DIFF " seconds (submitted=%lu)",
           n,
           VA_TIMEVAL_DIFF(diff),
           static_cast<unsigned long>(totalSubmitted_));
    done = true;

    transport.Timer(warmupSec * 1000, std::bind(&BenchmarkClient::CooldownDone, this));

    if (latencyFilename.size() > 0) {
        Latency_FlushTo(latencyFilename.c_str());
    }
}

} // namespace specpaxos

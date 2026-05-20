// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * client.cpp:
 *   test instantiation of a client application
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/udptransport.h"

#include "bench/benchmark.h"
#include "common/client.h"
#include "lib/configuration.h"

#include "vr/client.h"

#include <unistd.h>
#include <stdlib.h>
#include <algorithm>
#include <fstream>

static void
Usage(const char *progName)
{
        fprintf(stderr,
                "usage: %s [-n max-requests] [-t clients] [-w warmup-secs] [-l latency-file] "
                "[-q dscp] [-d delay-ms] [-O] [-R poisson-rate] -c conf-file -m vr\n"
                "  default: closed-loop; -O -R: open-loop (rate = mean reqs per 10ms)\n",
                progName);
        exit(1);
}

void
PrintReply(const string &request, const string &reply)
{
    Notice("Request succeeded; got response %s", reply.c_str());
}

int main(int argc, char **argv)
{
    const char *configPath = NULL;
    int numClients = 1;
    int numRequests = 1000;
    int warmupSec = 0;
    int dscp = 0;
    uint64_t delay = 0;
    uint64_t experimentDuration = 60; // 60 second experiments by default
    int tputInterval = 0;
    bool openLoop = false;
    int poissonRate = 0;

    enum
    {
        PROTO_UNKNOWN,
        PROTO_VR
    } proto = PROTO_UNKNOWN;

    string latencyFile;
    string latencyRawFile;
    string traceFile;

    // Parse arguments
    int opt;
    while ((opt = getopt(argc, argv, "c:d:e:q:k:l:m:n:t:w:i:OR:")) != -1) {
        switch (opt) {
        case 'c':
            configPath = optarg;
            break;

        case 'd':
        {
            char *strtolPtr;
            delay = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -d requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'e':
        {
            char *strtolPtr;
            experimentDuration = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -e requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'q':
        {
            char *strtolPtr;
            dscp = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (dscp < 0))
            {
                fprintf(stderr,
                        "option -q requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'k':
            traceFile = string(optarg);
            break;

        case 'l':
            latencyFile = string(optarg);
            break;

        case 'm':
            if (strcasecmp(optarg, "vr") == 0) {
                proto = PROTO_VR;
            } else {
                fprintf(stderr, "unknown mode '%s'\n", optarg);
                Usage(argv[0]);
            }
            break;

        case 'n':
        {
            char *strtolPtr;
            numRequests = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -n requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 't':
        {
            char *strtolPtr;
            numClients = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (numClients <= 0))
            {
                fprintf(stderr,
                        "option -t requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'w':
        {
            char *strtolPtr;
            warmupSec = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -w requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

	case 'i':
        {
            char *strtolPtr;
            tputInterval = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -d requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'O':
            openLoop = true;
            break;

        case 'R':
        {
            char *strtolPtr;
            poissonRate = static_cast<int>(strtoul(optarg, &strtolPtr, 10));
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (poissonRate <= 0))
            {
                fprintf(stderr, "option -R requires a positive numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        default:
            fprintf(stderr, "Unknown argument %s\n", argv[optind]);
            Usage(argv[0]);
            break;
        }
    }

    if (!configPath) {
        fprintf(stderr, "option -c is required\n");
        Usage(argv[0]);
    }
    if (proto == PROTO_UNKNOWN) {
        fprintf(stderr, "option -m is required\n");
        Usage(argv[0]);
    }
    if (openLoop && poissonRate <= 0) {
        fprintf(stderr, "open-loop mode (-O) requires -R <poisson-rate>\n");
        Usage(argv[0]);
    }
    if (openLoop && delay != 0) {
        fprintf(stderr, "open-loop mode (-O) cannot be used with -d delay\n");
        Usage(argv[0]);
    }
    if (!openLoop && numRequests <= 0) {
        fprintf(stderr, "closed-loop mode requires -n <max-requests> > 0\n");
        Usage(argv[0]);
    }

    // Load configuration
    std::ifstream configStream(configPath);
    if (configStream.fail()) {
        fprintf(stderr, "unable to read configuration file: %s\n",
                configPath);
        Usage(argv[0]);
    }
    specpaxos::Configuration config(configStream);

    UDPTransport transport(0, 0, dscp);
    std::vector<specpaxos::Client *> clients;
    std::vector<specpaxos::BenchmarkClient *> benchClients;

    const int perClientPoisson =
        openLoop ? std::max(1, (poissonRate + numClients - 1) / std::max(1, numClients)) : 0;

    for (int i = 0; i < numClients; i++) {
        specpaxos::Client *client;
        switch (proto) {
        case PROTO_VR:
            client = new specpaxos::vr::VRClient(config, &transport);
            break;

        default:
            NOT_REACHABLE();
        }

        const uint64_t poissonSeed = static_cast<uint64_t>(i + 1);
        specpaxos::BenchmarkClient *bench = new specpaxos::BenchmarkClient(
            *client,
            transport,
            numRequests,
            delay,
            warmupSec,
            tputInterval,
            traceFile,
            static_cast<int>(experimentDuration),
            latencyFile,
            openLoop,
            perClientPoisson,
            poissonSeed);

        transport.Timer(0, [=]() { bench->Start(); });
        clients.push_back(client);
        benchClients.push_back(bench);
    }

    Timeout checkTimeout(&transport, 100, [&]() {
            for (auto x : benchClients) {
                if (!x->cooldownDone) {
                    return;
                }
            }
            Notice("All clients done.");

            Latency_t sum;
            _Latency_Init(&sum, "total");
            for (unsigned int i = 0; i < benchClients.size(); i++) {
                Latency_Sum(&sum, &benchClients[i]->latency);
            }
            Latency_Dump(&sum);
            if (latencyFile.size() > 0) {
                Latency_FlushTo(latencyFile.c_str());
            }

            latencyRawFile = latencyFile+".raw";
            std::ofstream rawFile(latencyRawFile.c_str());        

            for (auto x : benchClients) {
            	int index = 0;
                for (const auto &e : x->latencies) {
                	rawFile << x->opcodes[index++] << e << "\n";
                }
            }
            rawFile.close();
            exit(0);
        });
    checkTimeout.Start();

    transport.Run();
}

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "lib/cpu_affinity.h"

#include "lib/message.h"

#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include <cctype>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace {

std::string Trim(const std::string &s)
{
    size_t i = 0, j = s.size();
    while (i < j && std::isspace(static_cast<unsigned char>(s[i]))) {
        ++i;
    }
    while (j > i && std::isspace(static_cast<unsigned char>(s[j - 1]))) {
        --j;
    }
    return s.substr(i, j - i);
}

std::vector<int> ParseCpulistString(const std::string &line)
{
    std::vector<int> out;
    std::string s = Trim(line);
    if (s.empty()) {
        return out;
    }
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
        item = Trim(item);
        if (item.empty()) {
            continue;
        }
        size_t dash = item.find('-');
        if (dash == std::string::npos) {
            out.push_back(std::stoi(item));
        } else {
            int a = std::stoi(item.substr(0, dash));
            int b = std::stoi(item.substr(dash + 1));
            for (int i = a; i <= b; ++i) {
                out.push_back(i);
            }
        }
    }
    return out;
}

std::vector<int> ReadCpuListFile(const std::string &path)
{
    std::ifstream in(path);
    if (!in) {
        return {};
    }
    std::string line;
    std::getline(in, line);
    return ParseCpulistString(line);
}

std::vector<int> CpusOnNumaNode(unsigned numa_node)
{
    std::string path = "/sys/devices/system/node/node" +
                       std::to_string(numa_node) + "/cpulist";
    std::vector<int> cpus = ReadCpuListFile(path);
    if (!cpus.empty()) {
        return cpus;
    }
    // No NUMA topology or missing node: treat node 0 as all online CPUs.
    if (numa_node == 0) {
        cpus = ReadCpuListFile("/sys/devices/system/cpu/present");
    }
    return cpus;
}

}  // namespace

std::size_t CpuCountOnNumaNode(unsigned numa_node)
{
    std::vector<int> cpus = CpusOnNumaNode(numa_node);
    if (!cpus.empty()) {
        return cpus.size();
    }
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n < 1) {
        return 1;
    }
    return static_cast<std::size_t>(n);
}

void BindThreadToNumaCpu(std::thread &thread, unsigned numa_node,
                         std::size_t cpu_index_on_node)
{
    std::vector<int> cpus = CpusOnNumaNode(numa_node);
    if (cpus.empty()) {
        return;
    }
    int cpu = cpus[cpu_index_on_node % cpus.size()];
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(static_cast<unsigned>(cpu), &cpuset);
    int rc = pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t),
                                    &cpuset);
    if (rc != 0) {
        Warning("pthread_setaffinity_np failed for cpu %d: %d", cpu, rc);
    }
}

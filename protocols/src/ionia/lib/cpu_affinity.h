// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
#pragma once

#include <cstddef>
#include <thread>

// Linux: CPU list from sysfs NUMA node; falls back to online CPUs when sysfs
// has no per-node list. Replaces eRPC helpers for UDP builds.
std::size_t CpuCountOnNumaNode(unsigned numa_node);
void BindThreadToNumaCpu(std::thread &thread, unsigned numa_node,
                         std::size_t cpu_index_on_node);

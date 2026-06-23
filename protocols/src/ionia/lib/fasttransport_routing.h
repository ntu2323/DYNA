// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * fasttransport_routing.h:
 *   Key-hash endpoint selection for IONIA/VR multi-threaded durability.
 *   Only used when FastTransport is constructed with ionia_multithread=true.
 *
 **********************************************************************/

#ifndef _LIB_FASTTRANSPORT_ROUTING_H_
#define _LIB_FASTTRANSPORT_ROUTING_H_

#include <google/protobuf/message.h>
#include <cstdint>
#include <string_view>

namespace fasttransport_routing {

// Matches AppReplica / vr/replica.cc bench trace layout.
constexpr std::size_t kBenchOpLength = 1;
constexpr std::size_t kBenchKeyLength = 24;

inline uint64_t
Djb2Hash(std::string_view key)
{
    uint64_t hash = 5381;
    for (const char c : key) {
        hash = ((hash << 5) + hash) + static_cast<unsigned char>(c);
    }
    return hash;
}

inline std::string_view
BenchKeyFromOp(std::string_view op)
{
    if (op.size() < kBenchOpLength + kBenchKeyLength) {
        return {};
    }
    return op.substr(kBenchOpLength, kBenchKeyLength);
}

uint8_t DstEndpointForMessage(const google::protobuf::Message &m, int num_endpoints);

} // namespace fasttransport_routing

#endif  // _LIB_FASTTRANSPORT_ROUTING_H_

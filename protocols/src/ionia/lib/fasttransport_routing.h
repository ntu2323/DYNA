// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * fasttransport_routing.h:
 *   Key-hash durability endpoint selection for multithreaded IONIA UDP.
 *   Matches vr/replica.cc execution-queue sharding only; does not change
 *   protocol semantics.
 *
 **********************************************************************/

#ifndef _LIB_FASTTRANSPORT_ROUTING_H_
#define _LIB_FASTTRANSPORT_ROUTING_H_

#include <cstdint>
#include <string_view>

namespace google {
namespace protobuf {
class Message;
}
}

namespace FastTransportRouting {

// Bench trace layout (specpaxos::AppReplica::opLength / keyLength).
constexpr std::size_t kBenchOpLength = 1;
constexpr std::size_t kBenchKeyLength = 24;

uint32_t DurabilityEndpointHash(std::string_view key);
uint8_t DurabilityEndpointForKey(std::string_view key, int num_endpoints);

// For client RequestMessage / UnloggedRequestMessage, set endpoint_out to
// hash(key) % num_endpoints (same formula as vr/replica.cc). Returns false
// for other message types; caller should use endpoint 0.
bool DurabilityEndpointForMessage(const google::protobuf::Message &m,
                                  int num_endpoints,
                                  uint8_t &endpoint_out);

}  // namespace FastTransportRouting

#endif  // _LIB_FASTTRANSPORT_ROUTING_H_

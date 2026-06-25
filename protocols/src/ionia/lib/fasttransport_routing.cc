// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * fasttransport_routing.cc:
 *   Key-hash durability endpoint selection for multithreaded IONIA UDP.
 *
 **********************************************************************/

#include "lib/fasttransport_routing.h"

#include "vr/vr-proto.pb.h"

#include <google/protobuf/message.h>

#include <algorithm>
#include <string>

namespace FastTransportRouting {

uint32_t
DurabilityEndpointHash(std::string_view key)
{
    // Match vr/replica.cc (queueIndex seed 0, not djb2's 5381).
    uint32_t queue_index = 0;
    for (const auto c : key) {
        queue_index = ((queue_index << 5) + queue_index) +
                      static_cast<unsigned char>(c);
    }
    return queue_index;
}

uint8_t
DurabilityEndpointForKey(std::string_view key, int num_endpoints)
{
    if (num_endpoints <= 1) {
        return 0;
    }
    const auto hash = DurabilityEndpointHash(key);
    return static_cast<uint8_t>(hash % static_cast<uint32_t>(num_endpoints));
}

static bool
EndpointForOpPayload(std::string_view op_data,
                     int num_endpoints,
                     uint8_t &endpoint_out)
{
    if (num_endpoints <= 1) {
        endpoint_out = 0;
        return true;
    }
    if (op_data.size() < kBenchOpLength + kBenchKeyLength) {
        endpoint_out = 0;
        return false;
    }
    const auto key =
        op_data.substr(kBenchOpLength, kBenchKeyLength);
    endpoint_out = DurabilityEndpointForKey(key, num_endpoints);
    return true;
}

bool
DurabilityEndpointForMessage(const google::protobuf::Message &m,
                             int num_endpoints,
                             uint8_t &endpoint_out)
{
    using specpaxos::vr::proto::RequestMessage;
    using specpaxos::vr::proto::UnloggedRequestMessage;

    const static std::string request_name =
        RequestMessage::default_instance().GetDescriptor()->full_name();
    const static std::string unlogged_request_name =
        UnloggedRequestMessage::default_instance().GetDescriptor()->full_name();

    const auto &type_name = m.GetTypeName();
    if (type_name == request_name) {
        const auto &request = dynamic_cast<const RequestMessage &>(m);
        return EndpointForOpPayload(request.req().op(), num_endpoints, endpoint_out);
    }
    if (type_name == unlogged_request_name) {
        const auto &request = dynamic_cast<const UnloggedRequestMessage &>(m);
        return EndpointForOpPayload(request.req().op(), num_endpoints, endpoint_out);
    }
    return false;
}

}  // namespace FastTransportRouting

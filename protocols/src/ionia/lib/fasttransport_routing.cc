// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "lib/fasttransport_routing.h"

#include "vr/vr-proto.pb.h"

#include <string>

namespace fasttransport_routing {

namespace {

const static std::string kRequestMessageName =
    specpaxos::vr::proto::RequestMessage::default_instance().GetDescriptor()->full_name();
const static std::string kUnloggedRequestMessageName =
    specpaxos::vr::proto::UnloggedRequestMessage::default_instance().GetDescriptor()->full_name();

uint8_t
EndpointFromKey(std::string_view key, int num_endpoints)
{
    if (num_endpoints <= 1 || key.empty()) {
        return 0;
    }
    return static_cast<uint8_t>(Djb2Hash(key) % static_cast<uint64_t>(num_endpoints));
}

} // namespace

uint8_t
DstEndpointForMessage(const google::protobuf::Message &m, int num_endpoints)
{
    if (num_endpoints <= 1) {
        return 0;
    }

    const std::string &type_name = m.GetTypeName();
    if (type_name == kRequestMessageName) {
        specpaxos::vr::proto::RequestMessage parsed;
        if (!parsed.ParseFromString(m.SerializeAsString())) {
            return 0;
        }
        return EndpointFromKey(BenchKeyFromOp(parsed.req().op()), num_endpoints);
    }
    if (type_name == kUnloggedRequestMessageName) {
        specpaxos::vr::proto::UnloggedRequestMessage parsed;
        if (!parsed.ParseFromString(m.SerializeAsString())) {
            return 0;
        }
        return EndpointFromKey(BenchKeyFromOp(parsed.req().op()), num_endpoints);
    }
    return 0;
}

} // namespace fasttransport_routing

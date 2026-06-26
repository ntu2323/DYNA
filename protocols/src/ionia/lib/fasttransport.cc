// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * fasttransport.cc:
 *   compatibility transport that preserves the FastTransport API while
 *   using UDPTransport as the backend.
 *
 **********************************************************************/

#include "lib/fasttransport.h"

#include "lib/assert.h"
#include "lib/fasttransport_endpoint.h"
#include "lib/fasttransport_routing.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <ifaddrs.h>
#include <net/if.h>

#include <algorithm>
#include <string_view>
#include <utility>

namespace {

int
ReplicaIndexForDestination(const specpaxos::Configuration &config,
                           const UDPTransportAddress &dst)
{
    for (int replica_idx = 0; replica_idx < config.n; ++replica_idx) {
        const auto &rep = config.replica(0, replica_idx);
        struct sockaddr_in sa{};
        inet_pton(AF_INET, rep.host.c_str(), &(sa.sin_addr));
        sa.sin_port = htons(static_cast<uint16_t>(std::stoi(rep.port)));
        if (dst == UDPTransportAddress(sa)) {
            return replica_idx;
        }
    }
    return -1;
}

int
DurabilityEndpointsForDestination(const specpaxos::Configuration &config,
                                  const UDPTransportAddress &dst,
                                  int fallback_endpoints)
{
    const int replica_idx = ReplicaIndexForDestination(config, dst);
    if (replica_idx < 0) {
        return std::max(1, fallback_endpoints);
    }
    return std::max(1, config.replica(0, replica_idx).threads);
}

}  // namespace

FastTransport::FastTransport(const specpaxos::Configuration &config,
                             int nthreads,
                             uint8_t nr_req_types,
                             uint8_t phy_port,
                             uint8_t numa_node,
                             uint8_t id,
                             uint64_t send_buf_size,
                             uint64_t receive_buf_size,
                             bool ionia_multithread)
    : config(config),
      udp_transport(std::make_unique<UDPTransport>(0.0, 0.0, 0, nullptr)),
      endpoint_id_(id),
      nthreads_(nthreads),
      ionia_multithread_(ionia_multithread)
{
    (void)nr_req_types;
    (void)phy_port;
    (void)numa_node;
    (void)send_buf_size;
    (void)receive_buf_size;
    udp_transport->SetEndpointId(endpoint_id_);
    udp_transport->EnableSharedPort(ionia_multithread_);
}

FastTransport::~FastTransport() = default;

std::string FastTransport::GetLocalAddress()
{
    struct ifaddrs *addrs = nullptr;
    getifaddrs(&addrs);
    std::string ip_address;
    for (struct ifaddrs *iap = addrs; iap != nullptr; iap = iap->ifa_next) {
        if (iap->ifa_addr && (iap->ifa_flags & IFF_UP) &&
            iap->ifa_addr->sa_family == AF_INET) {
            auto *sa = reinterpret_cast<struct sockaddr_in *>(iap->ifa_addr);
            char ip_addr[32];
            inet_ntop(iap->ifa_addr->sa_family, &(sa->sin_addr), ip_addr, sizeof(ip_addr));

            auto ip_addr_view = std::string_view(ip_addr);
            if (ip_addr_view.substr(0, 3) == "10.") {
                ip_address = std::string(ip_addr_view);
                break;
            }
            if (ip_addr_view.substr(0, 3) == "127") {
                continue;
            }
            ip_address = std::string(ip_addr_view);
            break;
        }
    }
    freeifaddrs(addrs);
    if (ip_address.empty()) {
        Panic("no local ip found");
    }
    return ip_address;
}

void FastTransport::Register(TransportReceiver *receiver,
                             const specpaxos::Configuration &config,
                             int groupIdx,
                             int replicaIdx)
{
    RegisterConfiguration(receiver, config, groupIdx, replicaIdx);
    udp_transport->Register(receiver, config, groupIdx, replicaIdx);
}

void FastTransport::Run()
{
    if (!ionia_multithread_) {
        udp_transport->Run();
        return;
    }
    stopped_.store(false);
    while (!stopped_.load(std::memory_order_relaxed)) {
        RunOnce();
    }
}

void FastTransport::RunOnce()
{
    udp_transport->RunOnce();
    if (on_event_loop_function) {
        on_event_loop_function();
    }
}

void FastTransport::Wait()
{
}

void FastTransport::Stop()
{
    if (ionia_multithread_) {
        stopped_.store(true);
    }
    udp_transport->Stop();
}

int FastTransport::Timer(uint64_t ms, timer_callback_t cb)
{
    return udp_transport->Timer(ms, std::move(cb));
}

int FastTransport::TimerUS(uint64_t us, timer_callback_t cb)
{
    return udp_transport->TimerUS(us, std::move(cb));
}

bool FastTransport::CancelTimer(int id)
{
    return udp_transport->CancelTimer(id);
}

void FastTransport::CancelAllTimers()
{
    udp_transport->CancelAllTimers();
}

int FastTransport::ManualEvent(timer_callback_t cb)
{
    return udp_transport->ManualEvent(std::move(cb));
}

bool FastTransport::TriggerManualEvent(int id)
{
    return udp_transport->TriggerManualEvent(id);
}

bool FastTransport::SendMessageInternal(TransportReceiver *src,
                                        const UDPTransportAddress &dst,
                                        const Message &m)
{
    if (!ionia_multithread_) {
        return udp_transport->SendMessage(src, dst, m);
    }

    uint8_t dst_endpoint = 0;
    const int num_endpoints =
        DurabilityEndpointsForDestination(config, dst, nthreads_);
    if (!FastTransportRouting::DurabilityEndpointForMessage(
            m, num_endpoints, dst_endpoint)) {
        dst_endpoint = 0;
    }

    const auto meta = MakeFastTransportEndpointMeta(endpoint_id_, dst_endpoint);
    return udp_transport->SendWithMeta(src, dst, m, &meta, sizeof(meta));
}

UDPTransportAddress FastTransport::LookupAddress(const specpaxos::ReplicaAddress &addr)
{
    struct sockaddr_in sa{};
    inet_pton(AF_INET, addr.host.c_str(), &(sa.sin_addr));
    sa.sin_port = htons(static_cast<uint16_t>(std::stoi(addr.port)));
    return UDPTransportAddress(sa);
}

UDPTransportAddress FastTransport::LookupAddress(const specpaxos::Configuration &config,
                                                 int groupIdx,
                                                 int replicaIdx)
{
    const specpaxos::ReplicaAddress &addr = config.replica(groupIdx, replicaIdx);
    return LookupAddress(addr);
}

const UDPTransportAddress *FastTransport::LookupMulticastAddress(const specpaxos::Configuration *config)
{
    if (!config->multicast()) {
        return nullptr;
    }
    return new UDPTransportAddress(LookupAddress(*(config->multicast())));
}

const UDPTransportAddress *FastTransport::LookupFCAddress(const specpaxos::Configuration *config)
{
    if (!config->fc()) {
        return nullptr;
    }
    return new UDPTransportAddress(LookupAddress(*(config->fc())));
}

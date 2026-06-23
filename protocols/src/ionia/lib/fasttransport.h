// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * fasttransport.h:
 *   compatibility transport that preserves the FastTransport API while
 *   using UDPTransport as the backend.
 *
 **********************************************************************/

#ifndef _LIB_FASTTRANSPORT_H_
#define _LIB_FASTTRANSPORT_H_

#include "lib/configuration.h"
#include "lib/udptransport.h"
#include <atomic>
#include <memory>
#include <optional>

class FastTransport : public TransportCommon<UDPTransportAddress>
{
public:
    FastTransport(const specpaxos::Configuration &config,
                  int nthreads,
                  uint8_t nr_req_types,
                  uint8_t phy_port,
                  uint8_t numa_node,
                  uint8_t id,
                  uint64_t send_buf_size = 0,
                  uint64_t receive_buf_size = 0,
                  bool ionia_multithread = false);
    virtual ~FastTransport();
    virtual void Register(TransportReceiver *receiver,
                          const specpaxos::Configuration &config,
                          int groupIdx,
                          int replicaIdx) override;
    void Run() override;
    void RunOnce() override;
    void Wait();
    void Stop() override;
    int Timer(uint64_t ms, timer_callback_t cb) override;
    int TimerUS(uint64_t us, timer_callback_t cb) override;
    bool CancelTimer(int id) override;
    void CancelAllTimers() override;

    int ManualEvent(timer_callback_t cb) override;
    bool TriggerManualEvent(int id) override;
    static std::string GetLocalAddress();

private:
    specpaxos::Configuration config;
    std::unique_ptr<UDPTransport> udp_transport;
    uint8_t endpoint_id_;
    int nthreads_;
    bool ionia_multithread_;
    std::atomic<bool> stopped_{false};

    std::optional<std::pair<int, int>>
    LookupReplicaForAddress(TransportReceiver *src, const UDPTransportAddress &dst);
    uint8_t ResolveDstEndpoint(TransportReceiver *src,
                               const UDPTransportAddress &dst,
                               const Message &m);

    bool SendMessageInternal(TransportReceiver *src,
                             const UDPTransportAddress &dst,
                             const Message &m) override;

    UDPTransportAddress LookupAddress(const specpaxos::ReplicaAddress &addr);
    UDPTransportAddress LookupAddress(const specpaxos::Configuration &cfg, int groupIdx, int replicaIdx) override;
    const UDPTransportAddress* LookupMulticastAddress(const specpaxos::Configuration *cfg) override;
    const UDPTransportAddress* LookupFCAddress(const specpaxos::Configuration *cfg) override;
};

#endif  // _LIB_FASTTRANSPORT_H_

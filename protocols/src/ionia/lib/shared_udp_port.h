// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * shared_udp_port.h:
 *   One UDP bind per host:port with many logical FastTransport endpoints.
 *
 **********************************************************************/

#ifndef _LIB_SHARED_UDP_PORT_H_
#define _LIB_SHARED_UDP_PORT_H_

#include "lib/fasttransport_endpoint.h"
#include "lib/transport.h"
#include "lib/udptransport.h"

#include <event2/event.h>

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

class SharedUDPPort {
public:
    static bool SharedPortsEnabled();

    // When IONIA_PER_THREAD_SOCKET is set, each Acquire() returns a private
    // instance bound with SO_REUSEPORT instead of a shared singleton, so every
    // durability thread receives and processes on its own socket + event loop.
    static bool PerThreadSocketEnabled();

    static SharedUDPPort *Acquire(const std::string &host,
                                  const std::string &port,
                                  int dscp);
    void Release();

    event_base *EventBase() const { return evbase_; }
    int Fd() const { return fd_; }
    bool PerThread() const { return per_thread_; }

    void RegisterEndpoint(uint8_t endpoint_id, TransportReceiver *receiver);
    void UnregisterEndpoint(uint8_t endpoint_id, TransportReceiver *receiver);

    void RunOnce();
    // Blocking pump: sleeps in the event loop (epoll) until an event/timer
    // fires or a short cap elapses, instead of returning immediately. The
    // per-thread pump loop uses this so each thread does not burn a whole core
    // spinning; busy-spinning every thread oversubscribes the CPU and adds tens
    // to hundreds of microseconds of scheduling latency to replies.
    void RunBlockingOnce();
    // IONIA_BUSY_POLL=1 restores the old non-blocking spin pump (for A/B).
    static bool BusyPollEnabled();
    void Stop();

private:
    struct PortKey {
        std::string host;
        uint16_t port;

        bool operator<(const PortKey &other) const
        {
            if (host != other.host) {
                return host < other.host;
            }
            return port < other.port;
        }
    };

    struct EndpointEntry {
        TransportReceiver *receiver = nullptr;
        int registrations = 0;
    };

    SharedUDPPort(const std::string &host, const std::string &port, int dscp);
    ~SharedUDPPort();

    void EnsureBound();
    void OnReadable();
    void ProcessPacket(sockaddr_in sender, char *buf, ssize_t sz);
    void DeliverMessage(uint8_t endpoint_id,
                        const UDPTransportAddress &sender,
                        const std::string &type,
                        const std::string &msg,
                        void *meta_data);

    static PortKey MakeKey(const std::string &host, const std::string &port);
    static void SocketCallback(evutil_socket_t fd, short what, void *arg);

    std::string host_;
    std::string port_str_;
    int dscp_;
    int fd_;
    event_base *evbase_;
    event *read_event_;
    std::map<uint8_t, EndpointEntry> endpoints_;
    std::mutex mutex_;
    int refcnt_;
    bool per_thread_ = false;

    struct FragInfo {
        uint64_t msgId = 0;
        std::string data;
    };
    std::map<UDPTransportAddress, FragInfo> frag_info_;

    static std::mutex registry_mutex_;
    static std::map<PortKey, SharedUDPPort *> registry_;
};

#endif  // _LIB_SHARED_UDP_PORT_H_

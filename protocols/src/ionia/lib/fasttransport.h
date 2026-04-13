// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * udptransport.h:
 *   message-passing network interface that uses UDP message delivery
 *   and libasync
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *           2018 Adriana Szekeres  <aaasz@cs.washington.edu>
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

#ifndef _LIB_FASTTRANSPORT_H_
#define _LIB_FASTTRANSPORT_H_

#include "lib/configuration.h"
#include "lib/udptransport.h"
#include <memory>

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
                  uint64_t receive_buf_size = 0);
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

    bool SendMessageInternal(TransportReceiver *src,
                             const UDPTransportAddress &dst,
                             const Message &m) override;

    UDPTransportAddress LookupAddress(const specpaxos::ReplicaAddress &addr);
    UDPTransportAddress LookupAddress(const specpaxos::Configuration &cfg, int groupIdx, int replicaIdx) override;
    const UDPTransportAddress* LookupMulticastAddress(const specpaxos::Configuration *cfg) override;
    const UDPTransportAddress* LookupFCAddress(const specpaxos::Configuration *cfg) override;
};

#endif  // _LIB_FASTTRANSPORT_H_

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * shared_udp_port.cc:
 *   Shared UDP listener with per-endpoint demux for FastTransport.
 *
 **********************************************************************/

#include "lib/shared_udp_port.h"

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"

#include <event2/event.h>
#include <event2/thread.h>

#include <arpa/inet.h>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

constexpr size_t kMaxUdpMessageSize = 9000;
constexpr uint32_t kNonfragMagic = 0x20050318;
constexpr uint32_t kFragMagic = 0x20101010;
constexpr int kSocketBufSize = 10485760;

static void
BindToPort(int fd, const std::string &host, const std::string &port)
{
    struct sockaddr_in sin{};

    struct addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = 0;
    hints.ai_flags = AI_PASSIVE;
    struct addrinfo *ai = nullptr;
    const int res = getaddrinfo(host.c_str(), port.c_str(), &hints, &ai);
    if (res != 0) {
        Panic("SharedUDPPort: failed to resolve %s:%s: %s",
              host.c_str(), port.c_str(), gai_strerror(res));
    }
    ASSERT(ai->ai_family == AF_INET);
    ASSERT(ai->ai_socktype == SOCK_DGRAM);
    sin = *reinterpret_cast<sockaddr_in *>(ai->ai_addr);
    freeaddrinfo(ai);

    Notice("SharedUDPPort binding to %s:%s", host.c_str(), port.c_str());
    if (bind(fd, reinterpret_cast<sockaddr *>(&sin), sizeof(sin)) < 0) {
        PPanic("SharedUDPPort: failed to bind socket");
    }
}

static size_t
DecodePacket(const char *buf, size_t sz, std::string &type, std::string &msg, void **meta_data)
{
    const char *ptr = buf;
    const size_t meta_len = *reinterpret_cast<const uint32_t *>(ptr);
    ptr += sizeof(uint32_t);

    if (meta_len > 0) {
        *meta_data = const_cast<char *>(ptr);
    } else {
        *meta_data = nullptr;
    }
    ptr += meta_len;

    const size_t typeLen = *reinterpret_cast<const size_t *>(ptr);
    ptr += sizeof(size_t);
    ASSERT(ptr - buf < static_cast<ssize_t>(sz));
    ASSERT(ptr + typeLen - buf < static_cast<ssize_t>(sz));
    type.assign(ptr, typeLen);
    ptr += typeLen;

    const size_t msgLen = *reinterpret_cast<const size_t *>(ptr);
    ptr += sizeof(size_t);
    ASSERT(ptr - buf < static_cast<ssize_t>(sz));
    ASSERT(ptr + msgLen - buf <= static_cast<ssize_t>(sz));
    msg.assign(ptr, msgLen);
    return meta_len;
}

} // namespace

std::mutex SharedUDPPort::registry_mutex_;
std::map<SharedUDPPort::PortKey, SharedUDPPort *> SharedUDPPort::registry_;

bool
SharedUDPPort::SharedPortsEnabled()
{
    const char *env = std::getenv("IONIA_SHARED_UDP_PORT");
    if (env == nullptr || env[0] == '\0') {
        return true;
    }
    return std::strcmp(env, "0") != 0;
}

SharedUDPPort::PortKey
SharedUDPPort::MakeKey(const std::string &host, const std::string &port)
{
    return PortKey{host, static_cast<uint16_t>(std::stoi(port))};
}

SharedUDPPort::SharedUDPPort(const std::string &host,
                             const std::string &port,
                             int dscp)
    : host_(host),
      port_str_(port),
      dscp_(dscp),
      fd_(-1),
      evbase_(nullptr),
      read_event_(nullptr),
      refcnt_(0)
{
}

SharedUDPPort::~SharedUDPPort()
{
    Stop();
    if (read_event_ != nullptr) {
        event_del(read_event_);
        event_free(read_event_);
        read_event_ = nullptr;
    }
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
    if (evbase_ != nullptr) {
        event_base_free(evbase_);
        evbase_ = nullptr;
    }
}

void
SharedUDPPort::EnsureBound()
{
    if (fd_ >= 0) {
        return;
    }

    if ((fd_ = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        PPanic("SharedUDPPort: failed to create socket");
    }
    if (fcntl(fd_, F_SETFL, O_NONBLOCK, 1)) {
        PWarning("SharedUDPPort: failed to set O_NONBLOCK");
    }

    int n = 1;
    if (setsockopt(fd_, SOL_SOCKET, SO_BROADCAST, &n, sizeof(n)) < 0) {
        PWarning("SharedUDPPort: failed to set SO_BROADCAST");
    }
    if (dscp_ != 0) {
        n = dscp_ << 2;
        if (setsockopt(fd_, IPPROTO_IP, IP_TOS, &n, sizeof(n)) < 0) {
            PWarning("SharedUDPPort: failed to set DSCP");
        }
    }
    n = kSocketBufSize;
    if (setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &n, sizeof(n)) < 0) {
        PWarning("SharedUDPPort: failed to set SO_RCVBUF");
    }
    if (setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &n, sizeof(n)) < 0) {
        PWarning("SharedUDPPort: failed to set SO_SNDBUF");
    }

    BindToPort(fd_, host_, port_str_);

    if (evbase_ == nullptr) {
        evthread_use_pthreads();
        evbase_ = event_base_new();
        evthread_make_base_notifiable(evbase_);
    }

    read_event_ = event_new(evbase_, fd_, EV_READ | EV_PERSIST, SocketCallback, this);
    event_add(read_event_, nullptr);
}

SharedUDPPort *
SharedUDPPort::Acquire(const std::string &host, const std::string &port, int dscp)
{
    const PortKey key = MakeKey(host, port);
    std::lock_guard<std::mutex> lock(registry_mutex_);
    auto it = registry_.find(key);
    if (it == registry_.end()) {
        auto *created = new SharedUDPPort(host, port, dscp);
        created->EnsureBound();
        created->refcnt_ = 1;
        registry_.emplace(key, created);
        return created;
    }
    it->second->refcnt_++;
    return it->second;
}

void
SharedUDPPort::Release()
{
    std::lock_guard<std::mutex> lock(registry_mutex_);
    const PortKey key = MakeKey(host_, port_str_);
    auto it = registry_.find(key);
    if (it == registry_.end()) {
        return;
    }
    it->second->refcnt_--;
    if (it->second->refcnt_ <= 0) {
        delete it->second;
        registry_.erase(it);
    }
}

void
SharedUDPPort::RegisterEndpoint(uint8_t endpoint_id, TransportReceiver *receiver)
{
    std::lock_guard<std::mutex> lock(mutex_);
    EnsureBound();
    auto &entry = endpoints_[endpoint_id];
    if (entry.receiver != nullptr && entry.receiver != receiver) {
        Panic("SharedUDPPort: endpoint %u already registered", endpoint_id);
    }
    entry.receiver = receiver;
    entry.registrations++;
}

void
SharedUDPPort::UnregisterEndpoint(uint8_t endpoint_id, TransportReceiver *receiver)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = endpoints_.find(endpoint_id);
    if (it == endpoints_.end()) {
        return;
    }
    if (it->second.receiver != receiver) {
        return;
    }
    it->second.registrations--;
    if (it->second.registrations <= 0) {
        endpoints_.erase(it);
    }
}

void
SharedUDPPort::RunOnce()
{
    event_base *base = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        base = evbase_;
    }
    if (base == nullptr) {
        return;
    }
    event_base_loop(base, EVLOOP_ONCE | EVLOOP_NONBLOCK);
}

void
SharedUDPPort::Stop()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (evbase_ != nullptr) {
        event_base_loopbreak(evbase_);
    }
}

void
SharedUDPPort::SocketCallback(evutil_socket_t fd, short what, void *arg)
{
    auto *port = static_cast<SharedUDPPort *>(arg);
    if (what & EV_READ) {
        port->OnReadable();
    }
}

void
SharedUDPPort::OnReadable()
{
    constexpr int kBufSize = 65536;
    char buf[kBufSize];
    sockaddr_in sender{};
    socklen_t sender_size = sizeof(sender);

    while (true) {
        const ssize_t sz = recvfrom(fd_, buf, kBufSize, 0,
                                    reinterpret_cast<sockaddr *>(&sender),
                                    &sender_size);
        if (sz < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            PWarning("SharedUDPPort: recvfrom failed");
            break;
        }
        ProcessPacket(sender, buf, sz);
    }
}

void
SharedUDPPort::DeliverMessage(uint8_t endpoint_id,
                              const UDPTransportAddress &sender,
                              const std::string &type,
                              const std::string &msg,
                              void *meta_data)
{
    TransportReceiver *receiver = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = endpoints_.find(endpoint_id);
        if (it != endpoints_.end()) {
            receiver = it->second.receiver;
        }
        if (receiver == nullptr) {
            auto fallback = endpoints_.find(0);
            if (fallback != endpoints_.end()) {
                receiver = fallback->second.receiver;
            }
        }
    }
    if (receiver == nullptr) {
        Warning("SharedUDPPort: no receiver for endpoint %u", endpoint_id);
        return;
    }
    receiver->ReceiveMessage(sender, type, msg, meta_data);
}

void
SharedUDPPort::ProcessPacket(sockaddr_in sender, char *buf, ssize_t sz)
{
    UDPTransportAddress sender_addr(sender);
    std::string msg_type;
    std::string msg;
    void *meta_data = nullptr;
    size_t meta_len = 0;

    ASSERT(sz >= static_cast<ssize_t>(sizeof(uint32_t)));
    const uint32_t magic = *reinterpret_cast<uint32_t *>(buf);

    if (magic == kNonfragMagic) {
        meta_len = DecodePacket(buf + sizeof(uint32_t),
                                static_cast<size_t>(sz - sizeof(uint32_t)),
                                msg_type, msg, &meta_data);
    } else if (magic == kFragMagic) {
        const char *ptr = buf;
        ptr += sizeof(uint32_t);
        const uint64_t msg_id = *reinterpret_cast<const uint64_t *>(ptr);
        ptr += sizeof(uint64_t);
        const size_t frag_start = *reinterpret_cast<const size_t *>(ptr);
        ptr += sizeof(size_t);
        const size_t msg_len = *reinterpret_cast<const size_t *>(ptr);
        ptr += sizeof(size_t);
        const size_t frag_len = static_cast<size_t>(buf + sz - ptr);

        FragInfo &info = frag_info_[sender_addr];
        if (info.msgId == 0) {
            info.msgId = msg_id;
            info.data.clear();
            info.data.resize(msg_len);
        } else if (info.msgId != msg_id) {
            info.msgId = msg_id;
            info.data.assign(msg_len, '\0');
        }
        if (frag_start + frag_len <= info.data.size()) {
            memcpy(&info.data[frag_start], ptr, frag_len);
        }
        if (info.data.size() == msg_len) {
            meta_len = DecodePacket(info.data.data(), info.data.size(),
                                    msg_type, msg, &meta_data);
            info.msgId = 0;
            info.data.clear();
        } else {
            return;
        }
    } else {
        Warning("SharedUDPPort: received packet with bad magic number");
        return;
    }

    uint8_t endpoint_id = 0;
    void *deliver_meta = meta_data;
    if (FastTransportEndpointMetaValid(meta_data, meta_len)) {
        endpoint_id = FastTransportEndpointFromMeta(meta_data, meta_len);
        deliver_meta = nullptr;
    } else if (meta_len > 0) {
        char *ptr = static_cast<char *>(meta_data);
        uint16_t udp_src = *reinterpret_cast<uint16_t *>(ptr);
        if (udp_src != 0) {
            sender.sin_port = udp_src;
            sender_addr = UDPTransportAddress(sender);
        }
        ptr += sizeof(uint16_t);

        multistamp_t stamp;
        stamp.sessnum = *reinterpret_cast<sessnum_t *>(ptr);
        ptr += sizeof(sessnum_t);
        const size_t ngroups = *reinterpret_cast<uint32_t *>(ptr);
        ptr += sizeof(uint32_t);
        for (unsigned int i = 0; i < ngroups; i++) {
            const shardnum_t group_idx = *reinterpret_cast<shardnum_t *>(ptr);
            ptr += sizeof(shardnum_t);
            const msgnum_t seqnum = *reinterpret_cast<msgnum_t *>(ptr);
            ptr += sizeof(msgnum_t);
            stamp.seqnums.insert(std::make_pair(group_idx, seqnum));
        }
        DeliverMessage(endpoint_id, sender_addr, msg_type, msg, &stamp);
        return;
    }

    DeliverMessage(endpoint_id, sender_addr, msg_type, msg, deliver_meta);
}

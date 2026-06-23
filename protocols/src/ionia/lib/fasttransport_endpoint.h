// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * fasttransport_endpoint.h:
 *   Endpoint routing metadata for multi-threaded FastTransport on one UDP port.
 *
 **********************************************************************/

#ifndef _LIB_FASTTRANSPORT_ENDPOINT_H_
#define _LIB_FASTTRANSPORT_ENDPOINT_H_

#include <cstddef>
#include <cstdint>

struct FastTransportEndpointMeta {
    static constexpr uint8_t kVersion = 1;
    static constexpr size_t kSize = 4;

    uint8_t version;
    uint8_t src_endpoint;
    uint8_t dst_endpoint;
    uint8_t reserved;
};

inline bool
FastTransportEndpointMetaValid(const void *meta, size_t meta_len)
{
    if (meta == nullptr || meta_len != FastTransportEndpointMeta::kSize) {
        return false;
    }
    const auto *m = static_cast<const FastTransportEndpointMeta *>(meta);
    return m->version == FastTransportEndpointMeta::kVersion;
}

inline FastTransportEndpointMeta
MakeFastTransportEndpointMeta(uint8_t src_endpoint, uint8_t dst_endpoint)
{
    return FastTransportEndpointMeta{
        FastTransportEndpointMeta::kVersion,
        src_endpoint,
        dst_endpoint,
        0,
    };
}

inline uint8_t
FastTransportEndpointFromMeta(const void *meta, size_t meta_len)
{
    if (!FastTransportEndpointMetaValid(meta, meta_len)) {
        return 0;
    }
    return static_cast<const FastTransportEndpointMeta *>(meta)->dst_endpoint;
}

#endif  // _LIB_FASTTRANSPORT_ENDPOINT_H_

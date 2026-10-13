// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/value_codec.h
//   On-store encoding of a versioned value: [t : 8 bytes little-endian][value bytes].
//   Keeps each key's commit timestamp next to its value, so recovery can select
//   key-values by timestamp without a separate index.

#ifndef _DYNA_VALUE_CODEC_H_
#define _DYNA_VALUE_CODEC_H_

#include "dyna/storage_backend.h"

#include <cstdint>
#include <string>
#include <string_view>

namespace specpaxos {
namespace dyna {

constexpr size_t kValueHeaderBytes = 8;

inline std::string EncodeVersioned(std::string_view value, uint64_t t)
{
    std::string out;
    out.resize(kValueHeaderBytes + value.size());
    for (size_t i = 0; i < kValueHeaderBytes; ++i) {
        out[i] = static_cast<char>((t >> (8 * i)) & 0xff);
    }
    out.replace(kValueHeaderBytes, value.size(), value.data(), value.size());
    return out;
}

// A stored blob shorter than the header was not written by DYNA (e.g. a preloaded
// value): returned as-is with t = 0.
inline VersionedValue DecodeVersioned(std::string_view stored)
{
    VersionedValue v;
    if (stored.size() < kValueHeaderBytes) {
        v.value = std::string(stored);
        return v;
    }
    for (size_t i = 0; i < kValueHeaderBytes; ++i) {
        v.t |= static_cast<uint64_t>(static_cast<unsigned char>(stored[i])) << (8 * i);
    }
    v.value = std::string(stored.substr(kValueHeaderBytes));
    return v;
}

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_VALUE_CODEC_H_

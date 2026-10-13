// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#include "dyna/in_memory_backend.h"

namespace specpaxos {
namespace dyna {

void
InMemoryBackend::apply(std::string_view key, std::string_view value, uint64_t t)
{
    std::lock_guard<std::mutex> lk(mu_);
    kv_[std::string(key)] = VersionedValue{std::string(value), t};
    ++apply_count_;
}

std::optional<VersionedValue>
InMemoryBackend::read(std::string_view key)
{
    std::lock_guard<std::mutex> lk(mu_);
    auto it = kv_.find(std::string(key));
    if (it == kv_.end()) return std::nullopt;
    return it->second;
}

std::string
InMemoryBackend::value_or_empty(std::string_view key)
{
    auto v = read(key);
    return v ? v->value : std::string();
}

uint64_t
InMemoryBackend::apply_count() const
{
    std::lock_guard<std::mutex> lk(mu_);
    return apply_count_;
}

} // namespace dyna
} // namespace specpaxos

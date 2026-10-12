// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/commit_quorum.h
//   Client-side commit rule (DECISION A, spec §1.6), generalized to MULTIPLE
//   involved partition leaders. A write commits on the FAST path when:
//     (1) every involved partition leader has replied,
//     (2) all involved leaders carry the SAME (viewId, t), and
//     (3) ≥ f+1 replies total (leaders + followers) carry that same (viewId, t).
//   A single-partition txn is just the m = 1 case.
//
//   If all leaders replied but their timestamps differ → SLOW path: the client sends
//   SlowCommit(agreedT = max over the leaders' t). Servers adopt agreedT and reply
//   CommitReply(agreedT); those flow into the SAME TxnQuorum (latest reply per server
//   wins), so once every leader shows agreedT and ≥ f+1 agree, fast_ready() fires —
//   unifying the fast-path and slow-path quorum checks.

#ifndef _DYNA_COMMIT_QUORUM_H_
#define _DYNA_COMMIT_QUORUM_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <set>

namespace specpaxos {
namespace dyna {

class TxnQuorum {
public:
    TxnQuorum() = default;
    TxnQuorum(std::set<uint32_t> involved_leaders, size_t quorum)
        : involved_leaders_(std::move(involved_leaders)), quorum_(quorum) {}

    // Record (the latest) reply from a server. SpecReply and CommitReply both feed
    // here; the latest per server overwrites the earlier (so an adopted agreedT
    // supersedes the original speculative t).
    void AddReply(uint32_t server_id, uint64_t view_id, uint64_t t, bool is_leader) {
        replies_[server_id] = Entry{view_id, t};
        if (is_leader) involved_leaders_.insert(server_id);  // defensive
    }

    // Every involved leader has replied.
    bool all_leaders_present() const {
        if (involved_leaders_.empty()) return false;
        for (uint32_t l : involved_leaders_) {
            if (replies_.find(l) == replies_.end()) return false;
        }
        return true;
    }

    // All involved leaders present AND agreeing on one (view, t); outputs it.
    bool leaders_agree(uint64_t *view, uint64_t *t) const {
        if (!all_leaders_present()) return false;
        bool first = true;
        uint64_t v = 0, tt = 0;
        for (uint32_t l : involved_leaders_) {
            const Entry &e = replies_.at(l);
            if (first) { v = e.view; tt = e.t; first = false; }
            else if (e.view != v || e.t != tt) return false;
        }
        if (view) *view = v;
        if (t) *t = tt;
        return true;
    }

    // DECISION A: all leaders agree on (V,T) and ≥ quorum replies carry (V,T).
    bool fast_ready() const {
        uint64_t v = 0, t = 0;
        if (!leaders_agree(&v, &t)) return false;
        return matching(v, t) >= quorum_;
    }

    // All leaders replied but their timestamps disagree → slow path required.
    bool leaders_disagree() const {
        return all_leaders_present() && !leaders_agree(nullptr, nullptr);
    }

    // The common leader timestamp (fast path); 0 if leaders don't agree.
    uint64_t agreed_t() const {
        uint64_t v = 0, t = 0;
        return leaders_agree(&v, &t) ? t : 0;
    }

    // max over the involved leaders' proposed t (the SlowCommit agreedT, spec §2).
    uint64_t slow_agreed_t() const {
        uint64_t m = 0;
        for (uint32_t l : involved_leaders_) {
            auto it = replies_.find(l);
            if (it != replies_.end()) m = std::max(m, it->second.t);
        }
        return m;
    }

    size_t reply_count() const { return replies_.size(); }
    const std::set<uint32_t> &involved_leaders() const { return involved_leaders_; }

private:
    struct Entry { uint64_t view; uint64_t t; };

    size_t matching(uint64_t view, uint64_t t) const {
        size_t n = 0;
        for (const auto &kv : replies_) {
            if (kv.second.view == view && kv.second.t == t) ++n;
        }
        return n;
    }

    std::set<uint32_t>          involved_leaders_;
    size_t                      quorum_ = 1;
    std::map<uint32_t, Entry>   replies_;
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_COMMIT_QUORUM_H_

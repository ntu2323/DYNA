// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/bg_outbox.h
//   Per-destination outbox for BACKGROUND server-to-server traffic (replication
//   prepare / ack / commit, ts-agreement): instead of one datagram per txn per message,
//   entries accumulate per destination replica and leave as one proto::BgBatch on a
//   timer flush (DYNA_REPL_FLUSH_US) or when the batch is full (DYNA_REPL_BATCH entries
//   or ~max_bytes serialized). Each entry is the unchanged per-txn message, so receivers
//   run the same per-txn handlers: the protocol is unchanged, only the framing is.
//   The client-facing commit path (requests, SpecReply, Fast/SlowCommit, CommitReply,
//   Fetch) never goes through here.
//
//   Thread-safe: receive threads, the timer thread and the flush all touch it.

#ifndef _DYNA_BG_OUTBOX_H_
#define _DYNA_BG_OUTBOX_H_

#include "dyna/dyna-proto.pb.h"

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <utility>
#include <vector>

namespace specpaxos {
namespace dyna {

class BgOutbox {
public:
    BgOutbox(uint32_t n_replicas, size_t max_entries, size_t max_bytes)
        : max_entries_(max_entries == 0 ? 1 : max_entries), max_bytes_(max_bytes),
          slots_(n_replicas) {}

    // Each Add* appends one entry for replica `to` and returns true when that
    // destination's batch is full: the caller should then Take(to) and send it now.
    bool AddPrepare(uint32_t to, const proto::ReplicatePrepare &m) {
        return Add(to, m.ByteSizeLong(), [&](proto::BgBatch &b) { *b.add_prepares() = m; });
    }
    bool AddPrepareReply(uint32_t to, const proto::ReplicatePrepareReply &m) {
        return Add(to, m.ByteSizeLong(),
                   [&](proto::BgBatch &b) { *b.add_prepare_replies() = m; });
    }
    bool AddCommit(uint32_t to, const proto::ReplicateCommit &m) {
        return Add(to, m.ByteSizeLong(), [&](proto::BgBatch &b) { *b.add_commits() = m; });
    }
    bool AddTsAgree(uint32_t to, const proto::TsAgree &m) {
        return Add(to, m.ByteSizeLong(), [&](proto::BgBatch &b) { *b.add_ts_agrees() = m; });
    }

    // Move out `to`'s pending batch; false if it was empty.
    bool Take(uint32_t to, proto::BgBatch *out, size_t *entries = nullptr) {
        std::lock_guard<std::mutex> lk(mu_);
        if (to >= slots_.size() || slots_[to].entries == 0) return false;
        Slot &s = slots_[to];
        out->Swap(&s.batch);
        s.batch.Clear();
        if (entries) *entries = s.entries;
        s.entries = 0;
        s.bytes = 0;
        return true;
    }

    // Move out every non-empty batch (timer flush).
    std::vector<std::pair<uint32_t, proto::BgBatch>> TakeAll() {
        std::vector<std::pair<uint32_t, proto::BgBatch>> out;
        std::lock_guard<std::mutex> lk(mu_);
        for (uint32_t i = 0; i < slots_.size(); ++i) {
            Slot &s = slots_[i];
            if (s.entries == 0) continue;
            out.emplace_back(i, proto::BgBatch());
            out.back().second.Swap(&s.batch);
            s.batch.Clear();
            s.entries = 0;
            s.bytes = 0;
        }
        return out;
    }

    void Clear() {
        std::lock_guard<std::mutex> lk(mu_);
        for (Slot &s : slots_) { s.batch.Clear(); s.entries = 0; s.bytes = 0; }
    }

    size_t pending(uint32_t to) {
        std::lock_guard<std::mutex> lk(mu_);
        return to < slots_.size() ? slots_[to].entries : 0;
    }

private:
    struct Slot {
        proto::BgBatch batch;
        size_t entries = 0;
        size_t bytes = 0;
    };

    template <class Fill>
    bool Add(uint32_t to, size_t bytes, Fill fill) {
        std::lock_guard<std::mutex> lk(mu_);
        if (to >= slots_.size()) return false;
        Slot &s = slots_[to];
        fill(s.batch);
        s.entries += 1;
        s.bytes += bytes + 4;   // + a little framing per repeated entry
        return s.entries >= max_entries_ || (max_bytes_ > 0 && s.bytes >= max_bytes_);
    }

    const size_t max_entries_;
    const size_t max_bytes_;
    std::mutex mu_;
    std::vector<Slot> slots_;
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_BG_OUTBOX_H_

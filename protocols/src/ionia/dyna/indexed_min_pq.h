// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/indexed_min_pq.h
//   A binary min-heap keyed by an Id, with O(1) `contains`/`find` and O(log n)
//   `update_key` (re-heapify after the caller mutates an entry's order) and
//   `erase(id)`. The codebase's only priority queue is std::priority_queue, which
//   supports none of update-key / erase-by-id — but DYNA's specLog "MUST support
//   update-key" because re-timestamp (late-leader bump, SlowCommit adopt-max,
//   TsAgree max) changes an entry's live `t` in place and must re-order it
//   (spec §1.4). earlyBuffer and specLog are both IndexedMinPQ<TxnId, TxnEntry>.
//
//   Invariant: `pos_[id]` is the current heap index of `id`, kept in sync on every
//   swap. This is the only correctness-critical property of the structure; it is
//   validated by CheckInvariants() (used in tests).
//
//   Values live out of line (the heap holds unique_ptrs): a sift swaps two pointers
//   instead of two whole values. TxnEntry carries a protobuf, sets and hash maps, and
//   moving those on every sift was the executor's main cost under the ordering lock
//   (CloudLab profile). It also makes value addresses stable until erase / pop.

#ifndef _DYNA_INDEXED_MIN_PQ_H_
#define _DYNA_INDEXED_MIN_PQ_H_

#include "lib/data_structure.h"   // HashMap (ankerl::unordered_dense::map)

#include <cstddef>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

namespace specpaxos {
namespace dyna {

// Compare is a strict-weak "less" on Value: comp(a, b) == true means a orders
// before b (a is nearer the top / min). Default std::less<Value>.
template <class Id, class Value, class Compare = std::less<Value>,
          class Hash = ankerl::unordered_dense::hash<Id>>
class IndexedMinPQ {
public:
    IndexedMinPQ() = default;
    explicit IndexedMinPQ(Compare comp) : comp_(std::move(comp)) {}

    bool   empty() const { return heap_.empty(); }
    size_t size()  const { return heap_.size(); }
    bool   contains(const Id &id) const { return pos_.find(id) != pos_.end(); }

    // Insert. Returns false (no-op) if `id` is already present.
    bool push(const Id &id, Value v) {
        if (pos_.find(id) != pos_.end()) return false;
        heap_.emplace_back(id, std::make_unique<Value>(std::move(v)));
        size_t i = heap_.size() - 1;
        pos_[id] = i;
        sift_up(i);
        return true;
    }

    const Id    &peek_min_id() const { return heap_.front().first; }
    const Value &peek_min()    const { return *heap_.front().second; }

    // Remove and return the minimum. Precondition: !empty().
    Value pop_min() {
        Id id = heap_.front().first;
        Value out = std::move(*heap_.front().second);
        remove_at(0);
        pos_.erase(id);
        return out;
    }

    // Mutable access WITHOUT re-heapify. After changing anything that affects the
    // order, the caller must call update_key(id). Returns nullptr if absent.
    Value *find(const Id &id) {
        auto it = pos_.find(id);
        return it == pos_.end() ? nullptr : heap_[it->second].second.get();
    }
    const Value *find(const Id &id) const {
        auto it = pos_.find(id);
        return it == pos_.end() ? nullptr : heap_[it->second].second.get();
    }

    // Re-establish heap order for `id` after the caller mutated its value. O(log n).
    bool update_key(const Id &id) {
        auto it = pos_.find(id);
        if (it == pos_.end()) return false;
        restore(it->second);
        return true;
    }

    bool erase(const Id &id) {
        auto it = pos_.find(id);
        if (it == pos_.end()) return false;
        size_t i = it->second;
        remove_at(i);
        pos_.erase(id);
        return true;
    }

    // Read-only iteration over all (id, value) pairs, in heap (not sorted) order.
    // The callback must NOT mutate the heap; collect ids first, then mutate.
    template <class F>
    void for_each(F &&f) const {
        for (const auto &kv : heap_) f(kv.first, *kv.second);
    }

    // Test/debug: verify heap property and that pos_ mirrors heap_ exactly.
    bool CheckInvariants() const {
        if (pos_.size() != heap_.size()) return false;
        for (size_t i = 0; i < heap_.size(); ++i) {
            auto it = pos_.find(heap_[i].first);
            if (it == pos_.end() || it->second != i) return false;
        }
        for (size_t i = 1; i < heap_.size(); ++i) {
            size_t parent = (i - 1) / 2;
            if (comp_(*heap_[i].second, *heap_[parent].second)) return false; // child < parent
        }
        return true;
    }

private:
    bool less(size_t a, size_t b) const {
        return comp_(*heap_[a].second, *heap_[b].second);
    }

    void swap_nodes(size_t a, size_t b) {
        std::swap(heap_[a], heap_[b]);
        pos_[heap_[a].first] = a;
        pos_[heap_[b].first] = b;
    }

    void sift_up(size_t i) {
        while (i > 0) {
            size_t parent = (i - 1) / 2;
            if (less(i, parent)) { swap_nodes(i, parent); i = parent; }
            else break;
        }
    }

    void sift_down(size_t i) {
        size_t n = heap_.size();
        while (true) {
            size_t l = 2 * i + 1, r = 2 * i + 2, m = i;
            if (l < n && less(l, m)) m = l;
            if (r < n && less(r, m)) m = r;
            if (m == i) break;
            swap_nodes(i, m);
            i = m;
        }
    }

    // Restore heap order for the element at index i, which may need to move up or
    // down after a mutation/removal. If it is smaller than its parent it can only
    // bubble up; otherwise it can only bubble down.
    void restore(size_t i) {
        if (i > 0 && less(i, (i - 1) / 2)) sift_up(i);
        else                               sift_down(i);
    }

    // Remove the element at index i (does NOT touch pos_ for that id; callers erase
    // the pos_ entry themselves, since they hold the id).
    void remove_at(size_t i) {
        size_t last = heap_.size() - 1;
        if (i == last) { heap_.pop_back(); return; }
        swap_nodes(i, last);   // moves target to the tail, tail-element to i
        heap_.pop_back();      // drop the target
        if (i < heap_.size()) restore(i);
    }

    std::vector<std::pair<Id, std::unique_ptr<Value>>> heap_;   // OrderKey-ordered min-heap
    ankerl::unordered_dense::map<Id, size_t, Hash> pos_;   // id -> heap index (kept in sync)
    Compare                           comp_{};
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_INDEXED_MIN_PQ_H_

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/write_quorum.h
//   Stage 4: the client-side write commit rule (spec §5), factored out as a pure
//   accumulator so it is unit-testable in isolation.
//
//   A write commits when the client holds `write_quorum` (default f+1) replies
//   that INCLUDE the partition leader's and AGREE on the leader's deadline and
//   view. The leader is authoritative: its reply defines the "epoch"
//   (view, deadline). If the leader reports a different deadline than expected
//   (it took the slow path, §6.2), we switch epochs — discard replies tallied
//   under the old deadline and re-count against the leader's. A follower's stale
//   first reply can therefore never by itself commit; only replies matching the
//   leader's deadline count (the corrected-reply supersession of §6.4).

#ifndef _DYNA_NEW_WRITE_QUORUM_H_
#define _DYNA_NEW_WRITE_QUORUM_H_

#include <cstddef>
#include <cstdint>
#include <set>

namespace specpaxos {
namespace dyna_new {

class WriteQuorum {
public:
    // initial_deadline: the client's stamped deadline, used as the provisional
    // epoch until the leader confirms or corrects it.
    WriteQuorum(uint32_t leader_id, size_t write_quorum, uint64_t initial_deadline)
        : leader_id_(leader_id), write_quorum_(write_quorum),
          active_deadline_(initial_deadline) {}

    // Record a reply; returns true once the commit condition holds.
    bool AddReply(uint32_t replica_id, uint64_t view_id, uint64_t deadline) {
        if (committed_) return true;

        const bool is_leader = (replica_id == leader_id_);
        if (is_leader) {
            // Switch epochs only if the leader's authoritative (deadline, view)
            // differs from what we have been tallying — otherwise a confirming
            // leader reply would wrongly discard already-matched follower replies.
            if (deadline != active_deadline_ || view_id != leader_view_) {
                active_deadline_ = deadline;
                leader_view_ = view_id;
                responders_.clear();
            }
            leader_acked_ = true;
        }

        // Only replies matching the leader-authoritative epoch count. Before the
        // leader is heard, leader_view_ is the default (0) and active_deadline_
        // is the client's stamp, so on-time follower replies pre-tally and are
        // either confirmed or cleared when the leader's reply lands.
        if (deadline != active_deadline_) return false;
        if (view_id != leader_view_) return false;

        responders_.insert(replica_id);
        if (leader_acked_ && responders_.size() >= write_quorum_) {
            committed_ = true;
        }
        return committed_;
    }

    bool committed() const { return committed_; }
    bool leader_acked() const { return leader_acked_; }
    size_t responders() const { return responders_.size(); }
    uint64_t active_deadline() const { return active_deadline_; }

private:
    const uint32_t leader_id_;
    const size_t write_quorum_;
    uint64_t active_deadline_;
    uint64_t leader_view_ = 0;
    bool leader_acked_ = false;
    bool committed_ = false;
    std::set<uint32_t> responders_;
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_WRITE_QUORUM_H_

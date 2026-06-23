// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna_new/commit_quorum.h
//   Stage 4/6: the client-side commit rule (spec §5), unified for writes and
//   reads. A request commits when the client holds `quorum` (default f+1) replies
//   that INCLUDE the partition leader's and AGREE on the leader's view and a
//   per-type match value:
//     * write -> match value is the leader's (possibly reassigned) deadline.
//     * read  -> match value is the leader's broadcast log position (§6.3).
//
//   The leader is authoritative: it defines the epoch (view, match). We keep the
//   latest reply per replica and re-evaluate against the leader's epoch on every
//   update, so this naturally handles any arrival order, a leader that reassigns
//   (slow path, §6.2), and corrected/superseding follower replies (§6.4) — a
//   follower's stale reply simply stops matching once the leader's epoch is known.

#ifndef _DYNA_NEW_COMMIT_QUORUM_H_
#define _DYNA_NEW_COMMIT_QUORUM_H_

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>

namespace specpaxos {
namespace dyna_new {

class CommitQuorum {
public:
    CommitQuorum(uint32_t leader_id, size_t quorum)
        : leader_id_(leader_id), quorum_(quorum) {}

    // Record (the latest) reply from a replica; returns true once committed.
    // has_result/result carry the leader's data for reads.
    bool AddReply(uint32_t replica_id, uint64_t view_id, uint64_t match_value,
                  bool has_result = false, const std::string &result = "") {
        if (committed_) return true;

        replies_[replica_id] = Entry{view_id, match_value};
        if (replica_id == leader_id_) {
            have_leader_ = true;
            leader_view_ = view_id;
            leader_match_ = match_value;
            if (has_result) result_ = result;
        }

        if (have_leader_ && Matching() >= quorum_) {
            committed_ = true;
        }
        return committed_;
    }

    bool committed() const { return committed_; }
    bool leader_acked() const { return have_leader_; }
    // Replies agreeing with the leader's epoch (0 until the leader is heard).
    size_t matching() const { return Matching(); }
    uint64_t active_match() const { return have_leader_ ? leader_match_ : 0; }
    const std::string &result() const { return result_; }

private:
    struct Entry {
        uint64_t view;
        uint64_t match;
    };

    size_t Matching() const {
        if (!have_leader_) return 0;
        size_t n = 0;
        for (const auto &kv : replies_) {
            if (kv.second.view == leader_view_ && kv.second.match == leader_match_) {
                ++n;
            }
        }
        return n;
    }

    const uint32_t leader_id_;
    const size_t quorum_;
    bool have_leader_ = false;
    uint64_t leader_view_ = 0;
    uint64_t leader_match_ = 0;
    std::string result_;
    bool committed_ = false;
    std::map<uint32_t, Entry> replies_;
};

} // namespace dyna_new
} // namespace specpaxos

#endif // _DYNA_NEW_COMMIT_QUORUM_H_

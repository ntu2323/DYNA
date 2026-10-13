// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/view_coordinator.h
//   The silent VR layer + recovery coordination (design doc § Recovery), as pure logic:
//   inputs are messages and the current time, outputs are messages to send (an outbox)
//   and two events for the replica (begin recovery / install). No transport, no orderer,
//   so it is unit-testable with a fake network.
//
//   A view is (view_id, alive set); the VR leader of a view is its min-id alive server.
//
//   NORMAL ──(peer silent > fail timeout, or a higher StartViewChange)──► VIEW_CHANGE
//     Broadcast StartViewChange(v', alive'); after f matching ones from others send
//     DoViewChange to the VR leader of v'. The VR leader, with f+1 DoViewChanges
//     (itself included), broadcasts StartRecovery.
//   VIEW_CHANGE ──(StartRecovery / RecoveryRequest / RecoveryPrepare for v')──► RECOVERING
//     Every survivor is PL for its delta-p: it collects f+1 RecoveryReplies (its own
//     included), merges them into M, sends RecoveryPrepare(M), and after f+1
//     RecoveryPrepareReplies (itself included) sends RecoveryReport to the VR leader.
//     The VR leader, with a report from every alive server, broadcasts StartView.
//   RECOVERING ──(StartView)──► NORMAL in v' (the replica installs every buffered M).
//
//   Rejoin. A restarted server (it lost its in-memory state; the replica tells us via
//   Config::start_joining) and a server left out of a started view (it resets its state)
//   are JOINING: never a voter until re-added.
//     JOINING ──(JoinInfo: current view without us)──► adopt it as a NON-member
//       (adopt_view → the replica catches up from the VR leader) ──(OnCaughtUp)──►
//       JoinReady to the VR leader, which starts a normal view change adding us; consistent
//       hashing then hands our home range back and we are its PL in that recovery.
//     A member that gets a JoinRequest from a server it still counts as alive (restarted
//     faster than failure detection) first changes view without it.
//     If every other server is JOINING too for grace_us (a whole-cluster restart: no
//     member survives to rejoin), start fresh in view 0.

#ifndef _DYNA_VIEW_COORDINATOR_H_
#define _DYNA_VIEW_COORDINATOR_H_

#include "dyna/dyna-proto.pb.h"
#include "dyna/recovery.h"

#include <google/protobuf/message.h>

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <vector>

namespace specpaxos {
namespace dyna {

class ViewCoordinator {
public:
    enum class Phase { NORMAL, VIEW_CHANGE, RECOVERING, JOINING };

    struct Config {
        uint32_t self = 0;
        uint32_t n = 1;
        uint32_t f = 0;
        uint64_t fail_timeout_us = 1000000;   // 0 disables failure detection
        uint64_t grace_us = 10000000;         // allowance for a peer never heard from
        uint64_t resend_us = 50000;           // re-send cadence of pending VR/recovery msgs
        bool     start_joining = false;       // restarted: rejoin instead of view 0
    };

    static constexpr int32_t kAll = -1;
    struct Out {
        int32_t to = kAll;   // replica id, or kAll (every other replica)
        std::shared_ptr<google::protobuf::Message> msg;
    };
    struct Effects {
        std::vector<Out> out;
        bool begin_recovery = false;   // entered RECOVERING: pause, then OnLocalRecoveryData
        bool install = false;          // StartView reached: TakeInstall(), install, resume
        bool excluded = false;         // left out of a started view: reset state, rejoin
        bool adopt_view = false;       // JOINING: adopt (view(), alive()) as a non-member and
                                       // (re)start catch-up from LeaderOf(alive())
        bool fresh_start = false;      // whole-cluster restart: NORMAL in view 0, all alive
    };

    // What to install at StartView.
    struct Install {
        uint64_t view = 0;
        uint64_t alive = 0;
        uint64_t old_alive = 0;
        std::vector<RecoveryData> ms;   // every M buffered for this view (own included)
    };

    ViewCoordinator(const Config &cfg, uint64_t now_us);

    Phase    phase() const { return phase_; }
    uint64_t view() const { return view_; }
    uint64_t alive() const { return alive_; }
    // The view being changed to (valid outside NORMAL), and the one it replaces.
    uint64_t target_view() const { return pv_; }
    uint64_t target_alive() const { return pa_; }
    uint64_t old_alive() const { return old_alive_; }
    bool     has_view() const { return phase_ != Phase::JOINING || have_view_; }
    static uint32_t LeaderOf(uint64_t alive);

    // Any message from replica `from` proves it alive.
    void OnHeard(uint32_t from, uint64_t now_us);
    // Failure detection + re-sends of whatever this phase is waiting on.
    void Tick(uint64_t now_us, Effects *fx);

    void OnStartViewChange(const proto::StartViewChange &m, uint64_t now_us, Effects *fx);
    void OnDoViewChange(const proto::DoViewChange &m, uint64_t now_us, Effects *fx);
    void OnStartRecovery(const proto::StartRecovery &m, uint64_t now_us, Effects *fx);
    // Before answering a RecoveryRequest: join recovery of that view if we haven't yet.
    // True iff we are now RECOVERING in exactly that view (so our reply is well-defined).
    bool AcceptRecoveryView(uint64_t view, uint64_t alive, uint64_t old_alive,
                            uint64_t now_us, Effects *fx);
    // This server's own contribution to its M (the replica collected it after
    // begin_recovery).
    void OnLocalRecoveryData(RecoveryData d, Effects *fx);
    void OnRecoveryReply(const proto::RecoveryReply &m, Effects *fx);
    void OnRecoveryPrepare(const proto::RecoveryPrepare &m, uint64_t now_us, Effects *fx);
    void OnRecoveryPrepareReply(const proto::RecoveryPrepareReply &m, Effects *fx);
    void OnRecoveryReport(const proto::RecoveryReport &m, Effects *fx);
    void OnStartView(const proto::StartView &m, uint64_t now_us, Effects *fx);

    // Rejoin.
    void OnJoinRequest(const proto::JoinRequest &m, uint64_t now_us, Effects *fx);
    void OnJoinInfo(const proto::JoinInfo &m, uint64_t now_us, Effects *fx);
    void OnJoinReady(const proto::JoinReady &m, uint64_t now_us, Effects *fx);
    // JOINING: the replica finished catching up in the adopted view.
    void OnCaughtUp(Effects *fx);

    // After fx->install: hand over what to install and enter NORMAL in the new view.
    Install TakeInstall(uint64_t now_us);

private:
    bool InMask(uint64_t mask, uint32_t s) const { return s < 64 && ((mask >> s) & 1ULL); }
    uint32_t Count(uint64_t mask) const { return static_cast<uint32_t>(__builtin_popcountll(mask)); }
    bool Excluded(uint64_t alive, Effects *fx);
    void BecomeJoining(Effects *fx);
    void AdoptAsNonMember(uint64_t v, uint64_t a, Effects *fx);
    void FreshStart(uint64_t now_us, Effects *fx);
    // The servers this one currently counts as members (alive_ / the target view).
    uint64_t Membership() const { return phase_ == Phase::NORMAL ? alive_ : pa_; }
    void StartViewChange(uint64_t v, uint64_t a, uint64_t now_us, Effects *fx);
    void EnterRecovering(uint64_t v, uint64_t a, uint64_t old_a, uint64_t now_us, Effects *fx);
    void MaybeSendDoViewChange(uint64_t now_us, Effects *fx);
    void MaybeStartRecovery(uint64_t now_us, Effects *fx);
    void MaybeBuildM(Effects *fx);
    void MaybeReport(Effects *fx);
    void MaybeStartView(Effects *fx);
    void ResetRound();

    // Message builders.
    std::shared_ptr<google::protobuf::Message> MakeSvc() const;
    std::shared_ptr<google::protobuf::Message> MakeDvc() const;
    std::shared_ptr<google::protobuf::Message> MakeStartRecovery() const;
    std::shared_ptr<google::protobuf::Message> MakeRecoveryRequest() const;
    std::shared_ptr<google::protobuf::Message> MakeRecoveryPrepare() const;
    std::shared_ptr<google::protobuf::Message> MakeReport() const;
    std::shared_ptr<google::protobuf::Message> MakeStartView() const;
    std::shared_ptr<google::protobuf::Message> MakeJoinRequest() const;
    std::shared_ptr<google::protobuf::Message> MakeJoinReady() const;

    Config cfg_;
    Phase  phase_ = Phase::NORMAL;
    uint64_t view_ = 0;        // current (last NORMAL) view
    uint64_t alive_ = 0;
    uint64_t pv_ = 0;          // target view during VIEW_CHANGE / RECOVERING
    uint64_t pa_ = 0;
    uint64_t old_alive_ = 0;   // alive set of the view being replaced

    std::vector<uint64_t> last_heard_;
    std::vector<bool>     ever_heard_;
    uint64_t start_us_ = 0;
    uint64_t last_resend_us_ = 0;
    uint64_t phase_since_us_ = 0;

    // VIEW_CHANGE round.
    std::set<uint32_t> svc_from_;
    bool               dvc_sent_ = false;
    // VR leader only: sender -> (its last NORMAL view, that view's alive set). Recovery
    // starts from the OLDEST of these (VR: adopt the most-behind participant's state), so
    // delta-p spans every key moved since any participant last installed a view — e.g.
    // when the previous VR leader's StartView reached only some servers.
    std::map<uint32_t, std::pair<uint64_t, uint64_t>> dvc_from_;

    // RECOVERING round.
    std::map<uint32_t, RecoveryData> replies_;   // PL: f+1 contributions (own included)
    bool               m_built_ = false;
    RecoveryData       my_m_;
    std::set<uint32_t> prep_acks_;
    bool               reported_ = false;
    std::set<uint32_t> reports_;    // VR leader only
    std::map<uint32_t, RecoveryData> ms_;        // every PL's M received (own included)
    bool               start_view_sent_ = false;

    // JOINING.
    bool               have_view_ = false;   // adopted the current view as a non-member
    bool               caught_up_ = false;
    std::set<uint32_t> join_from_;           // other joiners heard (whole-cluster restart)
    bool               fresh_started_ = false;   // NORMAL in view 0 via FreshStart
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_VIEW_COORDINATOR_H_

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/view_coordinator.cc

#include "dyna/view_coordinator.h"

#include "lib/message.h"

#include "dyna/topology.h"

#include <algorithm>
#include <cstdint>
#include <utility>

namespace specpaxos {
namespace dyna {

ViewCoordinator::ViewCoordinator(const Config &cfg, uint64_t now_us)
    : cfg_(cfg),
      alive_(Topology::AllAlive(cfg.n)),
      last_heard_(cfg.n, now_us),
      ever_heard_(cfg.n, false),
      start_us_(now_us),
      last_resend_us_(now_us),
      phase_since_us_(now_us)
{
    pv_ = view_;
    pa_ = alive_;
    old_alive_ = alive_;
    if (cfg.start_joining) phase_ = Phase::JOINING;
}

uint32_t
ViewCoordinator::LeaderOf(uint64_t alive)
{
    return alive == 0 ? 0 : static_cast<uint32_t>(__builtin_ctzll(alive));
}

void
ViewCoordinator::OnHeard(uint32_t from, uint64_t now_us)
{
    if (from >= cfg_.n) return;
    last_heard_[from] = now_us;
    ever_heard_[from] = true;
}

bool
ViewCoordinator::Excluded(uint64_t alive, Effects *fx)
{
    if (InMask(alive, cfg_.self)) return false;
    if (phase_ != Phase::JOINING) BecomeJoining(fx);
    return true;
}

void
ViewCoordinator::BecomeJoining(Effects *fx)
{
    // Left out of a started view (falsely suspected, or restarted undetected): our state
    // is no longer a member's; drop it and rejoin like a restarted server.
    phase_ = Phase::JOINING;
    have_view_ = false;
    caught_up_ = false;
    join_from_.clear();
    ResetRound();
    fx->excluded = true;
}

void
ViewCoordinator::AdoptAsNonMember(uint64_t v, uint64_t a, Effects *fx)
{
    view_ = pv_ = v;
    alive_ = pa_ = old_alive_ = a;
    have_view_ = true;
    caught_up_ = false;
    fx->adopt_view = true;
}

void
ViewCoordinator::ResetRound()
{
    svc_from_.clear();
    dvc_sent_ = false;
    dvc_from_.clear();
    replies_.clear();
    m_built_ = false;
    my_m_ = RecoveryData();
    prep_acks_.clear();
    reported_ = false;
    reports_.clear();
    ms_.clear();
    start_view_sent_ = false;
}

// ---------------------------------------------------------------------------------------
// Failure detection + re-sends
// ---------------------------------------------------------------------------------------

void
ViewCoordinator::Tick(uint64_t now_us, Effects *fx)
{
    const uint64_t prev_tick_us = last_tick_us_;
    last_tick_us_ = now_us;
    if (phase_ == Phase::JOINING) {
        if (now_us < last_resend_us_ + cfg_.resend_us) return;
        last_resend_us_ = now_us;
        if (!have_view_) {
            fx->out.push_back(Out{kAll, MakeJoinRequest()});
            // Every other server is JOINING too and nobody answered: a whole-cluster
            // restart (more than f failures). Nothing survives to rejoin; start fresh.
            if (join_from_.size() + 1 >= cfg_.n && now_us >= phase_since_us_ + cfg_.grace_us) {
                FreshStart(now_us, fx);
            }
        } else if (caught_up_) {
            fx->out.push_back(Out{static_cast<int32_t>(LeaderOf(alive_)), MakeJoinReady()});
        }
        return;
    }

    // Failure detection over the servers we currently expect to hear from (the current
    // view when NORMAL, the target view while changing — so a VR leader that dies
    // mid-change is itself replaced).
    if (cfg_.fail_timeout_us > 0) {
        const uint64_t base = (phase_ == Phase::NORMAL) ? alive_ : pa_;
        uint64_t suspects = 0;
        for (uint32_t s = 0; s < cfg_.n; ++s) {
            if (s == cfg_.self || !InMask(base, s)) continue;
            const uint64_t limit = ever_heard_[s] ? cfg_.fail_timeout_us
                                                  : std::max(cfg_.fail_timeout_us, cfg_.grace_us);
            if (now_us > last_heard_[s] + limit) suspects |= (1ULL << s);
        }
        if (suspects != 0) {
            const uint64_t next = base & ~suspects;
            if (Count(next) >= cfg_.f + 1) {
                // Diagnostics: who went silent, for how long, and how long since OUR
                // previous tick (a large own gap means this server stalled, not the peer).
                for (uint32_t s = 0; s < cfg_.n; ++s) {
                    if (suspects & (1ULL << s)) {
                        Notice("r%u suspects r%u: silent %lu us (own tick gap %lu us)",
                               cfg_.self, s, (unsigned long)(now_us - last_heard_[s]),
                               (unsigned long)(prev_tick_us ? now_us - prev_tick_us : 0));
                    }
                }
                const uint64_t v = std::max(view_, pv_) + 1;
                StartViewChange(v, next, now_us, fx);
                return;
            }
            // Fewer than f+1 left: no view can form; keep waiting.
        }
    }

    if (phase_ == Phase::NORMAL || now_us < last_resend_us_ + cfg_.resend_us) return;
    last_resend_us_ = now_us;

    if (phase_ == Phase::VIEW_CHANGE) {
        fx->out.push_back(Out{kAll, MakeSvc()});
        if (dvc_sent_ && LeaderOf(pa_) != cfg_.self) {
            fx->out.push_back(Out{static_cast<int32_t>(LeaderOf(pa_)), MakeDvc()});
        }
        return;
    }
    // RECOVERING
    const bool vr_leader = (LeaderOf(pa_) == cfg_.self);
    if (vr_leader) fx->out.push_back(Out{kAll, MakeStartRecovery()});
    if (!m_built_) {
        fx->out.push_back(Out{kAll, MakeRecoveryRequest()});
    } else if (!reported_) {
        fx->out.push_back(Out{kAll, MakeRecoveryPrepare()});
    } else if (!vr_leader) {
        fx->out.push_back(Out{static_cast<int32_t>(LeaderOf(pa_)), MakeReport()});
    }
}

// ---------------------------------------------------------------------------------------
// View change (VR)
// ---------------------------------------------------------------------------------------

void
ViewCoordinator::StartViewChange(uint64_t v, uint64_t a, uint64_t now_us, Effects *fx)
{
    if (phase_ == Phase::NORMAL || phase_ == Phase::JOINING) old_alive_ = alive_;
    phase_ = Phase::VIEW_CHANGE;
    fresh_started_ = false;
    pv_ = v;
    pa_ = a;
    phase_since_us_ = now_us;
    last_resend_us_ = now_us;
    ResetRound();
    fx->out.push_back(Out{kAll, MakeSvc()});
    MaybeSendDoViewChange(now_us, fx);   // f == 0
}

void
ViewCoordinator::OnStartViewChange(const proto::StartViewChange &m, uint64_t now_us,
                                   Effects *fx)
{
    OnHeard(m.sender_id(), now_us);
    if (phase_ == Phase::JOINING) {
        // Only the change that re-adds us, once caught up in the view it replaces.
        if (!have_view_ || !caught_up_ || !InMask(m.alive(), cfg_.self) ||
            m.view_id() <= view_) {
            return;
        }
    }
    const uint64_t cur = (phase_ == Phase::NORMAL) ? view_ : pv_;
    if (m.view_id() < cur) return;
    if (m.view_id() == view_ && phase_ == Phase::NORMAL) {
        // A laggard still changing to the view we already started: the VR leader re-sends
        // StartView.
        if (LeaderOf(alive_) == cfg_.self && m.view_id() > 0) {
            fx->out.push_back(Out{static_cast<int32_t>(m.sender_id()), MakeStartView()});
        }
        return;
    }
    if (!InMask(m.alive(), cfg_.self)) return;   // others suspect us; fenced if it forms

    if (m.view_id() > cur || phase_ == Phase::NORMAL) {
        StartViewChange(m.view_id(), m.alive(), now_us, fx);
    }
    if (phase_ != Phase::VIEW_CHANGE || m.view_id() != pv_) return;
    if (m.alive() != pa_) {
        // Conflicting proposals for one view number: move on with the intersection.
        const uint64_t next = pa_ & m.alive();
        if (Count(next) >= cfg_.f + 1 && InMask(next, cfg_.self)) {
            StartViewChange(pv_ + 1, next, now_us, fx);
        }
        return;
    }
    if (m.sender_id() != cfg_.self) svc_from_.insert(m.sender_id());
    MaybeSendDoViewChange(now_us, fx);
}

void
ViewCoordinator::MaybeSendDoViewChange(uint64_t now_us, Effects *fx)
{
    if (phase_ != Phase::VIEW_CHANGE || dvc_sent_ || svc_from_.size() < cfg_.f) return;
    dvc_sent_ = true;
    const uint32_t leader = LeaderOf(pa_);
    if (leader == cfg_.self) {
        dvc_from_[cfg_.self] = {view_, old_alive_};
        MaybeStartRecovery(now_us, fx);
    } else {
        fx->out.push_back(Out{static_cast<int32_t>(leader), MakeDvc()});
    }
}

void
ViewCoordinator::OnDoViewChange(const proto::DoViewChange &m, uint64_t now_us, Effects *fx)
{
    if (phase_ == Phase::JOINING) return;
    OnHeard(m.sender_id(), now_us);
    if (LeaderOf(m.alive()) != cfg_.self) return;
    const uint64_t cur = (phase_ == Phase::NORMAL) ? view_ : pv_;
    if (m.view_id() < cur) return;
    if (phase_ == Phase::NORMAL && m.view_id() == view_) {
        if (m.view_id() > 0) {
            fx->out.push_back(Out{static_cast<int32_t>(m.sender_id()), MakeStartView()});
        }
        return;
    }
    if (m.view_id() > cur || phase_ == Phase::NORMAL) {
        StartViewChange(m.view_id(), m.alive(), now_us, fx);
    }
    if (m.view_id() != pv_ || m.alive() != pa_) return;
    if (phase_ == Phase::RECOVERING) {
        fx->out.push_back(Out{static_cast<int32_t>(m.sender_id()), MakeStartRecovery()});
        return;
    }
    dvc_from_[m.sender_id()] = {m.has_last_normal_view() ? m.last_normal_view() : 0,
                                m.old_alive()};
    MaybeStartRecovery(now_us, fx);
}

void
ViewCoordinator::MaybeStartRecovery(uint64_t now_us, Effects *fx)
{
    if (phase_ != Phase::VIEW_CHANGE || LeaderOf(pa_) != cfg_.self) return;
    if (dvc_from_.count(cfg_.self) == 0 || dvc_from_.size() < cfg_.f + 1) return;
    // Recover from the oldest last-NORMAL view among the f+1 participants (ties: union).
    uint64_t oldest = UINT64_MAX, old_alive = 0;
    for (const auto &kv : dvc_from_) {
        const uint64_t v = kv.second.first, a = kv.second.second;
        if (v < oldest) { oldest = v; old_alive = a; }
        else if (v == oldest) old_alive |= a;
    }
    EnterRecovering(pv_, pa_, old_alive, now_us, fx);
    fx->out.push_back(Out{kAll, MakeStartRecovery()});
}

// ---------------------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------------------

void
ViewCoordinator::EnterRecovering(uint64_t v, uint64_t a, uint64_t old_a, uint64_t now_us,
                                 Effects *fx)
{
    phase_ = Phase::RECOVERING;
    pv_ = v;
    pa_ = a;
    old_alive_ = old_a;
    phase_since_us_ = now_us;
    last_resend_us_ = now_us;
    ResetRound();
    fx->begin_recovery = true;
    fx->out.push_back(Out{kAll, MakeRecoveryRequest()});
}

bool
ViewCoordinator::AcceptRecoveryView(uint64_t view, uint64_t alive, uint64_t old_alive,
                                    uint64_t now_us, Effects *fx)
{
    if (phase_ == Phase::JOINING) {
        // A recovery round that re-adds us (we missed its StartViewChange): join it only
        // once caught up. One that leaves us out: not ours to answer.
        if (!have_view_ || !caught_up_ || !InMask(alive, cfg_.self) || view <= view_) {
            return false;
        }
    } else if (Excluded(alive, fx)) {
        return false;
    }
    const uint64_t cur = (phase_ == Phase::NORMAL) ? view_ : pv_;
    if (view < cur || (phase_ == Phase::NORMAL && view == view_)) return false;
    if (phase_ != Phase::RECOVERING || view != pv_) {
        EnterRecovering(view, alive, old_alive, now_us, fx);
    }
    return phase_ == Phase::RECOVERING && pv_ == view;
}

void
ViewCoordinator::OnStartRecovery(const proto::StartRecovery &m, uint64_t now_us, Effects *fx)
{
    OnHeard(m.sender_id(), now_us);
    AcceptRecoveryView(m.view_id(), m.alive(), m.old_alive(), now_us, fx);
}

void
ViewCoordinator::OnLocalRecoveryData(RecoveryData d, Effects *fx)
{
    if (phase_ != Phase::RECOVERING) return;
    replies_[cfg_.self] = std::move(d);
    MaybeBuildM(fx);
}

void
ViewCoordinator::OnRecoveryReply(const proto::RecoveryReply &m, Effects *fx)
{
    if (phase_ != Phase::RECOVERING || m.view_id() != pv_) return;
    if (!InMask(pa_, m.sender_id()) || m_built_) return;
    replies_[m.sender_id()] = FromProto(m.txns(), m.kvs());
    MaybeBuildM(fx);
}

void
ViewCoordinator::MaybeBuildM(Effects *fx)
{
    // f+1 servers, itself included (design doc).
    if (m_built_ || replies_.count(cfg_.self) == 0 || replies_.size() < cfg_.f + 1) return;
    std::vector<RecoveryData> rs;
    rs.reserve(replies_.size());
    for (auto &kv : replies_) rs.push_back(std::move(kv.second));
    replies_.clear();
    my_m_ = MergeRecovery(rs);
    m_built_ = true;
    ms_[cfg_.self] = my_m_;
    prep_acks_.insert(cfg_.self);
    fx->out.push_back(Out{kAll, MakeRecoveryPrepare()});
    MaybeReport(fx);
}

void
ViewCoordinator::OnRecoveryPrepare(const proto::RecoveryPrepare &m, uint64_t now_us,
                                   Effects *fx)
{
    OnHeard(m.sender_id(), now_us);
    const bool installed_already = (phase_ == Phase::NORMAL && m.view_id() == view_);
    if (!installed_already &&
        !AcceptRecoveryView(m.view_id(), m.alive(), m.old_alive(), now_us, fx)) {
        return;
    }
    if (!installed_already) ms_[m.sender_id()] = FromProto(m.txns(), m.kvs());
    auto r = std::make_shared<proto::RecoveryPrepareReply>();
    r->set_view_id(m.view_id());
    r->set_sender_id(cfg_.self);
    r->set_pl_id(m.sender_id());
    fx->out.push_back(Out{static_cast<int32_t>(m.sender_id()), r});
}

void
ViewCoordinator::OnRecoveryPrepareReply(const proto::RecoveryPrepareReply &m, Effects *fx)
{
    if (phase_ != Phase::RECOVERING || m.view_id() != pv_ || m.pl_id() != cfg_.self) return;
    if (!m_built_ || !InMask(pa_, m.sender_id())) return;
    prep_acks_.insert(m.sender_id());
    MaybeReport(fx);
}

void
ViewCoordinator::MaybeReport(Effects *fx)
{
    if (reported_ || prep_acks_.size() < cfg_.f + 1) return;
    reported_ = true;
    const uint32_t leader = LeaderOf(pa_);
    if (leader == cfg_.self) {
        reports_.insert(cfg_.self);
        MaybeStartView(fx);
    } else {
        fx->out.push_back(Out{static_cast<int32_t>(leader), MakeReport()});
    }
}

void
ViewCoordinator::OnRecoveryReport(const proto::RecoveryReport &m, Effects *fx)
{
    if (phase_ == Phase::NORMAL && m.view_id() == view_ && LeaderOf(alive_) == cfg_.self) {
        // It missed our StartView.
        fx->out.push_back(Out{static_cast<int32_t>(m.sender_id()), MakeStartView()});
        return;
    }
    if (phase_ != Phase::RECOVERING || m.view_id() != pv_ || LeaderOf(pa_) != cfg_.self) return;
    if (!InMask(pa_, m.sender_id())) return;
    reports_.insert(m.sender_id());
    MaybeStartView(fx);
}

void
ViewCoordinator::MaybeStartView(Effects *fx)
{
    if (start_view_sent_) return;
    for (uint32_t s = 0; s < cfg_.n; ++s) {
        if (InMask(pa_, s) && reports_.count(s) == 0) return;   // every alive PL reported
    }
    start_view_sent_ = true;
    fx->out.push_back(Out{kAll, MakeStartView()});
    fx->install = true;
}

void
ViewCoordinator::OnStartView(const proto::StartView &m, uint64_t now_us, Effects *fx)
{
    OnHeard(m.sender_id(), now_us);
    if (!InMask(m.alive(), cfg_.self)) {
        // A view without us: (re)join it as a non-member.
        if (phase_ != Phase::JOINING) BecomeJoining(fx);
        if (!have_view_ || m.view_id() > view_) AdoptAsNonMember(m.view_id(), m.alive(), fx);
        return;
    }
    // A view with us while JOINING can only be a stale re-send (a round that re-adds us
    // needs our own report first, which takes us through RECOVERING).
    if (phase_ == Phase::JOINING) return;
    if (m.view_id() <= view_) return;
    if (phase_ == Phase::RECOVERING && m.view_id() == pv_) {
        fx->install = true;
        return;
    }
    if (phase_ != Phase::RECOVERING || m.view_id() > pv_) {
        // Missed the whole recovery round: adopt the view with whatever we hold.
        if (phase_ == Phase::NORMAL) old_alive_ = alive_;
        phase_ = Phase::RECOVERING;
        pv_ = m.view_id();
        pa_ = m.alive();
        ms_.clear();
        fx->install = true;
    }
}

ViewCoordinator::Install
ViewCoordinator::TakeInstall(uint64_t now_us)
{
    Install in;
    in.view = pv_;
    in.alive = pa_;
    in.old_alive = old_alive_;
    for (auto &kv : ms_) in.ms.push_back(std::move(kv.second));
    view_ = pv_;
    alive_ = pa_;
    phase_ = Phase::NORMAL;
    phase_since_us_ = now_us;
    ResetRound();
    for (uint32_t s = 0; s < cfg_.n; ++s) last_heard_[s] = std::max(last_heard_[s], now_us);
    return in;
}

// ---------------------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------------------

std::shared_ptr<google::protobuf::Message>
ViewCoordinator::MakeSvc() const
{
    auto m = std::make_shared<proto::StartViewChange>();
    m->set_view_id(pv_);
    m->set_sender_id(cfg_.self);
    m->set_alive(pa_);
    return m;
}

std::shared_ptr<google::protobuf::Message>
ViewCoordinator::MakeDvc() const
{
    auto m = std::make_shared<proto::DoViewChange>();
    m->set_view_id(pv_);
    m->set_sender_id(cfg_.self);
    m->set_alive(pa_);
    m->set_old_alive(old_alive_);
    m->set_last_normal_view(view_);
    return m;
}

std::shared_ptr<google::protobuf::Message>
ViewCoordinator::MakeStartRecovery() const
{
    auto m = std::make_shared<proto::StartRecovery>();
    m->set_view_id(pv_);
    m->set_sender_id(cfg_.self);
    m->set_alive(pa_);
    m->set_old_alive(old_alive_);
    return m;
}

std::shared_ptr<google::protobuf::Message>
ViewCoordinator::MakeRecoveryRequest() const
{
    auto m = std::make_shared<proto::RecoveryRequest>();
    m->set_view_id(pv_);
    m->set_requester_id(cfg_.self);
    m->set_alive(pa_);
    m->set_old_alive(old_alive_);
    return m;
}

std::shared_ptr<google::protobuf::Message>
ViewCoordinator::MakeRecoveryPrepare() const
{
    auto m = std::make_shared<proto::RecoveryPrepare>();
    m->set_view_id(pv_);
    m->set_sender_id(cfg_.self);
    m->set_alive(pa_);
    m->set_old_alive(old_alive_);
    ToProto(my_m_, m->mutable_txns(), m->mutable_kvs());
    return m;
}

std::shared_ptr<google::protobuf::Message>
ViewCoordinator::MakeReport() const
{
    auto m = std::make_shared<proto::RecoveryReport>();
    m->set_view_id(pv_);
    m->set_sender_id(cfg_.self);
    return m;
}

std::shared_ptr<google::protobuf::Message>
ViewCoordinator::MakeStartView() const
{
    auto m = std::make_shared<proto::StartView>();
    const bool normal = (phase_ == Phase::NORMAL);
    m->set_view_id(normal ? view_ : pv_);
    m->set_sender_id(cfg_.self);
    m->set_alive(normal ? alive_ : pa_);
    return m;
}

// ---------------------------------------------------------------------------------------
// Rejoin
// ---------------------------------------------------------------------------------------

void
ViewCoordinator::FreshStart(uint64_t now_us, Effects *fx)
{
    phase_ = Phase::NORMAL;
    view_ = pv_ = 0;
    alive_ = pa_ = old_alive_ = Topology::AllAlive(cfg_.n);
    for (uint32_t s = 0; s < cfg_.n; ++s) last_heard_[s] = now_us;
    fresh_started_ = true;
    fx->fresh_start = true;
}

void
ViewCoordinator::OnJoinRequest(const proto::JoinRequest &m, uint64_t now_us, Effects *fx)
{
    const uint32_t from = m.sender_id();
    OnHeard(from, now_us);
    if (from >= cfg_.n || from == cfg_.self) return;
    if (phase_ == Phase::JOINING) {
        join_from_.insert(from);
        return;
    }
    const uint64_t members = Membership();
    if (phase_ == Phase::NORMAL && fresh_started_ && view_ == 0 && InMask(members, from)) {
        // Whole-cluster restart and this peer hasn't started fresh yet: tell it view 0
        // (it starts fresh too) rather than excluding it.
        auto info = std::make_shared<proto::JoinInfo>();
        info->set_view_id(0);
        info->set_sender_id(cfg_.self);
        info->set_alive(alive_);
        fx->out.push_back(Out{static_cast<int32_t>(from), info});
        return;
    }
    if (InMask(members, from)) {
        // It restarted before we suspected it: its state is gone, so it can no longer
        // count as a member. Change view without it first (then it rejoins).
        const uint64_t next = members & ~(1ULL << from);
        if (Count(next) >= cfg_.f + 1) {
            StartViewChange(std::max(view_, pv_) + 1, next, now_us, fx);
        }
        return;
    }
    if (phase_ != Phase::NORMAL) return;   // it retries once the view settles
    auto info = std::make_shared<proto::JoinInfo>();
    info->set_view_id(view_);
    info->set_sender_id(cfg_.self);
    info->set_alive(alive_);
    fx->out.push_back(Out{static_cast<int32_t>(from), info});
}

void
ViewCoordinator::OnJoinInfo(const proto::JoinInfo &m, uint64_t now_us, Effects *fx)
{
    OnHeard(m.sender_id(), now_us);
    if (phase_ != Phase::JOINING) return;
    if (InMask(m.alive(), cfg_.self)) {
        // Only from a fresh-started peer of a whole-cluster restart: join view 0 fresh.
        if (!have_view_ && m.view_id() == 0) FreshStart(now_us, fx);
        return;
    }
    if (!have_view_ || m.view_id() > view_) AdoptAsNonMember(m.view_id(), m.alive(), fx);
}

void
ViewCoordinator::OnCaughtUp(Effects *fx)
{
    if (phase_ != Phase::JOINING || !have_view_ || caught_up_) return;
    caught_up_ = true;
    fx->out.push_back(Out{static_cast<int32_t>(LeaderOf(alive_)), MakeJoinReady()});
}

void
ViewCoordinator::OnJoinReady(const proto::JoinReady &m, uint64_t now_us, Effects *fx)
{
    OnHeard(m.sender_id(), now_us);
    if (phase_ != Phase::NORMAL || LeaderOf(alive_) != cfg_.self) return;
    if (m.view_id() != view_ || InMask(alive_, m.sender_id()) || m.sender_id() >= cfg_.n) {
        return;   // caught up in another view: it re-syncs and asks again
    }
    StartViewChange(view_ + 1, alive_ | (1ULL << m.sender_id()), now_us, fx);
}

std::shared_ptr<google::protobuf::Message>
ViewCoordinator::MakeJoinRequest() const
{
    auto m = std::make_shared<proto::JoinRequest>();
    m->set_sender_id(cfg_.self);
    return m;
}

std::shared_ptr<google::protobuf::Message>
ViewCoordinator::MakeJoinReady() const
{
    auto m = std::make_shared<proto::JoinReady>();
    m->set_view_id(view_);
    m->set_sender_id(cfg_.self);
    return m;
}

} // namespace dyna
} // namespace specpaxos

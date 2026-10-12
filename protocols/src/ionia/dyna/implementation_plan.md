# DYNA — Multi-Partition Write-Path Implementation Plan

> **Historical note.** This plan was written when this module lived in `dyna_multi/`
> alongside two earlier prototypes (`dyna/`, `dyna_new/`). Those prototypes have since been
> deleted and this module renamed to `dyna/` (namespace `dyna`, `-m dyna`, `DYNA_*` env
> vars); references below to the old prototypes are kept for context only.

> **Status.** Stage 1 (types + proto + `IndexedMinPQ` + bench wiring) and Stage 2
> (`ServerOrderer`: earlyBuffer, CV release loop, always-on counter-veto, specLog +
> `highestReleasedT`) are **complete and green** in Docker. "Single partition" was only a
> test-workload scaffold, never an implementation restriction — the protocol is
> multi-partition from the ground up, so all commit/quorum/slow-path logic is written
> multi-partition-generic (a single-partition txn is the *m = 1* case).

## Context

We are implementing the **final** version of the DYNA protocol as a new module,
`Ionia/protocols/src/ionia/dyna/`. The two existing prototypes (`dyna/`,
`dyna_new/`) were single-key / effectively single-partition proofs-of-concept and are
kept only as reference. The ground truth is `dyna/Dyna-Algorithm - Google Docs.pdf`
(the algorithm) and `dyna/spec.md` (implementation companion, sections 1–2 only;
handlers §3+ are forward-referenced but unwritten, and spec.md is itself subject to change).

The defining new capability is **multi-key, multi-partition transactions** under a
**shared-replica topology**: `N = 2f+1` servers, every server stores all `N` partitions,
server `i` leads partition `i` and follows the rest. A transaction has a single
`OrderKey` and therefore lives in exactly one server-wide buffer even when it spans
partitions. This inverts the `dyna_new` design (which sharded ordering state per
partition): dyna has **one per-server ordering domain**, not one per partition.

This plan covers the complete **multi-partition WRITE path**: types + `IndexedMinPQ`
(done), the per-server ordering engine (done), then the client↔replica commit handshake
(fast + slow, generic multi-partition quorum), background timestamp agreement
(`TsAgree`), and replication to durable `COMMITTED`. **Dependency-gated batched disk
execution and the read path are deferred to a follow-up plan** once the write path is
green. RMW, recovery/view-change, thrifty multicast, and network-partition handling
remain out of scope per spec §0.4.

### Resolved design decisions
- **DECISION B — Invariant 2 is unconditional, but the veto is scoped to
  client-originated transactions only (the `earlyBuffer` path).** A fresh client txn is
  *late* on arrival iff `clock() > T.t` **OR** `orderKey(T) <= highestReleasedT`; it is
  admitted to `earlyBuffer` only if on-time by *both*, and the same veto re-checks at
  release (never release/endorse a buffered client txn whose
  `orderKey <= highestReleasedT`). This is always-on, not a runtime flag.
  **Protocol-decided deadlines are exempt and are NEVER vetoed:** the slow-path/fast-path
  `agreedT` (`FastCommit`/`SlowCommit`), the `TsAgree` max, a leader→follower
  sync/replicate (`ReplicatePrepare`/`ReplicateCommit`), and a `FetchReply` body are all
  inserted **directly into `specLog`**, bypassing `earlyBuffer` — so the veto (which lives
  only on the `earlyBuffer` admission + release path) never sees them by construction.
  They still **bump** `highestReleasedT` (which remains the max `OrderKey` ever appended
  to `specLog`, monotonic non-decreasing, never lowered), but are themselves authoritative
  and installed unconditionally. *(In-scope from Stage 3: once the multi-partition slow
  path exists, a protocol-inserted high-`t` entry can advance `highestReleasedT` past a
  client txn still in `earlyBuffer`; at release that buffered client txn fails the veto —
  the `ServerOrderer` release guard already refuses it; the leader then re-timestamps it,
  a follower drops/rejects. Stage 3/4 must wire that re-timestamp-on-veto, which today
  just drops.)*
- **DECISION A — commit quorum (default, shared-pool):** `f+1` replies *total* that
  **include every involved partition leader**, all agreeing on `(viewId, t)`. Implemented
  **generically from Stage 3** via `TxnQuorum` over `involvedLeaders`; a single-partition
  txn is the *m = 1* case. The per-partition alternative (rejected under §0.2) is not built.
- **Coexist:** new `-m dyna` flag; leave `dyna/` and `dyna_new/` untouched; update
  `CLAUDE.md` to name dyna the active target.

---

## Architecture summary

Single per-server ordering engine (`ServerOrderer`) owning **one** set of buffers under
**one** lock, with a single condition-variable release loop (reusing the `dyna_new`
`RealClock::SleepUntil`/`Interrupt` pattern — no busy-wait). Partitioning survives only
as a static `Topology` map (`partitionOf(key)`, `leaderOf(partition)=identity`) used to
compute leadership and involved leaders, plus per-partition disk backends later. All
network sends and (later) disk I/O happen outside the ordering lock.

```
DynaReplica (TransportReceiver)
  └── ServerOrderer            // one per server
        ├── earlyBuffer : IndexedMinPQ<TxnId,TxnEntry>   // on-time, ordered by OrderKey
        ├── lateBuffer  : HashMap<TxnId,TxnEntry>        // late follower arrivals
        ├── specLog     : IndexedMinPQ<TxnId,TxnEntry>   // released; supports update-key
        ├── highestReleasedT : OrderKey                  // monotonic; drives the veto
        ├── replyCache  : reuse dedup_table.h / request_dedup.h (per-server)
        └── RealClock + one release thread (SleepUntil/Interrupt)
DynaClient (specpaxos::Client)
  └── per-request ClientReq state machine + TxnQuorum
```

---

## Files to create (`dyna/`)

New source (genuinely greenfield):
- `indexed_min_pq.h` — the update-key / erase-by-id binary min-heap (the one new data
  structure; the codebase has only `std::priority_queue`, no update-key). Interface:
  `push(id,val)`, `peek_min()`, `pop_min()`, `contains(id)`, `find(id)` (mutable, no
  reheap), `update_key(id)` (reheap after caller mutates `t`), `erase(id)`, `size/empty`.
  Backed by `std::vector<pair<Id,Val>>` + `HashMap<Id,size_t> pos_`; `pos_` maintained
  on every swap inside `sift_up`/`sift_down`. Ordered by
  `OrderKey = (t, ClientId, RequestId)` via an `OrderKeyOf` functor.
- `txn_types.h` — `TxnId{client_id,request_id}` (== `ClientAndReqId`), `OrderKey` with
  lexicographic `operator<=>`, `Status{SPECULATIVE,COMMITTING,COMMITTED}`,
  `TxnType{WRITE,READ}`, and the server-side `TxnEntry` (holds `txn`, live `t`, `status`,
  `isLeaderForThis`, `ledKeys`, `results`, and leader-only `tsProposals`/`prepareReplies`
  bookkeeping — the latter two unused until steps 6–7 but declared now).
- `server_orderer.h` / `server_orderer.cc` — the per-server engine (see methods below).
- `replica.h` / `replica.cc` — `DynaReplica : TransportReceiver`; ctor
  `(const Configuration&, int replicaIdx, Transport*)`; `ReceiveMessage` dispatches the
  proto types to handlers; owns the `ServerOrderer` and its release thread.
- `client.h` / `client.cc` — `DynaClient : specpaxos::Client`; ctor
  `(const Configuration&, Transport*, uint64_t clientid=0)`; implements `Invoke`,
  `InvokeUnlogged`, `ReceiveMessage`; runs the `ClientReq` state machine + `TxnQuorum`.
- `topology.h` — `partitionOf(key,n)` (hash%n, reuse `partition_map.h` logic),
  `leaderOf(p)=p`, `involvedPartitions(txn)`, `involvedLeaders(txn)` (§1.6).
- `commit_quorum.h` → adapt to `TxnQuorum`: replaces `dyna_new`'s single-leader
  `CommitQuorum` with the DECISION-A predicate (all involved leaders present AND
  `|replies|>=f+1` AND shared `(viewId,t)`; expose `fast_ready()`, `leaders_disagree()`,
  `slow_agreed_t()=max leader t`). In steps 3–4 the single-partition path uses it with a
  one-leader involved set.
- `dyna-proto.proto` — the 12 messages (§2). For foundation we exercise
  `ClientRequest`, `SpecReply`, `RejectReply`, `FastCommit`, `SlowCommit`, `CommitReply`,
  `FetchRequest`, `FetchReply`; the remaining four (`TsAgree`, `ReplicatePrepare*`,
  `ReplicateCommit`) are declared now (used in steps 6–7). `Txn` uses `repeated bytes
  r_set` and `repeated KV w_set` (proto2). Every message carries `txn_id` (client_id +
  request_id) and `view_id`.
- `Rules.mk` — mirror `dyna_new/Rules.mk`: add `SRCS`, `PROTOS`, define
  `OBJS-dyna-client` and `OBJS-dyna-replica`, `include tests/Rules.mk`.
- `tests/` — one binary per milestone: `types_test`, `indexed_pq_test`,
  `orderer_release_test`, `client_fastpath_test`, `slowpath_test`.

Copy/adapt from `dyna_new/` (rename namespace `dyna_new` → `dyna`):
- `wall_clock.h` — `Clock`/`RealClock`/`MockClock` used **as-is** (one instance per
  server, driving the single release loop; `MockClock` drives deterministic tests).
- `deadline.h` — `ReplicaOwdEstimator` + `ClientBoundTracker` for the client's initial
  `t = now + bound_us()` and (step 9, later) read-retry OWD growth.
- `dedup_table.h` + `request_dedup.h` — back `replyCache` (§1.4 `CachedReply`) and
  at-most-once, as a **per-server** instance (a `TxnId` is not owned by one partition).
  Add `Tthresh` GC: drop a cached reply when `reply.t + Tthresh < clock()`.
- `storage_backend.h` + `in_memory_backend.{h,cc}` — declared/linked now; disk execution
  wired in step 8 (later plan).

## Files to modify (framework wiring)

- `bench/replica.cc` — add `PROTO_DYNA` to the enum (~line 100), a
  `strcasecmp(optarg,"dyna")` case in the `-m` switch (~line 156), the usage
  string, and an instantiation block mirroring the `PROTO_DYNA_NEW` block (~line 300)
  that constructs `specpaxos::dyna::DynaReplica`.
- `bench/client.cc` — symmetric changes: `PROTO_DYNA` enum (~line 90), `-m` case
  (~line 162), and `case PROTO_DYNA: client = new
  specpaxos::dyna::DynaClient(...)` (~line 408).
- `bench/Rules.mk` — append `$(OBJS-dyna-client)` to the `client` link line and
  `$(OBJS-dyna-replica)` to the `replica` link line.
- `Makefile` — add `include dyna/Rules.mk` after `include dyna_new/Rules.mk`.
- `CLAUDE.md` — refresh: name `dyna/` the active target; note `dyna/` and
  `dyna_new/` are prior prototypes; point "source of truth" at `dyna/spec.md` +
  the algorithm PDF; record the shared-replica multi-partition model, the per-server
  single-ordering-domain design, and the always-on counter-veto (DECISION B).

---

## `ServerOrderer` — core methods (steps 1–4 subset)

- `SubmitTxn(TxnEntry, poke=true)` — arrival classification (§ write path):
  compute `isLeaderForThis`/`ledKeys` from `Topology`; if **on-time by both clauses**
  (`clock() <= T.t` AND `orderKey(T) > highestReleasedT`) → `earlyBuffer.push`; else
  *late* → if leads any key in wSet, bump `T.t := clock()` (guaranteed
  `> highestReleasedT`) and treat as released-late-leader; else follower → `lateBuffer`
  (writes silent; reads emit `RejectReply` via callback). Then `clock->Interrupt()`.
- `ReleaseDue()` — drain `earlyBuffer` entries with `orderKey.t <= clock()` **and**
  `orderKey > highestReleasedT` in OrderKey order: mark `SPECULATIVE`, `specLog.push`,
  set `highestReleasedT = max(highestReleasedT, orderKey)`, collect for `OnSpecReply`.
  Fire callbacks outside the lock. (Synchronous + `MockClock`-testable.)
- `Start()/Stop()` + `ReleaseLoop()` — single background thread; after each drain,
  `clock->SleepUntil(earlyBuffer.peek_min().t)`; woken by `Interrupt()`.
- `ApplyCommit(TxnId, agreedT, slow, out)` — **protocol-decided deadline path; NOT
  vetoed.** Locate `specLog → lateBuffer → kNeedFetch`; on found, set live `t := agreedT`
  and insert/keep **directly in `specLog`** (`update_key` re-heapifies), bump
  `highestReleasedT`, `status := COMMITTING`; if `slow`, signal caller to send
  `CommitReply`. `agreedT` is authoritative even if `agreedT <= highestReleasedT` — no
  veto is applied here (the entry never touches `earlyBuffer`).
- `InstallFetched(Txn)` — materialize a `TxnEntry` from a `FetchReply` and insert
  **directly into `specLog`** (protocol-decided; not vetoed).
- Accessors: `highest_released_t()`, `early_size()`, `late_size()`, `speclog_size()`
  (locked snapshots, like `PartitionOrderer`'s inspection methods).

## Client state machine (`ClientReq`)

`MULTICASTING → COLLECTING → SLOW_WAIT → DONE/FAILED`:
- **MULTICASTING:** `currentT = now + bound_us()`; multicast `ClientRequest` to all N.
- **COLLECTING:** accumulate `SpecReply` into `TxnQuorum`; on each →
  `fast_ready()` → broadcast `FastCommit(agreedT=t)` → DONE;
  `leaders_disagree()` → `SlowCommit(agreedT=max leader t)` → SLOW_WAIT;
  `RejectReply` → grow OWD, re-multicast with larger `currentT` → MULTICASTING.
- **SLOW_WAIT:** accumulate `CommitReply` at `agreedT`; quorum re-forms → DONE.
- Retransmission via a single periodic resend sweep (reuse `dyna_new` client pattern),
  not per-request timers.

---

## Staging (each stage green before the next)

**Done:**
1. ✅ **Types + IndexedMinPQ.** `txn_types.h`, `dyna-proto.proto`, `indexed_min_pq.h`,
   `Rules.mk`, bench wiring, `Makefile` include. *Green:* `types_test`, `indexed_pq_test`.
2. ✅ **Per-server ordering engine.** `ServerOrderer` earlyBuffer + CV release loop +
   always-on counter-veto + specLog + `highestReleasedT` + `SpecReply` callback;
   `topology.h`. *Green:* `orderer_release_test` (`MockClock`, no busy-wait).

**Remaining write path (commit logic multi-partition-generic; single- vs two-partition is
a test progression, not a code split):**

3. **Client + replica commit handshake (fast + slow).** Wire `DynaReplica::ReceiveMessage`
   to the `ServerOrderer` (`ClientRequest` → `SubmitTxn`; `OnSpecReply` → send `SpecReply`).
   `DynaClient` runs MULTICASTING → COLLECTING with `TxnQuorum` over `involvedLeaders`
   (DECISION A) → `FastCommit`; on leader-`t` disagreement → `SlowCommit(max)` → SLOW_WAIT
   → `CommitReply` quorum. Replica `ApplyCommit` (specLog → lateBuffer → `FetchRequest`),
   plus `lateBuffer` + late-leader re-timestamp and release-time-veto re-timestamp.
   *Tests* (end-to-end, 3 replicas): (a) single-partition fast commit; (b) two-partition
   fast commit at one timestamp; (c) late leader → slow path commits; (d) commit racing
   ahead of body → fetch then apply.
4. **Background timestamp agreement (`SPECULATIVE → COMMITTING`).** `TsAgree` among involved
   leaders for a txn stuck SPECULATIVE past a timeout; `t := max(proposals ∪ {own})` →
   `COMMITTING` (no-op with one involved leader). *Test:* two-partition write, late at one
   leader, resolves to the max without any client commit message.
5. **Replication (`COMMITTING → COMMITTED`).** `ReplicatePrepare` → `ReplicatePrepareReply`
   (collect `f`) → `ReplicateCommit`. *Test:* status reaches `COMMITTED` after `f` prepare-replies.

*(Then, follow-up plan: dependency-gated batched disk execution, and the read path.)*

---

## Verification

- **Build (Docker; host lacks toolchain):**
  `docker build --platform linux/amd64 -f docker/Dockerfile.udp -t ionia-udp .`
  then inside the container: `cd /work/ionia/protocols/src/ionia && make` — confirms the
  new module and bench wiring link, and `bench/replica -m dyna` / `bench/client -m
  dyna` are recognized.
- **Unit tests:** run each `tests/` binary per stage (built via `tests/Rules.mk`),
  gated on `MockClock` for deterministic release/veto behavior. Do not advance a stage
  until its test is green (per CLAUDE.md "one stage at a time").
- **Smoke (after step 3):** adapt the existing smoke harness pattern —
  `SMOKE_PROTOCOL=dyna ./docker/docker-smoke.sh` (add a `dyna` branch mirroring
  the `dyna` branch: `LOW_MEMORY=0`, `IONIA_DB_MODE=memory`). Confirm a single-partition
  write commits end-to-end on the fast path; with induced leader-reply delay it falls to
  the slow path and still commits.
- **Sanity invariants to assert in tests:** `earlyBuffer`/`specLog` ordering identical to
  `OrderKey`; `highestReleasedT` never decreases; no release with
  `orderKey <= highestReleasedT`; commit requires the (single) leader reply plus `f+1`
  agreement.

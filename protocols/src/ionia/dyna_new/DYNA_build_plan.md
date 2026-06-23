# DYNA — Implementation Build Plan

**Purpose.** A staged plan for implementing DYNA's normal operation (per `SPEC.md`) in C++. Each stage has a goal, the spec sections it draws from, concrete tasks, and an **acceptance test** that must pass before moving on. Build in order — each stage depends on the previous one being correct.

**How to use this with a coding agent (Claude Code or Cursor).**
- `DYNA_normal_operation_spec.md` (in this same `dyna_new/` directory) is the source of truth for protocol decisions.
- Drive **one stage at a time.** Do not ask the agent to implement multiple stages in a single pass — that is how implementations drift and how the previous version became hard to debug.
- Require the acceptance test to pass (and be reviewed) before starting the next stage.
- Reference spec section numbers in commits so the implementation stays auditable against the design.
- **Do not optimize for performance until Stage 8.** Correctness first; profiling is its own pass.

**Framework constraint — do not re-implement shared infrastructure.**
`dyna_new/` lives inside the existing Ionia codebase and must share its transport, configuration, build system, and bench harness with VR and IONIA. Specifically:
- **Build system:** Make (not CMake). Add a `dyna_new/Rules.mk` contributing `OBJS-dyna-new-client` and `OBJS-dyna-new-replica`; link those into `bench/client` and `bench/replica` via `bench/Rules.mk`.
- **Transport:** Use `FastTransport` from `lib/fasttransport.h` (UDP-backed). Do not write a transport layer. `DynaNewReplica` inherits `TransportReceiver`; `FastTransport` calls its `ReceiveMessage(remote, type, data, meta)`.
- **Client base class:** `DynaNewClient` inherits `specpaxos::Client` from `common/client.h` (same base as VR client).
- **Configuration:** Use `lib/configuration.h`'s `Configuration` class for cluster topology.
- **Bench entry points:** Wire `-m dyna_new` into `bench/client.cc` and `bench/replica.cc`. Do not modify VR/IONIA paths.
- **Protocol messages:** Define in a new `.proto` file (e.g. `dyna_new/dyna-new-proto.proto`). Dispatch is by protobuf type-name string via `lib/message.h`.
The existing `dyna/` module is the precise model for all of the above; consult it when in doubt about wiring.

**Guiding constraint for every stage.** Two invariants are load-bearing and easy to break silently — call them out to the agent in every relevant stage:
1. **Early-buffer ordering is deterministic and identical at all replicas:** sort by `deadline`, then by `⟨client_id, request_id⟩`. The per-request commit rule is only sound if every replica orders ties the same way.
2. **Disk persistence never sits on the critical path.** Commit depends on memory agreement (`f+1` including leader), not on disk.

---

## Stage 0 — Project scaffold

**Goal.** A buildable skeleton that compiles as part of the existing Ionia Make build and registers `dyna_new` as a selectable protocol mode — no protocol logic yet.

**Tasks.**
- Create `dyna_new/Rules.mk` following the pattern of `dyna/Rules.mk`: declare `SRCS`, define `OBJS-dyna-new-client` and `OBJS-dyna-new-replica` (each depending on `$(OBJS-client)`, `$(LIB-message)`, `$(LIB-configuration)`, `$(LIB-transport)` as appropriate).
- Create stub `dyna_new/replica.h` / `dyna_new/replica.cc`: `DynaNewReplica : public TransportReceiver` with an empty `ReceiveMessage` and a constructor `(const Configuration&, int replicaIdx, Transport*)`.
- Create stub `dyna_new/client.h` / `dyna_new/client.cc`: `DynaNewClient : public specpaxos::Client` with empty `Invoke` and `ReceiveMessage`.
- Create `dyna_new/dyna-new-proto.proto` with placeholder message types (at minimum `RequestMessage` and `Reply`).
- Add `-m dyna_new` handling to `bench/client.cc` and `bench/replica.cc` (instantiate `DynaNewClient` / `DynaNewReplica` with `FastTransport`).
- Include `dyna_new/Rules.mk` in the top-level `Makefile` and add `OBJS-dyna-new-client` / `OBJS-dyna-new-replica` to the `bench/client` and `bench/replica` link rules in `bench/Rules.mk`.
- A clock abstraction (`dyna_new/wall_clock.h`) wrapping `std::chrono::system_clock` in microseconds — identical in role to `dyna/wall_clock.h` — so the release loop can be tested with a mockable clock.

**Acceptance test.** `make` inside the container succeeds with no errors for `dyna_new` objects. Running `./bench/replica -i 0 -c sample/config -m dyna_new` starts and accepts connections without crashing (requests are silently dropped — that is fine at this stage).

---

## Stage 1 — Inert data types (no behavior)

**Goal.** All message formats and state containers from the spec exist as types, with serialization. No protocol logic.

**Spec sections.** §3 (per-replica state), §4 (message formats), §3.4 (StorageBackend interface).

**Tasks.**
- Define structs for every message in §4: `request`, `reply`, `sync`, `sync_confirm`, `commit`, `fetch`/`fetch_resp`. Include all fields exactly as specified (including `view_id`, `owd_estimate`, `is_leader`, `log_pos`).
- Define the per-partition state block (§3.2–3.3): `early_buffer`, `late_buffer`, `deadline_counter`, `synced_log`, `unsynced_log`, `sync_point`, `commit_point`.
- Define `StorageBackend` (§3.4) as an abstract interface.
- UDP wire serialization/deserialization for every message type.

**Acceptance test.** Round-trip serialization test for every message type (serialize → deserialize → assert structural equality). A test that constructs a per-partition state block and asserts initial values (`deadline_counter = 0`, empty buffers, `view_id = 0`).

---

## Stage 2 — Storage backend

**Goal.** The storage abstraction the protocol sits on. Transport is already provided by `FastTransport` — do not re-implement it.

**Spec sections.** §3.4 (storage).

**Note on transport.** `FastTransport` (from `lib/fasttransport.h`) handles all UDP send/receive, message framing, and type dispatch. To send a message: call `transport->SendMessage(receiver, addr, msg)` or `transport->SendMessageToReplica(receiver, replicaIdx, msg)`. To multicast: loop over replica indices. To handle inbound messages: parse them in `ReceiveMessage` by checking the `type` string against protobuf type names. Packet deduplication, reordering tolerance, and drops are already handled at the transport layer or must be handled in the replica's own dedup table — not by a custom socket loop.

**Tasks.**
- Implement `InMemoryBackend` satisfying the `StorageBackend` interface from Stage 1: `mem_apply`, `mem_read`, `disk_persist`, `disk_read` over `std::unordered_map`s, with an **optional configurable simulated write latency** for `disk_persist` (controlled by an env var or constructor parameter).
- Implement the dedup table `⟨client_id, request_id⟩ → cached_reply` (§6.1, §1.6) as a data structure — wiring comes later.
- Wire `InMemoryBackend` into `DynaNewReplica` (constructor takes or creates a backend).

**Acceptance test.** `InMemoryBackend`: write-then-read returns the value; `disk_persist` with simulated latency completes asynchronously — the calling thread is not blocked — and the value is eventually readable via `disk_read`.

---

## Stage 3 — Single replica, single partition: the deadline mechanism

**Goal.** The heart of the protocol, in isolation. One replica, acting as leader for one partition. No followers, no sync, no client quorum. **This is the stage most likely to be the source of the earlier slowness — get it provably right and clean.**

**Spec sections.** §2 (deadlines), §3.2, §6.1 (release handler), §6.2 (slow path reassignment).

**Tasks.**
- Implement the early-buffer as a min-priority-queue on `(deadline, client_id, request_id)`.
- Implement the **release loop**: release every entry whose `deadline ≤ clockTime()`, advance `deadline_counter`, append to `synced_log`, `mem_apply` writes. Drive it off the mockable clock — **no busy-wait**; use a timer/condition-variable that wakes at the next deadline.
- Implement on-time vs. late classification (`deadline > deadline_counter` → early buffer; else late).
- Implement the slow-path reassignment (§6.2): late request → `late_buffer` → reassign `deadline = max(clockTime(), deadline_counter + 1)` → into early buffer.

**Acceptance test.**
- Feed requests with out-of-order arrival but distinct deadlines; assert the log comes out in deadline order.
- Feed a "late" request (deadline ≤ counter); assert it is reassigned and still appended after the counter, never dropped.
- Tie-break test: two requests, equal deadline, different ids; assert deterministic ordering by `⟨client_id, request_id⟩`.
- Timing test on the mock clock: an entry with a future deadline is **not** released early and **is** released exactly when the clock reaches its deadline.
- **No-busy-wait check:** with an empty buffer and idle clock, the release thread consumes ~0 CPU (sanity, not a hard number).

---

## Stage 4 — Client + fast write path + commit rule

**Goal.** End-to-end fast-path blind write across `2f+1` replicas with the real client commit rule. Followers participate but only speculatively (no reconciliation yet).

**Spec sections.** §5 (client), §6.1 (leader fast path + follower speculative release), §1.1 (per-request commit assumption).

**Tasks.**
- Client: deadline computation `curr + s + d` (§2), multicast, collect replies, `checkCommit` for `BLIND_WRITE`.
- Leader replica: on release, send leader reply (`is_leader=true`) to client.
- Follower replicas: on speculative release from their own early buffer, append to `unsynced_log` and send a follower reply (`is_leader=false`, `result=null`).
- Client commit rule: `f+1` replies including the leader, agreeing on the leader's deadline and view. Expose the quorum as a `write_quorum` **parameter** (default `f+1`) per §5's note — do not hard-code the literal.
- OWD feedback: replicas report a percentile OWD estimate; client maintains `owd_table` and uses `max` across replicas with clamping to `[0, D_max]`.

**Acceptance test.**
- 3-replica (`f=1`) cluster, all on-time: a blind write commits with exactly the leader + 1 follower reply agreeing on the leader's deadline.
- A write where only followers (not the leader) reply does **not** commit (leader reply is mandatory).
- A write with replies carrying mismatched deadlines does **not** commit until `f+1` agree on the leader's deadline.
- `write_quorum` is read from config, not a literal `2`.

---

## Stage 5 — Follower reconciliation + slow write path + corrected replies

**Goal.** The full write path, including the subtle corrected-reply logic. **This is where the most careful tests belong** — the interaction of speculative reply, sync, corrected reply, and client supersession is the easiest place for a commit bug to hide.

**Spec sections.** §6.2 (slow path), §6.4 (follower reconciliation), §5 (client supersession), §3.3 (synced/unsynced split).

**Tasks.**
- Leader: broadcast `sync` per appended request (with possibly-reassigned deadline); doubles as heartbeat; allow batching hook (off by default).
- Follower: handle `sync` — the three cases of §6.4 (match; deadline differs → adopt + reposition + **send corrected reply to client**; not found → `fetch` from leader). Update `sync_point`, send `sync_confirm`.
- Follower: promote `unsynced_log` entries to `synced_log` on reconciliation.
- Client: a later reply from a replica **supersedes** its earlier reply (§5). Re-check commit on supersession.
- Implement `fetch`/`fetch_resp` for the sync-miss case.

**Acceptance test.**
- **Corrected-reply commit:** force the leader onto the slow path (deliver the request late at the leader only) so it reassigns the deadline; a follower that delivered on-time must send a corrected reply; assert the client commits only on the leader's (reassigned) deadline, and the follower's stale first reply never produces a false commit.
- **Sync-miss:** drop a request at one follower; on `sync`, that follower fetches the body and ends up with a `synced_log` matching the leader.
- **Supersession:** assert the client correctly replaces a follower's first reply with its corrected one and re-evaluates the quorum.
- After any write commits, all `2f+1` replicas' `synced_log`s for that partition are identical (content and order).

---

## Stage 6 — Read paths (fast + slow)

**Goal.** Linearizable reads with leader execution + follower endorsement.

**Spec sections.** §6.3 (read paths), §5 (read commit rule), §6.4 (slow read reconciliation).

**Tasks.**
- Leader: on read release, ensure all writes ordered before the read are **applied to memory** (ordering requirement, **not** a synchronous disk flush — §6.3 note), then `mem_read`, reply with `result` and `log_pos`.
- Followers: on read release, acknowledge/endorse with `view_id` and `log_pos` (no execution).
- Client read commit: leader's data reply + `f` endorsements agreeing on the leader's view and `log_pos`.
- Slow read path: same reassignment structure as slow write.

**Acceptance test.**
- A read issued after a committed write returns the written value (linearizable w.r.t. that write).
- A read commits with leader data reply + `f` endorsements on matching view/`log_pos`; fewer endorsements does not commit.
- Confirm via instrumentation that serving a read triggers memory application of prior writes but **not** a synchronous disk write.

---

## Stage 7 — Background ordering / persistence

**Goal.** Off-critical-path durability.

**Spec sections.** §7, §3.3 (`commit_point`).

**Tasks.**
- Leader: collect `sync_confirm`s; on `f+1` (including own append) with matching deadline, `disk_persist` and broadcast `commit`.
- Follower: on `commit`, `disk_persist` its own copy.
- Disk-write batching (hook; can be simple at first).
- Advance `commit_point`.

**Acceptance test.**
- Commit latency measured at the client is **unaffected** by `disk_persist` simulated latency (turn the simulated latency up and confirm client-observed commit time does not move).
- After `commit` propagates, every replica's `disk_read` for the key returns the committed value.
- `commit_point` advances to cover committed requests.

---

## Stage 8 — Performance pass

**Goal.** Now make it fast. Only start once Stages 0–7 are correct and tested.

**Spec sections.** §9 (implementation notes), §7 (batching).

**Tasks.**
- **Per-partition dispatch threads** (§9): one release/dispatch thread per partition so disjoint partitions order concurrently. Avoid a single coarse lock over shared replica state — this is a prime suspect for the earlier slowness.
- Confirm the release loop is timer-driven, not polling/busy-waiting.
- Confirm `disk_persist` is fully async and batched; nothing on the commit path blocks on it.
- Pipelining: a replica continues processing subsequent requests without blocking on quorum checks (the decoupling the design intends).
- Profile under the latency-throughput benchmark; identify the actual bottleneck (CPU-bound vs. lock contention vs. I/O on the critical path) before changing anything further.

**Acceptance test.**
- Latency-throughput curve generated under the same closed-loop / open-loop harness you will use against VR and IONIA.
- A profiling artifact (flame graph or equivalent) showing where time goes at saturation.
- Regression check: all Stage 0–7 acceptance tests still pass after the performance changes.

---

## Stage 9 — Benchmark harness + baseline comparison

**Goal.** The apples-to-apples comparison that motivated the rewrite.

**Tasks.**
- Closed-loop and open-loop (Poisson) clients, matching the methodology you'll use for VR and IONIA.
- **All protocols run over the same `StorageBackend`** so differences reflect the protocol, not the engine.
- Sweep load to saturation; record median latency and throughput.
- (When ready) swap `InMemoryBackend` → `SplinterDBBackend` for disk-based numbers without touching protocol code.

**Acceptance test.** Reproducible latency-throughput plots for DYNA vs. baselines over an identical backend and workload, with the run configuration captured for repeatability.

---

## A note on the existing Cursor implementation

Before discarding it, diagnose *why* it was slow — if the cause was architectural, a fresh agent given the same loose instruction could reproduce it. Likely suspects, in rough order:
1. A single coarse lock over all replica state instead of per-partition concurrency (Stage 8).
2. A polling/busy-wait release loop instead of a timer (Stage 3).
3. Synchronous disk writes leaking onto the commit path (Stage 7 / invariant 2).
4. The slow path triggering far more than expected because deadlines were too tight (OWD percentile / `s` / `d` estimation in Stage 4).

If you can profile the old version, the bottleneck it reveals tells you which stage to scrutinize hardest in the rewrite.

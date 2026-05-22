# DYNA Implementation Notes

This document tracks implementation progress, behavior choices, and known concerns for the DYNA protocol module under this directory.

## Scope

- Module path: `ionia/dyna/`
- Current focus: single-shard (single network-partition) normal processing fast path.
- Terminology:
  - "leader" means partition leader unless explicitly stated as global leader.
  - In one shard, every replica is leader for one or more partitions and follower for others.

## Implemented So Far

### Protocol wiring

- Added DYNA protobuf definitions in `dyna-proto.proto`.
- Added DYNA client and replica implementations:
  - `client.h`, `client.cc`
  - `replica.h`, `replica.cc`
- Integrated `dyna` build rules via `Rules.mk`.
- Integrated `-m dyna` mode in bench client/replica launch paths.

### Dynamic deadlines (Nezha-style, no proxy)

- Modular code under `dyna/deadline/`:
  - `deadline_config.{h,cc}` — `DeadlineConfig::FromEnvironment()` (`DYNA_*` knobs below).
  - `client_bound_tracker.{h,cc}` — event-driven bound = **max** of last known per-replica OWD (after **every K** samples), clamped by **`DYNA_MAX_OWD_US`**.
  - `replica_owd_estimator.{h,cc}` — per **`client_id`** sliding-window **percentile** over **`recv_us − send_time_us`** samples.
  - `wall_clock.h` — **`std::chrono::system_clock`** microsecond epoch (Nezha-style; comparable across GCP VMs with NTP).
- Wire: optional **`owd_us`** on **`FastReply`** and **`ReadResult`**; **`deadline_us = send_time_us + bound_us`** on new requests (`Invoke` snapshots **`send_us`** via wall clock, then adds current bound).

Environment (optional overrides):

| Variable | Role |
|----------|------|
| `DYNA_INITIAL_BOUND_US` | Cold-start bound per replica slot (μs); default `2000`. |
| `DYNA_DEADLINE_SLACK_US` | Legacy alias for initial bound if `DYNA_INITIAL_BOUND_US` unset. |
| `DYNA_MAX_OWD_US` | Clamp for OWD and bound (μs); default `500000`. |
| `DYNA_BOUND_RECOMPUTE_K` | Recompute aggregated bound after this many inbound **`owd_us > 0`** samples; default `4`. |
| `DYNA_OWD_WINDOW_LEN` | Replica sliding window length; default `100`. |
| `DYNA_OWD_PERCENTILE` | Percentile in `[0,100]` over sorted window; default `50`. |

### Replica threading (Nezha-style pipeline)

- **Transport thread:** `ReceiveMessage` parses client `RequestMessage`, samples OWD at **receive** (`recv_us − send_time_us`, Nezha-style), and enqueues to `incomingQu_[partition]` (single parse).
- **Commit worker (one thread per partition):** owns `PartitionState[partition]`. Drains `incomingQu_` via **`AdmitInboundClientRequest`** (Nezha-style per-key `lastReleasedByOpKey` watermark for on-time vs slow; not global `deadlineCounter` on the fast path). Runs **`ProcessDueFastReplies`** after each admitted request and in priority spins before append/sync (send `FastReply` at deadline; if admission is late, deadline is already past and replies go out immediately). **`ProcessPendingLogAppends`** uses a smaller budget while `incomingQu_` or due `earlyBuffer` entries remain (append/sync must not starve admission — open-loop bench latency is dominated by time until `FastReply`, not by `deadline_us` magnitude). `commitQu_` drain is capped per loop. Drains `commitQu_` for replication control messages.
- **Shared state:** `deadlineCounter` per partition is updated on **apply** (read visibility / commit ordering). Fast-path admission uses **`lastReleasedByOpKey`** `(deadline_us, request_id)` per KV op-key. `ReplicaOwdEstimator` is sampled on the transport thread; `owdMutex_` guards the estimator map.

### Fast path (single-shard network partition)

- Step 3: Replica buffers on-time requests in `earlyBuffer` until deadline `d`.
- Step 4: When `now >= d`, replica sends `FastReply` to the client (`ProcessDueFastReplies`).
- Step 5: After that reply, `ProcessPendingLogAppends` appends to the ordered log and runs sync/commit (budgeted so replies are not blocked by WAL/sync).
- Step 6: Partition leader sends `SyncBroadcast` with partition ID, log position, and deadline.
- Step 7:
  - Followers reconcile log position/deadline with leader broadcast.
  - Followers send `SyncAck`.
  - Partition leader commits on quorum (`f+1` acks), applies committed entries, and broadcasts `CommitBroadcast`.
  - Followers apply committed entries on `CommitBroadcast`.

### Bench client load modes (ionia + paxos `bench/client`)

- **Closed-loop (default):** each thread sends the next request only after the previous one completes (`-d` optional delay). Matches legacy SpecPaxos bench behavior.
- **Open-loop (`-O -R <rate>`):** Poisson submissions every 10ms (Nezha-style); `rate` is the mean number of requests per 10ms interval (~`rate × 100` offered ops/s per bench instance). Offered load does not slow down when the server stalls. `-n 0` means no submission cap; the measured run ends at `-e` seconds after warmup. Aggregate `-R` is split across DYNA `-j` / VR `-t` client threads.

### Client quorum behavior

- Client waits for `f+1` matching `FastReply` responses including leader reply.
- For reads, client also waits for leader `ReadResult` before completion.

### Client-side deadline field (current code vs intent)

- **Current implementation:** `DynaClient` matches `FastReply` / `ReadResult` `deadline_us` against the deadline captured at `Invoke` (`pending.deadlineUs`). That was mainly **correlation against stale or duplicated replies** (tie an inbound reply to *this* invocation), not because the client must enforce wall-clock deadline semantics.
- **Intended direction:** Completion should be keyed primarily on **`(client_id, request_id)`** (and partition consistency), with **leader participation** as the gate (see slow path below). When the partition leader **revises** the deadline on the slow path, the client should accept replies for that request under the **leader-authoritative** deadline (or drop strict deadline equality entirely once uniqueness and leader authority are guaranteed). A TODO remains in `client.cc` for slow-path deadline handling.

### Slow path — current behavior (v1)

When a request is **late** relative to fast-path rules at the partition leader, the leader **updates the deadline** (same `(client_id, request_id)`, new ordering epoch). After that, leader-side logging and replication mirror the **same structure as the fast path**, with two coordinated differences:

1. **Late sync to followers:** Followers receive a distinct `LateSyncBroadcast` carrying old/new deadline and resend a client-visible `FastReply` on the **new** deadline epoch.
2. **Leader reply to client:** The leader appends with revised deadline and sends a `FastReply` with the revised deadline to the client.

**Why followers talk to the client again:** Early follower `FastReply` waves may exist without a leader reply; the client **requires leader ack**, so those waves alone never complete. After the leader adopts the new deadline and drives late sync, followers must **echo** completion toward the client so the client can assemble `f+1` **including** the leader under the revised epoch.

**Current client behavior for epochs:** Client tracks an active deadline epoch per request. If the partition leader replies with a different deadline, client switches to that epoch and restarts quorum counting for the request.

**Correctness hinges:** Only the partition leader (for the current **view**) may originate deadline revision and late sync; **view management** remains an open concern until fully specified and implemented.

## Message Types Currently Used

- `RequestMessage`
- `FastReply`
- `ReadResult`
- `SyncBroadcast`
- `SyncAck`
- `CommitBroadcast`
- `LateSyncBroadcast`

All protocol messages include `partition_id` now so one replica process can host all partition states.

## Known Gaps / Concerns

1. **Durability backend not complete**
   - Added basic per-replica/per-partition WAL append+`fsync` before apply/reclaim on leader commit and follower commit-broadcast handling.
   - WAL format is line-based (partition/log/client/request/deadline/isRead/hex(command)); path defaults to `/tmp/ionia-dyna-wal` and can be overridden by `DYNA_WAL_DIR`.
   - Still missing crash recovery/replay and WAL compaction/truncation logic.

2. **Missing fetch path**
   - Implemented basic `FetchRequest` / `FetchResponse` path for followers missing request bodies.
   - Current behavior:
     - follower requests fetch when sync arrives but body is missing.
     - replicas respond from `latBuffer`, `earlyBuffer`, or local log copy if available.
   - Remaining concern:
     - no retry/backoff yet if leader misses fetch or response is lost (fetch targets partition leader only via `SendMessageToReplica`).

3. **Partition leadership mapping assumption**
   - Current code assumes `partitionLeader(partitionId) = partitionId % n`.
   - If deployment wants explicit metadata-driven ownership, this must move to config/proxy metadata.

4. **Late path is basic**
   - Implemented leader deadline revision + `LateSyncBroadcast` + follower re-ack to client + client epoch switch on leader reply.
   - Remaining: retries/backoff for late-sync loss, explicit dedupe tables, and full view-change recovery semantics.

5. **Recovery/view change not implemented**
   - No full view-change, reconfiguration, or crash-recovery reconciliation yet.

6. **Build environment issues outside DYNA module**
   - Repository has pre-existing toolchain/dependency incompatibilities in unrelated paths that may prevent full end-to-end build/run in this environment.

7. **Correctness risk to review**
   - Current read completion is tied to committed log application in partition order. This is intended to preserve deadline ordering, but should be validated against formal read visibility conditions in the spec before benchmarking claims.
   - Added explicit read visibility fence in code: leader returns read only when all committed writes with smaller deadlines are also applied.

8. **Idempotence/reorder hardening (partial)**
   - Added basic guards to ignore stale sync/ack/commit traffic by partition/view/log progress checks.
   - Further hardening can add explicit per-message dedupe tables for high-loss/high-dup environments.

## Behavior Notes

- **`deadline_us`** on new requests is **`send_time_us + dynamic_bound`** (`ClientBoundTracker`); **`FastReply`/`ReadResult`** may carry optional **`owd_us`** for bound refresh (ignored until replicas fill their estimator window).
- Client still matches **`deadline_us`** on replies to **`pending.deadlineUs`** (correlation / slow-path TODO unchanged).
- Early follower-only `FastReply` bursts do **not** complete the client (`leaderAcked` required); duplicate-completion from those alone is not expected.
- Read results are sent by the leader through `ReadResult` (not via `FastReply` payload).
- Leader and followers both apply committed entries when commit threshold/broadcast conditions are met.
- **Leader commit step:** on `f+1` `SyncAck`s, `LeaderCommitToDiskAndBroadcast` runs **WAL `fsync` then `CommitBroadcast`** (same logical commit); **`ApplyCommittedUpTo`** updates memory and **`deadlineCounter`**. Assumes normal-operation disk success; recovery may refine failure handling later.
- **`SyncAck` today goes follower → leader only**, not to the client; slow path explicitly adds **client-visible** follower acks after late sync (see slow-path section).

## Next Suggested Work

1. Add durable commit worker (WAL append + fsync + apply).
2. Add missing-request fetch messages and flow.
3. Tighten follower log-index conflict handling.
4. Implement explicit slow-path/late request handling: leader deadline revision, `LateSync`-style broadcast, follower client-visible acks, and client acceptance of leader-authoritative deadline for `(client_id, request_id)`.
5. Add targeted DYNA-only smoke test executable to validate protocol path independent of legacy modules.


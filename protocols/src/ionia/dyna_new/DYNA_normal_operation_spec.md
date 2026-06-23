# DYNA — Normal-Operation Implementation Specification

**Scope:** This document specifies the *normal operation* of DYNA only — fast and slow write paths, fast and slow read paths, and background ordering/persistence. **Failure recovery (leader change, replica rejoin, view change) is explicitly out of scope** and is expected to follow a Viewstamped-Replication-style design in later work. Where a design choice is constrained by future recovery, it is flagged inline so it is not hard-coded in a way that blocks recovery later.

**Target language:** C++.

**Style:** Modeled on Nezha's Algorithms 1 (replica) and 2 (proxy/client) — explicit state variables, message formats, and event-driven handlers.

---

## 1. Model and Assumptions

1. **Replicas.** There are `2f + 1` replicas. The system tolerates `f` simultaneous crash failures (recovery logic deferred).
2. **Partitions / scoped leadership.** The total key space is statically partitioned into `2f + 1` disjoint ranges, one per replica. For partition `P`, exactly one replica is the **partition leader** (authoritative orderer for keys in `P`); every other replica is a **follower** for `P`. Thus every replica is simultaneously a partition leader for its own range and a follower for the other `2f` ranges. The partition map (key → partition → leader replica) is static, shared by all replicas and clients, and known to everyone. No rebalancing in v1.
3. **Keys / operations.** Single-key operations only. Three request types: **blind write**, **read-only**, **read-modify-write (RMW)**. Cross-partition / multi-key transactions are out of scope for v1. An RMW operates within a single partition's range and is ordered by that partition's leader.
4. **Clock synchronization.** All machines (replicas and clients) run Huygens. Clocks are synchronized closely enough that the deadline mechanism is effective. As in Nezha, clock sync is relied on for **performance, not correctness** — poorly synchronized clocks degrade the fast-commit ratio but never violate safety, because late requests fall through to the slow path.
5. **Transport.** Application-level multicast implemented as UDP unicast fan-out (no switch multicast in cloud). Delivery is best-effort: messages may be dropped, duplicated, or reordered.
6. **At-most-once semantics.** Every request carries a unique `⟨client-id, request-id⟩`. Replicas deduplicate on this tuple and resend the prior reply on retry without re-execution.
7. **Storage layering (per replica).** Each replica maintains two layers:
   - **Memory KV** — the authoritative committed state. A write becomes visible here when it is released from the early buffer into the log (applied).
   - **Disk KV** — durable backing that *lags* memory. Committed writes are persisted asynchronously off the critical path (background ordering, §7).
   The storage engine sits behind an abstract `StorageBackend` interface (§3.4) with two concrete implementations: an in-memory map (optionally with simulated write latency) for bring-up/correctness, and a SplinterDB-backed implementation for disk-based measurements. All compared baselines should run over the same backend for apples-to-apples results.

### 1.1 Correctness scope assumption (per-request commit)

The client commit rule (§5) checks agreement on a single `⟨request-id, deadline⟩` rather than on a hash of the entire log prefix (Nezha's set-equality check). **Per-request agreement is sound only under the normal-operation assumption that the committed log prefix is not reordered across the quorum.** This holds in normal operation because (a) on-time requests are released in deadline order and (b) the synced/unsynced log split (§3.3) prevents a follower from treating a speculatively-ordered entry as leader-confirmed. This assumption is stated here explicitly; enforcing it across failures (e.g., via a prefix hash and a super-quorum) is deferred to recovery work.

---

## 2. Deadlines

Every request carries a deadline in synchronized global time:

```
deadline = curr + s + d
```

- `curr` — the client's current synchronized clock time at send.
- `s`    — the clock-synchronization error bound (from Huygens' σ terms, sender + receiver standard deviations). Small under good sync.
- `d`    — the estimated one-way delay (OWD) to the replicas.

**`d` is the maximum estimated OWD across all `2f + 1` replicas**, so that the deadline is one by which the request is expected to have arrived at *every* replica.

**Learning `d` (piggyback feedback loop, Nezha-style).**
- Each replica maintains, per client (or per partition-leader sender), a **sliding window** of OWD samples. A sample is computed on receipt as `receive_time − send_time` (the request carries its send time).
- The replica reports a **percentile** of its window (default **50th percentile**) back to the client by piggybacking on replies.
- The client sets `d = max` over the most recent per-replica reported OWDs.
- **Clamping:** if an estimated OWD falls outside a predefined range `[0, D_max]` (e.g., from a transient clock-sync glitch producing negative or absurd values), the replica substitutes `D_max`. `D_max` is a configuration parameter.

> The 50th percentile is the starting default (balances reordering rate against early-buffer holding delay). It is a tunable parameter, not a constant.

---

## 3. Per-Replica State

All ordering state below is **per partition**. A replica holds one instance of this state block for *each* of the `2f + 1` partitions: one for the partition it leads, and one for each partition it follows. Operations are routed to the correct partition's state block by key.

### 3.1 Identity / configuration
- `replica_id` — unique id in `[0, 2f]`.
- `f` — fault tolerance parameter.
- `partition_map` — static key → partition → leader-replica mapping.
- `view_id` — per partition; initialized 0. Used by the client/endorsement logic to confirm replies are from the same view. (Incremented only by recovery, which is out of scope — but the field exists and is carried in every reply so the normal-operation matching logic is recovery-ready.)

### 3.2 Buffers and counter (per partition)
- `early_buffer` — a **priority queue** ordered by request deadline. Holds requests that arrived "on time" (deadline strictly greater than `deadline_counter`). Releases entries when their deadline is reached.
- `late_buffer` — a **map** indexed by `⟨client-id, request-id⟩`. Holds requests that arrived "late" (deadline ≤ `deadline_counter`) and are therefore ineligible for the early buffer as-is.
- `deadline_counter` — the deadline of the **last request released** from the early buffer for this partition. Monotonically non-decreasing. An incoming request is "on time" iff its deadline `>` `deadline_counter`.

### 3.3 Logs (per partition) — synced / unsynced split

Following Nezha, each follower's per-partition log is split so that speculatively-ordered entries can be safely re-ordered before the leader confirms them:

- `synced_log` — entries confirmed consistent with the partition leader (immutable once synced). For the **partition leader**, all its appended entries go directly here (the leader *is* the source of truth).
- `unsynced_log` — entries a **follower** has speculatively released from its early buffer but not yet reconciled against a leader sync message. Entries here are mutable: their deadline may be rewritten, or they may be moved/removed, when the leader's sync message arrives.
- `sync_point` — the log position up to which this replica's log is confirmed equal to the leader's. The leader advances its own `sync_point` immediately on append.
- `commit_point` — log entries up to here are committed/stable (optional optimization; used to bound background persistence and, later, recovery). 

> Rationale for the split (normal operation): an on-time follower releases a request into its log at the *original* deadline and advances its `deadline_counter` before hearing from the leader. If the leader was late for that same request and reassigns a larger deadline, the follower must re-order an entry already below its counter. Keeping such speculative entries in `unsynced_log` makes that re-ordering well-defined.

### 3.4 Storage interface
```
interface StorageBackend {
    void   mem_apply(key, value);     // apply committed write to authoritative memory KV
    value  mem_read(key);             // memory read (endorsement / fast read)
    void   disk_persist(entry);       // async durable write (background)
    value  disk_read(key);            // durable read (leader, read path)
}
```
Implementations: `InMemoryBackend` (optional simulated latency), `SplinterDBBackend`.

---

## 4. Message Formats

> Leader-change messages (VR-style) are intentionally omitted — recovery is out of scope.

- **request** — client → all replicas:
  `request = ⟨client_id, request_id, type, key, command, send_time, deadline⟩`
  `type ∈ {BLIND_WRITE, READ_ONLY, RMW}`. `command` carries the value for writes. `send_time` and `deadline` are set by the client (§2).

- **reply** — replica → client (used for both writes and read endorsements):
  `reply = ⟨view_id, replica_id, partition_id, client_id, request_id, deadline, is_leader, result, owd_estimate⟩`
  `result` is populated only in the partition leader's reply for read-only / RMW (the value); it is null in follower replies and in blind-write replies. `is_leader` lets the client identify the authoritative reply. `owd_estimate` piggybacks the replica's percentile OWD for this sender (§2).

- **sync** — partition leader → followers (per appended request):
  `sync = ⟨view_id, partition_id, log_id, client_id, request_id, deadline⟩`
  Conveys the leader's authoritative log position and (possibly reassigned) deadline so followers reconcile their logs. Doubles as the leader's heartbeat. May be batched under high throughput.

- **sync_confirm** — follower → partition leader (per synced request):
  `sync_confirm = ⟨view_id, replica_id, partition_id, client_id, request_id, deadline⟩`
  Reports that the follower's log now matches the leader for this entry.

- **commit** — partition leader → followers (background):
  `commit = ⟨view_id, partition_id, client_id, request_id⟩`
  Signals that the request is client-committed and may be migrated to disk.

- **fetch / fetch_resp** — follower ⇄ leader (sync miss):
  Requests/returns the full request body when a follower received a `sync` for a request it never saw (dropped/delayed). `fetch_resp` carries the full `request`.

---

## 5. Client Algorithm

```
Client state:
    pending      : map ⟨request_id⟩ → { type, key, leader_id, replies_by_replica, committed }
    owd_table    : map replica_id → latest reported owd_estimate
    s            : current clock-sync error bound (from Huygens)

upon submit(type, key, command):
    rid       ← fresh request_id
    pid       ← partition_of(key)            // via static partition_map
    leader_id ← leader_of(pid)
    d         ← max over replicas of owd_table[r]   // default/clamp if unknown
    now       ← clockTime()
    deadline  ← now + s + d
    req ← ⟨client_id, rid, type, key, command, now, deadline⟩
    pending[rid] ← { type, key, leader_id, replies_by_replica:∅, committed:false }
    multicast req to all 2f+1 replicas
    start timer(rid)

upon receive reply r:
    if r.request_id ∉ pending or pending[r.request_id].committed: return
    P ← pending[r.request_id]
    // later reply from a replica supersedes its earlier reply (corrected deadline)
    P.replies_by_replica[r.replica_id] ← r
    owd_table[r.replica_id] ← r.owd_estimate     // refresh OWD feedback
    checkCommit(r.request_id)

function checkCommit(rid):
    P ← pending[rid]
    leaderReply ← P.replies_by_replica[ P.leader_id ]
    if leaderReply = null: return                 // leader's reply is mandatory
    if P.type = READ_ONLY:
        // need leader's data reply + f endorsements agreeing on view & leader's log position
        endorsers ← { r ∈ replies : r.replica_id ≠ leader_id
                                    ∧ r.view_id  = leaderReply.view_id
                                    ∧ r.log_pos  = leaderReply.log_pos }   // see §6.3
        if |endorsers| ≥ f:
            deliver(leaderReply.result); P.committed ← true
    else: // BLIND_WRITE or RMW
        D ← leaderReply.deadline                  // authoritative deadline
        matching ← { r ∈ replies : r.view_id = leaderReply.view_id
                                  ∧ r.deadline = D }   // includes leader
        if |matching| ≥ f + 1:
            if P.type = RMW: deliver(leaderReply.result)
            else:            deliver(ack)
            P.committed ← true

upon timeout(rid):
    // packet drop or (later) leader failure; recovery deferred
    resubmit req with a fresh send_time and recomputed deadline
```

**Commit rule (normal operation).** A write commits when the client holds `f + 1` replies that include the partition leader's reply and agree on the leader's deadline and view. A read commits when the client holds the leader's data reply plus `f` endorsements agreeing on the leader's view and broadcast log position. Commit does **not** depend on disk persistence.

> The `f + 1` write quorum is correct for failure-free normal operation. It is exposed as a **parameter** (`write_quorum`) rather than hard-coded, because recovery may require enlarging it (the Fast-Paxos / Nezha super-quorum `f + ⌈f/2⌉ + 1` argument). Out of scope here; flagged so it is not welded in.

---

## 6. Replica Algorithm

### 6.1 Write path — fast (request on time at the partition leader)

```
upon receive request req where I am partition leader for key(req):     // FAST
    if duplicate(req): resend prior reply; return
    record OWD sample for req.client_id
    if req.deadline > deadline_counter[pid]:        // on time → early buffer
        early_buffer[pid].insert(req)               // priority queue by deadline
    else:
        handle as SLOW path (§6.2)

upon early_buffer[pid].top().deadline ≤ clockTime():   // release at deadline
    req ← early_buffer[pid].pop()
    deadline_counter[pid] ← req.deadline               // advance counter
    append req to synced_log[pid]; advance sync_point
    if req is write:  mem_apply(req.key, req.value)     // authoritative memory state
    if req is RMW:    result ← execute(req)             // leader executes
    send reply ⟨view_id, replica_id, pid, client_id, request_id, req.deadline,
                is_leader=true, result, owd_estimate⟩ to client
    broadcast sync ⟨view_id, pid, log_id, client_id, request_id, req.deadline⟩ to followers
```

### 6.2 Write path — slow (request late at the partition leader)

```
upon receive request req where I am leader and req.deadline ≤ deadline_counter[pid]:  // SLOW
    late_buffer[pid].insert(req)
    newDdl ← max(clockTime(), deadline_counter[pid] + 1)   // make eligible
    req.deadline ← newDdl
    move req from late_buffer[pid] into early_buffer[pid]
    // released, appended, replied, and synced exactly as in §6.1 release handler
```

The released request is appended to `synced_log`, the (reassigned) deadline is what the leader replies to the client and broadcasts in `sync`. Because the deadline changed, the client's authoritative deadline is the reassigned one — followers will correct their replies to match (§6.4).

### 6.3 Read path

**Fast (read on time at the leader).**
```
upon receive read req where I am leader, on time:
    early_buffer[pid].insert(req)        // ordered with writes by deadline
upon release of read req at its deadline:
    deadline_counter[pid] ← req.deadline
    // serve from authoritative memory state; flush ordering, not durability, is what matters
    // (apply any not-yet-applied writes ordered before this read, then read memory)
    result ← mem_read(req.key)
    send reply (is_leader=true, result, log_pos = position of req) to client
    broadcast sync / log-position for this read to followers

upon receive read req where I am follower, on time:
    early_buffer[pid].insert(req)
upon release at deadline (follower):
    acknowledge: send reply (is_leader=false, result=null,
                             view_id, log_pos = position of req) to client   // endorsement
```

> Note on read-fast step 3: the leader must ensure all writes *ordered before* this read are **applied to memory** before serving it (so the read is linearizable w.r.t. committed writes). Per your confirmation, this is a memory-apply ordering requirement, **not** a synchronous disk flush — disk persistence remains in the background.

**Slow (read late at the leader).** Identical structure to the slow write path: leader puts the read in `late_buffer`, reassigns its deadline into the early buffer, then on release serves from memory and broadcasts its log position; followers reconcile and endorse as in §6.4.

### 6.4 Follower reconciliation (sync handling) — fast and slow

```
upon receive sync ⟨view_id, pid, log_id, client_id, request_id, deadline⟩ as follower:
    locate entry e for ⟨client_id, request_id⟩ in {unsynced_log, early_buffer, late_buffer}
    case found, same ⟨client_id, request_id⟩ at log_id with same deadline:
        // already consistent — promote to synced_log if needed
        promote e into synced_log[pid] at log_id
    case found, but deadline differs:
        e.deadline ← deadline                 // adopt leader's deadline
        reposition e to log_id in synced_log[pid]
        if deadline changed the request's client-visible ordering:
            send corrected reply to client with the leader's deadline
            possibly advance deadline_counter[pid]
    case not found (dropped/delayed):
        fetch full request body from leader (fetch / fetch_resp)
        insert at log_id in synced_log[pid] with leader's deadline
    after reconciling:
        update sync_point
        send sync_confirm ⟨view_id, replica_id, pid, client_id, request_id, deadline⟩ to leader
```

Key rule (from your spec): a follower that had to **change** a request's deadline sends a *second, corrected* reply to the client carrying the leader's deadline. The client treats this later reply as superseding the follower's earlier one (§5). A follower's *first* (speculative) reply can never by itself produce a commit, because the client's commit rule requires the leader's reply and agreement on the leader's deadline.

---

## 7. Background Ordering / Persistence

Runs off the critical path; does not affect client-visible commit latency.

```
Partition leader, per request:
    collect sync_confirm messages from followers
    upon f+1 confirmations (including leader's own append) with matching deadline for a request:
        // request is client-committed → safe to persist
        disk_persist(entry)                       // migrate memory → disk
        broadcast commit ⟨view_id, pid, client_id, request_id⟩ to followers

Follower:
    upon receive commit for ⟨client_id, request_id⟩:
        disk_persist(entry)                       // migrate its own copy memory → disk
```

Disk writes may be **batched** for throughput. The `commit_point` (§3.3) can bound how far disk persistence has progressed and later accelerate recovery.

---

## 8. Out of Scope (deferred to recovery work)

- Leader change / view change (VR-style), replica rejoin, log recovery.
- Enlarged fast-path quorum (`f + ⌈f/2⌉ + 1`) and prefix-hash (set-equality) checks needed to make commits safe across leader changes.
- Reconfiguration / changing `f`.
- Cross-partition or multi-key transactions.
- Persisting view metadata (`view_id`, crash vectors) for crash recovery.

These do not affect the normal-operation benchmarking goal (latency-throughput vs. VR and IONIA), but several normal-operation fields (`view_id`, `sync_point`, `commit_point`, the `write_quorum` parameter, the synced/unsynced split) are present specifically so recovery can be added without reworking the data path.

---

## 9. Implementation Notes for C++

- **Per-partition state** → one struct instance per partition; index by `partition_id`. A replica owns `2f + 1` such structs.
- **early_buffer** → `std::priority_queue` (min-heap on deadline) or an ordered structure allowing peek-at-min; ties broken by `⟨client_id, request_id⟩` for determinism across replicas.
- **late_buffer** → `std::unordered_map` keyed by `⟨client_id, request_id⟩`.
- **Release timing** → a per-partition timer/poll loop releasing all entries whose deadline ≤ now; one dispatcher thread per partition is a natural unit for the multi-threading optimization noted in your ongoing work.
- **Dedup table** → `std::unordered_map ⟨client_id, request_id⟩ → cached_reply`.
- **Transport** → UDP; one fan-out helper for multicast. Expect and tolerate reorder/dup/loss.
- **StorageBackend** → abstract base; `InMemoryBackend` first (with optional `simulated_write_latency`), `SplinterDBBackend` second, identical interface so all baselines share a backend.
- **Determinism for matching** → all replicas must order the early buffer identically (deadline, then `⟨client_id, request_id⟩`) so that per-request deadline agreement across the quorum is meaningful.

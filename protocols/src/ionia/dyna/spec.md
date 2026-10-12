# DYNA Implementation Spec — Sections 1 & 2

> **Purpose.** This is the *implementation* companion to the DYNA design/algorithm doc.
> The algorithm doc explains *why*; this doc pins down *exactly what an implementer must
> build* so that no operational detail is left to guesswork. Sections 1–2 define the data
> model and every message. Sections 3+ (handlers, client state machine, invariants,
> deferred features) reference the names defined here.
>
> **How to use with Claude Code.** Implement in the build order at the end of this file.
> Each numbered structure/message maps to one type; each handler pointer (`§3.x`) maps to
> one function. Do not implement anything under "Deferred" (§0.4).

---

## 0. Conventions & Parameters

### 0.1 Fault model & sizing
- `f` — the number of tolerated crash failures.
- `N = 2f + 1` — total number of servers.
- The key range is split into `N` partitions, one per server.

### 0.2 Topology (**shared-replica model — this is a load-bearing assumption**)
There is **one** shared pool of `N` servers. **Every server stores the data for every
partition.** Server `i` is the *Partition Leader* for partition `i` and a *follower* for the
other `2f` partitions. A single reply from server `S` therefore attests to `S`'s view of
**all** partitions it holds (i.e. all of them).

> This is not a stylistic choice. DYNA's real-time-ordering guarantee rests on
> *commit-quorum intersection across transactions on different partitions*, which only
> holds when all partitions draw from the same replica pool. A disjoint-shard topology
> (each partition its own `2f+1` group) would change every quorum count to `(f+1)·m` and
> would forfeit the intersection argument. Do not implement disjoint shards.

### 0.3 Decision points (resolve before/while implementing)
Both are marked inline where they bite. Defaults chosen; alternatives noted.

- **DECISION A — commit-quorum shape.** *Default:* `f+1` replies **total**, which must
  **include the Partition Leader of every involved partition**, all agreeing on
  `(viewId, timestamp)`. (This equals `max(f+1, m)` when the `m` involved leaders don't
  fit inside `f+1`.) *Alternative (rejected under §0.2):* `f+1` per involved partition.
  See §1.6.
- **DECISION B — Invariant-2 enforcement.** *Default (matches current algorithm doc):*
  wall-clock late-detection only — a server treats a txn as late iff its local clock has
  already passed `T.t`. This makes strict serializability **conditional on clock skew**
  staying within the deadline slack. *Recommended strengthening (unconditional Inv 2):*
  additionally refuse to release/endorse any txn whose `T.t ≤ highestReleasedT`
  (the counter-based veto), even if the wall clock says on-time. The data model below
  carries `highestReleasedT` so either choice is buildable without schema changes. See
  §1.4 and the note in §2 (SpecReply).

### 0.4 Deferred (explicitly out of scope for v1 — leave as stubs)
- Read-Modify-Write (RMW) path.
- Failure recovery / view change (Viewstamped-Replication-style). `viewId` is carried
  everywhere so recovery can be added without reshaping messages, but the recovery logic
  itself is not implemented in v1.
- "Thrifty" multicast (send to `f+1` first, more on timeout).
- Network-partition handling.

---

## 1. Data Model

### 1.1 Primitive types & the ordering key

```text
ServerId    = uint32          // 0 .. N-1
PartitionId = uint32          // 0 .. N-1   (normal operation: partition i led by server i)
ClientId    = uint32
RequestId   = uint64          // unique per (client) — a client's request counter
Key         = bytes | uint64  // opaque; implementer's choice
Value       = bytes
ViewId      = uint64          // current view; bumped on view change (recovery, deferred)

TxnId = (ClientId, RequestId) // globally unique identifier of a transaction

Timestamp   = uint64          // physical time (synchronized clock, e.g. Huygens/PTP), ns
```

**OrderKey — the total order everything sorts by.** Priority queues, the log, and the
on-time/late comparisons all order by:

```text
OrderKey = (Timestamp physical, ClientId, RequestId)   // compared lexicographically
```

- **Globally unique** (a client's `RequestId`s are unique), and **totally ordered** and
  **identical on every server**. This uniqueness is what makes the total-order proof
  (Invariant 1) go through — it is the tiebreaker that forbids two distinct transactions
  from occupying the same log position.
- Re-timestamping (§2 SpecReply / late-leader, and Slow-Commit) changes only the
  `Timestamp physical` component; `(ClientId, RequestId)` are preserved, so uniqueness
  survives a bump, and a max over per-leader bumps yields exactly one OrderKey.

```text
Status = enum { SPECULATIVE, COMMITTING, COMMITTED }
TxnType = enum { WRITE, READ }          // RMW deferred (§0.4)
```

### 1.2 Transaction — wire form (client → servers)

The immutable-ish payload a client multicasts. `t` is assigned by the client and may be
*advanced* later (never lowered).

```text
struct Txn {
    txnId:   TxnId
    type:    TxnType
    rSet:    Set<Key>            // keys read       (empty for pure WRITE)
    wSet:    Map<Key, Value>     // keys+values written (empty for pure READ)
    t:       Timestamp           // client-assigned deadline (physical component of OrderKey)
    viewId:  ViewId              // view the client believes current
}
```

> `orderKey(Txn) = (t, txnId.ClientId, txnId.RequestId)`. Servers compute this locally;
> it is not sent as a separate field.

### 1.3 TxnEntry — server-side stored form

What a server keeps in its buffers / log for a transaction. The entry's `t` is the
**authoritative, live** timestamp (may differ from the original wire `t` after a bump).

```text
struct TxnEntry {
    txn:            Txn              // the body (may be fetched later if server lacked it)
    t:              Timestamp        // LIVE timestamp (bumped on re-timestamp/commit); orders the entry
    status:         Status
    isLeaderForThis: bool           // does this server lead any key in txn.wSet ∪ txn.rSet?
    ledKeys:        Set<Key>         // subset of the txn's keys this server is leader for
    results:        Map<Key, Value>  // READ only, leader only: values produced by disk-read

    // Leader-only bookkeeping:
    tsProposals:    Map<ServerId, Timestamp>  // collected during timestamp-agreement (§2 TsAgree)
    prepareReplies: Set<ServerId>             // who has ack'd Replicate-Prepare (§2)
}
```

- `orderKey(entry) = (entry.t, entry.txn.txnId.ClientId, entry.txn.txnId.RequestId)`.
- Entries live in exactly one of `earlyBuffer`, `lateBuffer`, or `specLog` at a time
  (they move between them; see §3 handlers).

### 1.4 Per-server state

```text
struct Server {
    serverId:  ServerId
    viewId:    ViewId
    clock():   Timestamp                 // reads the synchronized physical clock (now)

    // --- Configuration (static in v1; changes only on view change, deferred) ---
    leaderOf:  (PartitionId) -> ServerId // normal op: identity (partition i -> server i)
    partitionOf: (Key) -> PartitionId    // key -> partition mapping (deterministic, known to all)

    // --- Ordering / buffers ---
    earlyBuffer: IndexedMinPQ<TxnId, TxnEntry>  // ordered by orderKey; on-time arrivals
    lateBuffer:  HashMap<TxnId, TxnEntry>       // late arrivals (followers)
    specLog:     IndexedMinPQ<TxnId, TxnEntry>  // ordered by orderKey; released/committed entries
                                                // MUST support update-key (re-timestamp re-heapifies)

    // --- The (implicit) deadline counter ---
    highestReleasedT: OrderKey                  // max orderKey ever appended to specLog.
                                                // Monotonic non-decreasing. NEVER lowered
                                                // (not by re-timestamp, not by reconciliation).
                                                // Used by the re-timestamp rule (§3) and,
                                                // under DECISION B, by the counter veto.

    // --- Retry idempotence ---
    replyCache: HashMap<TxnId, CachedReply>     // last reply sent for a txnId, for resends
                                                // GC'd by Tthresh (see below)
}

struct CachedReply {
    reply: SpecReply | RejectReply | CommitReply
    t:     Timestamp        // for GC: if reply.t + Tthresh < clock(), entry may be dropped
}
```

**`highestReleasedT` invariant (both DECISION B choices depend on this existing):**
it is updated to `max(highestReleasedT, orderKey(entry))` **whenever** an entry is appended
to `specLog` — whether via early-buffer release, late-leader bump, fetch, or commit-message
insertion. It is never decreased. Because release happens only when `clock() ≥ entry.t`,
under synced clocks `highestReleasedT.physical ≤ clock()` normally holds — but a
commit/agreement that inserts a high-`t` entry can push it *ahead* of the local wall clock,
which is exactly the case where the DECISION-B counter veto is stronger than wall-clock
alone.

**`replyCache` GC (`Tthresh`).** A cached reply whose `t + Tthresh < clock()` is considered
too old and may be deleted; a later retry for it is then ignored. This affects **liveness,
not safety**.

### 1.5 Per-client request state

One instance per outstanding transaction the client is driving. Drives the client state
machine (states/transitions in §4).

```text
struct ClientReq {
    txn:        Txn
    phase:      enum { MULTICASTING, COLLECTING, SLOW_WAIT, DONE, FAILED }
    currentT:   Timestamp               // the timestamp of the attempt in flight (grows on retry)

    replies:      Map<ServerId, SpecReply>    // SpecReplies collected for currentT
    commitReplies: Map<ServerId, CommitReply> // Commit-Replies (slow path quorum re-check)
    rejects:      Set<ServerId>               // RejectReplies (read path -> triggers retry)

    // Retry control:
    retryCount:  uint32
    owdEstimate: Timestamp              // one-way-delay estimate; grows each retry (read path)
    timers:      { collectDeadline, ... }

    // Cached derived sets (computed from txn + config):
    involvedPartitions: Set<PartitionId>      // partitions touched by rSet ∪ wSet
    involvedLeaders:    Set<ServerId>         // { leaderOf(p) : p in involvedPartitions }
}
```

### 1.6 Predicates

**`involvedPartitions(txn)`** = `{ partitionOf(k) : k ∈ txn.rSet ∪ keys(txn.wSet) }`.
**`involvedLeaders(txn)`** = `{ leaderOf(p) : p ∈ involvedPartitions(txn) }`.

**Commit-quorum predicate `commitReady(replies)` — DECISION A.**

*Default (shared-pool):*
```text
commitReady(replies) :=
    (∀ leader ∈ involvedLeaders : leader ∈ keys(replies))        // (1) every involved leader replied
    AND |replies| ≥ f + 1                                        // (2) at least f+1 total
    AND all replies in `replies` share the same (viewId, t)      // (3) consistent view & timestamp
```
Equivalently: a set of `≥ max(f+1, m)` matching replies that contains all `m` involved
leaders. The `f+1` gives fault-tolerant durability + (under DECISION B) the intersection
veto; the "all leaders" clause gives Invariant 1's single-position property.

*Alternative (per-partition, rejected under §0.2):* for each `p ∈ involvedPartitions`,
`≥ f+1` replies attributable to partition `p` including `leaderOf(p)`. **Do not use unless
the topology decision in §0.2 is reversed.**

> **Reads** additionally require the read `results` to be taken from the involved leaders'
> replies (only leaders disk-read; followers endorse). See §2 SpecReply and §3 read handler.

---

## 2. Message Catalog

All messages carry `txnId` and `viewId` unless noted. "Handler `§3.x`" is a forward
reference to the handler section (to be written next). "Keyed on" = the field(s) the
receiver uses to locate/dedup the relevant `TxnEntry` / `ClientReq`.

| # | Message | Direction | Trigger | Handler | Keyed on |
|---|---------|-----------|---------|---------|----------|
| 1 | `ClientRequest` | Client → all servers | client multicast / retry | onClientRequest (§3.1) | txnId |
| 2 | `SpecReply` | Server → Client | server releases txn from early buffer (leader disk-reads first for READs) | client onSpecReply (§4) | txnId, serverId |
| 3 | `RejectReply` | Server → Client | READ arrives late at a **follower** | client onRejectReply (§4) | txnId, serverId |
| 4 | `FastCommit` | Client → all servers | client `commitReady` on fast path | onFastCommit (§3.2) | txnId |
| 5 | `SlowCommit` | Client → all servers | client got all leaders (1) but timestamps disagree | onSlowCommit (§3.3) | txnId |
| 6 | `CommitReply` | Server → Client | server processed a `SlowCommit` | client onCommitReply (§4) | txnId, serverId |
| 7 | `FetchRequest` | Server → Server | commit msg arrived for a txn the server lacks | onFetchRequest (§3.4) | txnId |
| 8 | `FetchReply` | Server → Server | answer to `FetchRequest` | onFetchReply (§3.4) | txnId |
| 9 | `TsAgree` | Leader → other involved leaders | background: txn stuck `SPECULATIVE` past timeout | onTsAgree (§3.5) | txnId, senderId |
| 10 | `ReplicatePrepare` | Leader → all other servers | txn is `COMMITTING`, leader seeks durability | onReplicatePrepare (§3.6) | txnId |
| 11 | `ReplicatePrepareReply` | Server → Leader | ack of `ReplicatePrepare` | leader onReplicatePrepareReply (§3.6) | txnId, serverId |
| 12 | `ReplicateCommit` | Leader → all other servers | leader collected `f` prepare-replies | onReplicateCommit (§3.7) | txnId |

### 2.1 Field detail

Only fields beyond the implicit `txnId` + `viewId` are listed; the implicit two are always
present.

```text
// 1. ClientRequest — carries the whole transaction.
struct ClientRequest {
    txn: Txn                     // txnId, type, rSet, wSet, t, viewId  (see §1.2)
}

// 2. SpecReply — the speculative reply after a server releases the txn.
struct SpecReply {
    serverId: ServerId
    t:        Timestamp          // the LIVE timestamp this server assigned/released at.
                                 //   - on-time server: == txn.t (original)
                                 //   - late LEADER (write): its bumped timestamp
    isLeader: bool               // convenience; client can also derive via config.
                                 //   true iff this server leads a key in the txn.
    results:  Map<Key, Value>    // READ only, and only when isLeader: the disk-read values.
                                 //   empty for writes and for follower read-endorsements.
}
// NOTE (DECISION B): a WRITE that arrives late at a FOLLOWER produces NO SpecReply
// (the follower silently buffers it in lateBuffer). That silence is the default
// (wall-clock) non-endorsement. Under the strengthened veto, a server also withholds
// the SpecReply if txn.t <= highestReleasedT even when the wall clock says on-time.

// 3. RejectReply — READ late at a follower; tells client to retry at a larger t.
struct RejectReply {
    serverId: ServerId
}

// 4. FastCommit — client-driven commit after fast-path quorum.
struct FastCommit {
    agreedT: Timestamp           // the commonly-agreed timestamp (all leaders matched this)
}

// 5. SlowCommit — client-driven commit when leaders disagreed; carries the resolved max.
struct SlowCommit {
    agreedT: Timestamp           // max over the involved leaders' proposed timestamps
}

// 6. CommitReply — server's response to SlowCommit (client re-checks quorum at agreedT).
struct CommitReply {
    serverId: ServerId
    t:        Timestamp          // == agreedT after the server adopted it
    isLeader: bool
}

// 7. FetchRequest — pull a missing txn body from a peer.
struct FetchRequest {
    requesterId: ServerId
}

// 8. FetchReply — the requested txn body.
struct FetchReply {
    txn: Txn                     // full body so the requester can materialize a TxnEntry
}

// 9. TsAgree — background timestamp agreement among involved leaders.
struct TsAgree {
    senderId:  ServerId
    proposedT: Timestamp         // this leader's current live t for the txn
}
// Each involved leader sends TsAgree(its t) to every OTHER involved leader, collects all
// of them into TxnEntry.tsProposals, then sets t := max(all proposals incl. its own),
// status := COMMITTING. (See §3.5.)

// 10. ReplicatePrepare — leader drives durability of the agreed order.
struct ReplicatePrepare {
    leaderId: ServerId
    agreedT:  Timestamp
    txn:      Txn?               // OPTIONAL body: include so a follower lacking the txn can
                                 //   materialize it without a separate fetch.
}

// 11. ReplicatePrepareReply — follower acks it holds the txn at agreedT.
struct ReplicatePrepareReply {
    serverId: ServerId
}

// 12. ReplicateCommit — leader tells everyone the txn is durably committed.
struct ReplicateCommit {
    leaderId: ServerId
}
```

### 2.2 Notes on message semantics an implementer must not miss

- **`SpecReply.t` may differ from `ClientRequest.txn.t`.** The client's `commitReady`
  check (§1.6) compares the `t` in the *replies*, not the original. Fast path = all counted
  replies (incl. all leaders) carry the **same** `t`. Different leader `t`s ⇒ slow path.
- **Late writes at followers are silent** (default). The client reaching `commitReady`
  therefore depends on enough *on-time* servers, which — under synced clocks — is the
  implicit real-time-ordering guard (DECISION B).
- **Commit messages (4, 5) trigger fetch (7/8) on a server that lacks the txn.** Order:
  check `specLog` → `lateBuffer` → else `FetchRequest`. Only after the body is present is
  the commit applied. A `SlowCommit` additionally elicits a `CommitReply` (6) because the
  client is still waiting on a quorum at `agreedT`; a `FastCommit` does **not** (client
  already committed).
- **`highestReleasedT` is bumped on every append to `specLog`**, including via commit
  messages that carry a higher `agreedT` than the server has seen (§1.4).

---

## Build order (for step-by-step implementation)

Each step is a testable milestone; earlier steps are prerequisites.

1. **Types only.** §1.1–1.3 structs + `OrderKey` ordering + the 12 message structs (§2).
   Compiles, no behavior. Unit-test `OrderKey` comparison + uniqueness.
2. **Single server, single partition, fast path.** `earlyBuffer`, release loop
   (clock passes `t` → append `specLog`, bump `highestReleasedT`, emit `SpecReply`).
   Test: one write releases at its deadline and replies.
3. **Client fast-path commit (single partition).** Multicast, collect, `commitReady`,
   `FastCommit`. Test: end-to-end single-partition write commits.
4. **Late arrival + slow path (single partition).** `lateBuffer`, late-leader re-timestamp
   (`t := clock()`, guaranteed `> highestReleasedT`), `SlowCommit`/`CommitReply`, client
   `SLOW_WAIT` quorum. Test: a late write resolves.
5. **Multi-partition fast path.** `commitReady` across partitions incl. all involved
   leaders (DECISION A). Test: two-partition write commits at one timestamp.
6. **Background ts-agreement (`SPECULATIVE → COMMITTING`).** `TsAgree` exchange, max.
   Test: two-partition write, late at one leader, resolves to the max.
7. **Replication (`COMMITTING → COMMITTED`).** `ReplicatePrepare/Reply/Commit`.
   Test: status reaches `COMMITTED` after `f` prepare-replies.
8. **Dependency-gated batched disk execution.** Execute only when the txn and all
   conflicting smaller-`t` dependents are `COMMITTED`. Test: conflicting txns apply in
   dependency order.
9. **Read path.** Fast path (leader disk-reads after dependents committed+applied;
   followers endorse), then late-follower `RejectReply` + client retry with larger `t`.
   Test: consistent multi-partition read; late read retries and eventually succeeds.

> After step 9 the write + read paths are complete. RMW, recovery/view-change, thrifty
> multicast, and partition handling (§0.4) come afterward and are out of scope here.
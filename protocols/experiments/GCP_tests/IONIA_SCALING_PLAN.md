# IONIA Scaling Plan — Why latency grows with load, and the top-2 fixes

Status: **proposal for review** (not yet implemented). Do not start coding until this is reviewed/tweaked.

## Question this answers

Why does IONIA median latency double/triple/quadruple as offered throughput
rises, even though the network is fine (ping-pong sustains ~9k ops/s at ~60 µs)?
The ~14 µs idle overhead is *not* the issue. The issue is **queueing**, and the
code shows exactly where it comes from.

---

## Root cause (the smoking gun): the "15 durability threads" are effectively one thread

Each replica spawns `-j` (default 15) durability threads. Each builds its own
`FastTransport` with a distinct endpoint id (0..14) and calls `Run()`:

- `bench/replica.cc:352-362` — `server_thread_func`: `transport_id = thread_id`,
  one `VRDurabilityReplica` per thread, then `client_transport.Run()`.

All 15 threads register on the **same** client `host:port`, so
`SharedUDPPort::Acquire` returns **one shared socket + one libevent base**
(refcounted singleton keyed by host:port). Only endpoint 0 ever pumps it:

- `lib/udptransport.cc:645-669` — `Run()` / `RunOnce()`:
  `if (endpoint_id_ == 0) shared_port_->RunOnce();` Everyone else's pump is a no-op.
- `lib/shared_udp_port.cc:264-331` — `RunOnce()` → `OnReadable()` →
  `ProcessPacket()` → `DeliverMessage()` → `receiver->ReceiveMessage()` runs
  **synchronously on the calling thread**.

Therefore:

- **Thread 0** does *all* inbound processing for *all 15 endpoints*: protobuf
  parse, `HandleRequest`, the entire `AppUpcall` write path, and the
  write/fast-read reply send.
- **Threads 1–14** never receive anything. Their loop body
  `udp_transport->RunOnce()` is a no-op; they only run the `ExecutionCallback`
  (drain `executionQueues[i]` for ordered reads / committed execution) and
  otherwise **busy-spin a full core** (`udptransport.cc:649-653`), stealing
  memory bandwidth and cache from thread 0.

The Phase-2 key→endpoint routing we added (`lib/fasttransport.cc:185-203`,
`lib/fasttransport_routing.*`) only selects *which receiver object* handles a
packet; since every receiver's `ReceiveMessage` runs on thread 0, it currently
buys **zero** CPU parallelism on the write path.

### Why this is exactly a 2×/3×/4× curve

A single processing thread is an **M/M/1 queue**. The 10 MB `SO_RCVBUF`
(`shared_udp_port.cc:33,167-170`) lets datagrams pile up in the kernel while
thread 0 is busy. Median ≈ service_time + queueing, and queueing → ∞ as offered
load λ approaches the single thread's service rate μ. That is precisely the
observed shape (≈74 µs → ≈157 µs as cluster throughput went ≈1k → ≈3.3k ops/s
under `--ionia-fast-quorum`, and accelerating). Ping-pong stays flat because it
does not run this serialized `AppUpcall` pipeline.

---

## Re-prioritized bottleneck list

| Rank | Bottleneck | Evidence | Scaling effect |
|------|-----------|----------|----------------|
| **1** | **Single-thread ingress+process** (only endpoint 0 pumps; 1–14 spin) | `udptransport.cc:645-669`, `shared_udp_port.cc:264-331` | Hard μ ceiling = 1 core → M/M/1 blowup. **Dominant.** |
| **2** | **Per-request service time in `AppUpcall`** (sets μ) | 2 full protobuf copies/write + 3 mutex-guarded map inserts + CAS loop, `common/replica.h:425-461` | Lower it → curve shifts right (higher saturation tput) |
| 3 | Leader-only concentration under `--ionia-fast-quorum` | `vr/client.cc` | Puts 100% of λ on one replica's thread 0 — compounds #1 |
| 4 | Quorum-3 tail (default) | `vr/client.cc` waits for 3rd reply | Tail grows with per-replica load |
| 5 | Wire/protobuf per-op (full type-name on wire, `type.assign`/`msg.assign`) | `shared_udp_port.cc:62-89` | Fixed µs/op; part of #2's μ |
| 6 | Busy-spin of idle threads | `udptransport.cc:649-653` | Lowers μ by contending with thread 0 (free win to remove) |

#1 dominates; #2 sets how high the per-thread ceiling is once #1 is parallelized.

---

# Priority 1 — Make the client-facing receive+process path genuinely parallel

**Goal:** N threads each receive and process requests concurrently, so μ scales
~N× and the single deep M/M/1 queue becomes N shallow queues.

**Key correctness fact that makes this safe:** durability state is *shared and
already thread-safe*. `durabilityLog`, `cxidDurIndexMap`, `lastUpdateToKey` live
on the single shared `nullApp` (`AppReplica`) and are
`phmap::parallel_flat_hash_map` guarded by per-submap `std::mutex`
(`data_structure.h:257-263`, `common/replica.h:98-109`). So **any thread may
process any key** — there is no per-key thread-affinity requirement. That removes
the main obstacle to parallelizing.

## Option A — `SO_REUSEPORT`, one socket + event loop per thread (recommended first)

- Each durability thread opens its **own** UDP socket bound to the same client
  address with `SO_REUSEPORT`, its **own** `event_base`, and pumps its own loop.
  The kernel load-balances datagrams across the sockets by 4-tuple.
- Changes localized to `lib/shared_udp_port.cc` / `lib/udptransport.cc`: in this
  mode, do **not** share one fd/base; each endpoint binds + pumps independently.
  Remove the `endpoint_id_ == 0` gate in this mode.
- Drop (or ignore) the Phase-2 `dst_endpoint` routing — kernel does the
  spreading; shared maps make it correct.
- **Pros:** kernel parallelizes *recv* too; least lock contention; closest to how
  Nezha/eRPC scale; smallest steady-state code.
- **Cons / must-verify:** loses deterministic key→thread mapping (fine — maps are
  shared); replies now leave from per-thread source ports, so confirm the client
  matches replies by `(clientid, clientreqid)` and not by peer port (UDP
  `recvfrom` accepts from any port; check `vr/client.cc`).

## Option B — One recv pump → per-thread queues → parallel workers (fallback)

- Thread 0 does **only** `recvfrom` (no app work) and pushes each decoded packet
  into one of N MPSC queues chosen by the existing key hash. Workers 1..N drain
  their queue and run `HandleRequest`/`AppUpcall`.
- **Pros:** preserves Phase-2 deterministic key→thread routing (less same-key
  cross-thread contention on the shared maps); a no-app recv thread can sustain
  millions of pps.
- **Cons:** one extra copy + enqueue per packet; single recv thread can still cap
  *ingress* pps (far higher than today); more new code.

## Recommendation

Implement **Option A** first — smaller change, removes the busy-spin, and directly
tests the hypothesis that μ scales with cores. Keep **Option B** as the fallback
if same-key contention on the shared maps becomes the next ceiling.

## Steps

1. **Cheap proof first (before any refactor):** add `IONIA_LEADER_STATS=1`
   counters on thread 0 — requests processed/s and a busy-time fraction — plus a
   per-thread "inbound packets handled" counter. Run the ≈3.3k-ops/s fast-quorum
   case; expect thread 0 ≈100% busy and threads 1–14 handling ≈0 inbound. Confirms
   #1 in ~30 min, low risk.
2. Add an opt-in transport mode (env, e.g. `IONIA_PER_THREAD_SOCKET=1`) so A is
   A/B-testable against the current build without deleting the old path.
3. In `udptransport.cc` / `shared_udp_port.cc`: when enabled, each endpoint binds
   its own `SO_REUSEPORT` socket + `event_base`; every thread pumps its own loop
   (remove the `endpoint_id_==0` gate in this mode).
4. Verify replies: client accepts replies regardless of replica source port
   (confirm in `vr/client.cc`).
5. Make the legacy-mode idle threads `yield()` instead of hot-spinning so the A/B
   comparison isn't polluted by cache/bandwidth theft.

## Validation

Sweep `--poisson-rate {1,3,10,30}` with `--ionia-fast-quorum`; plot median + p99
vs **achieved** cluster ops/s for old vs new mode. Success = median curve flattens
and saturation throughput rises ~toward N×.

## Risks / open questions

- Reply source-port matching on the client (must verify before committing to A).
- `SO_REUSEPORT` flow-hash imbalance under few clients/connections — may need more
  client source ports to spread evenly; revisit with Option B if skewed.
- Internal/consensus transport (port 31880, `replica.cc:288`) is separate and
  lower-volume; leave it single-pump for now.

---

# Priority 2 — Lower per-request service time in `AppUpcall` (raise μ per thread)

Even with N threads, each thread's μ is set by the write path:

- `common/replica.h:425-461`. Per write, single-threaded cost today:
  - **Two full `RequestMessage` protobuf copies**: `DurIndexAndMessage{ myIndex,
    msg }` into `message_vector` (leader), and `v.second = msg` into
    `durabilityLog`.
  - **Three** parallel-hashmap inserts (`durabilityLog`, `cxidDurIndexMap`,
    `lastUpdateToKey`), each taking a submap mutex.
  - A **CAS loop** on `lastUpdateToKey`.
  - Plus inbound `ParseFromArray` and the `type.assign`/`msg.assign` string copies
    in packet decode (`shared_udp_port.cc:62-89`).

## Targets (in impact order)

1. **Kill the duplicate protobuf copies** — store the message once (arena/view/
   pointer shared between `message_vector` and `durabilityLog`) instead of copying
   the full proto twice. Biggest per-op CPU item for writes.
2. **Collapse the 3 map inserts** — they are separately mutex-guarded; consider a
   single combined entry, or defer non-critical-path bookkeeping off the hot path.
   Confirm whether `keyToDurIndex` (folly map, `common/replica.h:105`) is dead and
   remove it.
3. **Shrink wire/decode cost** — replace the full protobuf **type-name string** on
   every packet with an integer tag, and avoid `std::string` copies in
   `DecodePacket`/`DeliverMessage` (parse in place). Helps both #2 and Option B's
   recv pump.

## Validation

Keep a **closed-loop, single-thread, write-only** microbench (1 client thread)
and measure ops/s for thread 0 before/after each change — a direct μ readout,
independent of the Priority-1 parallelism work.

---

# Suggested execution order

1. Instrument (`IONIA_LEADER_STATS`) → prove thread-0 saturation. *(fast, low-risk)*
2. Priority 1, Option A behind `IONIA_PER_THREAD_SOCKET` → A/B the rate sweep.
   *(the big win)*
3. Priority 2 copy/map/wire cuts → raise per-thread ceiling further. *(compounding)*
4. Re-evaluate: if same-key map contention is the new wall, switch to Option B routing.

# Files likely to change

- `lib/udptransport.cc`, `lib/udptransport.h` — per-thread socket/loop mode, pump gate.
- `lib/shared_udp_port.cc`, `lib/shared_udp_port.h` — `SO_REUSEPORT` bind path.
- `lib/fasttransport.cc` — mode plumbing; routing becomes optional in Option A.
- `bench/replica.cc` — thread/transport wiring; stats hook.
- `common/replica.h` — `AppUpcall` copy/map reductions; optional `IONIA_LEADER_STATS`.
- `experiments/GCP_tests/run_cluster_bench.py` — env plumbing + sweep harness.

# Out of scope (for now)

- Internal/consensus transport parallelization.
- Disk/SplinterDB persistence path (already off the commit critical path).
- Protocol/quorum semantics changes beyond the existing `--ionia-fast-quorum`.

---

# Implementation status (updated)

> Built locally as source edits only. **Not yet compiled** — the host lacks the
> toolchain; build in Docker (`docker build ... -t ionia-udp`) or on the cluster.

## Done

### Step 1 — `IONIA_LEADER_STATS` instrumentation (proves the bottleneck)
- `vr/durabilityreplica.cc`: per-**OS-thread** inbound counters (keyed by the
  actual thread running `HandleRequest`, not the logical endpoint), plus a
  detached printer thread that logs once per second:
  `IONIA_LEADER_STATS inbound/s total=<N> active_os_threads=<k> os_thrI=<delta> ...`
- Legacy mode should show **total carried by ~1 os thread** (`active_os_threads≈1`).
  Per-thread-socket mode should show the load spread across many.

### Step 2 — `IONIA_PER_THREAD_SOCKET` (the structural fix, Priority 1 / Option A)
- `lib/shared_udp_port.{h,cc}`: `PerThreadSocketEnabled()`; when set, `Acquire()`
  returns a **private** instance per durability thread, bound with
  `SO_REUSEADDR|SO_REUSEPORT`; `DeliverMessage()` delivers to the single local
  receiver (kernel already flow-hashed the datagram to this socket, so the
  per-packet endpoint hint is ignored); `Release()` deletes the private instance.
- `lib/udptransport.cc`: pump gate changed from "only endpoint 0" to
  "per-thread OR endpoint 0", so **every** durability thread pumps its own socket.
  Legacy non-pumping threads now `yield()` instead of hot-spinning (Priority #6).
- Correctness rests on durability maps being shared + thread-safe
  (`phmap::parallel_flat_hash_map` w/ per-submap mutex), so any thread may handle
  any key. Client reply matching is by `(clientid, clientreqid)`/`replicaidx`
  only (`vr/client.cc`), so per-thread reply source ports are safe.

### Priority 2 (partial) — remove dead state
- `common/replica.h`: removed `keyInDurabilityLog` and `keyToDurIndex`
  (referenced only in commented-out code).

### Harness
- `run_cluster_bench.py`: new flags `--ionia-per-thread-socket` and
  `--ionia-leader-stats` (export the env vars on replicas); recorded in
  `run_meta.json`; validated to `--protocol ionia`.

## Not done (needs a Docker build to validate; do not do blind)
- **Priority 2 headline — dedup the two full `RequestMessage` copies per write.**
  `message_vector` (consumed by the leader ordering thread, `vr/replica.cc:447-465`)
  and `durabilityLog` (read-after-write checks) each store their own copy. Safe
  dedup needs shared ownership (e.g. `shared_ptr`/arena view) with lifetime/
  concurrency review + a compile+test cycle. Left for a verified follow-up.
- Integer wire tag + in-place parse (replace protobuf type-name string).

# How to run

Prove the bottleneck (legacy vs fixed), fast-quorum high load:

```bash
# Baseline (legacy single-pump) WITH stats — expect active_os_threads≈1 in replica logs
python3 run_cluster_bench.py --protocol ionia --hosts-file cluster_hosts.json \
  --ionia-fast-quorum --ionia-leader-stats --poisson-rate 10 -e 60 -w 5

# Fixed (per-thread sockets) WITH stats — expect load spread; median should flatten
python3 run_cluster_bench.py --protocol ionia --hosts-file cluster_hosts.json \
  --ionia-fast-quorum --ionia-per-thread-socket --ionia-leader-stats \
  --poisson-rate 10 -e 60 -w 5
```

Then sweep `--poisson-rate {1,3,10,30}` for both modes and compare median/p99 vs
achieved cluster ops/s. Read the per-second `IONIA_LEADER_STATS` lines from the
replica logs to confirm thread distribution.

**Important — give it enough client flows.** With `SO_REUSEPORT` the kernel hashes
each datagram by 4-tuple, so *all requests from one client thread (one source
port) land on one durability thread*. The number of durability threads that
actually do work is therefore bounded by the number of client flows
(≈ senders × `--threads`). To exercise all 15 threads, run with enough client
threads, e.g. add `--threads 8` (or more) and/or more senders. Under
`--ionia-fast-quorum` every flow targets the leader replica, so the leader needs
≥ ~15 flows to fill its 15 durability threads. If `IONIA_LEADER_STATS` shows
`active_os_threads` far below the durability-thread count, add client flows before
concluding anything about scaling.

# Closing the normal-quorum gap

Normal quorum (no `--ionia-fast-quorum`) waits for `FastQuorumSize()` replies,
which is **3 (all replicas)** for `f=1` — a 3-of-3 super-quorum. Its latency is
therefore the **max** of three independent replica round-trips, so it is
inherently more tail-sensitive than fast quorum (leader-only, 1-of-1). Before
optimizing we must know whether the remaining gap is the **follower tail** (we
wait on the slowest replica) or the **leader** (per-op cost / queueing on the
leader's own reply).

## Diagnostic: `IONIA_CLIENT_TIMING` (client-side latency split)
Implemented in `vr/client.cc` / `vr/client.h`, gated by `IONIA_CLIENT_TIMING=1`
(harness flag `--ionia-client-timing`). For every completed request the client
records, in microseconds (steady clock):
- **leader** = leader-ack arrival − send
- **quorum** = quorum-completing reply − send (this is end-to-end)
- **tail**   = quorum − leader-ack (extra time spent waiting on the slowest follower)

A background thread logs median/p99/avg for each every 2 s to the client log:
```
IONIA_CLIENT_TIMING leader n=... med~Xus p99~Yus avg=Zus
IONIA_CLIENT_TIMING quorum n=... med~Xus p99~Yus avg=Zus
IONIA_CLIENT_TIMING tail   n=... med~Xus p99~Yus avg=Zus
```
(Percentiles are log2-bucket lower edges — coarse but enough to attribute the
gap. `n` is the per-2s count.)

### How to interpret
- **`tail` ≈ `quorum` − small `leader`** (large tail): bound by the follower
  tail. Closing the gap means reducing per-replica variance — e.g. the periodic
  `indices_to_check` drain scan that runs *before* the reply in
  `vr/durabilityreplica.cc` (every 512th set op stalls that request on every
  replica), allocator resets, and the two protobuf copies per write.
- **`leader` ≈ `quorum`** (tail ≈ 0): bound by the leader itself; the slowest
  reply is usually the leader's. Closing the gap means cutting leader per-op cost
  (Priority 2 copy dedup) and/or leader queueing.

### How to run
```bash
# Normal quorum + per-thread sockets + client timing, with enough flows
python3 run_cluster_bench.py --protocol ionia --hosts-file cluster_hosts.json \
  --ionia-per-thread-socket --ionia-client-timing --threads 8 \
  --poisson-rate 10 -e 60 -w 5
```
Then read `IONIA_CLIENT_TIMING` lines from each sender's `client.log`. Decide the
next optimization from whether the gap is `tail`- or `leader`-dominated.

# ROOT CAUSE of the ~150us baseline: busy-spin pump (FIXED)

Investigating a low-load fast-quorum run (`results/20260630T150305Z`) showed every
sender's per-thread medians were bimodal and identical across machines: ~6 threads
near ~150us and ~2 threads near ~22us. The ~22us is the true LAN 1-RTT; ~150us is
7x that, uniform across all sender machines and **independent of op type** (every
op-type prefix in `latencies.raw` had ~150us median), and the leader was lightly
loaded (4.8k/s spread over all 15 threads). So it is not hardware, not the
network, and not reads-vs-writes.

The cause: in `--ionia-per-thread-socket` mode `UDPTransport::Run()` pumped the
event loop with `event_base_loop(EVLOOP_ONCE | EVLOOP_NONBLOCK)` in a tight
`while(true)` — i.e. **every pump thread busy-spins a core at 100%**. With 15
durability threads on the leader plus 8 client threads per sender, threads far
exceed cores, so a reply sits in the socket until the OS reschedules that thread
(~130us). The few flows whose thread happens to hold a core return in ~22us.

**Fix:** `SharedUDPPort::RunBlockingOnce()` blocks in epoll (with a 1ms cap via
`event_base_loopexit`) instead of spinning; `Run()` uses it by default. A real
packet wakes epoll in sub-us so latency is unaffected, but idle CPU drops from
100% to ~0, removing the oversubscription. `IONIA_BUSY_POLL=1` restores the old
spin pump for A/B.

After rebuilding, the per-thread medians should collapse toward ~22-30us instead
of ~150us. Re-run the low-load fast-quorum case and compare.

# Harness load knobs (important)

`--poisson-rate R` is **per benchmark thread after a ceil-divide floored at 1**,
and the open-loop tick is 10ms (`kOpenLoopIntervalMs`). So `perThreadPoisson =
max(1, ceil(R / threads))`, and each rate-1 thread offers ~100 ops/s. With
`-R 1 --threads 8` you get 1 per thread => 8 x 100 = ~800 ops/s per sender =>
~5600 ops/s across 7 senders (observed ~4k achieved) — NOT the ~700 the harness
prints. The printed "~100 offered ops/s per sender" ignores `--threads` and the
floor. To set a true low rate with many threads, use `--offered-ops-per-sec`
(`-G`), which is divided across threads as a float (no floor), or use
`--threads 1`.

# IONIA UDP Multithreading Plan

**Status:** Implemented (SharedUDPPort + FastTransport endpoint meta)  
**Scope:** Restore multi-threaded IONIA (VR bench harness, `-m vr`) on the current UDP `FastTransport` stack  
**Primary approach:** Option A — shared UDP listener + endpoint demux (eRPC Nexus + Rpc-id equivalent)

---

## 1. Problem statement

IONIA was originally built around **eRPC** `FastTransport`, which supported many logical endpoints on **one bound port** via:

- one shared `erpc::Nexus` per `host:port`
- many `erpc::Rpc` objects distinguished by `id` (the `FastTransport` constructor’s `id` / bench `transport_id`)
- session routing with `GetSession(replicaIdx, dstRpcIdx)`

The current UDP shim (`src/ionia/lib/fasttransport.cc`) wraps `UDPTransport` but **does not implement this model**. Each `FastTransport` instance owns its own `UDPTransport`, and `Register()` always calls `bind()` on the configured replica port. A second durability thread on the same port fails with **“Address already in use”**.

As a result:

- `bench/replica` defaults to `totalThreads = 15` (`-j`), but GCP/docker harnesses force **`-j 1`**
- config lines like `replica 10.0.0.1:31850/15` parse `threads=15` into `ReplicaAddress`, but **nothing reads that field**
- IONIA benchmarks run with artificially reduced durability parallelism

This is a **transport regression**, not an intentional protocol limit.

---

## 2. Goals

| ID | Goal |
|----|------|
| G1 | Allow `-j N` durability threads per IONIA replica (target: restore default **N=15** on CloudLab-class nodes) |
| G2 | Keep **one UDP port per role** (client-facing 31850, internal consensus 31880) — no firewall explosion |
| G3 | Preserve backward compatibility: single-endpoint deployments (`N=1`, DYNA, VR internal path) behave as today |
| G4 | Keep protocol logic in `vr/` unchanged unless a small hook is required for endpoint-aware sends |
| G5 | Enable fair IONIA vs DYNA benchmarks by removing the `-j 1` transport cap |

## 3. Non-goals

| ID | Non-goal |
|----|----------|
| NG1 | Rewriting VR or DYNA consensus/durability algorithms |
| NG2 | Merging VR’s durability threading model with DYNA’s partition-worker model |
| NG3 | Re-introducing eRPC as a dependency |
| NG4 | `SO_REUSEPORT` as the permanent design (acceptable only as a short-lived spike, not this plan) |
| NG5 | Per-thread port offsets (`31850 + i`) unless Option A proves infeasible |
| NG6 | Changing DYNA’s replica architecture (DYNA already parallelizes above a single transport) |

---

## 4. Current architecture (baseline)

### 4.1 IONIA / VR replica process (`bench/replica -m vr`)

```
main thread
├── internal_transport_thread
│     VRReplica on internalConfig (port 31880)
│     internalTransport.Run()          ← single bind, single event loop
│
└── durability_threads[0 .. ht_ct-1]   ← ht_ct = -j (default 15)
      each thread:
        FastTransport client_transport(clientConfig, ..., transport_id)
        VRDurabilityReplica(..., threadIndex)
        client_transport.Run()           ← each tries bind(31850) → FAILS when ht_ct > 1
```

**Consensus replication** (Prepare/Commit, `BgRepl`) runs on the **internal** transport thread only.  
**Durability front-end** (client `RequestMessage`, execution replies) runs on **client-facing** transport threads.

Post-commit execution is already sharded in `vr/replica.cc`:

```cpp
queueIndex = hash(key) % app->GetNumDurabilityReplicas();
app->SendDataToExecutionQueue(queueIndex, data);
```

Each `VRDurabilityReplica` pulls from `executionQueues[threadIndex]` via `PullDataFromExecutionQueue(threadIndex)`.

### 4.2 What broke in the UDP migration

`fasttransport.cc` ignores `nthreads`, `id`, `phy_port`, buffer sizes, etc., and delegates blindly to `UDPTransport::Register()`, which maps **one fd → one receiver**:

```cpp
receivers[fd] = receiver;   // last Register wins; second bind() panics earlier
```

### 4.3 What did *not* exist before

Config `host:port/N` stores `ReplicaAddress.threads = N` but was **never used for port arithmetic or routing** in either eRPC or UDP `LookupAddress()`. The `/N` suffix is metadata (“this replica expects N durability endpoints”), not `port + N`.

---

## 5. Proposed design: shared listener + endpoint demux

### 5.1 Concept

Introduce a **process-wide shared UDP listener** per bound `(ip, port)`:

```
SharedUDPPort("0.0.0.0:31850")
  event_base (one libevent loop, optionally driven by one or many RunOnce() callers)
  fd (single bind)
  endpoints[0 .. N-1] → TransportReceiver*
```

Each `FastTransport` registers as **endpoint `id`** on the shared port instead of binding a new socket.

This mirrors eRPC:

| eRPC | UDP (proposed) |
|------|----------------|
| `erpc::Nexus` on `host:port` | `SharedUDPPort` on `host:port` |
| `erpc::Rpc` with `id` | `FastTransport` with `endpoint_id = id` |
| demux in eRPC runtime | demux in `ProcessPacket` via meta header |
| `GetSession(replicaIdx, dstRpcIdx)` | optional phase-2: send to `dst_endpoint` |

### 5.2 Wire format extension

Existing UDP payload layout (`udptransport.cc`):

```
NONFRAG_MAGIC | meta_len | meta | type_len | type | data_len | data
```

**Ordered multicast** already uses `meta` for sequencer fields. **FastTransport endpoint routing** will use a dedicated meta layout only when `FastTransport` sends/receives (not for plain `UDPTransport` direct users).

Proposed **FastTransport meta** (version 1):

```c
struct FastTransportEndpointMeta {
    uint8_t  version;        // = 1
    uint8_t  src_endpoint;   // sender endpoint id
    uint8_t  dst_endpoint;   // intended receiver endpoint id
    uint8_t  reserved;
};
// sizeof = 4 bytes; meta_len = 4 when present
```

**Compatibility rules:**

| Condition | Behavior |
|-----------|----------|
| `meta_len == 0` | Legacy packet → deliver to **endpoint 0** on that port |
| `meta_len == 4` and `version == 1` | Demux to `dst_endpoint` |
| `meta_len > 0` and not FastTransport meta | Existing multicast/sequencer path unchanged |

Send path (`FastTransport` only): stamp `src_endpoint` from constructor `id`; set `dst_endpoint` from routing policy (default 0 in phase 1).

Receive path (`SharedUDPPort`): after decode, if FastTransport meta present → `endpoints[dst_endpoint]->ReceiveMessage(...)`; else → `endpoints[0]`.

### 5.3 Who shares a port?

| Registration | groupIdx | replicaIdx | Port | Endpoints |
|--------------|----------|------------|------|-----------|
| VR internal (`VRReplica`) | 0 | `myIdx` | 31880 | **1** (id=0) |
| VR durability thread `i` | 0 | `myIdx` | 31850 | **N** (id=`transport_id`, typically `i % n`) |
| DYNA replica | 0 | `myIdx` | 31880 | **1** (id=0) |
| Bench client thread | -1 | -1 | ephemeral | **1** each (separate binds, unchanged) |

Client threads (`replicaIdx == -1`) continue to bind `"any"` — no shared listener needed.

### 5.4 Event-loop ownership

**Constraint today:** each durability thread calls `client_transport.Run()` (blocking `event_base_dispatch`) or, with `USE_EVENTS_FOR_LOOP`, `RunOnce()` in a tight loop plus `OnEventLoop` hooks.

**Proposed model:**

1. **First registrant** on a shared port creates the listener and event base.
2. **Subsequent registrants** only add endpoint entries.
3. **Run / RunOnce:**
   - If this `FastTransport` is the **loop owner** (first on that port): run shared `event_base` as today.
   - If not loop owner: `RunOnce()` only pumps the **shared** base once (idempotent across callers) and then runs local `on_event_loop_function` (durability execution callback).

Alternative (simpler for phase 1): designate durability thread 0 as the sole `Run()` blocker; threads `1..N-1` only call `RunOnce()` + execution hooks in their loop. Document this in `bench/replica.cc`.

**Timers:** migrate to shared `event_base` with per-`FastTransport` timer tags (`UDPTransportTimerInfo` already stores `transport*`). Timers registered on any endpoint attached to a port schedule on the shared base.

### 5.5 Send-side routing (phased)

**Phase 1 (minimum viable):** all sends use `dst_endpoint = 0`. Sufficient when each client thread has its own ephemeral source port and the bench does not target specific durability shards. Incoming client requests hit endpoint 0 only — **incorrect for full eRPC parity** but may work if durability threads are symmetric and can share request handling.

**Phase 2 (recommended before `-j 15` production benchmarks):** route to remote endpoint using the same hash as execution queues:

```cpp
endpoint = hash(key) % remote_replica.threads;
```

Requires:

- reading `ReplicaAddress.threads` from config (already parsed)
- optional key extraction in `SendMessageToReplica` for `RequestMessage` / unlogged ops, **or** explicit `dst_endpoint` parameter on internal fast send API

**Phase 1 risk:** if only endpoint 0 receives client requests, threads 1..14 only serve post-commit execution queues — may underutilize durability ingress. Validate with a microbenchmark before declaring victory.

---

## 6. File-level change plan

### 6.1 New files

| File | Purpose |
|------|---------|
| `src/ionia/lib/shared_udp_port.h` | `SharedUDPPort` registry API |
| `src/ionia/lib/shared_udp_port.cc` | bind-once, endpoint table, recv demux, refcount teardown |

### 6.2 Modified files

| File | Changes |
|------|---------|
| `src/ionia/lib/udptransport.h` | Optional hook: `RegisterOnSharedPort(...)`, `endpoint_id` member, shared vs private bind mode |
| `src/ionia/lib/udptransport.cc` | Branch `Register()`: client → private bind; replica + shared mode → delegate to `SharedUDPPort`; `ProcessPacket` demux; `_SendMessageInternal` accept meta from caller |
| `src/ionia/lib/fasttransport.h` | Store `endpoint_id`, `shared_port_key`, `owns_event_loop` |
| `src/ionia/lib/fasttransport.cc` | Wire constructor `id` → `endpoint_id`; stamp FastTransport meta on send; enable shared registration for replica binds |
| `src/ionia/lib/Rules.mk` | Add `shared_udp_port.cc` to `LIB-udptransport` or new `LIB-shared-udp` |
| `src/ionia/bench/replica.cc` | (Small) document loop-ownership; optionally pin thread 0 as `Run()` owner; validate `-j` against `ReplicaAddress.threads` |
| `experiments/GCP_tests/remote/setup_ionia.sh` | Raise default `IONIA_DURABILITY_THREADS` after validation; remove “will fail on UDP” warning |
| `experiments/GCP_tests/setup_ionia_cluster.py` | Default `DURABILITY_THREADS=15` (or env-driven) once smoke passes |
| `experiments/GCP_tests/README.md` | Document multithreading restore and benchmark fairness notes |

### 6.3 Explicitly unchanged (default path)

| File | Reason |
|------|--------|
| `src/ionia/dyna/replica.cc` | Single `Register`, single endpoint — uses `endpoint_id=0` legacy path |
| `src/ionia/vr/replica.cc` | Internal transport stays single-threaded |
| `src/ionia/vr/durabilityreplica.cc` | No changes expected if transport demux is correct |
| `src/ionia/common/replica.cc` | Execution queue sharding already correct |
| `src/ionia/lib/configuration.cc` | Already parses `/N`; start **reading** `.threads` in routing (phase 2) |

### 6.4 Reference only (do not build)

| File | Role |
|------|------|
| `src/ionia/lib/deprecated_fasttransport.cc` | Behavioral reference for Nexus sharing, Rpc `id`, `GetSession` |

---

## 7. API sketch

### 7.1 `SharedUDPPort`

```cpp
class SharedUDPPort {
public:
    static SharedUDPPort *Acquire(const std::string &host, uint16_t port,
                                  event_base *evbase_or_null);
    void Release();

    int RegisterEndpoint(uint8_t endpoint_id, TransportReceiver *receiver);
    void UnregisterEndpoint(uint8_t endpoint_id);

    void Run();      // dispatch loop
    void RunOnce();
    void Stop();

    int fd() const;
    event_base *EventBase();

private:
    void OnReadable();
    void Deliver(uint8_t dst_endpoint, const UDPTransportAddress &sender,
                 const std::string &type, const std::string &data, void *meta);
};
```

Registry key: `(host, port)` normalized from config bind target.

### 7.2 `FastTransport` constructor

```cpp
FastTransport(..., uint8_t id, ...)
    : endpoint_id_(id),
      use_shared_port_(true),   // replica registrations only
      ...
```

### 7.3 `UDPTransport::Register` decision tree

```
if replicaIdx == -1:
    private bind (ephemeral) — unchanged
else if FastTransport shared mode enabled:
    SharedUDPPort::Acquire(host, port)->RegisterEndpoint(endpoint_id, receiver)
    record endpoint_id → receiver; do NOT insert into receivers[fd] 1:1 map
else:
    existing private bind path (for tests / rollback)
```

---

## 8. Threading model after fix

### IONIA replica (`-j 15`)

| Component | Threads | Transport |
|-----------|---------|-----------|
| VR consensus | 1 | shared port 31880, endpoint 0 |
| Durability front-end | 15 | shared port 31850, endpoints 0..14 |
| Per-thread execution | 15 | `RunOnce` + `PullDataFromExecutionQueue(i)` |

### DYNA replica

| Component | Threads | Transport |
|-----------|---------|-----------|
| UDP I/O | 1 event loop | port 31880, endpoint 0 |
| Protocol workers | 3× partitions + fast-reply shards | in-memory queues (unchanged) |

**Fair comparison note:** after this fix, both protocols use real UDP and full intended parallelism. Remaining differences (2-port VR vs 1-port DYNA, durability vs partition workers) are **protocol design**, not transport bugs. Document in benchmark writeups.

---

## 9. Implementation phases

### Phase 0 — Spike & validation (1–2 days)

- [ ] Confirm `bench/replica -m vr -j 2` reproduces bind failure locally
- [ ] Prototype `SharedUDPPort` with two endpoints on localhost; ping-pong test
- [ ] Verify `meta_len == 0` legacy packets still hit endpoint 0

### Phase 1 — Core transport (3–5 days)

- [ ] Implement `SharedUDPPort` + integrate into `UDPTransport::Register`
- [ ] Wire `FastTransport` `id` → `endpoint_id`
- [ ] Add FastTransport meta on send; demux on receive
- [ ] Unify timer scheduling on shared `event_base`
- [ ] Resolve `Run()` / `RunOnce()` ownership in `bench/replica.cc`
- [ ] Unit test: 4 endpoints, 1 port, demux correctness

### Phase 2 — Routing parity (2–3 days)

- [ ] Use `ReplicaAddress.threads` in `LookupAddress` / send path
- [ ] Key-hash endpoint selection for `RequestMessage` sends (mirror `vr/replica.cc` queue hash)
- [ ] Client `bench/client` multi-thread: verify replies reach correct client transport

### Phase 3 — Harness & benchmarks (1–2 days)

- [ ] Update `setup_ionia.sh` / `setup_ionia_cluster.py` defaults
- [ ] GCP smoke: `-j 15`, 3 replicas, short open-loop run
- [ ] Compare `-j 1` vs `-j 15` throughput/latency on same cluster
- [ ] Regression: DYNA bench unchanged (`-j 1` transport, partition workers)

### Phase 4 — Hardening (ongoing)

- [ ] Stress: packet loss / reorder sim unchanged
- [ ] Large message fragmentation with meta present
- [ ] Clean shutdown: `Release()` when last endpoint unregisters
- [ ] Thread sanitizer / valgrind on 2-endpoint setup

---

## 10. Testing plan

### 10.1 Transport-level tests (new: `lib/tests/shared_udp_port_test.cc`)

| Test | Pass criteria |
|------|---------------|
| `SharedBindTwoEndpoints` | Two `Register` on same port, no panic |
| `DemuxToEndpoint` | Send with `dst_endpoint=3` received only by endpoint 3 |
| `LegacyMetaZero` | `meta_len=0` delivered to endpoint 0 |
| `MulticastUnchanged` | Ordered multicast meta still parsed; no collision with FastTransport meta |
| `TimerOnSharedBase` | Timers fire on correct `FastTransport` instance |

### 10.2 Integration tests

| Test | Command / setup | Pass criteria |
|------|-----------------|---------------|
| VR durability smoke | `bench/replica -m vr -j 4` local 3-node | All replicas start, no bind error |
| Docker UDP smoke | existing `run_udp_smoke.sh` with `SMOKE_PROTOCOL=vr` | Pass with `-j 15` |
| DYNA regression | `bench/replica -m dyna` | Identical behavior, single bind |
| Client multi-thread | `bench/client -m vr -j 4` | Completes ops, no hang |

### 10.3 Benchmark acceptance

| Metric | `-j 1` (baseline) | `-j 15` (target) |
|--------|-------------------|------------------|
| Startup | clean | clean |
| Throughput @ fixed load | X ops/s | ≥ X (expect improvement on multi-core) |
| p99 latency | baseline | no regression > 20% at same load |
| DYNA @ same load | unchanged | unchanged |

---

## 11. Rollback strategy

- Feature flag: `IONIA_SHARED_UDP_PORT=0` disables shared mode → legacy per-socket bind (forces `-j 1`)
- Keep private-bind code path behind the flag for one release cycle
- `deprecated_fasttransport.cc` remains unlinked; no runtime fallback to eRPC

---

## 12. Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Phase 1 routes all ingress to endpoint 0 | High | Medium | Phase 2 hash routing; measure per-thread request counts |
| Multiple threads call `Run()` on shared base | Medium | High | Document single owner; assert in debug builds |
| Meta namespace collision with multicast | Low | High | Distinct `version` byte; only FastTransport sets 4-byte meta |
| Timer callbacks on wrong transport | Medium | Medium | Tag `UDPTransportTimerInfo.transport`; audit all timer paths |
| Fragment reassembly + meta | Low | Medium | Test fragmented RequestMessage with endpoint meta |
| `ReplicaAddress.threads` mismatch vs `-j` | Medium | Low | Warn in `bench/replica` if `-j != config.threads` |

---

## 13. Open questions (resolve before coding)

1. **Loop owner:** Should durability thread 0 alone call `Run()`, or should all threads call `RunOnce()` in a coordinated loop (current `USE_EVENTS_FOR_LOOP` style)?
   - **Recommendation:** match existing `USE_EVENTS_FOR_LOOP` path (already defined in `data_structure.h`) — all threads `RunOnce()` + hooks; no thread blocks in `dispatch` except optionally thread 0 as primary pump.

2. **Ingress symmetry:** Are all durability endpoints required to handle client `RequestMessage`, or only endpoint 0?
   - **Recommendation:** phase 2 hash routing to match execution queue sharding; until then, document that `-j > 1` primarily parallelizes **post-commit execution**, not client ingress.

3. **Config enforcement:** Should `bench/replica` require `-j == replica.threads` from config?
   - **Recommendation:** warn if mismatched; default `-j` from first config line’s `/N` suffix when present.

4. **GCP default:** Restore `-j 15` immediately or gate behind `IONIA_DURABILITY_THREADS` env?
   - **Recommendation:** env default 15 after smoke; keep `1` in CI until phase 1 tests land.

---

## 14. Success criteria (definition of done)

- [ ] `bench/replica -m vr -j 15` starts on 3-replica cluster without bind errors
- [ ] Open-loop bench completes with throughput ≥ `-j 1` case on 8+ vCPU nodes
- [ ] DYNA bench results within noise of pre-change baseline
- [ ] `meta_len == 0` senders (legacy) still work
- [ ] GCP README documents restored multithreading and fair-comparison notes
- [ ] This plan’s open questions resolved in an addendum or PR description

---

## 15. Related paths

| Path | Relevance |
|------|-----------|
| `src/ionia/lib/fasttransport.cc` | Current broken shim |
| `src/ionia/lib/deprecated_fasttransport.cc` | eRPC reference implementation |
| `src/ionia/bench/replica.cc` | Durability thread spawn (`-j`) |
| `src/ionia/vr/durabilityreplica.cc` | Per-thread durability receiver |
| `src/ionia/vr/replica.cc` | Execution queue hash routing |
| `experiments/GCP_tests/setup_ionia_cluster.py` | Harness defaults |
| `experiments/GCP_tests/remote/setup_ionia.sh` | Remote start script |

---

## 16. Review checklist

Before implementation, confirm:

- [ ] Shared-listener + meta demux approach approved (vs SO_REUSEPORT or port offsets)
- [ ] Phase 1 “endpoint 0 only” ingress acceptable for initial merge, with phase 2 routing fast-follow
- [ ] OK to touch shared `lib/` (`udptransport`, `fasttransport`) with DYNA on legacy single-endpoint path
- [ ] Target default `-j` for GCP/CloudLab benchmarks (1, 4, or 15)
- [ ] Event-loop ownership model (`USE_EVENTS_FOR_LOOP` vs single `Run()` thread)

---

*End of plan.*

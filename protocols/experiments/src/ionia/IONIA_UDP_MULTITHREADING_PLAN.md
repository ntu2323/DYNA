# IONIA UDP Multithreading Plan

**Status:** Planned (reverted after first attempt; re-implement in phases below)  
**Scope:** Restore multi-threaded IONIA (`bench/replica` + `bench/client`, `-m vr`) on the UDP `FastTransport` stack  
**Approach:** Option A — shared UDP listener + endpoint demux (eRPC Nexus + Rpc-id equivalent)

---

## 0. Post-mortem (first attempt, June 2026)

A full implementation landed on branch `main` but **failed on the distributed cluster** (0 completions, ~376k submissions). After **revert to pre-multithread code**, the same cluster run succeeded (~250 ops/s window mean, ~1.7 ms median latency, 50k ops in 150s).

### What worked

| Item | Evidence |
|------|----------|
| `SharedUDPPort` + replica-side demux | Docker smoke with `-j 4` passed; replica logs showed 2 binds per port (internal + client) |
| Network / firewall UDP 31850 | Manual probe + tcpdump: 195-byte bench packets reached `10.10.1.1:31850` |
| Pre-multithread quorum / VRClient logic | Same trace + `f=1` worked before multithreading; **do not change quorum** |
| `ionia_multithread` isolation flag | DYNA/DYNA_new must stay on legacy wire format |

### What broke (root causes)

| Bug | Symptom | Fix |
|-----|---------|-----|
| **`Rules.mk` link error** | `shared_udp_port.cc` in `SRCS` but **not** in `LIB-fasttransport` → undefined refs at link (or stale partial deploy) | `LIB-fasttransport := … $(o)shared_udp_port.o $(LIB-udptransport)` |
| **Client reply parsing** | Replicas send replies with 4-byte `FastTransportEndpointMeta`; client listens on **ephemeral port** (not `SharedUDPPort`). `UDPTransport::ProcessPacket` mis-parsed meta as multicast stamp → replies dropped → **0 completions** | Strip `FastTransportEndpointMeta` in `UDPTransport::ProcessPacket` (same as `SharedUDPPort`) **in Phase 1** |
| **Premature GCP open-loop** | Hard to debug 0-ops at 3000 offered ops/s | Gate on **closed-loop** + return-path tcpdump before open-loop |
| **Incorrect diagnosis** | Quorum / partial replica reachability blamed | Reverted quorum experiments; quorum=3 was fine pre-multithread |

### Rollback

Code restored to pre-multithread baseline (`916b5c6` transport/bench). GCP harness kept with `-j 1` default. Kill switch when re-landing: `IONIA_SHARED_UDP_PORT=0`.

---

## 1. Problem statement

IONIA was originally built around **eRPC** `FastTransport`: many logical endpoints on **one bound port** via one `Nexus` per `host:port` and many `Rpc` objects with distinct `id`.

The current UDP shim wraps `UDPTransport` but each `FastTransport` calls `bind()` on the configured port. A second durability thread on the same port fails with **"Address already in use"**.

As a result:

- `bench/replica` defaults to `totalThreads = 15` (`-j`), but harnesses forced **`-j 1`**
- Config `replica host:port/N` parses `threads=N` into `ReplicaAddress`, but routing was unused until Phase 2
- IONIA benchmarks ran with artificially reduced durability parallelism

This is a **transport regression**, not an intentional protocol limit.

---

## 2. Goals and non-goals

### Goals

| ID | Goal |
|----|------|
| G1 | Allow `-j N` durability threads per IONIA replica (target **N=15** on CloudLab-class nodes) |
| G2 | One UDP port per role (client 31850, internal 31880) — no per-thread port offsets |
| G3 | `N=1` and DYNA/DYNA_new behave exactly as today |
| G4 | Keep `vr/` protocol logic unchanged except transport hooks |
| G5 | Fair IONIA vs DYNA benchmarks without artificial `-j 1` cap |

### Non-goals

| ID | Non-goal |
|----|----------|
| NG1 | Rewriting VR or DYNA consensus/durability algorithms |
| NG2 | Merging VR durability threading with DYNA partition workers |
| NG3 | Re-introducing eRPC |
| NG4 | `SO_REUSEPORT` as permanent design |
| NG5 | Per-thread port offsets (`31850 + i`) unless Option A fails |
| NG6 | Changing `VRClient` quorum / `leader_acked` semantics |
| NG7 | Enabling multithread wire format for `dyna` / `dyna_new` |

---

## 3. Architecture (Option A)

### 3.1 Replica process (`bench/replica -m vr`)

```
main thread
├── internal_transport_thread
│     VRReplica on internalConfig (port 31880)
│     SharedUDPPort endpoint 0, single event loop
│
└── durability_threads[0 .. N-1]   (N = -j, default 15)
      each thread:
        FastTransport(clientConfig, …, transport_id=thread_id, ionia_multithread=true)
        VRDurabilityReplica(…, threadIndex)
        client_transport.Run()   → SharedUDPPort on 31850, demux by endpoint
```

Post-commit execution is already sharded in `vr/replica.cc`:

```cpp
queueIndex = hash(key) % app->GetNumDurabilityReplicas();
app->SendDataToExecutionQueue(queueIndex, data);
```

### 3.2 Shared listener

```
SharedUDPPort("10.x.x.x:31850")
  one fd, one bind
  endpoints[0..N-1] → TransportReceiver*
  ProcessPacket → demux via FastTransportEndpointMeta.dst_endpoint
```

| eRPC | UDP |
|------|-----|
| `erpc::Nexus` | `SharedUDPPort` |
| `erpc::Rpc` + `id` | `FastTransport` + `endpoint_id` |
| session routing | Phase 2: `dst_endpoint` on send |

### 3.3 Wire format

4-byte meta after UDP packet `meta_len` (see `fasttransport_endpoint.h`):

```cpp
struct FastTransportEndpointMeta {
    uint8_t version;        // 1
    uint8_t src_endpoint;
    uint8_t dst_endpoint;
    uint8_t reserved;
};
```

When `ionia_multithread=false` (DYNA, legacy): no meta (`meta_len=0`), unchanged wire format.

### 3.4 Client asymmetry (critical)

| Side | Socket model | Multithread |
|------|----------------|-------------|
| Replica durability | `SharedUDPPort` on 31850 | send + receive with meta |
| Bench client | Ephemeral `bind(0)` | **send** with meta; **receive** must strip meta in `UDPTransport::ProcessPacket` |

The client never uses `SharedUDPPort` (`replicaIdx == -1` in `Register`). Reply parsing fix is **mandatory** in Phase 1.

### 3.5 Isolation

```cpp
// bench/replica.cc, bench/client.cc — PROTO_VR only
const bool ionia_multithread = (proto == PROTO_VR);
FastTransport transport(..., ionia_multithread);
```

DYNA / DYNA_new: `ionia_multithread=false` always.

### 3.6 Config `/N` suffix

`replica 10.0.0.1:31850/15` stores `ReplicaAddress.threads = 15`. It documents expected endpoint count and drives Phase 2 routing; it is **not** `port + N`.

---

## 4. Phased rollout

Do **not** skip gates. Do **not** merge Phase 2 before Phase 1 passes on the cluster.

### Phase 0 — Prep (no cluster behavior change)

**Files to add/restore:**

| File | Purpose |
|------|---------|
| `lib/shared_udp_port.{cc,h}` | Shared listener + demux |
| `lib/fasttransport_endpoint.h` | 4-byte meta |
| `lib/udptransport.{cc,h}` | Shared-port registration, `SendWithMeta` |
| `lib/fasttransport.{cc,h}` | `ionia_multithread` flag, `Run()` loop |

**Build fix (do first):**

```makefile
# lib/Rules.mk
LIB-fasttransport := $(o)fasttransport.o $(o)shared_udp_port.o $(LIB-udptransport)
```

**Other:**

- `IONIA_SHARED_UDP_PORT=0` disables shared listener (rollback)
- Docker build must run `make bench/client bench/replica` (catch link errors)

**Gate:** `docker build -f docker/Dockerfile.udp -t ionia-udp .` succeeds.

---

### Phase 1 — Replica multithreading + client reply fix (no key routing)

**Scope:**

- Integrate `SharedUDPPort` for `replicaIdx != -1`
- `bench/replica.cc`: `transport_id = thread_id`; `ionia_multithread=true` for VR only
- **`UDPTransport::ProcessPacket`**: strip `FastTransportEndpointMeta` before multicast-stamp parsing
- **No** `fasttransport_routing` yet — client may send `dst_endpoint=0` only, or cluster stays `-j 1` until routing lands
- Config: plain `replica ip:31850` (no `/N` required)

**Gates (in order):**

1. `LOW_MEMORY=1 ./docker/docker-smoke.sh` — VR `-j 1` regression
2. `./docker/docker-smoke-ionia-mt.sh` — single replica `-j 4`; logs contain `SharedUDPPort binding`; completions > 0
3. **Cluster closed-loop** (before any open-loop):
   ```bash
   ./bench/client -c ~/bench/ioniaconfig.client -s ~/bench/ioniaconfig -m vr \
     -k ~/bench/trace.micro --closed-loop -n 20 -e 30 -w 2 -t 1 -j 1 -l /tmp/lat.test
   ```
4. **On sender during closed-loop:** `sudo tcpdump -ni any 'udp and src port 31850' -c 10` — must see return packets
5. Client log: `completed > 0`

**Do not proceed to Phase 2 until Phase 1 closed-loop passes with `-j 15` on at least one replica (Docker or cluster).**

---

### Phase 2 — Client request routing (`/N` + key hash)

**Scope:**

- Add `lib/fasttransport_routing.{cc,h}` — djb2 over 24-byte bench key (match `vr/replica.cc` execution queue)
- `FastTransport::SendMessageInternal`: set `dst_endpoint` for `RequestMessage` / `UnloggedRequestMessage`
- Config `replica ip:31850/N`; GCP `IONIA_DURABILITY_THREADS=N` (default 15)
- `setup_ionia_cluster.py` / `remote/setup_ionia.sh` — `/N` in client config, `ulimit` when `N > 1`

**Gates:**

1. Docker multithread smoke at `N=4` and `N=15` (if cores allow)
2. Cluster closed-loop with `-j 15` on all replicas
3. Cluster open-loop at **low** rate (`--poisson-rate 5`) — completions > 0
4. Optional: per-thread queue depth / log spread across durability threads under load

---

### Phase 3 — Harness and production defaults

- GCP README: firewall **UDP 31850 and 31880**
- Default `IONIA_DURABILITY_THREADS=15` in setup (after Phase 2 gates)
- Document rollback: `IONIA_DURABILITY_THREADS=1 IONIA_SHARED_UDP_PORT=0`
- Full open-loop bench (`--poisson-rate 30`) vs reverted baseline

**Success criteria (vs reverted baseline ~250 ops/s, ~1.7 ms median):**

- Open-loop completes with `-j 15` (no 0-ops)
- Throughput ≥ reverted baseline at same offered load, or latency improves
- DYNA docker smoke still passes
- No `Address already in use` in replica logs

---

## 5. Test matrix

```
Layer 1: make link              Rules.mk includes shared_udp_port.o
Layer 2: docker VR -j 1         no regression
Layer 3: docker VR -j 4         SharedUDPPort + completions > 0
Layer 4: cluster closed-loop    return tcpdump on sender shows 31850 → client
Layer 5: cluster open-loop      aggregate total_ops > 0
Layer 6: -j 15 + routing        load spread across durability threads
Layer 7: dyna docker smoke      DYNA unchanged
```

---

## 6. PR strategy

| PR | Contents | Merge when |
|----|----------|------------|
| **PR 1** | Phase 0 + Phase 1: shared port, endpoint meta, client reply strip, replica `-j N`, docker mt smoke | All Phase 1 gates green |
| **PR 2** | Phase 2: `fasttransport_routing`, `/N` config, GCP `-j 15` | Phase 2 gates + open-loop at low rate |
| **PR 3** | Phase 3: docs, defaults, full bench validation | Matches or beats reverted throughput |

---

## 7. Files touched (checklist)

### New

- `lib/shared_udp_port.{cc,h}`
- `lib/fasttransport_endpoint.h`
- `lib/fasttransport_routing.{cc,h}` (Phase 2 only)
- `docker/docker-smoke-ionia-mt.sh`

### Modified

- `lib/udptransport.{cc,h}`
- `lib/fasttransport.{cc,h}`
- `lib/Rules.mk`
- `bench/replica.cc`
- `bench/client.cc` (`ionia_multithread` for `PROTO_VR` only)
- `vr/Rules.mk` (if `vr-proto.o` link needed)
- `docker/run_udp_smoke.sh`, `docker/docker-smoke.sh`
- `experiments/GCP_tests/setup_ionia_cluster.py`, `remote/setup_ionia.sh`, `README.md` (Phase 2+)

### Unchanged

- `vr/durabilityreplica.cc`, `vr/client.cc` (quorum logic)
- `dyna/*`, `dyna_new/*`
- DYNA bench paths

---

## 8. Debugging checklist (0 completions)

1. `ss -ulnp | grep 31850` on receivers — listening?
2. `tcpdump udp port 31850` on receiver — inbound 195-byte packets?
3. `tcpdump 'udp and src port 31850'` on sender — **return** packets?
4. `grep -iE 'bad magic|no receiver|Address already' ~/bench/replica-0.log`
5. Confirm `bench/client` rebuilt after `udptransport.cc` meta strip
6. Bisect: `IONIA_DURABILITY_THREADS=1 IONIA_SHARED_UDP_PORT=0` — should match reverted behavior
7. **Do not** lower `VRClient` quorum as a first resort

---

## 9. References

- Original eRPC model: `deprecated_fasttransport.{cc,h}`
- Execution queue shard: `vr/replica.cc` (`hash(key) % GetNumDurabilityReplicas()`)
- Canonical ports: internal **31880**, client **31850** (CloudLab / GCP harness)
- Reverted working cluster run: `20260623T220117Z` (~250 ops/s, ~1.7 ms median)

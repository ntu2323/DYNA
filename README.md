# DYNA

DYNA is a replication protocol for partitioned key-value stores that orders transactions by client-assigned deadlines. A deployment has N = 2f+1 servers. Each server is the Partition Leader for one partition and a follower for the other 2f.

- Clients multicast each transaction with a deadline timestamp. Servers release transactions in deadline order once their clocks pass it.
- **Fast path:** the transaction commits in one round trip when every involved Partition Leader and at least f+1 replicas reply on time with the same timestamp.
- **Slow path:** otherwise the leaders agree on a timestamp (leader sync), and a primary leader makes the commit durable on a quorum before the transaction is executed.
- **Truly late transactions:** these are moved to a new deadline through a primary-gated deadline upgrade.
- **Leader failures:** these are handled by a view-change recovery protocol.

## Repositories

- **[DynaGo](https://github.com/ntu2323/DynaGo):** the current DYNA implementation, written in Go. Active development happens there.
- **[Dyna_TLA](https://github.com/ntu2323/Dyna_TLA):** the TLA+ specification of DYNA, plus TLC model-checking configurations for its safety properties.
- **This repository:** the original C++ prototype, built on the Ionia codebase. It is no longer the main implementation. The protocol design notes are in [`protocols/src/ionia/dyna/dyna_algo_design.txt`](protocols/src/ionia/dyna/dyna_algo_design.txt).

## C++ prototype

DYNA commands (DYNA lives in `protocols/src/ionia/dyna/`; see its `README.md` for unit tests):

### Build image
```bash
docker build --platform linux/amd64 -f docker/Dockerfile.udp -t ionia-udp .
```

### End-to-end smoke (3 replicas)
```bash
SMOKE_PROTOCOL=dyna ./docker/docker-smoke.sh
```

### Request / reply / quorum debug logging
```bash
SMOKE_PROTOCOL=dyna DYNA_DEBUG=1 ./docker/docker-smoke.sh
```

Changes made so far:
Swapped out fasttransport functionality to use existing udp transport when possible. Swapped out/replaced other erpc related function calls in bench folder.

How to test:
### 1) Build image

```bash
docker build --platform linux/amd64 -f docker/Dockerfile.udp -t ionia-udp .
```

Notes:
- `linux/amd64` is for mac
- first build can take a while because Folly and SplinterDB are compiled inside the image.

### 2) Run UDP smoke test


```bash
chmod +x docker/docker-smoke.sh   # once
./docker/docker-smoke.sh
```

DYNA happy path (same image build as above; bench uses `-m dyna`):

```bash
SMOKE_PROTOCOL=dyna ./docker/docker-smoke.sh
```

The wrapper defaults **`LOW_MEMORY=1`** for VR (single-replica trace). **DYNA cannot use that mode**: commits require an f+1 replica quorum, so the smoke script **automatically forces `LOW_MEMORY=0`** when **`SMOKE_PROTOCOL=dyna`**.
For DYNA smoke, the script also forces **`IONIA_DB_MODE=memory`** to avoid stale shared SplinterDB mount state across local container runs.
DYNA smoke also points bench-client `-c` at the replica config (`sample/config`) so client traffic targets active replica ports.
The Docker wrapper now forwards `DYNA_*` environment variables into the container, so debug/bound overrides work as expected.

Request / reply / quorum debug logging:

```bash
SMOKE_PROTOCOL=dyna DYNA_DEBUG=1 ./docker/docker-smoke.sh
```

Inside an interactive container (`docker run ... ionia-udp`), run `SMOKE_PROTOCOL=dyna ./docker/run_udp_smoke.sh`.

**Without the wrapper** (logs only inside the container unless you mount):

```bash
docker run --rm -it --platform linux/amd64 ionia-udp ./docker/run_udp_smoke.sh
```

### 3) Interactive shell

```bash
docker run --rm -it --platform linux/amd64 ionia-udp
```

Then inside container:

```bash
cd /work/ionia/protocols/src/ionia
./bench/replica -i 0 -j 1 -b 1 -c sample/config -m vr

```

Below is README content from original repo. 

# Ionia

IONIA: High-Performance Replication for key-value stores

This is artifact of `[TDDO: add link to ionia]`

## Requirements

* Multiple (15+) XL170 machines on [cloudlab](https://www.cloudlab.us) (Mellanox MT27710 Family ConnectX-4 Lx/Ubuntu 20.04 LTS)

## Setup

```
Go to setup
Copy the manifest in cloudlab into config.xml
then run 

skyros_upload.py --step=0
...
skyros_upload.py --step=5
```

## Running experiments

Basic experiments for runs are using ycsb
```
./run_everything.sh
```






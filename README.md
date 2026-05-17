DYNA commands:

### Build image
```bash
docker build --platform linux/amd64 -f docker/Dockerfile.udp -t ionia-udp .
```

### Fast path smoke
```bash
SMOKE_PROTOCOL=dyna ./docker/docker-smoke.sh
```

### Slow path forced debug
```bash
SMOKE_PROTOCOL=dyna DYNA_SLOWPATH_DEBUG=1 DYNA_FORCE_SLOWPATH=1 ./docker/docker-smoke.sh
```

### Disk writing
```
SMOKE_PROTOCOL=dyna SMOKE_SHOW_LOGS=0 DYNA_WAL_DIR=/work/ionia/docker/smoke-output/wal ./docker/docker-smoke.sh

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

The wrapper defaults **`LOW_MEMORY=1`** for VR (single-replica trace). **DYNA cannot use that mode**: commits require follower **`SyncAck`**s, so the smoke script **automatically forces `LOW_MEMORY=0`** when **`SMOKE_PROTOCOL=dyna`**.
For DYNA smoke, the script also forces **`IONIA_DB_MODE=memory`** to avoid stale shared SplinterDB mount state across local container runs.
DYNA smoke also points bench-client `-c` at the replica config (`sample/config`) so client traffic targets active replica ports.
The Docker wrapper now forwards `DYNA_*` environment variables into the container, so debug/bound overrides work as expected.

Force slow-path validation:

```bash
SMOKE_PROTOCOL=dyna DYNA_SLOWPATH_DEBUG=1 DYNA_FORCE_SLOWPATH=1 ./docker/docker-smoke.sh
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






# GCP bench cluster (VR & DYNA)

3 receivers + 1+ senders. Commands run from this directory.

**Prereqs:** `pip install -r requirements-deploy.txt`, `secrets/gcp_secrets.py` (GCP service account + GitHub deploy key for [ntu2323/IONIA](https://github.com/ntu2323/IONIA)), firewall **UDP 31880** between VMs.

```bash
export SSH_USER=nathan
export SSH_KEY_FILE=~/.ssh/id_ed25519   # optional
cd Ionia/protocols/experiments/GCP_tests
```

`cluster_hosts.json` and `secrets/` are gitignored.

**Trace:** write-only `trace.micro` (Insert/Update; cycles for `-n`). **Load:** open-loop Poisson by default (Nezha-style, ~`--poisson-rate × 100` offered ops/s per sender). Use `--closed-loop` for legacy send-after-ack behavior.

---

## 1. Create cluster (once)

```bash
pip install -r requirements-deploy.txt
python3 create_cluster.py 4 --hosts-file cluster_hosts.json
```

---

## 2. Setup VMs

Run **one protocol at a time** (same UDP port 31880). First install on all VMs uses full deps; day-to-day use **`--skip-build`** after pushing to GitHub.

### First time (full build)

```bash
# VR
SSH_USER=$SSH_USER python3 setup_paxos_vr_cluster.py --hosts-file cluster_hosts.json

# DYNA (after VR or instead of VR — not in parallel)
SSH_USER=$SSH_USER python3 setup_dyna_cluster.py --hosts-file cluster_hosts.json
```

Optional: `IONIA_GIT_REF=my-branch` for a non-default branch. Local tree without push: `--rsync-from /path/to/Ionia` (skips git clone).

### After code changes (`--skip-build`)

Skips apt, Folly, Splinter/RocksDB bootstrap. Still runs **`git pull`** (or uses rsync tree) and **`make bench/replica bench/client`** so edits like `replica.cc` are recompiled.

```bash
# Push to GitHub first, then:
SSH_USER=nathan python3 setup_paxos_vr_cluster.py --hosts-file cluster_hosts.json --skip-build
SSH_USER=nathan python3 setup_dyna_cluster.py --hosts-file cluster_hosts.json --skip-build

# Or rsync local Ionia without push:
IONIA_REPO_ROOT=/path/to/Ionia SSH_USER=$SSH_USER python3 setup_dyna_cluster.py \
  --hosts-file cluster_hosts.json --skip-build --rsync-from "$IONIA_REPO_ROOT"
```

Logs: `~/paxos_vr_setup.log` / `~/dyna_setup.log` on each VM. Success markers: `~/bench/.setup_paxos_done` / `.setup_dyna_done`.

---

## 3. Run benchmarks (from laptop)

Stop the other protocol’s replicas before switching (VR ↔ DYNA).

### Orchestrated (recommended)

SSH_USER=nathan python3 run_cluster_bench.py \
  --protocol dyna \
  --hosts-file cluster_hosts.json \
  --ssh-user nathan \
  -e 90 \
  -w 10 \
  --interval-ms 1000 \
  --trim-pad-sec 3 \
  -j 1 \
  -n 0 \
  --poisson-rate 40 \
  --replica-wait-sec 8



`run_cluster_bench.py` starts replicas, launches clients in the background, waits, fetches logs. Each run **clears remote bench state by default** (stops old processes, `rm ~/bench/runs/*` on senders, VR `rm /dev/shm/rocks{idx}`, DYNA `rm ~/bench/wal`). Use `--no-clean-remote` only if you need to preserve state for debugging.

```bash
# VR — open-loop default (~6000 offered ops/s with --poisson-rate 60)
SSH_USER=nathan python3 run_cluster_bench.py --protocol vr --hosts-file cluster_hosts.json \
  -e 150 -w 5 --interval-ms 1000 --trim-pad-sec 10 --poisson-rate 70

# DYNA — same defaults
SSH_USER=nathan python3 run_cluster_bench.py --protocol dyna --hosts-file cluster_hosts.json \
  -e 150 -w 5 --interval-ms 1000 --trim-pad-sec 10 --poisson-rate 70

# Tune offered load (rate = mean reqs per 10ms; ~rate×100 ops/s per sender)
SSH_USER=$SSH_USER python3 run_cluster_bench.py --protocol dyna --hosts-file cluster_hosts.json \
  --poisson-rate 100 -e 90 -w 5 --interval-ms 1000

# Legacy closed-loop (send after each completion; -n caps total ops)
SSH_USER=$SSH_USER python3 run_cluster_bench.py --protocol dyna --hosts-file cluster_hosts.json \
  --closed-loop -n 100000 -e 90 -w 5 --interval-ms 1000

# Aggregate
python3 aggregate_bench_results.py results/<run_id>

# Thread sweep
SSH_USER=$SSH_USER python3 run_throughput_sweep.py --protocol dyna --threads 1 4 8 -e 120
```

| Flag | Meaning |
|------|---------|
| `-e` | Measured experiment seconds (after warmup) |
| `-w` | Warmup seconds |
| `-n` | Submission cap (`0` = unlimited; default for open-loop) |
| `--poisson-rate` | Open-loop offered load (default `60`) |
| `--closed-loop` | Disable Poisson; wait for ack before next send |
| `--interval-ms` | Throughput log interval (`bench/client -i`) |
| `--no-clean-remote` | Skip wiping remote runs / RocksDB / WAL (default is clean) |

Results:

```text
results/<run_id>/
  run_meta.json
  sender1/client.log
  summary.json
```

### Manual (SSH)

**Receivers:**

```bash
~/bench/start_paxos_vr_replica.sh 0    # VR on receiver1 (index 0)
~/bench/start_dyna_replica.sh 0        # DYNA
```

**Sender (open-loop example):**

```bash
cd ~/Ionia/protocols/src/ionia
./bench/client -s ~/bench/dynaconfig -c ~/bench/dynaconfig -m dyna \
  -k ~/bench/trace.micro -O -R 60 -n 0 -e 90 -w 5 -t 1 -j 1 -i 1000 -l ~/bench/latencies
```

Helpers: `~/bench/run_paxos_vr_client.sh`, `~/bench/run_dyna_client.sh` (update these if you want open-loop flags baked in).

---

## Reference

| Path | Purpose |
|------|---------|
| `~/bench/vrconfig` / `~/bench/dynaconfig` | Replicas (internal IPs, port 31880) |
| `~/bench/trace.micro` | Shared write-only trace |
| `~/paxos_vr_setup.log` / `~/dyna_setup.log` | Remote setup logs |

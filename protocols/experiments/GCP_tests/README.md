# GCP bench cluster (NOPaxos VR & DYNA)

3 receivers + 1+ senders. Run commands from this directory.

**Prereqs:** `pip install -r requirements-deploy.txt`, copy `secrets/gcp_secrets.py.example` → `secrets/gcp_secrets.py` (GCP service account + GitHub deploy key for `ntu2323/IONIA`), GCP firewall **UDP 31880** between VMs.

```bash
export SSH_USER=nathan          # your VM login user
export SSH_KEY_FILE=~/.ssh/id_ed25519   # optional
cd Ionia/protocols/experiments/GCP_tests
```

`cluster_hosts.json` and `secrets/` are gitignored (see `.gitignore`).

---

## 1. Create cluster (once)

```bash
# Install local Python deps
pip install -r requirements-deploy.txt

# 3 replicas + 1 sender (use 7 for 3 replicas + 4 senders, etc.)
python3 create_cluster.py 4 --hosts-file cluster_hosts.json
```

---

Bench traces are **write-only** (Insert/Update; no reads) in `trace.micro`, shared by VR and DYNA. Both clients **cycle** the trace for `-n` (e.g. 100000).

## 2. Setup VR (all VMs)

```bash
# Git clone + build on every VM (nohup; log ~/paxos_vr_setup.log)
SSH_USER=$SSH_USER python3 setup_paxos_vr_cluster.py --hosts-file cluster_hosts.json

# Optional: rsync local tree instead of git clone
# IONIA_REPO_ROOT=/path/to/Ionia SSH_USER=$SSH_USER python3 setup_paxos_vr_cluster.py \
#   --hosts-file cluster_hosts.json --rsync-from "$IONIA_REPO_ROOT"
```

# Image already has deps: refresh IONIA from GitHub + incremental make (push code first)
SSH_USER=$SSH_USER python3 setup_paxos_vr_cluster.py --hosts-file cluster_hosts.json --skip-build
# Optional branch: IONIA_GIT_REF=my-branch SSH_USER=$SSH_USER python3 setup_paxos_vr_cluster.py ... --skip-build
# Or sync local tree: IONIA_REPO_ROOT=/path/to/Ionia python3 setup_paxos_vr_cluster.py ... --skip-build --rsync-from "$IONIA_REPO_ROOT"

---

## 3. Setup DYNA (all VMs, same cluster OK)

Run after VR setup (or first). Do **not** run both setup scripts at the same time.

```bash
SSH_USER=$SSH_USER python3 setup_dyna_cluster.py --hosts-file cluster_hosts.json
```

SSH_USER=$SSH_USER python3 setup_dyna_cluster.py --hosts-file cluster_hosts.json --skip-build

(Same as VR: `--skip-build` runs `git pull` on `git@github.com:ntu2323/IONIA.git` then incremental `make`; skips apt/folly.)


---

## 4. Run benchmarks (from laptop)

Do **not** run VR and DYNA replicas at the same time (same UDP port 31880). Stop the other protocol before switching.

### Orchestrated (recommended)

Starts replicas on all receivers, waits, launches all senders in parallel, fetches logs, aggregates cluster throughput (trimmed window) + per-sender latency.

```bash
# VR
SSH_USER=$SSH_USER python3 run_cluster_bench.py --protocol vr --hosts-file cluster_hosts.json

# DYNA
SSH_USER=$SSH_USER python3 run_cluster_bench.py --protocol dyna --hosts-file cluster_hosts.json \
  -n 100000 -e 90 -w 5 --interval-ms 1000 --trim-pad-sec 3

# Re-aggregate an existing run
python3 aggregate_bench_results.py results/<run_id>

# Latency vs throughput sweep (vary threads: VR -t, DYNA -j)
SSH_USER=$SSH_USER python3 run_throughput_sweep.py --protocol vr --threads 1 2 4 8
SSH_USER=$SSH_USER python3 run_throughput_sweep.py --protocol dyna --threads 1 4 8 -e 120
```

Results layout:

```text
results/<run_id>/
  run_meta.json       # t0, protocol, flags, hosts
  sender1/client.log  # -i interval lines for throughput
  summary.json        # cluster ops/s + per-sender latency
```

### Manual (SSH per VM)

**Receivers** — start replica `0` / `1` / `2` on receiver1 / receiver2 / receiver3:

```bash
# VR
~/bench/start_paxos_vr_replica.sh 0
# optional: rm -rf /dev/shm/rocks0   # rocks1 / rocks2 on other hosts

# DYNA
pkill -f '[b]ench/replica' 2>/dev/null || true
~/bench/start_dyna_replica.sh 0
tail -f ~/bench/replica-0.log
```

**Sender** — example matched run:

```bash
# VR
cd ~/Ionia/protocols/src/paxos
./bench/client -c ~/bench/vrconfig -m vr -k ~/bench/trace.micro \
  -n 100 -e 30 -w 3 -t 1 -i 1000 -l ~/bench/latencies 2>&1 | tee ~/bench/client-fg.log

# DYNA
cd ~/Ionia/protocols/src/ionia
./bench/client -s ~/bench/dynaconfig -c ~/bench/dynaconfig -m dyna \
  -k ~/bench/trace.micro -n 100 -e 30 -w 3 -t 1 -j 1 -i 1000 \
  -l ~/bench/latencies 2>&1 | tee ~/bench/client-fg.log
```

Helpers: `~/bench/run_paxos_vr_client.sh`, `~/bench/run_dyna_client.sh`.

---

## Reference: files on VMs

| Path | Purpose |
|------|---------|
| `~/bench/vrconfig` / `~/bench/dynaconfig` | Replica addresses (internal IPs, port 31880) |
| `~/bench/trace.micro` | Write-only micro trace (VR & DYNA; cycles for `-n`) |
| `~/bench/.setup_paxos_done` / `.setup_dyna_done` | Remote setup success |
| `~/paxos_vr_setup.log` / `~/dyna_setup.log` | Remote setup build log |

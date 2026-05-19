# GCP bench cluster (NOPaxos VR & DYNA)

Commands for a fresh cluster: 3 receivers + 1+ senders. Run from this directory.

**Prereqs:** `pip install -r requirements-deploy.txt`, `secrets/gcp_secrets.py` (GCP service account + GitHub deploy key for `ntu2323/IONIA`), GCP firewall **UDP 31880** between VMs.

```bash
export SSH_USER=nathan          # your VM login user
export SSH_KEY_FILE=~/.ssh/id_ed25519   # optional
cd Ionia/protocols/experiments/GCP_tests
```

---

## Shared (both protocols)

```bash
# Install local Python deps for create/setup scripts
pip install -r requirements-deploy.txt

# Create VMs (3 replicas + senders); writes cluster_hosts.json
python3 create_cluster.py 4 --hosts-file cluster_hosts.json

```

Do **not** run VR and DYNA replicas at the same time (same UDP port 31880).

---

## Orchestrated benchmark (throughput + latency)

Starts all replicas, waits, launches all senders in parallel, collects logs, aggregates cluster throughput (with trimmed window) and per-sender latency.

```bash
# Single VR run (default: 100k ops, 60s experiment, 5s warmup, 1s -i intervals)
SSH_USER=$SSH_USER python3 run_cluster_bench.py --protocol vr --hosts-file cluster_hosts.json

# DYNA run
SSH_USER=$SSH_USER python3 run_cluster_bench.py --protocol dyna --hosts-file cluster_hosts.json \
  -n 100000 -e 90 -w 5 --interval-ms 1000 --trim-pad-sec 3

# Aggregate (also run automatically printed at end of run_cluster_bench)
python3 aggregate_bench_results.py results/<run_id>

# Latency vs throughput sweep (vary -j / VR -t threads: 1, 2, 4, 8)
SSH_USER=$SSH_USER python3 run_throughput_sweep.py --protocol vr --threads 1 2 4 8
SSH_USER=$SSH_USER python3 run_throughput_sweep.py --protocol dyna --threads 1 4 8 -e 120
```

Results layout:

```text
results/<run_id>/
  run_meta.json       # t0, protocol, flags, hosts
  sender1/client.log  # use -i interval lines for throughput
  summary.json        # cluster ops/s + per-sender latency
```

Throughput uses orchestrator `t0` + `warmup` + `trim_pad` through `experiment` window, summing `Completed N requests at <wall_ms>` across senders. Latency comes from `Median/Average/...` lines in each client log.

---

## NOPaxos VR (`protocols/src/paxos/vr`)

### From your laptop

```bash
# Clone, build paxos bench on all VMs (nohup; logs ~/paxos_vr_setup.log)
SSH_USER=$SSH_USER python3 setup_paxos_vr_cluster.py --hosts-file cluster_hosts.json

```
### On each receiver (replica 0 / 1 / 2)

```bash
# Start VR replica
~/bench/start_paxos_vr_replica.sh 0

# Optional: clear RocksDB state before restart
rm -rf /dev/shm/rocks0   # use rocks1 / rocks2 on other receivers

# Check replica process / logs
ps -ef | grep '[b]ench/replica'
tail -f ~/bench/replica-0.log
```

### On sender

```bash
# Foreground benchmark (example: 100 ops, 30s experiment, 3s warmup)
cd ~/Ionia/protocols/src/paxos
./bench/client -c ~/bench/vrconfig -m vr -k ~/bench/trace.micro \
  -n 100 -e 30 -w 3 -t 1 -l ~/bench/latencies 2>&1 | tee ~/bench/client-fg.log

# Background client via helper script
~/bench/run_paxos_vr_client.sh 1 100 30 3
tail -f ~/bench/client.log

# Verify setup finished
test -f ~/bench/.setup_paxos_done && echo ok
```

---

## DYNA (`protocols/src/ionia`, `ionia -m dyna`)

### From your laptop

```bash
# Clone, build ionia bench on all VMs (nohup; logs ~/dyna_setup.log)
SSH_USER=$SSH_USER python3 setup_dyna_cluster.py --hosts-file cluster_hosts.json

```

### On each receiver (replica 0 / 1 / 2)

```bash
# Stop any prior bench processes
pkill -f 'bench/replica' 2>/dev/null || true

# Start DYNA replica
~/bench/start_dyna_replica.sh 0

# Check replica process / logs
ps -ef | grep '[b]ench/replica'
tail -f ~/bench/replica-0.log
```

### On sender

```bash
# Foreground benchmark (match VR knobs for fair comparison)
cd ~/Ionia/protocols/src/ionia
./bench/client -s ~/bench/dynaconfig -c ~/bench/dynaconfig -m dyna \
  -k ~/bench/trace/run -n 100 -e 30 -w 3 -t 1 -j 1 \
  -l ~/bench/latencies 2>&1 | tee ~/bench/client-fg.log

# Background client via helper script (defaults: 100k ops, 60s — heavier than example above)
~/bench/run_dyna_client.sh 1 1 100 30 3
tail -f ~/bench/client-1.log

# Latency summary from log
grep -E 'Median|Average|90th|95th|99th' ~/bench/client-fg.log

# Verify setup finished
test -f ~/bench/.setup_dyna_done && echo ok
```

### If link failed with `-liberty` (manual fix on VM)

```bash
sudo apt-get install -y libiberty-dev libelf-dev libjemalloc-dev
cd ~/Ionia/protocols/src/ionia && make bench/replica bench/client -j"$(nproc)"
touch ~/bench/.setup_dyna_done
```

---

## Files on VMs

| Path | Purpose |
|------|---------|
| `~/bench/vrconfig` / `~/bench/dynaconfig` | Replica addresses (internal IPs, port 31880) |
| `~/bench/trace.micro` | VR trace |
| `~/bench/trace/run1` | DYNA trace |
| `~/bench/.setup_paxos_done` / `.setup_dyna_done` | Remote setup success marker |
| `~/paxos_vr_setup.log` / `~/dyna_setup.log` | Remote setup build log |

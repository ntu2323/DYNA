#!/usr/bin/env python3
"""
Start replicas on all receivers, then start bench clients on all senders in parallel.

Writes run_meta.json and pulls per-sender client logs for aggregate_bench_results.py.

Example:
  SSH_USER=nathan python3 run_cluster_bench.py --protocol vr --hosts-file cluster_hosts.json
  SSH_USER=nathan python3 run_cluster_bench.py --protocol dyna -n 100000 -e 120 -w 5 --interval-ms 1000
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from gcp_bench_common import (
    PKILL_CLIENT,
    PKILL_REPLICA,
    fetch_remote_file,
    load_hosts_file,
    receiver_index,
    remote_receiver_cleanup_cmd,
    remote_sender_cleanup_cmd,
    sender_client_id,
    ssh_run,
    wait_ionia_replica_ready,
)

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# IONIA bench/replica -j (durability threads). Override: IONIA_DURABILITY_THREADS=1 or
# run_cluster_bench.py --ionia-durability-threads N
IONIA_DEFAULT_DURABILITY_THREADS = int(os.environ.get("IONIA_DURABILITY_THREADS", "15"))

PROTOCOLS = {
    "vr": {
        "replica_start": "~/bench/start_paxos_vr_replica.sh {idx}",
        "client_cwd": "${HOME}/Ionia/protocols/src/paxos",
        "client_bin": "./bench/client",
    },
    "dyna": {
        "replica_start": "~/bench/start_dyna_replica.sh {idx}",
        "client_cwd": "${HOME}/Ionia/protocols/src/ionia",
        "client_bin": "./bench/client",
    },
    # IONIA = ionia/vr (VRReplica + VRDurabilityReplica), distinct from the "vr"
    # baseline which is the paxos/src NOPaxos VR. Same ionia binary as dyna, but
    # launched with -m vr and a two-config setup (internal + .client).
    "ionia": {
        "replica_start": "~/bench/start_ionia_replica.sh {idx}",
        "client_cwd": "${HOME}/Ionia/protocols/src/ionia",
        "client_bin": "./bench/client",
    },
}


def _remote_run_dir(run_id: str) -> str:
    """Path for remote bash ($HOME expanded by the shell)."""
    return f"$HOME/bench/runs/{run_id}"


def _remote_run_dir_scp(run_id: str) -> str:
    """Path for scp (~ expanded by sshd on the remote)."""
    return f"~/bench/runs/{run_id}"


def _bash_word(word: str) -> str:
    """Bash word for bash -lc: allow $HOME expansion; quote other values safely."""
    if "$HOME" in word:
        return word
    return shlex.quote(word)


def _ionia_replica_ready_timeout_sec(replica_wait_sec: int, durability_threads: int) -> int:
    """Allow time for -j N durability thread startup on each receiver."""
    return max(replica_wait_sec, 10 + durability_threads * 2)


def _wait_ionia_replicas_ready(
    receiver_roles: List[str],
    hosts: Dict[str, str],
    ssh_user: str,
    ssh_key: Optional[str],
    durability_threads: int,
    timeout_sec: int,
) -> None:
    print(
        f"Waiting up to {timeout_sec}s for IONIA_REPLICA_READY "
        f"(durability_threads={durability_threads}) on {len(receiver_roles)} receivers..."
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(receiver_roles)) as ex:
        futs = [
            ex.submit(
                wait_ionia_replica_ready,
                r,
                hosts[r],
                receiver_index(r),
                durability_threads,
                ssh_user,
                ssh_key,
                timeout_sec,
            )
            for r in receiver_roles
        ]
        for fut in concurrent.futures.as_completed(futs):
            role, ok, err = fut.result()
            if not ok:
                sys.exit(f"IONIA replica not ready on {role}: {err}")
            print(f"  [{role}] IONIA_REPLICA_READY")


def _offered_ops_per_sender(offered_ops_per_sec: float, poisson_rate: int) -> float:
    if offered_ops_per_sec > 0.0:
        return offered_ops_per_sec
    return float(poisson_rate * 100)


def _auto_max_inflight_cap(
    offered_ops_per_sec: float,
    poisson_rate: int,
    *,
    expected_rtt_sec: float,
    inflight_factor: float,
    floor: int,
) -> int:
    """Pipeline depth for open-loop: offered * RTT * factor (Little's law)."""
    offered = _offered_ops_per_sender(offered_ops_per_sec, poisson_rate)
    computed = int(offered * expected_rtt_sec * inflight_factor + 0.999)
    return max(floor, computed)


def _build_client_cmd(
    protocol: str,
    run_id: str,
    *,
    num_ops: int,
    experiment_sec: int,
    warmup_sec: int,
    interval_ms: int,
    threads: int,
    machine_id: int,
    open_loop: bool,
    poisson_rate: int,
    offered_ops_per_sec: float,
    max_inflight: int,
    ionia_fast_quorum: bool = False,
    ionia_client_timing: bool = False,
) -> str:
    run_dir = _remote_run_dir(run_id)
    cfg = PROTOCOLS[protocol]
    if protocol == "vr":
        args = [
            "-c", "$HOME/bench/vrconfig",
            "-m", "vr",
            "-k", "$HOME/bench/trace.micro",
            "-n", str(num_ops),
            "-e", str(experiment_sec),
            "-w", str(warmup_sec),
            "-t", str(threads),
            "-i", str(interval_ms),
            "-l", f"{run_dir}/latencies",
        ]
        if open_loop:
            args.append("-O")
            if offered_ops_per_sec > 0.0:
                args.extend(["-G", repr(offered_ops_per_sec)])
            else:
                args.extend(["-R", str(poisson_rate)])
            if max_inflight > 0:
                args.extend(["-F", str(max_inflight)])
    elif protocol == "ionia":
        # ionia/vr client: -c is the client-facing (durability) config, -s the
        # internal consensus config. -t is the client/machine id, -j is threads
        # (same ionia binary semantics as dyna; the "vr" baseline binary differs).
        args = [
            "-c", "$HOME/bench/ioniaconfig.client",
            "-s", "$HOME/bench/ioniaconfig",
            "-m", "vr",
            "-k", "$HOME/bench/trace.micro",
            "-n", str(num_ops),
            "-e", str(experiment_sec),
            "-w", str(warmup_sec),
            "-t", str(machine_id),
            "-j", str(threads),
            "-i", str(interval_ms),
            "-l", f"{run_dir}/latencies",
        ]
        if open_loop:
            args.append("-O")
            if offered_ops_per_sec > 0.0:
                args.extend(["-G", repr(offered_ops_per_sec)])
            else:
                args.extend(["-R", str(poisson_rate)])
            if max_inflight > 0:
                args.extend(["-F", str(max_inflight)])
    else:
        # dyna: single dynaconfig (client and replica), shared trace.
        args = [
            "-s", "$HOME/bench/dynaconfig",
            "-c", "$HOME/bench/dynaconfig",
            "-m", protocol,
            "-k", "$HOME/bench/trace.micro",
            "-n", str(num_ops),
            "-e", str(experiment_sec),
            "-w", str(warmup_sec),
            "-t", str(machine_id),
            "-j", str(threads),
            "-i", str(interval_ms),
            "-l", f"{run_dir}/latencies",
        ]
        if open_loop:
            args.append("-O")
            if offered_ops_per_sec > 0.0:
                args.extend(["-G", repr(offered_ops_per_sec)])
            else:
                args.extend(["-R", str(poisson_rate)])
            if max_inflight > 0:
                args.extend(["-F", str(max_inflight)])
    quoted = " ".join(_bash_word(a) for a in args)
    dyna_env = ""
    if protocol == "dyna":
        # The client reads the deadline vars (dyna/deadline.h) plus its resend timeout.
        # BENCH_INFLIGHT_LOG surfaces inflight/submitted on throughput lines.
        # Bound knobs pass through from the LOCAL environment (they're evaluated in
        # the remote shell otherwise, which never sees the local export). Lets you
        # A/B the deadline hold: e.g. DYNA_MAX_OWD_US=40 python3 run_cluster_bench.py …
        # The deadline-hold bound is the dominant client-visible latency term. It is
        # sized to the real one-way delay: measured OWD on the CloudLab cluster is
        # ~40µs, so DYNA_MAX_OWD_US=40 caps the (noisy, sometimes-inflated) OWD
        # estimate at the true wire delay. These are cluster-calibrated defaults; on a
        # slower network raise them (bound ≈ p50 one-way delay). Override via local env.
        _init_bound = os.environ.get("DYNA_INITIAL_BOUND_US", "30")
        _max_owd = os.environ.get("DYNA_MAX_OWD_US", "40")
        # Client transport mode, baked from the LOCAL env (a remote ${VAR:-1} would not
        # see the local export). Default 1 = busy-poll (lowest single-thread latency).
        # Set DYNA_BUSY_POLL=0 to make the CLIENT epoll-block: many client threads
        # (-j N) then PARK when idle instead of each spinning a core, so multi-thread
        # load scales without CPU / NIC-RX-softirq starvation (which collapses busy-poll
        # at high -j). This affects clients only; replicas keep their own default.
        _client_busy = os.environ.get("DYNA_BUSY_POLL", "1")
        dyna_env = (
            f"export DYNA_INITIAL_BOUND_US={_init_bound}; "
            f"export DYNA_MAX_OWD_US={_max_owd}; "
            f"export DYNA_BUSY_POLL={_client_busy}; "
            "export DYNA_RESEND_TIMEOUT_MS=${DYNA_RESEND_TIMEOUT_MS:-30}; "
            "export BENCH_INFLIGHT_LOG=${BENCH_INFLIGHT_LOG:-1}; "
        )
        # Slow-path trigger (all leaders replied, quorum short): forward if set locally.
        if "DYNA_SLOW_TIMEOUT_US" in os.environ:
            dyna_env += f"export DYNA_SLOW_TIMEOUT_US={shlex.quote(os.environ['DYNA_SLOW_TIMEOUT_US'])}; "
    ionia_env = ""
    if protocol == "ionia" and ionia_fast_quorum:
        ionia_env = "export IONIA_FAST_QUORUM=1; "
    if protocol == "ionia" and ionia_client_timing:
        ionia_env += "export IONIA_CLIENT_TIMING=1; "
    # Background subshell: SSH returns immediately; client writes done when finished.
    client_run = (
        f"{dyna_env}{ionia_env}{cfg['client_bin']} {quoted} "
        f"> {run_dir}/client.log 2>&1; "
        f"echo $? > {run_dir}/exit_code; "
        f"touch {run_dir}/done"
    )
    return (
        f"mkdir -p {run_dir} && rm -f {run_dir}/done {run_dir}/exit_code && "
        f"cd {cfg['client_cwd']} && "
        f"nohup bash -c {shlex.quote(client_run)} >/dev/null 2>&1 &"
    )


# Replica-side DYNA knobs forwarded from the LOCAL env when set (a remote ${VAR:-…}
# would not see the local export); unset knobs fall back to start_dyna_replica.sh.
_DYNA_REPLICA_ENV_KNOBS = (
    "DYNA_INITIAL_BOUND_US",
    "DYNA_MAX_OWD_US",
    "DYNA_OWD_PERCENTILE",
    "DYNA_OWD_WINDOW_LEN",
    "DYNA_BUSY_POLL",
    "DYNA_TS_SCAN_MS",
    "DYNA_TS_TIMEOUT_MS",
    "DYNA_SIM_DISK_LATENCY_US",
    "DYNA_DEBUG",
)


def _dyna_replica_env_prefix() -> str:
    parts = [
        f"export {k}={shlex.quote(os.environ[k])}"
        for k in _DYNA_REPLICA_ENV_KNOBS
        if k in os.environ
    ]
    return "; ".join(parts) + "; " if parts else ""


def _ionia_replica_env_prefix(
    durability_threads: int,
    *,
    per_thread_socket: bool = False,
    leader_stats: bool = False,
    batch_size: int = 1,
) -> str:
    parts = [f"export IONIA_DURABILITY_THREADS={durability_threads}"]
    parts.append(f"export IONIA_BATCH_SIZE={int(batch_size)}")
    if per_thread_socket:
        parts.append("export IONIA_PER_THREAD_SOCKET=1")
    if leader_stats:
        parts.append("export IONIA_LEADER_STATS=1")
    if durability_threads > 1:
        parts.append("ulimit -n 65536 2>/dev/null || true")
    return "; ".join(parts) + "; "


def _start_replica(
    role: str,
    host: str,
    protocol: str,
    ssh_user: str,
    ssh_key: Optional[str],
    *,
    clean_remote: bool,
    ionia_durability_threads: int,
    ionia_per_thread_socket: bool = False,
    ionia_leader_stats: bool = False,
    ionia_batch_size: int = 1,
) -> tuple[str, bool, str]:
    idx = receiver_index(role)
    cfg = PROTOCOLS[protocol]
    parts = []
    if clean_remote:
        parts.append(remote_receiver_cleanup_cmd(protocol, idx))
    else:
        parts.extend([PKILL_REPLICA, "sleep 1"])
    replica_start = cfg["replica_start"].format(idx=idx)
    if protocol == "dyna":
        parts.append(_dyna_replica_env_prefix() + replica_start)
    elif protocol == "ionia":
        # Force the consensus batch size (bench/replica -b) in the remote helper.
        # Matches both the legacy literal (`-b 1`) and the env form (`-b "${BATCH_SIZE}"`),
        # so no re-deploy of start_ionia_replica.sh is required.
        parts.append(
            f"sed -i -E 's#-b [^ ]+#-b {int(ionia_batch_size)}#' "
            "~/bench/start_ionia_replica.sh 2>/dev/null || true"
        )
        parts.append(
            _ionia_replica_env_prefix(
                ionia_durability_threads,
                per_thread_socket=ionia_per_thread_socket,
                leader_stats=ionia_leader_stats,
                batch_size=ionia_batch_size,
            )
            + replica_start
        )
    else:
        parts.append(replica_start)
    cmd = "; ".join(parts)
    r = ssh_run(host, ssh_user, ssh_key, cmd)
    ok = r.returncode == 0
    err = _ssh_error_message(r)
    return role, ok, err


def _ssh_error_message(r: subprocess.CompletedProcess) -> str:
    parts = []
    if (r.stderr or "").strip():
        parts.append(r.stderr.strip())
    if (r.stdout or "").strip():
        parts.append(r.stdout.strip())
    if not parts:
        parts.append(f"ssh exit {r.returncode}")
    return ": ".join(parts)


def _start_client(
    role: str,
    host: str,
    protocol: str,
    run_id: str,
    ssh_user: str,
    ssh_key: Optional[str],
    client_args: dict,
    *,
    clean_remote: bool,
) -> tuple[str, bool, str]:
    mid = sender_client_id(role)
    # Separate SSH: pkill must not share a command line with ./bench/client.
    if clean_remote:
        r0 = ssh_run(host, ssh_user, ssh_key, remote_sender_cleanup_cmd())
    else:
        r0 = ssh_run(host, ssh_user, ssh_key, f"{PKILL_CLIENT}; sleep 1")
    if r0.returncode != 0:
        return role, False, f"stop old client: {_ssh_error_message(r0)}"

    inner = _build_client_cmd(protocol, run_id, machine_id=mid, **client_args)
    cmd = f"bash -lc {shlex.quote(inner)}"
    r = ssh_run(host, ssh_user, ssh_key, cmd)
    ok = r.returncode == 0
    err = _ssh_error_message(r)
    return role, ok, err


def _wait_client(
    role: str,
    host: str,
    run_id: str,
    ssh_user: str,
    ssh_key: Optional[str],
    timeout_sec: int,
) -> tuple[str, bool, str]:
    run_dir = _remote_run_dir(run_id)
    # Avoid pgrep in this ssh -c string (would match bench/client in argv). Use done/exit_code only.
    cmd = (
        f"for i in $(seq 1 {timeout_sec}); do "
        f"test -f {run_dir}/done && exit 0; "
        f"sleep 1; done; exit 1"
    )
    r = ssh_run(host, ssh_user, ssh_key, cmd)
    ok = r.returncode == 0
    err = (r.stderr or r.stdout or "").strip() or f"ssh exit {r.returncode}"
    return role, ok, err


def _fetch_logs(
    role: str,
    host: str,
    run_id: str,
    local_run_dir: str,
    ssh_user: str,
    ssh_key: Optional[str],
) -> tuple[str, bool, str]:
    run_dir = _remote_run_dir_scp(run_id)
    local_sender = os.path.join(local_run_dir, role)
    os.makedirs(local_sender, exist_ok=True)
    try:
        fetch_remote_file(
            host,
            ssh_user,
            ssh_key,
            f"{run_dir}/client.log",
            os.path.join(local_sender, "client.log"),
        )
        for name in ("latencies", "latencies.raw"):
            remote = f"{run_dir}/{name}"
            r = ssh_run(host, ssh_user, ssh_key, f"test -f {remote} && echo yes")
            if "yes" in (r.stdout or ""):
                fetch_remote_file(
                    host,
                    ssh_user,
                    ssh_key,
                    remote,
                    os.path.join(local_sender, name),
                    binary=(name == "latencies"),
                )
        return role, True, ""
    except subprocess.CalledProcessError as e:
        return role, False, str(e)


def _fetch_replica_log(
    role: str,
    host: str,
    run_id: str,
    local_run_dir: str,
    ssh_user: str,
    ssh_key: Optional[str],
) -> tuple[str, bool, str]:
    """Pull ~/bench/replica-<idx>.log from a receiver (best-effort, for debugging)."""
    idx = receiver_index(role)
    local_dir = os.path.join(local_run_dir, role)
    os.makedirs(local_dir, exist_ok=True)
    remote = f"~/bench/replica-{idx}.log"
    r = ssh_run(host, ssh_user, ssh_key, f"test -f {remote} && echo yes")
    if "yes" not in (r.stdout or ""):
        return role, False, f"no {remote}"
    try:
        fetch_remote_file(
            host,
            ssh_user,
            ssh_key,
            remote,
            os.path.join(local_dir, f"replica-{idx}.log"),
        )
        return role, True, ""
    except subprocess.CalledProcessError as e:
        return role, False, str(e)


def main() -> None:
    ap = argparse.ArgumentParser(description="Orchestrate GCP VR/DYNA/IONIA cluster benchmark run.")
    ap.add_argument("--hosts-file", default=os.path.join(_SCRIPT_DIR, "cluster_hosts.json"))
    ap.add_argument("--protocol", choices=sorted(PROTOCOLS), required=True)
    ap.add_argument("--ssh-user", default=os.environ.get("SSH_USER", ""))
    ap.add_argument("--ssh-key", default=os.environ.get("SSH_KEY_FILE", ""))
    ap.add_argument("--run-id", default="", help="Run id (default: UTC timestamp).")
    ap.add_argument("--results-dir", default="", help="Local dir (default: results/<run_id>).")
    ap.add_argument(
        "-n",
        "--num-ops",
        type=int,
        default=0,
        help="Max submissions (0 = unlimited). Open-loop default uses 0; use with --closed-loop to cap.",
    )
    ap.add_argument("-e", "--experiment-sec", type=int, default=60)
    ap.add_argument("-w", "--warmup-sec", type=int, default=5)
    ap.add_argument("--interval-ms", type=int, default=1000, help="bench/client -i (throughput samples).")
    ap.add_argument("--trim-pad-sec", type=float, default=3.0, help="Stored in meta for aggregator.")
    ap.add_argument("-j", "--threads", type=int, default=1, help="Client threads per sender (bench/client -j).")
    ap.add_argument(
        "--ionia-durability-threads",
        type=int,
        default=IONIA_DEFAULT_DURABILITY_THREADS,
        help="IONIA only: bench/replica -j durability threads per replica (default 15).",
    )
    ap.add_argument(
        "--ionia-batch-size",
        type=int,
        default=1,
        help="IONIA only: bench/replica -b consensus batch size (default 1 = no "
        "batching). The single ordering thread commits up to this many ops per "
        "Prepare round; raise it (e.g. 64/128) to lift the write-throughput ceiling.",
    )
    ap.add_argument(
        "--ionia-fast-quorum",
        action="store_true",
        help="IONIA only: IONIA_FAST_QUORUM=1 on clients (quorum=1, send writes to leader only).",
    )
    ap.add_argument(
        "--ionia-per-thread-socket",
        action="store_true",
        help="IONIA only: IONIA_PER_THREAD_SOCKET=1 on replicas (each durability "
        "thread gets its own SO_REUSEPORT socket + event loop instead of one "
        "shared socket pumped by a single thread).",
    )
    ap.add_argument(
        "--ionia-leader-stats",
        action="store_true",
        help="IONIA only: IONIA_LEADER_STATS=1 on replicas (log per-OS-thread "
        "inbound request rate once per second to reveal pump-thread saturation).",
    )
    ap.add_argument(
        "--ionia-client-timing",
        action="store_true",
        help="IONIA only: IONIA_CLIENT_TIMING=1 on clients (log per-2s latency "
        "split into leader-ack, quorum, and follower-tail components to reveal "
        "whether normal quorum is leader- or tail-bound).",
    )
    ap.add_argument(
        "--closed-loop",
        action="store_true",
        help="Closed-loop: send next request only after each completion (legacy bench).",
    )
    ap.add_argument(
        "--poisson-rate",
        type=int,
        default=60,
        help="Open-loop: mean requests per 10ms per sender (~rate*100 offered ops/s). Ignored if --offered-ops-per-sec set.",
    )
    ap.add_argument(
        "--offered-ops-per-sec",
        type=float,
        default=0.0,
        help="Open-loop: target offered ops/s per sender (-G; use for WAN instead of --poisson-rate).",
    )
    ap.add_argument(
        "--max-inflight",
        type=int,
        default=0,
        help="Open-loop: cap outstanding requests per sender (0 = unlimited). Backpressure for WAN.",
    )
    ap.add_argument(
        "--wan-open-loop",
        action="store_true",
        help="Open-loop WAN preset: -G 10 ops/s per sender, -F 500 max inflight (override with flags).",
    )
    ap.add_argument(
        "--auto-max-inflight",
        action="store_true",
        help="Set -F from offered load and --expected-rtt-sec (pipeline depth for high-throughput latency).",
    )
    ap.add_argument(
        "--expected-rtt-sec",
        type=float,
        default=0.35,
        help="Planning RTT (seconds) for --auto-max-inflight (WAN ~0.33; same-region ~0.0004–0.001).",
    )
    ap.add_argument(
        "--inflight-factor",
        type=float,
        default=4.0,
        help="max_inflight = offered_ops/s * expected_rtt_sec * factor (with --auto-max-inflight).",
    )
    ap.add_argument(
        "--high-throughput-latency",
        action="store_true",
        help="Shorthand: open-loop latency under load; enables --auto-max-inflight (set offered rate explicitly).",
    )
    ap.add_argument(
        "--replica-wait-sec",
        type=int,
        default=20,
        help="Non-IONIA: fixed sleep after replica start. IONIA: max wait for "
        "IONIA_REPLICA_READY (actual timeout is max(this, 10 + 2*durability_threads)).",
    )
    ap.add_argument("--client-timeout-sec", type=int, default=7200)
    ap.add_argument(
        "--no-clean-remote",
        action="store_true",
        help="Do not wipe remote bench state (default: pkill, rm runs/*, VR rocks).",
    )
    ap.add_argument("--skip-replicas", action="store_true")
    ap.add_argument("--skip-clients", action="store_true", help="Only (re)start replicas.")
    args = ap.parse_args()

    if not args.ssh_user:
        sys.exit("Set --ssh-user or SSH_USER")
    if args.ionia_fast_quorum and args.protocol != "ionia":
        sys.exit("--ionia-fast-quorum applies only to --protocol ionia")
    if args.ionia_per_thread_socket and args.protocol != "ionia":
        sys.exit("--ionia-per-thread-socket applies only to --protocol ionia")
    if args.ionia_leader_stats and args.protocol != "ionia":
        sys.exit("--ionia-leader-stats applies only to --protocol ionia")
    if args.ionia_client_timing and args.protocol != "ionia":
        sys.exit("--ionia-client-timing applies only to --protocol ionia")
    if args.wan_open_loop and args.closed_loop:
        sys.exit("--wan-open-loop requires open-loop (omit --closed-loop)")
    if args.high_throughput_latency and args.closed_loop:
        sys.exit("--high-throughput-latency requires open-loop (omit --closed-loop)")

    offered_ops_per_sec = args.offered_ops_per_sec
    max_inflight = args.max_inflight
    auto_max_inflight = args.auto_max_inflight or args.high_throughput_latency
    if args.wan_open_loop:
        if offered_ops_per_sec <= 0.0:
            offered_ops_per_sec = 10.0
        if max_inflight <= 0 and not auto_max_inflight:
            max_inflight = 500

    if auto_max_inflight and not args.closed_loop:
        computed = _auto_max_inflight_cap(
            offered_ops_per_sec,
            args.poisson_rate,
            expected_rtt_sec=args.expected_rtt_sec,
            inflight_factor=args.inflight_factor,
            floor=64,
        )
        max_inflight = max(max_inflight, computed)

    hosts_path = args.hosts_file
    if not os.path.isabs(hosts_path):
        hosts_path = os.path.join(_SCRIPT_DIR, hosts_path)
    hosts, _internal = load_hosts_file(hosts_path)

    receiver_roles = sorted(k for k in hosts if k.startswith("receiver"))
    sender_roles = sorted(k for k in hosts if k.startswith("sender"))

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    local_run_dir = args.results_dir or os.path.join(_SCRIPT_DIR, "results", run_id)
    os.makedirs(local_run_dir, exist_ok=True)

    ssh_key = args.ssh_key or None
    clean_remote = not args.no_clean_remote
    open_loop = not args.closed_loop
    if open_loop:
        num_ops = args.num_ops
    else:
        num_ops = args.num_ops if args.num_ops > 0 else 100_000
    client_args = {
        "num_ops": num_ops,
        "experiment_sec": args.experiment_sec,
        "warmup_sec": args.warmup_sec,
        "interval_ms": args.interval_ms,
        "threads": args.threads,
        "open_loop": open_loop,
        "poisson_rate": args.poisson_rate,
        "offered_ops_per_sec": offered_ops_per_sec,
        "max_inflight": max_inflight,
        "ionia_fast_quorum": args.ionia_fast_quorum if args.protocol == "ionia" else False,
        "ionia_client_timing": args.ionia_client_timing if args.protocol == "ionia" else False,
    }

    senders_meta = [
        {
            "role": role,
            "host": hosts[role],
            "client_id": sender_client_id(role),
            "threads": args.threads,
        }
        for role in sender_roles
    ]

    meta: Dict = {
        "run_id": run_id,
        "protocol": args.protocol,
        "hosts_file": hosts_path,
        "num_ops": num_ops,
        "open_loop": open_loop,
        "poisson_rate": args.poisson_rate if open_loop else None,
        "offered_ops_per_sec_per_sender": offered_ops_per_sec if open_loop else None,
        "max_inflight_per_sender": max_inflight if open_loop else None,
        "auto_max_inflight": auto_max_inflight if open_loop else None,
        "expected_rtt_sec": args.expected_rtt_sec if open_loop and auto_max_inflight else None,
        "inflight_factor": args.inflight_factor if open_loop and auto_max_inflight else None,
        "high_throughput_latency": args.high_throughput_latency if open_loop else None,
        "wan_open_loop": args.wan_open_loop if open_loop else None,
        "offered_ops_per_sec": (
            offered_ops_per_sec
            if open_loop and offered_ops_per_sec > 0.0
            else ((args.poisson_rate * 100) if open_loop else None)
        ),
        "experiment_sec": args.experiment_sec,
        "warmup_sec": args.warmup_sec,
        "interval_ms": args.interval_ms,
        "trim_pad_sec": args.trim_pad_sec,
        "clean_remote": clean_remote,
        "threads": args.threads,
        "ionia_durability_threads": (
            args.ionia_durability_threads if args.protocol == "ionia" else None
        ),
        "ionia_batch_size": (
            args.ionia_batch_size if args.protocol == "ionia" else None
        ),
        "ionia_fast_quorum": (
            args.ionia_fast_quorum if args.protocol == "ionia" else None
        ),
        "ionia_per_thread_socket": (
            args.ionia_per_thread_socket if args.protocol == "ionia" else None
        ),
        "ionia_leader_stats": (
            args.ionia_leader_stats if args.protocol == "ionia" else None
        ),
        "ionia_client_timing": (
            args.ionia_client_timing if args.protocol == "ionia" else None
        ),
        "replica_wait_sec": args.replica_wait_sec,
        "ionia_replica_ready_timeout_sec": (
            _ionia_replica_ready_timeout_sec(
                args.replica_wait_sec, args.ionia_durability_threads
            )
            if args.protocol == "ionia"
            else None
        ),
        "receivers": [{"role": r, "host": hosts[r], "index": receiver_index(r)} for r in receiver_roles],
        "senders": senders_meta,
        "t0_unix": None,
        "t0_iso": None,
    }

    if not args.skip_replicas:
        if clean_remote:
            print(f"Cleaning remote replica state ({args.protocol})...")
        if args.protocol == "ionia":
            print(
                f"IONIA durability threads per replica: -j {args.ionia_durability_threads}"
            )
            print(f"IONIA consensus batch size: -b {args.ionia_batch_size}")
            if args.ionia_batch_size <= 1:
                print(
                    "  (note: -b 1 = no batching; single ordering thread commits one "
                    "op per consensus round and back-pressures the fast path)"
                )
            if args.ionia_fast_quorum:
                print(
                    "IONIA fast quorum: on (client quorum=1, writes to leader durability only)"
                )
            if args.ionia_per_thread_socket:
                print(
                    "IONIA per-thread socket: on (SO_REUSEPORT; each durability "
                    "thread receives + processes independently)"
                )
            if args.ionia_leader_stats:
                print(
                    "IONIA leader stats: on (per-OS-thread inbound rate logged to "
                    "replica logs once per second)"
                )
            if args.ionia_client_timing:
                print(
                    "IONIA client timing: on (per-2s leader/quorum/tail latency "
                    "split logged to client logs)"
                )
        print(f"Starting {args.protocol} replicas on {len(receiver_roles)} hosts...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(receiver_roles)) as ex:
            futs = [
                ex.submit(
                    _start_replica,
                    r,
                    hosts[r],
                    args.protocol,
                    args.ssh_user,
                    ssh_key,
                    clean_remote=clean_remote,
                    ionia_durability_threads=args.ionia_durability_threads,
                    ionia_per_thread_socket=args.ionia_per_thread_socket,
                    ionia_leader_stats=args.ionia_leader_stats,
                    ionia_batch_size=args.ionia_batch_size,
                )
                for r in receiver_roles
            ]
            for fut in concurrent.futures.as_completed(futs):
                role, ok, err = fut.result()
                if not ok:
                    sys.exit(f"Replica start failed on {role}: {err}")
                print(f"  [{role}] replica started")

        if args.protocol == "ionia":
            ready_timeout = _ionia_replica_ready_timeout_sec(
                args.replica_wait_sec, args.ionia_durability_threads
            )
            _wait_ionia_replicas_ready(
                receiver_roles,
                hosts,
                args.ssh_user,
                ssh_key,
                args.ionia_durability_threads,
                ready_timeout,
            )
        else:
            print(f"Waiting {args.replica_wait_sec}s for replicas...")
            time.sleep(args.replica_wait_sec)

    if args.skip_clients:
        meta_path = os.path.join(local_run_dir, "run_meta.json")
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)
        print(f"Wrote {meta_path} (clients skipped)")
        return

    if clean_remote:
        print("Cleaning remote sender run dirs...")
    if open_loop:
        effective_offered = _offered_ops_per_sender(offered_ops_per_sec, args.poisson_rate)
        if offered_ops_per_sec > 0.0:
            print(
                f"Open-loop load: {offered_ops_per_sec:g} offered ops/s per sender "
                f"(max_inflight={max_inflight or 'unlimited'})"
            )
        else:
            print(
                f"Open-loop load: poisson-rate={args.poisson_rate} "
                f"(~{effective_offered:g} offered ops/s per sender, "
                f"max_inflight={max_inflight or 'unlimited'})"
            )
        if auto_max_inflight:
            print(
                f"  auto max_inflight: offered({effective_offered:g}) * "
                f"rtt({args.expected_rtt_sec}s) * factor({args.inflight_factor}) "
                f"→ {max_inflight}"
            )
    print(f"Starting clients on {len(sender_roles)} senders (run_id={run_id})...")
    t0 = time.time()
    meta["t0_unix"] = t0
    meta["t0_iso"] = datetime.fromtimestamp(t0, tz=timezone.utc).isoformat()

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(sender_roles)) as ex:
        futs = [
            ex.submit(
                _start_client,
                r,
                hosts[r],
                args.protocol,
                run_id,
                args.ssh_user,
                ssh_key,
                client_args,
                clean_remote=clean_remote,
            )
            for r in sender_roles
        ]
        for fut in concurrent.futures.as_completed(futs):
            role, ok, err = fut.result()
            if not ok:
                sys.exit(f"Client launch failed on {role}: {err}")
            print(f"  [{role}] client launched")

    print("Waiting for clients to finish...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(sender_roles)) as ex:
        futs = [
            ex.submit(
                _wait_client,
                r,
                hosts[r],
                run_id,
                args.ssh_user,
                ssh_key,
                args.client_timeout_sec,
            )
            for r in sender_roles
        ]
        for fut in concurrent.futures.as_completed(futs):
            role, ok, err = fut.result()
            if not ok:
                print(f"  [{role}] WARNING: wait timed out or failed: {err}", file=sys.stderr)
            else:
                print(f"  [{role}] client done")

    print("Fetching logs...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(sender_roles)) as ex:
        futs = [
            ex.submit(
                _fetch_logs,
                r,
                hosts[r],
                run_id,
                local_run_dir,
                args.ssh_user,
                ssh_key,
            )
            for r in sender_roles
        ]
        for fut in concurrent.futures.as_completed(futs):
            role, ok, err = fut.result()
            if not ok:
                print(f"  [{role}] WARNING: fetch failed: {err}", file=sys.stderr)
            else:
                print(f"  [{role}] logs saved")

    # Best-effort: pull replica logs too, so server-side crashes are visible locally.
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(receiver_roles))) as ex:
        futs = [
            ex.submit(
                _fetch_replica_log,
                r,
                hosts[r],
                run_id,
                local_run_dir,
                args.ssh_user,
                ssh_key,
            )
            for r in receiver_roles
        ]
        for fut in concurrent.futures.as_completed(futs):
            role, ok, err = fut.result()
            if ok:
                print(f"  [{role}] replica log saved")
            else:
                print(f"  [{role}] no replica log ({err})", file=sys.stderr)

    meta_path = os.path.join(local_run_dir, "run_meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print(f"\nRun complete. Local results: {local_run_dir}")
    print(f"  python3 aggregate_bench_results.py {local_run_dir}")


if __name__ == "__main__":
    main()

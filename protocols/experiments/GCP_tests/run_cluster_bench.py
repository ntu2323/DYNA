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
)

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

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
            args.extend(["-O", "-R", str(poisson_rate)])
    else:
        args = [
            "-s", "$HOME/bench/dynaconfig",
            "-c", "$HOME/bench/dynaconfig",
            "-m", "dyna",
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
            args.extend(["-O", "-R", str(poisson_rate)])
    quoted = " ".join(_bash_word(a) for a in args)
    dyna_env = ""
    if protocol == "dyna":
        dyna_env = (
            "export DYNA_CLIENT_STATS=${DYNA_CLIENT_STATS:-1}; "
            "export DYNA_CLIENT_STATS_LOG_EVERY=${DYNA_CLIENT_STATS_LOG_EVERY:-10000}; "
            "export BENCH_INFLIGHT_LOG=${BENCH_INFLIGHT_LOG:-1}; "
        )
    # Background subshell: SSH returns immediately; client writes done when finished.
    client_run = (
        f"{dyna_env}{cfg['client_bin']} {quoted} "
        f"> {run_dir}/client.log 2>&1; "
        f"echo $? > {run_dir}/exit_code; "
        f"touch {run_dir}/done"
    )
    return (
        f"mkdir -p {run_dir} && rm -f {run_dir}/done {run_dir}/exit_code && "
        f"cd {cfg['client_cwd']} && "
        f"nohup bash -c {shlex.quote(client_run)} >/dev/null 2>&1 &"
    )


def _start_replica(
    role: str,
    host: str,
    protocol: str,
    ssh_user: str,
    ssh_key: Optional[str],
    *,
    clean_remote: bool,
) -> tuple[str, bool, str]:
    idx = receiver_index(role)
    cfg = PROTOCOLS[protocol]
    parts = []
    if clean_remote:
        parts.append(remote_receiver_cleanup_cmd(protocol, idx))
    else:
        parts.extend([PKILL_REPLICA, "sleep 1"])
    parts.append(cfg["replica_start"].format(idx=idx))
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


def main() -> None:
    ap = argparse.ArgumentParser(description="Orchestrate GCP VR/DYNA cluster benchmark run.")
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
    ap.add_argument("-j", "--threads", type=int, default=1, help="VR: -t threads; DYNA: -j threads per sender.")
    ap.add_argument(
        "--closed-loop",
        action="store_true",
        help="Closed-loop: send next request only after each completion (legacy bench).",
    )
    ap.add_argument(
        "--poisson-rate",
        type=int,
        default=60,
        help="Open-loop (default): mean requests per 10ms per sender (~rate*100 offered ops/s). Nezha-style.",
    )
    ap.add_argument("--replica-wait-sec", type=int, default=8)
    ap.add_argument("--client-timeout-sec", type=int, default=7200)
    ap.add_argument(
        "--no-clean-remote",
        action="store_true",
        help="Do not wipe remote bench state (default: pkill, rm runs/*, VR rocks, DYNA WAL).",
    )
    ap.add_argument("--skip-replicas", action="store_true")
    ap.add_argument("--skip-clients", action="store_true", help="Only (re)start replicas.")
    args = ap.parse_args()

    if not args.ssh_user:
        sys.exit("Set --ssh-user or SSH_USER")

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
        "offered_ops_per_sec": (args.poisson_rate * 100) if open_loop else None,
        "experiment_sec": args.experiment_sec,
        "warmup_sec": args.warmup_sec,
        "interval_ms": args.interval_ms,
        "trim_pad_sec": args.trim_pad_sec,
        "clean_remote": clean_remote,
        "threads": args.threads,
        "receivers": [{"role": r, "host": hosts[r], "index": receiver_index(r)} for r in receiver_roles],
        "senders": senders_meta,
        "t0_unix": None,
        "t0_iso": None,
    }

    if not args.skip_replicas:
        if clean_remote:
            print(f"Cleaning remote replica state ({args.protocol})...")
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
                )
                for r in receiver_roles
            ]
            for fut in concurrent.futures.as_completed(futs):
                role, ok, err = fut.result()
                if not ok:
                    sys.exit(f"Replica start failed on {role}: {err}")
                print(f"  [{role}] replica started")

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

    meta_path = os.path.join(local_run_dir, "run_meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print(f"\nRun complete. Local results: {local_run_dir}")
    print(f"  python3 aggregate_bench_results.py {local_run_dir}")


if __name__ == "__main__":
    main()

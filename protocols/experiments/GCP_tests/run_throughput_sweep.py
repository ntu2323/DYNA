#!/usr/bin/env python3
"""
Run a throughput/latency sweep: vary client threads (-j / VR -t) and aggregate each run.

Example:
  SSH_USER=nathan python3 run_throughput_sweep.py --protocol vr --threads 1 2 4 8
  SSH_USER=nathan python3 run_throughput_sweep.py --protocol dyna --threads 1 4 --experiment-sec 90
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def main() -> None:
    ap = argparse.ArgumentParser(description="Sweep client threads and aggregate throughput/latency.")
    ap.add_argument("--protocol", choices=["vr", "dyna"], required=True)
    ap.add_argument("--hosts-file", default=os.path.join(_SCRIPT_DIR, "cluster_hosts.json"))
    ap.add_argument("--threads", type=int, nargs="+", default=[1, 2, 4, 8])
    ap.add_argument("-n", "--num-ops", type=int, default=0, help="0 = unlimited (open-loop default).")
    ap.add_argument(
        "--closed-loop",
        action="store_true",
        help="Pass through to run_cluster_bench.py (default there is open-loop).",
    )
    ap.add_argument("--poisson-rate", type=int, default=60)
    ap.add_argument("-e", "--experiment-sec", type=int, default=60)
    ap.add_argument("-w", "--warmup-sec", type=int, default=5)
    ap.add_argument("--interval-ms", type=int, default=1000)
    ap.add_argument("--trim-pad-sec", type=float, default=3.0)
    ap.add_argument("--sweep-id", default="")
    ap.add_argument(
        "--no-clean-remote",
        action="store_true",
        help="Pass through to run_cluster_bench.py (default wipes remote state each run).",
    )
    ap.add_argument(
        "--wal-tmpfs",
        action="store_true",
        help="Pass through to run_cluster_bench.py (DYNA WAL on tmpfs).",
    )
    ap.add_argument(
        "--no-eager-fast-reply",
        action="store_true",
        help="Pass through to run_cluster_bench.py (disable eager fast-reply).",
    )
    ap.add_argument("--offered-ops-per-sec", type=float, default=0.0)
    ap.add_argument("--max-inflight", type=int, default=0)
    ap.add_argument("--wan-open-loop", action="store_true")
    ap.add_argument("--auto-max-inflight", action="store_true")
    ap.add_argument("--expected-rtt-sec", type=float, default=0.35)
    ap.add_argument("--inflight-factor", type=float, default=4.0)
    ap.add_argument("--high-throughput-latency", action="store_true")
    args = ap.parse_args()

    sweep_id = args.sweep_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    sweep_dir = os.path.join(_SCRIPT_DIR, "results", f"sweep_{args.protocol}_{sweep_id}")
    os.makedirs(sweep_dir, exist_ok=True)

    ssh_user = os.environ.get("SSH_USER", "")
    ssh_key = os.environ.get("SSH_KEY_FILE", "")
    if not ssh_user:
        sys.exit("Set SSH_USER")

    rows = []
    for t in args.threads:
        run_id = f"t{t}"
        run_dir = os.path.join(sweep_dir, run_id)
        print(f"\n=== sweep threads={t} -> {run_dir} ===\n")

        run_cmd = [
            sys.executable,
            os.path.join(_SCRIPT_DIR, "run_cluster_bench.py"),
            "--protocol",
            args.protocol,
            "--hosts-file",
            args.hosts_file,
            "--ssh-user",
            ssh_user,
            "--run-id",
            run_id,
            "--results-dir",
            run_dir,
            "-n",
            str(args.num_ops),
            "-e",
            str(args.experiment_sec),
            "-w",
            str(args.warmup_sec),
            "--interval-ms",
            str(args.interval_ms),
            "--trim-pad-sec",
            str(args.trim_pad_sec),
            "-j",
            str(t),
        ]
        if ssh_key:
            run_cmd.extend(["--ssh-key", ssh_key])
        if args.no_clean_remote:
            run_cmd.append("--no-clean-remote")
        if args.closed_loop:
            run_cmd.append("--closed-loop")
        if args.poisson_rate != 60:
            run_cmd.extend(["--poisson-rate", str(args.poisson_rate)])
        if args.wal_tmpfs:
            run_cmd.append("--wal-tmpfs")
        if args.no_eager_fast_reply:
            run_cmd.append("--no-eager-fast-reply")
        if args.offered_ops_per_sec > 0.0:
            run_cmd.extend(["--offered-ops-per-sec", str(args.offered_ops_per_sec)])
        if args.max_inflight > 0:
            run_cmd.extend(["--max-inflight", str(args.max_inflight)])
        if args.wan_open_loop:
            run_cmd.append("--wan-open-loop")
        if args.auto_max_inflight:
            run_cmd.append("--auto-max-inflight")
        if args.high_throughput_latency:
            run_cmd.append("--high-throughput-latency")
        if args.expected_rtt_sec != 0.35:
            run_cmd.extend(["--expected-rtt-sec", str(args.expected_rtt_sec)])
        if args.inflight_factor != 4.0:
            run_cmd.extend(["--inflight-factor", str(args.inflight_factor)])

        subprocess.run(run_cmd, check=True)

        agg_cmd = [
            sys.executable,
            os.path.join(_SCRIPT_DIR, "aggregate_bench_results.py"),
            run_dir,
            "--trim-pad-sec",
            str(args.trim_pad_sec),
        ]
        subprocess.run(agg_cmd, check=True)

        with open(os.path.join(run_dir, "summary.json"), encoding="utf-8") as f:
            summary = json.load(f)

        tp = summary["throughput"]
        mean_tput = tp.get("mean_ops_per_sec") or 0.0
        if "fallback" in tp:
            mean_tput = tp["fallback"].get("mean_ops_per_sec", mean_tput)

        medians = []
        for _role, lats in summary.get("per_sender_latency_ns", {}).items():
            if isinstance(lats, dict) and "median" in lats:
                medians.append(lats["median"])

        row = {
            "threads": t,
            "mean_ops_per_sec": mean_tput,
            "peak_ops_per_sec": tp.get("peak_ops_per_sec", 0.0),
            "total_ops": tp.get("total_ops", 0),
            "median_latency_ns": sum(medians) / len(medians) if medians else "",
            "run_dir": run_dir,
        }
        rows.append(row)

    csv_path = os.path.join(sweep_dir, "sweep.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nSweep complete: {csv_path}")
    print(f"{'threads':>8}  {'ops/s':>12}  {'median_ns':>14}")
    for r in rows:
        med = r["median_latency_ns"]
        med_s = f"{med:.0f}" if med != "" else "n/a"
        print(f"{r['threads']:>8}  {r['mean_ops_per_sec']:>12.1f}  {med_s:>14}")


if __name__ == "__main__":
    main()

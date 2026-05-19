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
    ap.add_argument("-n", "--num-ops", type=int, default=100_000)
    ap.add_argument("-e", "--experiment-sec", type=int, default=60)
    ap.add_argument("-w", "--warmup-sec", type=int, default=5)
    ap.add_argument("--interval-ms", type=int, default=1000)
    ap.add_argument("--trim-pad-sec", type=float, default=3.0)
    ap.add_argument("--sweep-id", default="")
    ap.add_argument("--reset-rocksdb", action="store_true")
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
        if args.reset_rocksdb and t == args.threads[0]:
            run_cmd.append("--reset-rocksdb")

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

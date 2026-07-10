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


def _row_from_summary(summary: dict, offered: float, threads: int, run_dir: str) -> dict:
    """One sweep.csv row: achieved throughput + latency (median/p99) + completion %."""
    tp = summary.get("throughput", {})
    mean_tput = tp.get("mean_ops_per_sec") or 0.0
    if "fallback" in tp:
        mean_tput = tp["fallback"].get("mean_ops_per_sec", mean_tput)

    psl = summary.get("per_sender_latency_ns", {})
    medians = [v["median"] for v in psl.values()
               if isinstance(v, dict) and "median" in v]
    p99s = [v["99th"] for v in psl.values()
            if isinstance(v, dict) and "99th" in v]

    # Completion = delivered ops / submitted ops, summed across senders (per_sender_finish).
    pf = summary.get("per_sender_finish", {})
    ops = sum(v.get("ops", 0) for v in pf.values() if isinstance(v, dict))
    sub = sum(v.get("submitted", 0) for v in pf.values() if isinstance(v, dict))
    completion = (100.0 * ops / sub) if sub else ""

    return {
        "offered_rate": offered,
        "threads": threads,
        "mean_ops_per_sec": mean_tput,
        "peak_ops_per_sec": tp.get("peak_ops_per_sec", 0.0),
        "total_ops": tp.get("total_ops", 0),
        "median_latency_ns": sum(medians) / len(medians) if medians else "",
        "p99_latency_ns": sum(p99s) / len(p99s) if p99s else "",
        "completion_pct": completion,
        "run_dir": run_dir,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Sweep client threads and aggregate throughput/latency.")
    ap.add_argument("--protocol", choices=["vr", "dyna", "dyna_new", "ionia"], required=True)
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
    ap.add_argument(
        "--offered-sweep",
        type=float,
        nargs="+",
        default=None,
        help="Open-loop offered-rate sweep: iterate these ops/s (each run -j 1, "
        "--max-inflight 0) INSTEAD of --threads. Matches the low-load latency baseline "
        "and exposes each protocol's saturation knee (completion drops, p99 spikes).",
    )
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

    # Sweep dimension: open-loop offered-rate (--offered-sweep) OR closed-loop threads.
    # Each point is (run_id, offered_rate, threads); offered_rate=0 => threads mode.
    if args.offered_sweep:
        points = [(f"g{r:g}", float(r), 1) for r in args.offered_sweep]
        sweep_label = "offered"
    else:
        points = [(f"t{t}", 0.0, t) for t in args.threads]
        sweep_label = "threads"

    rows = []
    for run_id, offered, t in points:
        run_dir = os.path.join(sweep_dir, run_id)
        # offered-sweep mode drives load open-loop (per-point rate, no inflight cap);
        # threads mode passes any global --offered-ops-per-sec / --max-inflight through.
        point_offered = offered if offered > 0.0 else args.offered_ops_per_sec
        point_inflight = 0 if offered > 0.0 else args.max_inflight
        print(f"\n=== sweep {sweep_label}: {run_id} "
              f"(offered={point_offered or 0:g} threads={t}) -> {run_dir} ===\n")

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
        if point_offered > 0.0:
            run_cmd.extend(["--offered-ops-per-sec", str(point_offered)])
        if point_inflight > 0:
            run_cmd.extend(["--max-inflight", str(point_inflight)])
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

        rows.append(_row_from_summary(summary, offered, t, run_dir))

    csv_path = os.path.join(sweep_dir, "sweep.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nSweep complete: {csv_path}")
    hdr = "offered" if args.offered_sweep else "threads"
    key = "offered_rate" if args.offered_sweep else "threads"
    print(f"{hdr:>9}  {'ach_ops/s':>11}  {'median_us':>10}  {'p99_us':>10}  {'compl%':>7}")
    for r in rows:
        med = r["median_latency_ns"]
        p99 = r["p99_latency_ns"]
        comp = r["completion_pct"]
        med_s = f"{med/1000:.1f}" if med != "" else "n/a"
        p99_s = f"{p99/1000:.1f}" if p99 != "" else "n/a"
        comp_s = f"{comp:.1f}" if comp != "" else "n/a"
        print(f"{r[key]:>9g}  {r['mean_ops_per_sec']:>11.1f}  {med_s:>10}  "
              f"{p99_s:>10}  {comp_s:>7}")


if __name__ == "__main__":
    main()

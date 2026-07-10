#!/usr/bin/env python3
"""
Plot the latency-vs-throughput curve from one or more sweep.csv files.

Each sweep.csv is produced by run_throughput_sweep.py (columns include
mean_ops_per_sec, median_latency_ns, p99_latency_ns, completion_pct). Pass one
`label=path` per protocol; x = achieved throughput, y = latency (median solid,
p99 dashed). Points below the saturation knee (completion >= --min-completion)
are drawn filled; saturated points (completion below the threshold) are hollow,
so the knee is visible.

Example:
  python3 plot_sweep.py \
    dyna_new=results/sweep_dyna_new_<id>/sweep.csv \
    ionia=results/sweep_ionia_<id>/sweep.csv \
    vr=results/sweep_vr_<id>/sweep.csv \
    --out results/latency_throughput.png
"""

from __future__ import annotations

import argparse
import csv
import os
import sys


def _load(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    def fnum(r: dict, k: str):
        v = r.get(k, "")
        return float(v) if v not in ("", None) else None

    out = []
    for r in rows:
        out.append({
            "tput": fnum(r, "mean_ops_per_sec"),
            "median_us": (fnum(r, "median_latency_ns") or 0) / 1000.0,
            "p99_us": (fnum(r, "p99_latency_ns") or 0) / 1000.0,
            "completion": fnum(r, "completion_pct"),
        })
    # Sort by achieved throughput so the line traces the curve.
    return sorted([r for r in out if r["tput"] is not None], key=lambda r: r["tput"])


def main() -> None:
    ap = argparse.ArgumentParser(description="Plot latency vs throughput from sweep.csv files.")
    ap.add_argument("series", nargs="+", help="label=path/to/sweep.csv (one per protocol)")
    ap.add_argument("--out", default="latency_throughput.png")
    ap.add_argument("--min-completion", type=float, default=99.0,
                    help="Points with completion below this are drawn hollow (saturated).")
    ap.add_argument("--p99", action="store_true", help="Also draw p99 (dashed).")
    ap.add_argument("--logx", action="store_true", help="Log-scale the throughput axis.")
    args = ap.parse_args()

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        sys.exit("matplotlib not installed: `pip install matplotlib` (or read the CSVs directly).")

    fig, ax = plt.subplots(figsize=(7, 5))
    for spec in args.series:
        if "=" not in spec:
            sys.exit(f"expected label=path, got: {spec}")
        label, path = spec.split("=", 1)
        rows = _load(path)
        if not rows:
            print(f"warning: no rows in {path}", file=sys.stderr)
            continue
        xs = [r["tput"] for r in rows]
        med = [r["median_us"] for r in rows]
        line, = ax.plot(xs, med, marker="o", label=f"{label} (median)")
        color = line.get_color()
        # Hollow-out saturated points (completion below the knee threshold).
        sat_x = [r["tput"] for r in rows
                 if r["completion"] is not None and r["completion"] < args.min_completion]
        sat_y = [r["median_us"] for r in rows
                 if r["completion"] is not None and r["completion"] < args.min_completion]
        if sat_x:
            ax.scatter(sat_x, sat_y, facecolors="white", edgecolors=color, zorder=3, s=60)
        if args.p99:
            ax.plot(xs, [r["p99_us"] for r in rows], marker="^", linestyle="--",
                    color=color, alpha=0.6, label=f"{label} (p99)")

    ax.set_xlabel("Achieved throughput (ops/s)")
    ax.set_ylabel("Latency (µs)")
    ax.set_title("Latency vs throughput (hollow = saturated, completion < "
                 f"{args.min_completion:g}%)")
    if args.logx:
        ax.set_xscale("log")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(args.out, dpi=150)
    print(f"wrote {os.path.abspath(args.out)}")


if __name__ == "__main__":
    main()

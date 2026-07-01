#!/usr/bin/env python3
"""
Aggregate cluster throughput (interval logs with trim window) and per-sender latency stats.

Reads a local results directory produced by run_cluster_bench.py (run_meta.json + sender*/client.log).

Example:
  python3 aggregate_bench_results.py results/20260519T120000Z
  python3 aggregate_bench_results.py results/20260519T120000Z --trim-pad-sec 5
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from gcp_bench_common import (
    DYNA_CLIENT_FIRST_REPLY_US_RE,
    DYNA_CLIENT_QUORUM_WAIT_US_RE,
    DYNA_CLIENT_STATS_RE,
    DYNA_CLIENT_TOTAL_US_RE,
    FINISH_LINE_RE,
    INFLIGHT_LINE_RE,
    INTERVAL_LINE_RE,
    LATENCY_LINE_RE,
)


@dataclass
class IntervalSample:
    sender: str
    wall_ms: int
    delta_ops: int


def _parse_log(path: str, sender: str) -> Tuple[List[IntervalSample], Optional[Tuple[int, float]], Dict[str, int]]:
    intervals: List[IntervalSample] = []
    # One client process prints a finish summary PER benchmark thread (--threads),
    # so collect every per-thread finish line and aggregate (sum completed/submitted,
    # max wall time) instead of letting the last line win (which silently reported a
    # single thread's count as if it were the whole sender).
    finish_completed = 0
    finish_submitted = 0
    finish_secs: List[float] = []
    finish_threads = 0
    # One client process prints a latency summary PER benchmark thread (--threads),
    # so collect every per-thread value and summarize, rather than letting the last
    # line win (which silently reports a single arbitrary thread).
    latency_lists: Dict[str, List[int]] = {}

    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = INTERVAL_LINE_RE.search(line)
            if m:
                intervals.append(
                    IntervalSample(sender, int(m.group(2)), int(m.group(1)))
                )
                continue
            m = FINISH_LINE_RE.search(line)
            if m and "warm" not in line.lower():
                finish_completed += int(m.group(1))
                finish_secs.append(float(m.group(2)))
                if m.group(3) is not None:
                    finish_submitted += int(m.group(3))
                finish_threads += 1
                continue
            m = LATENCY_LINE_RE.search(line)
            if m:
                key = m.group(1).lower().replace(" percentile", "")
                latency_lists.setdefault(key, []).append(int(m.group(2)))

    finish: Optional[Tuple[int, float]] = None
    if finish_threads:
        # Wall time is the longest-running thread (threads run concurrently, so the
        # sender's elapsed time is the max, not the sum).
        finish = (finish_completed, max(finish_secs))
    summary = _summarize_thread_latencies(latency_lists)
    summary["_finish_completed"] = finish_completed
    summary["_finish_submitted"] = finish_submitted
    summary["_finish_threads"] = finish_threads
    summary["_finish_secs"] = int(max(finish_secs) * 1000) if finish_secs else 0
    return intervals, finish, summary


def _summarize_thread_latencies(latency_lists: Dict[str, List[int]]) -> Dict[str, int]:
    """Collapse per-thread latency lines into per-sender stats.

    For back-compat, `median`/`average`/`99th` remain scalars but are now the mean
    across threads. Per-metric `<key>_min`/`<key>_max`/`<key>_threads` expose the
    cross-thread spread so a bimodal per-flow distribution is visible instead of
    being hidden behind one arbitrary thread's value.
    """
    out: Dict[str, int] = {}
    thread_count = 0
    for key, vals in latency_lists.items():
        if not vals:
            continue
        thread_count = max(thread_count, len(vals))
        svals = sorted(vals)
        out[key] = sum(svals) // len(svals)
        out[f"{key}_min"] = svals[0]
        out[f"{key}_max"] = svals[-1]
        out[f"{key}_threads"] = len(svals)
    if thread_count:
        out["thread_count"] = thread_count
    return out


def _parse_client_diagnostics(path: str) -> dict:
    """Parse DYNA client stats and open-loop inflight lines from client.log."""
    max_inflight = 0
    last_summary: dict = {}
    last_total_us: dict = {}
    last_first_reply_us: dict = {}
    last_quorum_wait_us: dict = {}

    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = INFLIGHT_LINE_RE.search(line)
            if m:
                max_inflight = max(max_inflight, int(m.group(1)))
                continue
            m = DYNA_CLIENT_STATS_RE.search(line)
            if m:
                last_summary = {
                    "completions": int(m.group(1)),
                    "bound_us": int(m.group(2)),
                }
                continue
            m = DYNA_CLIENT_TOTAL_US_RE.search(line)
            if m:
                last_total_us = {
                    "samples": int(m.group(1)),
                    "avg_us": int(m.group(2)),
                    "p50_us": int(m.group(3)),
                    "p99_us": int(m.group(4)),
                    "max_us": int(m.group(5)),
                }
                continue
            m = DYNA_CLIENT_FIRST_REPLY_US_RE.search(line)
            if m:
                last_first_reply_us = {
                    "samples": int(m.group(1)),
                    "avg_us": int(m.group(2)),
                    "p50_us": int(m.group(3)),
                    "p99_us": int(m.group(4)),
                    "max_us": int(m.group(5)),
                }
                continue
            m = DYNA_CLIENT_QUORUM_WAIT_US_RE.search(line)
            if m:
                last_quorum_wait_us = {
                    "samples": int(m.group(1)),
                    "avg_us": int(m.group(2)),
                    "p50_us": int(m.group(3)),
                    "p99_us": int(m.group(4)),
                    "max_us": int(m.group(5)),
                }

    out: dict = {}
    if max_inflight > 0:
        out["max_inflight"] = max_inflight
    if last_summary:
        out["client_stats"] = last_summary
    if last_total_us:
        out["total_us"] = last_total_us
    if last_first_reply_us:
        out["first_reply_us"] = last_first_reply_us
    if last_quorum_wait_us:
        out["quorum_wait_us"] = last_quorum_wait_us
    return out


def _measurement_window_ms(meta: dict, trim_pad_sec: float) -> Tuple[int, int]:
    t0_ms = int(float(meta["t0_unix"]) * 1000)
    warmup_ms = int(meta["warmup_sec"]) * 1000
    experiment_ms = int(meta["experiment_sec"]) * 1000
    pad_ms = int(trim_pad_sec * 1000)
    start = t0_ms + warmup_ms + pad_ms
    end = t0_ms + warmup_ms + experiment_ms - pad_ms
    if end <= start:
        raise SystemExit(
            f"Invalid measurement window: start={start} end={end} "
            f"(warmup={meta['warmup_sec']}s experiment={meta['experiment_sec']}s pad={trim_pad_sec}s)"
        )
    return start, end


def _aggregate_throughput(
    intervals: List[IntervalSample],
    window_start_ms: int,
    window_end_ms: int,
    bucket_ms: int,
) -> dict:
    if not intervals:
        return {
            "window_start_ms": window_start_ms,
            "window_end_ms": window_end_ms,
            "bucket_ms": bucket_ms,
            "total_ops": 0,
            "window_sec": (window_end_ms - window_start_ms) / 1000.0,
            "mean_ops_per_sec": 0.0,
            "peak_ops_per_sec": 0.0,
            "buckets": [],
            "per_sender_ops": {},
        }

    # Clip to window and bucket by wall clock
    buckets: Dict[int, int] = {}
    per_sender: Dict[str, int] = {}
    for s in intervals:
        if s.wall_ms < window_start_ms or s.wall_ms >= window_end_ms:
            continue
        b = (s.wall_ms - window_start_ms) // bucket_ms
        buckets[b] = buckets.get(b, 0) + s.delta_ops
        per_sender[s.sender] = per_sender.get(s.sender, 0) + s.delta_ops

    window_sec = (window_end_ms - window_start_ms) / 1000.0
    total_ops = sum(buckets.values())
    bucket_rates = [(ops / (bucket_ms / 1000.0)) for ops in buckets.values()]
    mean_rate = total_ops / window_sec if window_sec > 0 else 0.0
    peak_rate = max(bucket_rates) if bucket_rates else 0.0

    bucket_list = [
        {
            "bucket_index": idx,
            "start_ms": window_start_ms + idx * bucket_ms,
            "ops": ops,
            "ops_per_sec": ops / (bucket_ms / 1000.0),
        }
        for idx, ops in sorted(buckets.items())
    ]

    return {
        "window_start_ms": window_start_ms,
        "window_end_ms": window_end_ms,
        "bucket_ms": bucket_ms,
        "total_ops": total_ops,
        "window_sec": window_sec,
        "mean_ops_per_sec": mean_rate,
        "peak_ops_per_sec": peak_rate,
        "buckets": bucket_list,
        "per_sender_ops": per_sender,
    }


def _latency_across_senders(per_sender_latency: Dict[str, dict]) -> dict:
    """Mean of per-sender medians and means (senders with valid percentile lines only)."""
    medians: List[int] = []
    averages: List[int] = []
    for lats in per_sender_latency.values():
        if "error" in lats:
            continue
        if "median" in lats:
            medians.append(lats["median"])
        if "average" in lats:
            averages.append(lats["average"])

    out: dict = {"sender_count_with_latency": len(medians)}
    if medians:
        out["mean_of_medians_ns"] = sum(medians) / len(medians)
    if averages:
        out["mean_of_averages_ns"] = sum(averages) / len(averages)
    if not medians and not averages:
        out["note"] = "no per-sender percentile lines"
    return out


def _fallback_finish_throughput(
    senders: List[dict],
    run_dir: str,
    trim_pad_sec: float,
    meta: dict,
) -> Optional[dict]:
    """Sum per-sender Finish() ops/time when interval lines are missing."""
    total_ops = 0
    total_rate = 0.0
    per_sender = {}
    for s in senders:
        role = s["role"]
        log_path = os.path.join(run_dir, role, "client.log")
        if not os.path.isfile(log_path):
            continue
        _, finish, _ = _parse_log(log_path, role)
        if not finish:
            continue
        ops, sec = finish
        if sec <= 2 * trim_pad_sec:
            continue
        effective_sec = max(sec - 2 * trim_pad_sec, 0.001)
        rate = ops / effective_sec
        per_sender[role] = {"ops": ops, "seconds": sec, "ops_per_sec": rate}
        total_ops += ops
        total_rate += rate

    if not per_sender:
        return None
    return {
        "mode": "finish_line_sum",
        "note": "No -i interval lines; summed per-sender Completed N in S (approximate).",
        "total_ops": total_ops,
        "mean_ops_per_sec": total_rate,
        "per_sender": per_sender,
        "experiment_sec": meta.get("experiment_sec"),
    }


def aggregate_run(run_dir: str, trim_pad_sec: Optional[float]) -> dict:
    meta_path = os.path.join(run_dir, "run_meta.json")
    if not os.path.isfile(meta_path):
        raise SystemExit(f"Missing {meta_path}")

    with open(meta_path, encoding="utf-8") as f:
        meta = json.load(f)

    pad = trim_pad_sec if trim_pad_sec is not None else float(meta.get("trim_pad_sec", 3.0))
    bucket_ms = int(meta.get("interval_ms", 1000))

    all_intervals: List[IntervalSample] = []
    per_sender_latency: Dict[str, dict] = {}
    per_sender_finish: Dict[str, dict] = {}
    per_sender_diagnostics: Dict[str, dict] = {}

    for s in meta["senders"]:
        role = s["role"]
        log_path = os.path.join(run_dir, role, "client.log")
        if not os.path.isfile(log_path):
            per_sender_latency[role] = {"error": "missing client.log"}
            continue
        intervals, finish, lats = _parse_log(log_path, role)
        all_intervals.extend(intervals)
        completed = lats.pop("_finish_completed", 0)
        submitted = lats.pop("_finish_submitted", 0)
        finish_threads = lats.pop("_finish_threads", 0)
        lats.pop("_finish_secs", 0)
        per_sender_latency[role] = lats
        if meta.get("protocol") == "dyna":
            diag = _parse_client_diagnostics(log_path)
            if diag:
                per_sender_diagnostics[role] = diag
        if finish:
            ops, sec = finish
            entry = {
                "ops": ops,
                "seconds": sec,
                "ops_per_sec": ops / sec if sec > 0 else 0.0,
                "threads": finish_threads,
            }
            if submitted > 0:
                entry["submitted"] = submitted
                entry["submitted_per_sec"] = submitted / sec if sec > 0 else 0.0
                entry["complete_frac"] = completed / submitted if submitted > 0 else 0.0
            per_sender_finish[role] = entry

    if meta.get("t0_unix") is not None:
        window_start, window_end = _measurement_window_ms(meta, pad)
        # Also tighten to observed interval data if present
        if all_intervals:
            obs_start = min(s.wall_ms for s in all_intervals)
            obs_end = max(s.wall_ms for s in all_intervals)
            window_start = max(window_start, obs_start)
            window_end = min(window_end, obs_end)
        throughput = _aggregate_throughput(all_intervals, window_start, window_end, bucket_ms)
    else:
        throughput = _aggregate_throughput(all_intervals, 0, 2**62, bucket_ms)
        throughput["note"] = "run_meta.json missing t0_unix; used all interval samples"

    if throughput["total_ops"] == 0:
        fb = _fallback_finish_throughput(meta["senders"], run_dir, pad, meta)
        if fb:
            throughput["fallback"] = fb

    latency_across_senders = _latency_across_senders(per_sender_latency)

    summary = {
        "run_id": meta.get("run_id"),
        "protocol": meta.get("protocol"),
        "trim_pad_sec": pad,
        "throughput": throughput,
        "per_sender_latency_ns": per_sender_latency,
        "latency_across_senders_ns": latency_across_senders,
        "per_sender_finish": per_sender_finish,
        "per_sender_diagnostics": per_sender_diagnostics,
        "meta": {
            "num_ops": meta.get("num_ops"),
            "experiment_sec": meta.get("experiment_sec"),
            "warmup_sec": meta.get("warmup_sec"),
            "threads": meta.get("threads"),
            "sender_count": len(meta.get("senders", [])),
        },
    }
    return summary


def _print_summary(summary: dict) -> None:
    print(f"Run {summary.get('run_id')}  protocol={summary.get('protocol')}")
    tp = summary["throughput"]
    if "fallback" in tp:
        fb = tp["fallback"]
        print(f"  Throughput (fallback): {fb['mean_ops_per_sec']:.1f} ops/s  total_ops={fb['total_ops']}")
        print(f"    {fb.get('note', '')}")
    else:
        print(
            f"  Throughput: {tp['mean_ops_per_sec']:.1f} ops/s (mean over window)  "
            f"peak_bucket={tp['peak_ops_per_sec']:.1f} ops/s  "
            f"total_ops={tp['total_ops']}  window_sec={tp['window_sec']:.1f}"
        )
    print("  Per-sender latency (ns):")
    for role, lats in summary["per_sender_latency_ns"].items():
        if "error" in lats:
            print(f"    {role}: {lats['error']}")
            continue
        med = lats.get("median")
        avg = lats.get("average")
        p99 = lats.get("99th")
        if med is not None:
            nthreads = lats.get("thread_count", lats.get("median_threads", 1))
            spread = ""
            if nthreads and nthreads > 1:
                spread = (
                    f" [across {nthreads} threads: median min={lats.get('median_min')} "
                    f"max={lats.get('median_max')}]"
                )
            print(f"    {role}: median={med} avg={avg} p99={p99}{spread}")
        else:
            print(f"    {role}: (no percentile lines in log)")
    cross = summary.get("latency_across_senders_ns") or {}
    mean_med = cross.get("mean_of_medians_ns")
    mean_avg = cross.get("mean_of_averages_ns")
    n_lat = cross.get("sender_count_with_latency", 0)
    if mean_med is not None or mean_avg is not None:
        parts = []
        if mean_med is not None:
            parts.append(f"mean_of_medians={mean_med:.0f}")
        if mean_avg is not None:
            parts.append(f"mean_of_averages={mean_avg:.0f}")
        print(f"    Across {n_lat} sender(s): " + "  ".join(parts))
    elif cross.get("note"):
        print(f"    Across senders: ({cross['note']})")
    print("  Per-sender finish line (summed across threads):")
    tot_completed_rate = 0.0
    tot_offered_rate = 0.0
    for role, fin in summary["per_sender_finish"].items():
        nthreads = fin.get("threads", 0)
        thr = f" [{nthreads} threads]" if nthreads else ""
        line = (
            f"    {role}: {fin['ops']} ops in {fin['seconds']:.2f}s "
            f"=> {fin['ops_per_sec']:.1f} ops/s{thr}"
        )
        tot_completed_rate += fin["ops_per_sec"]
        if "submitted" in fin:
            line += (
                f"  (offered {fin['submitted_per_sec']:.1f} ops/s, "
                f"completed {100.0 * fin['complete_frac']:.1f}%)"
            )
            tot_offered_rate += fin["submitted_per_sec"]
        print(line)
    if tot_completed_rate > 0:
        agg = f"    All senders: completed {tot_completed_rate:.1f} ops/s"
        if tot_offered_rate > 0:
            agg += (
                f"  offered {tot_offered_rate:.1f} ops/s "
                f"({100.0 * tot_completed_rate / tot_offered_rate:.1f}% of offered)"
            )
        print(agg)
    diag_all = summary.get("per_sender_diagnostics") or {}
    if diag_all:
        print("  Per-sender diagnostics (DYNA):")
        for role, diag in diag_all.items():
            parts = []
            if "max_inflight" in diag:
                parts.append(f"max_inflight={diag['max_inflight']}")
            cs = diag.get("client_stats") or {}
            if cs:
                parts.append(f"bound_us={cs.get('bound_us')}")
            tr = diag.get("total_us") or {}
            if tr.get("p50_us") is not None:
                parts.append(f"total_p50_us={tr['p50_us']}")
            fr = diag.get("first_reply_us") or {}
            if fr.get("p50_us") is not None:
                parts.append(f"first_reply_p50_us={fr['p50_us']}")
            qw = diag.get("quorum_wait_us") or {}
            if qw.get("p50_us") is not None:
                parts.append(f"quorum_wait_p50_us={qw['p50_us']}")
            print(f"    {role}: " + "  ".join(parts))


def main() -> None:
    ap = argparse.ArgumentParser(description="Aggregate GCP cluster bench throughput and latency.")
    ap.add_argument("run_dir", help="Local results directory from run_cluster_bench.py")
    ap.add_argument(
        "--trim-pad-sec",
        type=float,
        default=None,
        help="Override trim pad (default: value from run_meta.json).",
    )
    ap.add_argument(
        "--output",
        default="",
        help="Write summary JSON (default: <run_dir>/summary.json).",
    )
    args = ap.parse_args()

    run_dir = os.path.abspath(args.run_dir)
    summary = aggregate_run(run_dir, args.trim_pad_sec)

    out_path = args.output or os.path.join(run_dir, "summary.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    _print_summary(summary)
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()

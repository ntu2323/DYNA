#!/usr/bin/env python3
"""
Verify cluster clock sync by MEASURING it (not by trusting ptp4l / chrony status).

Runs remote/clock_probe.py on every host in the hosts file: each host serves, and each
host probes every other host over the experiment LAN. Prints the pairwise offset matrix
(peer clock minus prober clock, µs) and fails (exit 1) if any |offset| exceeds
--max-offset-us. DYNA's deadline ordering assumes clocks agree to well under the
deadline bound (tens of µs), so run this after sync_cluster_clocks.py and before
benchmarks.

Example:
  SSH_USER=nathantu SSH_KEY_FILE=~/.ssh/corfu_remote \\
    python3 check_clock_sync.py --hosts-file cluster_hosts_clemson.json
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import sys
import time
from typing import Dict, List, Tuple

from gcp_bench_common import load_hosts_file, scp_to_remote, ssh_run

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROBE = os.path.join(_SCRIPT_DIR, "remote", "clock_probe.py")
_REMOTE_PROBE = "~/clock_probe.py"


def _sorted_roles(hosts: Dict[str, str]) -> List[str]:
    return sorted(k for k in hosts if k.startswith("receiver")) + sorted(
        k for k in hosts if k.startswith("sender")
    )


def _experiment_ip(host: str, user: str, key, prefix: str) -> str:
    r = ssh_run(
        host, user, key,
        f"ip -4 -o addr show | awk '{{print $4}}' | cut -d/ -f1 | grep '^{prefix}' | head -1",
    )
    ip = (r.stdout or "").strip()
    if not ip:
        sys.exit(f"{host}: no address on {prefix}*")
    return ip


def main() -> None:
    ap = argparse.ArgumentParser(description="Measure pairwise clock offsets.")
    ap.add_argument("--hosts-file", default=os.path.join(_SCRIPT_DIR, "cluster_hosts.json"))
    ap.add_argument("--ssh-user", default=os.environ.get("SSH_USER", ""))
    ap.add_argument("--ssh-key", default=os.environ.get("SSH_KEY_FILE", ""))
    ap.add_argument("--exp-prefix", default=os.environ.get("EXP_PREFIX", "10.10."))
    ap.add_argument("--port", type=int, default=31999)
    ap.add_argument("--count", type=int, default=2000, help="Probes per pair.")
    ap.add_argument("--max-offset-us", type=float, default=10.0)
    args = ap.parse_args()
    if not args.ssh_user:
        sys.exit("Set --ssh-user or SSH_USER")
    key = args.ssh_key or None

    hosts_path = args.hosts_file
    if not os.path.isabs(hosts_path):
        hosts_path = os.path.join(_SCRIPT_DIR, hosts_path)
    hosts, _ = load_hosts_file(hosts_path)
    roles = _sorted_roles(hosts)

    def prep(role: str) -> Tuple[str, str]:
        scp_to_remote(hosts[role], args.ssh_user, key, _PROBE, _REMOTE_PROBE)
        return role, _experiment_ip(hosts[role], args.ssh_user, key, args.exp_prefix)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(roles)) as ex:
        ips = dict(ex.map(prep, roles))

    # Every host serves for long enough to cover all probe rounds.
    serve_secs = 30 + args.count * 0.002 * len(roles)
    for role in roles:
        # Kill a stale server in its own ssh call: a `pkill -f ...serve` sharing a command
        # line with the nohup launch would match (and kill) its own shell.
        ssh_run(hosts[role], args.ssh_user, key, "pkill -f '[c]lock_probe.py serve' || true")
        ssh_run(
            hosts[role], args.ssh_user, key,
            f"nohup python3 {_REMOTE_PROBE} serve {ips[role]} {args.port} "
            f"{serve_secs:.0f} </dev/null >/dev/null 2>&1 &",
        )
    time.sleep(1.0)

    def probe_from(role: str) -> Tuple[str, Dict[str, dict]]:
        out: Dict[str, dict] = {}
        for peer in roles:
            if peer == role:
                continue
            r = ssh_run(
                hosts[role], args.ssh_user, key,
                f"python3 {_REMOTE_PROBE} probe {ips[peer]} {args.port} {args.count}",
            )
            try:
                out[peer] = json.loads((r.stdout or "").strip().splitlines()[-1])
            except (IndexError, json.JSONDecodeError):
                out[peer] = {"error": (r.stderr or r.stdout or "no output").strip()[:200]}
        return role, out

    # Probe sequentially per prober but in parallel across probers: each pair still gets
    # a clean min-RTT sample because probes are tiny.
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(roles)) as ex:
        results = dict(ex.map(probe_from, roles))
    for role in roles:
        ssh_run(hosts[role], args.ssh_user, key, "pkill -f '[c]lock_probe.py serve' || true")

    width = max(len(r) for r in roles) + 2
    print("Offset matrix (µs): row = prober, column = peer; value = peer clock − prober clock")
    print(" " * width + "".join(f"{r:>{width}}" for r in roles))
    worst = 0.0
    errors = []
    for role in roles:
        cells = []
        for peer in roles:
            if peer == role:
                cells.append(f"{'—':>{width}}")
                continue
            res = results[role].get(peer, {})
            if "offset_us" not in res:
                errors.append(f"{role}->{peer}: {res.get('error', 'missing')}")
                cells.append(f"{'ERR':>{width}}")
                continue
            worst = max(worst, abs(res["offset_us"]))
            cells.append(f"{res['offset_us']:>{width}.2f}")
        print(f"{role:<{width}}" + "".join(cells))

    print("\nMin RTT (µs) / spread of best-sample offsets (µs):")
    for role in roles:
        parts = []
        for peer in roles:
            res = results[role].get(peer, {})
            if "min_rtt_us" in res:
                parts.append(f"{peer}:{res['min_rtt_us']:.1f}/{res['offset_spread_us']:.1f}")
        print(f"  {role}: " + "  ".join(parts))

    if errors:
        print("\nERRORS:\n  " + "\n  ".join(errors), file=sys.stderr)
        sys.exit(1)
    verdict = "OK" if worst <= args.max_offset_us else "FAIL"
    print(f"\nWorst |offset| = {worst:.2f} µs (limit {args.max_offset_us:.1f}) -> {verdict}")
    sys.exit(0 if verdict == "OK" else 1)


if __name__ == "__main__":
    main()

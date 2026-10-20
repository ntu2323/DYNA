#!/usr/bin/env python3
"""
CPU-profile the running bench/replica on every receiver (perf record -g), e.g. in a
second terminal while run_cluster_bench.py is in its measurement window.

  SSH_USER=nathantu SSH_KEY_FILE=~/.ssh/corfu_remote python3 profile_replicas.py \\
      --hosts-file cluster_hosts_clemson.json --delay-sec 30 --duration-sec 5 --out prof/

Writes <out>/<role>.txt: per-thread CPU share (top -H) plus the hottest symbols per
thread (perf report --sort pid,symbol, self time). Installs perf if missing.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import sys
import time

from gcp_bench_common import load_hosts_file, ssh_run

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def _profile(host: str, user: str, key, duration: int, top: int) -> str:
    cmd = f"""
set -e
command -v perf >/dev/null && perf --version >/dev/null 2>&1 || \
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq linux-tools-$(uname -r) linux-tools-common >/dev/null
sudo sysctl -qw kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0
P=$(pgrep -f '[b]ench/replica' | head -1)
[ -n "$P" ] || {{ echo 'no bench/replica running'; exit 0; }}
echo "== threads (top -H, %CPU) =="
top -b -H -n 1 -p $P | sed -n '7,20p'
sudo perf record -q -F 999 -g -p $P -o /tmp/replica.perf -- sleep {duration} >/dev/null 2>&1
echo "== hottest symbols (self time) by thread =="
sudo perf report -i /tmp/replica.perf --no-children --sort pid,symbol --stdio -g none 2>/dev/null \
  | grep -v '^#' | grep -v '^$' | head -{top} | cut -c1-200
"""
    r = ssh_run(host, user, key, cmd)
    return (r.stdout or "") + (r.stderr or "")


def main() -> None:
    ap = argparse.ArgumentParser(description="perf-profile replicas on all receivers.")
    ap.add_argument("--hosts-file", default=os.path.join(_SCRIPT_DIR, "cluster_hosts.json"))
    ap.add_argument("--ssh-user", default=os.environ.get("SSH_USER", ""))
    ap.add_argument("--ssh-key", default=os.environ.get("SSH_KEY_FILE", ""))
    ap.add_argument("--delay-sec", type=float, default=0.0, help="Wait before recording.")
    ap.add_argument("--duration-sec", type=int, default=5)
    ap.add_argument("--top", type=int, default=45, help="Symbol lines per host.")
    ap.add_argument("--out", default="", help="Directory for <role>.txt (default: print).")
    args = ap.parse_args()
    if not args.ssh_user:
        sys.exit("Set --ssh-user or SSH_USER")
    hosts_path = args.hosts_file
    if not os.path.isabs(hosts_path):
        hosts_path = os.path.join(_SCRIPT_DIR, hosts_path)
    hosts, _ = load_hosts_file(hosts_path)
    receivers = sorted(r for r in hosts if r.startswith("receiver"))
    if args.delay_sec > 0:
        time.sleep(args.delay_sec)
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(receivers)) as ex:
        futs = {r: ex.submit(_profile, hosts[r], args.ssh_user, args.ssh_key or None,
                             args.duration_sec, args.top) for r in receivers}
        for role in receivers:
            out = futs[role].result()
            if args.out:
                os.makedirs(args.out, exist_ok=True)
                with open(os.path.join(args.out, f"{role}.txt"), "w") as f:
                    f.write(out)
                print(f"  [{role}] -> {os.path.join(args.out, role + '.txt')}")
            else:
                print(f"===== {role} =====\n{out}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
One-time (per reservation / per reboot) preparation of bench nodes, then verification.

  1. Host tuning on every node (remote/tune_node.sh): performance governor, no deep
     C-states (they added ~30 µs to every packet's latency on CloudLab c6320).
  2. Receivers: an ext4 working dir at /mydata/dyna (remote/prepare_data_disk.sh) —
     SplinterDB can't run on CloudLab's ext3 root. Only a completely blank disk is ever
     formatted.
  3. Clock sync (sync_cluster_clocks.py): chrony by default (--clock-mode ptp|auto for
     hardware PTP). Both measured ~1.6 us worst pairwise offset on CloudLab c6320.
  4. Waits for the clocks to converge, measuring them with check_clock_sync.py, and
     exits non-zero if they don't get within --max-offset-us.

Example:
  SSH_USER=nathantu SSH_KEY_FILE=~/.ssh/corfu_remote \\
    python3 prepare_cluster.py --hosts-file cluster_hosts_clemson.json
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import subprocess
import sys
import time
from typing import List

from gcp_bench_common import load_hosts_file, scp_to_remote, ssh_run

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def _run_remote_script(host: str, user: str, key, local: str, remote: str, cmd: str) -> str:
    scp_to_remote(host, user, key, local, remote)
    r = ssh_run(host, user, key, f"chmod +x {remote} && {cmd}")
    if r.returncode != 0:
        raise RuntimeError((r.stderr or r.stdout or f"exit {r.returncode}").strip())
    return (r.stdout or "").strip()


def main() -> None:
    ap = argparse.ArgumentParser(description="Tune nodes, prepare disks, sync + verify clocks.")
    ap.add_argument("--hosts-file", default=os.path.join(_SCRIPT_DIR, "cluster_hosts.json"))
    ap.add_argument("--ssh-user", default=os.environ.get("SSH_USER", ""))
    ap.add_argument("--ssh-key", default=os.environ.get("SSH_KEY_FILE", ""))
    ap.add_argument("--clock-mode", choices=["auto", "ptp", "chrony"], default="chrony")
    ap.add_argument("--skip-tune", action="store_true")
    ap.add_argument("--skip-disk", action="store_true")
    ap.add_argument("--skip-clock", action="store_true", help="Only verify the clocks.")
    ap.add_argument("--max-offset-us", type=float, default=10.0)
    ap.add_argument("--converge-timeout-sec", type=int, default=300)
    args = ap.parse_args()
    if not args.ssh_user:
        sys.exit("Set --ssh-user or SSH_USER")
    key = args.ssh_key or None
    hosts_path = args.hosts_file
    if not os.path.isabs(hosts_path):
        hosts_path = os.path.join(_SCRIPT_DIR, hosts_path)
    hosts, _ = load_hosts_file(hosts_path)
    roles: List[str] = sorted(hosts)
    receivers = [r for r in roles if r.startswith("receiver")]

    def each(fn, which: List[str], what: str) -> None:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(which)) as ex:
            futs = {ex.submit(fn, r): r for r in which}
            failed = False
            for fut in concurrent.futures.as_completed(futs):
                role = futs[fut]
                try:
                    out = fut.result()
                    print(f"  [{role}] {what}: " + (out.splitlines()[-1] if out else "ok"))
                except Exception as e:  # noqa: BLE001 — report every host, then fail
                    failed = True
                    print(f"  [{role}] {what} FAILED: {e}", file=sys.stderr)
            if failed:
                sys.exit(1)

    if not args.skip_tune:
        print("Tuning hosts (performance governor, C-states > C1 off)...")
        each(lambda r: _run_remote_script(
            hosts[r], args.ssh_user, key,
            os.path.join(_SCRIPT_DIR, "remote", "tune_node.sh"), "~/tune_node.sh",
            "sudo ~/tune_node.sh apply"), roles, "tuned")

    if not args.skip_disk:
        print("Preparing /mydata/dyna (ext4) on receivers...")
        each(lambda r: _run_remote_script(
            hosts[r], args.ssh_user, key,
            os.path.join(_SCRIPT_DIR, "remote", "prepare_data_disk.sh"),
            "~/prepare_data_disk.sh",
            f"sudo OWNER={args.ssh_user} ~/prepare_data_disk.sh"), receivers, "disk")

    env = dict(os.environ, SSH_USER=args.ssh_user, SSH_KEY_FILE=args.ssh_key or "")
    if not args.skip_clock:
        print(f"Configuring clock sync (mode={args.clock_mode})...")
        r = subprocess.run(
            [sys.executable, os.path.join(_SCRIPT_DIR, "sync_cluster_clocks.py"),
             "--hosts-file", hosts_path, "--mode", args.clock_mode],
            env=env, capture_output=True, text=True)
        if r.returncode != 0:
            sys.exit(f"sync_cluster_clocks.py failed:\n{r.stdout}\n{r.stderr}")
        print("  clock daemons configured")

    print(f"Measuring clocks until |offset| <= {args.max_offset_us} µs "
          f"(timeout {args.converge_timeout_sec}s)...")
    deadline = time.time() + args.converge_timeout_sec
    while True:
        r = subprocess.run(
            [sys.executable, os.path.join(_SCRIPT_DIR, "check_clock_sync.py"),
             "--hosts-file", hosts_path, "--max-offset-us", str(args.max_offset_us),
             "--count", "1000"],
            env=env, capture_output=True, text=True)
        last = r.stdout.strip().splitlines()[-1] if r.stdout.strip() else r.stderr.strip()
        print(f"  {last}")
        if r.returncode == 0:
            print(r.stdout)
            print("Cluster ready.")
            return
        if time.time() > deadline:
            print(r.stdout)
            print(r.stderr, file=sys.stderr)
            sys.exit("Clocks did not converge within the limit.")
        time.sleep(15)


if __name__ == "__main__":
    main()

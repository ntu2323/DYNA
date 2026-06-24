#!/usr/bin/env python3
"""
Deploy clock_sync.sh to every host in cluster_hosts.json and configure PTP.

receiver1 (default) runs as PTP grandmaster; all other hosts run as followers.

Example:
  SSH_USER=nathantu SSH_KEY_FILE=~/.ssh/corfu_remote \\
    python3 sync_cluster_clocks.py --hosts-file cluster_hosts.json

  python3 sync_cluster_clocks.py --hosts-file cluster_hosts.json --status
  python3 sync_cluster_clocks.py --hosts-file cluster_hosts.json --stop
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

from gcp_bench_common import load_hosts_file, scp_to_remote, ssh_run

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_CLOCK_SYNC_SCRIPT = os.path.join(_SCRIPT_DIR, "clock_sync.sh")
_REMOTE_SCRIPT = "~/clock_sync.sh"
_DEFAULT_MASTER_ROLE = "receiver1"
_MASTER_SETTLE_SEC = 5


def _ssh_error_message(r) -> str:
    parts = []
    if (r.stderr or "").strip():
        parts.append(r.stderr.strip())
    if (r.stdout or "").strip():
        parts.append(r.stdout.strip())
    if not parts:
        parts.append(f"ssh exit {r.returncode}")
    return ": ".join(parts)


def _sorted_roles(hosts: Dict[str, str]) -> List[str]:
    receivers = sorted(k for k in hosts if k.startswith("receiver"))
    senders = sorted(k for k in hosts if k.startswith("sender"))
    return receivers + senders


def _deploy_script(
    role: str,
    host: str,
    ssh_user: str,
    ssh_key: Optional[str],
    local_script: str,
) -> Tuple[str, bool, str]:
    try:
        scp_to_remote(host, ssh_user, ssh_key, local_script, _REMOTE_SCRIPT)
    except Exception as e:
        return role, False, f"scp failed: {e}"
    r1 = ssh_run(host, ssh_user, ssh_key, f"chmod +x {_REMOTE_SCRIPT}")
    if r1.returncode != 0:
        return role, False, f"chmod failed: {_ssh_error_message(r1)}"
    return role, True, ""


def _run_clock_sync(
    role: str,
    host: str,
    ssh_user: str,
    ssh_key: Optional[str],
    ptp_role: str,
    exp_prefix: str,
) -> Tuple[str, bool, str]:
    cmd = (
        f"sudo EXP_PREFIX={exp_prefix} bash {_REMOTE_SCRIPT} {ptp_role}"
    )
    r = ssh_run(host, ssh_user, ssh_key, cmd)
    if r.returncode != 0:
        return role, False, _ssh_error_message(r)
    out = (r.stdout or "").strip()
    return role, True, out


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Deploy clock_sync.sh and configure PTP on all cluster VMs."
    )
    ap.add_argument("--hosts-file", default=os.path.join(_SCRIPT_DIR, "cluster_hosts.json"))
    ap.add_argument("--ssh-user", default=os.environ.get("SSH_USER", ""))
    ap.add_argument("--ssh-key", default=os.environ.get("SSH_KEY_FILE", ""))
    ap.add_argument(
        "--master-role",
        default=_DEFAULT_MASTER_ROLE,
        help=f"Host role that runs PTP master (default: {_DEFAULT_MASTER_ROLE}).",
    )
    ap.add_argument(
        "--exp-prefix",
        default=os.environ.get("EXP_PREFIX", "10.10."),
        help="Experiment subnet prefix for NIC selection (clock_sync.sh EXP_PREFIX).",
    )
    ap.add_argument(
        "--master-settle-sec",
        type=int,
        default=_MASTER_SETTLE_SEC,
        help="Seconds to wait after starting master before followers.",
    )
    ap.add_argument(
        "--status",
        action="store_true",
        help="Run clock_sync.sh status on all hosts (no deploy).",
    )
    ap.add_argument(
        "--stop",
        action="store_true",
        help="Run clock_sync.sh stop on all hosts (tear down PTP).",
    )
    ap.add_argument(
        "--skip-deploy",
        action="store_true",
        help="Skip scp; assume clock_sync.sh already exists at ~/clock_sync.sh.",
    )
    args = ap.parse_args()

    if not args.ssh_user:
        sys.exit("Set --ssh-user or SSH_USER")
    if not os.path.isfile(_CLOCK_SYNC_SCRIPT):
        sys.exit(f"Missing {_CLOCK_SYNC_SCRIPT}")

    hosts_path = args.hosts_file
    if not os.path.isabs(hosts_path):
        hosts_path = os.path.join(_SCRIPT_DIR, hosts_path)
    hosts, _internal = load_hosts_file(hosts_path)
    roles = _sorted_roles(hosts)

    if args.master_role not in hosts:
        sys.exit(f"--master-role {args.master_role!r} not in hosts file")
    followers = [r for r in roles if r != args.master_role]

    ssh_key = args.ssh_key or None

    if args.status or args.stop:
        action = "status" if args.status else "stop"
        print(f"Running clock_sync.sh {action} on {len(roles)} hosts...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(roles)) as ex:
            futs = [
                ex.submit(
                    _run_clock_sync,
                    role,
                    hosts[role],
                    args.ssh_user,
                    ssh_key,
                    action,
                    args.exp_prefix,
                )
                for role in roles
            ]
            for fut in concurrent.futures.as_completed(futs):
                role, ok, msg = fut.result()
                print(f"\n=== {role} ({hosts[role]}) ===")
                if ok:
                    print(msg or "(ok)")
                else:
                    print(f"FAILED: {msg}", file=sys.stderr)
                    sys.exit(1)
        return

    if not args.skip_deploy:
        print(f"Deploying {_CLOCK_SYNC_SCRIPT} -> {_REMOTE_SCRIPT} on {len(roles)} hosts...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(roles)) as ex:
            futs = [
                ex.submit(
                    _deploy_script,
                    role,
                    hosts[role],
                    args.ssh_user,
                    ssh_key,
                    _CLOCK_SYNC_SCRIPT,
                )
                for role in roles
            ]
            for fut in concurrent.futures.as_completed(futs):
                role, ok, err = fut.result()
                if not ok:
                    sys.exit(f"Deploy failed on {role}: {err}")
                print(f"  [{role}] deployed")

    master_host = hosts[args.master_role]
    print(
        f"Starting PTP master on {args.master_role} ({master_host}) "
        f"EXP_PREFIX={args.exp_prefix!r}..."
    )
    role, ok, out = _run_clock_sync(
        args.master_role,
        master_host,
        args.ssh_user,
        ssh_key,
        "master",
        args.exp_prefix,
    )
    if not ok:
        sys.exit(f"Master failed on {args.master_role}: {out}")
    if out:
        print(out)

    if args.master_settle_sec > 0:
        print(f"Waiting {args.master_settle_sec}s for master to settle...")
        time.sleep(args.master_settle_sec)

    print(f"Starting PTP followers on {len(followers)} hosts...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(followers)) as ex:
        futs = [
            ex.submit(
                _run_clock_sync,
                role,
                hosts[role],
                args.ssh_user,
                ssh_key,
                "follower",
                args.exp_prefix,
            )
            for role in followers
        ]
        for fut in concurrent.futures.as_completed(futs):
            role, ok, out = fut.result()
            if not ok:
                sys.exit(f"Follower failed on {role}: {out}")
            print(f"  [{role}] follower configured")
            if out:
                for line in out.splitlines():
                    print(f"    {line}")

    print("\nClock sync complete.")
    print(f"  Master:    {args.master_role} ({master_host})")
    print(f"  Followers: {', '.join(followers)}")
    print(f"  Remote script: {_REMOTE_SCRIPT} on each host")
    print(
        f"Check status: SSH_USER={args.ssh_user} python3 sync_cluster_clocks.py "
        f"--hosts-file {args.hosts_file} --status"
    )
    print(f"On a VM directly: sudo ~/clock_sync.sh status")


if __name__ == "__main__":
    main()

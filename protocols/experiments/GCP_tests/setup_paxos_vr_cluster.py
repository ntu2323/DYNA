#!/usr/bin/env python3
"""
Set up an existing GCP bench cluster for classic NOPaxos VR (protocols/src/paxos/vr).

Each VM git-clones git@github.com:ntu2323/IONIA.git using SSH keys from
secrets/gcp_secrets.py, then builds under nohup (SSH drops won't stop the build).

Example:
  SSH_USER=ubuntu python3 setup_paxos_vr_cluster.py --hosts-file cluster_hosts.json

Optional: --rsync-from /path/to/Ionia for a local tree instead of git.

If you disconnect, check progress on a VM:
  tail -f ~/paxos_vr_setup.log
  test -f ~/bench/.setup_paxos_done && echo ok
"""

from __future__ import annotations

import argparse
import concurrent.futures
import importlib.util
import json
import os
import sys
from typing import Dict, List, Optional, Tuple

import bench_trace
import gcp_remote_setup
import gcp_setup_log

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_GCP_SECRETS_PATH = os.path.join(_SCRIPT_DIR, "secrets", "gcp_secrets.py")
_REMOTE_SETUP_SCRIPT = os.path.join(_SCRIPT_DIR, "remote", "setup_paxos_vr.sh")
_REMOTE_LOG = "~/paxos_vr_setup.log"
_DONE_MARKER = "~/bench/.setup_paxos_done"
_FAIL_MARKER = "~/bench/.setup_paxos_failed"

NUM_RECEIVERS = 3
VR_PORT = 31880


def _load_gcp_secrets():
    if not os.path.isfile(_GCP_SECRETS_PATH):
        sys.exit(f"Missing {_GCP_SECRETS_PATH}")
    spec = importlib.util.spec_from_file_location("gcp_secrets", _GCP_SECRETS_PATH)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)  # type: ignore[arg-type]
    return mod


def _load_hosts_file(path: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    hosts = data.get("hosts", data)
    internal = data.get("receiver_internal", {})
    receivers = {k: v for k, v in hosts.items() if k.startswith("receiver")}
    senders = {k: v for k, v in hosts.items() if k.startswith("sender")}
    if len(receivers) != NUM_RECEIVERS:
        sys.exit(f"Expected {NUM_RECEIVERS} receivers, got {len(receivers)}")
    if not senders:
        sys.exit("No senders in hosts file")
    if not internal:
        sys.exit("Missing receiver_internal in hosts file")
    return hosts, internal


def _vr_config(ips: List[str], port: int) -> str:
    lines = ["f 1"]
    for ip in ips:
        lines.append(f"replica {ip}:{port}")
    return "\n".join(lines) + "\n"


def _replica_index(role: str) -> Optional[int]:
    if not role.startswith("receiver"):
        return None
    return int(role.replace("receiver", "")) - 1


def main() -> None:
    ap = argparse.ArgumentParser(description="Set up GCP cluster for NOPaxos paxos/vr baseline.")
    ap.add_argument("--hosts-file", default=os.path.join(_SCRIPT_DIR, "cluster_hosts.json"))
    ap.add_argument("--ssh-user", default=os.environ.get("SSH_USER", ""))
    ap.add_argument("--ssh-key", default=os.environ.get("SSH_KEY_FILE", ""))
    ap.add_argument(
        "--rsync-from",
        default=os.environ.get("IONIA_REPO_ROOT", ""),
        help="Rsync local Ionia tree instead of git clone (dev only).",
    )
    ap.add_argument(
        "--git-ref",
        default=os.environ.get("IONIA_GIT_REF", ""),
        help="Git branch/tag/commit to checkout after clone (default: remote HEAD).",
    )
    ap.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip apt/folly bootstrap; git pull IONIA (or use --rsync-from) and incremental make.",
    )
    ap.add_argument(
        "--no-wait",
        action="store_true",
        help="Launch remote setup and exit (do not poll for completion).",
    )
    ap.add_argument(
        "--setup-timeout",
        type=int,
        default=7200,
        help="Seconds to wait for remote setup (default 7200).",
    )
    ap.add_argument(
        "--poll-interval",
        type=int,
        default=30,
        help="Seconds between remote status checks (default 30).",
    )
    ap.add_argument(
        "--log-dir",
        default="",
        help="Directory for per-host setup logs (default: GCP_tests/logs/setup_paxos_<timestamp>).",
    )
    args = ap.parse_args()

    log_dir = args.log_dir or gcp_setup_log.make_log_dir(_SCRIPT_DIR, "setup_paxos")
    print(f"Local setup logs: {log_dir}")

    if not args.ssh_user:
        sys.exit("Set --ssh-user or SSH_USER")
    if not os.path.isfile(_REMOTE_SETUP_SCRIPT):
        sys.exit(f"Missing {_REMOTE_SETUP_SCRIPT}")

    secrets = _load_gcp_secrets()
    ssh_private = getattr(secrets, "SSH_PRIVATE_KEY", "").strip()
    ssh_public = getattr(secrets, "SSH_PUBLIC_KEY", "").strip()
    if not ssh_private or not ssh_public:
        sys.exit(
            "SSH_PRIVATE_KEY and SSH_PUBLIC_KEY required in secrets/gcp_secrets.py "
            "(deploy key must be authorized on github.com/ntu2323/IONIA)"
        )

    hosts_path = args.hosts_file
    if not os.path.isabs(hosts_path):
        hosts_path = os.path.join(_SCRIPT_DIR, hosts_path)
    hosts, receiver_internal = _load_hosts_file(hosts_path)

    receiver_roles = sorted(k for k in hosts if k.startswith("receiver"))
    sender_roles = sorted(k for k in hosts if k.startswith("sender"))
    all_roles = receiver_roles + sender_roles

    vrconfig = _vr_config([receiver_internal[r] for r in receiver_roles], VR_PORT)
    num_keys = int(os.environ.get("BENCH_TRACE_NUM_KEYS", bench_trace.DEFAULT_TRACE_NUM_KEYS))
    trace = bench_trace.trace_micro_bytes(bench_trace.build_write_trace_spec(num_keys))
    bundle_files = {"vrconfig": vrconfig, bench_trace.TRACE_MICRO_FILENAME: trace}

    ssh_key = args.ssh_key or None
    rsync_path = args.rsync_from.strip()
    if rsync_path:
        if not os.path.isdir(rsync_path):
            sys.exit(f"--rsync-from not a directory: {rsync_path}")
        rsync_root = os.path.abspath(rsync_path)
        use_git = False
    else:
        rsync_root = None
        use_git = True
    git_ref = args.git_ref.strip()

    print("Waiting for SSH...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(all_roles)) as ex:
        futs = {
            ex.submit(gcp_remote_setup.wait_for_ssh, hosts[r], args.ssh_user, ssh_key): r
            for r in all_roles
        }
        for fut in concurrent.futures.as_completed(futs):
            if not fut.result():
                sys.exit(f"SSH timeout for {futs[fut]}")

    if args.skip_build:
        mode = (
            f"git pull + make ({gcp_remote_setup.IONIA_GIT_URL})"
            if use_git
            else "rsync + incremental make"
        )
    else:
        mode = (
            f"git clone {gcp_remote_setup.IONIA_GIT_URL}"
            if use_git
            else "rsync + full build"
        )
    print(f"Launching remote setup on {len(all_roles)} VM(s) ({mode})...")

    def _deploy(role: str) -> Tuple[str, str]:
        host = hosts[role]
        pid = gcp_remote_setup.deploy_role(
            host,
            role,
            _replica_index(role),
            args.ssh_user,
            ssh_key,
            ssh_private,
            ssh_public,
            bundle_files,
            _REMOTE_SETUP_SCRIPT,
            _REMOTE_LOG,
            rsync_from=rsync_root,
            skip_build=args.skip_build,
            use_git=use_git,
            git_ref=git_ref,
        )
        return role, pid

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(all_roles)) as ex:
        futs = [ex.submit(_deploy, role) for role in all_roles]
        for fut in concurrent.futures.as_completed(futs):
            role, pid = fut.result()
            print(f"  [{role}] remote setup pid={pid} log={_REMOTE_LOG}")

    if args.no_wait:
        print("\nRemote setup running under nohup on each VM.")
        print(f"  tail -f {_REMOTE_LOG}")
        print(f"  test -f {_DONE_MARKER}")
        print("Re-run without --no-wait to poll, or check markers manually.")
        return

    print(f"Waiting for remote setup (timeout {args.setup_timeout}s)...")
    try:
        gcp_remote_setup.wait_for_all(
            {r: hosts[r] for r in all_roles},
            args.ssh_user,
            ssh_key,
            _DONE_MARKER,
            _FAIL_MARKER,
            _REMOTE_LOG,
            timeout_sec=args.setup_timeout,
            poll_sec=args.poll_interval,
            log_dir=log_dir,
        )
    except RuntimeError as e:
        sys.exit(str(e))

    print("\n=== NOPaxos VR (paxos/vr) setup complete ===\n")
    print(f"Local logs: {log_dir}")
    print(f"Remote setup log (each VM): {_REMOTE_LOG}")
    print("Baseline: protocols/src/paxos/vr + UDPTransport (not ionia/vr).")
    print("  Receivers: ~/bench/start_paxos_vr_replica.sh <0|1|2>")
    print("  Senders:   ~/bench/run_paxos_vr_client.sh [threads] [ops] [secs] [warmup]")
    print(f"  Config:    ~/bench/vrconfig (replicas on internal IPs, port {VR_PORT})")
    print("Open GCP firewall UDP", VR_PORT, "between cluster VMs.")


if __name__ == "__main__":
    main()

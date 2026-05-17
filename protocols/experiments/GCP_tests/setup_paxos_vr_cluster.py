#!/usr/bin/env python3
"""
Set up an existing GCP bench cluster for classic NOPaxos VR (protocols/src/paxos/vr).

Uploads source (rsync), drops a setup bundle + bash script on each VM, and launches
setup under nohup so a dropped SSH session does not stop the long build.

Example:
  SSH_USER=ubuntu python3 setup_paxos_vr_cluster.py --hosts-file cluster_hosts.json

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

import gcp_remote_setup
import gcp_setup_log

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_GCP_SECRETS_PATH = os.path.join(_SCRIPT_DIR, "secrets", "gcp_secrets.py")
_REMOTE_SETUP_SCRIPT = os.path.join(_SCRIPT_DIR, "remote", "setup_paxos_vr.sh")
_REMOTE_LOG = "~/paxos_vr_setup.log"
_DONE_MARKER = "~/bench/.setup_paxos_done"
_FAIL_MARKER = "~/bench/.setup_paxos_failed"
_DEFAULT_IONIA_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", "..", ".."))

NUM_RECEIVERS = 3
VR_PORT = 31880


def _paxos_trace_bytes(num_ops: int = 10) -> str:
    spec = [
        ("I", "user10000000000000000001"),
        ("U", "user20000000000000000002"),
        ("I", "user30000000000000000003"),
        ("R", "user30000000000000000003"),
        ("U", "user10000000000000000001"),
        ("I", "user20000000000000000002"),
        ("U", "user30000000000000000003"),
        ("R", "user10000000000000000001"),
        ("I", "user40000000000000000004"),
        ("U", "user40000000000000000004"),
    ]
    out = []
    for op, key in spec[:num_ops]:
        k = (key + " " * 24)[:24]
        out.append(f"{op} {k} ")
    return "".join(out)


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
    ap.add_argument("--rsync-from", default=os.environ.get("IONIA_REPO_ROOT", _DEFAULT_IONIA_ROOT))
    ap.add_argument("--git-build", action="store_true", help="Clone Ionia on VM instead of rsync.")
    ap.add_argument("--skip-build", action="store_true", help="Config/helpers only (repo must exist).")
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
        sys.exit("SSH_PRIVATE_KEY and SSH_PUBLIC_KEY required in gcp_secrets.py")

    hosts_path = args.hosts_file
    if not os.path.isabs(hosts_path):
        hosts_path = os.path.join(_SCRIPT_DIR, hosts_path)
    hosts, receiver_internal = _load_hosts_file(hosts_path)

    receiver_roles = sorted(k for k in hosts if k.startswith("receiver"))
    sender_roles = sorted(k for k in hosts if k.startswith("sender"))
    all_roles = receiver_roles + sender_roles

    vrconfig = _vr_config([receiver_internal[r] for r in receiver_roles], VR_PORT)
    trace = _paxos_trace_bytes()
    bundle_files = {"vrconfig": vrconfig, "trace.micro": trace}

    ssh_key = args.ssh_key or None
    use_rsync = (
        bool(args.rsync_from)
        and not args.git_build
        and not args.skip_build
        and os.path.isdir(args.rsync_from)
    )
    rsync_root = os.path.abspath(args.rsync_from) if use_rsync else None
    use_git = bool(args.git_build) and not args.skip_build

    print("Waiting for SSH...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(all_roles)) as ex:
        futs = {
            ex.submit(gcp_remote_setup.wait_for_ssh, hosts[r], args.ssh_user, ssh_key): r
            for r in all_roles
        }
        for fut in concurrent.futures.as_completed(futs):
            if not fut.result():
                sys.exit(f"SSH timeout for {futs[fut]}")

    mode = "wire-only" if args.skip_build else ("git" if use_git else "rsync+nohup")
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

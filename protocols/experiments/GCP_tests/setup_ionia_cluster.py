#!/usr/bin/env python3
"""
Set up an existing bench cluster for IONIA (protocols/src/ionia, -m vr:
VRReplica + VRDurabilityReplica).

This is the ionia/vr system, NOT the paxos/src "vr" baseline. It reuses the same
ionia build as DYNA but launches with -m vr and a TWO-config layout:

  ioniaconfig         internal consensus config (VRReplica <-> VRReplica),  port 31880
  ioniaconfig.client  client-facing durability config (clients/durability), port 31882

Both files share the same replicas and f, but use different UDP ports because the
internal consensus transport and the client-facing durability transport run on the
same hosts. The replica reads ioniaconfig (-c) and auto-loads ioniaconfig.client.

Each VM git-clones git@github.com:ntu2323/IONIA.git (SSH keys from gcp_secrets.py),
then builds under nohup. Use --rsync-from for a local tree instead.

Example:
  SSH_USER=ubuntu python3 setup_ionia_cluster.py --hosts-file cluster_hosts.json
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
_REMOTE_SETUP_SCRIPT = os.path.join(_SCRIPT_DIR, "remote", "setup_ionia.sh")
_REMOTE_LOG = "~/ionia_setup.log"
_DONE_MARKER = "~/bench/.setup_ionia_done"
_FAIL_MARKER = "~/bench/.setup_ionia_failed"

NUM_RECEIVERS = 3
# Internal consensus port and client-facing durability port (must differ: both run
# on the same hosts). Client port 31850 matches the canonical IONIA harness.
# Keep in sync with remote/setup_ionia.sh REPLICA_PORT/CLIENT_PORT.
REPLICA_PORT = 31880
CLIENT_PORT = 31850
# Durability threads (-j on bench/replica).
DURABILITY_THREADS = int(os.environ.get("IONIA_DURABILITY_THREADS", "1"))


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
    if len([k for k in hosts if k.startswith("receiver")]) != NUM_RECEIVERS:
        sys.exit(f"Need {NUM_RECEIVERS} receivers in hosts file")
    if not internal:
        sys.exit("Missing receiver_internal in hosts file")
    return hosts, internal


def _replica_config(ips: List[str], port: int) -> str:
    lines = ["f 1"]
    for ip in ips:
        lines.append(f"replica {ip}:{port}")
    return "\n".join(lines) + "\n"


def _replica_index(role: str) -> Optional[int]:
    if not role.startswith("receiver"):
        return None
    return int(role.replace("receiver", "")) - 1


def main() -> None:
    ap = argparse.ArgumentParser(description="Set up cluster for IONIA (ionia -m vr).")
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
        help="Git branch/tag/commit after clone (default: remote HEAD).",
    )
    ap.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip apt/folly bootstrap; git pull IONIA (or use --rsync-from) and incremental make.",
    )
    ap.add_argument("--no-wait", action="store_true")
    ap.add_argument("--setup-timeout", type=int, default=7200)
    ap.add_argument("--poll-interval", type=int, default=30)
    ap.add_argument(
        "--log-dir",
        default="",
        help="Directory for per-host setup logs (default: GCP_tests/logs/setup_ionia_<timestamp>).",
    )
    args = ap.parse_args()

    log_dir = args.log_dir or gcp_setup_log.make_log_dir(_SCRIPT_DIR, "setup_ionia")
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

    hosts_path = args.hosts_file if os.path.isabs(args.hosts_file) else os.path.join(_SCRIPT_DIR, args.hosts_file)
    hosts, receiver_internal = _load_hosts_file(hosts_path)
    receiver_roles = sorted(k for k in hosts if k.startswith("receiver"))
    sender_roles = sorted(k for k in hosts if k.startswith("sender"))
    all_roles = receiver_roles + sender_roles

    receiver_ips = [receiver_internal[r] for r in receiver_roles]
    internal_config = _replica_config(receiver_ips, REPLICA_PORT)
    client_config = _replica_config(receiver_ips, CLIENT_PORT)
    num_keys = int(os.environ.get("BENCH_TRACE_NUM_KEYS", bench_trace.DEFAULT_TRACE_NUM_KEYS))
    trace = bench_trace.trace_micro_bytes(bench_trace.build_write_trace_spec(num_keys))
    bundle_files = {
        "ioniaconfig": internal_config,
        "ioniaconfig.client": client_config,
        bench_trace.TRACE_MICRO_FILENAME: trace,
    }

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
            ex.submit(gcp_remote_setup.wait_for_ssh, hosts[r], args.ssh_user, ssh_key): r for r in all_roles
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
        pid = gcp_remote_setup.deploy_role(
            hosts[role],
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

    print("\n=== IONIA setup complete ===\n")
    print(f"Local logs: {log_dir}")
    print(f"Remote setup log (each VM): {_REMOTE_LOG}")
    print("System: protocols/src/ionia -m vr (VRReplica + VRDurabilityReplica).")
    print("  Receivers: ~/bench/start_ionia_replica.sh <0|1|2>")
    print("  Senders:   ~/bench/run_ionia_client.sh <client_id> [threads] [ops] [secs] [warmup]")
    print(f"  Configs:   ~/bench/ioniaconfig (internal, port {REPLICA_PORT})")
    print(f"             ~/bench/ioniaconfig.client (client-facing, port {CLIENT_PORT})")
    print(f"  Replicas:  -j {DURABILITY_THREADS}")
    print("Open firewall UDP", REPLICA_PORT, "and", CLIENT_PORT, "between cluster VMs.")


if __name__ == "__main__":
    main()

"""Shared helpers for GCP cluster benchmark orchestration and aggregation."""

from __future__ import annotations

import json
import os
import re
import subprocess
from typing import Dict, List, Optional, Tuple

NUM_RECEIVERS = 3

# Bracket trick: avoid pkill -f matching the ssh remote command line (contains "bench/replica").
PKILL_REPLICA = (
    "pkill -f '[b]ench/replica' 2>/dev/null || true; "
    "for _ in 1 2 3 4 5; do "
    "pgrep -f '[b]ench/replica' >/dev/null || break; "
    "sleep 1; "
    "pkill -9 -f '[b]ench/replica' 2>/dev/null || true; "
    "done"
)
PKILL_CLIENT = "pkill -f '[b]ench/client' 2>/dev/null || true"
PGREP_CLIENT = "pgrep -f '[b]ench/client' >/dev/null"

def remote_sender_cleanup_cmd() -> str:
    """Stop clients and remove prior run artifacts (logs, latencies) on a sender VM."""
    # Avoid bare glob (fails when runs/ is empty). Do not combine with pkill on one ssh -c
    # line that also mentions bench/client paths (same self-match issue as receivers).
    return (
        f"{PKILL_CLIENT}; sleep 1; "
        "find $HOME/bench/runs -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null || true"
    )


def remote_receiver_cleanup_cmd(
    protocol: str,
    replica_idx: int,
) -> str:
    """Stop replicas and wipe durable bench state for a clean run.

    Must not embed ``bench/replica`` in the same remote shell command as PKILL_REPLICA:
    pkill -f matches the ssh -c argv and kills the session (ssh exit 255).
    Replica logs are truncated by start_*_replica.sh on start.
    """
    parts = [PKILL_REPLICA, "sleep 1"]
    if protocol in ("vr", "ionia"):
        # vr baseline (paxos) and ionia (ionia -m vr) both back AppReplica with the
        # rocks/in-mem KV under /dev/shm/rocks<idx>; wipe it for a clean run.
        parts.append(f"rm -rf /dev/shm/rocks{replica_idx}")
    elif protocol == "dyna":
        # DYNA's store is the harness SplinterDB file in the replica's working dir;
        # remove it so each run starts empty (the replica then creates a fresh one).
        parts.append("rm -f $HOME/Ionia/protocols/src/ionia/splinterdb.db")
        # ...and its persisted VR state: a leftover dyna-vr-<idx>.state makes a replica
        # start as a REJOINER (it believes it crashed) instead of in a fresh view 0.
        parts.append("rm -f $HOME/Ionia/protocols/src/ionia/dyna-vr-*.state "
                     "$HOME/Ionia/protocols/src/ionia/dyna-vr-*.state.tmp")
    return "; ".join(parts)


def ssh_common_opts() -> List[str]:
    return [
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "LogLevel=ERROR",
    ]


def ssh_run(
    host: str,
    ssh_user: str,
    ssh_key: Optional[str],
    remote_cmd: str,
) -> subprocess.CompletedProcess:
    ssh_cmd = ["ssh", *ssh_common_opts()]
    if ssh_key:
        ssh_cmd.extend(["-i", ssh_key])
    ssh_cmd.extend([f"{ssh_user}@{host}", remote_cmd])
    return subprocess.run(
        ssh_cmd,
        capture_output=True,
        text=True,
        check=False,
    )


def ionia_replica_ready_marker(replica_idx: int, durability_threads: int) -> str:
    return (
        f"IONIA_REPLICA_READY durability_threads={durability_threads} "
        f"replica_index={replica_idx}"
    )


def wait_ionia_replica_ready(
    role: str,
    host: str,
    replica_idx: int,
    durability_threads: int,
    ssh_user: str,
    ssh_key: Optional[str],
    timeout_sec: int,
) -> tuple[str, bool, str]:
    """Poll replica log until IONIA_REPLICA_READY or timeout / process exit."""
    marker = ionia_replica_ready_marker(replica_idx, durability_threads)
    log_path = f"~/bench/replica-{replica_idx}.log"
    cmd = (
        f"for i in $(seq 1 {timeout_sec}); do "
        f"grep -Fq '{marker}' {log_path} 2>/dev/null && exit 0; "
        f"pgrep -f '[b]ench/replica' >/dev/null || "
        f"{{ echo 'replica process exited before ready'; exit 2; }}; "
        f"sleep 1; done; "
        f"echo 'timeout waiting for {marker}'; exit 1"
    )
    r = ssh_run(host, ssh_user, ssh_key, cmd)
    ok = r.returncode == 0
    err_parts = []
    if (r.stderr or "").strip():
        err_parts.append(r.stderr.strip())
    if (r.stdout or "").strip():
        err_parts.append(r.stdout.strip())
    if not err_parts:
        err_parts.append(f"ssh exit {r.returncode}")
    return role, ok, ": ".join(err_parts)


def scp_from_remote(
    host: str,
    ssh_user: str,
    ssh_key: Optional[str],
    remote_path: str,
    local_path: str,
) -> None:
    """remote_path must use ~/... (scp does not expand $HOME on the remote)."""
    cmd = ["scp", *ssh_common_opts()]
    if ssh_key:
        cmd.extend(["-i", ssh_key])
    cmd.extend([f"{ssh_user}@{host}:{remote_path}", local_path])
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def scp_to_remote(
    host: str,
    ssh_user: str,
    ssh_key: Optional[str],
    local_path: str,
    remote_path: str,
) -> None:
    """remote_path must use ~/... (scp does not expand $HOME on the remote)."""
    cmd = ["scp", *ssh_common_opts()]
    if ssh_key:
        cmd.extend(["-i", ssh_key])
    cmd.extend([local_path, f"{ssh_user}@{host}:{remote_path}"])
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def fetch_remote_file(
    host: str,
    ssh_user: str,
    ssh_key: Optional[str],
    remote_path: str,
    local_path: str,
    *,
    binary: bool = False,
) -> None:
    """Copy a remote file via scp (~ paths), falling back to ssh cat / base64."""
    try:
        scp_from_remote(host, ssh_user, ssh_key, remote_path, local_path)
        return
    except subprocess.CalledProcessError:
        pass

    if binary:
        import base64

        r = ssh_run(host, ssh_user, ssh_key, f"base64 {remote_path}")
        if r.returncode != 0:
            err = (r.stderr or r.stdout or "").strip() or f"ssh exit {r.returncode}"
            raise subprocess.CalledProcessError(r.returncode, f"fetch {remote_path}", err)
        with open(local_path, "wb") as f:
            f.write(base64.b64decode("".join(r.stdout.split())))
        return

    r = ssh_run(host, ssh_user, ssh_key, f"cat {remote_path}")
    if r.returncode != 0:
        err = (r.stderr or r.stdout or "").strip() or f"ssh exit {r.returncode}"
        raise subprocess.CalledProcessError(r.returncode, f"fetch {remote_path}", err)
    with open(local_path, "w", encoding="utf-8") as f:
        f.write(r.stdout)


def load_hosts_file(path: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    hosts = data.get("hosts", data)
    internal = data.get("receiver_internal", {})
    receivers = {k: v for k, v in hosts.items() if k.startswith("receiver")}
    senders = {k: v for k, v in hosts.items() if k.startswith("sender")}
    if len(receivers) != NUM_RECEIVERS:
        raise SystemExit(f"Expected {NUM_RECEIVERS} receivers, got {len(receivers)}")
    if not senders:
        raise SystemExit("No senders in hosts file")
    if not internal:
        raise SystemExit("Missing receiver_internal in hosts file")
    return hosts, internal


def receiver_index(role: str) -> int:
    if not role.startswith("receiver"):
        raise ValueError(role)
    return int(role.replace("receiver", "")) - 1


def sender_client_id(role: str) -> int:
    if not role.startswith("sender"):
        raise ValueError(role)
    return int(role.replace("sender", ""))


# bench/client Notice lines (with optional log prefix)
INTERVAL_LINE_RE = re.compile(
    r"Completed\s+(\d+)\s+requests\s+at\s+(\d+)\s+ms",
    re.IGNORECASE,
)
FINISH_LINE_RE = re.compile(
    r"Completed\s+(\d+)\s+requests\s+in\s+([\d.]+)\s+seconds"
    r"(?:\s*\(submitted=(\d+)\))?",
    re.IGNORECASE,
)
LATENCY_LINE_RE = re.compile(
    r"(Median|Average|90th percentile|95th percentile|99th percentile)\s+latency\s+is\s+(\d+)\s+ns",
    re.IGNORECASE,
)
INFLIGHT_LINE_RE = re.compile(
    r"Completed\s+\d+\s+requests\s+at\s+\d+\s+ms\s+inflight=(\d+)\s+submitted=(\d+)",
    re.IGNORECASE,
)
DYNA_CLIENT_STATS_RE = re.compile(
    r"DYNA client stats client=\d+\s+completions=(\d+).*?bound_us=(\d+)",
    re.IGNORECASE,
)
DYNA_CLIENT_TOTAL_US_RE = re.compile(
    r"DYNA client stats client=\d+\s+total_us\s+samples=(\d+)\s+avg=(\d+)\s+p50=(\d+)\s+p99=(\d+)\s+max=(\d+)",
    re.IGNORECASE,
)
DYNA_CLIENT_FIRST_REPLY_US_RE = re.compile(
    r"DYNA client stats client=\d+\s+first_reply_us\s+samples=(\d+)\s+avg=(\d+)\s+p50=(\d+)\s+p99=(\d+)\s+max=(\d+)",
    re.IGNORECASE,
)
DYNA_CLIENT_QUORUM_WAIT_US_RE = re.compile(
    r"DYNA client stats client=\d+\s+quorum_wait_us\s+samples=(\d+)\s+avg=(\d+)\s+p50=(\d+)\s+p99=(\d+)\s+max=(\d+)",
    re.IGNORECASE,
)
DYNA_REPLICA_STATS_RE = re.compile(
    r"DYNA stats rep=(\d+)\s+incoming=(\d+)\s+early=(\d+)\s+fastQ=(\d+)\s+durQ=(\d+)\s+replQ=(\d+)\s+"
    r"releases=([\d.]+)/s\s+slowpath=([\d.]+)/s",
    re.IGNORECASE,
)

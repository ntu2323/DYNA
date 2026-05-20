"""Shared helpers for GCP cluster benchmark orchestration and aggregation."""

from __future__ import annotations

import json
import os
import re
import subprocess
from typing import Dict, List, Optional, Tuple

NUM_RECEIVERS = 3

# Bracket trick: avoid pkill -f matching the ssh remote command line (contains "bench/replica").
PKILL_REPLICA = "pkill -f '[b]ench/replica' 2>/dev/null || true"
PKILL_CLIENT = "pkill -f '[b]ench/client' 2>/dev/null || true"
PGREP_CLIENT = "pgrep -f '[b]ench/client' >/dev/null"


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
    r"Completed\s+(\d+)\s+requests\s+in\s+([\d.]+)\s+seconds",
    re.IGNORECASE,
)
LATENCY_LINE_RE = re.compile(
    r"(Median|Average|90th percentile|95th percentile|99th percentile)\s+latency\s+is\s+(\d+)\s+ns",
    re.IGNORECASE,
)

"""Shared helpers for GCP cluster benchmark orchestration and aggregation."""

from __future__ import annotations

import json
import os
import re
import subprocess
from typing import Dict, List, Optional, Tuple

NUM_RECEIVERS = 3


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
    cmd = ["scp", *ssh_common_opts()]
    if ssh_key:
        cmd.extend(["-i", ssh_key])
    cmd.extend([f"{ssh_user}@{host}:{remote_path}", local_path])
    subprocess.run(cmd, check=True, capture_output=True, text=True)


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

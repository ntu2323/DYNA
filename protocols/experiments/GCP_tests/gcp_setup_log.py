"""Local logging helpers for GCP setup scripts."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Optional


def make_log_dir(base_dir: str, prefix: str) -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(base_dir, "logs", f"{prefix}_{stamp}")
    os.makedirs(path, exist_ok=True)
    return path


def write_step_log(
    log_dir: str,
    step: str,
    host: str,
    role: str,
    stdout: str,
    stderr: str,
    returncode: int,
) -> str:
    safe_host = host.replace(".", "_")
    path = os.path.join(log_dir, f"{step}__{role}__{safe_host}.log")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"host={host}\nrole={role}\nstep={step}\nexit={returncode}\n\n")
        f.write("=== stdout ===\n")
        f.write(stdout or "")
        f.write("\n\n=== stderr ===\n")
        f.write(stderr or "")
        f.write("\n")
    return path


def log_failure_hint(remote_log: str, local_log_path: Optional[str]) -> str:
    parts = [f"Remote build log on VM: {remote_log}"]
    if local_log_path:
        parts.append(f"Local copy: {local_log_path}")
    return " | ".join(parts)

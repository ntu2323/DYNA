"""Upload setup bundles and launch disconnect-safe remote bash setup scripts."""

from __future__ import annotations

import os
import subprocess
import tempfile
import time
from typing import Dict, List, Optional

import shutil

import gcp_setup_log

IONIA_GIT_URL = "git@github.com:ntu2323/IONIA.git"


def wait_for_ssh(
    host: str, ssh_user: str, ssh_key: Optional[str], timeout_sec: int = 300
) -> bool:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        r = ssh_run(host, ssh_user, ssh_key, "exit 0")
        if r.returncode == 0:
            return True
        time.sleep(5)
    return False


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
    *,
    input_text: Optional[str] = None,
) -> subprocess.CompletedProcess:
    ssh_cmd = ["ssh", *ssh_common_opts()]
    if ssh_key:
        ssh_cmd.extend(["-i", ssh_key])
    ssh_cmd.extend([f"{ssh_user}@{host}", remote_cmd])
    return subprocess.run(
        ssh_cmd,
        input=input_text,
        capture_output=True,
        text=True,
        check=False,
    )


def scp_to(host: str, ssh_user: str, ssh_key: Optional[str], local_path: str, remote_path: str) -> None:
    cmd = ["scp", "-r", *ssh_common_opts()]
    if ssh_key:
        cmd.extend(["-i", ssh_key])
    cmd.extend([local_path, f"{ssh_user}@{host}:{remote_path}"])
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def rsync_ionia(local_root: str, host: str, ssh_user: str, ssh_key: Optional[str]) -> None:
    ssh_part = " ".join(ssh_common_opts())
    if ssh_key:
        ssh_part = f"-i {ssh_key} " + ssh_part
    subprocess.run(
        [
            "rsync",
            "-az",
            "--exclude",
            ".git",
            "--exclude",
            "protocols/src/ionia/.obj",
            "--exclude",
            "protocols/src/paxos/.obj",
            "--exclude",
            "docker/smoke-output",
            "-e",
            f"ssh {ssh_part}",
            f"{local_root.rstrip('/')}/",
            f"{ssh_user}@{host}:~/Ionia/",
        ],
        check=True,
    )


def write_bundle(
    bundle_dir: str,
    *,
    role: str,
    replica_index: Optional[int],
    ssh_private: str,
    ssh_public: str,
    extra_files: Dict[str, str],
) -> None:
    os.makedirs(bundle_dir, exist_ok=True)
    with open(os.path.join(bundle_dir, "role.txt"), "w", encoding="utf-8") as f:
        f.write(role)
    with open(os.path.join(bundle_dir, "replica_index.txt"), "w", encoding="utf-8") as f:
        f.write("" if replica_index is None else str(replica_index))
    key_path = os.path.join(bundle_dir, "id_ed25519")
    with open(key_path, "w", encoding="utf-8") as f:
        f.write(ssh_private)
        if not ssh_private.endswith("\n"):
            f.write("\n")
    os.chmod(key_path, 0o600)
    with open(os.path.join(bundle_dir, "ssh_public_line"), "w", encoding="utf-8") as f:
        f.write(ssh_public.strip() + "\n")
    for rel_path, content in extra_files.items():
        path = os.path.join(bundle_dir, rel_path)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)


def attach_ionia_repo_helper(bundle_dir: str, remote_script_path: str) -> None:
    """Copy ionia_repo.sh next to run.sh in the setup bundle."""
    helper = os.path.join(os.path.dirname(remote_script_path), "ionia_repo.sh")
    if not os.path.isfile(helper):
        raise FileNotFoundError(f"Missing {helper}")
    shutil.copy2(helper, os.path.join(bundle_dir, "ionia_repo.sh"))


def upload_and_launch(
    host: str,
    ssh_user: str,
    ssh_key: Optional[str],
    bundle_dir: str,
    remote_script: str,
    *,
    remote_log: str,
    skip_build: bool,
    use_git: bool,
    git_ref: str = "",
) -> str:
    remote_bundle = "~/bench/setup_bundle"
    ssh_run(host, ssh_user, ssh_key, f"rm -rf {remote_bundle} && mkdir -p {remote_bundle}")
    scp_to(host, ssh_user, ssh_key, bundle_dir + "/", remote_bundle + "/")
    scp_to(host, ssh_user, ssh_key, remote_script, remote_bundle + "/run.sh")
    skip = "1" if skip_build else "0"
    git_flag = "1" if use_git else "0"
    env = (
        f"SETUP_BUNDLE={remote_bundle} SKIP_BUILD={skip} USE_GIT={git_flag} "
        f"IONIA_GIT_URL={IONIA_GIT_URL}"
    )
    if git_ref:
        env += f" IONIA_GIT_REF={git_ref}"
    launch = (
        f"cd {remote_bundle} && "
        f"nohup env {env} bash run.sh >> {remote_log} 2>&1 </dev/null & echo $!"
    )
    r = ssh_run(host, ssh_user, ssh_key, launch)
    if r.returncode != 0:
        raise RuntimeError(f"Failed to launch setup on {host}:\n{r.stdout}\n{r.stderr}")
    pid = (r.stdout or "").strip().splitlines()[-1] if r.stdout else "?"
    return pid


def setup_status(host: str, ssh_user: str, ssh_key: Optional[str], done_marker: str, fail_marker: str) -> str:
    cmd = (
        f"if test -f {done_marker}; then echo DONE; "
        f"elif test -f {fail_marker}; then echo FAIL; "
        f"else echo RUNNING; fi"
    )
    r = ssh_run(host, ssh_user, ssh_key, cmd)
    if r.returncode != 0:
        return "SSH_ERROR"
    return (r.stdout or "").strip() or "UNKNOWN"


def fetch_remote_log_tail(
    host: str, ssh_user: str, ssh_key: Optional[str], remote_log: str, lines: int = 40
) -> str:
    r = ssh_run(host, ssh_user, ssh_key, f"tail -n {lines} {remote_log} 2>/dev/null || true")
    return r.stdout or ""


def wait_for_all(
    hosts_by_role: Dict[str, str],
    ssh_user: str,
    ssh_key: Optional[str],
    done_marker: str,
    fail_marker: str,
    remote_log: str,
    *,
    timeout_sec: int,
    poll_sec: int,
    log_dir: Optional[str],
) -> None:
    pending = set(hosts_by_role.keys())
    deadline = time.monotonic() + timeout_sec
    while pending and time.monotonic() < deadline:
        finished = []
        for role in sorted(pending):
            host = hosts_by_role[role]
            status = setup_status(host, ssh_user, ssh_key, done_marker, fail_marker)
            if status == "DONE":
                print(f"  [{role}] setup complete")
                finished.append(role)
            elif status in ("FAIL", "SSH_ERROR"):
                tail = fetch_remote_log_tail(host, ssh_user, ssh_key, remote_log)
                local_log = None
                if log_dir:
                    local_log = gcp_setup_log.write_step_log(
                        log_dir,
                        "remote_setup_failed",
                        host,
                        role,
                        tail,
                        f"status={status}",
                        1,
                    )
                hint = gcp_setup_log.log_failure_hint(remote_log, local_log)
                raise RuntimeError(f"Setup failed on {host} ({role}) status={status}\n{tail}\n{hint}")
        for role in finished:
            pending.discard(role)
        if pending:
            print(f"  waiting on {len(pending)} VM(s)... ({', '.join(sorted(pending))})")
            time.sleep(poll_sec)
    if pending:
        raise RuntimeError(
            f"Setup timed out after {timeout_sec}s; still pending: {', '.join(sorted(pending))}. "
            f"Check remote log: {remote_log}"
        )


def deploy_role(
    host: str,
    role: str,
    replica_index: Optional[int],
    ssh_user: str,
    ssh_key: Optional[str],
    ssh_private: str,
    ssh_public: str,
    bundle_files: Dict[str, str],
    remote_script_path: str,
    remote_log: str,
    *,
    rsync_from: Optional[str],
    skip_build: bool,
    use_git: bool,
    git_ref: str = "",
) -> str:
    if rsync_from and not skip_build and not use_git:
        print(f"  [{role}] rsync {rsync_from} -> {host}:~/Ionia/ (USE_GIT=0)")
        rsync_ionia(rsync_from, host, ssh_user, ssh_key)
    elif use_git and not skip_build:
        print(f"  [{role}] will git clone {IONIA_GIT_URL} on VM")
    with tempfile.TemporaryDirectory(prefix=f"gcp_setup_{role}_") as tmp:
        bundle_dir = os.path.join(tmp, "bundle")
        write_bundle(
            bundle_dir,
            role=role,
            replica_index=replica_index,
            ssh_private=ssh_private,
            ssh_public=ssh_public,
            extra_files=bundle_files,
        )
        attach_ionia_repo_helper(bundle_dir, remote_script_path)
        pid = upload_and_launch(
            host,
            ssh_user,
            ssh_key,
            bundle_dir,
            remote_script_path,
            remote_log=remote_log,
            skip_build=skip_build,
            use_git=use_git,
            git_ref=git_ref,
        )
    return pid

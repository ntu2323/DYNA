#!/usr/bin/env python3
"""
Create a GCP VM cluster for DYNA vs NOPaxos VR benchmarks.

Fixed replica receivers (3) + scalable senders (clients).
Use setup_paxos_vr_cluster.py or setup_dyna_cluster.py after this step.

Examples:

  python3 create_cluster.py 4

creates all VMs in deploy_vm.ZONE (default us-east4-c) — no zone overrides.

  python3 create_cluster.py 4 --spread-replica-zones

places receiver1..3 in EXTRA_REPLICA_ZONES and senders in EXTRA_CLIENT_ZONE.

This script only creates VMs and prints role -> external IP mapping (plus receiver
internal IPs for replica config). It does not install dependencies or build IONIA.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import sys
from typing import Dict, List, Optional

import deploy_vm

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REQUIREMENTS = os.path.join(_SCRIPT_DIR, "requirements-deploy.txt")

# Match create_networktp_cluster.py: replica/receiver nodes use c4-standard-2.
RECEIVER_MACHINE_TYPE = "c4-standard-2"
RECIEVER_FAMILY = "reciever1"
SENDER_FAMILY = "sender1"
IMAGE_PROJECT = "corfu-488815"
NUM_RECEIVERS = 3
# One zone per replica when --spread-replica-zones is set (receiver1 -> [0], etc.).
# Region names (e.g. europe-north1) are expanded to {region}-b automatically.
EXTRA_REPLICA_ZONES = [
    "europe-north2",
    "asia-northeast1",
    "us-east4-c",
]
# Sender zone when --spread-replica-zones is set (region or full zone).
EXTRA_CLIENT_ZONE = "us-west1"


def _normalize_gcp_zone(zone_or_region: str) -> str:
    """Accept a full zone (us-east4-c) or a region (europe-north1 -> europe-north1-b)."""
    z = zone_or_region.strip()
    if z.count("-") >= 2:
        return z
    return f"{z}-b"


def _roles(machine_count: int) -> List[str]:
    if machine_count < NUM_RECEIVERS + 1:
        raise ValueError(
            f"machine_count must be at least {NUM_RECEIVERS + 1} "
            f"({NUM_RECEIVERS} receivers + 1 sender)"
        )
    receivers = [f"receiver{i}" for i in range(1, NUM_RECEIVERS + 1)]
    num_senders = machine_count - NUM_RECEIVERS
    senders = [f"sender{i}" for i in range(1, num_senders + 1)]
    return receivers + senders


def _is_receiver(role: str) -> bool:
    return role.startswith("receiver")


def _receiver_index(role: str) -> int:
    if not _is_receiver(role):
        raise ValueError(f"not a receiver role: {role}")
    return int(role.replace("receiver", "")) - 1


def _receiver_zone(receiver_index: int, spread_replica_zones: bool) -> Optional[str]:
    """None -> deploy_vm.create_vm uses its default ZONE."""
    if not spread_replica_zones:
        return None
    if receiver_index < 0 or receiver_index >= NUM_RECEIVERS:
        raise ValueError(f"receiver index out of range: {receiver_index}")
    if len(EXTRA_REPLICA_ZONES) < NUM_RECEIVERS:
        raise ValueError(
            f"EXTRA_REPLICA_ZONES must list at least {NUM_RECEIVERS} zones "
            f"(got {len(EXTRA_REPLICA_ZONES)})"
        )
    return _normalize_gcp_zone(EXTRA_REPLICA_ZONES[receiver_index])


def _sender_zone(spread_replica_zones: bool) -> Optional[str]:
    """None -> deploy_vm default; set when --spread-replica-zones."""
    if not spread_replica_zones:
        return None
    return _normalize_gcp_zone(EXTRA_CLIENT_ZONE)


def _zone_label(zone: Optional[str]) -> str:
    return zone if zone is not None else deploy_vm.ZONE


def create_cluster(
    machine_count: int,
    name_prefix: str = "ionia",
    *,
    spread_replica_zones: bool = False,
) -> tuple[Dict[str, str], Dict[str, str], Optional[str]]:
    """Create bench VMs.

    Returns (role -> external IP, receiver role -> GCP zone used, sender zone or None).
    """
    hosts: Dict[str, str] = {}
    receiver_zones: Dict[str, str] = {}
    sender_zone = _sender_zone(spread_replica_zones)
    roles = _roles(machine_count)

    def _create_one(role: str) -> tuple[str, str, str]:
        instance_name = f"{name_prefix}-{role}"
        if _is_receiver(role):
            ridx = _receiver_index(role)
            zone = _receiver_zone(ridx, spread_replica_zones)
            zone_label = _zone_label(zone)
            print(f"Creating VM '{instance_name}' for role '{role}' in zone {zone_label}...")
            kwargs = {
                "instance_name": instance_name,
                "machine_type": RECEIVER_MACHINE_TYPE,
                "image_family": RECIEVER_FAMILY,
                "image_project": IMAGE_PROJECT,
            }
            if zone is not None:
                kwargs["zone"] = zone
            ip = deploy_vm.create_vm(**kwargs)
            return role, ip, zone_label
        zone_label = _zone_label(sender_zone)
        print(f"Creating VM '{instance_name}' for role '{role}' in zone {zone_label}...")
        kwargs = {
            "instance_name": instance_name,
            "image_family": SENDER_FAMILY,
            "image_project": IMAGE_PROJECT,
        }
        if sender_zone is not None:
            kwargs["zone"] = sender_zone
        ip = deploy_vm.create_vm(**kwargs)
        return role, ip, zone_label

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(roles)) as ex:
        futures = {ex.submit(_create_one, role): role for role in roles}
        for fut in concurrent.futures.as_completed(futures):
            role, ip, zone = fut.result()
            if not ip:
                raise SystemExit(
                    f"Failed to obtain external IP for role '{role}' "
                    f"(instance '{name_prefix}-{role}', zone {zone}). "
                    "Check your GCP network / external IP configuration."
                )
            hosts[role] = ip
            if _is_receiver(role):
                receiver_zones[role] = zone

    return hosts, receiver_zones, sender_zone


def _get_internal_ip(
    instance_name: str,
    project_id: str = deploy_vm.PROJECT_ID,
    zone: str = deploy_vm.ZONE,
) -> str:
    try:
        from google.cloud import compute_v1  # type: ignore
        from google.oauth2 import service_account  # type: ignore
    except ImportError as e:
        sys.exit(
            "Missing required packages. Install with:\n"
            f"  pip install -r {_REQUIREMENTS}\n"
            f"Error: {e}"
        )

    credentials = service_account.Credentials.from_service_account_file(deploy_vm._key_file_path())
    instances_client = compute_v1.InstancesClient(credentials=credentials)
    instance_obj = instances_client.get(project=project_id, zone=zone, instance=instance_name)

    for nic in getattr(instance_obj, "network_interfaces", []) or []:
        ip = getattr(nic, "network_i_p", "") or ""
        if ip:
            return ip
    return ""


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Create GCP VMs for IONIA bench: "
            f"{NUM_RECEIVERS} receivers (replicas) + (N-3) senders (clients)."
        )
    )
    ap.add_argument(
        "machine_count",
        type=int,
        help=f"Total VMs to create (minimum {NUM_RECEIVERS + 1}).",
    )
    ap.add_argument(
        "--name-prefix",
        type=str,
        default="ionia",
        help="Instance name prefix, e.g. PREFIX-receiver1, PREFIX-sender1.",
    )
    ap.add_argument(
        "--hosts-file",
        type=str,
        default="cluster_hosts.json",
        help="Write role->external IP JSON next to this script (default: cluster_hosts.json).",
    )
    ap.add_argument(
        "--spread-replica-zones",
        action="store_true",
        help=(
            "Place receivers in EXTRA_REPLICA_ZONES and senders in EXTRA_CLIENT_ZONE "
            "(wide-area RTT: 1-RTT DYNA vs 2-RTT VR). Default: all VMs in deploy_vm.ZONE."
        ),
    )
    args = ap.parse_args()

    try:
        roles = _roles(args.machine_count)
    except ValueError as e:
        raise SystemExit(str(e)) from e

    if args.spread_replica_zones:
        if len(EXTRA_REPLICA_ZONES) < NUM_RECEIVERS:
            raise SystemExit(
                f"--spread-replica-zones requires at least {NUM_RECEIVERS} entries in "
                f"EXTRA_REPLICA_ZONES (got {len(EXTRA_REPLICA_ZONES)})."
            )
        if not EXTRA_CLIENT_ZONE.strip():
            raise SystemExit(
                "--spread-replica-zones requires EXTRA_CLIENT_ZONE to be set in create_cluster.py."
            )

    hosts, receiver_zones, sender_zone = create_cluster(
        args.machine_count,
        name_prefix=args.name_prefix,
        spread_replica_zones=args.spread_replica_zones,
    )
    sender_zone_label = _zone_label(sender_zone)

    receiver_roles = [r for r in roles if _is_receiver(r)]
    sender_roles = [r for r in roles if not _is_receiver(r)]

    receiver_internal: Dict[str, str] = {}
    for role in receiver_roles:
        inst = f"{args.name_prefix}-{role}"
        zone = receiver_zones.get(role, deploy_vm.ZONE)
        receiver_internal[role] = _get_internal_ip(inst, zone=zone)

    print("\nCreated IONIA bench cluster:")
    if args.spread_replica_zones:
        print(f"  Replicas: EXTRA_REPLICA_ZONES; senders: {sender_zone_label}")
    else:
        print(f"  All VMs: deploy_vm.ZONE ({deploy_vm.ZONE})")
    print("  Role        Zone                           Instance name                  External IP")
    print("  ----        ----                           -------------                  -----------")
    for role in roles:
        inst = f"{args.name_prefix}-{role}"
        if _is_receiver(role):
            zone = receiver_zones.get(role, deploy_vm.ZONE)
        else:
            zone = sender_zone_label
        print(f"  {role:<11} {zone:<30} {inst:<30} {hosts[role]}")

    print("\nReceiver internal IPs (for replica-to-replica config):")
    for role in receiver_roles:
        internal = receiver_internal.get(role) or "<not found>"
        zone = receiver_zones.get(role, deploy_vm.ZONE)
        print(f"  {role} ({zone}): {internal}")

    print("\nPython dict:")
    print("HOSTS = {")
    for role in roles:
        print(f'    "{role}": "{hosts[role]}",')
    print("}")

    print("\nReceivers (external):")
    print("RECEIVERS = [")
    for role in receiver_roles:
        print(f'    "{hosts[role]}",')
    print("]")

    print("\nSenders (external):")
    print("SENDERS = [")
    for role in sender_roles:
        print(f'    "{hosts[role]}",')
    print("]")

    hosts_path = args.hosts_file
    if not os.path.isabs(hosts_path):
        hosts_path = os.path.join(_SCRIPT_DIR, hosts_path)
    record = {
        "hosts": hosts,
        "receiver_internal": receiver_internal,
        "receiver_zones": receiver_zones,
        "spread_replica_zones": args.spread_replica_zones,
        "machine_count": args.machine_count,
        "name_prefix": args.name_prefix,
    }
    if args.spread_replica_zones:
        record["extra_client_zone"] = sender_zone_label
    record["sender_zone"] = sender_zone_label

    with open(hosts_path, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, sort_keys=True)
    print(f"\nWrote {hosts_path}")

    hosts_base = os.path.basename(hosts_path)
    print("\nNext steps (pick one protocol setup per experiment run):")
    print(
        f"  SSH_USER=<user> python3 {os.path.join(_SCRIPT_DIR, 'setup_paxos_vr_cluster.py')} "
        f"--hosts-file {hosts_base}"
    )
    print(
        f"  SSH_USER=<user> python3 {os.path.join(_SCRIPT_DIR, 'setup_dyna_cluster.py')} "
        f"--hosts-file {hosts_base}"
    )


if __name__ == "__main__":
    main()

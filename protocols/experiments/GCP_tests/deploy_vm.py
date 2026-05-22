#!/usr/bin/env python3
"""
Barebones script to create a single minimal GCE VM using the Compute Engine API.

Requires: pip install google-cloud-compute google-auth

Secrets (only): from scripts/secrets/gcp_secrets.py we use SERVICE_ACCOUNT_KEY_FILE
(and optionally SERVICE_ACCOUNT_EMAIL). All other settings are constants below.

SSH: After the VM is created, use:
  gcloud compute ssh INSTANCE_NAME --zone=ZONE --project=PROJECT_ID
That uses your gcloud/IAM identity. The VM does not need metadata ssh-keys for that.
To use a specific SSH key on the VM, add metadata["ssh-keys"] (see GCP docs).
"""

import json
import os
import sys
import time

# -----------------------------------------------------------------------------
# Constants (non-secret configuration)
# -----------------------------------------------------------------------------
PROJECT_ID = "corfu-488815"
ZONE = "us-east4-c"
MACHINE_TYPE = "n2-standard-2"  # 2 GB RAM; needed for clone + deps + build on each VM
IMAGE_PROJECT = "ubuntu-os-cloud"
IMAGE_FAMILY = "ubuntu-2204-lts"  # Folly pin patched in remote/folly_build.sh (glibc 2.34 / SIGSTKSZ)
INSTANCE_NAME = "lazylog-vm-1"

EXTRA_REPLICA_ZONES = ['europe-north1','asia-northeast1','southamerica-east1']

# -----------------------------------------------------------------------------
# Load secrets (only key file path and service account email)
# -----------------------------------------------------------------------------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_GCP_SECRETS_PATH = os.path.join(_SCRIPT_DIR, "secrets", "gcp_secrets.py")

def _load_gcp_secrets():
    if not os.path.isfile(_GCP_SECRETS_PATH):
        sys.exit(
            "Missing scripts/secrets/gcp_secrets.py. Create it with at least:\n"
            "  SERVICE_ACCOUNT_KEY_FILE = \"secrets/service_acc_key.json\""
        )
    import importlib.util
    spec = importlib.util.spec_from_file_location("gcp_secrets", _GCP_SECRETS_PATH)
    gcp_secrets = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gcp_secrets)
    return gcp_secrets

gcp_secrets = _load_gcp_secrets()


def _key_file_path():
    path = getattr(gcp_secrets, "SERVICE_ACCOUNT_KEY_FILE", None)
    path = path.strip()
    if not os.path.isabs(path):
        path = os.path.join(_SCRIPT_DIR, path)

    with open(path) as f:
        raw = f.read().strip()

    if not raw:
        sys.exit(
            f"Service account key file is empty: {path}\n"
            "Add your GCP service account JSON key (from IAM > Service accounts > Keys)."
        )
    try:
        json.loads(raw)
    except json.JSONDecodeError as e:
        sys.exit(
            f"Service account key file is not valid JSON: {path}\n"
            f"Error: {e}"
        )
    return path


def create_vm(
    project_id: str = PROJECT_ID,
    zone: str = ZONE,
    instance_name: str = INSTANCE_NAME,
    machine_type: str = MACHINE_TYPE,
    image_project: str = IMAGE_PROJECT,
    image_family: str = IMAGE_FAMILY,
) -> str:
    """Create one GCE instance via the Compute Engine API and wait until it is running.

    Returns the external IP address of the instance (empty string if none)."""
    try:
        from google.oauth2 import service_account  # type: ignore
        from google.cloud import compute_v1  # type: ignore
    except ImportError as e:
        sys.exit(
            "Missing required packages. Install with:\n"
            "  pip install -r requirements-deploy.txt\n"
            "  (from protocols/experiments/GCP_tests)\n"
            f"Error: {e}"
        )

    key_path = _key_file_path()
    credentials = service_account.Credentials.from_service_account_file(key_path)

    # Full URLs required by the API
    machine_type_url = f"zones/{zone}/machineTypes/{machine_type}"
    source_image = f"projects/{image_project}/global/images/{image_family}"

    # Boot disk (minimal like example: boot, autoDelete, sourceImage only; size = API default)
    disk = compute_v1.AttachedDisk()
    disk.auto_delete = True
    disk.boot = True
    disk.initialize_params = compute_v1.AttachedDiskInitializeParams(
        source_image=source_image,
    )

    # Network: default network + external IP (so you can SSH from the internet)
    network_interface = compute_v1.NetworkInterface()
    network_interface.network = "global/networks/default"
    network_interface.access_configs = [
        compute_v1.AccessConfig(
            name="External NAT",
            type_="ONE_TO_ONE_NAT",
        )
    ]

    instance = compute_v1.Instance()
    instance.name = instance_name
    instance.machine_type = machine_type_url
    instance.disks = [disk]
    instance.network_interfaces = [network_interface]

    instances_client = compute_v1.InstancesClient(credentials=credentials)
    request = compute_v1.InsertInstanceRequest(
        project=project_id,
        zone=zone,
        instance_resource=instance,
    )

    print(f"Creating instance {instance_name} in {zone}...")
    operation = instances_client.insert(request=request)

    # Wait for the zone operation to complete
    zone_ops_client = compute_v1.ZoneOperationsClient(credentials=credentials)
    op_name = operation.name
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        op = zone_ops_client.get(
            project=project_id,
            zone=zone,
            operation=op_name,
        )
        if getattr(op.status, "name", op.status) == "DONE":
            if op.error:
                raise RuntimeError(f"Instance creation failed: {op.error}")
            print(f"Instance {instance_name} is running.")
            # Fetch instance details to get external IP
            instance_obj = instances_client.get(
                project=project_id,
                zone=zone,
                instance=instance_name,
            )
            external_ip = ""
            for nic in getattr(instance_obj, "network_interfaces", []) or []:
                for ac in getattr(nic, "access_configs", []) or []:
                    # In the Python client, this field is exposed as nat_i_p (see printed proto).
                    ip = getattr(ac, "nat_i_p", "") or ""
                    if ip:
                        external_ip = ip
                        break
                if external_ip:
                    break
            if external_ip:
                print(f"Instance {instance_name} external IP: {external_ip}")
            else:
                print(f"Instance {instance_name} has no external IP.")
            return external_ip
        time.sleep(2)

    raise TimeoutError("Instance creation did not complete within 120s.")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Create a single GCE VM (Compute Engine API).")
    p.add_argument("--name", default=INSTANCE_NAME, help="Instance name")
    p.add_argument("--zone", default=ZONE, help="GCP zone")
    p.add_argument("--project", default=PROJECT_ID, help="GCP project ID")
    p.add_argument("--machine-type", default=MACHINE_TYPE, dest="machine_type", help="Machine type")
    p.add_argument(
        "--role",
        choices=["dur", "shard0", "shard1", "shard2", "client"],
        help="Optional CORFU role for this VM; if set, record role->IP in a hosts file.",
    )
    p.add_argument(
        "--hosts-file",
        default="cluster_hosts.json",
        help="Relative or absolute path to JSON file to store role->host mapping (default: scripts/cluster_hosts.json).",
    )
    args = p.parse_args()

    ip = create_vm(
        project_id=args.project,
        zone=args.zone,
        instance_name=args.name,
        machine_type=args.machine_type,
    )

    if args.role and ip:
        # Resolve hosts file path (default: alongside this script)
        hosts_path = args.hosts_file
        if not os.path.isabs(hosts_path):
            hosts_path = os.path.join(_SCRIPT_DIR, hosts_path)

        try:
            if os.path.isfile(hosts_path):
                with open(hosts_path) as f:
                    hosts = json.load(f)
            else:
                hosts = {}
        except Exception:
            hosts = {}

        hosts[args.role] = ip
        with open(hosts_path, "w") as f:
            json.dump(hosts, f, indent=2, sort_keys=True)
        print(f"Recorded role '{args.role}' with host '{ip}' in {hosts_path}")

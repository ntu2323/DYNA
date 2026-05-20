#!/usr/bin/env bash
# Full NOPaxos VR (paxos/vr) setup on one VM: deps, git clone, build, config, helpers.
# Launched via nohup from setup_paxos_vr_cluster.py so SSH drops do not stop the build.
#
# Bundle (~/bench/setup_bundle): role.txt, replica_index.txt, vrconfig, trace.micro,
#   id_ed25519, ssh_public_line, ionia_repo.sh
#
# Default: git clone git@github.com:ntu2323/IONIA.git using keys from gcp_secrets.py.
# Set USE_GIT=0 only when source was rsync'd from laptop (--rsync-from).

if [ -z "${BASH_VERSION:-}" ]; then
  exec /usr/bin/env bash "$0" "$@"
fi

set -euo pipefail

SETUP_BUNDLE="${SETUP_BUNDLE:-${HOME}/bench/setup_bundle}"
LOGFILE="${HOME}/paxos_vr_setup.log"
DONE_MARKER="${HOME}/bench/.setup_paxos_done"
FAIL_MARKER="${HOME}/bench/.setup_paxos_failed"
PAXOS_DIR="${HOME}/Ionia/protocols/src/paxos"
VR_PORT="${VR_PORT:-31880}"

rm -f "${DONE_MARKER}" "${FAIL_MARKER}"
trap 'echo "=== setup failed at $(date -Iseconds) ===" >> "${LOGFILE}"; touch "${FAIL_MARKER}"' ERR

exec >>"${LOGFILE}" 2>&1
echo "=== NOPaxos VR remote setup started at $(date -Iseconds) pid=$$ ==="
echo "=== bundle: ${SETUP_BUNDLE} log: ${LOGFILE} ==="

if [[ -f "${SETUP_BUNDLE}/ionia_repo.sh" ]]; then
  # shellcheck source=/dev/null
  source "${SETUP_BUNDLE}/ionia_repo.sh"
elif ! declare -f install_ssh_from_bundle >/dev/null 2>&1; then
  echo "Missing git/SSH helpers (ionia_repo.sh or inlined run.sh). Re-run setup_paxos_vr_cluster.py" >&2
  exit 1
fi

if [[ -f "${SETUP_BUNDLE}/folly_build.sh" ]]; then
  # shellcheck source=/dev/null
  source "${SETUP_BUNDLE}/folly_build.sh"
elif ! declare -f install_folly_if_needed >/dev/null 2>&1; then
  echo "Missing folly_build.sh. Re-run setup_paxos_vr_cluster.py" >&2
  exit 1
fi

ROLE="$(cat "${SETUP_BUNDLE}/role.txt")"
REPLICA_INDEX="$(cat "${SETUP_BUNDLE}/replica_index.txt" 2>/dev/null || true)"
SKIP_BUILD="${SKIP_BUILD:-0}"

install_config() {
  cp "${SETUP_BUNDLE}/vrconfig" ~/bench/vrconfig
  cp "${SETUP_BUNDLE}/trace.micro" ~/bench/trace.micro
}

install_helpers() {
  cat > ~/bench/start_paxos_vr_replica.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail
IDX="${1:?replica index 0|1|2}"
BATCH="${2:-1}"
cd "${HOME}/Ionia/protocols/src/paxos"
mkdir -p ~/bench
pkill -f '[b]ench/replica' 2>/dev/null || true
sleep 1
nohup ./bench/replica -i "${IDX}" -b "${BATCH}" -c ~/bench/vrconfig -m vr \
  > ~/bench/replica-"${IDX}".log 2>&1 &
echo "Started NOPaxos VR replica index=${IDX} pid=$!"
EOF
  chmod +x ~/bench/start_paxos_vr_replica.sh

  cat > ~/bench/run_paxos_vr_client.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail
# Usage: ./run_paxos_vr_client.sh [threads] [num_ops] [experiment_secs] [warmup_secs]
THREADS="${1:-1}"
NUM_OPS="${2:-100000}"
EXPERIMENT_SECS="${3:-60}"
WARMUP_SECS="${4:-5}"
cd "${HOME}/Ionia/protocols/src/paxos"
mkdir -p ~/bench
pkill -f '[b]ench/client' 2>/dev/null || true
sleep 1
if [[ ! -f ~/bench/trace.micro ]]; then
  echo "Missing ~/bench/trace.micro" >&2
  exit 1
fi
nohup ./bench/client \
  -c ~/bench/vrconfig \
  -m vr \
  -k ~/bench/trace.micro \
  -n "${NUM_OPS}" \
  -e "${EXPERIMENT_SECS}" \
  -w "${WARMUP_SECS}" \
  -t "${THREADS}" \
  -l ~/bench/latencies \
  > ~/bench/client.log 2>&1 &
echo "Started NOPaxos VR client threads=${THREADS} pid=$!"
EOF
  chmod +x ~/bench/run_paxos_vr_client.sh
}

install_apt_deps() {
  export DEBIAN_FRONTEND=noninteractive
  sudo cloud-init status --wait || true
  sudo apt-get update -qq
  sudo apt-get install -y -qq \
    git python3 openssh-client build-essential cmake ninja-build pkg-config \
    protobuf-compiler libprotobuf-dev libevent-dev libssl-dev \
    libunwind-dev libboost-all-dev libdouble-conversion-dev \
    libgoogle-glog-dev libgflags-dev libfmt-dev liblz4-dev liblzma-dev \
    libsnappy-dev zlib1g-dev binutils-dev libiberty-dev libelf-dev \
    libjemalloc-dev libaio-dev \
    libxxhash-dev libtbb-dev libnuma-dev numactl g++ gcc make
}

build_paxos() {
  test -d "${PAXOS_DIR}" || { echo "Missing ${PAXOS_DIR} after git clone" >&2; exit 1; }
  cd "${HOME}/Ionia"
  echo "Building RocksDB (DISABLE_WARNING_AS_ERROR=1 for Ubuntu 22.04 / GCC 11)..."
  make -C protocols/src/deps/rocksdb-6.12.7 -j"$(nproc)" static_lib DISABLE_WARNING_AS_ERROR=1
  echo "Building paxos bench/replica bench/client..."
  make -C protocols/src/paxos bench/replica bench/client -j"$(nproc)"
  test -x "${PAXOS_DIR}/bench/replica"
  test -x "${PAXOS_DIR}/bench/client"
}

# SSH keys (for GitHub + inter-VM ssh) and bench config first.
install_ssh_from_bundle
install_config
install_helpers
echo "SSH keys and config installed for role=${ROLE} (USE_GIT=${USE_GIT})"

if [[ "${SKIP_BUILD}" == "1" ]]; then
  echo "SKIP_BUILD=1 — git update + incremental compile (image deps; no apt/folly)"
  if [[ "${USE_GIT}" == "1" ]]; then
    ensure_ionia_repo
  elif [[ ! -d "${IONIA_DIR}" ]]; then
    echo "Missing ${IONIA_DIR} (USE_GIT=0). Run full setup, --rsync-from, or set USE_GIT=1." >&2
    exit 1
  fi
  build_paxos
else
  install_apt_deps
  ensure_ionia_repo
  install_folly_if_needed
  build_paxos
fi

if [[ "${ROLE}" == receiver* ]] && [[ -n "${REPLICA_INDEX}" ]]; then
  echo "Receiver ${ROLE} default replica index: ${REPLICA_INDEX}"
  echo "Start with: ~/bench/start_paxos_vr_replica.sh ${REPLICA_INDEX}"
elif [[ "${ROLE}" == sender* ]]; then
  echo "Sender ${ROLE}: ~/bench/run_paxos_vr_client.sh [threads] ..."
fi

touch "${DONE_MARKER}"
echo "=== NOPaxos VR remote setup finished at $(date -Iseconds) ==="
echo "Repo: ${IONIA_DIR} (${IONIA_GIT_URL})"
echo "Done marker: ${DONE_MARKER}"
echo "UDP port ${VR_PORT} — open firewall between cluster VMs."

#!/usr/bin/env bash
# Full DYNA (ionia -m dyna) setup on one VM: deps, git clone, build, config, helpers.
# Default: git clone git@github.com:ntu2323/IONIA.git (keys from gcp_secrets.py bundle).

if [ -z "${BASH_VERSION:-}" ]; then
  exec /usr/bin/env bash "$0" "$@"
fi

set -euo pipefail

SETUP_BUNDLE="${SETUP_BUNDLE:-${HOME}/bench/setup_bundle}"
LOGFILE="${HOME}/dyna_setup.log"
DONE_MARKER="${HOME}/bench/.setup_dyna_done"
FAIL_MARKER="${HOME}/bench/.setup_dyna_failed"
IONIA_SRC="${HOME}/Ionia/protocols/src/ionia"
REPLICA_PORT="${REPLICA_PORT:-31880}"

rm -f "${DONE_MARKER}" "${FAIL_MARKER}"
trap 'echo "=== setup failed at $(date -Iseconds) ===" >> "${LOGFILE}"; touch "${FAIL_MARKER}"' ERR

exec >>"${LOGFILE}" 2>&1
echo "=== DYNA remote setup started at $(date -Iseconds) pid=$$ ==="
echo "=== bundle: ${SETUP_BUNDLE} log: ${LOGFILE} ==="

if [[ -f "${SETUP_BUNDLE}/ionia_repo.sh" ]]; then
  # shellcheck source=/dev/null
  source "${SETUP_BUNDLE}/ionia_repo.sh"
elif ! declare -f install_ssh_from_bundle >/dev/null 2>&1; then
  echo "Missing git/SSH helpers (ionia_repo.sh or inlined run.sh). Re-run setup_dyna_cluster.py" >&2
  exit 1
fi

if [[ -f "${SETUP_BUNDLE}/folly_build.sh" ]]; then
  # shellcheck source=/dev/null
  source "${SETUP_BUNDLE}/folly_build.sh"
elif ! declare -f install_folly_if_needed >/dev/null 2>&1; then
  echo "Missing folly_build.sh. Re-run setup_dyna_cluster.py" >&2
  exit 1
fi

ROLE="$(cat "${SETUP_BUNDLE}/role.txt")"
REPLICA_INDEX="$(cat "${SETUP_BUNDLE}/replica_index.txt" 2>/dev/null || true)"
SKIP_BUILD="${SKIP_BUILD:-0}"

install_config() {
  cp "${SETUP_BUNDLE}/dynaconfig" ~/bench/dynaconfig
  cp "${SETUP_BUNDLE}/trace.micro" ~/bench/trace.micro
}

install_helpers() {
  cat > ~/bench/start_dyna_replica.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail
IDX="${1:?replica index 0|1|2}"
export IONIA_DB_MODE=memory
export IONIA_SPLINTER_CACHE_MB="${IONIA_SPLINTER_CACHE_MB:-256}"
export IONIA_SPLINTER_DISK_GB="${IONIA_SPLINTER_DISK_GB:-2}"
export DYNA_WAL_DIR="${DYNA_WAL_DIR:-$HOME/bench/wal}"
unset DYNA_FORCE_SLOWPATH
mkdir -p "$DYNA_WAL_DIR"
cd "${HOME}/Ionia/protocols/src/ionia"
pkill -f '[b]ench/replica' 2>/dev/null || true
sleep 1
nohup ./bench/replica -i "${IDX}" -j 1 -b 1 -c ~/bench/dynaconfig -m dyna \
  > ~/bench/replica-"${IDX}".log 2>&1 &
echo "Started DYNA replica index=${IDX} pid=$!"
EOF
  chmod +x ~/bench/start_dyna_replica.sh

  cat > ~/bench/run_dyna_client.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail
# Usage: ./run_dyna_client.sh <client_id> [threads] [num_ops] [experiment_secs] [warmup_secs]
CLIENT_ID="${1:-1}"
THREADS="${2:-1}"
NUM_OPS="${3:-100000}"
EXPERIMENT_SECS="${4:-60}"
WARMUP_SECS="${5:-5}"
cd "${HOME}/Ionia/protocols/src/ionia"
mkdir -p ~/bench
pkill -f '[b]ench/client' 2>/dev/null || true
sleep 1
if [[ ! -f ~/bench/trace.micro ]]; then
  echo "Missing ~/bench/trace.micro" >&2
  exit 1
fi
nohup ./bench/client \
  -s ~/bench/dynaconfig \
  -c ~/bench/dynaconfig \
  -m dyna \
  -k ~/bench/trace.micro \
  -n "${NUM_OPS}" \
  -e "${EXPERIMENT_SECS}" \
  -w "${WARMUP_SECS}" \
  -t "${THREADS}" \
  -j "${CLIENT_ID}" \
  -l ~/bench/latencies \
  > ~/bench/client-"${CLIENT_ID}".log 2>&1 &
echo "Started DYNA client client_id=${CLIENT_ID} threads=${THREADS} pid=$!"
EOF
  chmod +x ~/bench/run_dyna_client.sh
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

build_dyna() {
  test -d "${IONIA_SRC}" || { echo "Missing ${IONIA_SRC} after git clone" >&2; exit 1; }
  cd "${HOME}/Ionia"
  chmod +x docker/bootstrap_deps.sh
  ./docker/bootstrap_deps.sh
  make -C protocols/src/ionia bench/replica bench/client -j"$(nproc)"
  test -x "${IONIA_SRC}/bench/replica"
  test -x "${IONIA_SRC}/bench/client"
}

install_ssh_from_bundle
mkdir -p ~/bench
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
  build_dyna
else
  install_apt_deps
  ensure_ionia_repo
  install_folly_if_needed
  build_dyna
fi

if [[ "${ROLE}" == receiver* ]] && [[ -n "${REPLICA_INDEX}" ]]; then
  echo "Receiver ${ROLE} default replica index: ${REPLICA_INDEX}"
  echo "Start with: ~/bench/start_dyna_replica.sh ${REPLICA_INDEX}"
elif [[ "${ROLE}" == sender* ]]; then
  echo "Sender ${ROLE}: ~/bench/run_dyna_client.sh <client_id> ..."
fi

touch "${DONE_MARKER}"
echo "=== DYNA remote setup finished at $(date -Iseconds) ==="
echo "Repo: ${IONIA_DIR} (${IONIA_GIT_URL})"
echo "Done marker: ${DONE_MARKER}"
echo "UDP port ${REPLICA_PORT} — open firewall between cluster VMs."

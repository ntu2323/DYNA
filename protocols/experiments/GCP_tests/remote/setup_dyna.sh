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
  # Remove helpers from the retired dyna_new prototype (older setups installed them).
  rm -f ~/bench/start_dyna_new_replica.sh ~/bench/run_dyna_new_client.sh

  cat > ~/bench/start_dyna_replica.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail
# Usage: start_dyna_replica.sh <idx>
IDX="${1:?replica index 0|1|2}"
# DYNA executes committed writes into the harness store: SplinterDB by default (real
# disk; same cache/disk sizes as start_ionia_replica.sh). The DB file is splinterdb.db in
# the replica's working directory: /mydata/dyna when it exists (an ext4 disk; SplinterDB
# needs fallocate, which CloudLab's ext3 root lacks), else ~/Ionia/protocols/src/ionia.
# IONIA_DB_MODE=memory uses lib's in-memory store instead.
export IONIA_DB_MODE="${IONIA_DB_MODE:-splinter}"
export IONIA_SPLINTER_CACHE_MB="${IONIA_SPLINTER_CACHE_MB:-256}"
export IONIA_SPLINTER_DISK_GB="${IONIA_SPLINTER_DISK_GB:-2}"
# Deadline / OWD (dyna/deadline.h). Steady-state bound is OWD-driven; INITIAL_BOUND
# only matters for the first few ops; MAX_OWD caps OWD spikes.
export DYNA_INITIAL_BOUND_US="${DYNA_INITIAL_BOUND_US:-100}"
export DYNA_MAX_OWD_US="${DYNA_MAX_OWD_US:-1000}"
export DYNA_OWD_PERCENTILE="${DYNA_OWD_PERCENTILE:-95}"
export DYNA_OWD_WINDOW_LEN="${DYNA_OWD_WINDOW_LEN:-100}"
# Busy-poll transport + release loop (default on; 0 = epoll/cv park).
export DYNA_BUSY_POLL="${DYNA_BUSY_POLL:-1}"
# Background ts-agreement / replication / execution tick, and SPECULATIVE timeout.
export DYNA_TS_SCAN_MS="${DYNA_TS_SCAN_MS:-50}"
export DYNA_TS_TIMEOUT_MS="${DYNA_TS_TIMEOUT_MS:-100}"
# CPU pinning (dyna/cpu_pin.h): give each busy replica thread its own PHYSICAL core on
# the NIC's NUMA node (hyperthread siblings of spinning threads run at ~half speed).
# DYNA_PIN=0 disables; explicit DYNA_PIN_* values win.
if [[ "${DYNA_PIN:-1}" != "0" ]] && command -v lscpu >/dev/null; then
  IFACE=$(ip -4 -o addr show | awk '$4 ~ /^10[.]10[.]/ {print $2; exit}')
  NODE=$(cat "/sys/class/net/${IFACE}/device/numa_node" 2>/dev/null || echo 0)
  [[ "${NODE}" -lt 0 ]] && NODE=0
  mapfile -t CORES < <(lscpu -p=CPU,CORE,NODE | grep -v '^#' \
                         | awk -F, -v n="${NODE}" '$3==n && !seen[$2]++ {print $1}' | tail -n +2)
  RX="${DYNA_RX_THREADS:-1}"
  if (( ${#CORES[@]} >= RX + 3 )); then
    RXLIST=$(IFS=,; echo "${CORES[*]:0:RX}")
    export DYNA_PIN_RX="${DYNA_PIN_RX:-${RXLIST}}"
    export DYNA_PIN_ORDER="${DYNA_PIN_ORDER:-${CORES[RX]}}"
    export DYNA_PIN_SEND="${DYNA_PIN_SEND:-${CORES[RX+1]}}"
    export DYNA_PIN_EXEC="${DYNA_PIN_EXEC:-${CORES[RX+2]}}"
  fi
fi
RUN_DIR="${HOME}/Ionia/protocols/src/ionia"
if [[ -d /mydata/dyna ]]; then RUN_DIR=/mydata/dyna; fi
cd "${RUN_DIR}"
pkill -f '[b]ench/replica' 2>/dev/null || true
sleep 1
nohup "${HOME}/Ionia/protocols/src/ionia/bench/replica" -i "${IDX}" -j 1 -b 1 -c ~/bench/dynaconfig -m dyna \
  > ~/bench/replica-"${IDX}".log 2>&1 &
echo "Started DYNA replica index=${IDX} pid=$! pin rx=${DYNA_PIN_RX:-} order=${DYNA_PIN_ORDER:-} send=${DYNA_PIN_SEND:-} exec=${DYNA_PIN_EXEC:-}"
EOF
  chmod +x ~/bench/start_dyna_replica.sh

  cat > ~/bench/run_dyna_client.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail
# Usage: ./run_dyna_client.sh <client_id> [threads] [poisson_rate] [experiment_secs] [warmup_secs]
# Open-loop Poisson (~poisson_rate*100 offered ops/s per sender) over the shared
# 1000-key trace.micro (bench picks a uniformly random key per request).
CLIENT_ID="${1:-1}"
THREADS="${2:-1}"
POISSON_RATE="${3:-30}"
EXPERIMENT_SECS="${4:-60}"
WARMUP_SECS="${5:-5}"
export DYNA_INITIAL_BOUND_US="${DYNA_INITIAL_BOUND_US:-100}"
export DYNA_MAX_OWD_US="${DYNA_MAX_OWD_US:-1000}"
export DYNA_BUSY_POLL="${DYNA_BUSY_POLL:-1}"
export DYNA_RESEND_TIMEOUT_MS="${DYNA_RESEND_TIMEOUT_MS:-30}"
export BENCH_INFLIGHT_LOG="${BENCH_INFLIGHT_LOG:-1}"
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
  -O -R "${POISSON_RATE}" \
  -n 0 \
  -e "${EXPERIMENT_SECS}" \
  -w "${WARMUP_SECS}" \
  -t "${CLIENT_ID}" \
  -j "${THREADS}" \
  -i 1000 \
  -l ~/bench/latencies \
  > ~/bench/client-"${CLIENT_ID}".log 2>&1 &
echo "Started DYNA client client_id=${CLIENT_ID} threads=${THREADS} rate=${POISSON_RATE} pid=$!"
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

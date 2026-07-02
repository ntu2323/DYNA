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
# Usage: start_dyna_replica.sh <idx> [--wal-tmpfs]
#   --wal-tmpfs  WAL on /dev/shm/dyna-wal (low-jitter fsync triage; not durable across reboot)
IDX="${1:?replica index 0|1|2}"
shift || true
while [[ $# -gt 0 ]]; do
  case "$1" in
    --wal-tmpfs)
      export DYNA_WAL_DIR=/dev/shm/dyna-wal
      shift
      ;;
    *)
      echo "Unknown option: $1 (usage: $0 <idx> [--wal-tmpfs])" >&2
      exit 1
      ;;
  esac
done
export IONIA_DB_MODE=memory
export IONIA_SPLINTER_CACHE_MB="${IONIA_SPLINTER_CACHE_MB:-256}"
export IONIA_SPLINTER_DISK_GB="${IONIA_SPLINTER_DISK_GB:-2}"
# WAL on local persistent disk; for low-jitter throughput sweeps set
# DYNA_WAL_DIR=/dev/shm/dyna-wal to take real fsync out of the loop.
export DYNA_WAL_DIR="${DYNA_WAL_DIR:-$HOME/bench/wal}"
# Level-1 tuning: tighter deadline bound to shrink earlyBuffer hold under load.
# Steady-state bound is driven by the OWD estimator (~20-50us in same-region runs),
# so INITIAL_BOUND mainly affects the first few ops; MAX_OWD caps OWD spikes that
# would otherwise lengthen the per-op hold.
export DYNA_INITIAL_BOUND_US="${DYNA_INITIAL_BOUND_US:-50}"
export DYNA_MAX_OWD_US="${DYNA_MAX_OWD_US:-2000}"
export DYNA_BOUND_RECOMPUTE_K="${DYNA_BOUND_RECOMPUTE_K:-1}"
# Default fast-reply shard count is sized for the current 8 vCPU replica VMs:
# 4 shards leaves headroom on 3 partitions × 3 worker threads + transport without
# CPU oversubscription. Lower to 2 on 2 vCPU boxes; raise toward 8 only if
# fastreply_full counters start showing up in DYNA stats.
export DYNA_FAST_REPLY_SHARDS="${DYNA_FAST_REPLY_SHARDS:-4}"
# Eager fast-reply bypasses the earlyBuffer deadline hold (lower mean/p99 under load).
# Set DYNA_EAGER_FAST_REPLY=0 for Nezha-style deadline-ordered release.
export DYNA_EAGER_FAST_REPLY="${DYNA_EAGER_FAST_REPLY:-1}"
export DYNA_SLOWPATH_DEBUG="${DYNA_SLOWPATH_DEBUG:-1}"
export DYNA_STATS="${DYNA_STATS:-1}"
export DYNA_STATS_INTERVAL_SEC="${DYNA_STATS_INTERVAL_SEC:-1}"
export DYNA_STATS_SAMPLE="${DYNA_STATS_SAMPLE:-1024}"
export DYNA_REPLICATION_BUDGET="${DYNA_REPLICATION_BUDGET:-256}"
export DYNA_REPLICATION_BURST_BUDGET="${DYNA_REPLICATION_BURST_BUDGET:-8192}"
export DYNA_REPLICATION_BACKLOG_THRESHOLD="${DYNA_REPLICATION_BACKLOG_THRESHOLD:-1000}"
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
export DYNA_INITIAL_BOUND_US="${DYNA_INITIAL_BOUND_US:-50}"
export DYNA_MAX_OWD_US="${DYNA_MAX_OWD_US:-2000}"
export DYNA_BOUND_RECOMPUTE_K="${DYNA_BOUND_RECOMPUTE_K:-1}"
export DYNA_CLIENT_STATS="${DYNA_CLIENT_STATS:-1}"
export DYNA_CLIENT_STATS_LOG_EVERY="${DYNA_CLIENT_STATS_LOG_EVERY:-10000}"
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

  # --- dyna_new (rewritten DYNA, bench/replica -m dyna_new) --------------------
  # Reuses the same binary, ~/bench/dynaconfig and ~/bench/trace.micro as dyna.
  # No WAL / SplinterDB (InMemoryBackend); only deadline + resend env apply.
  cat > ~/bench/start_dyna_new_replica.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail
# Usage: start_dyna_new_replica.sh <idx>
IDX="${1:?replica index 0|1|2}"
# dyna_new uses an in-memory backend; IONIA_DB_MODE is set for safety only.
export IONIA_DB_MODE="${IONIA_DB_MODE:-memory}"
# Deadline / OWD (deadline.h). Steady-state bound is OWD-driven; INITIAL_BOUND
# only matters for the first few ops; MAX_OWD caps OWD spikes.
export DYNA_INITIAL_BOUND_US="${DYNA_INITIAL_BOUND_US:-50}"
export DYNA_MAX_OWD_US="${DYNA_MAX_OWD_US:-2000}"
export DYNA_OWD_PERCENTILE="${DYNA_OWD_PERCENTILE:-50}"
export DYNA_OWD_WINDOW_LEN="${DYNA_OWD_WINDOW_LEN:-100}"
export DYNA_NEW_RECON_INTERVAL_MS="${DYNA_NEW_RECON_INTERVAL_MS:-5}"
# Diagnostics: per-second "DYNA_NEW stats" line (grep the replica log). On by
# default during perf tuning; set DYNA_NEW_STATS=0 to silence.
export DYNA_NEW_STATS="${DYNA_NEW_STATS:-1}"
export DYNA_NEW_STATS_INTERVAL_MS="${DYNA_NEW_STATS_INTERVAL_MS:-1000}"
cd "${HOME}/Ionia/protocols/src/ionia"
pkill -f '[b]ench/replica' 2>/dev/null || true
sleep 1
nohup ./bench/replica -i "${IDX}" -j 1 -b 1 -c ~/bench/dynaconfig -m dyna_new \
  > ~/bench/replica-"${IDX}".log 2>&1 &
echo "Started dyna_new replica index=${IDX} pid=$!"
EOF
  chmod +x ~/bench/start_dyna_new_replica.sh

  cat > ~/bench/run_dyna_new_client.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail
# Usage: ./run_dyna_new_client.sh <client_id> [threads] [poisson_rate] [experiment_secs] [warmup_secs]
# Open-loop Poisson (~poisson_rate*100 offered ops/s per sender) over the shared
# 1000-key trace.micro (bench picks a uniformly random key per request).
CLIENT_ID="${1:-1}"
THREADS="${2:-1}"
POISSON_RATE="${3:-30}"
EXPERIMENT_SECS="${4:-60}"
WARMUP_SECS="${5:-5}"
export DYNA_INITIAL_BOUND_US="${DYNA_INITIAL_BOUND_US:-50}"
export DYNA_MAX_OWD_US="${DYNA_MAX_OWD_US:-2000}"
export DYNA_NEW_RESEND_TIMEOUT_MS="${DYNA_NEW_RESEND_TIMEOUT_MS:-30}"
export DYNA_NEW_CLIENT_STATS="${DYNA_NEW_CLIENT_STATS:-1}"
export DYNA_NEW_CLIENT_STATS_LOG_EVERY="${DYNA_NEW_CLIENT_STATS_LOG_EVERY:-2000}"
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
  -m dyna_new \
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
echo "Started dyna_new client client_id=${CLIENT_ID} threads=${THREADS} rate=${POISSON_RATE} pid=$!"
EOF
  chmod +x ~/bench/run_dyna_new_client.sh
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

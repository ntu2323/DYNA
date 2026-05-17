#!/usr/bin/env bash
# Full NOPaxos VR (paxos/vr) setup on one VM: deps, build, SSH keys, config, helpers.
# Launched via nohup from setup_paxos_vr_cluster.py so SSH drops do not stop the build.
#
# Expects a bundle under SETUP_BUNDLE (default ~/bench/setup_bundle):
#   role.txt, replica_index.txt (empty for senders), vrconfig, trace.micro,
#   id_ed25519, ssh_public_line
#
# Env:
#   SETUP_BUNDLE   path to bundle directory
#   SKIP_BUILD=1   skip apt/folly/rocksdb/make (config + helpers only)
#   USE_GIT=1      clone ~/Ionia from GitHub if missing (needs id_ed25519 in bundle)

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

ROLE="$(cat "${SETUP_BUNDLE}/role.txt")"
REPLICA_INDEX="$(cat "${SETUP_BUNDLE}/replica_index.txt" 2>/dev/null || true)"
SKIP_BUILD="${SKIP_BUILD:-0}"
USE_GIT="${USE_GIT:-0}"

install_ssh() {
  mkdir -p ~/.ssh ~/bench
  chmod 700 ~/.ssh
  cp "${SETUP_BUNDLE}/id_ed25519" ~/.ssh/id_ed25519
  chmod 600 ~/.ssh/id_ed25519
  touch ~/.ssh/authorized_keys
  chmod 600 ~/.ssh/authorized_keys
  pub="$(tr -d '\r\n' < "${SETUP_BUNDLE}/ssh_public_line")"
  grep -qxF "${pub}" ~/.ssh/authorized_keys || echo "${pub}" >> ~/.ssh/authorized_keys
}

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
pkill -f 'bench/replica' 2>/dev/null || true
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
pkill -f 'bench/client' 2>/dev/null || true
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

install_folly_if_needed() {
  if pkg-config --exists folly 2>/dev/null; then
    return 0
  fi
  echo "Building Folly..."
  rm -rf /tmp/folly
  git clone --depth 1 https://github.com/facebook/folly.git /tmp/folly
  cd /tmp/folly
  git checkout 49926b98f5afb5667d0c06807da79d606a6d43c3
  cmake -S . -B _build -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=OFF -DFOLLY_USE_SYMBOLIZER=OFF
  cmake --build _build -j"$(nproc)"
  sudo cmake --install _build
  cd "${HOME}"
}

install_apt_deps() {
  export DEBIAN_FRONTEND=noninteractive
  sudo cloud-init status --wait || true
  sudo apt-get update -qq
  sudo apt-get install -y -qq \
    git build-essential cmake ninja-build pkg-config \
    protobuf-compiler libprotobuf-dev libevent-dev libssl-dev \
    libunwind-dev libboost-all-dev libdouble-conversion-dev \
    libgoogle-glog-dev libgflags-dev libfmt-dev liblz4-dev liblzma-dev \
    libsnappy-dev zlib1g-dev binutils-dev libjemalloc-dev libaio-dev \
    libxxhash-dev libtbb-dev libnuma-dev numactl g++ gcc make
}

ensure_ionia_repo() {
  if [[ -d "${HOME}/Ionia/protocols/src/paxos" ]]; then
    return 0
  fi
  if [[ "${USE_GIT}" != "1" ]]; then
    echo "Missing ${HOME}/Ionia — rsync source first or set USE_GIT=1" >&2
    exit 1
  fi
  ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null || true
  git clone git@github.com:ntu2323/IONIA.git "${HOME}/Ionia"
  cd "${HOME}/Ionia"
  git submodule update --init --recursive || true
  cd "${HOME}"
}

build_paxos() {
  cd "${HOME}/Ionia"
  echo "Building RocksDB..."
  make -C protocols/src/deps/rocksdb-6.12.7 -j"$(nproc)" static_lib
  echo "Building paxos bench/replica bench/client..."
  make -C protocols/src/paxos bench/replica bench/client -j"$(nproc)"
  test -x "${PAXOS_DIR}/bench/replica"
  test -x "${PAXOS_DIR}/bench/client"
}

install_ssh
install_config
install_helpers
echo "Config and helpers installed for role=${ROLE}"

if [[ "${SKIP_BUILD}" == "1" ]]; then
  echo "SKIP_BUILD=1 — skipping compile"
else
  install_apt_deps
  install_folly_if_needed
  ensure_ionia_repo
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
echo "Done marker: ${DONE_MARKER}"
echo "UDP port ${VR_PORT} — open firewall between cluster VMs."

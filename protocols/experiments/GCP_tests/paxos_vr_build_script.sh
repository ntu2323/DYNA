#!/usr/bin/env bash
# Remote NOPaxos VR (protocols/src/paxos) build.
# Deploy key placeholder is replaced by setup_paxos_vr_cluster.py before upload.

if [ -z "${BASH_VERSION:-}" ]; then
  exec /usr/bin/env bash "$0" "$@"
fi

set -euo pipefail

LOGFILE="${HOME}/paxos_vr_build.log"
exec > >(tee -a "$LOGFILE") 2>&1

echo "=== NOPaxos VR (paxos/) remote build started at $(date -Iseconds) ==="

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

if ! pkg-config --exists folly 2>/dev/null; then
  echo "Building Folly..."
  rm -rf /tmp/folly
  git clone https://github.com/facebook/folly.git /tmp/folly
  cd /tmp/folly
  git checkout 49926b98f5afb5667d0c06807da79d606a6d43c3
  cmake -S . -B _build -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=OFF -DFOLLY_USE_SYMBOLIZER=OFF
  cmake --build _build -j"$(nproc)"
  sudo cmake --install _build
  cd "${HOME}"
fi

mkdir -p ~/.ssh
chmod 700 ~/.ssh
cat > ~/.ssh/id_ed25519 << 'EOF_SSH_KEY'
___IONIA_SSH_PRIVATE_KEY___
EOF_SSH_KEY
chmod 600 ~/.ssh/id_ed25519
ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null || true

IONIA_DIR="${HOME}/Ionia"
if [ ! -d "${IONIA_DIR}" ]; then
  git clone git@github.com:ntu2323/IONIA.git "${IONIA_DIR}"
fi

cd "${IONIA_DIR}"
git submodule update --init --recursive || true

echo "Building RocksDB (static lib)..."
make -C protocols/src/deps/rocksdb-6.12.7 -j"$(nproc)" static_lib DISABLE_WARNING_AS_ERROR=1

echo "Building paxos bench/replica and bench/client..."
make -C protocols/src/paxos bench/replica bench/client -j"$(nproc)"

echo "=== NOPaxos VR remote build finished at $(date -Iseconds) ==="

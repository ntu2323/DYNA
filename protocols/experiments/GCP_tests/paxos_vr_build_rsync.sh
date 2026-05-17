#!/usr/bin/env bash
# Deprecated: use setup_paxos_vr_cluster.py (remote/setup_paxos_vr.sh under nohup).
# Build paxos/vr after rsync of ~/Ionia (no git clone). Logs: ~/paxos_vr_build.log

if [ -z "${BASH_VERSION:-}" ]; then
  exec /usr/bin/env bash "$0" "$@"
fi

set -euo pipefail

LOGFILE="${HOME}/paxos_vr_build.log"
exec > >(tee -a "$LOGFILE") 2>&1

echo "=== NOPaxos VR rsync build started at $(date -Iseconds) ==="
echo "=== log: $LOGFILE ==="

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

cd "${HOME}/Ionia"
echo "Building RocksDB..."
make -C protocols/src/deps/rocksdb-6.12.7 -j"$(nproc)" static_lib
echo "Building paxos bench/replica bench/client..."
make -C protocols/src/paxos bench/replica bench/client -j"$(nproc)"
echo "=== NOPaxos VR rsync build finished at $(date -Iseconds) ==="

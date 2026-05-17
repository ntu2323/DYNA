#!/usr/bin/env bash
# Remote DYNA (protocols/src/ionia) build.
# Deploy key placeholder is replaced by setup_dyna_cluster.py before upload.

if [ -z "${BASH_VERSION:-}" ]; then
  exec /usr/bin/env bash "$0" "$@"
fi

set -euo pipefail

LOGFILE="${HOME}/dyna_build.log"
exec > >(tee -a "$LOGFILE") 2>&1

echo "=== DYNA (ionia/) remote build started at $(date -Iseconds) ==="

export DEBIAN_FRONTEND=noninteractive
sudo cloud-init status --wait || true
sudo apt-get update -qq
sudo apt-get install -y -qq \
  git build-essential cmake ninja-build pkg-config \
  protobuf-compiler libprotobuf-dev libevent-dev libssl-dev \
  libunwind-dev libboost-all-dev libdouble-conversion-dev \
  libgoogle-glog-dev libgflags-dev libfmt-dev liblz4-dev liblzma-dev \
  libsnappy-dev zlib1g-dev binutils-dev libjemalloc-dev libaio-dev \
  libxxhash-dev libtbb-dev libnuma-dev numactl make

if ! pkg-config --exists folly 2>/dev/null; then
  echo "Building Folly..."
  rm -rf /tmp/folly
  git clone --depth 1 https://github.com/facebook/folly.git /tmp/folly
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
chmod +x docker/bootstrap_deps.sh
./docker/bootstrap_deps.sh
make -C protocols/src/ionia bench/replica bench/client -j"$(nproc)"

echo "=== DYNA remote build finished at $(date -Iseconds) ==="

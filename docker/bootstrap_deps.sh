#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
THIRD_PARTY_DIR="${ROOT_DIR}/protocols/src/ionia/third_party"
DEPS_DIR="${ROOT_DIR}/protocols/src/deps"

clone_if_missing() {
    local url="$1"
    local dir="$2"

    if [[ -d "${dir}" ]]; then
        return
    fi
    git clone --depth 1 "${url}" "${dir}"
}

mkdir -p "${THIRD_PARTY_DIR}" "${DEPS_DIR}"

clone_if_missing "https://github.com/greg7mdp/parallel-hashmap.git" "${THIRD_PARTY_DIR}/parallel-hashmap"
clone_if_missing "https://github.com/cameron314/concurrentqueue.git" "${THIRD_PARTY_DIR}/concurrentqueue"
clone_if_missing "https://github.com/cameron314/readerwriterqueue.git" "${THIRD_PARTY_DIR}/readerwriterqueue"
clone_if_missing "https://github.com/martinus/unordered_dense.git" "${THIRD_PARTY_DIR}/unordered_dense"
clone_if_missing "https://github.com/Naios/function2.git" "${THIRD_PARTY_DIR}/function2"

if [[ ! -e "${DEPS_DIR}/splinterdb" ]]; then
    ln -s ../ionia/third_party/splinterdb "${DEPS_DIR}/splinterdb"
fi

pushd "${THIRD_PARTY_DIR}/splinterdb" >/dev/null
if [[ -d build ]]; then
    make clean >/dev/null 2>&1 || true
    rm -rf build
fi
env CC=gcc CXX=g++ BUILD_DOCKER=1 make build/release/lib/libsplinterdb.a -j"$(nproc)"
popd >/dev/null

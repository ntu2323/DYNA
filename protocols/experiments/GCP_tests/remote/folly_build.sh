# Build pinned Folly (same commit as docker/Dockerfile.udp) on Ubuntu 22.04+.
# glibc 2.34 makes SIGSTKSZ non-constexpr; patch FiberManager.cpp before cmake.

patch_folly_for_glibc234() {
  local f="/tmp/folly/folly/fibers/FiberManager.cpp"
  [[ -f "$f" ]] || return 0
  if grep -q 'std::max(size_t(SIGSTKSZ)' "$f" 2>/dev/null; then
    echo "Folly FiberManager.cpp already patched for glibc 2.34+"
    return 0
  fi
  if ! grep -q 'constexpr_max(SIGSTKSZ' "$f" 2>/dev/null; then
    return 0
  fi
  echo "Patching Folly FiberManager.cpp for Ubuntu 22.04 (SIGSTKSZ / glibc 2.34+)..."
  python3 - "$f" << 'PY'
import re
import sys

path = sys.argv[1]
text = open(path, encoding="utf-8").read()

text = text.replace(
    "constexpr size_t kAltStackSize = folly::constexpr_max(SIGSTKSZ, 32 * 1024);",
    "size_t kAltStackSize = std::max(size_t(SIGSTKSZ), size_t(32 * 1024));",
)
text = re.sub(r"\s*using AltStackBuffer = std::array<char, kAltStackSize>;\n", "\n", text)
text = text.replace("std::unique_ptr<AltStackBuffer>", "std::unique_ptr<char[]>")
text = text.replace("std::make_unique<AltStackBuffer>()", "std::make_unique<char[]>(kAltStackSize)")
text = text.replace("stack_->data()", "stack_.get()")
text = text.replace("stack_->size()", "kAltStackSize")

if "std::max(size_t(SIGSTKSZ)" not in text:
    sys.exit("folly patch failed: expected substitutions missing")

open(path, "w", encoding="utf-8").write(text)
PY
}

install_folly_if_needed() {
  if pkg-config --exists folly 2>/dev/null; then
    echo "Using system Folly ($(pkg-config --modversion folly 2>/dev/null || echo installed))"
    return 0
  fi
  FOLLY_COMMIT="${FOLLY_COMMIT:-49926b98f5afb5667d0c06807da79d606a6d43c3}"
  echo "Building Folly at ${FOLLY_COMMIT}..."
  rm -rf /tmp/folly
  git clone https://github.com/facebook/folly.git /tmp/folly
  cd /tmp/folly
  git checkout "${FOLLY_COMMIT}"
  patch_folly_for_glibc234
  cmake -S . -B _build -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=OFF -DFOLLY_USE_SYMBOLIZER=OFF
  cmake --build _build -j"$(nproc)"
  sudo cmake --install _build
  cd "${HOME}"
}

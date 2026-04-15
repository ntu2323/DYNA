#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_HOST="${REPO_ROOT}/docker/smoke-output"
mkdir -p "${OUT_HOST}"

IMG="${IONIA_UDP_IMAGE:-ionia-udp}"

run_flags=(--rm --platform linux/amd64)
[[ -t 1 ]] && run_flags+=(-it)

exec docker run "${run_flags[@]}" \
  -v "${OUT_HOST}:/work/ionia/docker/smoke-output" \
  -v "${OUT_HOST}:/tmp/ionia-docker" \
  -e "LOW_MEMORY=${LOW_MEMORY:-1}" \
  -e "SMOKE_SHOW_LOGS=${SMOKE_SHOW_LOGS:-1}" \
  -e "SMOKE_TAIL_LINES=${SMOKE_TAIL_LINES:-40}" \
  "${IMG}" ./docker/run_udp_smoke.sh "$@"

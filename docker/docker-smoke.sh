#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_HOST="${REPO_ROOT}/docker/smoke-output"
mkdir -p "${OUT_HOST}"

IMG="${IONIA_UDP_IMAGE:-ionia-udp}"

run_flags=(--rm)
case "$(uname -m)" in
  arm64|aarch64) ;;
  *) run_flags+=(--platform linux/amd64) ;;
esac
[[ -t 1 ]] && run_flags+=(-it)

exec docker run "${run_flags[@]}" \
  -v "${OUT_HOST}:/work/ionia/docker/smoke-output" \
  -v "${OUT_HOST}:/tmp/ionia-docker" \
  -e "LOW_MEMORY=${LOW_MEMORY:-1}" \
  -e "SMOKE_PROTOCOL=${SMOKE_PROTOCOL:-vr}" \
  -e "SMOKE_DURABILITY_THREADS=${SMOKE_DURABILITY_THREADS:-1}" \
  -e "SMOKE_SHOW_LOGS=${SMOKE_SHOW_LOGS:-1}" \
  -e "SMOKE_TAIL_LINES=${SMOKE_TAIL_LINES:-40}" \
  -e "SMOKE_DYNA_DB=${SMOKE_DYNA_DB:-}" \
  -e "DYNA_DEBUG=${DYNA_DEBUG:-}" \
  -e "SMOKE_KILL_REPLICA=${SMOKE_KILL_REPLICA:-}" \
  -e "SMOKE_KILL_AFTER_MS=${SMOKE_KILL_AFTER_MS:-}" \
  -e "SMOKE_RESTART_AFTER_MS=${SMOKE_RESTART_AFTER_MS:-}" \
  -e "DYNA_FAIL_TIMEOUT_MS=${DYNA_FAIL_TIMEOUT_MS:-}" \
  -e "DYNA_BUSY_POLL=${DYNA_BUSY_POLL:-}" \
  -e "DYNA_SLOW_TIMEOUT_US=${DYNA_SLOW_TIMEOUT_US:-}" \
  -e "DYNA_INITIAL_BOUND_US=${DYNA_INITIAL_BOUND_US:-}" \
  -e "DYNA_MAX_OWD_US=${DYNA_MAX_OWD_US:-}" \
  -e "DYNA_OWD_WINDOW_LEN=${DYNA_OWD_WINDOW_LEN:-}" \
  -e "DYNA_OWD_PERCENTILE=${DYNA_OWD_PERCENTILE:-}" \
  "${IMG}" ./docker/run_udp_smoke.sh "$@"

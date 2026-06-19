#!/usr/bin/env bash
#
# run_read_correctness.sh
#   End-to-end read-after-write correctness check for DYNA.
#
#   Starts a local 3-replica DYNA cluster (f=1, the minimum that can commit,
#   since DYNA commits via follower SyncAck and a single replica never reaches
#   quorum) and runs the dyna-rw-verify client in two passes:
#     1. single-key freshness (write/read/overwrite/read)
#     2. interleaved multi-key (many keys written, read, double-overwritten and
#        read again in deterministically shuffled order)
#   Each read must return the latest committed value.
#
#   Run from the ionia source root (.../protocols/src/ionia) after `make`:
#       ./dyna/tests/run_read_correctness.sh
#
#   Env overrides:
#       CONFIG          cluster config (default sample/config, 3x localhost)
#       OUT_DIR         log directory (default /tmp/dyna-rw-test)
#       SHOW_LOGS       tail replica/client logs on exit (default 1)
#       NUM_KEYS        keys for the interleaved pass (default 16)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# .../protocols/src/ionia
IONIA_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${IONIA_DIR}"

CONFIG="${CONFIG:-sample/config}"
OUT_DIR="${OUT_DIR:-/tmp/dyna-rw-test}"
SHOW_LOGS="${SHOW_LOGS:-1}"
TAIL_LINES="${TAIL_LINES:-40}"
NUM_KEYS="${NUM_KEYS:-16}"

REPLICA_BIN="./bench/replica"
VERIFY_BIN="./dyna/tests/dyna-rw-verify"

if [[ ! -x "${REPLICA_BIN}" ]]; then
    echo "error: ${REPLICA_BIN} not found. Run 'make' first." >&2
    exit 2
fi
if [[ ! -x "${VERIFY_BIN}" ]]; then
    echo "error: ${VERIFY_BIN} not found. Run 'make' first." >&2
    exit 2
fi
if [[ ! -f "${CONFIG}" ]]; then
    echo "error: config ${CONFIG} not found." >&2
    exit 2
fi

# DYNA keeps its own in-memory KV store; memory mode avoids any stale on-disk
# state from a previous local run interfering with the check.
export IONIA_DB_MODE=memory

# Number of replicas = number of "replica " lines in the config.
NUM_REPLICAS="$(grep -c '^replica ' "${CONFIG}")"
if [[ "${NUM_REPLICAS}" -lt 3 ]]; then
    echo "warning: ${CONFIG} has ${NUM_REPLICAS} replica(s); DYNA needs >=3 (f=1) to commit." >&2
fi

mkdir -p "${OUT_DIR}"

pids=()
cleanup() {
    for pid in "${pids[@]:-}"; do
        if kill -0 "${pid}" 2>/dev/null; then
            kill "${pid}" 2>/dev/null || true
        fi
    done
}
trap cleanup EXIT

echo "Starting ${NUM_REPLICAS} DYNA replica(s) using ${CONFIG}..."
for (( i = 0; i < NUM_REPLICAS; i++ )); do
    "${REPLICA_BIN}" \
        -i "${i}" \
        -j 1 \
        -b 1 \
        -c "${CONFIG}" \
        -m dyna \
        > "${OUT_DIR}/replica-${i}.log" 2>&1 &
    pids+=("$!")
done

# Give replicas time to bind sockets and form the group.
sleep 3

rc=0

echo "Pass 1: single-key freshness..."
set +e
timeout 40s "${VERIFY_BIN}" -c "${CONFIG}" -t 15000 \
    > "${OUT_DIR}/verify-single.log" 2>&1
rc_single=$?
set -e
[[ "${rc_single}" -ne 0 ]] && rc=${rc_single}

echo "Pass 2: interleaved multi-key (${NUM_KEYS} keys)..."
set +e
timeout 60s "${VERIFY_BIN}" -c "${CONFIG}" -K "${NUM_KEYS}" -t 30000 \
    > "${OUT_DIR}/verify-multi.log" 2>&1
rc_multi=$?
set -e
[[ "${rc_multi}" -ne 0 ]] && rc=${rc_multi}

if [[ "${SHOW_LOGS}" == "1" ]]; then
    echo ""
    echo "---- verify-single.log ----"
    tail -n "${TAIL_LINES}" "${OUT_DIR}/verify-single.log" || true
    echo "---- verify-multi.log ----"
    tail -n "${TAIL_LINES}" "${OUT_DIR}/verify-multi.log" || true
    for (( i = 0; i < NUM_REPLICAS; i++ )); do
        echo "---- tail replica-${i}.log ----"
        tail -n "${TAIL_LINES}" "${OUT_DIR}/replica-${i}.log" || true
    done
fi

echo ""
echo "single-key pass exit=${rc_single}, multi-key pass exit=${rc_multi}"
if [[ "${rc}" -eq 0 ]]; then
    echo "DYNA read-correctness test PASSED."
else
    echo "DYNA read-correctness test FAILED. See ${OUT_DIR}/verify-*.log" >&2
fi
exit "${rc}"

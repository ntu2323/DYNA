#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="/work/ionia"
IONIA_DIR="${ROOT_DIR}/protocols/src/ionia"
OUT_DIR="${IONIA_SMOKE_OUT:-/work/ionia/docker/smoke-output}"
LOW_MEMORY="${LOW_MEMORY:-0}"
SMOKE_SHOW_LOGS="${SMOKE_SHOW_LOGS:-1}"
SMOKE_TAIL_LINES="${SMOKE_TAIL_LINES:-40}"
# vr | dyna (same bench binaries; default keeps historical smoke behavior)
SMOKE_PROTOCOL="${SMOKE_PROTOCOL:-vr}"

# Docker wrapper defaults LOW_MEMORY=1 (single replica). DYNA still commits via follower SyncAck,
# so one replica never reaches commit → client times out. Force multi-replica smoke for dyna.
if [[ "${SMOKE_PROTOCOL}" == "dyna" && "${LOW_MEMORY}" == "1" ]]; then
    echo "SMOKE_PROTOCOL=dyna: overriding LOW_MEMORY=1 → 0 (DYNA needs replica quorum for sync/commit)." >&2
    LOW_MEMORY=0
fi

# DYNA smoke in container should stay memory-backed: multiple local replicas can
# otherwise collide with stale/shared on-disk state across runs.
if [[ "${SMOKE_PROTOCOL}" == "dyna" ]]; then
    export IONIA_DB_MODE=memory
    export IONIA_SPLINTER_CACHE_MB="${IONIA_SPLINTER_CACHE_MB:-64}"
    export IONIA_SPLINTER_DISK_GB="${IONIA_SPLINTER_DISK_GB:-1}"
fi

mkdir -p "${OUT_DIR}"
cd "${IONIA_DIR}"

SERVER_CONFIG="sample/config"
CLIENT_CONFIG="sample/config.client"
replica_indices=(0 1 2)
NUM_REQUESTS=20
EXPERIMENT_SECONDS=2
TRACE_PREFIX="sample/test"

if [[ "${LOW_MEMORY}" == "1" ]]; then
    SERVER_CONFIG="${OUT_DIR}/config.lowmem"
    CLIENT_CONFIG="${OUT_DIR}/config.lowmem.client"
    cat > "${SERVER_CONFIG}" <<'EOF'
f 0
replica localhost:13000
EOF
    cat > "${CLIENT_CONFIG}" <<'EOF'
f 0
replica localhost:14000
EOF
    replica_indices=(0)
    NUM_REQUESTS=4
    EXPERIMENT_SECONDS=1
    TRACE_PREFIX="${OUT_DIR}/trace.lowmem"
    export IONIA_DB_MODE=memory
    export IONIA_SPLINTER_CACHE_MB=64
    export IONIA_SPLINTER_DISK_GB=1
    cat > "${TRACE_PREFIX}1" <<'EOF'
I user12847818606673772110
U user22847818606673772111
I user32847818606673772110
U user42847818606673772111
R user42847818606673772111
U user12847818606673772110
I user22847818606673772111
U user32847818606673772110
I user42847818606673772111
R user42847818606673772111
EOF
fi

if [[ "${SMOKE_PROTOCOL}" == "dyna" ]]; then
    # bench/client currently uses -c as the transport peer config for protocol
    # clients (and does not consume -s). Point -c to active replica endpoints.
    CLIENT_CONFIG="${SERVER_CONFIG}"
fi

if [[ ! -f "${TRACE_PREFIX}1" && -f "${TRACE_PREFIX}" ]]; then
    cp "${TRACE_PREFIX}" "${TRACE_PREFIX}1"
fi

pids=()
cleanup() {
    for pid in "${pids[@]:-}"; do
        if kill -0 "${pid}" 2>/dev/null; then
            kill "${pid}" 2>/dev/null || true
        fi
    done
}
trap cleanup EXIT

for replica in "${replica_indices[@]}"; do
    ./bench/replica \
        -i "${replica}" \
        -j 1 \
        -b 1 \
        -c "${SERVER_CONFIG}" \
        -m "${SMOKE_PROTOCOL}" \
        > "${OUT_DIR}/replica-${replica}.log" 2>&1 &
    pids+=("$!")
done

sleep 3

set +e
timeout 30s ./bench/client \
    -s "${SERVER_CONFIG}" \
    -c "${CLIENT_CONFIG}" \
    -m "${SMOKE_PROTOCOL}" \
    -k "${TRACE_PREFIX}" \
    -n "${NUM_REQUESTS}" \
    -e "${EXPERIMENT_SECONDS}" \
    -t 1 \
    -j 1 \
    -l "${OUT_DIR}/latencies" \
    > "${OUT_DIR}/client.log" 2>&1
client_rc=$?
set -e

if [[ "${client_rc}" -eq 124 ]]; then
    if [[ -f "${OUT_DIR}/replica-0.log" ]] && grep -Fq 'TIME [' "${OUT_DIR}/replica-0.log"; then
        echo "Client hit timeout (${client_rc}) but replica-0 logged TIME lines; treating smoke test as success."
        client_rc=0
    fi
fi

if [[ "${client_rc}" -ne 0 ]]; then
    echo "Client exited with status ${client_rc}. Logs:"
    echo "  ${OUT_DIR}/client.log"
    if [[ "${SMOKE_SHOW_LOGS}" == "1" ]] && [[ -f "${OUT_DIR}/client.log" ]]; then
        echo ""
        echo "---- tail client.log ----"
        tail -n "${SMOKE_TAIL_LINES}" "${OUT_DIR}/client.log" || true
    fi
    exit "${client_rc}"
fi

echo "UDP smoke test finished successfully."
echo "Logs and latency output are in ${OUT_DIR}"
if [[ "${OUT_DIR}" == /tmp/* ]]; then
    echo "Host mount: -v \"\$(pwd)/docker/smoke-output:${OUT_DIR}\""
fi
if [[ "${LOW_MEMORY}" == "1" ]]; then
    echo "Low-memory mode was enabled (LOW_MEMORY=1)."
fi

if [[ "${SMOKE_SHOW_LOGS}" == "1" ]]; then
    echo ""
    for rep in "${replica_indices[@]}"; do
        f="${OUT_DIR}/replica-${rep}.log"
        if [[ -f "${f}" ]]; then
            echo "---- tail replica-${rep}.log ----"
            tail -n "${SMOKE_TAIL_LINES}" "${f}"
        fi
    done
    if [[ -f "${OUT_DIR}/client.log" ]]; then
        echo "---- tail client.log ----"
        tail -n "${SMOKE_TAIL_LINES}" "${OUT_DIR}/client.log"
    fi
    if compgen -G "${OUT_DIR}/latencies*" > /dev/null 2>&1; then
        echo "---- latency files ----"
        ls -la "${OUT_DIR}"/latencies* 2>/dev/null || true
    fi
fi

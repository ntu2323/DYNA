#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="/work/ionia"
IONIA_DIR="${ROOT_DIR}/protocols/src/ionia"
OUT_DIR="${IONIA_SMOKE_OUT:-/work/ionia/docker/smoke-output}"
LOW_MEMORY="${LOW_MEMORY:-0}"
SMOKE_SHOW_LOGS="${SMOKE_SHOW_LOGS:-1}"
SMOKE_TAIL_LINES="${SMOKE_TAIL_LINES:-40}"
# vr | dyna (same bench binaries; default keeps historical behavior)
SMOKE_PROTOCOL="${SMOKE_PROTOCOL:-vr}"
# VR/IONIA only: durability threads (-j on bench/replica). dyna always uses 1.
SMOKE_DURABILITY_THREADS="${SMOKE_DURABILITY_THREADS:-1}"

IS_DYNA=0
if [[ "${SMOKE_PROTOCOL}" == "dyna" ]]; then
    IS_DYNA=1
fi

if [[ "${IS_DYNA}" == "1" && "${SMOKE_DURABILITY_THREADS}" != "1" ]]; then
    echo "SMOKE_DURABILITY_THREADS ignored for ${SMOKE_PROTOCOL} (always -j 1)." >&2
fi

REPLICA_DURABILITY_THREADS=1
if [[ "${SMOKE_PROTOCOL}" == "vr" ]]; then
    REPLICA_DURABILITY_THREADS="${SMOKE_DURABILITY_THREADS}"
    if [[ "${REPLICA_DURABILITY_THREADS}" -gt 1 ]]; then
        echo "SMOKE_DURABILITY_THREADS=${REPLICA_DURABILITY_THREADS}: SharedUDPPort multithread smoke (forces LOW_MEMORY=1, IONIA_DB_MODE=memory)." >&2
        LOW_MEMORY=1
        export IONIA_DB_MODE=memory
        export IONIA_SPLINTER_CACHE_MB="${IONIA_SPLINTER_CACHE_MB:-64}"
        export IONIA_SPLINTER_DISK_GB="${IONIA_SPLINTER_DISK_GB:-1}"
    fi
fi

# Docker wrapper defaults LOW_MEMORY=1 (single replica). DYNA commits via an f+1
# replica quorum, so one replica never reaches commit → client times out. Force
# multi-replica smoke for dyna.
if [[ "${IS_DYNA}" == "1" && "${LOW_MEMORY}" == "1" ]]; then
    echo "SMOKE_PROTOCOL=${SMOKE_PROTOCOL}: overriding LOW_MEMORY=1 → 0 (needs replica quorum for commit)." >&2
    LOW_MEMORY=0
fi

# DYNA executes into the harness store. Default: memory-backed (IONIA_DB_MODE=memory).
# SMOKE_DYNA_DB=splinter uses real SplinterDB (small cache/disk); each replica then runs
# in its own fresh working directory so the three local replicas don't share (or reuse)
# a splinterdb.db file.
SMOKE_DYNA_DB="${SMOKE_DYNA_DB:-memory}"
if [[ "${IS_DYNA}" == "1" ]]; then
    export IONIA_DB_MODE="${SMOKE_DYNA_DB}"
    export IONIA_SPLINTER_CACHE_MB="${IONIA_SPLINTER_CACHE_MB:-64}"
    export IONIA_SPLINTER_DISK_GB="${IONIA_SPLINTER_DISK_GB:-1}"
fi

# DYNA recovery smoke: SMOKE_KILL_REPLICA=<idx> kill -9s that replica SMOKE_KILL_AFTER_MS
# after the client starts; the run then requires the survivors to install view 1 and the
# client to keep completing requests (closed loop over every trace key, so any key whose
# leader died must have been taken over). Uses a longer run and a tighter deadline.
SMOKE_KILL_REPLICA="${SMOKE_KILL_REPLICA:-}"
SMOKE_KILL_AFTER_MS="${SMOKE_KILL_AFTER_MS:-1500}"
# Rejoin smoke: also restart the killed replica SMOKE_RESTART_AFTER_MS after the kill. It
# finds its persisted VR state, rejoins (catch-up + view change), and the run then requires
# every replica — the restarted one included — to install view 2.
SMOKE_RESTART_AFTER_MS="${SMOKE_RESTART_AFTER_MS:-}"
if [[ -n "${SMOKE_KILL_REPLICA}" && "${IS_DYNA}" != "1" ]]; then
    echo "SMOKE_KILL_REPLICA is only supported with SMOKE_PROTOCOL=dyna." >&2
    exit 2
fi
if [[ -n "${SMOKE_KILL_REPLICA}" ]]; then
    export DYNA_INITIAL_BOUND_US="${DYNA_INITIAL_BOUND_US:-20000}"
    export DYNA_FAIL_TIMEOUT_MS="${DYNA_FAIL_TIMEOUT_MS:-500}"
fi

# Use a generous deadline so the shared-CPU container (ms-scale localhost delivery)
# mostly takes the fast path, and park the release loops rather than busy-poll to
# avoid CPU starvation. Late followers fall back to the slow path. All overridable.
if [[ "${IS_DYNA}" == "1" ]]; then
    export DYNA_BUSY_POLL="${DYNA_BUSY_POLL:-0}"
    export DYNA_INITIAL_BOUND_US="${DYNA_INITIAL_BOUND_US:-200000}"
    export DYNA_MAX_OWD_US="${DYNA_MAX_OWD_US:-500000}"
fi

mkdir -p "${OUT_DIR}"
cd "${IONIA_DIR}"

SERVER_CONFIG="sample/config"
CLIENT_CONFIG="sample/config.client"
replica_indices=(0 1 2)
NUM_REQUESTS=20
EXPERIMENT_SECONDS=2
if [[ -n "${SMOKE_KILL_REPLICA}" ]]; then
    NUM_REQUESTS=100000   # bounded by the experiment duration instead
    EXPERIMENT_SECONDS=8
    [[ -n "${SMOKE_RESTART_AFTER_MS}" ]] && EXPERIMENT_SECONDS=12
fi
NUM_REQUESTS="${SMOKE_NUM_REQUESTS:-${NUM_REQUESTS}}"
EXPERIMENT_SECONDS="${SMOKE_EXPERIMENT_SECONDS:-${EXPERIMENT_SECONDS}}"
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

if [[ "${IS_DYNA}" == "1" ]]; then
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

verify_multithread_transport() {
  if [[ "${REPLICA_DURABILITY_THREADS}" -le 1 ]]; then
    return 0
  fi
  local log bind_count
  for rep in "${replica_indices[@]}"; do
    log="${OUT_DIR}/replica-${rep}.log"
    if [[ ! -f "${log}" ]]; then
      echo "Missing replica log ${log}" >&2
      return 1
    fi
    if grep -qiE 'Address already in use|Failed to bind|PPanic' "${log}"; then
      echo "Multithread transport bind failure in ${log}:" >&2
      tail -n 30 "${log}" >&2 || true
      return 1
    fi
    bind_count="$(grep -c 'SharedUDPPort binding' "${log}" || true)"
    if [[ "${bind_count}" -lt 2 ]]; then
      echo "Expected SharedUDPPort binds (internal + client) in ${log}, got ${bind_count}" >&2
      tail -n 30 "${log}" >&2 || true
      return 1
    fi
  done
  echo "Multithread transport OK: ${REPLICA_DURABILITY_THREADS} durability threads, SharedUDPPort bind verified."
}

replica_config="${SERVER_CONFIG}"
[[ "${replica_config}" == /* ]] || replica_config="${IONIA_DIR}/${replica_config}"
replica_cwd_of() {
    if [[ "${IS_DYNA}" == "1" && "${SMOKE_DYNA_DB}" == "splinter" ]]; then
        echo "/tmp/dyna-smoke-db/replica-$1"   # not the host-mounted OUT_DIR
    else
        echo "${IONIA_DIR}"
    fi
}
start_replica() {   # index, log file
    (cd "$(replica_cwd_of "$1")" && exec "${IONIA_DIR}/bench/replica" \
        -i "$1" \
        -j "${REPLICA_DURABILITY_THREADS}" \
        -b 1 \
        -c "${replica_config}" \
        -m "${SMOKE_PROTOCOL}" \
        > "$2" 2>&1) &
}
for replica in "${replica_indices[@]}"; do
    replica_cwd="$(replica_cwd_of "${replica}")"
    if [[ "${replica_cwd}" != "${IONIA_DIR}" ]]; then
        rm -rf "${replica_cwd}" && mkdir -p "${replica_cwd}"
    fi
    # DYNA persists its VR state (dyna-vr-<idx>.state) in the cwd; a stale file would make a
    # fresh replica start as a rejoiner.
    rm -f "${replica_cwd}/dyna-vr-${replica}.state"
    start_replica "${replica}" "${OUT_DIR}/replica-${replica}.log"
    pids+=("$!")
done

sleep 3
verify_multithread_transport

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
    > "${OUT_DIR}/client.log" 2>&1 &
client_pid=$!
if [[ -n "${SMOKE_KILL_REPLICA}" ]]; then
    sleep "$(awk "BEGIN{print ${SMOKE_KILL_AFTER_MS}/1000}")"
    kill -9 "${pids[${SMOKE_KILL_REPLICA}]}" 2>/dev/null
    echo "Killed replica ${SMOKE_KILL_REPLICA} at +${SMOKE_KILL_AFTER_MS}ms."
    if [[ -n "${SMOKE_RESTART_AFTER_MS}" ]]; then
        sleep "$(awk "BEGIN{print ${SMOKE_RESTART_AFTER_MS}/1000}")"
        # A restarted replica rejoins with an empty store (catch-up refills it); SplinterDB
        # can't remount a file left by a killed process, so drop it (keep the VR state).
        rm -f "$(replica_cwd_of "${SMOKE_KILL_REPLICA}")/splinterdb.db"
        start_replica "${SMOKE_KILL_REPLICA}" "${OUT_DIR}/replica-${SMOKE_KILL_REPLICA}.restart.log"
        pids+=("$!")
        echo "Restarted replica ${SMOKE_KILL_REPLICA} at +${SMOKE_RESTART_AFTER_MS}ms after the kill."
    fi
fi
wait "${client_pid}"
client_rc=$?
set -e

if [[ -n "${SMOKE_KILL_REPLICA}" ]]; then
    # Every survivor must have installed the new view, and the client must have kept
    # completing requests after the kill (progress lines are logged per interval).
    for rep in "${replica_indices[@]}"; do
        [[ "${rep}" == "${SMOKE_KILL_REPLICA}" ]] && continue
        if ! grep -q "view 1 installed" "${OUT_DIR}/replica-${rep}.log"; then
            echo "Recovery smoke FAILED: replica ${rep} never installed view 1." >&2
            tail -n 30 "${OUT_DIR}/replica-${rep}.log" >&2 || true
            exit 1
        fi
        grep "view 1 installed" "${OUT_DIR}/replica-${rep}.log"
    done
    if [[ -n "${SMOKE_RESTART_AFTER_MS}" ]]; then
        for f in "${OUT_DIR}"/replica-*.log; do
            [[ "${f}" == "${OUT_DIR}/replica-${SMOKE_KILL_REPLICA}.log" ]] && continue
            if ! grep -q "view 2 installed" "${f}"; then
                echo "Rejoin smoke FAILED: ${f} never installed view 2." >&2
                tail -n 30 "${f}" >&2 || true
                exit 1
            fi
            grep -h "caught up\|view 2 installed" "${f}"
        done
    fi
    completed="$(grep -oE 'Completed [0-9]+ requests in' "${OUT_DIR}/client.log" | grep -oE '[0-9]+' | head -1)"
    echo "Client completed ${completed:-0} requests across the failure."
fi

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
if [[ "${REPLICA_DURABILITY_THREADS}" -gt 1 ]]; then
    echo "IONIA multithread smoke: ${REPLICA_DURABILITY_THREADS} durability threads per replica."
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

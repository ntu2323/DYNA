#!/usr/bin/env bash
#
# tune_node.sh — low-latency host tuning for benchmark nodes (run with sudo).
#
#   sudo ./tune_node.sh apply     # performance governor + no deep C-states
#   sudo ./tune_node.sh status
#   sudo ./tune_node.sh revert    # back to the image defaults (schedutil, all C-states)
#
# Why: with C3/C6 enabled a parked core takes 33–133 µs to wake, which landed on every
# packet: measured kernel ping RTT 64 µs vs 33 µs with C-states off (CloudLab c6320). That
# inflates request latency and makes DYNA's one-way-delay estimates noisy. Only states
# with exit latency > 2 µs are disabled (POLL and C1 stay), so idle cores still halt.
#
# Network: the image caps socket buffers at net.core.rmem_max = 208 KB, so the
# transport's 10 MB SO_RCVBUF request was silently clamped and bursts overflowed it
# (hundreds of millions of UDP "receive buffer errors" on the replicas -> resends, fetches,
# slow path, collapse). Raise rmem/wmem_max, netdev_max_backlog, and the experiment
# NIC's rings (512 -> hardware max; rx_missed_errors was climbing too).
#
# IRQs: irqbalance spreads the NIC's receive-queue interrupts over the NIC node's CPUs —
# the very cores the replica's busy-polling threads are pinned to (setup_dyna.sh: every
# physical core of the NIC node but the first). Softirq processing then competes with a
# thread spinning at 100% and the NIC's rings overflow (rx_missed_errors climbed into the
# tens of millions at ~170k ops/s with 4 DYNA lanes). Stop irqbalance and steer the
# queues' IRQs to the other NUMA node plus the NIC node's first core (IRQ_CPUS overrides,
# e.g. IRQ_CPUS=0,20). IRQ_STEER=0 skips this.
# Not persistent across reboot.

set -euo pipefail
ACTION="${1:-apply}"
[[ $EUID -eq 0 ]] || { echo "run with sudo/root" >&2; exit 1; }

if ! command -v cpupower >/dev/null 2>&1 || ! cpupower --version >/dev/null 2>&1; then
    # A fresh CloudLab image has no package index yet ("Unable to locate package").
    apt-get update -qq >/dev/null
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
        "linux-tools-$(uname -r)" linux-tools-common >/dev/null
fi

EXP_PREFIX="${EXP_PREFIX:-10.10.}"
IFACE=$(ip -4 -o addr show | awk -v p="^${EXP_PREFIX//./[.]}" '$4 ~ p {print $2; exit}')

steer_irqs() {
    [[ -n "$IFACE" && "${IRQ_STEER:-1}" != "0" ]] || return 0
    local node cpus
    node=$(cat "/sys/class/net/${IFACE}/device/numa_node" 2>/dev/null || echo 0)
    (( node < 0 )) && node=0
    if [[ -n "${IRQ_CPUS:-}" ]]; then
        IFS=, read -r -a cpus <<< "${IRQ_CPUS}"
    else
        # Other node(s), then the NIC node's first physical core (both its threads).
        mapfile -t cpus < <(lscpu -p=CPU,CORE,NODE | grep -v '^#' | awk -F, -v n="$node" '
            $3 != n { print $1; next }
            first == "" { first = $2 }
            $2 == first { print $1 }')
    fi
    (( ${#cpus[@]} > 0 )) || return 0
    systemctl stop irqbalance 2>/dev/null || true
    local i=0 irq
    for irq in $(grep "${IFACE}" /proc/interrupts | awk -F: '{print $1}' | tr -d ' '); do
        echo "${cpus[i % ${#cpus[@]}]}" > "/proc/irq/${irq}/smp_affinity_list" 2>/dev/null || true
        i=$((i + 1))
    done
    echo "irqs: ${i} ${IFACE} IRQs -> CPUs $(IFS=,; echo "${cpus[*]}")"
}

show() {
    echo "governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo n/a)"
    echo "nic: $(ethtool -c "$IFACE" 2>/dev/null | awk '/^rx-usecs:/{print "rx-usecs=" $2}')"
    echo "net: rmem_max=$(sysctl -n net.core.rmem_max) wmem_max=$(sysctl -n net.core.wmem_max) backlog=$(sysctl -n net.core.netdev_max_backlog) ${IFACE:-?} rings rx/tx=$(ethtool -g "$IFACE" 2>/dev/null | awk '/Current/{c=1} c&&/^RX:/{r=$2} c&&/^TX:/{t=$2} END{print r"/"t}')"
    for s in /sys/devices/system/cpu/cpu0/cpuidle/state*; do
        [[ -d "$s" ]] || continue
        printf '  %-5s latency=%4sus disabled=%s\n' "$(cat "$s/name")" "$(cat "$s/latency")" \
            "$(cat "$s/disable")"
    done
}

case "$ACTION" in
    apply)
        cpupower frequency-set -g performance >/dev/null 2>&1 || true
        cpupower idle-set -D 2 >/dev/null
        sysctl -qw net.core.rmem_max=67108864 net.core.wmem_max=67108864 \
            net.core.netdev_max_backlog=250000
        if [[ -n "$IFACE" ]]; then
            max_rx=$(ethtool -g "$IFACE" | awk '/Pre-set/{p=1} p&&/^RX:/{print $2; exit}')
            max_tx=$(ethtool -g "$IFACE" | awk '/Pre-set/{p=1} p&&/^TX:/{print $2; exit}')
            ethtool -G "$IFACE" rx "$max_rx" tx "$max_tx" 2>/dev/null || true
            # Interrupt moderation off: ixgbe's adaptive ITR (rx-usecs 1) batches
            # interrupts as packet rates rise, adding load-dependent latency (ping RTT
            # 45 -> 21 µs, DYNA's p95 one-way delay at 150k ops/s 400 -> 300 µs). The
            # IRQs run on otherwise idle CPUs (steer_irqs), so the extra interrupts are free.
            ethtool -C "$IFACE" rx-usecs "${RX_USECS:-0}" 2>/dev/null || true
        fi
        steer_irqs
        show
        ;;
    revert)
        cpupower frequency-set -g schedutil >/dev/null 2>&1 || true
        cpupower idle-set -E >/dev/null
        sysctl -qw net.core.rmem_max=212992 net.core.wmem_max=212992 \
            net.core.netdev_max_backlog=1000
        [[ -n "$IFACE" ]] && ethtool -G "$IFACE" rx 512 tx 512 2>/dev/null || true
        [[ -n "$IFACE" ]] && ethtool -C "$IFACE" rx-usecs 1 2>/dev/null || true
        systemctl start irqbalance 2>/dev/null || true
        show
        ;;
    status) show ;;
    *) echo "usage: $0 apply|status|revert" >&2; exit 1 ;;
esac

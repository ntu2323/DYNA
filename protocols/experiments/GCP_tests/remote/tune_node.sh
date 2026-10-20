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
# Not persistent across reboot.

set -euo pipefail
ACTION="${1:-apply}"
[[ $EUID -eq 0 ]] || { echo "run with sudo/root" >&2; exit 1; }

if ! command -v cpupower >/dev/null 2>&1 || ! cpupower --version >/dev/null 2>&1; then
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
        "linux-tools-$(uname -r)" linux-tools-common >/dev/null
fi

show() {
    echo "governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo n/a)"
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
        show
        ;;
    revert)
        cpupower frequency-set -g schedutil >/dev/null 2>&1 || true
        cpupower idle-set -E >/dev/null
        show
        ;;
    status) show ;;
    *) echo "usage: $0 apply|status|revert" >&2; exit 1 ;;
esac

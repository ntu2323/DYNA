#!/usr/bin/env bash
#
# setup-ptp.sh — configure hardware-timestamped PTP clock sync on a node.
#
# Usage:
#   sudo ./setup-ptp.sh master            # run on ONE node (the grandmaster)
#   sudo ./setup-ptp.sh follower          # run on every other node
#   sudo ./setup-ptp.sh status            # show current sync state
#   sudo ./setup-ptp.sh stop              # tear down the services
#
# Optional: override the experiment-subnet prefix used to pick the NIC.
#   EXP_PREFIX=10.10. sudo ./setup-ptp.sh follower
#
# Assumptions (all verified earlier on these nodes):
#   - NIC supports hardware timestamping (ethtool -T shows hardware-*).
#   - All nodes share one L2 experiment VLAN reachable on EXP_PREFIX.
#   - Transport is L3/UDP (default). L2 (-2) was dropped on this fabric.

set -euo pipefail

ROLE="${1:-}"
EXP_PREFIX="${EXP_PREFIX:-10.}"   # match the experiment-net IP, e.g. 10.10.1.x

die() { echo "ERROR: $*" >&2; exit 1; }

[[ $EUID -eq 0 ]] || die "run with sudo/root"

# ---- locate the experiment-net interface and its PTP hardware clock ----
detect_iface() {
    ip -4 -o addr show \
        | awk -v p="^${EXP_PREFIX//./\\.}" '$4 ~ p {print $2; exit}'
}

detect_phc() {
    # prints the PHC index, e.g. 0, or empty if none
    ethtool -T "$1" 2>/dev/null \
        | awk '/PTP Hardware Clock/{print $4}'
}

teardown() {
    systemctl stop phc2sys.service ptp4l.service 2>/dev/null || true
    systemctl disable phc2sys.service ptp4l.service 2>/dev/null || true
    echo "PTP services stopped and disabled."
}

show_status() {
    local iface; iface=$(detect_iface) || true
    echo "== experiment iface: ${iface:-<none found>} =="
    systemctl --no-pager --lines=3 status ptp4l.service 2>/dev/null || true
    echo
    systemctl --no-pager --lines=3 status phc2sys.service 2>/dev/null || true
    echo
    echo "== offset to master (5 samples) =="
    for _ in 1 2 3 4 5; do
        pmc -u -b 0 'GET CURRENT_DATA_SET' \
            | awk '/offsetFromMaster|meanPathDelay/{printf "%-18s %s\n",$1,$2}'
        sleep 1
    done
}

case "$ROLE" in
    status) show_status; exit 0 ;;
    stop)   teardown;    exit 0 ;;
    master|follower) : ;;
    *) die "first argument must be: master | follower | status | stop" ;;
esac

# ---- preflight ----
command -v ptp4l >/dev/null 2>&1 || {
    echo "installing linuxptp..."
    apt-get update -qq && apt-get install -y -qq linuxptp
}

IFACE=$(detect_iface) || true
[[ -n "${IFACE:-}" ]] || die "no interface found on subnet ${EXP_PREFIX}* (set EXP_PREFIX)"

PHC=$(detect_phc "$IFACE")
[[ "$PHC" =~ ^[0-9]+$ ]] || die "interface $IFACE has no PTP hardware clock (ethtool -T $IFACE)"

echo "node role : $ROLE"
echo "interface : $IFACE"
echo "PHC index : $PHC  (/dev/ptp$PHC)"

# ---- write the ptp4l service ----
# master: grandmaster (no -s). follower: slaveOnly (-s) so it never self-elects.
if [[ "$ROLE" == master ]]; then
    PTP_FLAGS="-i $IFACE -H -m"
else
    PTP_FLAGS="-i $IFACE -H -s -m"
fi

cat > /etc/systemd/system/ptp4l.service <<EOF
[Unit]
Description=PTP boundary/ordinary clock (ptp4l) on $IFACE
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/sbin/ptp4l $PTP_FLAGS
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF

# ---- write the phc2sys service ----
# master  : system clock (UTC, kept by chrony) -> PHC, seeds real time.
# follower: PHC (synced to master) -> system clock; -w waits for ptp4l lock.
if [[ "$ROLE" == master ]]; then
    P2S_FLAGS="-s CLOCK_REALTIME -c $IFACE -O 0 -m"
else
    P2S_FLAGS="-s $IFACE -c CLOCK_REALTIME -O 0 -w -m"
fi

cat > /etc/systemd/system/phc2sys.service <<EOF
[Unit]
Description=Sync system clock and PHC (phc2sys) on $IFACE
After=ptp4l.service
Requires=ptp4l.service

[Service]
ExecStart=/usr/sbin/phc2sys $P2S_FLAGS
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF

# ---- on followers, stop daemons that fight phc2sys for the system clock ----
if [[ "$ROLE" == follower ]]; then
    timedatectl set-ntp false 2>/dev/null || true
    for svc in chrony chronyd ntp ntpd systemd-timesyncd; do
        systemctl stop  "$svc" 2>/dev/null || true
        systemctl mask  "$svc" 2>/dev/null || true   # mask so it can't restart
    done
fi

systemctl daemon-reload
systemctl enable --now ptp4l.service
systemctl enable --now phc2sys.service

echo
echo "Done. Watch convergence with:   sudo ./setup-ptp.sh status"
echo "Or live:   journalctl -u ptp4l -u phc2sys -f"
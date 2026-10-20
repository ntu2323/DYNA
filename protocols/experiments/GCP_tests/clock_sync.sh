#!/usr/bin/env bash
#
# clock_sync.sh — configure cluster clock sync on a node: hardware-timestamped PTP
# (ptp4l + phc2sys) when the experiment NIC has a PTP hardware clock, else chrony.
#
# Usage:
#   sudo ./clock_sync.sh master            # run on ONE node (the time source)
#   sudo MASTER_IP=10.10.1.1 ./clock_sync.sh follower   # every other node
#   sudo ./clock_sync.sh status            # show current sync state
#   sudo ./clock_sync.sh stop              # tear down PTP / restore chrony defaults
#
# CLOCK_MODE=auto (default) | ptp | chrony. auto = PTP if `ethtool -T` shows a PTP
# hardware clock on the experiment NIC, else chrony (NTP with hardware timestamping where
# the NIC supports it, polling the master 16x/s). MASTER_IP (the master's experiment-net
# address) is required for chrony followers.
# Optional: override the experiment-subnet prefix used to pick the NIC.
#   EXP_PREFIX=10.10. sudo ./clock_sync.sh follower
#
# Verify the RESULT with check_clock_sync.py (measures pairwise CLOCK_REALTIME offsets);
# the daemons' own offset reports don't include host-stack asymmetry.
#
# Assumptions (all verified earlier on these nodes):
#   - NIC supports hardware timestamping (ethtool -T shows hardware-*).
#   - All nodes share one L2 experiment VLAN reachable on EXP_PREFIX.
#   - Transport is L3/UDP (default). L2 (-2) was dropped on this fabric.

set -euo pipefail

ROLE="${1:-}"
EXP_PREFIX="${EXP_PREFIX:-10.}"   # match the experiment-net IP, e.g. 10.10.1.x
CLOCK_MODE="${CLOCK_MODE:-auto}"
MASTER_IP="${MASTER_IP:-}"
CHRONY_CONF=/etc/chrony/chrony.conf

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

teardown_ptp() {
    systemctl stop phc2sys.service ptp4l.service 2>/dev/null || true
    systemctl disable phc2sys.service ptp4l.service 2>/dev/null || true
}

teardown() {
    teardown_ptp
    echo "PTP services stopped and disabled."
    if [[ -f "${CHRONY_CONF}.orig" ]]; then
        cp "${CHRONY_CONF}.orig" "$CHRONY_CONF"
        systemctl restart chrony 2>/dev/null || true
        echo "chrony config restored from ${CHRONY_CONF}.orig."
    fi
}

# chrony: master serves its own (free-running) clock to the experiment net; followers poll it 16x/s with interleaved mode
# and NIC hardware timestamps (chrony silently falls back to software timestamps).
setup_chrony() {
    local role="$1"
    teardown_ptp
    if ! command -v chronyd >/dev/null 2>&1; then
        echo "installing chrony..."
        apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq chrony
    fi
    for svc in ntp ntpd systemd-timesyncd; do
        systemctl stop "$svc" 2>/dev/null || true
        systemctl mask "$svc" 2>/dev/null || true
    done
    systemctl unmask chrony 2>/dev/null || true
    [[ -f "${CHRONY_CONF}.orig" ]] || cp "$CHRONY_CONF" "${CHRONY_CONF}.orig"
    {
        echo "# written by clock_sync.sh ($role)"
        if [[ "$role" == master ]]; then
            # No upstream servers: the master serves its OWN system clock. With internet
            # sources it serves its estimate of UTC while its system clock is still being
            # slewed toward it (measured: followers agreed to 1 us, the master was 414 us
            # off). DYNA needs the nodes to agree with each other, not with UTC.
            echo "local stratum 8"
            echo "allow"   # experiment-net clients (nodes are not internet-facing on it)
        else
            [[ -n "$MASTER_IP" ]] || die "chrony follower needs MASTER_IP"
            echo "server $MASTER_IP iburst minpoll -4 maxpoll -4 xleave prefer"
        fi
        echo "hwtimestamp $IFACE"
        echo "makestep 0.1 3"
        echo "driftfile /var/lib/chrony/chrony.drift"
        echo "rtcsync"
    } > "$CHRONY_CONF"
    systemctl enable chrony >/dev/null 2>&1 || true
    systemctl restart chrony
    echo "node role : $role (chrony)"
    echo "interface : $IFACE"
    echo
    echo "Done. Watch convergence with:   chronyc tracking; chronyc sources -v"
}

show_status() {
    local iface; iface=$(detect_iface) || true
    echo "== experiment iface: ${iface:-<none found>} =="
    systemctl --no-pager --lines=3 status ptp4l.service 2>/dev/null || true
    echo
    systemctl --no-pager --lines=3 status phc2sys.service 2>/dev/null || true
    echo
    if systemctl is-active --quiet chrony 2>/dev/null; then
        echo "== chrony =="
        chronyc tracking 2>/dev/null | grep -E 'Reference|System time|RMS offset|Last offset' || true
        chronyc sources 2>/dev/null | tail -n +3 || true
    fi
    systemctl is-active --quiet ptp4l 2>/dev/null || return 0
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
IFACE=$(detect_iface) || true
[[ -n "${IFACE:-}" ]] || die "no interface found on subnet ${EXP_PREFIX}* (set EXP_PREFIX)"

PHC=$(detect_phc "$IFACE")
case "$CLOCK_MODE" in
    auto)   if [[ "$PHC" =~ ^[0-9]+$ ]]; then CLOCK_MODE=ptp; else CLOCK_MODE=chrony; fi ;;
    ptp)    [[ "$PHC" =~ ^[0-9]+$ ]] || die "interface $IFACE has no PTP hardware clock (ethtool -T $IFACE); use CLOCK_MODE=chrony" ;;
    chrony) ;;
    *)      die "CLOCK_MODE must be auto | ptp | chrony" ;;
esac
if [[ "$CLOCK_MODE" == chrony ]]; then
    setup_chrony "$ROLE"
    exit 0
fi

command -v ptp4l >/dev/null 2>&1 || {
    echo "installing linuxptp..."
    apt-get update -qq && apt-get install -y -qq linuxptp
}

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
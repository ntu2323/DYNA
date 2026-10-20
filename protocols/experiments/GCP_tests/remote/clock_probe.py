#!/usr/bin/env python3
"""
Measure CLOCK_REALTIME offsets between cluster nodes over the experiment LAN.

Independent of PTP/chrony's own status: it compares the wall clocks DYNA actually
reads (CLOCK_REALTIME, like WallClockMicros()) with NTP-style UDP probes.

  server mode:  clock_probe.py serve <bind_ip> <port> <seconds>
  client mode:  clock_probe.py probe <peer_ip> <port> <count>

Each probe: t1 = local send, t2 = peer receive, t3 = peer send, t4 = local receive
(all time.time_ns()). offset = ((t2 - t1) + (t3 - t4)) / 2 is the peer clock minus the
local clock; its error is bounded by the asymmetry of the round trip, so only the
samples with the smallest RTT are kept. Prints one JSON line.
"""

import json
import socket
import struct
import sys
import time

_FMT = "!QQQ"   # seq, t2, t3


def serve(bind_ip: str, port: int, seconds: float) -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((bind_ip, port))
    s.settimeout(0.5)
    end = time.time() + seconds
    while time.time() < end:
        try:
            data, addr = s.recvfrom(64)
        except socket.timeout:
            continue
        t2 = time.time_ns()
        (seq,) = struct.unpack("!Q", data[:8])
        t3 = time.time_ns()
        s.sendto(struct.pack(_FMT, seq, t2, t3), addr)


def probe(peer_ip: str, port: int, count: int) -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0.2)
    samples = []   # (rtt_ns, offset_ns)
    lost = 0
    for seq in range(count):
        t1 = time.time_ns()
        s.sendto(struct.pack("!Q", seq), (peer_ip, port))
        while True:
            try:
                data, _ = s.recvfrom(64)
            except socket.timeout:
                lost += 1
                break
            t4 = time.time_ns()
            rseq, t2, t3 = struct.unpack(_FMT, data[:24])
            if rseq != seq:
                continue   # a late reply to an earlier probe
            rtt = (t4 - t1) - (t3 - t2)
            samples.append((rtt, ((t2 - t1) + (t3 - t4)) / 2.0))
            break
        if lost >= 20 and not samples:
            break   # peer not answering: don't spend count * timeout on it
        time.sleep(0.0005)
    if not samples:
        print(json.dumps({"peer": peer_ip, "error": "no replies", "lost": lost}))
        return
    samples.sort()
    best = samples[: max(1, len(samples) // 20)]   # lowest-RTT 5%
    offs = sorted(o for _, o in best)
    print(json.dumps({
        "peer": peer_ip,
        "n": len(samples),
        "lost": lost,
        "min_rtt_us": samples[0][0] / 1000.0,
        "median_rtt_us": samples[len(samples) // 2][0] / 1000.0,
        "offset_us": offs[len(offs) // 2] / 1000.0,      # median over the best samples
        "offset_spread_us": (offs[-1] - offs[0]) / 1000.0,
    }))


if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == "serve":
        serve(sys.argv[2], int(sys.argv[3]), float(sys.argv[4]))
    elif mode == "probe":
        probe(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
    else:
        sys.exit("usage: clock_probe.py serve <ip> <port> <secs> | probe <ip> <port> <count>")

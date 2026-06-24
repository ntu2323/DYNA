#!/usr/bin/env bash
# IONIA/VR SharedUDPPort multithread smoke (single replica, no distributed cluster).
#
# Usage (from repo root, after: docker build -f docker/Dockerfile.udp -t ionia-udp .):
#   ./docker/docker-smoke-ionia-mt.sh
#   SMOKE_DURABILITY_THREADS=15 ./docker/docker-smoke-ionia-mt.sh
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export LOW_MEMORY=1
export SMOKE_PROTOCOL=vr
export SMOKE_DURABILITY_THREADS="${SMOKE_DURABILITY_THREADS:-4}"
export IONIA_UDP_IMAGE="${IONIA_UDP_IMAGE:-ionia-udp}"

exec "${REPO_ROOT}/docker/docker-smoke.sh" "$@"

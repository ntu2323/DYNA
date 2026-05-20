"""Shared micro trace for GCP VR/DYNA benchmarks (write-only, cycles for -n)."""

from __future__ import annotations

from typing import List, Tuple

# Insert/Update only — no reads (R) for fair write-path comparison.
WRITE_TRACE_SPEC: List[Tuple[str, str]] = [
    ("I", "user10000000000000000001"),
    ("U", "user20000000000000000002"),
    ("I", "user30000000000000000003"),
    ("U", "user10000000000000000001"),
    ("I", "user20000000000000000002"),
    ("U", "user30000000000000000003"),
    ("I", "user40000000000000000004"),
    ("U", "user40000000000000000004"),
    ("U", "user20000000000000000002"),
    ("I", "user30000000000000000003"),
]

TRACE_MICRO_FILENAME = "trace.micro"


def trace_micro_bytes(spec: List[Tuple[str, str]] | None = None) -> str:
    """mmap trace format: '{op} {24-char-key} ' per op (no newlines)."""
    rows = spec if spec is not None else WRITE_TRACE_SPEC
    out: List[str] = []
    for op, key in rows:
        k = (key + " " * 24)[:24]
        out.append(f"{op} {k} ")
    return "".join(out)

"""Shared micro trace for GCP VR/DYNA benchmarks (write-only, cycles for -n)."""

from __future__ import annotations

from typing import List, Tuple

# Default keyspace size for GCP benches (papers commonly use hundreds–thousands of keys).
DEFAULT_TRACE_NUM_KEYS = 1000

TRACE_MICRO_FILENAME = "trace.micro"


def trace_key_for_index(key_index: int) -> str:
    """24-char key: 'user' + zero-padded index (matches legacy trace layout)."""
    if key_index < 1:
        raise ValueError("key_index must be >= 1")
    return f"user{str(key_index).zfill(20)}"


def build_write_trace_spec(num_keys: int = DEFAULT_TRACE_NUM_KEYS) -> List[Tuple[str, str]]:
    """One Insert/Update op per key; client cycles this list when -n 0."""
    if num_keys < 1:
        raise ValueError("num_keys must be >= 1")
    spec: List[Tuple[str, str]] = []
    for i in range(1, num_keys + 1):
        key = trace_key_for_index(i)
        if len(key) != 24:
            raise ValueError(f"trace key must be 24 chars, got {len(key)}: {key!r}")
        op = "U" if (i % 2) == 0 else "I"
        spec.append((op, key))
    return spec


# Insert/Update only — no reads (R) for fair write-path comparison.
WRITE_TRACE_SPEC: List[Tuple[str, str]] = build_write_trace_spec()


def trace_micro_bytes(spec: List[Tuple[str, str]] | None = None) -> str:
    """mmap trace format: '{op} {24-char-key} ' per op (no newlines)."""
    rows = spec if spec is not None else WRITE_TRACE_SPEC
    out: List[str] = []
    for op, key in rows:
        k = (key + " " * 24)[:24]
        out.append(f"{op} {k} ")
    return "".join(out)

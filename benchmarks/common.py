"""Shared helpers for the Bloomsieve benchmark suite."""

from __future__ import annotations

import platform
import statistics
import sys
import time
from collections.abc import Callable, Sequence
from typing import Any


def percentiles(samples: Sequence[float]) -> dict[str, float]:
    """Return approximate p50/p95/p99 of a sample series (input units preserved)."""
    ordered = sorted(samples)
    n = len(ordered)

    def value(quantile: float) -> float:
        if not ordered:
            return 0.0
        return ordered[min(n - 1, int(quantile * n))]

    return {"p50": value(0.50), "p95": value(0.95), "p99": value(0.99)}


def time_loop(fn: Callable[[int], Any], iters: int) -> tuple[list[float], float]:
    """Run ``fn(i)`` ``iters`` times returning per-call CPU wall times (seconds)."""
    samples: list[float] = []
    for i in range(iters):
        start = time.perf_counter()
        fn(i)
        samples.append(time.perf_counter() - start)
    return samples, sum(samples)


def fmt_ops_per_sec(total_items: int, elapsed_seconds: float) -> str:
    if elapsed_seconds <= 0:
        return "n/a"
    return f"{total_items / elapsed_seconds:,.0f}"


def fmt_latency_us(latency_seconds: float) -> str:
    return f"{latency_seconds * 1_000_000:.1f} µs"


def fmt_bytes(n: int) -> str:
    value = float(n)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if value < 1024 or unit == "GiB":
            return f"{value:,.1f} {unit}"
        value /= 1024
    return f"{value:,.1f} GiB"  # pragma: no cover


def hardware_summary() -> str:
    bits, _ = platform.architecture()
    py = sys.version.split()[0]
    return f"Python {py} ({bits}) on {platform.platform()}, {platform.processor() or 'unknown CPU'}"


def mean(items: Sequence[float]) -> float:
    return statistics.fmean(items) if items else 0.0


def print_report(lines: list[str]) -> None:
    print("\n".join(lines))
    print("-" * 60)

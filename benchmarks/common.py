"""Shared helpers for the Bloomsieve benchmark suite."""

from __future__ import annotations

import argparse
import json
import os
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


def save_json(data: dict[str, Any], path: str | None) -> None:
    """Save benchmark result to JSON."""
    if not path:
        return
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"Saved JSON results to {path}")


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add common arguments used by most benchmark scripts."""
    parser.add_argument(
        "--output",
        type=str,
        help="Path to save JSON results.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=50_000,
        help="Number of iterations for lookups.",
    )
    parser.add_argument(
        "--capacity",
        type=int,
        default=1_000_000,
        help="Capacity of the Bloom filter.",
    )
    parser.add_argument(
        "--error-rate",
        type=float,
        default=0.001,
        help="False positive rate of the Bloom filter.",
    )
    parser.add_argument(
        "--redis-url",
        type=str,
        default=os.environ.get("BLOOMSIEVE_REDIS_URL", "redis://localhost:6379/0"),
        help="Redis URL to use for Redis tests.",
    )


def bloomsieve_version() -> str:
    try:
        import bloomsieve

        return bloomsieve.__version__
    except (ImportError, AttributeError):
        return "unknown"


def make_queries(n_items: int, n_queries: int, negative_ratio: float, seed: int) -> list[str]:
    """Deterministic query set of ``n_queries`` with ``negative_ratio`` absent entries."""
    import random

    rng = random.Random(seed)
    n_negative = int(n_queries * negative_ratio)
    n_positive = n_queries - n_negative
    negatives = [f"absent-{i}" for i in range(n_items, n_items + n_negative)]
    positives = [f"item-{i}" for i in range(n_positive)]
    queries = negatives + positives
    rng.shuffle(queries)
    return queries


class SimLatencyRedis:
    """A wrapper for a Redis client that injects simulated network latency and counts existence queries."""

    def __init__(self, client: Any, rtt_seconds: float) -> None:
        self.client = client
        self.rtt_seconds = rtt_seconds
        self.exists_calls = 0
        self._orig = client.execute_command

    def execute_command(self, *args: Any, **kwargs: Any) -> Any:
        if args and args[0] == "BF.EXISTS":
            self.exists_calls += 1
            if self.rtt_seconds > 0:
                time.sleep(self.rtt_seconds)
        return self._orig(*args, **kwargs)

    def __getattr__(self, item: str) -> Any:
        return getattr(self.client, item)

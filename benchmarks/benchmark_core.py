"""Standalone in-memory Bloom filter micro-benchmark.

Measures add and lookup throughput for a range of filter sizes and
false-positive rates, plus p50/p95/p99 lookup latencies.

Run:
    python benchmarks/benchmark_core.py
"""

from __future__ import annotations

from common import (
    fmt_ops_per_sec,
    hardware_summary,
    percentiles,
    print_report,
    time_loop,
)

from bloomsieve import BloomFilter

CAPACITIES = (10_000, 100_000, 1_000_000)
ERROR_RATES = (0.01, 0.001)
LOOKUP_ITERS = 50_000


def main() -> None:
    report = ["# Core Bloom filter benchmark", hardware_summary(), ""]
    report.append(f"{'capacity':>12} {'error':>7} {'m(MiB)':>8} {'add/s':>10} {'lookup/s':>10} "
                  f"{'p50(us)':>9} {'p95(us)':>9} {'p99(us)':>9}")
    for capacity in CAPACITIES:
        for error_rate in ERROR_RATES:
            bf = BloomFilter(capacity=capacity, error_rate=error_rate)

            add_seconds = time_loop(
                lambda i, _bf=bf: _bf.add(f"item-{i}"), capacity
            )[1]

            # Actually populate the filter, then time lookups over the same items.
            for i in range(capacity):
                bf.add(f"pop-{i}")
            lookup_samples, lookup_seconds = time_loop(
                lambda i, _bf=bf: _bf.__contains__(f"pop-{i}"), LOOKUP_ITERS
            )

            p = percentiles(lookup_samples)
            report.append(
                f"{capacity:>12,} {error_rate:>7} {bf.byte_size / (1024**2):>8.2f} "
                f"{fmt_ops_per_sec(capacity, add_seconds):>10} "
                f"{fmt_ops_per_sec(LOOKUP_ITERS, lookup_seconds):>10} "
                f"{p['p50'] * 1e6:>9.1f} {p['p95'] * 1e6:>9.1f} {p['p99'] * 1e6:>9.1f}"
            )
    print_report(report)
    print("Note: local in-memory/shared-memory timings; add uses one SHA-256 digest per item.")


if __name__ == "__main__":
    main()

"""Filter size benchmark.

Measures actual mmap file size and initialization throughput for large datasets.

Run:
    python benchmarks/benchmark_memory.py --help
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
from typing import Any

from common import (
    add_common_args,
    bloomsieve_version,
    fmt_bytes,
    fmt_ops_per_sec,
    hardware_summary,
    print_report,
    save_json,
    time_loop,
)

from bloomsieve import BloomFilter


def main() -> None:
    parser = argparse.ArgumentParser(description="Memory and filter size benchmark")
    add_common_args(parser)
    parser.add_argument(
        "--capacities",
        type=int,
        nargs="+",
        default=[1_000, 10_000, 100_000, 1_000_000],
        help="List of capacities to test.",
    )
    args = parser.parse_args()

    report = [
        "# Filter Size & Throughput Benchmark",
        hardware_summary(),
        f"Bloomsieve version: {bloomsieve_version()}",
        f"Error rate: {args.error_rate}",
        "",
    ]

    results: dict[str, Any] = {
        "benchmark": "memory_and_throughput",
        "bloomsieve_version": bloomsieve_version(),
        "python_version": sys.version.split()[0],
        "error_rate": args.error_rate,
        "runs": [],
    }

    header = f"{'capacity':>12} {'error':>7} {'mmap size':>12} {'init(ms)':>10} {'insert/s':>12} {'lookup/s':>12}"
    report.append(header)

    workdir = tempfile.mkdtemp(prefix="bloomsieve_bench_mem_")

    for capacity in args.capacities:
        path = os.path.join(workdir, f"filter_{capacity}.bloom")

        start = time.perf_counter()
        bf = BloomFilter(capacity=capacity, error_rate=args.error_rate, filepath=path)
        init_ms = (time.perf_counter() - start) * 1000

        actual_size = os.path.getsize(path)

        _, insert_sec = time_loop(lambda i, bf=bf: bf.add(f"k-{i}"), capacity)

        lookups = min(capacity, args.iterations)
        _, lookup_sec = time_loop(lambda i, bf=bf, cap=capacity: bf.__contains__(f"k-{i % cap}"), lookups)

        insert_throughput = capacity / insert_sec if insert_sec else 0
        lookup_throughput = lookups / lookup_sec if lookup_sec else 0

        report.append(
            f"{capacity:>12,} {args.error_rate:>7} {fmt_bytes(actual_size):>12} {init_ms:>10.2f} "
            f"{fmt_ops_per_sec(capacity, insert_sec):>12} {fmt_ops_per_sec(lookups, lookup_sec):>12}"
        )

        results["runs"].append(
            {
                "capacity": capacity,
                "error_rate": args.error_rate,
                "mmap_file_size_bytes": actual_size,
                "initialization_ms": init_ms,
                "insert_ops_sec": insert_throughput,
                "lookup_ops_sec": lookup_throughput,
            }
        )

        bf.close()

    print_report(report)
    save_json(results, args.output)


if __name__ == "__main__":
    main()

"""False-positive error rates benchmark.

Measures the relationship between configured error rate, observed false-positive rate,
filter size, and how it impacts Redis requests.

Run:
    python benchmarks/benchmark_error_rates.py --help
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from common import (
    add_common_args,
    bloomsieve_version,
    fmt_bytes,
    hardware_summary,
    print_report,
    save_json,
)

from bloomsieve import BloomFilter


def main() -> None:
    parser = argparse.ArgumentParser(description="False-positive rate benchmark")
    add_common_args(parser)
    parser.add_argument(
        "--error-rates",
        type=float,
        nargs="+",
        default=[0.1, 0.01, 0.001, 0.0001],
        help="List of error rates to test.",
    )
    args = parser.parse_args()

    n_items = args.capacity
    n_queries = args.iterations

    report = [
        "# False-positive rate benchmark",
        hardware_summary(),
        f"Bloomsieve version: {bloomsieve_version()}",
        f"Capacity: {n_items:,} | Absent Queries: {n_queries:,}",
        "",
    ]

    results: dict[str, Any] = {
        "benchmark": "error_rates",
        "bloomsieve_version": bloomsieve_version(),
        "python_version": sys.version.split()[0],
        "capacity": n_items,
        "absent_queries": n_queries,
        "runs": [],
    }

    header = f"{'config err':>12} {'obs err':>12} {'fp count':>10} {'filter size':>12}"
    report.append(header)

    for err_rate in args.error_rates:
        bf = BloomFilter(capacity=n_items, error_rate=err_rate)

        # Populate
        for i in range(n_items):
            bf.add(f"item-{i}")

        # Test absent queries
        false_positives = 0
        for i in range(n_queries):
            if f"absent-{i}" in bf:
                false_positives += 1

        obs_rate = false_positives / n_queries

        report.append(f"{err_rate:>12.5f} {obs_rate:>12.5f} {false_positives:>10,} {fmt_bytes(bf.total_size):>12}")

        results["runs"].append(
            {
                "configured_error_rate": err_rate,
                "observed_error_rate": obs_rate,
                "false_positives": false_positives,
                "total_size_bytes": bf.total_size,
                "redis_requests_caused_by_fp": false_positives,
            }
        )

    print_report(report)
    save_json(results, args.output)


if __name__ == "__main__":
    main()

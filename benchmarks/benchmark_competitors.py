"""Competitor benchmark.

Compares Bloomsieve against pybloomer and pybloomfiltermmap3.
Requires them to be installed:
    pip install pybloomer pybloomfiltermmap3

Run:
    python benchmarks/benchmark_competitors.py --help
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
    parser = argparse.ArgumentParser(description="Competitor benchmark")
    add_common_args(parser)
    args = parser.parse_args()

    n_items = args.capacity
    n_queries = args.iterations

    report = [
        "# Competitor Benchmark",
        hardware_summary(),
        f"Capacity: {n_items:,} | Error rate: {args.error_rate} | Queries: {n_queries:,}",
        "",
    ]
    
    results: dict[str, Any] = {
        "benchmark": "competitors",
        "bloomsieve_version": bloomsieve_version(),
        "python_version": sys.version.split()[0],
        "capacity": n_items,
        "error_rate": args.error_rate,
        "runs": {},
    }

    # Helper to run standard benchmark for a given implementation
    def benchmark_impl(name: str, create_fn: Any, add_fn: Any, contains_fn: Any, close_fn: Any, file_path: str) -> None:
        start = time.perf_counter()
        obj = create_fn()
        init_sec = time.perf_counter() - start
        
        _, insert_sec = time_loop(lambda i: add_fn(obj, f"item-{i}"), n_items)
        
        # Test lookups
        _, pos_sec = time_loop(lambda i: contains_fn(obj, f"item-{i % n_items}"), n_queries)
        _, neg_sec = time_loop(lambda i: contains_fn(obj, f"absent-{i}"), n_queries)
        
        # Get file size before close (or after, depending on impl)
        if close_fn:
            close_fn(obj)
            
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0

        insert_ops = n_items / insert_sec if insert_sec else 0
        pos_ops = n_queries / pos_sec if pos_sec else 0
        neg_ops = n_queries / neg_sec if neg_sec else 0

        report.append(f"## {name}")
        report.append(f"Init: {init_sec * 1000:.2f} ms | File Size: {fmt_bytes(file_size)}")
        report.append(f"Insert: {fmt_ops_per_sec(n_items, insert_sec)} ops/s")
        report.append(f"Pos Lookup: {fmt_ops_per_sec(n_queries, pos_sec)} ops/s")
        report.append(f"Neg Lookup: {fmt_ops_per_sec(n_queries, neg_sec)} ops/s")
        report.append("")
        
        results["runs"][name] = {
            "initialization_ms": init_sec * 1000,
            "mmap_file_size_bytes": file_size,
            "insert_ops_sec": insert_ops,
            "positive_lookup_ops_sec": pos_ops,
            "negative_lookup_ops_sec": neg_ops,
        }

    workdir = tempfile.mkdtemp(prefix="bloomsieve_bench_comp_")
    
    # Bloomsieve
    path_bs = os.path.join(workdir, "bloomsieve.bloom")
    benchmark_impl(
        "Bloomsieve",
        lambda: BloomFilter(capacity=n_items, error_rate=args.error_rate, filepath=path_bs),
        lambda obj, item: obj.add(item),
        lambda obj, item: item in obj,
        lambda obj: obj.close(),
        path_bs,
    )
    
    # pybloomer
    try:
        import pybloomer
        path_pb = os.path.join(workdir, "pybloomer.bloom")
        benchmark_impl(
            "pybloomer",
            lambda: pybloomer.BloomFilter(capacity=n_items, error_rate=args.error_rate, filename=path_pb),
            lambda obj, item: obj.add(item),
            lambda obj, item: item in obj,
            lambda obj: obj.close(),
            path_pb,
        )
    except ImportError:
        report.append("## pybloomer\n(Not installed - skipping)\n")
        
    # pybloomfiltermmap3
    try:
        import pybloomfilter
        path_pb3 = os.path.join(workdir, "pybloomfilter3.bloom")
        benchmark_impl(
            "pybloomfiltermmap3",
            lambda: pybloomfilter.BloomFilter(n_items, args.error_rate, path_pb3),
            lambda obj, item: obj.add(item),
            lambda obj, item: item in obj,
            lambda obj: obj.close(),
            path_pb3,
        )
    except ImportError:
        report.append("## pybloomfiltermmap3\n(Not installed - skipping)\n")

    print_report(report)
    save_json(results, args.output)


if __name__ == "__main__":
    main()

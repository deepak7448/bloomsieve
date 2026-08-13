"""Local Bloom-filter microbenchmark.

Benchmarks the local data structure separately (in-memory and mmap).
Measures:
* add
* positive contains
* negative contains
* mmap lookup
* in-memory lookup
* reopen
* initialization

Run:
    python benchmarks/benchmark_local.py --help
"""

from __future__ import annotations

import argparse
import os
import tempfile
import time
from typing import Any

from common import (
    add_common_args,
    bloomsieve_version,
    fmt_bytes,
    fmt_ops_per_sec,
    hardware_summary,
    percentiles,
    print_report,
    save_json,
    time_loop,
)

from bloomsieve import BloomFilter


def main() -> None:
    parser = argparse.ArgumentParser(description="Local Bloom-filter microbenchmark")
    add_common_args(parser)
    args = parser.parse_args()

    report = [
        "# Local Bloom filter microbenchmark",
        hardware_summary(),
        f"Bloomsieve version: {bloomsieve_version()}",
        f"Capacity: {args.capacity:,} | Error rate: {args.error_rate} | Lookups: {args.iterations:,}",
        "",
    ]

    results: dict[str, Any] = {
        "benchmark": "local",
        "bloomsieve_version": bloomsieve_version(),
        "python_version": platform_python_version(),
        "capacity": args.capacity,
        "error_rate": args.error_rate,
        "lookups": args.iterations,
        "modes": {},
    }

    # Run Memory benchmark
    report.append("## In-Memory Filter")
    start = time.perf_counter()
    mem_bf = BloomFilter(capacity=args.capacity, error_rate=args.error_rate)
    mem_init_time = time.perf_counter() - start
    report.append(f"Initialization: {mem_init_time * 1000:.2f} ms")
    
    _, insert_seconds = time_loop(lambda i: mem_bf.add(f"key-{i}"), args.capacity)
    report.append(f"Insert: {fmt_ops_per_sec(args.capacity, insert_seconds)} ops/s ({args.capacity:,} items)")

    pos_samples, pos_sec = time_loop(lambda i: mem_bf.__contains__(f"key-{i % args.capacity}"), args.iterations)
    neg_samples, neg_sec = time_loop(lambda i: mem_bf.__contains__(f"absent-{i}"), args.iterations)
    
    p_pos = percentiles(pos_samples)
    p_neg = percentiles(neg_samples)
    report.append(
        "Lookup (positive, present): "
        f"{fmt_ops_per_sec(args.iterations, pos_sec)} ops/s, "
        f"p50={p_pos['p50'] * 1e6:.1f}us p95={p_pos['p95'] * 1e6:.1f}us p99={p_pos['p99'] * 1e6:.1f}us"
    )
    report.append(
        "Lookup (negative, absent):  "
        f"{fmt_ops_per_sec(args.iterations, neg_sec)} ops/s, "
        f"p50={p_neg['p50'] * 1e6:.1f}us p95={p_neg['p95'] * 1e6:.1f}us p99={p_neg['p99'] * 1e6:.1f}us"
    )

    results["modes"]["memory"] = {
        "initialization_ms": mem_init_time * 1000,
        "insert_ops_sec": args.capacity / insert_seconds if insert_seconds > 0 else 0,
        "positive_lookup": {"ops_sec": args.iterations / pos_sec, "p50_us": p_pos['p50'] * 1e6, "p95_us": p_pos['p95'] * 1e6, "p99_us": p_pos['p99'] * 1e6},
        "negative_lookup": {"ops_sec": args.iterations / neg_sec, "p50_us": p_neg['p50'] * 1e6, "p95_us": p_neg['p95'] * 1e6, "p99_us": p_neg['p99'] * 1e6},
    }

    # Run mmap benchmark
    report.append("\n## Mmap Filter")
    workdir = tempfile.mkdtemp(prefix="bloomsieve_bench_local_")
    path = os.path.join(workdir, "filter.bloom")
    
    start = time.perf_counter()
    mmap_bf = BloomFilter(capacity=args.capacity, error_rate=args.error_rate, filepath=path)
    mmap_init_time = time.perf_counter() - start
    report.append(f"Initialization: {mmap_init_time * 1000:.2f} ms")
    report.append(f"Backing file: {path} (size: {fmt_bytes(mmap_bf.total_size)})")
    
    _, insert_seconds = time_loop(lambda i: mmap_bf.add(f"key-{i}"), args.capacity)
    report.append(f"Insert: {fmt_ops_per_sec(args.capacity, insert_seconds)} ops/s ({args.capacity:,} items)")

    pos_samples, pos_sec = time_loop(lambda i: mmap_bf.__contains__(f"key-{i % args.capacity}"), args.iterations)
    neg_samples, neg_sec = time_loop(lambda i: mmap_bf.__contains__(f"absent-{i}"), args.iterations)
    
    p_pos = percentiles(pos_samples)
    p_neg = percentiles(neg_samples)
    report.append(
        "Lookup (positive, present): "
        f"{fmt_ops_per_sec(args.iterations, pos_sec)} ops/s, "
        f"p50={p_pos['p50'] * 1e6:.1f}us p95={p_pos['p95'] * 1e6:.1f}us p99={p_pos['p99'] * 1e6:.1f}us"
    )
    report.append(
        "Lookup (negative, absent):  "
        f"{fmt_ops_per_sec(args.iterations, neg_sec)} ops/s, "
        f"p50={p_neg['p50'] * 1e6:.1f}us p95={p_neg['p95'] * 1e6:.1f}us p99={p_neg['p99'] * 1e6:.1f}us"
    )
    mmap_bf.close()

    # Reopen
    start = time.perf_counter()
    reopened = BloomFilter(capacity=args.capacity, error_rate=args.error_rate, filepath=path)
    reopen_time = time.perf_counter() - start
    report.append(f"Reopen cost (mmap): {reopen_time * 1000:.2f} ms")
    reopened.close()

    results["modes"]["mmap"] = {
        "initialization_ms": mmap_init_time * 1000,
        "reopen_ms": reopen_time * 1000,
        "file_size_bytes": os.path.getsize(path),
        "insert_ops_sec": args.capacity / insert_seconds if insert_seconds > 0 else 0,
        "positive_lookup": {"ops_sec": args.iterations / pos_sec, "p50_us": p_pos['p50'] * 1e6, "p95_us": p_pos['p95'] * 1e6, "p99_us": p_pos['p99'] * 1e6},
        "negative_lookup": {"ops_sec": args.iterations / neg_sec, "p50_us": p_neg['p50'] * 1e6, "p95_us": p_neg['p95'] * 1e6, "p99_us": p_neg['p99'] * 1e6},
    }

    print_report(report)
    save_json(results, args.output)


def platform_python_version() -> str:
    import sys
    return sys.version.split()[0]


if __name__ == "__main__":
    main()

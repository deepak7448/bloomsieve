"""Memory-mapped (mmap) Bloom filter benchmark.

Measures insert throughput, lookup latency (positive and negative queries),
file size and reopen cost for a persistent mmap filter.

Run:
    python benchmarks/benchmark_mmap.py
"""

from __future__ import annotations

import os
import tempfile
import time

from common import (
    fmt_bytes,
    fmt_ops_per_sec,
    hardware_summary,
    percentiles,
    print_report,
    time_loop,
)

from bloomsieve import BloomFilter

CAPACITY = 1_000_000
ERROR_RATE = 0.001
LOOKUP_ITERS = 50_000


def main() -> None:
    workdir = tempfile.mkdtemp(prefix="bloomsieve_mmap_bench_")
    path = os.path.join(workdir, "filter.bloom")
    report = [
        "# mmap Bloom filter benchmark",
        hardware_summary(),
        f"capacity={CAPACITY:,}, error_rate={ERROR_RATE}",
        "",
    ]

    bf = BloomFilter(capacity=CAPACITY, error_rate=ERROR_RATE, filepath=path)
    report.append(f"backing file: {path}")
    report.append(f"bit array: {fmt_bytes(bf.byte_size)}  file size: {fmt_bytes(bf.total_size)}")

    _, insert_seconds = time_loop(lambda i: bf.add(f"key-{i}"), CAPACITY)
    report.append(f"insert: {fmt_ops_per_sec(CAPACITY, insert_seconds)} ops/s ({CAPACITY:,} items)")

    positive_samples, positive_seconds = time_loop(
        lambda i: bf.__contains__(f"key-{i % CAPACITY}"), LOOKUP_ITERS
    )
    negative_samples, negative_seconds = time_loop(
        lambda i: bf.__contains__(f"absent-{i}"), LOOKUP_ITERS
    )

    p_pos = percentiles(positive_samples)
    p_neg = percentiles(negative_samples)
    report.append(
        "lookup (positive, present): "
        f"{fmt_ops_per_sec(LOOKUP_ITERS, positive_seconds)} ops/s, "
        f"p50={p_pos['p50'] * 1e6:.1f}us p95={p_pos['p95'] * 1e6:.1f}us p99={p_pos['p99'] * 1e6:.1f}us"
    )
    report.append(
        "lookup (negative, absent): "
        f"{fmt_ops_per_sec(LOOKUP_ITERS, negative_seconds)} ops/s, "
        f"p50={p_neg['p50'] * 1e6:.1f}us p95={p_neg['p95'] * 1e6:.1f}us p99={p_neg['p99'] * 1e6:.1f}us"
    )

    bf.close()

    # Reopen cost (fresh mapping of the same file).
    start = time.perf_counter()
    reopened = BloomFilter(capacity=CAPACITY, error_rate=ERROR_RATE, filepath=path)
    reopen_seconds = time.perf_counter() - start
    report.append(f"reopen (mmap): {reopen_seconds * 1000:.1f} ms")
    assert "key-0" in reopened
    reopened.close()

    report.append(f"final file size: {fmt_bytes(os.path.getsize(path))}")
    print_report(report)


if __name__ == "__main__":
    main()

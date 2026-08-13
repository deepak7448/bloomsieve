"""Redis workload benchmark: RedisBloom alone vs. Bloomsieve pre-filter.

Measures the number of `BF.EXISTS` network requests avoided for negative-heavy workloads.
No simulated network latency; measures raw performance against the provided Redis URL.

Run:
    python benchmarks/benchmark_redis.py --help
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
from typing import Any, Callable

import redis
from common import (
    SimLatencyRedis,
    add_common_args,
    bloomsieve_version,
    fmt_bytes,
    hardware_summary,
    make_queries,
    percentiles,
    print_report,
    save_json,
)

from bloomsieve import BloomFilterService


def run_workload(
    call_exists: Callable[[str], bool],
    queries: list[str],
    counter: SimLatencyRedis,
) -> tuple[list[float], int]:
    counter.exists_calls = 0
    latencies: list[float] = []
    for item in queries:
        start = time.perf_counter_ns()
        call_exists(item)
        latencies.append((time.perf_counter_ns() - start) / 1e9)
    return latencies, counter.exists_calls


def main() -> None:
    parser = argparse.ArgumentParser(description="Redis workload benchmark")
    add_common_args(parser)
    parser.add_argument(
        "--negative-ratios",
        type=float,
        nargs="+",
        default=[0.50, 0.75, 0.90, 0.95, 0.99],
        help="List of negative query ratios to test.",
    )
    args = parser.parse_args()

    client = redis.from_url(args.redis_url, socket_timeout=5)
    try:
        client.ping()
    except Exception as e:
        print(f"Failed to connect to Redis at {args.redis_url}: {e}")
        return

    counting = SimLatencyRedis(client, rtt_seconds=0.0)

    n_items = args.capacity
    n_queries = n_items

    key = f"bloomsieve:bench:{os.getpid()}"
    workdir = tempfile.mkdtemp(prefix="bloomsieve_bench_redis_")
    svc = BloomFilterService(
        redis_client=counting,
        capacity=n_items,
        error_rate=args.error_rate,
        use_mmap=True,
        mmap_dir=workdir,
    )

    report = [
        "# Redis workload benchmark",
        hardware_summary(),
        f"Bloomsieve version: {bloomsieve_version()}",
        f"Items in filter: {n_items:,} | Queries: {n_queries:,} | Error rate: {args.error_rate}",
        f"Redis: {args.redis_url}",
        "",
    ]

    results: dict[str, Any] = {
        "benchmark": "redis_membership",
        "bloomsieve_version": bloomsieve_version(),
        "python_version": sys.version.split()[0],
        "redis_url": args.redis_url,
        "capacity": n_items,
        "queries": n_queries,
        "error_rate": args.error_rate,
        "runs": [],
    }


    try:
        svc.create_filter(key, n_items, args.error_rate)
        # Fast population via pipeline for setup
        pipe = client.pipeline()
        for i in range(n_items):
            pipe.execute_command("BF.ADD", key, f"item-{i}")
            svc.add(key, f"item-{i}") # Also add to local mmap
            if i % 10000 == 0:
                pipe.execute()
        pipe.execute()

        local_file_bytes = os.path.getsize(svc._mmap_path(key))
        redis_bytes = int(client.execute_command("MEMORY", "USAGE", key) or 0)
        report.append(f"RedisBloom memory: {fmt_bytes(redis_bytes)}  Local mmap file: {fmt_bytes(local_file_bytes)}")
        report.append("")

        header = (
            f"{'neg ratio':>10} {'path':>15} {'requests':>10} {'avoided':>10} {'avoid %':>9} "
            f"{'time/s':>9} {'p50(us)':>9} {'p95(us)':>9} {'p99(us)':>9}"
        )
        report.append(header)

        for ratio in args.negative_ratios:
            queries = make_queries(n_items, n_queries, ratio, seed=n_items + int(ratio * 100))

            latent_b, req_b = run_workload(
                lambda item: bool(counting.execute_command("BF.EXISTS", key, item)), queries, counting
            )
            latent_s, req_s = run_workload(
                lambda item: svc.exists(key, item), queries, counting
            )

            avoided = req_b - req_s
            avoid_pct = avoided / req_b if req_b else 0

            p_b = percentiles(latent_b)
            p_s = percentiles(latent_s)

            report.append(
                f"{ratio:>10.0%} {'baseline':>15} {req_b:>10,} {'-':>10} {'-':>9} "
                f"{sum(latent_b):>9.2f} {p_b['p50']*1e6:>9.1f} {p_b['p95']*1e6:>9.1f} {p_b['p99']*1e6:>9.1f}"
            )
            report.append(
                f"{ratio:>10.0%} {'bloomsieve':>15} {req_s:>10,} {avoided:>10,} {avoid_pct:>8.1%} "
                f"{sum(latent_s):>9.2f} {p_s['p50']*1e6:>9.1f} {p_s['p95']*1e6:>9.1f} {p_s['p99']*1e6:>9.1f}"
            )

            results["runs"].append({
                "negative_ratio": ratio,
                "total_queries": n_queries,
                "baseline_redis_requests": req_b,
                "bloomsieve_redis_requests": req_s,
                "requests_avoided": avoided,
                "avoidance_rate": avoid_pct,
                "baseline_p50_us": p_b["p50"] * 1e6,
                "bloomsieve_p50_us": p_s["p50"] * 1e6,
            })

    finally:
        try:
            client.delete(key)
        except Exception:
            pass
        for name in list(BloomFilterService._mmaps):
            svc._close_mmap(name)

    print_report(report)
    save_json(results, args.output)


if __name__ == "__main__":
    main()

"""End-to-End benchmark for Bloomsieve.

This benchmark simulates different network latencies (RTT) to Redis, demonstrating
the increasing value of the local Bloom filter as network latency increases.

Run:
    python benchmarks/benchmark_end_to_end.py --help
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
    parser = argparse.ArgumentParser(description="End-to-end benchmark with network latency simulation")
    add_common_args(parser)
    parser.add_argument(
        "--negative-ratio",
        type=float,
        default=0.90,
        help="Negative query ratio (default: 0.90).",
    )
    parser.add_argument(
        "--rtts-ms",
        type=float,
        nargs="+",
        default=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0],
        help="List of simulated network RTTs in milliseconds.",
    )
    # Default to smaller queries for end-to-end so it doesn't take forever with simulated latency
    parser.set_defaults(capacity=10_000)

    args = parser.parse_args()

    client = redis.from_url(args.redis_url, socket_timeout=5)
    try:
        client.ping()
    except Exception as e:
        print(f"Failed to connect to Redis at {args.redis_url}: {e}")
        return

    n_items = args.capacity
    n_queries = n_items

    report = [
        "# End-to-end latency benchmark",
        hardware_summary(),
        f"Bloomsieve version: {bloomsieve_version()}",
        f"Items in filter: {n_items:,} | Queries: {n_queries:,}",
        f"Negative ratio: {args.negative_ratio:.0%}",
        "Simulated Latency: Yes",
        "",
    ]

    results: dict[str, Any] = {
        "benchmark": "end_to_end_latency",
        "bloomsieve_version": bloomsieve_version(),
        "python_version": sys.version.split()[0],
        "negative_ratio": args.negative_ratio,
        "queries": n_queries,
        "runs": [],
    }

    key = f"bloomsieve:bench_e2e:{os.getpid()}"
    workdir = tempfile.mkdtemp(prefix="bloomsieve_bench_e2e_")

    counting = SimLatencyRedis(client, rtt_seconds=0.0)
    svc = BloomFilterService(
        redis_client=counting,
        capacity=n_items,
        error_rate=args.error_rate,
        use_mmap=True,
        mmap_dir=workdir,
    )

    try:
        svc.create_filter(key, n_items, args.error_rate)
        pipe = client.pipeline()
        for i in range(n_items):
            pipe.execute_command("BF.ADD", key, f"item-{i}")
            svc.add(key, f"item-{i}")
            if i % 10000 == 0:
                pipe.execute()
        pipe.execute()

        header = (
            f"{'rtt(ms)':>8} {'path':>15} {'requests':>10} {'avoided':>10} {'avoid %':>9} "
            f"{'time/s':>9} {'p50(ms)':>9} {'p95(ms)':>9} {'p99(ms)':>9}"
        )
        report.append(header)

        queries = make_queries(n_items, n_queries, args.negative_ratio, seed=n_items)

        for rtt_ms in args.rtts_ms:
            counting.rtt_seconds = rtt_ms / 1000.0

            # Baseline
            latent_b, req_b = run_workload(
                lambda item: bool(counting.execute_command("BF.EXISTS", key, item)), queries, counting
            )
            # Bloomsieve
            latent_s, req_s = run_workload(lambda item: svc.exists(key, item), queries, counting)

            avoided = req_b - req_s
            avoid_pct = avoided / req_b if req_b else 0

            p_b = percentiles(latent_b)
            p_s = percentiles(latent_s)

            report.append(
                f"{rtt_ms:>8.1f} {'baseline':>15} {req_b:>10,} {'-':>10} {'-':>9} "
                f"{sum(latent_b):>9.2f} {p_b['p50'] * 1e3:>9.2f} {p_b['p95'] * 1e3:>9.2f} {p_b['p99'] * 1e3:>9.2f}"
            )
            report.append(
                f"{rtt_ms:>8.1f} {'bloomsieve':>15} {req_s:>10,} {avoided:>10,} {avoid_pct:>8.1%} "
                f"{sum(latent_s):>9.2f} {p_s['p50'] * 1e3:>9.2f} {p_s['p95'] * 1e3:>9.2f} {p_s['p99'] * 1e3:>9.2f}"
            )

            results["runs"].append(
                {
                    "simulated_rtt_ms": rtt_ms,
                    "total_queries": n_queries,
                    "baseline_redis_requests": req_b,
                    "bloomsieve_redis_requests": req_s,
                    "requests_avoided": avoided,
                    "avoidance_rate": avoid_pct,
                    "baseline_p50_ms": p_b["p50"] * 1e3,
                    "bloomsieve_p50_ms": p_s["p50"] * 1e3,
                }
            )

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

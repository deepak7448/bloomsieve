"""A/B benchmark: RedisBloom alone vs. a Bloomsieve local pre-filter in front of it.

This measures the number of ``BF.EXISTS`` network requests avoided for
negative-heavy workloads, along with per-request latency.  It requires a Redis
server with the RedisBloom module, e.g.:

    docker run -d --name bloomsieve-rebloom -p 6379:6379 redislabs/rebloom:latest

Environment:
    BLOOMSIEVE_REDIS_URL    Redis URL (default: redis://localhost:6379/0)

Run:
    python benchmarks/benchmark_redis.py
"""

from __future__ import annotations

import os
import random
import tempfile
import time
from typing import Any, Callable

import redis
from common import fmt_bytes, hardware_summary, percentiles, print_report

from bloomsieve import BloomFilterService

REDIS_URL = os.environ.get("BLOOMSIEVE_REDIS_URL", "redis://localhost:6379/0")
ITEMS = int(os.environ.get("BLOOMSIEVE_ITEMS", "20_000"))
RATIOS = (0.50, 0.75, 0.90, 0.99)


class CountingRedis:
    """Wraps a redis client and counts BF.EXISTS calls."""

    def __init__(self, client: redis.Redis) -> None:
        self.client = client
        self.exists_calls = 0
        self._orig = client.execute_command

    def execute_command(self, *args: Any, **kwargs: Any) -> Any:
        if args and args[0] == "BF.EXISTS":
            self.exists_calls += 1
        return self._orig(*args, **kwargs)

    def __getattr__(self, item: str) -> Any:
        return getattr(self.client, item)


def make_queries(n_items: int, n_queries: int, negative_ratio: float, seed: int) -> list[str]:
    """Deterministic query set of ``n_queries`` with ``negative_ratio`` absent entries."""
    rng = random.Random(seed)
    n_negative = int(n_queries * negative_ratio)
    n_positive = n_queries - n_negative
    negatives = [f"absent-{i}" for i in range(n_items, n_items + n_negative)]
    positives = [f"item-{i}" for i in range(n_positive)]
    queries = negatives + positives
    rng.shuffle(queries)
    return queries


def run(
    call_exists: Callable[[str], bool],
    queries: list[str],
    counter: CountingRedis,
) -> tuple[list[float], int]:
    counter.exists_calls = 0
    latencies: list[float] = []
    for item in queries:
        start = time.perf_counter_ns()
        call_exists(item)
        latencies.append((time.perf_counter_ns() - start) / 1e9)
    return latencies, counter.exists_calls


def main() -> None:
    client = redis.from_url(REDIS_URL, socket_timeout=5)
    client.ping()
    counting = CountingRedis(client)

    n_items = ITEMS
    n_queries = n_items  # one query per item, split across positive/negative

    key = f"bloomsieve:bench:{os.getpid()}"
    workdir = tempfile.mkdtemp(prefix="bloomsieve_bench_")
    svc = BloomFilterService(
        redis_client=counting,
        capacity=n_items,
        error_rate=0.001,
        use_mmap=True,
        mmap_dir=workdir,
    )

    report = [
        "# Redis workload benchmark: RedisBloom alone vs. Bloomsieve pre-filter",
        hardware_summary(),
        f"items in filter: {n_items:,}   queries per workload: {n_queries:,}",
        f"Redis: {REDIS_URL}",
        "",
    ]

    try:
        svc.create_filter(key, n_items, 0.001)
        for i in range(n_items):
            svc.add(key, f"item-{i}")
        local_file_bytes = os.path.getsize(svc._mmap_path(key))
        redis_bytes = int(client.execute_command("MEMORY", "USAGE", key) or 0)
        report.append(f"RedisBloom memory: {fmt_bytes(redis_bytes)}  "
                      f"local mmap file: {fmt_bytes(local_file_bytes)}")
        report.append("")

        header = [
            f"{'neg ratio':>10} {'path':>42} {'requests':>10} {'avoided':>10} {'time/s':>9} "
            f"{'per req':>9} {'p50':>9} {'p95':>9} {'p99':>9}"
        ]
        report.append(header[0])

        for ratio in RATIOS:
            queries = make_queries(n_items, n_queries, ratio, seed=n_items + int(ratio * 100))

            # Baseline: every query is a Redis request.
            latent_b, requests_b = run(
                lambda item: bool(counting.execute_command("BF.EXISTS", key, item)),
                queries,
                counting,
            )
            baseline_seconds = sum(latent_b)

            # Bloomsieve: local definitely-negative answers skip Redis entirely.
            latent_s, requests_s = run(lambda item: svc.exists(key, item), queries, counting)
            bloomsieve_seconds = sum(latent_s)

            avoided = requests_b - requests_s
            p_b = percentiles(latent_b)
            p_s = percentiles(latent_s)

            for label, _latencies, requests, seconds, p in (
                ("baseline", latent_b, requests_b, baseline_seconds, p_b),
                ("bloomsieve", latent_s, requests_s, bloomsieve_seconds, p_s),
            ):
                report.append(
                    f"{ratio:>10.0%} {label:>42} {requests:>10,} {avoided if label == 'bloomsieve' else '-':>10} "
                    f"{seconds:>9.2f} {(seconds / len(queries)) * 1e6:>9.1f}us "
                    f"{p['p50'] * 1e6:>9.1f} {p['p95'] * 1e6:>9.1f} {p['p99'] * 1e6:>9.1f}"
                )
            report.append(
                f"{'':>10} {'-> requests avoided by local negatives':>42} "
                f"{avoided:>10,} ({avoided / requests_b:.0%})"
            )
    finally:
        try:
            client.delete(key)
        except Exception:
            pass
        for name in list(BloomFilterService._mmaps):
            svc._close_mmap(name)

    print_report(report)
    print("Interpretation: 'avoided' counts BF.EXISTS round-trips the local filter removed.")
    print("Latency here includes localhost overhead; remote deployments amplify the benefit.")


if __name__ == "__main__":
    main()

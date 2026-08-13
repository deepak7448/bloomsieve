# Bloomsieve Benchmarks

This document describes the reproducible benchmark suite for Bloomsieve.

All results depend heavily on hardware, network configuration, and the specific workload. We strongly encourage running the benchmarks in your own environment rather than relying on abstract numbers.

## What we measure

The benchmark suite measures both the local performance of the mmap Bloom filter and its system-level impact when used in front of RedisBloom.

- **Local Performance**: Initialization time, file size, insert throughput, and lookup throughput for both positive (present) and negative (absent) queries.
- **System Impact**: The percentage of remote Redis `BF.EXISTS` requests avoided by the local filter, and how that impacts end-to-end latency under different network RTTs.
- **Accuracy**: The actual observed false-positive rate compared to the configured theoretical rate.

## Why Redis request reduction matters

Every remote membership check incurs network round-trip latency. For negative-heavy workloads (where most checked items do not exist), directly querying Redis means paying this network cost for every single miss. 

Bloomsieve uses a local mmap Bloom filter. Because Bloom filters guarantee **no false negatives**, if the local filter says an item is absent, it is definitively absent. The request can be answered locally in microseconds, skipping the network entirely. Only possible positives are sent to Redis.

## Test environment

When running these benchmarks, you should document your environment. The suite provides a `hardware_summary()` helper that captures:
- Python version
- Operating System
- CPU architecture and model
- Bloomsieve version
- Configured parameters

## Workload

We test using deterministic query sets consisting of an exact ratio of negative (absent) to positive (present) queries. Typical benchmarks run 10,000 to 1,000,000 queries to ensure statistical significance.

## Baseline

The **baseline** for our system benchmarks is a direct query to RedisBloom (`BF.EXISTS`) for every item in the workload.

## Bloomsieve

The **Bloomsieve** path routes the same queries through the `BloomFilterService`. It first checks the local mmap filter. Only if the local filter returns `True` (possible positive) does it fall back to querying RedisBloom.

## Negative-query results

Run `python benchmarks/benchmark_redis.py` to see how the negative-query ratio impacts the number of avoided Redis requests.

*(Run the benchmark script in your environment to generate these numbers. They scale almost perfectly with the negative query ratio minus the small false-positive rate.)*

## Redis RTT results

Run `python benchmarks/benchmark_end_to_end.py` to simulate varying network latencies (from 0.1ms up to 10ms). As latency increases, the throughput and latency benefits of avoiding remote requests become increasingly dominant.

## False-positive results

Run `python benchmarks/benchmark_error_rates.py` to verify accuracy.

A Bloom filter will have some false positives. This benchmark verifies that the observed rate matches the configured rate (e.g., 0.001) and shows how those false positives result in unnecessary (but safe) Redis requests.

## Memory/storage results

Run `python benchmarks/benchmark_memory.py` to see the actual size of the memory-mapped file for different capacities. The size scales linearly with capacity and logarithmically with the error rate.

## Local Bloom-filter results

Run `python benchmarks/benchmark_local.py` for raw throughput numbers on your CPU/storage. Lookups are typically on the order of hundreds of thousands of operations per second with single-digit microsecond latencies.

## pybloomer comparison

Run `python benchmarks/benchmark_competitors.py`.
*(Requires `pip install pybloomer`)*

We compare Bloomsieve's local mmap filter initialization, insert, and lookup speeds directly against `pybloomer` using the same error rate and capacity.

## pybloomfiltermmap3 comparison

Run `python benchmarks/benchmark_competitors.py`.
*(Requires `pip install pybloomfiltermmap3`)*

We compare Bloomsieve against `pybloomfiltermmap3` for completeness.

## Limitations

- Local throughput benchmarks are highly dependent on your CPU cache, RAM speed, and storage performance (for initial mmap faults).
- The network simulation in `benchmark_end_to_end.py` adds artificial sleep latency to the Python client, which models the latency penalty but doesn't fully capture TCP/IP stack overhead or Redis server concurrency limits.
- The A/B benchmarks do not model the background cost of rebuilding/rotating the filter on a live system.

## Reproducing the benchmarks

1. Ensure Redis with RedisBloom is running (for system benchmarks).
2. Install Bloomsieve from source.
3. Run the scripts in the `benchmarks/` directory:

```bash
python benchmarks/benchmark_local.py
python benchmarks/benchmark_memory.py
python benchmarks/benchmark_error_rates.py
BLOOMSIEVE_REDIS_URL=redis://localhost:6379/0 python benchmarks/benchmark_redis.py
BLOOMSIEVE_REDIS_URL=redis://localhost:6379/0 python benchmarks/benchmark_end_to_end.py
```
# Bloomsieve Benchmarks

This directory contains the reproducible benchmark suite for Bloomsieve. The primary goal of these benchmarks is to demonstrate Bloomsieve's core value proposition: reducing unnecessary remote Redis membership checks for negative-heavy workloads.

## Structure

* `benchmark_local.py`: Microbenchmark for the local Bloom filter data structure (in-memory and mmap).
* `benchmark_redis.py`: End-to-end workload simulation measuring Redis requests avoided for various negative query ratios.
* `benchmark_end_to_end.py`: Similar to `benchmark_redis.py`, but simulates varying network latencies (RTT) to demonstrate how remote latency impacts overall performance.
* `benchmark_competitors.py`: Compares the local mmap filter performance and file size against `pybloomer` and `pybloomfiltermmap3`.
* `benchmark_memory.py`: Measures memory usage (mmap file size), initialization time, and throughput across varying capacities.
* `benchmark_error_rates.py`: Validates that the configured error rate matches observed false-positive rates and measures their impact.

## Running

All benchmarks support standard arguments such as `--capacity`, `--error-rate`, `--iterations`, and `--output` for JSON reporting.

To see all available options for a specific benchmark, run:

```bash
python benchmarks/benchmark_local.py --help
```

To run a basic suite, you can do:

```bash
python benchmarks/benchmark_local.py
python benchmarks/benchmark_memory.py
python benchmarks/benchmark_error_rates.py
python benchmarks/benchmark_redis.py
python benchmarks/benchmark_end_to_end.py
```

*Note: `benchmark_redis.py` and `benchmark_end_to_end.py` require a Redis server with the RedisBloom module running.*

To run the competitor benchmarks, you must first install the competitor libraries:

```bash
pip install pybloomer pybloomfiltermmap3
python benchmarks/benchmark_competitors.py
```

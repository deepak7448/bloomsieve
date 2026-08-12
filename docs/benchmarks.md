# Benchmarks

The benchmark suite measures the actual value proposition of Bloomsieve:
a local pre-filter that avoids unnecessary Redis membership requests, plus raw
throughput/latency numbers for the standalone filter.

All benchmarks are real measurements produced by the scripts in `benchmarks/`.
They are **not** marketing numbers — they were generated on the machine described
below, and the same commands can be re-run to reproduce them.

## How to run

Only the standalone core and mmap benchmarks require no Redis. The Redis workload
benchmark needs a Redis server with the RedisBloom module.

```bash
# standalone in-memory filter
python benchmarks/benchmark_core.py

# persistent mmap filter
python benchmarks/benchmark_mmap.py

# Redis workload A/B comparison
# 1) start Redis with RedisBloom if needed:
#    docker run -d --name rebloom -p 6379:6379 redislabs/rebloom:latest
# 2) run the benchmark
BLOOMSIEVE_REDIS_URL=redis://localhost:6379/0 python benchmarks/benchmark_redis.py
```

The Redis benchmark can be tuned via environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `BLOOMSIEVE_REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `BLOOMSIEVE_ITEMS` | `20000` | Number of items inserted into the filter and queried |

## Environment (when the numbers below were captured)

- Date: 2026-08-12
- CPU: Intel(R) Core(TM) i7-14650HX (16 cores / 24 threads)
- Memory: 16 GiB
- OS: Linux (kernel 7.0.0-28-generic), x86_64
- Python: 3.9.25 (64-bit)
- Redis: 8.6.2 with the RedisBloom module (`BF.*` commands), connected over
  localhost (loopback)
- Bloomsieve: 0.2.0 (this commit)

## Benchmark A — standalone core filter

Method: each row times `capacity` inserts into a fresh in-memory filter, then times
50,000 lookups of already-inserted items. `m` is the bit-array size.

```
    capacity   error   m(MiB)      add/s   lookup/s   p50(us)   p95(us)   p99(us)
      10,000    0.01     0.01    220,337    306,818       3.3       3.8       4.7
      10,000   0.001     0.02    202,211    273,889       3.5       4.3       5.0
     100,000    0.01     0.11    254,040    275,837       3.5       4.3       5.2
     100,000   0.001     0.17    200,311    236,722       4.2       4.4       5.5
   1,000,000    0.01     1.14    250,633    251,067       3.6       7.3       9.1
   1,000,000   0.001     1.71    201,369    226,788       4.3       5.2       6.5
```

## Benchmark B — persistent mmap filter

Method: 1,000,000 inserts into an mmap-backed filter; 50,000 lookups of present
("positive") and absent ("negative") items; reopen measures remapping the file.

```
backing file: /tmp/bloomsieve_mmap_bench_<tmp>/filter.bloom
bit array: 1.7 MiB
insert: 189,688 ops/s (1,000,000 items)
lookup (positive, present): 216,031 ops/s, p50=4.5us p95=4.9us p99=6.9us
lookup (negative, absent):  295,576 ops/s, p50=3.2us p95=4.0us p99=5.1us
reopen (mmap): 0.1 ms
```

## Benchmark C — Redis workload A/B

Method: 20,000 items are inserted into RedisBloom (and, for the Bloomsieve path,
into the local mmap mirror). For each negative ratio a fresh deterministic query
set of 20,000 entries is generated with that share of absent items, then executed
twice:

1. **baseline** — every query issues `BF.EXISTS`.
2. **bloomsieve** — local definitely-negative answers return locally; only
   possible positives issue `BF.EXISTS`.

Requests are counted via a wrapper around `execute_command`; latencies are
per-query wall time (ns). Results:

```
items in filter: 20,000   queries per workload: 20,000
RedisBloom memory: 38.8 KiB  local mmap file: 35.1 KiB

 neg ratio            path      requests    avoided    time/s    per req       p50      p95      p99
       50%         baseline        20,000          -      0.79     39.3us     29.8     86.3    169.5
       50%       bloomsieve        10,011       9989      0.66     33.0us     34.6    100.2    217.5
       75%         baseline        20,000          -      0.64     32.1us     28.9     41.2    109.7
       75%       bloomsieve         5,013      14987      0.29     14.7us      4.3     42.7     97.7
       90%         baseline        20,000          -      0.66     33.0us     30.1     45.0     84.0
       90%       bloomsieve         2,014      17986      0.17      8.3us      3.8     38.4     45.1
       99%         baseline        20,000          -      0.74     36.8us     29.6     74.6    153.3
       99%       bloomsieve           216      19784      0.09      4.5us      3.6      4.8     37.9
```

Interpretation:

- The number of Redis requests removed tracks the negative ratio almost exactly
  (workload-99%: 19,784 of 20,000 requests avoided, leaving 216).
- These numbers were measured over **localhost**, where each `BF.EXISTS` already
  costs ~30–40 µs. On a remote Redis deployment the removed round-trips dominate
  the savings and the end-to-end latency improvement is larger.
- The surviving requests are the items that are actually present plus a small
  number of local false positives at the configured 0.1 % error rate.

Caveats:

- Latency percentiles are noisy on a shared laptop; treat the request-avoidance
  counts as the primary, stable result.
- Do not extrapolate these numbers to your hardware — re-run the scripts.
# Bloomsieve

[![CI](https://img.shields.io/github/actions/workflow/status/deepak7448/bloomsieve/ci.yml?branch=master&label=CI)](https://github.com/deepak7448/bloomsieve/actions)
[![Python](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://pypi.org/project/bloomsieve/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Stop sending unnecessary membership checks to Redis.** Bloomsieve is a persistent
mmap-backed local Bloom filter that rejects definite-negative membership queries
before they ever become a network request to RedisBloom.

```text
                         Application
                              │
                              ▼
                     ┌─────────────────┐
                     │    Bloomsieve   │
                     │   local mmap    │
                     └────────┬────────┘
                              │
                   ┌──────────┴──────────┐
                   │                     │
             definitely absent      possibly present
                   │                     │
                   ▼                     ▼
                 return               RedisBloom
                locally              verification
```

## Why this exists

Most membership workloads are negative-heavy: "is this user active?", "is this
token valid?", "is this key seen before?" are mostly answered "no". With a direct
RedisBloom setup, **every** one of those queries crosses the network — even the
ones that are trivially absent.

A Bloom filter has **no false negatives**, so a local "definitely absent" answer
is provably correct. Bloomsieve keeps a persistent local mirror on disk
(`mmap`), answers negatives locally, and only sends the *possible positives* to
RedisBloom for verification. On a 99%-negative workload the local filter removes
**~99% of Redis membership requests** (see [Benchmarks](docs/benchmarks.md)).

## Installation

Core mode has **no runtime dependencies**:

```bash
pip install bloomsieve
```

RedisBloom integration is optional:

```bash
pip install "bloomsieve[redis]"
```

## 30-second example

```python
from bloomsieve import BloomFilter


def lookup_user(user_id: str):
    """Return the cached answer, or None when the user is definitely absent."""
    bloom = BloomFilter(
        capacity=10_000_000,
        error_rate=0.001,
        filepath="./users.bloom",  # optional: persists the filter to disk
    )
    bloom.add("user:123")  # normal application write path

    if user_id not in bloom:
        return None  # definite answer, nothing more to do

    return f"look up {user_id} in your database for an exact answer"


print(lookup_user("user:999"))  # None  – rejected by the local filter
print(lookup_user("user:123"))  # possible positive -> verify downstream
```

`capacity` is the expected number of items, `error_rate` the target false-positive
probability. Pass `filepath` to persist the filter across restarts.

## Redis example

```python
import redis
from bloomsieve import BloomFilterService

client = redis.Redis(host="redis.example.com", port=6379, db=0)

svc = BloomFilterService(
    redis_client=client,
    capacity=1_000_000,
    error_rate=0.001,
    use_mmap=True,  # enable the local pre-filter
    mmap_dir="/var/lib/bloomsieve",
)

svc.create_filter("active_tokens")
svc.add("active_tokens", "tok_abc")

svc.exists("active_tokens", "tok_xyz")  # False  – answered locally, no network
svc.exists("active_tokens", "tok_abc")  # True   – possible positive, verified in Redis
```

## How it works

- One SHA-256 digest per item, expanded to `k` positions with the
  Kirsch-Mitzenmacher double-hashing technique.
- A 16-byte header (`m`, `k`) plus the bit array, stored in a memory-mapped file;
  reopening a file always uses the stored configuration.
- `BloomFilterService` layers RedisBloom on top. Every `add` writes to both; every
  lookup checks the local mirror first and only verifies in Redis when the local
  answer is not a definite negative.

See [docs/architecture.md](docs/architecture.md) for the full design including the
on-disk format, failure modes, and consistency model.

## Benchmarks

Bloomsieve is designed to reduce remote Redis membership checks,
especially when the workload contains many negative lookups.

See the full reproducible methodology in `docs/benchmarks.md`.

### Key metric

Redis requests avoided:

| Negative workload | Redis requests avoided |
|---:|---:|
| 50% | (run benchmark to measure) |
| 75% | (run benchmark to measure) |
| 90% | (run benchmark to measure) |
| 95% | (run benchmark to measure) |
| 99% | (run benchmark to measure) |

*Results depend heavily on hardware, network configuration, and the specific dataset. Run `benchmarks/benchmark_redis.py` to measure exactly how many requests are avoided in your environment.*

## When should I use Bloomsieve?

Good fit:

- membership checks against Redis are frequent and mostly **negative**
- Redis is remote, so network latency matters
- a tunable probabilistic pre-filter is acceptable
- you benefit from a persistent, process-independent local filter
  (multiple app instances can share one file)

Poor fit:

- almost every lookup is positive (the local filter buys you nothing)
- membership checks are already local (you don't need Redis at all)
- exact membership is required with no verification step (Bloom filters have
  false positives)
- the dataset churns faster than your rebuild/rotation cycle can refresh the
  mirror

Bloom-filter semantics, precisely:

- **no false negatives** under correct operation — a local "absent" is definite;
- **possible false positives** — a local "present" must be verified against
  RedisBloom (or another authoritative source) when exact membership matters;
- false positives can be traded down by lowering `error_rate` (larger filter).

## Features

- Standalone `BloomFilter`: in-memory or persistent mmap, zero dependencies.
- `BloomFilterService`: local-negative short-circuit in front of RedisBloom.
- `rebuild()` + `swap()` rotation with chunked bulk insertion.
- Advisory Redis locks for coordinated rebuilds.
- Corrupt/truncated file detection (`BloomFilterFileError`), conservative Redis
  failure fallbacks, and full logging of fallback situations.

## API overview

### `BloomFilter`

```python
BloomFilter(capacity: int, error_rate: float, filepath: str | None = None)
```

- `add(item: str | bytes) -> bool` — insert; `True` if a bit changed, `False` if already likely present.
- `item in bf` — membership (no false negatives; `True` = possible positive).
- `clear() -> None` — reset all bits.
- `flush() -> None` — persist dirty pages to disk.
- `close() -> None` — flush and close file handles (context-manager compatible).
- `m`, `k`, `byte_size`, `newly_created`, `synced` — read-only diagnostics.

### `BloomFilterService`

```python
BloomFilterService(
    redis_client, capacity=1_000_000, error_rate=0.001, expansion=2, use_mmap=False, mmap_dir="bloom_filters"
)
```

- `create_filter(name, capacity=None, error_rate=None) -> bool` — reserve via `BF.RESERVE`
  (`createFilter` kept as a backwards-compatible alias).
- `add(name, item) -> bool`
- `exists(name, item) -> bool` — the local-negative short-circuit.
- `rebuild(name, items, capacity=None, error_rate=None) -> bool`
- `swap(temp_name, live_name) -> bool` — rotate a rebuilt filter into place.
- `get_info(name) -> dict`, `load_ratio(name) -> float`
- `acquire_lock(name, ttl=600) / release_lock(name) -> bool`
- `flush(name=None) -> None`

## Persistence / mmap behavior

- Writes go to the kernel page cache immediately and are visible to every process
  mapping the file; they are durable on disk after `flush()`/`close()` or OS
  writeback.
- Reopening a file trusts the stored header; a corrupt header or a file truncated
  below its bit array raises `BloomFilterFileError`.
- The rotation path (`swap`) flushes the temporary mirror before renaming it into
  place.

## Consistency and recovery

Redis and the local filesystem are updated as two separate steps — Bloomsieve
does **not** claim a cross-system atomic swap:

1. `RENAME` the filter in Redis; if that fails nothing else happens.
2. Rotate the local files.

If a failure lands between the two steps the service logs it and returns `False`;
a subsequent `rebuild()` repopulates both sides consistently. A freshly created
local mirror is treated as "unknown" (falling back to Redis) until items have
been added through the service, so an empty mirror can never produce false
negatives. Details: [docs/architecture.md](docs/architecture.md).

## Limitations

- Bloom filters cannot delete items; refresh with `rebuild()`/`swap()`.
- After capacity is exceeded the false-positive rate rises; it does not break.
- The local mirror is only as fresh as its last `flush()`/`close()`; if the
  process crashes mid-write the mirror can lag Redis (rebuild to recover).
- The service's locks are advisory; they are not a consensus-grade distributed
  lock.
- Core is tested on Python 3.9–3.13; Python 3.8 is not supported.

## Development

```bash
git clone https://github.com/deepak7448/bloomsieve.git
cd bloomsieve
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,redis]"
```

## Testing

```bash
ruff check .
pytest                                 # unit tests (no Redis required)

# also run the opt-in live-Redis integration suite:
BLOOMSIEVE_REDIS_URL=redis://localhost:6379/0 pytest
```



## Contributing

Issues and pull requests are welcome. Please run the linter and the full test
suite (including the live Redis suite if you can) before submitting.

## License

MIT. See [LICENSE](LICENSE).
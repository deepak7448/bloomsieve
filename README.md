# Bloomsieve

[![PyPI](https://img.shields.io/pypi/v/bloomsieve.svg)](https://pypi.org/project/bloomsieve/)
[![CI](https://img.shields.io/github/actions/workflow/status/deepak7448/bloomsieve/ci.yml?branch=master&label=CI)](https://github.com/deepak7448/bloomsieve/actions)
[![Python](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://pypi.org/project/bloomsieve/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Stop sending unnecessary membership checks to Redis.** Bloomsieve is a fast,
persistent, mmap-backed Bloom filter for Python. On its own it is a zero-dependency
Bloom filter you can share between processes. In front of RedisBloom it answers
"definitely absent" locally, so only the *possible positives* cross the network,
and it keeps the local filters on all your servers in sync through Redis.

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

On a 99%-negative workload this removes **~99% of Redis membership requests**
([benchmarks](#benchmarks)).

## Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Use cases](#use-cases)
- [Examples](#examples)
- [How it works](#how-it-works)
- [Choosing capacity and error rate](#choosing-capacity-and-error-rate)
- [Benchmarks](#benchmarks)
- [API reference](#api-reference)
- [Limitations](#limitations)
- [Upgrading from 0.2](#upgrading-from-02)
- [Development](#development)

## Installation

The core filter has **no runtime dependencies**:

```bash
pip install bloomsieve
```

The RedisBloom integration is an optional extra:

```bash
pip install "bloomsieve[redis]"
```

Redis needs the Bloom filter commands (`BF.*`) and, for multi-server sync,
Redis 6.2 or newer. Redis 8 includes the Bloom commands; for older versions use
Redis Stack or the RedisBloom module.

## Quick start

```python
from bloomsieve import BloomFilter

bloom = BloomFilter(capacity=1_000_000, error_rate=0.001)

bloom.add("user:123")
"user:123" in bloom  # True  -> possibly present
"user:999" in bloom  # False -> definitely never added
```

`capacity` is how many items you expect and `error_rate` the false-positive
probability you accept. A `False` answer is always correct; a `True` answer is
wrong with probability about `error_rate`.

## Use cases

### 1. In-memory set membership and de-duplication

`add()` returns `True` only for items it has not seen, so one call checks and
records an item. This is handy for de-duplicating streams, crawler frontiers or
event IDs in a fraction of a `set`'s memory.

```python
from bloomsieve import BloomFilter

seen = BloomFilter(capacity=10_000_000, error_rate=0.001)  # ~17 MiB

for event in stream:
    if seen.add(event.id):  # True -> first time
        process(event)

seen.add_many(ids)  # batch insert, returns how many were new
```

### 2. A persistent filter that survives restarts

Pass `filepath` and the filter lives in a memory-mapped file. Reopening it later
(or from another process) restores every bit. The stored configuration wins, so
reopening with different arguments can never corrupt it.

```python
from bloomsieve import BloomFilter

with BloomFilter(capacity=1_000_000, error_rate=0.001, filepath="/var/lib/app/seen.bloom") as bloom:
    bloom.add("session:abc")
    bloom.flush()  # durable on disk now (close() also flushes)

# next run
with BloomFilter(capacity=1_000_000, error_rate=0.001, filepath="/var/lib/app/seen.bloom") as bloom:
    "session:abc" in bloom  # True
```

A damaged file raises `BloomFilterFileError` instead of silently returning wrong
answers.

### 3. One filter shared by many processes

Several processes (gunicorn workers, multiprocessing pools, cron jobs) can open
the same file. Adds from one are immediately visible to all, and writes take an
exclusive file lock, so concurrent adds never lose bits. The file lock is POSIX
only; on Windows use one writer process per file.

```python
# in every worker process
bloom = BloomFilter(capacity=1_000_000, error_rate=0.001, filepath="/dev/shm/shared.bloom")
bloom.add(f"job:{job_id}")
```

### 4. A local pre-filter in front of RedisBloom

`BloomFilterService` keeps a local mirror of each Redis filter. Negative lookups
are answered locally with no network round trip; possible positives are verified
in Redis, so answers stay exact for anything RedisBloom would say.

```python
import redis
from bloomsieve import BloomFilterService

svc = BloomFilterService(
    redis_client=redis.Redis(host="redis.internal"),
    capacity=1_000_000,
    error_rate=0.001,
    use_mmap=True,  # enable the local pre-filter
    mmap_dir="/var/lib/bloomsieve",
)

svc.create_filter("active_tokens")  # every server can call this at startup
svc.add("active_tokens", "tok_abc")

svc.exists("active_tokens", "tok_xyz")  # False - answered locally, no network
svc.exists("active_tokens", "tok_abc")  # True  - verified in Redis
```

### 5. Many servers, each with its own local filter

Run the same code on every application server; each keeps its mirror on its own
disk. Every write, on any server, is also recorded in a small per-filter change
log in Redis, and each mirror catches up on that log before it answers "absent".
That makes a local "absent" correct for items added on *any* server, up to a
bounded delay:

```python
svc = BloomFilterService(
    redis_client=redis.Redis(host="redis.internal"),
    use_mmap=True,
    mmap_dir="/var/lib/bloomsieve",
    sync_interval=0.5,  # a local "absent" is at most 0.5 s behind other servers (default)
)
```

**Guarantee:** `exists()` only answers `False` locally if the mirror caught up
with Redis less than `sync_interval` seconds ago. So it can only miss an item
added on *another* server within the last `sync_interval` seconds. Items added
through the same mirror are visible at once. If a mirror cannot catch up (Redis
down, filter not replicated), it stops answering negatives and every lookup goes
to Redis. `sync_interval=0` makes every lookup check Redis first: always exact,
but no faster than plain Redis.

How it stays cheap: catching up costs one round trip per `sync_interval`, not
one per lookup. Mirrors take turns compacting the log into a bit snapshot, so the
log stays small, and a server that starts later downloads the snapshot plus the
recent log instead of every item. Writes cost a little more (one Lua script
instead of a plain `BF.ADD`, about 2× the latency on localhost).

Rules: write through `BloomFilterService` on every server (with or without
`use_mmap`). Items written to the Redis filter some other way are invisible to
mirrors. Don't let Redis evict the `bloomsieve:*` keys. If no server uses
`use_mmap`, pass `replicate=False` to skip the change log.

### 6. Batch writes and lookups

```python
svc.add_many("active_tokens", tokens)  # BF.MADD, one round trip
svc.exists_many("active_tokens", ["a", "b", "c"])  # [True, False, False]
```

`exists_many` answers local negatives without Redis and sends the rest in a
single `BF.MEXISTS` round trip.

### 7. Rebuilding and rotating without downtime

Bloom filters cannot delete items, and their error rate rises past capacity.
Rebuild periodically from your source of truth into a temporary filter, then
swap it in while the live filter keeps serving reads:

```python
if svc.load_ratio("active_users") > 0.8 and svc.acquire_lock("rebuild:active_users", ttl=300):
    try:
        if svc.rebuild("active_users:next", load_users_from_db(), capacity=2_000_000):
            svc.swap("active_users:next", "active_users")
    finally:
        svc.release_lock("rebuild:active_users")
```

Only one server runs the rebuild. The others see the new generation on their
next sync and load its snapshot.

### 8. Adopting a Redis filter that already has data

A filter created outside Bloomsieve (or by 0.2) has no change log, so a mirror
cannot know what it holds. Lookups stay correct, but every one goes to Redis (a
warning is logged once). Rebuild it once from your source of truth to bring it
under replication:

```python
svc.rebuild("legacy_filter", all_items)
```

## Examples

Runnable scripts in [`examples/`](examples/). The Redis ones need a server
(`BLOOMSIEVE_REDIS_URL`, default `redis://localhost:6379/0`) and skip cleanly
without one.

| Script | Shows |
| --- | --- |
| [`basic_usage.py`](examples/basic_usage.py) | in-memory filter, `add` return value, `add_many`, `clear`, sizing |
| [`dedupe_stream.py`](examples/dedupe_stream.py) | de-duplicating a stream of URLs |
| [`persistent_filter.py`](examples/persistent_filter.py) | file-backed filter, reopen, `flush`, corrupt-file handling |
| [`multiprocess_shared_file.py`](examples/multiprocess_shared_file.py) | several processes writing one file without losing items |
| [`redis_prefilter.py`](examples/redis_prefilter.py) | Redis pre-filter, batch APIs, counting avoided requests |
| [`redis_rotation.py`](examples/redis_rotation.py) | `load_ratio` trigger, lock, `rebuild` + `swap` |
| [`redis_multi_host.py`](examples/redis_multi_host.py) | several servers: a write on one is seen by the others; a late joiner loads the snapshot |
| [`redis_adopt_existing.py`](examples/redis_adopt_existing.py) | fallback for a filter that is not replicated, then `rebuild` |

## How it works

- **Hashing:** one SHA-256 digest per item, expanded to `k` bit positions with
  Kirsch-Mitzenmacher double hashing. `str` items are hashed as UTF-8, so `"a"`
  and `b"a"` are the same item.
- **Storage:** a 16-byte header (`m`, `k`) followed by the bit array, in memory or
  in a memory-mapped file. New files are created atomically, so concurrent
  starters never clobber each other.
- **Concurrency:** writes are serialised by a thread lock plus an `flock` on the
  file; lookups are lock-free.
- **Replication:** each replicated filter has two extra Redis keys: a stream
  `bloomsieve:{<name>}:log` with the items written since the last snapshot, and a
  hash `bloomsieve:{<name>}:snap` with a generation id and a compacted bit array.
  Writes go to the log and the filter in one atomic Lua script. Mirrors replay the
  log, and compact it once it grows. A rebuild starts a new generation, which every
  mirror detects and reloads. The keys share the filter's Redis Cluster hash slot.
- **Trust:** the service only answers negatives locally from a mirror that is
  provably complete and fresh. Its state (generation, log position) is saved in a
  small `<file>.synced` marker tied to the mirror's identity on disk. A restart
  resumes from where it left off, and a recreated file is never trusted by
  accident.

Full design, on-disk format, failure modes and consistency model:
[docs/architecture.md](docs/architecture.md).

## Choosing capacity and error rate

Memory is `m / 8` bytes, fixed up front:

| capacity | error_rate 1% | 0.1% | 0.01% |
| ---: | ---: | ---: | ---: |
| 100 000 | 0.1 MiB (k=7) | 0.2 MiB (k=10) | 0.2 MiB (k=13) |
| 1 000 000 | 1.1 MiB (k=7) | 1.7 MiB (k=10) | 2.3 MiB (k=13) |
| 10 000 000 | 11.4 MiB (k=7) | 17.1 MiB (k=10) | 22.9 MiB (k=13) |
| 100 000 000 | 114 MiB (k=7) | 171 MiB (k=10) | 229 MiB (k=13) |

`get_optimal_m_k(capacity, error_rate)` returns `(m, k)` without allocating
anything. Going past `capacity` does not break the filter; its false-positive rate
gradually rises, which is the signal to rebuild with a larger capacity.

## Benchmarks

`benchmarks/benchmark_redis.py`, 100 000 items, 100 000 lookups, `error_rate=0.001`,
Redis 8.6 on **localhost** (so the network is as fast as it gets),
Python 3.9, Intel i7-14650HX:

| Negative lookups | Redis requests avoided | Total time, Redis only | Total time, Bloomsieve |
| ---: | ---: | ---: | ---: |
| 50% | 50.0% | 3.60 s | 2.48 s |
| 75% | 74.9% | 3.17 s | 1.20 s |
| 90% | 89.9% | 3.35 s | 0.58 s |
| 95% | 94.9% | 3.30 s | 0.42 s |
| 99% | 98.9% | 3.50 s | 0.31 s |

With a simulated 2 ms round trip (`benchmark_end_to_end.py`, 90% negative), total
time drops from 44.8 s to 5.4 s. The further away Redis is, the bigger the win.

Local operations (same machine): lookup of an absent item ~1.4 µs, lookup of a
present item ~3.0 µs, `add` ~3.9 µs (in memory) / ~4.8 µs (file-backed, including
the file lock), `add_many` ~3.4 µs per item. Through the service on localhost
Redis: a local negative ~2.1 µs, `add` ~77 µs with replication (~39 µs without).

Results depend on hardware and network; run the scripts to measure your own
environment. Methodology: [docs/benchmarks.md](docs/benchmarks.md).

## API reference

### `BloomFilter(capacity, error_rate, filepath=None)`

| Member | Description |
| --- | --- |
| `add(item) -> bool` | Insert; `True` if the item was new. |
| `add_many(items) -> int` | Insert many under one lock; returns how many were new. |
| `item in bf` | `False` = definitely absent, `True` = possibly present. |
| `clear()` | Reset all bits (configuration kept). |
| `flush()` | Make file-backed writes durable on disk. |
| `close()` | Flush and release the file; also via `with BloomFilter(...) as bf:`. |
| `closed` | `True` after `close()`. |
| `m`, `k`, `byte_size`, `total_size` | Bits, hash functions, bit-array bytes, file bytes. |
| `newly_created` | `True` if this call created the file (or the filter is in memory). |

Items are `str` or `bytes`; anything else raises `TypeError`. Operations on a
closed filter raise `RuntimeError`. A corrupt file raises `BloomFilterFileError`
(a `ValueError` subclass).

### `BloomFilterService(redis_client, capacity=1_000_000, error_rate=0.001, expansion=2, use_mmap=False, mmap_dir=None, replicate=True, sync_interval=0.5, max_log_entries=1_000_000)`

`replicate` turns on the change log that keeps mirrors on different servers in
sync (it needs a client with the default `decode_responses=False`).
`sync_interval` bounds how stale a local negative can be. `max_log_entries` is a
safety cap: if the log ever reaches it (no mirror is compacting), replication of
that filter switches off and mirrors fall back to Redis until it is rebuilt. Keep
it well above `write rate × sync_interval`.

| Method | Description |
| --- | --- |
| `create_filter(name, capacity=None, error_rate=None) -> bool` | `BF.RESERVE`; an existing filter counts as success (`createFilter` is an alias). |
| `add(name, item) -> bool` | Add to the mirror, then `BF.ADD`. |
| `add_many(name, items) -> bool` | Add to the mirror, then pipelined `BF.MADD`. |
| `exists(name, item) -> bool` | Local definite negative, otherwise `BF.EXISTS`. |
| `exists_many(name, items) -> list[bool]` | Local negatives, the rest in one `BF.MEXISTS` round trip. |
| `rebuild(name, items, capacity=None, error_rate=None) -> bool` | Recreate the filter from `items` as a new generation. |
| `swap(temp_name, live_name) -> bool` | Rename the filter (and its log and snapshot) in Redis and rotate the local mirror. |
| `delete_filter(name) -> bool` | Delete the filter, its log and snapshot, and the local mirror. |
| `mark_synced(name) -> bool` | `replicate=False` only: declare the local mirror complete (use with care). |
| `get_info(name) -> dict`, `load_ratio(name) -> float` | `BF.INFO`: capacity, inserted, ratio. |
| `acquire_lock(name, ttl=600)`, `release_lock(name) -> bool` | Advisory Redis lock with an ownership token. |
| `flush(name=None)`, `close(name=None)` | Flush / close local mirrors; also usable as a context manager. |

When Redis is unreachable, writes return `False` and lookups return a
conservative `True`, both with a logged warning. Bloomsieve logs through the
standard `logging` module (`bloomsieve.core`, `bloomsieve.redis_service`).

## Limitations

- Bloom filters cannot delete items; refresh with `rebuild()` + `swap()`.
- Past `capacity` the false-positive rate rises; it does not fail.
- A local "absent" can miss an item added on another server within the last
  `sync_interval` seconds ([guarantee](#5-many-servers-each-with-its-own-local-filter)).
- Writes must go through `BloomFilterService`; items added to Redis some other
  way are invisible to mirrors.
- With `replicate=False`, every writer must share the mirror file (one host or a
  shared volume).
- After a machine crash, unflushed mirror writes can be lost; `rebuild()` after
  an unclean shutdown.
- Cross-process write locking is POSIX only (Linux, macOS).
- Service locks are advisory (token-checked, but not fenced).
- Python 3.9–3.13.

## Upgrading from 0.2

- **Files are compatible:** the on-disk format and hashing are unchanged.
- **Multi-server sync is on by default** (`replicate=True`). In 0.2 a mirror only
  knew about its own host's writes, and any existing mirror file, or any mirror
  after a single `add`, was trusted. That produced wrong "absent" answers. In 0.3,
  filters from 0.2 start *unreplicated*: lookups stay correct but go to Redis
  until you run `rebuild(name, items)` once. A warning is logged once per filter.
- Writes through the service now run a Lua script (Redis 6.2+), and replicated
  filters use two extra keys per filter (`bloomsieve:{<name>}:log` / `:snap`).
- Mirror files for names containing characters outside `[A-Za-z0-9_.-]` get a
  hash suffix, so they start as new (untrusted) files.
- `rebuild` uses `BF.MADD` and skips only `None` items (`""` is now added).
- `release_lock` only releases locks acquired by the same service instance.
- `error_rate` values needing more than 128 hash functions now raise `ValueError`.

Full list: [CHANGELOG.md](CHANGELOG.md).

## Development

```bash
git clone https://github.com/deepak7448/bloomsieve.git
cd bloomsieve
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,redis]"

ruff check . && ruff format --check .
pytest                                                # unit tests, no Redis needed
BLOOMSIEVE_REDIS_URL=redis://localhost:6379/0 pytest  # plus the live Redis suite
```

**Releasing:** bump `__version__` in `src/bloomsieve/__init__.py`, update
`CHANGELOG.md`, commit and push, then publish a GitHub release with tag `v<version>`
(e.g. `v0.3.0`). The `Publish to PyPI` workflow (`.github/workflows/publish.yml`)
checks the tag against `__version__`, runs lint and unit tests, builds once, and
uploads with Trusted Publishing and signed attestations, so every file shows its
provenance on PyPI. A release marked *pre-release* goes to TestPyPI. Tests run
separately in `ci.yml` on every push and pull request. `./build.sh` runs the same
checks locally; manual uploads carry no provenance, so publish releases from GitHub.

Issues and pull requests are welcome. Please run the linter and the full test
suite (including the live Redis suite if you can) before submitting.

## License

MIT. See [LICENSE](LICENSE).

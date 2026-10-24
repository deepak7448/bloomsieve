# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0]

### Fixed
- **Multi-server correctness.** A local mirror only knew about items added on its
  own machine, so applications running on several servers could get wrong
  "absent" answers. Writes are now replicated through a per-filter change log in
  Redis, and every mirror catches up before answering a negative (see *Added*).
- A local mirror became trusted after a single `add`, and any existing mirror file
  was trusted on open, even when Redis already held items the mirror had never
  seen. That gave wrong "absent" answers. Trust now has to be established
  (fresh filter, `rebuild`, replication catch-up, `mark_synced`).
- `swap()` left the previous generation's local mirror in place, and trusted it,
  when the temporary filter had no local mirror. That hid newly added items.
- Concurrent writers (threads, or processes sharing one file) could lose bits, and
  so produce false negatives, because setting a bit was an unlocked
  read-modify-write. Writes are now serialised with a thread lock plus `flock`.
- A trust marker could vouch for a deleted and recreated (empty) mirror when
  the file system reused the inode number, giving wrong "absent" answers (seen on
  GitHub Actions). Files now carry a random id in a 24-byte trailer (readable by
  0.2), and markers refer to that id.
- Two processes creating the same filter file at once could wipe each other's
  bits. New files are now written fully and published atomically.
- The mirror cache was shared by all `BloomFilterService` instances and keyed by
  filter name only, so services with different `mmap_dir`s (or different Redis
  servers) shared one mirror. It is now per instance.
- Processes did not notice another process's `rebuild`/`swap` and kept using the
  old file. Mirrors are now re-checked on disk every `sync_interval`.
- Filter names that sanitise to the same filename (`a/b`, `a_b`) shared a mirror
  file. Unsafe names now get a short hash suffix.
- `error_rate` values needing more than 128 hash functions created files the
  library refused to reopen; they now raise `ValueError`. NaN is rejected too.
- `release_lock()` could delete a lock held by someone else after expiry. Locks
  now carry an ownership token and are released with compare-and-delete.
- CI never published to PyPI: the pre-release check matched the "a" in
  `refs/tags/`. Publishing is now driven by GitHub releases (see *Changed*).

### Added
- Replication (`replicate=True`, default): writes go through an atomic Lua script
  that appends to `bloomsieve:{<name>}:log` and runs `BF.MADD`. Mirrors catch up at
  most every `sync_interval` seconds (default 0.5), compact the log into a bit
  snapshot (`bloomsieve:{<name>}:snap`), and bootstrap from that snapshot when they
  start late or after another host's `rebuild`/`swap`. A local negative can only
  miss an item added on another server within the last `sync_interval` seconds.
  The keys share the filter's Redis Cluster hash slot. `max_log_entries` caps the
  log.
- `BloomFilter.add_many()` (one lock per batch), `to_bytes()`, `merge_bytes()`,
  `closed`, and a `repr`.
- `BloomFilterService.add_many()` (`BF.MADD`) and `exists_many()` (local
  negatives, the rest in one `BF.MEXISTS` round trip), `delete_filter()`,
  `mark_synced()` (single-host mode), `close()`, and context-manager support.
- `MAX_HASHES` constant.
- Examples for every use case: de-duplication, persistent filters, multi-process
  sharing, the Redis pre-filter, multiple servers, rotation, and adopting
  existing filters. CI runs all of them.

### Changed
- Lookups are about 2x faster for absent items and about 25% faster for present
  ones (incremental double hashing, a single code path for memory and mmap). The
  bit layout and file format are unchanged, and files from 0.2 open correctly.
- New files are sparse (`truncate`) instead of being written with zeros.
- `rebuild()` inserts with `BF.MADD` and skips only `None` items (`""` is a valid
  item). Items must be `str` or `bytes`.
- Filters created by 0.2 start unreplicated (lookups use Redis) until they are
  rebuilt once.
- Redis 6.2+ is required for replication (`replicate=False` works with older
  servers).
- Publishing moved to its own workflow, `publish.yml`, triggered by publishing a
  GitHub release. It checks the tag against `__version__`, tests, builds once and
  uploads with PyPI Trusted Publishing and signed PEP 740 attestations, so files
  show their provenance on PyPI. Earlier releases were uploaded by hand and have
  none. `ci.yml` now only tests.
- CI uses the `redis:8.0` image (Bloom commands built in), checks formatting, and
  runs the examples. `build.sh` stops on the first failure and only uploads when
  asked.

## [0.2.0]

### Added
- `BloomFilter.flush()` to explicitly persist mmap-backed writes to disk.
- `BloomFilterService.flush()` to flush local mmap mirrors.
- `BloomFilterFileError` raised for unreadable/corrupt mmap files instead of a
  silent failure.
- Local mmap "cold filter" handling: a freshly created, still-empty local mirror
  is treated as unknown and falls back to Redis (with a one-time warning) until it
  has been populated through the service. This prevents empty local filters from
  producing false negatives.
- Comprehensive pytest suite: Bloom filter boundary conditions, mmap file sizing
  and corruption handling, Redis/mmap consistency, partial-failure rotation
  tests, and an opt-in live-Redis integration suite
  (`BLOOMSIEVE_REDIS_URL=redis://localhost:6379/0 pytest`).
- Benchmark suite under `benchmarks/` (core, mmap, and Redis workload A/B).
- Documentation: `docs/architecture.md` and `docs/benchmarks.md`.
- CI now runs on the `master` branch across Python 3.9–3.13 with the live Redis
  integration suite enabled.

### Changed
- Redis is now an **optional** dependency. `pip install bloomsieve` installs the
  standalone filter with no runtime dependencies; `pip install bloomsieve[redis]`
  enables `BloomFilterService`.
- Canonical service API renamed to snake_case: `create_filter()` is now the
  documented name. The previous `createFilter()` is retained as a
  backwards-compatible alias.
- mmap-backed `add()` no longer calls `msync` on every insertion; writes are
  flushed on `close()`/`flush()` or by the OS. See the consistency model in
  `docs/architecture.md`.
- Minimum supported Python raised from 3.8 to 3.9 (Python 3.8 is end-of-life).
- Core `BloomFilter` now rejects non-`str`/`bytes` items with a `TypeError`
  instead of silently coercing them with `str(item)`.
- `swap()` no longer touches local files when the Redis `RENAME` has failed, and
  returns `False` (with a logged error) when the two steps disagree.
- Reference to a store-and-forward rotation is documented as a two-step operation
  that is *not* a cross-system atomic transaction.
- Broad `except Exception` blocks now log before falling back.

### Removed
- Unsupported marketing claims from README/docstrings (e.g. "zero-latency",
  "zero-downtime", "atomic swap").

## [0.1.0] - 2026-08-08

### Added
- Modular package layout under `src/bloomsieve/`.
- Standalone local `BloomFilter` supporting both in-memory and disk-backed `mmap`
  mode.
- Kirsch-Mitzenmacher double hashing to reduce hashing operations from O(k) to
  O(1) SHA-256 digests per item.
- `BloomFilterService` with RedisBloom integration, distributed locking, and an
  optional mmap cache.
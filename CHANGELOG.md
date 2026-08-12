# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Targeted as **0.2.0**.

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
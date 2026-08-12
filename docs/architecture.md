# Architecture

Bloomsieve is a local, persistent Bloom filter layer designed to eliminate
unnecessary remote membership checks against RedisBloom.

## Components

| Component | Purpose |
| --- | --- |
| `BloomFilter` (`src/bloomsieve/core.py`) | Standalone Bloom filter with an in-memory mode and a persistent memory-mapped (`mmap`) file mode. Zero runtime dependencies. |
| `BloomFilterService` (`src/bloomsieve/redis_service.py`) | Optional integration layer that combines RedisBloom with a local mmap pre-filter. Requires the `redis` package (installed via `pip install bloomsieve[redis]`). |
| `utils.py` | Sizing (`m`, `k`) and Kirsch-Mitzenmacher double-hashing helpers. |

## On-disk format

A 16-byte header is followed by the bit array:

```
offset  size  meaning
0       8     m — number of bits (unsigned 64-bit)
8       8     k — number of hash functions (unsigned 64-bit)
16      m//8  bit array (ones = possibly present)
```

`m` is always a multiple of 8 so the bit array is byte-aligned. When a filter is
reopened, the stored header wins over the constructor's `capacity`/`error_rate`,
which guarantees a filter is always reopened with the same parameters it was
created with. The header is validated against the file size on load; an unreadable
or implausible header raises `BloomFilterFileError`.

## Hashing

`get_hash_indices` computes one SHA-256 digest per item and splits the first 16
bytes into two 64-bit values `h1`, `h2`, then derives `k` indices using the
Kirsch-Mitzenmacher technique: `g_i(x) = (h1 + i * h2) % m`. This reduces hashing
cost from `k` digests to one digest per item.

## Membership flow

For a service with mmap enabled:

```
Application
    │  exists(name, item)
    ▼
local mmap Bloom filter
    │
    ├── definitely absent  ──────────────► return False   (no Redis request)
    │
    └── possibly present / unknown ──────► BF.EXISTS /verify against Redis
```

Semantics:

- A local **negative** is a definite negative *only if* the local mirror is
  trusted (`synced`), i.e. it was loaded from an existing file that is assumed to
  mirror Redis, or it has been populated through this service's `add`/`rebuild`.
- A freshly created, still-empty local file is treated as **cold/unknown** and
  falls back to Redis (with a one-time warning) until the first item is added
  locally. This avoids the dangerous state where an empty local filter would
  answer "definitely absent" for everything and cause false negatives.
- A local **possible positive** always verifies against RedisBloom by design —
  Bloom filters are allowed to produce false positives, but a false positive must
  never be returned as an authoritative "present".

## Persistence and mmap semantics

- Writes are applied to the kernel page cache immediately and are visible to other
  processes mapping the same file with shared access.
- They are guaranteed durable on disk only after `BloomFilter.flush()` or
  `BloomFilter.close()` (both call `msync`), or after the OS flushes the page
  cache. `add` does **not** flush on every insertion; scaling tests showed that a
  per-insert `msync` dominates cost.
- `BloomFilterService.flush()` flushes all (or one named) cached mirrors, and the
  rotation path (`swap`) flushes the temporary mirror before renaming it into
  place.

### Crash consistency

Because duplicates are applied lazily (page cache), a crash between a write and a
flush can leave the on-disk mirror behind the in-Redis state. A stale mirror could
then answer "definitely absent" for an item that Redis considers present — a
**false negative**. Mitigations:

- Call `flush()` after bulk writes, or after `rebuild`/`swap` (the swap path
  already flushes the temporary mirror).
- Treat the local mirror as a cache of the authoritative Redis filter. If the
  mirror is ever suspect, `rebuild()` is the supported recovery path: it
  rebuilds Redis and the mirror from the same item set.

## Rebuild and rotation

`rebuild(name, items, ...)`:

1. Deletes the Redis filter (errors are logged, not fatal).
2. Closes the local mirror and removes its file (mmap mode).
3. Re-reserves the filter in Redis (`BF.RESERVE`).
4. Bulk-inserts items in chunks of 1000 through a pipeline, adding each item to the
   local mirror as well.

If Redis fails during step 4, `rebuild` returns `False` and leaves the partially
populated filter in place for inspection.

Rotating in a new generation is `swap(temp_name, live_name)`:

1. `RENAME temp_name live_name` in Redis. If this fails, nothing is touched and
   `False` is returned.
2. If mmap is enabled and a local file for `temp_name` exists, the local files are
   rotated with `os.replace` (atomic within a filesystem). The mirror for the live
   name is then reopened from the new file.

### Consistency model — `swap` is not a cross-system transaction

Redis and the local filesystem are two independent systems; there is no single
atomic operation that spans both. The two steps above are ordered so a failure in
step 1 changes nothing. A failure *between* the two steps (Redis renamed, local
replace failed) leaves the local mirror behind Redis. `swap` then logs the failure
and returns `False`, signalling a partial rotation so the caller can remedy it
(a subsequent `rebuild` repopulates both sides consistently). False negatives are
possible while the two sides disagree.

## Failure modes

| Failure | Behaviour |
| --- | --- |
| mmap file missing | Created and initialised with requested parameters. |
| mmap file smaller than 16 bytes | Treat + reinitialise with requested parameters, log a warning. |
| mmap file has corrupt/unplausible header | Raise `BloomFilterFileError`. |
| mmap file shorter than its header's bit array | Raise `BloomFilterFileError`. |
| Local mirror cannot be opened | Log a warning, fall back to Redis for every lookup. |
| Local mirror read error | Log a warning, fall back to Redis for that lookup. |
| Redis `BF.RESERVE` fails (already exists) | Treated as success. |
| Redis `BF.RESERVE` fails (connection/other) | Return `False`, log a warning. |
| Redis `BF.ADD` fails | Return `False`, log a warning; the local mirror may be ahead of Redis. |
| Redis `BF.EXISTS` fails | Return a conservative `True`, log a warning (fail-open for membership so callers do not get an incorrect negative). |
| Redis unavailable at all | `BloomFilterService` methods degrade to logged `False`/`True` fallbacks; the standalone `BloomFilter` is unaffected. |

The `exists` failure policy is deliberately conservative: when Redis is unreachable
the service returns `True` rather than a potentially incorrect `False`, because
`False` short-circuits the caller's downstream logic as a definite negative.

## Locking and concurrency

- A single `BloomFilter` instance is not thread-safe; concurrent use needs external
  locking.
- `BloomFilterService` guards its mmap cache (a module-level dict shared across
  instances) with an internal `threading.RLock`.
- `acquire_lock`/`release_lock` provide advisory, TTL-based coordination for
  rebuild/rotation (Redis `SET NX EX` / `DEL`). They are not a consensus-grade
  distributed lock: there is no ownership token or fencing, and a lock can expire
  while an operation is still running.

## Security notes

- Filter names are sanitised before being turned into file paths (everything but
  `[A-Za-z0-9_.-]` becomes `_`), so service filter names cannot escape
  `mmap_dir`.
- Untrusted/malformed mmap files are rejected by header validation; the package
  never evaluates or deserialises untrusted content.
- Redis is configured and supplied by the caller (connection details, timeouts,
  auth) — Bloomsieve never holds connection secrets.
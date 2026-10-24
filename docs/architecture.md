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
0       8     m — number of bits (unsigned 64-bit, little-endian)
8       8     k — number of hash functions (unsigned 64-bit, little-endian)
16      m//8  bit array (ones = possibly present); bit i is (byte 16 + i//8) >> (i%8)
```

`m` is always a multiple of 8 so the bit array is byte-aligned, and `k` is at most
128 (`MAX_HASHES`; sizing rejects error rates that would need more). When a filter
is reopened, the stored header wins over the constructor's `capacity`/`error_rate`,
which guarantees a filter is always reopened with the same parameters it was
created with. The header is validated against the file size on load; an unreadable
or implausible header raises `BloomFilterFileError`.

The format, including the hashing below, has not changed since 0.2: files written
by older versions open and answer correctly. A golden-value test guards this.

### Creating a file

A new file is written completely (header, then a zero-filled bit array created
with `truncate`, which is sparse where the filesystem supports it) under a
temporary name and published with a hard link. The link fails atomically if
another process created the file first, in which case that file is used. Other
processes therefore never observe a half-initialised file, and two processes
starting at once never wipe each other's bits. On filesystems without hard links
a best-effort check-then-rename is used instead.

## Hashing

One SHA-256 digest is computed per item (`str` is hashed as UTF-8, so `"a"` and
`b"a"` are the same item). Its first 16 bytes are split into two little-endian
64-bit values `h1`, `h2`, and the `k` indices are derived with the
Kirsch-Mitzenmacher technique: `g_i(x) = (h1 + i * h2) % m`. The hot path computes
the same indices incrementally (`idx += h2 % m`, wrapping at `m`) to avoid
big-integer multiplication; `get_hash_indices` exposes the reference formula.

## Concurrency

- **Writes** (`add`, `add_many`, `clear`) take a per-instance thread lock and, for
  file-backed filters on POSIX, an exclusive `flock` on the file. Setting a bit is
  a read-modify-write of one byte, so without the lock two writers touching the
  same byte could erase each other's bit, which is a false negative. With it,
  threads and processes sharing a file never lose an add. `add_many` takes the
  lock once per batch, which is why it is faster than `add` in a loop.
- **Lookups** take no lock. Bits only ever go from 0 to 1 (outside `clear`), so a
  concurrent lookup can at worst miss an add that has not finished yet.
- Windows has no `fcntl`, so there is no cross-process lock there: use one writer
  process per file.
- Do not `close()` a filter while other threads are still using it.

## Membership flow

For a service with mmap enabled:

```
Application
    │  exists(name, item)
    ▼
local mmap Bloom filter
    │
    ├── fresh, trusted mirror says "definitely absent" ──► return False   (no Redis request)
    │
    └── possibly present / mirror not fresh or trusted ──► BF.EXISTS (verify against Redis)
```

A local answer may only be "definitely absent" if the mirror holds every item in
the Redis filter. How the service establishes that depends on `replicate`.

### Replication across hosts (`replicate=True`, the default)

Each application server keeps its own mirror file, so the servers need a way to
learn about each other's writes. Redis carries them as a **change log** plus a
**snapshot** per filter:

| Key | Type | Content |
| --- | --- | --- |
| `<name>` | RedisBloom filter | the authoritative filter |
| `bloomsieve:{<name>}:log` | stream | one entry per write call; the fields are the items |
| `bloomsieve:{<name>}:snap` | hash | `gen` (generation id), `m`, `k`, `cursor` (log id covered), `data` (bit array) |

Wrapping the name in `{...}` puts both keys in the filter's Redis Cluster hash
slot. If the name already has a hash tag, the keys reuse it.

**Writes.** `add`, `add_many` and `rebuild` call one Lua script per chunk. If the
filter has a snapshot, the script appends the items to the log, then runs
`BF.MADD`. The log entry comes first, so a failure can only leave an item in the
log that is missing from the filter. That costs mirrors a false positive, never a
false negative. Writers without a mirror (`use_mmap=False`) log too, so mirrors on
other hosts still see their items.

**Catching up.** A mirror remembers `(generation, cursor)`. Before answering a
negative, if its last catch-up is older than `sync_interval`, it reads in one
`MULTI` transaction:
`HMGET snap gen cursor`, `XREAD` from its cursor, and `XLEN`. Then:

- no snapshot: the filter is not replicated, so the mirror is untrusted and every
  lookup goes to Redis;
- different generation (the filter was rebuilt or swapped elsewhere), or the
  snapshot's cursor is past ours (the log was compacted beyond our position):
  **bootstrap**, i.e. download the snapshot. A new generation replaces the file
  (with the snapshot's `m`, `k`); a compaction gap is closed by OR-merging the
  snapshot bits;
- otherwise: apply the new log entries (`add_many`) and advance the cursor,
  repeating until caught up.

Only then is the mirror trusted, and its catch-up time recorded. Hosts with a
mirror also catch up on the write path, at most once per interval.

**Guarantee.** A local "absent" reflects every write that reached the log before
a catch-up done less than `sync_interval` seconds ago. It can only miss an item
added on another host within that window; writes through the same mirror file
are visible at once. If a catch-up fails (Redis unreachable, snapshot gone), the
mirror stops answering negatives and lookups use Redis, retrying the catch-up once
per interval.

**Compaction.** When a caught-up mirror sees the log at or above
`min(max(10 000, mirror bytes / 64), max_log_entries / 2)` entries, it uploads
its bit array as the new snapshot at its cursor and trims the log up to that
cursor (`XTRIM MINID`). A Lua script does this atomically, and only if the
generation still matches and the new cursor is newer, so concurrent compactions
from several hosts are safe. The log therefore stays small, and a new host
downloads one snapshot (about the mirror's size) plus the recent log instead of
every item.

**Safety cap.** If the log reaches `max_log_entries` (for example, writers but
no mirror running to compact it), the write script deletes the snapshot and the
log. Every mirror then falls back to Redis until the filter is rebuilt. Redis
memory stays bounded, and correctness is never traded for it.

**Generations.** `create_filter` on a brand-new Redis filter creates an empty
snapshot with a new generation id; an already existing filter is left alone.
`rebuild` deletes the filter, log and snapshot, starts a new generation, writes
all items through the log, then publishes its mirror as the snapshot and skips
replaying its own entries when no other host wrote meanwhile. `swap` renames the
filter, log and snapshot together in one `MULTI`, so every host sees the new
generation on its next catch-up and loads it.

**Restarts and processes.** The mirror's `(file identity, generation, cursor)` is
saved in `<mirror>.bloom.synced` (after a bootstrap, on flush/close, and every
10 s), after flushing the mirror, so the saved state never runs ahead of the
file. A restarted process resumes from it without downloading the snapshot.
Processes on one host share the mirror file; a bootstrap holds an `flock` on
`<mirror>.bloom.lock`, so only one process reloads the file and the others adopt
it.

### Single-host mode (`replicate=False`)

Without the change log, the mirror only sees writes made through services that
share its file, so **every writer must use the same `mmap_dir`** (one host or a
shared volume). Trust is tracked explicitly:

| Event | Effect on trust |
| --- | --- |
| `create_filter` reserved a **new** Redis filter | trusted (an empty Redis filter cannot hold anything the mirror lacks) |
| `create_filter` found the Redis filter **already existed** | unchanged |
| `rebuild(name, items)` completed | trusted (both sides filled from the same items) |
| `swap(temp, live)` with a trusted temp mirror | live mirror trusted |
| `swap(temp, live)` with no temp mirror on this host | live mirror deleted, untrusted |
| `mark_synced(name)` | trusted (the caller vouches for it) |
| `add` / `add_many` | **unchanged**: adding items never makes a mirror trusted |
| mirror file deleted or replaced | the new file is untrusted until one of the above |

Trust is the marker file holding the mirror's device and inode numbers, so it
survives restarts and is shared by every process on the host. Because it names a
specific file, a recreated mirror is never trusted by accident. Processes notice
another process's rebuild or swap within `sync_interval` seconds.

### In both modes

A local **possible positive** always verifies against RedisBloom. Bloom filters
may produce false positives, but a false positive must never be returned as an
authoritative "present". Items written to the Redis filter by anything other than
`BloomFilterService` are invisible to mirrors.

## Persistence and mmap semantics

- Writes are applied to the kernel page cache immediately and are visible to other
  processes mapping the same file.
- They are guaranteed durable on disk only after `BloomFilter.flush()` or
  `BloomFilter.close()` (both call `msync`), or after the OS flushes the page
  cache. `add` does **not** flush on every insertion, because a per-insert `msync`
  dominates cost.
- `BloomFilterService.flush()` flushes all (or one named) cached mirrors.
  `rebuild` flushes the mirror when it finishes, and writing a trust marker
  always flushes the mirror first.

### Crash consistency

The service writes the local mirror *before* Redis, so in normal operation the
mirror is a superset of Redis. After a machine crash (not just a process crash)
before a flush, the on-disk mirror can be missing recent writes that reached
Redis, and a trusted stale mirror could then answer "definitely absent" for an
item Redis holds. Mitigations:

- Call `flush()` after important bulk writes.
- After an unclean machine shutdown, run `rebuild()` (or rebuild + `swap`) from
  the source of truth.

## Rebuild and rotation

`rebuild(name, items, ...)`:

1. Deletes the Redis filter (errors are logged, not fatal).
2. Discards the local mirror: revokes trust, closes it and deletes its file.
3. Re-reserves the filter in Redis (`BF.RESERVE`) and, with replication, starts
   a new generation.
4. Inserts items in chunks of 1000: each chunk goes to the local mirror
   (`add_many`), then to Redis (one `BF.MADD`, through the change log when
   replicating). `None` items are skipped.
5. Publishes the mirror as the snapshot (replication) or flushes it.

If Redis fails during step 4, `rebuild` returns `False` and leaves the partially
populated filter in place. The mirror is still a superset of Redis, so it stays
correct. A rebuild in place leaves the filter empty while it refills, so rebuild
under a temporary name and `swap` it in when the filter is serving reads.

Rotating in a new generation is `swap(temp_name, live_name)`:

1. `RENAME temp_name live_name` in Redis (with replication, together with the
   log and snapshot keys in one `MULTI`). If this fails, nothing is touched and
   `False` is returned.
2. With mmap enabled: both mirrors are closed and trust in the live mirror is
   revoked. Then
   - if a temp mirror file exists, it is moved over the live one with `os.replace`
     (atomic within a filesystem) together with its marker, and the live mirror is
     reopened;
   - if no temp mirror exists on this host (the temp filter was built elsewhere or
     without mmap), the stale live mirror is deleted. With replication it reloads
     from the new snapshot on the next lookup; without, lookups use Redis until the
     filter is rebuilt here.

### `swap` is not a cross-system transaction

Redis and the local filesystem are two independent systems; no single atomic
operation spans both. The steps are ordered so that a failure never leaves a
stale mirror answering negatives: trust is revoked before any local file changes.
If the local step fails, `swap` logs it and returns `False`, and lookups fall back
to Redis until the next `rebuild`.

## Failure modes

| Failure | Behaviour |
| --- | --- |
| mmap file missing | Created atomically with the requested parameters. |
| mmap file smaller than 16 bytes | Reinitialised with the requested parameters, with a warning. |
| mmap file has corrupt/implausible header | Raise `BloomFilterFileError`. |
| mmap file shorter than its header's bit array | Raise `BloomFilterFileError`. |
| `error_rate` needs more than 128 hash functions | Raise `ValueError` at construction. |
| Local mirror cannot be opened | Log a warning, fall back to Redis for every lookup. |
| Local mirror read error | Log a warning, fall back to Redis for that lookup. |
| Local mirror not trusted | Fall back to Redis for every lookup, one-time warning. |
| Catch-up with the change log fails | Fall back to Redis until a catch-up succeeds (retried once per `sync_interval`). |
| Snapshot missing (filter not replicated, or log cap reached) | Fall back to Redis until the filter is rebuilt. |
| Redis `BF.RESERVE` fails (already exists) | Treated as success; mirror trust unchanged. |
| Redis `BF.RESERVE` fails (connection/other) | Return `False`, log a warning. |
| Redis write (script / `BF.ADD` / `BF.MADD`) fails | Return `False`, log a warning; the local mirror is ahead of Redis (only extra false positives). |
| Redis `BF.EXISTS`/`BF.MEXISTS` fails | Return a conservative `True`, log a warning. |
| Redis unavailable at all | `BloomFilterService` methods degrade to logged `False`/`True` fallbacks; the standalone `BloomFilter` is unaffected. |

The `exists` failure policy is deliberately conservative: when Redis is unreachable
the service returns `True` rather than a potentially incorrect `False`, because
`False` short-circuits the caller's downstream logic as a definite negative.

## Service locks

`acquire_lock`/`release_lock` provide advisory, TTL-based coordination for
rebuild/rotation. `acquire_lock` stores a random token (`SET lock:<name> <token>
NX EX ttl`). `release_lock` deletes the key only if it still holds that token
(compare-and-delete in a Lua script), so a service can never release a lock that
expired and was taken by someone else. There is no fencing: a lock can still
expire while its holder is working, so pick a TTL longer than your rebuild.

Each `BloomFilterService` instance has its own mirror cache and thread lock;
instances do not share state other than files on disk and keys in Redis.

## Security notes

- Filter names are turned into file names inside `mmap_dir`: names made only of
  `[A-Za-z0-9_.-]` are used as-is. In other names those characters become `_` and
  a short hash of the original name is appended, so names cannot escape
  `mmap_dir` and two different names (`a/b`, `a_b`) never share a file.
- Untrusted/malformed mmap files are rejected by header validation; the package
  never evaluates or deserialises untrusted content.
- Redis is configured and supplied by the caller (connection details, timeouts,
  auth); Bloomsieve never holds connection secrets.

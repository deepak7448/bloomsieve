"""Optional RedisBloom-backed service with a local mmap pre-filter.

The :class:`BloomFilterService` combines RedisBloom with an optional local
memory-mapped Bloom filter.  The local filter acts as a cheap pre-filter:
a definite-negative answer short-circuits a Redis round-trip, while a possible
positive falls back to RedisBloom for verification.

With ``replicate=True`` (the default) every write is also recorded in a
per-filter change log in Redis, and each host's local mirror catches up on that
log before it answers a negative.  That keeps mirrors on *different machines*
correct; see :class:`BloomFilterService` for the exact guarantee.

The ``redis`` package is only required when this module is used; the core
:class:`bloomsieve.BloomFilter` has no dependencies.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
import time
import uuid
from collections.abc import Iterable, Iterator
from types import TracebackType
from typing import Any

from .core import BloomFilter, _publish_file
from .utils import get_optimal_m_k

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows has no fcntl
    fcntl = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

_SAFE_FILENAME = re.compile(r"[^A-Za-z0-9_.-]")
_TRUST_SUFFIX = ".synced"
_CHUNK_SIZE = 1000
_READ_BATCH = 500  # log entries per XREAD (each entry holds up to _CHUNK_SIZE items)
_MARKER_EVERY = 10.0  # seconds between persisting the replication cursor
_ZERO_ID = "0-0"

# Delete the lock key only while it still holds our token (compare-and-delete).
_RELEASE_LOCK_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""

# Write items: append them to the change log (only for filters that have a
# replication snapshot), then BF.MADD them.  The log entry is written first, so a
# failure can only leave an item in the log that is missing from the filter
# (an extra false positive on mirrors), never the other way round.
# KEYS: filter, snapshot, log.  ARGV: max log entries, items...
_WRITE_SCRIPT = """
local id = ''
local dropped = 0
if redis.call('EXISTS', KEYS[2]) == 1 then
    if redis.call('XLEN', KEYS[3]) >= tonumber(ARGV[1]) then
        -- Nobody is compacting the log: stop replicating this filter.  Mirrors
        -- see the snapshot disappear and fall back to Redis.
        redis.call('DEL', KEYS[2], KEYS[3])
        dropped = 1
    else
        local fields = {}
        for i = 2, #ARGV do
            fields[#fields + 1] = ARGV[i]
            fields[#fields + 1] = ''
        end
        id = redis.call('XADD', KEYS[3], '*', unpack(fields))
    end
end
redis.call('BF.MADD', KEYS[1], unpack(ARGV, 2))
return {id, dropped}
"""

# Create the replication snapshot of a new generation unless one exists.
# KEYS: snapshot.  ARGV: generation, m, k.
_CREATE_SNAPSHOT_SCRIPT = """
if redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end
redis.call('HSET', KEYS[1], 'gen', ARGV[1], 'm', ARGV[2], 'k', ARGV[3], 'cursor', '0-0', 'data', '')
return 1
"""

# Publish a newer snapshot and drop the log entries it covers.
# KEYS: snapshot, log.  ARGV: generation, cursor, bit array.
_COMPACT_SCRIPT = """
if redis.call('HGET', KEYS[1], 'gen') ~= ARGV[1] then
    return 0
end
local function parse(id)
    local ms, seq = string.match(id, '^(%d+)-(%d+)$')
    return tonumber(ms), tonumber(seq)
end
local a_ms, a_seq = parse(redis.call('HGET', KEYS[1], 'cursor'))
local b_ms, b_seq = parse(ARGV[2])
if b_ms < a_ms or (b_ms == a_ms and b_seq <= a_seq) then
    return 0
end
redis.call('HSET', KEYS[1], 'cursor', ARGV[2], 'data', ARGV[3])
redis.call('XTRIM', KEYS[2], 'MINID', ARGV[2])
return 1
"""


class _Mirror:
    """A cached local mirror and its trust / replication state (per process)."""

    __slots__ = (
        "bf",
        "identity",
        "file_id",
        "checked_at",
        "trusted",
        "generation",
        "cursor",
        "synced_at",
        "sync_attempt_at",
        "marker_written_at",
    )

    def __init__(self, bf: BloomFilter, identity: str, file_id: str) -> None:
        self.bf = bf
        # ``identity`` (device and inode) detects that the path now points at a
        # different file; it is only compared while this process holds the old file
        # open, so the inode cannot have been reused.  ``file_id`` (random, stored in
        # the file) ties a trust marker to one specific file, even across restarts.
        self.identity = identity
        self.file_id = file_id
        self.checked_at = time.monotonic()
        self.trusted = False
        self.generation: str | None = None
        self.cursor = _ZERO_ID
        self.synced_at = float("-inf")
        self.sync_attempt_at = float("-inf")
        self.marker_written_at = float("-inf")


def _file_identity(st: os.stat_result) -> str:
    return f"{st.st_dev}:{st.st_ino}"


def _text(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _stream_id(value: Any) -> tuple[int, int]:
    ms, _, seq = _text(value).partition("-")
    return int(ms), int(seq or 0)


def _chunks(items: Iterable[Any], size: int) -> Iterator[list[Any]]:
    chunk: list[Any] = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _has_hash_tag(name: str) -> bool:
    start = name.find("{")
    if start == -1:
        return False
    end = name.find("}", start + 1)
    return end > start + 1


class BloomFilterService:
    """A generic, reusable Redis Bloom filter service.

    Uses the native RedisBloom module commands (``BF.RESERVE``, ``BF.MADD``,
    ``BF.EXISTS``, ``BF.MEXISTS``) and an optional memory-mapped (``mmap``) local
    pre-filter.

    Arguments:
        redis_client: A ``redis.Redis`` (or compatible) client.  With
            ``replicate=True`` it must return bytes (``decode_responses=False``,
            the redis-py default).
        capacity, error_rate, expansion: Defaults for ``BF.RESERVE`` and for the
            size of local mirrors.
        use_mmap: Enable the local pre-filter on this host.
        mmap_dir: Directory holding local mirror files (default ``./bloom_filters``).
        replicate: Record every write in a per-filter change log in Redis so that
            local mirrors on *other* hosts learn about it (default ``True``).  Turn
            it off only if all writers of a filter share one mirror directory, or
            no host uses ``use_mmap``.
        sync_interval: Upper bound, in seconds, on how stale a local negative can
            be.  A mirror catches up with Redis before answering a negative if its
            last catch-up is older than this.  ``0`` means every lookup checks
            Redis first (always exact, but no faster than plain Redis).
        max_log_entries: Safety cap on the change log.  Mirrors compact the log
            automatically; if it still reaches this length (no mirror is running),
            replication of that filter stops and mirrors fall back to Redis until
            the filter is rebuilt.

    Consistency guarantee (``use_mmap=True``):
        ``exists`` returns ``False`` without asking Redis only when the local mirror
        has caught up with the filter's change log within the last
        ``sync_interval`` seconds.  So a local "absent" can only be wrong for an
        item added on *another* host less than ``sync_interval`` seconds ago.
        Writes made through the same mirror file are visible at once.  If the
        mirror cannot catch up (Redis unreachable, log missing or replication
        disabled), it stops answering negatives and every lookup goes to Redis.

        With ``replicate=False`` the mirror only sees writes made through services
        that share its file, so every writer must use the same ``mmap_dir`` (one
        host or a shared volume).

        The change log only contains writes made through ``BloomFilterService``
        (any host, with or without mmap).  Items added to the Redis filter by other
        means are invisible to mirrors.

    Redis requirements:
        Bloom commands (Redis 8, Redis Stack or RedisBloom) and Redis >= 6.2 for
        replication.  In Redis Cluster the filter, its change log and its snapshot
        share a hash slot automatically, unless the name contains ``}`` without a
        matching ``{``.

    Locking:
        ``acquire_lock``/``release_lock`` provide advisory TTL-based coordination for
        rebuild/rotation operations.  A lock is released only by the service that
        acquired it (ownership token), but there is no fencing: a lock can expire
        while its holder is still working.
    """

    def __init__(
        self,
        redis_client: Any,
        capacity: int = 1000000,
        error_rate: float = 0.001,
        expansion: int = 2,
        use_mmap: bool = False,
        mmap_dir: str | None = None,
        replicate: bool = True,
        sync_interval: float = 0.5,
        max_log_entries: int = 1_000_000,
    ):
        self.redis = redis_client
        self.capacity = capacity
        self.error_rate = error_rate
        self.expansion = expansion
        self.use_mmap = use_mmap
        self.mmap_dir = mmap_dir or os.path.join(os.getcwd(), "bloom_filters")
        self.replicate = replicate
        self.sync_interval = sync_interval
        self.max_log_entries = max_log_entries

        if replicate:
            pool_kwargs = getattr(getattr(redis_client, "connection_pool", None), "connection_kwargs", {})
            if isinstance(pool_kwargs, dict) and pool_kwargs.get("decode_responses") is True:
                raise ValueError("replicate=True needs a Redis client with decode_responses=False")

        self._lock = threading.RLock()
        self._mirrors: dict[str, _Mirror] = {}
        self._cold_warned: set[str] = set()
        self._lock_tokens: dict[str, str] = {}
        self._write_script = self.redis.register_script(_WRITE_SCRIPT) if replicate else None
        self._create_snapshot_script = self.redis.register_script(_CREATE_SNAPSHOT_SCRIPT) if replicate else None
        self._compact_script = self.redis.register_script(_COMPACT_SCRIPT) if replicate else None

    # ------------------------------------------------------------------ naming

    def _mmap_name(self, name: str) -> str:
        """Return a filesystem-safe filename for a filter name.

        Names made only of ``[A-Za-z0-9_.-]`` are used as-is.  Other names are
        sanitised and suffixed with a short hash of the original name so that two
        different names (``a/b`` and ``a_b``) never share a file.
        """
        safe = _SAFE_FILENAME.sub("_", name)
        if safe != name:
            safe = f"{safe}-{hashlib.sha1(name.encode('utf-8')).hexdigest()[:8]}"
        return safe

    def _mmap_path(self, name: str) -> str:
        return os.path.join(self.mmap_dir, f"{self._mmap_name(name)}.bloom")

    def _redis_key(self, name: str, kind: str) -> str:
        # Keep the key in the filter's Redis Cluster hash slot: reuse the name's own
        # hash tag if it has one, otherwise make the whole name the tag (a key's
        # slot is the slot of its tag, and an untagged key hashes as a whole).
        if _has_hash_tag(name):
            return f"bloomsieve:{kind}:{name}"
        return f"bloomsieve:{{{name}}}:{kind}"

    def _snapshot_key(self, name: str) -> str:
        return self._redis_key(name, "snap")

    def _log_key(self, name: str) -> str:
        return self._redis_key(name, "log")

    # ------------------------------------------------------------------ trust markers

    def _read_marker(self, name: str, file_id: str) -> dict[str, Any] | None:
        """Return the marker for ``name`` if it belongs to the file with ``file_id``."""
        try:
            with open(self._mmap_path(name) + _TRUST_SUFFIX, encoding="utf-8") as fh:
                marker = json.load(fh)
        except (OSError, ValueError):
            return None
        if not isinstance(marker, dict) or marker.get("file_id") != file_id:
            return None
        return marker

    def _write_marker(self, name: str, entry: _Mirror) -> bool:
        """Persist the mirror's state; flushes the mirror first so the marker never runs ahead."""
        marker_path = self._mmap_path(name) + _TRUST_SUFFIX
        marker: dict[str, Any] = {"file_id": entry.file_id}
        if self.replicate:
            if entry.generation is None:
                return False
            marker.update(generation=entry.generation, cursor=entry.cursor)
        try:
            entry.bf.flush()
            tmp = f"{marker_path}.{os.getpid()}.{uuid.uuid4().hex[:12]}.tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(marker, fh)
            os.replace(tmp, marker_path)
        except (OSError, RuntimeError, ValueError) as exc:
            logger.warning("failed to persist local mirror state for %s: %s", name, exc)
            return False
        entry.marker_written_at = time.monotonic()
        return True

    def _remove_quietly(self, path: str) -> None:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            logger.warning("failed to remove %s: %s", path, exc)

    # ------------------------------------------------------------------ mirror cache

    def _open_mirror(self, name: str, capacity: int | None, error_rate: float | None) -> _Mirror | None:
        filepath = self._mmap_path(name)
        cap = capacity if capacity is not None else self.capacity
        err = error_rate if error_rate is not None else self.error_rate
        try:
            bf = BloomFilter(capacity=cap, error_rate=err, filepath=filepath)
            identity = _file_identity(os.fstat(bf._file.fileno()))  # type: ignore[union-attr]
            file_id = bf._ensure_file_id()
        except Exception as exc:
            logger.warning("failed to open local mmap filter %s at %s: %s", name, filepath, exc)
            return None

        entry = _Mirror(bf, identity, file_id)
        marker = self._read_marker(name, file_id)
        if self.replicate:
            if marker is not None and isinstance(marker.get("generation"), str):
                entry.generation = marker["generation"]
                entry.cursor = str(marker.get("cursor") or _ZERO_ID)
        else:
            entry.trusted = marker is not None
        bf.synced = entry.trusted
        self._mirrors[name] = entry
        if bf.newly_created:
            logger.info("created local Bloom filter file %s (name=%s)", filepath, name)
        return entry

    def _entry(self, name: str, capacity: int | None = None, error_rate: float | None = None) -> _Mirror | None:
        """Return the cached mirror entry, (re)opening it when needed."""
        if not self.use_mmap:
            return None

        entry = self._mirrors.get(name)
        if entry is not None and time.monotonic() - entry.checked_at < self.sync_interval:
            return entry

        with self._lock:
            entry = self._mirrors.get(name)
            if entry is not None:
                try:
                    current = _file_identity(os.stat(self._mmap_path(name)))
                except OSError:
                    current = None
                if current == entry.identity:
                    if not self.replicate:
                        entry.trusted = self._read_marker(name, entry.file_id) is not None
                        entry.bf.synced = entry.trusted
                    entry.checked_at = time.monotonic()
                    return entry
                logger.info("local mirror %s was replaced on disk; reopening it", self._mmap_path(name))
                self._close_mmap(name)
            return self._open_mirror(name, capacity, error_rate)

    def _init_mmap(
        self,
        name: str,
        capacity: int | None = None,
        error_rate: float | None = None,
    ) -> BloomFilter | None:
        """Return the local mmap filter for ``name``, creating it on demand.

        Returns ``None`` when mmap is disabled or the local filter could not be
        opened; callers then fall back to Redis.  Failures are logged.
        """
        entry = self._entry(name, capacity, error_rate)
        return entry.bf if entry is not None else None

    def _close_mmap(self, name: str) -> None:
        with self._lock:
            entry = self._mirrors.pop(name, None)
            if entry is None:
                return
            if self.replicate and entry.trusted:
                self._write_marker(name, entry)
            try:
                entry.bf.close()
            except Exception as exc:
                logger.warning("failed to close local mmap filter %s: %s", name, exc)

    def _discard_local(self, name: str) -> None:
        """Close the mirror and delete its file and trust marker."""
        with self._lock:
            entry = self._mirrors.pop(name, None)
            if entry is not None:
                entry.bf.close()
            path = self._mmap_path(name)
            # Revoke trust first so other processes stop trusting the old file.
            self._remove_quietly(path + _TRUST_SUFFIX)
            self._remove_quietly(path)

    def _fresh_entry(self, name: str) -> _Mirror | None:
        """Return the mirror entry, catching it up with Redis first if a sync is due.

        Returns ``None`` when the mirror exists but could not be brought up to date.
        """
        entry = self._entry(name)
        if entry is None or not self.replicate:
            return entry
        now = time.monotonic()
        if now - entry.synced_at < self.sync_interval:
            return entry
        # A trusted mirror re-syncs whenever it is stale; a mirror that could not
        # sync (no snapshot, Redis down) retries once per interval and meanwhile
        # leaves every lookup to Redis.
        if not (entry.trusted or now - entry.sync_attempt_at >= self.sync_interval):
            return None
        if not self._sync(name):
            return None
        return self._mirrors.get(name)

    def _usable_mirror(self, name: str) -> BloomFilter | None:
        """Return the mirror if it may answer definite negatives right now, else ``None``."""
        if not self.use_mmap:
            return None
        entry = self._fresh_entry(name)
        if entry is None:
            current = self._mirrors.get(name)
            if current is None or current.trusted or not self.replicate:
                return None
            entry = current  # fall through to the one-time "not trusted" warning
        if not entry.trusted:
            if name not in self._cold_warned:
                self._cold_warned.add(name)
                logger.warning(
                    "local mirror for %s is not known to hold every item in Redis; lookups fall back to Redis%s",
                    name,
                    " (no replication snapshot: rebuild the filter to enable it)"
                    if self.replicate
                    else " until it is rebuilt (rebuild/swap/mark_synced)",
                )
            return None
        return entry.bf

    def _sync_after_write(self, name: str) -> None:
        # Hosts with a mirror also sync (and therefore compact the change log) on
        # the write path, so a write-heavy, lookup-light filter cannot fill the log.
        if self.use_mmap and self.replicate:
            try:
                self._fresh_entry(name)
            except Exception as exc:  # pragma: no cover - _sync already logs and absorbs errors
                logger.warning("failed to sync local mirror %s after a write: %s", name, exc)

    # ------------------------------------------------------------------ replication

    def _sync(self, name: str) -> bool:
        """Catch the local mirror up with the filter's change log in Redis.

        Returns ``True`` when the mirror is now trusted and up to date.
        """
        with self._lock:
            entry = self._entry(name)
            if entry is None:
                return False
            started = time.monotonic()
            if entry.trusted and started - entry.synced_at < self.sync_interval:
                return True  # another thread synced while we waited for the lock
            entry.sync_attempt_at = started
            try:
                caught_up = self._catch_up(name, entry)
            except Exception as exc:
                logger.warning("failed to sync local mirror %s with Redis (falling back to Redis): %s", name, exc)
                current = self._mirrors.get(name)
                if current is not None:
                    current.trusted = current.bf.synced = False
                    current.sync_attempt_at = started
                return False
            if caught_up is None:
                return False
            caught_up.sync_attempt_at = started
            if not caught_up.trusted:
                return False
            caught_up.synced_at = started
            if started - caught_up.marker_written_at >= _MARKER_EVERY:
                self._write_marker(name, caught_up)
            return True

    def _catch_up(self, name: str, entry: _Mirror) -> _Mirror | None:
        snap_key, log_key = self._snapshot_key(name), self._log_key(name)
        while True:
            pipe = self.redis.pipeline(transaction=True)
            pipe.hmget(snap_key, "gen", "cursor")
            pipe.xread({log_key: entry.cursor}, count=_READ_BATCH)
            pipe.xlen(log_key)
            (generation, snap_cursor), streams, log_len = pipe.execute()

            if generation is None:
                # No snapshot: the filter is not replicated (never created through a
                # replicating service, or replication was dropped).
                entry.trusted = entry.bf.synced = False
                entry.generation = None
                return entry

            generation = _text(generation)
            if generation != entry.generation or _stream_id(snap_cursor) > _stream_id(entry.cursor):
                # New generation (rebuild/swap elsewhere), or the log was compacted
                # past our position: start over from the snapshot.
                replaced = self._bootstrap(name, entry)
                if replaced is None:
                    return None
                entry = replaced
                continue

            entries = []
            for _, stream_entries in streams.items() if isinstance(streams, dict) else streams or ():
                entries.extend(stream_entries)
            if entries:
                items = [field for _, fields in entries for field in fields]
                entry.bf.add_many(items)
                entry.cursor = _text(entries[-1][0])
            if len(entries) < _READ_BATCH:
                break

        entry.trusted = entry.bf.synced = True
        threshold = min(max(10_000, entry.bf.byte_size // 64), max(1, self.max_log_entries // 2))
        if log_len >= threshold:
            self._compact(name, entry)
        return entry

    def _bootstrap(self, name: str, entry: _Mirror) -> _Mirror | None:
        """Make the local mirror equal to the filter's current Redis snapshot."""
        generation, m, k, cursor, data = self.redis.hmget(self._snapshot_key(name), "gen", "m", "k", "cursor", "data")
        if generation is None:
            entry.generation = None
            return entry
        generation, m, k, cursor = _text(generation), int(m), int(k), _text(cursor)
        data = data or b""

        path = self._mmap_path(name)
        lock_fh = open(path + ".lock", "a+b")
        try:
            if fcntl is not None:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)  # one bootstrap per host at a time

            # Another process on this host may have bootstrapped the file already.
            self._close_mmap(name)
            fresh = self._open_mirror(name, None, None)
            if fresh is None:
                return None
            if fresh.generation == generation and (fresh.bf.m, fresh.bf.k) == (m, k):
                if data and _stream_id(cursor) > _stream_id(fresh.cursor):
                    fresh.bf.merge_bytes(data)  # fill the gap the compaction trimmed
                    fresh.cursor = cursor
                return fresh

            if generation != fresh.generation or (fresh.bf.m, fresh.bf.k) != (m, k):
                logger.info("loading local mirror %s from the Redis snapshot (generation %s)", name, generation)
                self._remove_quietly(path + _TRUST_SUFFIX)
                self._mirrors.pop(name, None)
                fresh.bf.close()
                _publish_file(path, m, k, replace=True)
                fresh = self._open_mirror(name, None, None)
                if fresh is None:
                    return None
            if data:
                fresh.bf.merge_bytes(data)
            fresh.generation, fresh.cursor = generation, cursor
            self._write_marker(name, fresh)
            return fresh
        finally:
            lock_fh.close()

    def _compact(self, name: str, entry: _Mirror) -> None:
        """Publish this up-to-date mirror as the new snapshot and trim the log."""
        try:
            done = self._compact_script(
                keys=[self._snapshot_key(name), self._log_key(name)],
                args=[entry.generation, entry.cursor, entry.bf.to_bytes()],
            )
        except Exception as exc:
            logger.warning("failed to compact the change log of %s: %s", name, exc)
            return
        if done:
            logger.info("compacted the change log of %s at %s", name, entry.cursor)

    def _write_items(self, name: str, items: list[str | bytes]) -> Any:
        """Write items to Redis (change log + filter); returns the log entry id or None."""
        if not self.replicate:
            self.redis.execute_command("BF.MADD", name, *items)
            return None
        entry_id, dropped = self._write_script(
            keys=[name, self._snapshot_key(name), self._log_key(name)],
            args=[self.max_log_entries, *items],
        )
        if dropped:
            logger.error(
                "change log of %s reached %d entries without being compacted; replication is disabled "
                "for it and local mirrors fall back to Redis until it is rebuilt",
                name,
                self.max_log_entries,
            )
        return entry_id or None

    def _create_snapshot(self, name: str, capacity: int, error_rate: float) -> bool:
        m, k = get_optimal_m_k(capacity, error_rate)
        try:
            return bool(self._create_snapshot_script(keys=[self._snapshot_key(name)], args=[uuid.uuid4().hex, m, k]))
        except Exception as exc:
            logger.warning("failed to create the replication snapshot of %s: %s", name, exc)
            return False

    # ------------------------------------------------------------------ housekeeping

    def flush(self, name: str | None = None) -> None:
        """Persist local mmap mirrors to durable storage.

        With ``name`` set only that mirror is flushed; otherwise every cached
        mirror is flushed.  No-op when mmap is disabled.
        """
        with self._lock:
            names = [name] if name is not None else list(self._mirrors)
            for filter_name in names:
                entry = self._mirrors.get(filter_name)
                if entry is None:
                    continue
                try:
                    entry.bf.flush()
                    if self.replicate and entry.trusted:
                        self._write_marker(filter_name, entry)
                except Exception as exc:
                    logger.warning("failed to flush local mmap filter %s: %s", filter_name, exc)

    def close(self, name: str | None = None) -> None:
        """Flush and close local mirrors (one, or all of them).

        The service stays usable; mirrors are reopened on the next call that needs
        them.  Files on disk are kept.
        """
        with self._lock:
            for filter_name in [name] if name is not None else list(self._mirrors):
                self._close_mmap(filter_name)

    def __enter__(self) -> BloomFilterService:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def mark_synced(self, name: str) -> bool:
        """Declare that the local mirror of ``name`` holds every item in Redis.

        Only for ``replicate=False`` (with replication, mirrors sync themselves;
        use :meth:`rebuild` to bring a filter under replication).  Only call this
        when you know it is true, for example after populating the mirror and
        Redis from the same source: marking an incomplete mirror as synced produces
        false negatives.  Returns ``False`` if it does not apply or failed.
        """
        if self.replicate:
            logger.warning("mark_synced has no effect with replicate=True; use rebuild() instead")
            return False
        entry = self._entry(name)
        if entry is None:
            return False
        with self._lock:
            if not self._write_marker(name, entry):
                return False
            entry.trusted = entry.bf.synced = True
            return True

    # ------------------------------------------------------------------ local ops

    def _local_add_many(self, name: str, items: list[str | bytes]) -> None:
        # One retry covers a mirror that was swapped out underneath us (it gets
        # closed and reopened); losing a local write would cause false negatives.
        for _ in range(2):
            bf = self._init_mmap(name)
            if bf is None:
                return
            try:
                bf.add_many(items)
                return
            except RuntimeError:
                continue  # closed concurrently: reopen and retry
            except Exception as exc:
                logger.warning("failed to add items to local mmap filter %s: %s", name, exc)
                return
        logger.warning("failed to add items to local mmap filter %s: mirror kept closing", name)

    def _local_add(self, name: str, item: str | bytes) -> None:
        self._local_add_many(name, [item])

    # ------------------------------------------------------------------ public API

    def create_filter(
        self,
        name: str,
        capacity: int | None = None,
        error_rate: float | None = None,
    ) -> bool:
        """Reserve a new Bloom filter in Redis using ``BF.RESERVE``.

        Returns ``True`` if the filter was created or already existed, ``False`` on
        an unrecoverable error.  A brand-new filter is put under replication (with
        ``replicate=True``) and its local mirror is trusted straight away, because
        an empty filter cannot hold anything the mirror lacks.  An existing
        filter is left as it is.
        """
        cap = capacity if capacity is not None else self.capacity
        err = error_rate if error_rate is not None else self.error_rate

        try:
            self.redis.execute_command("BF.RESERVE", name, str(err), str(cap), "EXPANSION", str(self.expansion))
        except Exception as exc:
            message = str(exc)
            if "item exists" in message or "already exists" in message:
                if self.use_mmap:
                    self._init_mmap(name, cap, err)
                return True
            logger.warning("BF.RESERVE failed for %s: %s", name, message)
            return False

        if self.replicate:
            self._create_snapshot(name, cap, err)
        if self.use_mmap:
            entry = self._entry(name, cap, err)
            if entry is not None and not self.replicate:
                with self._lock:
                    if self._write_marker(name, entry):
                        entry.trusted = entry.bf.synced = True
        return True

    # Backwards-compatible alias for the previous camelCase public name.
    createFilter = create_filter

    def add(self, name: str, item: str | bytes) -> bool:
        """Add an item to the filter (local mirror first, then Redis).

        Returns ``True`` when Redis accepted the write.
        """
        if self.use_mmap:
            self._local_add(name, item)
        try:
            if self.replicate:
                self._write_items(name, [item])
            else:
                self.redis.execute_command("BF.ADD", name, item)
        except Exception as exc:
            logger.warning("BF.ADD failed for %s: %s", name, exc)
            return False
        self._sync_after_write(name)
        return True

    def add_many(self, name: str, items: Iterable[str | bytes]) -> bool:
        """Add many items, sending all chunks to Redis in one round trip.

        Returns ``True`` when Redis accepted every chunk.
        """
        items = list(items)
        if not items:
            return True
        if self.use_mmap:
            self._local_add_many(name, items)
        try:
            pipe = self.redis.pipeline(transaction=False)
            for chunk in _chunks(items, _CHUNK_SIZE):
                if self.replicate:
                    self._write_script(
                        keys=[name, self._snapshot_key(name), self._log_key(name)],
                        args=[self.max_log_entries, *chunk],
                        client=pipe,
                    )
                else:
                    pipe.execute_command("BF.MADD", name, *chunk)
            for result in pipe.execute():
                if self.replicate and result and result[1]:
                    logger.error("change log of %s overflowed; replication is disabled until rebuild", name)
        except Exception as exc:
            logger.warning("BF.MADD failed for %s: %s", name, exc)
            return False
        self._sync_after_write(name)
        return True

    def exists(self, name: str, item: str | bytes) -> bool:
        """Check whether an item is present.

        With a usable local mirror, a definite-negative local answer returns
        ``False`` without a Redis request.  Everything else (possible positive,
        mirror not caught up, or mmap disabled) verifies against RedisBloom.  If
        Redis is unreachable the method falls back to a conservative ``True`` and
        logs a warning.
        """
        if self.use_mmap:
            bf = self._usable_mirror(name)
            if bf is not None:
                try:
                    if item not in bf:
                        return False
                except Exception as exc:
                    logger.warning("failed to read local mmap filter %s: %s", name, exc)
        try:
            return bool(self.redis.execute_command("BF.EXISTS", name, item))
        except Exception as exc:
            logger.warning("BF.EXISTS failed for %s (returning a conservative True): %s", name, exc)
            return True

    def exists_many(self, name: str, items: Iterable[str | bytes]) -> list[bool]:
        """Check many items; one result per item, in order.

        Local definite negatives are answered without Redis; all remaining items are
        verified with ``BF.MEXISTS`` in a single round trip.  On a Redis failure the
        unverified items are reported as a conservative ``True``.
        """
        items = list(items)
        results = [True] * len(items)
        pending = list(range(len(items)))

        bf = self._usable_mirror(name) if self.use_mmap else None
        if bf is not None:
            try:
                maybe = [item in bf for item in items]
            except Exception as exc:
                logger.warning("failed to read local mmap filter %s: %s", name, exc)
            else:
                pending = [i for i, hit in enumerate(maybe) if hit]
                results = maybe  # local False is final; local True is re-checked below

        if not pending:
            return results
        try:
            pipe = self.redis.pipeline(transaction=False)
            index_chunks = list(_chunks(pending, _CHUNK_SIZE))
            for chunk in index_chunks:
                pipe.execute_command("BF.MEXISTS", name, *(items[i] for i in chunk))
            for chunk, answers in zip(index_chunks, pipe.execute()):
                for i, answer in zip(chunk, answers):
                    results[i] = bool(answer)
        except Exception as exc:
            logger.warning("BF.MEXISTS failed for %s (returning a conservative True): %s", name, exc)
        return results

    def rebuild(
        self,
        name: str,
        items: Iterable[str | bytes] | None,
        capacity: int | None = None,
        error_rate: float | None = None,
    ) -> bool:
        """Re-create a filter with a fresh set of items.

        The existing Redis filter (and its change log) is deleted, the local mirror
        is discarded, a new filter is reserved and populated in chunks (local
        mirror first, then Redis).  ``None`` items are skipped.  With
        ``replicate=True`` the result is published as a snapshot that mirrors on
        other hosts download on their next sync.

        Rebuilding a filter in place leaves it empty while it is being refilled; to
        keep serving reads, rebuild under a temporary name and :meth:`swap` it in.

        On any Redis error during population the method returns ``False`` after
        logging the failure; the partially populated filter is left in place.
        """
        try:
            if self.replicate:
                self.redis.delete(name, self._snapshot_key(name), self._log_key(name))
            else:
                self.redis.delete(name)
        except Exception as exc:
            logger.warning("failed to delete Redis filter %s during rebuild: %s", name, exc)

        if self.use_mmap:
            self._discard_local(name)

        if not self.create_filter(name, capacity, error_rate):
            logger.error("rebuild of %s failed: could not reserve the Redis filter", name)
            return False

        entry = self._entry(name, capacity, error_rate) if self.use_mmap else None
        written = 0
        last_id = None
        for chunk in _chunks((item for item in items or () if item is not None), _CHUNK_SIZE):
            if self.use_mmap:
                self._local_add_many(name, chunk)
            try:
                last_id = self._write_items(name, chunk) or last_id
                written += 1
            except Exception as exc:
                logger.error("rebuild of %s failed while inserting a chunk: %s", name, exc)
                return False

        if entry is not None and self.replicate:
            self._adopt_rebuild(name, written, last_id)
        elif self.use_mmap:
            self.flush(name)
        return True

    def _adopt_rebuild(self, name: str, written: int, last_id: Any) -> None:
        """After a rebuild, skip replaying our own log entries and publish a snapshot."""
        with self._lock:
            entry = self._entry(name)
            if entry is None:
                return
            try:
                generation, snap_cursor = self.redis.hmget(self._snapshot_key(name), "gen", "cursor")
                log_len = self.redis.xlen(self._log_key(name))
            except Exception as exc:
                logger.warning("could not finish publishing rebuild of %s: %s", name, exc)
                return
            if generation is None:
                return
            entry.generation = _text(generation)
            # Only this rebuild wrote to the fresh log: the mirror already holds every
            # entry, so it can start after the last one instead of replaying them.
            if last_id is not None and log_len == written and _text(snap_cursor) == _ZERO_ID:
                entry.cursor = _text(last_id)
                entry.trusted = entry.bf.synced = True
                entry.synced_at = time.monotonic()
                self._compact(name, entry)
                self._write_marker(name, entry)
            else:
                entry.synced_at = float("-inf")  # replay on the next lookup

    def delete_filter(self, name: str) -> bool:
        """Delete a filter everywhere: in Redis (with its change log and snapshot) and its local mirror.

        Returns ``False`` if the Redis delete failed (the local mirror is removed anyway).
        """
        if self.use_mmap:
            self._discard_local(name)
        try:
            self.redis.delete(name, self._snapshot_key(name), self._log_key(name))
            return True
        except Exception as exc:
            logger.warning("failed to delete Redis filter %s: %s", name, exc)
            return False

    def get_info(self, name: str) -> dict[str, Any]:
        """Return ``{"capacity": int, "inserted": int, "ratio": float}`` for a filter.

        A missing filter reports ``ratio`` 1.0 (so "rebuild when ratio is high"
        logic recreates it); any other Redis error reports 0.0.
        """
        try:
            info = self.redis.execute_command("BF.INFO", name)
        except Exception as exc:
            message = str(exc)
            if "not found" in message or "no such key" in message:
                return {"capacity": 0, "inserted": 0, "ratio": 1.0}
            logger.warning("BF.INFO failed for %s: %s", name, message)
            return {"capacity": 0, "inserted": 0, "ratio": 0.0}

        info_dict: dict[str, Any] = {}
        if isinstance(info, dict):
            info_dict = {k.decode("utf-8") if isinstance(k, bytes) else k: v for k, v in info.items()}
        elif isinstance(info, list):
            for i in range(0, len(info), 2):
                k = info[i]
                k_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                info_dict[k_str] = info[i + 1]

        inserted = info_dict.get("Number of items inserted") or info_dict.get("number of items inserted") or 0
        capacity = (
            info_dict.get("max_elements")
            or info_dict.get("maxElements")
            or info_dict.get("Capacity")
            or info_dict.get("capacity")
            or 1
        )
        return {
            "capacity": int(capacity),
            "inserted": int(inserted),
            "ratio": float(inserted) / float(capacity) if capacity else 0.0,
        }

    def load_ratio(self, name: str) -> float:
        """Return the fill ratio (``inserted / capacity``, 0.0-1.0)."""
        return self.get_info(name)["ratio"]

    def acquire_lock(self, lock_name: str, ttl: int = 600) -> bool:
        """Advisorily acquire a Redis lock (``SET NX EX`` with a random token)."""
        token = uuid.uuid4().hex
        try:
            acquired = bool(self.redis.set(f"lock:{lock_name}", token, nx=True, ex=ttl))
        except Exception as exc:
            logger.warning("failed to acquire lock %s: %s", lock_name, exc)
            return False
        if acquired:
            with self._lock:
                self._lock_tokens[lock_name] = token
        return acquired

    def release_lock(self, lock_name: str) -> bool:
        """Release a lock previously acquired by *this* service.

        Returns ``False`` if this service does not hold the lock, or if it expired
        and was taken by someone else in the meantime (their lock is left alone).
        """
        with self._lock:
            token = self._lock_tokens.pop(lock_name, None)
        if token is None:
            logger.warning("lock %s is not held by this service; not releasing it", lock_name)
            return False
        try:
            released = bool(self.redis.eval(_RELEASE_LOCK_SCRIPT, 1, f"lock:{lock_name}", token))
        except Exception as exc:
            logger.warning("failed to release lock %s: %s", lock_name, exc)
            return False
        if not released:
            logger.warning("lock %s had expired or was taken over before release", lock_name)
        return released

    def swap(self, temp_name: str, live_name: str) -> bool:
        """Point the live filter at a rebuilt temporary filter.

        Order of operations and recovery model:

        1. In Redis, rename ``temp_name`` to ``live_name``; with replication its
           change log and snapshot move with it in the same transaction.  If this
           fails, nothing is changed and ``False`` is returned.
        2. With mmap enabled, trust in the live mirror is revoked, then the temporary
           mirror file (and its marker) is moved over the live one with
           ``os.replace``.  If there is no temporary mirror on this host, the stale
           live mirror is deleted instead.

        With replication, mirrors on every host (including one without a temporary
        mirror) notice the new generation on their next sync and load it from the
        snapshot.  Without replication, a host without the temporary mirror falls
        back to Redis until the filter is rebuilt there.  A failure in step 2 never
        leaves a stale mirror answering negatives; the method returns ``False``.
        """
        try:
            if self.replicate:
                temp_snap, temp_log = self._snapshot_key(temp_name), self._log_key(temp_name)
                live_snap, live_log = self._snapshot_key(live_name), self._log_key(live_name)
                check = self.redis.pipeline(transaction=False)
                check.exists(temp_name)
                check.exists(temp_snap)
                check.exists(temp_log)
                has_filter, has_snap, has_log = check.execute()
                if not has_filter:
                    raise RuntimeError(f"no such key {temp_name!r}")
                pipe = self.redis.pipeline(transaction=True)
                pipe.rename(temp_name, live_name)
                if has_snap:
                    pipe.rename(temp_snap, live_snap)
                else:
                    pipe.delete(live_snap)
                if has_snap and has_log:
                    pipe.rename(temp_log, live_log)
                else:
                    pipe.delete(live_log)
                pipe.execute()
            else:
                self.redis.rename(temp_name, live_name)
        except Exception as exc:
            logger.error("Redis rename %s -> %s failed: %s", temp_name, live_name, exc)
            return False

        if not self.use_mmap:
            return True

        temp_path = self._mmap_path(temp_name)
        live_path = self._mmap_path(live_name)
        with self._lock:
            self._close_mmap(temp_name)
            self._close_mmap(live_name)
            self._remove_quietly(live_path + _TRUST_SUFFIX)

            if not os.path.exists(temp_path):
                logger.warning(
                    "no local mirror exists for %s; discarding the local mirror of %s%s",
                    temp_name,
                    live_name,
                    " (it reloads from Redis on the next lookup)" if self.replicate else " (lookups use Redis)",
                )
                self._remove_quietly(live_path)
                return True

            try:
                os.replace(temp_path, live_path)
            except OSError as exc:
                logger.error(
                    "local mmap swap %s -> %s failed after Redis rename succeeded: %s",
                    temp_path,
                    live_path,
                    exc,
                )
                return False
            logger.info("rotated local mmap %s -> %s", temp_path, live_path)

            result = True
            if os.path.exists(temp_path + _TRUST_SUFFIX):
                try:
                    os.replace(temp_path + _TRUST_SUFFIX, live_path + _TRUST_SUFFIX)
                except OSError as exc:
                    logger.error("failed to move trust marker for %s: %s", live_name, exc)
                    result = False
            if self._init_mmap(live_name) is None:
                logger.error("failed to reopen local mmap for %s after swap", live_name)
                result = False
            return result

"""Optional RedisBloom-backed service with a local mmap pre-filter.

The :class:`BloomFilterService` combines RedisBloom with an optional local
memory-mapped Bloom filter.  The local filter acts as a cheap pre-filter:
a definite-negative answer short-circuits a Redis round-trip, while a possible
positive falls back to RedisBloom for verification.

The ``redis`` package is only required when this module is used; the core
:class:`bloomsieve.BloomFilter` has no dependencies.
"""

from __future__ import annotations

import logging
import os
import re
import threading
from collections.abc import Iterator
from typing import Any, ClassVar

from .core import BloomFilter

logger = logging.getLogger(__name__)

_SAFE_FILENAME = re.compile(r"[^A-Za-z0-9_.-]")


class BloomFilterService:
    """A generic, reusable Redis Bloom filter service.

    Uses the native RedisBloom module commands ``BF.RESERVE``/``BF.ADD``/``BF.EXISTS``
    and an optional memory-mapped (``mmap``) local pre-filter.

    Consistency model:
        The local mmap filter mirrors items that were added through this service
        (``add``/``rebuild``).  A locally stored filter is only trusted for
        definite-negative answers once it has been populated through the service.
        A freshly created empty local file is treated as "unknown" and lookups fall
        back to Redis until the first item is added locally.

        Redis and the local filesystem are updated independently, so a partial
        failure between the two is possible.  This class does **not** provide a
        cross-system atomic transaction; see :meth:`swap` for the recovery model.

    Locking:
        ``acquire_lock``/``release_lock`` provide advisory TTL-based coordination for
        rebuild/rotation operations.  They are not a drop-in replacement for a
        consensus-grade distributed lock (no ownership token, no fencing).
    """

    # Thread-safe lock for modifying the mapping dictionary and resources.
    _lock = threading.RLock()
    _mmaps: ClassVar[dict[str, BloomFilter]] = {}  # Cache format: { name: BloomFilter }
    _cold_warned: ClassVar[set[str]] = set()

    def __init__(
        self,
        redis_client: Any,
        capacity: int = 1000000,
        error_rate: float = 0.001,
        expansion: int = 2,
        use_mmap: bool = False,
        mmap_dir: str | None = None,
    ):
        self.redis = redis_client
        self.capacity = capacity
        self.error_rate = error_rate
        self.expansion = expansion
        self.use_mmap = use_mmap
        self.mmap_dir = mmap_dir or os.path.join(os.getcwd(), "bloom_filters")

    def _mmap_name(self, name: str) -> str:
        """Return a filesystem-safe filename for a filter name."""
        return _SAFE_FILENAME.sub("_", name)

    def _mmap_path(self, name: str) -> str:
        return os.path.join(self.mmap_dir, f"{self._mmap_name(name)}.bloom")

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
        if not self.use_mmap:
            return None

        with self._lock:
            cached = self._mmaps.get(name)
            if cached is not None:
                return cached

            filepath = self._mmap_path(name)
            cap = capacity if capacity is not None else self.capacity
            err = error_rate if error_rate is not None else self.error_rate

            try:
                bf = BloomFilter(capacity=cap, error_rate=err, filepath=filepath)
            except Exception as exc:
                logger.warning("failed to open local mmap filter %s at %s: %s", name, filepath, exc)
                return None

            self._mmaps[name] = bf
            if bf.newly_created:
                logger.info("created local Bloom filter file %s (name=%s)", filepath, name)
            return bf

    def _close_mmap(self, name: str) -> None:
        with self._lock:
            bf = self._mmaps.pop(name, None)
            if bf is not None:
                try:
                    bf.close()
                except Exception as exc:
                    logger.warning("failed to close local mmap filter %s: %s", name, exc)

    def flush(self, name: str | None = None) -> None:
        """Persist local mmap mirrors to durable storage.

        With ``name`` set only that mirror is flushed; otherwise every cached
        mirror is flushed.  Calling this after bulk writes (for example after
        ``rebuild``+``swap``) reduces the window in which a crash could leave the
        local mirror behind Redis.  No-op when mmap is disabled.
        """
        with self._lock:
            if name is not None:
                targets: list[tuple[str, BloomFilter]] = []
                bf = self._mmaps.get(name)
                if bf is not None:
                    targets = [(name, bf)]
            else:
                targets = list(self._mmaps.items())
            for filter_name, bf in targets:
                try:
                    bf.flush()
                except Exception as exc:
                    logger.warning("failed to flush local mmap filter %s: %s", filter_name, exc)

    def _local_add(self, name: str, item: str) -> bool:
        bf = self._init_mmap(name)
        if bf is None:
            return False
        try:
            changed = bf.add(item)
            bf.synced = True
            return changed
        except Exception as exc:
            logger.warning("failed to add item to local mmap filter %s: %s", name, exc)
            return False

    def _local_exists(self, name: str, item: str) -> bool:
        """Return ``False`` only for a definite-negative answer.

        Any unknown state (no local filter, a cold/unsynced filter, or a read error)
        returns ``True`` so the caller proceeds to Redis verification.
        """
        bf = self._init_mmap(name)
        if bf is None:
            return True  # no local filter -> fall back to Redis

        if not bf.synced:
            if name not in self._cold_warned:
                self._cold_warned.add(name)
                logger.warning(
                    "local mirror for %s has not been populated through this service; "
                    "negative lookups fall back to Redis until an item is added locally",
                    name,
                )
            return True  # unknown state -> fall back to Redis

        try:
            return item in bf
        except Exception as exc:
            logger.warning("failed to read local mmap filter %s: %s", name, exc)
            return True  # unknown state -> fall back to Redis

    def create_filter(
        self,
        name: str,
        capacity: int | None = None,
        error_rate: float | None = None,
    ) -> bool:
        """Reserve a new Bloom filter in Redis using ``BF.RESERVE``.

        Returns ``True`` if the filter was created or already existed, ``False`` on
        an unrecoverable error.  While mmap is enabled, the local mirror is created
        up front.
        """
        cap = capacity if capacity is not None else self.capacity
        err = error_rate if error_rate is not None else self.error_rate

        if self.use_mmap:
            self._init_mmap(name, cap, err)

        try:
            self.redis.execute_command(
                "BF.RESERVE", name, str(err), str(cap), "EXPANSION", str(self.expansion)
            )
            return True
        except Exception as exc:
            message = str(exc)
            if "item exists" in message or "already exists" in message:
                return True
            logger.warning("BF.RESERVE failed for %s: %s", name, message)
            return False

    # Backwards-compatible alias for the previous camelCase public name.
    createFilter = create_filter

    def add(self, name: str, item: str) -> bool:
        """Add an item to the filter (local mirror first, then RedisBloom)."""
        if self.use_mmap:
            self._local_add(name, item)
        try:
            self.redis.execute_command("BF.ADD", name, item)
            return True
        except Exception as exc:
            logger.warning("BF.ADD failed for %s: %s", name, exc)
            return False

    def exists(self, name: str, item: str) -> bool:
        """Check whether an item is present.

        With mmap enabled, a definite-negative local answer returns ``False``
        without a Redis request.  Everything else (possible positive, cold local
        filter, or mmap disabled) verifies against RedisBloom.  If Redis is
        unreachable the method falls back to a conservative ``True`` and logs a
        warning.
        """
        if self.use_mmap and not self._local_exists(name, item):
            return False
        try:
            return bool(self.redis.execute_command("BF.EXISTS", name, item))
        except Exception as exc:
            logger.warning("BF.EXISTS failed for %s (returning a conservative True): %s", name, exc)
            return True

    def rebuild(
        self,
        name: str,
        items: Iterator[str] | None,
        capacity: int | None = None,
        error_rate: float | None = None,
    ) -> bool:
        """Re-create a filter with a fresh set of items.

        The existing Redis filter is deleted, the local mirror is closed and its file
        removed, a new filter is reserved and populated in chunks.  On any Redis
        error during population the method returns ``False`` after logging the
        failure; the partially populated filter is left in place for inspection.
        """
        try:
            self.redis.delete(name)
        except Exception as exc:
            logger.warning("failed to delete Redis filter %s during rebuild: %s", name, exc)

        if self.use_mmap:
            self._close_mmap(name)
            filepath = self._mmap_path(name)
            try:
                if os.path.exists(filepath):
                    os.remove(filepath)
            except OSError as exc:
                logger.warning("failed to remove local mmap file %s during rebuild: %s", filepath, exc)

        if not self.create_filter(name, capacity, error_rate):
            logger.error("rebuild of %s failed: could not reserve the Redis filter", name)
            return False

        chunk_size = 1000
        chunk: list[str] = []
        for item in items or ():
            if not item:
                continue
            chunk.append(item)
            if self.use_mmap:
                self._local_add(name, item)
            if len(chunk) >= chunk_size:
                try:
                    self._insert_chunk(name, chunk)
                except Exception as exc:
                    logger.error("rebuild of %s failed while inserting a chunk: %s", name, exc)
                    return False
                chunk = []
        if chunk:
            try:
                self._insert_chunk(name, chunk)
            except Exception as exc:
                logger.error("rebuild of %s failed while inserting the final chunk: %s", name, exc)
                return False
        return True

    def _insert_chunk(self, name: str, chunk: list[str]) -> None:
        pipe = self.redis.pipeline()
        for item in chunk:
            pipe.execute_command("BF.ADD", name, item)
        pipe.execute()

    def get_info(self, name: str) -> dict[str, Any]:
        """Return ``{"capacity": int, "inserted": int, "ratio": float}`` for a filter."""
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

        inserted = (
            info_dict.get("Number of items inserted")
            or info_dict.get("number of items inserted")
            or 0
        )
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
        """Advisably acquire a Redis lock for a specific operation."""
        try:
            return bool(self.redis.set(f"lock:{lock_name}", "1", nx=True, ex=ttl))
        except Exception as exc:
            logger.warning("failed to acquire lock %s: %s", lock_name, exc)
            return False

    def release_lock(self, lock_name: str) -> bool:
        """Release a previously acquired lock."""
        try:
            self.redis.delete(f"lock:{lock_name}")
            return True
        except Exception as exc:
            logger.warning("failed to release lock %s: %s", lock_name, exc)
            return False

    def swap(self, temp_name: str, live_name: str) -> bool:
        """Point the live filter at a rebuilt temporary filter.

        Order of operations and recovery model:

        1. ``RENAME temp_name live_name`` in Redis.  If this fails, nothing is
           changed and ``False`` is returned.
        2. If mmap is enabled and a local mirror file exists for ``temp_name``, the
           local files are rotated with ``os.replace`` so the local copy follows the
           Redis generation.

        The two steps are intentionally *not* described as one atomic transaction:
        a crash or a filesystem failure between step 1 and step 2 can leave the
        local mirror older than Redis.  In that case the method returns ``False`` and
        logs the failure, and the local filter is simply repopulated by the next
        ``rebuild``; a stale local mirror risks false negatives until then.
        """
        try:
            self.redis.rename(temp_name, live_name)
        except Exception as exc:
            logger.error("Redis rename %s -> %s failed: %s", temp_name, live_name, exc)
            return False

        result = True
        if self.use_mmap:
            temp_path = self._mmap_path(temp_name)
            live_path = self._mmap_path(live_name)
            if os.path.exists(temp_path):
                with self._lock:
                    self._close_mmap(temp_name)
                    self._close_mmap(live_name)
                    try:
                        os.replace(temp_path, live_path)
                    except OSError as exc:
                        logger.error(
                            "local mmap swap %s -> %s failed after Redis rename succeeded: %s",
                            temp_path,
                            live_path,
                            exc,
                        )
                        result = False
                    else:
                        logger.info("rotated local mmap %s -> %s", temp_path, live_path)
                    if self._init_mmap(live_name) is None:
                        logger.error("failed to reopen local mmap for %s after swap", live_name)
                        result = False
            else:
                logger.info("no local mmap mirror exists for %s; skipping local mirror swap", temp_name)
        return result

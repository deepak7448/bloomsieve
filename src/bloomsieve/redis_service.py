# ruff: noqa: BLE001, S110
from __future__ import annotations

import os
import threading
from typing import Any, ClassVar

from .core import BloomFilter


class BloomFilterService:
    """A generic, reusable Redis Bloom Filter service.

    Uses native RedisBloom module commands: BF.RESERVE, BF.ADD, BF.EXISTS.
    Supports optional memory-mapped (mmap) local file caching.
    """

    # Thread-safe lock for modifying mapping dictionary and resources
    _lock = threading.RLock()
    _mmaps: ClassVar[dict[str, BloomFilter]] = {}  # Cache format: { name: BloomFilter }

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

    def _init_mmap(self, name: str, capacity: int | None = None, error_rate: float | None = None) -> BloomFilter | None:
        if not self.use_mmap:
            return None

        with self._lock:
            if name in self._mmaps:
                return self._mmaps[name]

            safe_filename = name.replace(":", "_")
            directory = self.mmap_dir
            filepath = os.path.join(directory, f"{safe_filename}.bloom")

            cap = capacity if capacity is not None else self.capacity
            err = error_rate if error_rate is not None else self.error_rate

            try:
                bf = BloomFilter(capacity=cap, error_rate=err, filepath=filepath)
                self._mmaps[name] = bf
                return bf
            except Exception:
                return None

    def _close_mmap(self, name: str) -> None:
        with self._lock:
            if name in self._mmaps:
                try:
                    bf = self._mmaps.pop(name)
                    bf.close()
                except Exception:
                    pass

    def _local_add(self, name: str, item: str) -> bool:
        bf = self._init_mmap(name)
        if bf is None:
            return False
        try:
            return bf.add(item)
        except Exception:
            return False

    def _local_exists(self, name: str, item: str) -> bool:
        bf = self._init_mmap(name)
        if bf is None:
            return True  # Fallback to checking Redis
        try:
            return item in bf
        except Exception:
            return True

    def createFilter(self, name: str, capacity: int | None = None, error_rate: float | None = None) -> bool:
        """Reserve a new Bloom filter using BF.RESERVE."""
        cap = capacity if capacity is not None else self.capacity
        err = error_rate if error_rate is not None else self.error_rate
        
        if self.use_mmap:
            self._init_mmap(name, cap, err)

        try:
            self.redis.execute_command(
                "BF.RESERVE", name, str(err), str(cap), "EXPANSION", str(self.expansion)
            )
            return True
        except Exception as e:
            err_str = str(e)
            return bool("ERR item exists" in err_str or "already exists" in err_str)

    def add(self, name: str, item: str) -> bool:
        """Add an item to the Bloom filter."""
        if self.use_mmap:
            self._local_add(name, item)
        try:
            self.redis.execute_command("BF.ADD", name, item)
            return True
        except Exception:
            return False

    def exists(self, name: str, item: str) -> bool:
        """Check if an item exists in the Bloom filter."""
        if self.use_mmap and not self._local_exists(name, item):
            return False
        try:
            return bool(self.redis.execute_command("BF.EXISTS", name, item))
        except Exception:
            return True

    def rebuild(self, name: str, items: Any, capacity: int | None = None, error_rate: float | None = None) -> bool:
        """Rebuild a Bloom filter key with a fresh set of items."""
        try:
            self.redis.delete(name)
        except Exception:
            pass

        # Also delete local mmap file if enabled to ensure stale local cached bits are cleared
        if self.use_mmap:
            self._close_mmap(name)
            safe_filename = name.replace(":", "_")
            filepath = os.path.join(self.mmap_dir, f"{safe_filename}.bloom")
            try:
                if os.path.exists(filepath):
                    os.remove(filepath)
            except Exception:
                pass

        if not self.createFilter(name, capacity, error_rate):
            return False

        try:
            chunk_size = 1000
            chunk = []
            for item in items:
                if item:
                    chunk.append(item)
                    if self.use_mmap:
                        self._local_add(name, item)
                    if len(chunk) >= chunk_size:
                        self._insert_chunk(name, chunk)
                        chunk = []
            if chunk:
                self._insert_chunk(name, chunk)
            return True
        except Exception:
            return False

    def _insert_chunk(self, name: str, chunk: list[str]) -> None:
        try:
            pipe = self.redis.pipeline()
            for item in chunk:
                pipe.execute_command("BF.ADD", name, item)
            pipe.execute()
        except Exception:
            pass

    def get_info(self, name: str) -> dict[str, Any]:
        """Retrieve information about the Bloom filter."""
        try:
            info = self.redis.execute_command("BF.INFO", name)
            info_dict = {}
            if isinstance(info, dict):
                info_dict = {k.decode("utf-8") if isinstance(k, bytes) else k: v for k, v in info.items()}
            elif isinstance(info, list):
                for i in range(0, len(info), 2):
                    k = info[i]
                    k_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                    info_dict[k_str] = info[i+1]
            inserted = info_dict.get("Number of items inserted") or info_dict.get("number of items inserted") or 0
            capacity = info_dict.get("max_elements") or info_dict.get("maxElements") or info_dict.get("Capacity") or info_dict.get("capacity") or 1
            return {
                "capacity": int(capacity),
                "inserted": int(inserted),
                "ratio": float(inserted) / float(capacity) if capacity else 0.0
            }
        except Exception as e:
            err_str = str(e)
            if "not found" in err_str or "no such key" in err_str:
                return {"capacity": 0, "inserted": 0, "ratio": 1.0}
            return {"capacity": 0, "inserted": 0, "ratio": 0.0}

    def load_ratio(self, name: str) -> float:
        """Calculate the fill ratio (0.0-1.0) of the filter."""
        return self.get_info(name)["ratio"]

    def acquire_lock(self, lock_name: str, ttl: int = 600) -> bool:
        """Acquire a Redis distributed lock for a specific operation."""
        try:
            return bool(self.redis.set(f"lock:{lock_name}", "1", nx=True, ex=ttl))
        except Exception:
            return False

    def release_lock(self, lock_name: str) -> bool:
        """Release a previously acquired lock."""
        try:
            self.redis.delete(f"lock:{lock_name}")
            return True
        except Exception:
            return False

    def swap(self, temp_name: str, live_name: str) -> bool:
        """Atomically swap temporary filter to live filter (both in Redis and local mmap)."""
        redis_swapped = False
        try:
            self.redis.rename(temp_name, live_name)
            redis_swapped = True
        except Exception:
            pass

        # Swap in local mmap files
        if self.use_mmap:
            with self._lock:
                temp_bf = self._init_mmap(temp_name)
                live_bf = self._init_mmap(live_name)
                if temp_bf and live_bf:
                    temp_path = temp_bf.filepath
                    live_path = live_bf.filepath

                    # Close both mapping objects and files to release OS locks
                    self._close_mmap(temp_name)
                    self._close_mmap(live_name)

                    try:
                        if temp_path and live_path:
                            os.replace(temp_path, live_path)
                    except Exception:
                        pass
                    finally:
                        # Re-initialize live mapping
                        self._init_mmap(live_name)

        return redis_swapped

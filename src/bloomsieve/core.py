"""Core standalone Bloom filter implementation.

A Bloom filter is a probabilistic data structure that can answer membership
queries with a tunable false-positive rate and no false negatives (under
correct operation).  This module provides the :class:`BloomFilter` class, which
supports both pure in-memory operation and disk-backed memory-mapped (``mmap``)
persistence.
"""

from __future__ import annotations

import logging
import mmap
import os
import struct
import threading
import uuid
from collections.abc import Iterable
from types import TracebackType

from .utils import MAX_HASHES, get_optimal_m_k, hash_pair

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows has no fcntl
    fcntl = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

_HEADER = struct.Struct("<QQ")
_HEADER_SIZE = _HEADER.size  # 16 bytes: unsigned m (bit count), unsigned k (hash count)


def _publish_file(path: str, m: int, k: int, replace: bool) -> bool:
    """Write a complete, zeroed filter file with geometry ``(m, k)`` and move it into place.

    With ``replace=False`` an existing file is never overwritten (the file is
    published with a hard link, which fails atomically if the target exists).
    Returns ``True`` when this call's file is the one now at ``path``.
    """
    tmp_path = f"{path}.{os.getpid()}.{uuid.uuid4().hex[:12]}.tmp"
    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(_HEADER.pack(m, k))
            # Extending with truncate yields zero bytes without writing them, and
            # creates a sparse file where the filesystem supports it.
            fh.truncate(_HEADER_SIZE + m // 8)
        if replace:
            os.replace(tmp_path, path)
            return True
        try:
            os.link(tmp_path, path)
            return True
        except FileExistsError:
            return False
        except OSError:
            # Filesystems without hard links: fall back to a best-effort
            # check-then-rename (there is a small race window here).
            if os.path.exists(path):
                return False
            os.replace(tmp_path, path)
            return True
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


class BloomFilterFileError(ValueError):
    """Raised when an existing Bloom filter file cannot be read or is corrupt."""


class BloomFilter:
    """A standalone local Bloom filter.

    Arguments:
        capacity: Expected number of items stored in the filter (``n``).
        error_rate: Target false-positive probability (``p``), between 0 and 1 exclusive.
        filepath: Optional path of a memory-mapped file to back the filter with.  When
            ``None``, the filter lives purely in memory and is not persisted.  When the
            file already exists its stored configuration wins over ``capacity`` and
            ``error_rate`` so the same filter can be reopened consistently.

    Attributes:
        m: Size of the bit array.
        k: Number of hash functions used.
        capacity, error_rate: The values passed to the constructor.  When an existing
            file is reopened these are *not* read back from it; ``m`` and ``k`` are the
            authoritative stored configuration.
        newly_created: ``True`` when the filter has no pre-existing persistent state
            (in-memory filter, or a file that was freshly created).
        synced: ``True`` when the instance is expected to fully mirror any external
            copy of the same logical filter.  This is maintained by
            :class:`bloomsieve.BloomFilterService`; standalone users can ignore it.

    Notes on atomicity:
        A new file is written completely (header and zeroed bit array) under a
        temporary name and then published, so other processes never observe a
        half-initialised file.  Writes go to the OS page cache immediately and are
        flushed to the backing file on :meth:`flush`/:meth:`close`; a process crash
        before that may leave the file with a partially written bit region.  The
        16-byte header (``m``, ``k``) is validated against the file size when the
        filter is reopened; an unreadable or implausible header raises
        :class:`BloomFilterFileError`.

    Concurrency:
        Writes (:meth:`add`, :meth:`add_many`, :meth:`clear`) are serialised with an
        internal thread lock and, for file-backed filters on POSIX systems, an
        exclusive ``flock`` on the file.  Several threads, and several processes
        mapping the same file, can therefore add concurrently without losing bits.
        Lookups take no lock: bits are only ever set (outside :meth:`clear`), so a
        lookup can at worst miss an add that is still in flight.  On Windows there is
        no cross-process lock; use a single writer process per file there.
        Do not :meth:`close` a filter while other threads are still using it.
    """

    def __init__(
        self,
        capacity: int,
        error_rate: float,
        filepath: str | None = None,
    ):
        self.capacity = capacity
        self.error_rate = error_rate
        self.filepath = filepath

        self.m, self.k = get_optimal_m_k(capacity, error_rate)
        self.byte_size = self.m // 8
        self.total_size = _HEADER_SIZE + self.byte_size

        self._file = None
        self._mmap: mmap.mmap | None = None
        self._bitarray: bytearray | None = None
        # ``_buf`` is whichever of the two buffers is live; bit ``i`` lives in byte
        # ``_offset + i // 8`` so a single code path serves both storage modes.
        self._buf: mmap.mmap | bytearray | None = None
        self._offset = 0
        self._lock = threading.Lock()

        self.newly_created = True
        self.synced = True

        if filepath:
            self._init_mmap()
        else:
            self._bitarray = bytearray(self.byte_size)
            self._buf = self._bitarray

    # ------------------------------------------------------------------ file setup

    def _init_mmap(self) -> None:
        if not self.filepath:
            return

        dir_name = os.path.dirname(self.filepath)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

        # A missing file is created and initialised with the requested configuration.
        # If another process wins the race to create it, its file is used instead.
        created = False
        if not os.path.exists(self.filepath):
            created = self._publish_new_file(replace=False)

        # A file too small to contain a header is treated as truncated/corrupt.
        # Recover by recreating it and log the recovery so it is observable.
        if not created and os.path.getsize(self.filepath) < _HEADER_SIZE:
            logger.warning(
                "Bloom filter file %s is truncated (%d bytes); reinitialising with the requested configuration",
                self.filepath,
                os.path.getsize(self.filepath),
            )
            created = self._publish_new_file(replace=True)

        # Whether new or pre-existing, the file's stored configuration wins so the
        # same filter can be reopened consistently.
        self._load_existing_file()
        if created:
            self.newly_created = True
            self.synced = False

    def _publish_new_file(self, replace: bool) -> bool:
        assert self.filepath is not None
        return _publish_file(self.filepath, self.m, self.k, replace)

    def _load_existing_file(self) -> None:
        self._file = open(self.filepath, "r+b")
        try:
            self._file.seek(0)
            header = self._file.read(_HEADER_SIZE)
            m, k = _HEADER.unpack(header)
        except (struct.error, OSError) as exc:
            self._file.close()
            self._file = None
            raise BloomFilterFileError(f"unreadable Bloom filter header in {self.filepath!r}: {exc}") from exc

        bit_area = m // 8
        full_size = _HEADER_SIZE + bit_area
        file_size = os.fstat(self._file.fileno()).st_size
        if not (m >= 8 and m % 8 == 0 and 1 <= k <= MAX_HASHES and full_size <= file_size):
            self._file.close()
            self._file = None
            raise BloomFilterFileError(
                f"invalid Bloom filter header in {self.filepath!r}: m={m}, k={k}, file size={file_size}"
            )

        if self.m != m or self.k != k:
            logger.info(
                "Reopened %s with stored configuration (m=%d, k=%d); requested (m=%d, k=%d) was ignored",
                self.filepath,
                m,
                k,
                self.m,
                self.k,
            )

        self.m, self.k = m, k
        self.byte_size = bit_area
        self.total_size = full_size
        try:
            self._mmap = mmap.mmap(self._file.fileno(), self.total_size, access=mmap.ACCESS_WRITE)
        except (OSError, ValueError):
            self._file.close()
            self._file = None
            raise
        self._buf = self._mmap
        self._offset = _HEADER_SIZE
        self.newly_created = False
        self.synced = True

    # ------------------------------------------------------------------ locking

    def _acquire_write(self) -> None:
        self._lock.acquire()
        if self._file is not None and fcntl is not None:
            try:
                fcntl.flock(self._file.fileno(), fcntl.LOCK_EX)
            except BaseException:
                self._lock.release()
                raise

    def _release_write(self) -> None:
        try:
            if self._file is not None and fcntl is not None:
                fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        finally:
            self._lock.release()

    def _live_buffer(self) -> mmap.mmap | bytearray:
        buf = self._buf
        if buf is None:
            raise RuntimeError("Bloom filter is closed")
        return buf

    # ------------------------------------------------------------------ operations

    def _set_bits(self, buf: mmap.mmap | bytearray, item: str | bytes) -> bool:
        # (h1 + i*h2) % m computed incrementally: identical indices to
        # utils.get_hash_indices, without big-integer multiplication.
        h1, h2 = hash_pair(item)
        m = self.m
        idx = h1 % m
        step = h2 % m
        off = self._offset
        changed = False
        for _ in range(self.k):
            pos = off + (idx >> 3)
            mask = 1 << (idx & 7)
            val = buf[pos]
            if not val & mask:
                buf[pos] = val | mask
                changed = True
            idx += step
            if idx >= m:
                idx -= m
        return changed

    def add(self, item: str | bytes) -> bool:
        """Add an item to the Bloom filter.

        Returns ``True`` if at least one bit changed from 0 to 1 (the item was not
        already present), ``False`` if all candidate bits were already set.

        Raises:
            TypeError: If ``item`` is neither ``str`` nor ``bytes``.
            RuntimeError: If the filter has been closed.
        """
        buf = self._live_buffer()
        self._acquire_write()
        try:
            return self._set_bits(buf, item)
        finally:
            self._release_write()

    def add_many(self, items: Iterable[str | bytes]) -> int:
        """Add several items, taking the write lock once for the whole batch.

        Returns the number of items that changed at least one bit (i.e. items that
        were not already present).  Much faster than calling :meth:`add` in a loop
        for file-backed filters, because the file lock is acquired only once.

        Raises:
            TypeError: If an item is neither ``str`` nor ``bytes``.  Items before the
                offending one have already been added.
            RuntimeError: If the filter has been closed.
        """
        buf = self._live_buffer()
        set_bits = self._set_bits
        added = 0
        self._acquire_write()
        try:
            for item in items:
                if set_bits(buf, item):
                    added += 1
        finally:
            self._release_write()
        return added

    def __contains__(self, item: str | bytes) -> bool:
        """Return whether the item is *possibly* present.

        A Bloom filter has no false negatives under correct operation: ``False`` is a
        definite negative.  ``True`` means the item may be present, and callers that
        need an exact answer should verify it against an authoritative source.

        Raises:
            TypeError: If ``item`` is neither ``str`` nor ``bytes``.
            RuntimeError: If the filter has been closed.
        """
        buf = self._live_buffer()
        h1, h2 = hash_pair(item)
        m = self.m
        idx = h1 % m
        step = h2 % m
        off = self._offset
        for _ in range(self.k):
            if not buf[off + (idx >> 3)] & (1 << (idx & 7)):
                return False
            idx += step
            if idx >= m:
                idx -= m
        return True

    def to_bytes(self) -> bytes:
        """Return a copy of the bit array (``byte_size`` bytes, without the header).

        Together with ``m`` and ``k`` this fully describes the filter; see
        :meth:`merge_bytes`.
        """
        buf = self._live_buffer()
        off = self._offset
        return bytes(buf[off : off + self.byte_size])

    def merge_bytes(self, data: bytes) -> None:
        """OR another filter's bit array into this one (a set union).

        ``data`` must come from :meth:`to_bytes` of a filter with the same ``m`` and
        ``k``; afterwards this filter answers "possibly present" for every item
        either filter contained.

        Raises:
            ValueError: If ``data`` is not exactly ``byte_size`` bytes long.
            RuntimeError: If the filter has been closed.
        """
        if len(data) != self.byte_size:
            raise ValueError(f"expected {self.byte_size} bytes of bit array, got {len(data)}")
        buf = self._live_buffer()
        off, size = self._offset, self.byte_size
        self._acquire_write()
        try:
            merged = int.from_bytes(buf[off : off + size], "little") | int.from_bytes(data, "little")
            buf[off : off + size] = merged.to_bytes(size, "little")
        finally:
            self._release_write()

    def clear(self) -> None:
        """Reset all bits to zero.

        The header (and therefore the filter's configuration) is preserved.  When the
        filter mirrors a remote datasource, callers must repopulate it or the local
        state will disagree with the remote state.
        """
        buf = self._live_buffer()
        self._acquire_write()
        try:
            if self._mmap is not None:
                self._mmap[_HEADER_SIZE:] = bytes(self.byte_size)
                self._mmap.flush()
            else:
                buf[:] = bytes(self.byte_size)
        finally:
            self._release_write()

    def flush(self) -> None:
        """Persist dirty pages of the backing file to disk.

        Writes are applied to the kernel page cache immediately and are visible to
        other processes mapping the same file, but they are only guaranteed durable
        on disk after this method (or :meth:`close`) is called.
        """
        self._live_buffer()
        if self._mmap is not None:
            self._mmap.flush()

    def close(self) -> None:
        """Flush and close the backing file and memory map.

        Safe to call multiple times.
        """
        with self._lock:
            self._buf = None
            if self._mmap is not None:
                self._mmap.flush()
                self._mmap.close()
                self._mmap = None
            if self._file is not None:
                self._file.close()
                self._file = None
            self._bitarray = None

    @property
    def closed(self) -> bool:
        """``True`` once :meth:`close` has been called."""
        return self._buf is None

    def __repr__(self) -> str:
        where = repr(self.filepath) if self.filepath else "memory"
        state = " closed" if self.closed else ""
        return f"<BloomFilter m={self.m} k={self.k} {where}{state}>"

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def __enter__(self) -> BloomFilter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

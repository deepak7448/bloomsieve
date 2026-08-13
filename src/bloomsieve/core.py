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
from types import TracebackType

from .utils import get_hash_indices, get_optimal_m_k

logger = logging.getLogger(__name__)

_HEADER = struct.Struct("<QQ")
_HEADER_SIZE = _HEADER.size  # 16 bytes: unsigned m (bit count), unsigned k (hash count)


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
        newly_created: ``True`` when the filter has no pre-existing persistent state
            (in-memory filter, or a file that was freshly created).
        synced: ``True`` when the instance is expected to fully mirror any external
            copy of the same logical filter.  This is maintained by
            :class:`bloomsieve.BloomFilterService`; standalone users can ignore it.

    Notes on atomicity:
        Writes go to the OS page cache immediately and are flushed to the backing
        file on :meth:`flush`/:meth:`close`; a process crash before that may leave
        the file with a partially written bit region.  The 16-byte header
        (``m``, ``k``) is validated against the file size when the filter is
        reopened; an unreadable or implausible header raises
        :class:`BloomFilterFileError`.

    Thread safety:
        A single :class:`BloomFilter` instance is not thread-safe.  Concurrent
        access from multiple threads requires external locking.
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
        self._mmap = None
        self._bitarray = None

        self.newly_created = True
        self.synced = True

        if filepath:
            self._init_mmap()
        else:
            self._bitarray = bytearray(self.byte_size)

    def _init_mmap(self) -> None:
        if not self.filepath:
            return

        dir_name = os.path.dirname(self.filepath)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

        exists = os.path.exists(self.filepath)

        # A missing file is created and initialised with the requested configuration.
        if not exists:
            self._write_new_file()
            return

        # A file too small to contain a header is treated as truncated/corrupt.
        # Recover by recreating it and log the recovery so it is observable.
        if os.path.getsize(self.filepath) < _HEADER_SIZE:
            logger.warning(
                "Bloom filter file %s is truncated (%d bytes); reinitialising with the requested configuration",
                self.filepath,
                os.path.getsize(self.filepath),
            )
            self._write_new_file()
            return

        # An existing file with a valid header: the stored configuration wins so the
        # same filter can be reopened consistently.
        self._load_existing_file()

    def _write_new_file(self) -> None:
        self._file = open(self.filepath, "w+b")
        self._file.truncate(self.total_size)
        self._file.seek(0)
        self._file.write(_HEADER.pack(self.m, self.k))
        self._file.write(b"\x00" * self.byte_size)
        self._file.flush()
        self._mmap = mmap.mmap(self._file.fileno(), self.total_size, access=mmap.ACCESS_WRITE)
        self.newly_created = True
        self.synced = False

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
        if not (m >= 8 and m % 8 == 0 and 1 <= k <= 128 and full_size <= os.path.getsize(self.filepath)):
            self._file.close()
            self._file = None
            raise BloomFilterFileError(
                f"invalid Bloom filter header in {self.filepath!r}: m={m}, k={k}, "
                f"file size={os.path.getsize(self.filepath)}"
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
        self._mmap = mmap.mmap(self._file.fileno(), self.total_size, access=mmap.ACCESS_WRITE)
        self.newly_created = False
        self.synced = True

    def _raise_if_closed(self) -> None:
        if self._mmap is None and self._bitarray is None:
            raise RuntimeError("Bloom filter is closed")

    def add(self, item: str | bytes) -> bool:
        """Add an item to the Bloom filter.

        Returns ``True`` if at least one bit changed from 0 to 1 (the item was not
        already present), ``False`` if all candidate bits were already set.

        Raises:
            TypeError: If ``item`` is neither ``str`` nor ``bytes``.
            RuntimeError: If the filter has been closed.
        """
        self._raise_if_closed()
        indices = get_hash_indices(item, self.m, self.k)
        changed = False

        if self._mmap is not None:
            for idx in indices:
                byte_idx = _HEADER_SIZE + (idx // 8)
                bit_idx = idx % 8
                val = self._mmap[byte_idx]
                bit_mask = 1 << bit_idx
                if not (val & bit_mask):
                    self._mmap[byte_idx] = val | bit_mask
                    changed = True
        else:
            for idx in indices:
                byte_idx = idx // 8
                bit_idx = idx % 8
                val = self._bitarray[byte_idx]
                bit_mask = 1 << bit_idx
                if not (val & bit_mask):
                    self._bitarray[byte_idx] = val | bit_mask
                    changed = True

        return changed

    def __contains__(self, item: str | bytes) -> bool:
        """Return whether the item is *possibly* present.

        A Bloom filter has no false negatives under correct operation: ``False`` is a
        definite negative.  ``True`` means the item may be present, and callers that
        need an exact answer should verify it against an authoritative source.

        Raises:
            TypeError: If ``item`` is neither ``str`` nor ``bytes``.
            RuntimeError: If the filter has been closed.
        """
        self._raise_if_closed()
        indices = get_hash_indices(item, self.m, self.k)

        if self._mmap is not None:
            for idx in indices:
                byte_idx = _HEADER_SIZE + (idx // 8)
                bit_idx = idx % 8
                if not (self._mmap[byte_idx] & (1 << bit_idx)):
                    return False
        else:
            for idx in indices:
                byte_idx = idx // 8
                bit_idx = idx % 8
                if not (self._bitarray[byte_idx] & (1 << bit_idx)):
                    return False

        return True

    def clear(self) -> None:
        """Reset all bits to zero.

        The header (and therefore the filter's configuration) is preserved.  When the
        filter mirrors a remote datasource, callers must repopulate it or the local
        state will disagree with the remote state.
        """
        self._raise_if_closed()
        if self._mmap is not None:
            self._mmap[_HEADER_SIZE:] = b"\x00" * self.byte_size
            self._mmap.flush()
        else:
            self._bitarray = bytearray(self.byte_size)

    def flush(self) -> None:
        """Persist dirty pages of the backing file to disk.

        Writes are applied to the kernel page cache immediately and are visible to
        other processes mapping the same file, but they are only guaranteed durable
        on disk after this method (or :meth:`close`) is called.
        """
        self._raise_if_closed()
        if self._mmap is not None:
            self._mmap.flush()

    def close(self) -> None:
        """Flush and close the backing file and memory map.

        Safe to call multiple times.
        """
        if self._mmap is not None:
            self._mmap.flush()
            self._mmap.close()
            self._mmap = None
        if self._file is not None:
            self._file.close()
            self._file = None
        self._bitarray = None

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

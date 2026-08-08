from __future__ import annotations

import mmap
import os
import struct
from typing import Any

from .utils import get_hash_indices, get_optimal_m_k


class BloomFilter:
    """A standalone, high-performance local Bloom Filter.

    Supports both pure in-memory operation and disk-backed memory-mapped (mmap)
    persistence.
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
        self.total_size = 16 + self.byte_size  # 16 bytes header for (m, k)

        self._file = None
        self._mmap = None
        self._bitarray = None

        if filepath:
            self._init_mmap()
        else:
            self._bitarray = bytearray(self.byte_size)

    def _init_mmap(self) -> None:
        if not self.filepath:
            return

        exists = os.path.exists(self.filepath)
        # Open in r+b mode if it exists, otherwise w+b
        mode = "r+b" if exists else "w+b"
        
        # Ensure directory exists
        dir_name = os.path.dirname(self.filepath)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

        self._file = open(self.filepath, mode)  # noqa: SIM115

        if not exists or os.path.getsize(self.filepath) < self.total_size:
            # Initialize new file
            self._file.truncate(self.total_size)
            self._file.seek(0)
            self._file.write(struct.pack("<QQ", self.m, self.k))
            self._file.write(b"\x00" * self.byte_size)
            self._file.flush()
        else:
            # Read existing metadata
            self._file.seek(0)
            header = self._file.read(16)
            m_loaded, k_loaded = struct.unpack("<QQ", header)
            self.m, self.k = m_loaded, k_loaded
            self.byte_size = self.m // 8
            self.total_size = 16 + self.byte_size

        self._mmap = mmap.mmap(self._file.fileno(), self.total_size, access=mmap.ACCESS_WRITE)

    def add(self, item: str | bytes) -> bool:
        """Add an item to the Bloom filter.

        Returns:
            True if the item was newly added (at least one bit changed from 0 to 1),
            False if it was already likely present.
        """
        indices = get_hash_indices(item, self.m, self.k)
        changed = False

        if self._mmap is not None:
            for idx in indices:
                byte_idx = 16 + (idx // 8)
                bit_idx = idx % 8
                val = self._mmap[byte_idx]
                bit_mask = 1 << bit_idx
                if not (val & bit_mask):
                    self._mmap[byte_idx] = val | bit_mask
                    changed = True
            if changed:
                self._mmap.flush()
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
        """Check if an item is likely in the Bloom filter."""
        indices = get_hash_indices(item, self.m, self.k)

        if self._mmap is not None:
            for idx in indices:
                byte_idx = 16 + (idx // 8)
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
        """Clear all bits in the Bloom filter."""
        if self._mmap is not None:
            self._mmap[16:] = b"\x00" * self.byte_size
            self._mmap.flush()
        else:
            self._bitarray = bytearray(self.byte_size)

    def close(self) -> None:
        """Close open file and memory mapping handles."""
        if self._mmap is not None:
            self._mmap.close()
            self._mmap = None
        if self._file is not None:
            self._file.close()
            self._file = None

    def __enter__(self) -> BloomFilter:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

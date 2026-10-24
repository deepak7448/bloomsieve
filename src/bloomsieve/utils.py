"""Shared hashing and sizing helpers."""

from __future__ import annotations

import hashlib
import math
import struct

_MIN_BITS = 8

#: Largest number of hash functions a filter may use.  The on-disk header
#: validation rejects anything above this, so sizing must never exceed it.
MAX_HASHES = 128


def get_optimal_m_k(capacity: int, error_rate: float) -> tuple[int, int]:
    """Calculate the optimal bit array size (m) and number of hash functions (k).

    Args:
        capacity: Expected number of items.
        error_rate: Acceptable false positive probability.

    Returns:
        A tuple of (m, k) where m is rounded up to the nearest multiple of 8.

    Raises:
        ValueError: If ``capacity`` <= 0, ``error_rate`` is not strictly between 0 and 1
            (NaN included), or ``error_rate`` is so small that more than
            :data:`MAX_HASHES` hash functions would be needed.
    """
    if capacity <= 0:
        raise ValueError("Capacity must be greater than 0")
    # Written as a positive range check so that NaN is rejected too.
    if not 0 < error_rate < 1:
        raise ValueError("Error rate must be between 0 and 1 (exclusive)")

    m = int(-(capacity * math.log(error_rate)) / (math.log(2) ** 2))
    # Round up to a multiple of 8 for byte-alignment, never below 8 bits.
    m = max(_MIN_BITS, ((m + 7) // 8) * 8)
    k = max(1, int(round((m / capacity) * math.log(2))))
    if k > MAX_HASHES:
        raise ValueError(f"Error rate {error_rate!r} is too small: it needs {k} hash functions (max {MAX_HASHES})")
    return m, k


def get_hash_indices(item: str | bytes, m: int, k: int) -> list[int]:
    """Generate k bit indices for a given item.

    Uses the Kirsch-Mitzenmacher technique so that only a single SHA-256 digest is
    needed per item: ``g_i(x) = (h1 + i * h2) % m``.

    Args:
        item: The string or bytes item to hash.
        m: Size of the bit array.
        k: Number of hash functions.

    Returns:
        List of k integer indices in the range [0, m-1].

    Raises:
        TypeError: If ``item`` is neither ``str`` nor ``bytes``.
        ValueError: If ``m`` <= 0 or ``k`` <= 0.
    """
    if m <= 0 or k <= 0:
        raise ValueError("m and k must both be greater than 0")

    h1, h2 = hash_pair(item)
    # Kirsch-Mitzenmacher double hashing: g_i(x) = (h1 + i * h2) % m
    return [(h1 + i * h2) % m for i in range(k)]


_DIGEST_PAIR = struct.Struct("<QQ")
_sha256 = hashlib.sha256


def hash_pair(item: str | bytes) -> tuple[int, int]:
    """Return the two 64-bit base hashes ``(h1, h2)`` for an item.

    This is part of the on-disk format: files written by earlier versions are only
    readable correctly while this function returns the same values, so it must not
    change.

    Raises:
        TypeError: If ``item`` is neither ``str`` nor ``bytes``.
    """
    if isinstance(item, str):
        data = item.encode("utf-8")
    elif isinstance(item, bytes):
        data = item
    else:
        raise TypeError(f"items must be str or bytes, got {type(item).__name__}")

    # A single SHA-256 digest; its first 16 bytes give two little-endian 64-bit values.
    return _DIGEST_PAIR.unpack_from(_sha256(data).digest())

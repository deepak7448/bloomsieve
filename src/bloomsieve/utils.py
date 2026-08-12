"""Shared hashing and sizing helpers."""

from __future__ import annotations

import hashlib
import math
import struct

_MIN_BITS = 8


def get_optimal_m_k(capacity: int, error_rate: float) -> tuple[int, int]:
    """Calculate the optimal bit array size (m) and number of hash functions (k).

    Args:
        capacity: Expected number of items.
        error_rate: Acceptable false positive probability.

    Returns:
        A tuple of (m, k) where m is rounded up to the nearest multiple of 8.

    Raises:
        ValueError: If ``capacity`` <= 0 or ``error_rate`` is not strictly between 0 and 1.
    """
    if capacity <= 0:
        raise ValueError("Capacity must be greater than 0")
    if error_rate <= 0 or error_rate >= 1:
        raise ValueError("Error rate must be between 0 and 1 (exclusive)")

    m = int(-(capacity * math.log(error_rate)) / (math.log(2) ** 2))
    # Round up to a multiple of 8 for byte-alignment, never below 8 bits.
    m = max(_MIN_BITS, ((m + 7) // 8) * 8)
    k = max(1, int(round((m / capacity) * math.log(2))))
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

    if isinstance(item, str):
        data = item.encode("utf-8")
    elif isinstance(item, bytes):
        data = item
    else:
        raise TypeError(f"items must be str or bytes, got {type(item).__name__}")

    # Generate a single 256-bit hash (SHA-256) and split it into two 64-bit values.
    digest = hashlib.sha256(data).digest()
    h1, h2 = struct.unpack("<QQ", digest[:16])

    # Kirsch-Mitzenmacher double hashing: g_i(x) = (h1 + i * h2) % m
    return [(h1 + i * h2) % m for i in range(k)]

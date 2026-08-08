from __future__ import annotations

import hashlib
import math
import struct


def get_optimal_m_k(capacity: int, error_rate: float) -> tuple[int, int]:
    """Calculate the optimal bit array size (m) and number of hash functions (k).

    Args:
        capacity: Expected number of items.
        error_rate: Acceptable false positive probability.

    Returns:
        A tuple of (m, k) where m is rounded to the nearest multiple of 8.
    """
    if capacity <= 0:
        raise ValueError("Capacity must be greater than 0")
    if error_rate <= 0 or error_rate >= 1:
        raise ValueError("Error rate must be between 0 and 1 (exclusive)")

    m = int(-(capacity * math.log(error_rate)) / (math.log(2) ** 2))
    # Round up to a multiple of 8 for byte-alignment
    m = ((m + 7) // 8) * 8
    k = max(1, int((m / capacity) * math.log(2)))
    return m, k


def get_hash_indices(item: str | bytes, m: int, k: int) -> list[int]:
    """Generate k bit indices for a given item using Kirsch-Mitzenmacher optimization.

    Args:
        item: The string or bytes item to hash.
        m: Size of the bit array.
        k: Number of hash functions.

    Returns:
        List of k integer indices in the range [0, m-1].
    """
    if isinstance(item, str):
        data = item.encode("utf-8")
    elif isinstance(item, bytes):
        data = item
    else:
        data = str(item).encode("utf-8")

    # Generate a single 256-bit hash (SHA-256)
    h = hashlib.sha256(data).digest()
    # Unpack first 16 bytes into two 64-bit unsigned integers
    h1, h2 = struct.unpack("<QQ", h[:16])

    # Kirsch-Mitzenmacher technique: g_i(x) = (h1 + i * h2) % m
    return [(h1 + i * h2) % m for i in range(k)]

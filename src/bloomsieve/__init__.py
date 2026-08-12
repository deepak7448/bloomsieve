"""Bloomsieve: persistent mmap Bloom filters with optional RedisBloom integration.

The standalone :class:`BloomFilter` has no runtime dependencies.  The optional
:class:`BloomFilterService` layers RedisBloom on top and can use a local mmap
filter to reject definite-negative membership queries without a network request.
"""

from .core import BloomFilter, BloomFilterFileError
from .redis_service import BloomFilterService
from .utils import get_hash_indices, get_optimal_m_k

__version__ = "0.2.0"

__all__ = [
    "BloomFilter",
    "BloomFilterFileError",
    "BloomFilterService",
    "__version__",
    "get_hash_indices",
    "get_optimal_m_k",
]

# Bloomsieve

[![PyPI version](https://img.shields.io/badge/pypi-v0.1.2-blue.svg)](https://pypi.org/project/bloomsieve/)
[![Python Version](https://img.shields.io/pypi/pyversions/bloomsieve.svg)](https://pypi.org/project/bloomsieve/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Bloomsieve** is a high-performance Python Bloom Filter library with Kirsch-Mitzenmacher double-hashing, local memory-mapped (`mmap`) persistence, and integrated RedisBloom support.

---

## Features

- ⚡ **Kirsch-Mitzenmacher Hashing**: Reduces cryptographic hash calls from $O(k)$ to $O(1)$ per item via SHA-256 double-hashing.
- 💾 **Memory-Mapped (`mmap`) Persistence**: Fast disk storage backed by kernel page caching with header metadata (`<QQ`).
- 🚀 **Hybrid Redis + mmap Acceleration**: Zero-latency local negative lookups backed by distributed RedisBloom synchronization.
- 🔄 **Zero-Downtime Rotation**: Built-in `rebuild()` and atomic `swap()` for live cache rotation and capacity expansion.
- 🛡️ **Zero Dependencies for Core Mode**: Standalone `BloomFilter` requires standard library modules only.

---

## Requirements

- **Python**: `3.8+`
- **Redis Server**: Redis 4.0+ with RedisBloom (Optional: required only for `BloomFilterService`).

---

## Installation

```bash
pip install bloomsieve
```

---

## Quick Start

### 1. Redis-Backed Service

#### With Local mmap Cache (Zero-Latency Hybrid Acceleration)

```python
import redis
from bloomsieve import BloomFilterService

redis_client = redis.Redis(host="localhost", port=6379, db=0)

service = BloomFilterService(redis_client=redis_client, capacity=10000, error_rate=0.01, use_mmap=True)
service.createFilter("active_tokens")

service.add("active_tokens", "token_xyz")
print(service.exists("active_tokens", "token_unknown"))  # Returns: False (0 network latency)
```

#### Without Local mmap Cache (Pure Distributed Redis)

```python
import redis
from bloomsieve import BloomFilterService

redis_client = redis.Redis(host="localhost", port=6379, db=0)

service = BloomFilterService(redis_client=redis_client, capacity=10000, use_mmap=False)
service.createFilter("global_users")

service.add("global_users", "user_alice")
print(service.exists("global_users", "user_alice"))  # Returns: True
```

### 2. Standalone Bloom Filter (In-Memory & Persistent mmap)

```python
from bloomsieve import BloomFilter

# In-Memory
bf = BloomFilter(capacity=100000, error_rate=0.01)
bf.add("user_101")
print("user_101" in bf)  # Returns: True

# Disk-backed mmap file
with BloomFilter(capacity=50000, error_rate=0.005, filepath="cache.bloom") as bf_disk:
    bf_disk.add("session_abc")
    print("session_abc" in bf_disk)  # Returns: True
```

---

## API Overview

### `BloomFilter`

`BloomFilter(capacity: int, error_rate: float, filepath: str | None = None)`

- `add(item: str | bytes) -> bool`: Add item to filter.
- `__contains__(item: str | bytes) -> bool`: Check membership (`item in bf`).
- `clear()`: Reset all bits to zero.
- `close()`: Flush and close file handles.

### `BloomFilterService`

`BloomFilterService(redis_client, capacity=1000000, error_rate=0.001, expansion=2, use_mmap=False, mmap_dir=None)`

- `createFilter(name, capacity=None, error_rate=None) -> bool`: Reserve Redis filter.
- `add(name, item) -> bool`: Add item to Redis and local mmap cache.
- `exists(name, item) -> bool`: Check membership (skips network if local mmap is False).
- `rebuild(name, items, capacity=None, error_rate=None) -> bool`: Re-create filter with optional capacity expansion and bulk-insert items.
- `swap(temp_name, live_name) -> bool`: Atomically swap temporary filter key to live filter in Redis and disk mmap.
- `get_info(name) -> dict`: Returns `{"capacity": int, "inserted": int, "ratio": float}`.
- `load_ratio(name) -> float`: Return fill ratio (`inserted / capacity`).
- `acquire_lock(lock_name, ttl=600)` / `release_lock(lock_name)`: Distributed Redis lock management.

---

## Configuration

| Option       | Default                | Description                                      |
| :----------- | :--------------------- | :----------------------------------------------- |
| `capacity`   | _Required_ / `1000000` | Target element capacity ($n$)                    |
| `error_rate` | _Required_ / `0.001`   | Target false positive probability ($p$)          |
| `use_mmap`   | `False`                | Enable local disk mmap cache acceleration        |
| `mmap_dir`   | `./bloom_filters`      | Storage directory for local `.bloom` cache files |

_Memory Footprint_: ~9.6 bits per item for 1% error rate (~1.14 MB per 1,000,000 items).

---

## Examples

### Threshold Rebuilding & Atomic Rotation (`rebuild` + `swap`)

```python
import redis
from bloomsieve import BloomFilterService

client = redis.Redis(host="localhost", port=6379, db=0)
service = BloomFilterService(redis_client=client, capacity=10000, error_rate=0.001, use_mmap=True)

# Auto-rebuild with 2x capacity headroom when load ratio >= 80%
if service.load_ratio("bloom:users") >= 0.8:
    if service.acquire_lock("rebuild:users", ttl=600):
        try:
            items = (f"user_{i}" for i in range(20000))
            service.rebuild("bloom:users:temp", items=items, capacity=40000)
            service.swap("bloom:users:temp", "bloom:users")
        finally:
            service.release_lock("rebuild:users")
```

---

## Performance Notes

- **Time Complexity**: $O(1)$ for `add` and membership checks (1 SHA-256 hash digest evaluation).
- **Sub-Microsecond Latency**: Local `mmap` checks resolve negative lookups in sub-microsecond time outside the Python GC heap.

---

## FAQ

- **Can items be deleted?** No. Standard Bloom filters do not support deletion. Use `rebuild()` or `swap()` to refresh filters.
- **What happens when capacity is exceeded?** The filter continues working, but false positive rate increases.
- **Is Redis required?** No. Core `BloomFilter` works 100% standalone without Redis.

---

## Contributing

```bash
git clone https://github.com/deepak7448/bloomsieve.git
cd bloomsieve
pip install -e .[dev]
pytest
```

---

## License

MIT License. See [LICENSE](LICENSE) for details.

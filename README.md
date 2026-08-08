# Bloomsieve

[![PyPI version](https://img.shields.io/pypi/v/bloomsieve.svg)](https://pypi.org/project/bloomsieve/)
[![Python Version](https://img.shields.io/pypi/pyversions/bloomsieve.svg)](https://pypi.org/project/bloomsieve/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Bloomsieve** is a high-performance, standalone Python Bloom Filter library featuring Kirsch-Mitzenmacher double-hashing optimization, persistent memory-mapped (`mmap`) file storage, and integrated hybrid Redis (`RedisBloom`) support with local disk caching.

---

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Core Concepts](#core-concepts)
- [Quick Start](#quick-start)
- [API Overview](#api-overview)
  - [BloomFilter](#bloomfilter)
  - [BloomFilterService](#bloomfilterservice)
  - [Utility Functions](#utility-functions)
- [Configuration](#configuration)
- [Examples](#examples)
- [Performance Notes](#performance-notes)
- [FAQ](#faq)
- [Contributing](#contributing)
- [License](#license)

---

## Features

- ⚡ **Kirsch-Mitzenmacher Double-Hashing**: Reduces expensive cryptographic hash function evaluations from $O(k)$ to $O(1)$ by deriving $k$ bit indices from a single SHA-256 digest.
- 💾 **Memory-Mapped (`mmap`) Persistence**: Persists bit arrays to disk using OS kernel-level page mapping. Includes automatic binary header serialization (`<QQ` format) to preserve filter metadata ($m$ and $k$) across process restarts.
- 🚀 **Hybrid Redis + Local mmap Acceleration**: `BloomFilterService` routes negative membership queries locally via disk `mmap` cache, eliminating Redis network round-trips for non-existent items while maintaining distributed state synchronization on Redis.
- 🛡️ **Zero Dependencies for Core Mode**: Standalone `BloomFilter` uses Python standard library modules only.
- 🔄 **Zero-Downtime Filter Rotation**: Built-in `rebuild()` and atomic `swap()` methods enable seamless filter rebuilds and blue/green cache deployments without service interruption.
- 🔒 **Thread-Safe Service Operations**: `BloomFilterService` uses recursive locking (`RLock`) to safely manage concurrent mmap handles and distributed lock primitives (`acquire_lock` / `release_lock`).
- 🐍 **Pythonic & Fully Typed**: Complete type annotations, context manager support (`with`), and standard membership syntax (`item in bf`).

---

## Requirements

- **Python**: `3.8+`
- **Redis Server**: Redis 4.0+ with **RedisBloom** (Optional: required only for `BloomFilterService`).

---

## Installation

```bash
pip install bloomsieve
```

For development dependencies (testing and linting):

```bash
pip install bloomsieve[dev]
```

Or from source:

```bash
git clone https://github.com/deepak7448/bloomsieve.git
cd bloomsieve
pip install -e .
```

---

## Core Concepts

### What is a Bloom Filter?

A space-efficient probabilistic data structure used to test set membership.

- **False Positives**: Possible. Returns `True` if an item is likely in the set.
- **False Negatives**: Impossible. Returns `False` only when an item is **guaranteed** not in the set.

### Kirsch-Mitzenmacher Technique

Computes a single SHA-256 hash per item, unpacks the first 16 bytes into two 64-bit unsigned integers ($h_1, h_2$), and derives $k$ bit indices via:

$$g_i(x) = (h_1 + i \cdot h_2) \pmod m \quad \text{for } i \in [0, k-1]$$

This reduces cryptographic hash evaluations per item from $O(k)$ to $O(1)$, using cheap linear arithmetic instead while maintaining false-positive rates equivalent to $k$ independent hash functions.

### Persistent Memory-Mapped (`mmap`) I/O

When `filepath` is set, `BloomFilter` writes a 16-byte header (`<QQ` format for $m$ and $k$) followed by the bit array. Reads and writes bypass standard file offset seeking, mapping directly to OS virtual memory.

### Hybrid Redis & Local mmap Acceleration

When `BloomFilterService` runs with `use_mmap=True`, negative queries (`exists`) are resolved locally by disk `mmap` in sub-microsecond time with zero network latency.

---

## Quick Start

### 1. Redis-Backed Service

#### With Local mmap Cache (Zero-Latency Hybrid Acceleration)

```python
import redis
from bloomsieve import BloomFilterService

redis_client = redis.Redis(host="localhost", port=6379, db=0)

service = BloomFilterService(
    redis_client=redis_client,
    capacity=10000,
    error_rate=0.01,
    use_mmap=True,
    mmap_dir="./bloom_cache"
)

filter_name = "active_tokens"
service.createFilter(filter_name)

# Add item (updates Redis and local mmap cache)
service.add(filter_name, "token_xyz")

# Negative query resolved instantly via local mmap (0 network latency)
print(service.exists(filter_name, "token_unknown"))  # Returns: False
```

#### Without Local mmap Cache (Pure Distributed Redis)

```python
import redis
from bloomsieve import BloomFilterService

redis_client = redis.Redis(host="localhost", port=6379, db=0)

service = BloomFilterService(
    redis_client=redis_client,
    capacity=10000,
    error_rate=0.01,
    use_mmap=False
)

filter_name = "global_users"
service.createFilter(filter_name)

service.add(filter_name, "user_alice")
print(service.exists(filter_name, "user_alice"))  # Returns: True
print(service.exists(filter_name, "user_bob"))    # Returns: False
```

### 2. Standalone In-Memory Bloom Filter

```python
from bloomsieve import BloomFilter

bf = BloomFilter(capacity=100000, error_rate=0.01)

bf.add("user_101")
bf.add("user_102")

print("user_101" in bf)  # Returns: True
print("user_103" in bf)  # Returns: False
```

### 3. Standalone Disk-Backed Persistent Bloom Filter

```python
from bloomsieve import BloomFilter

# Open or create persistent bloom filter file
with BloomFilter(capacity=50000, error_rate=0.005, filepath="cache.bloom") as bf:
    bf.add("session_abc123")
    print("session_abc123" in bf)  # Returns: True

# Re-opening automatically restores metadata (m, k) and state from header
with BloomFilter(capacity=50000, error_rate=0.005, filepath="cache.bloom") as bf_reloaded:
    print("session_abc123" in bf_reloaded)  # Returns: True
```

---

## API Overview

### `BloomFilter`

Standalone Bloom filter with optional `mmap` disk persistence.

#### Constructor

```python
BloomFilter(capacity: int, error_rate: float, filepath: str | None = None)
```

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `capacity` | `int` | _Required_ | Expected item count ($n > 0$). |
| `error_rate` | `float` | _Required_ | Target false positive rate ($0 < p < 1$). |
| `filepath` | `str \| None` | `None` | Disk path for persistent `.bloom` file. |

#### Attributes

| Attribute | Type | Description |
| :--- | :--- | :--- |
| `capacity` | `int` | Target item capacity ($n$). |
| `error_rate` | `float` | Target false positive rate ($p$). |
| `filepath` | `str \| None` | File path, or `None` if in-memory. |
| `m` | `int` | Total bit array size (rounded to multiple of 8). |
| `k` | `int` | Number of hash functions. |
| `byte_size` | `int` | Bit array size in bytes ($m / 8$). |
| `total_size` | `int` | Total file size in bytes ($16 + \text{byte\_size}$). |

#### Methods

- `add(item: str | bytes) -> bool`: Adds item. Returns `True` if new bits were set, `False` if already present.
- `__contains__(item: str | bytes) -> bool`: Checks membership (`item in bf`).
- `clear() -> None`: Resets all bits to zero.
- `close() -> None`: Flushes and closes file handles.
- `__enter__()` / `__exit__()`: Context manager support.

---

### `BloomFilterService`

Distributed RedisBloom client wrapper with optional local `mmap` caching.

#### Constructor

```python
BloomFilterService(
    redis_client: Any,
    capacity: int = 1000000,
    error_rate: float = 0.001,
    expansion: int = 2,
    use_mmap: bool = False,
    mmap_dir: str | None = None
)
```

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `redis_client` | `Any` | _Required_ | Redis client instance (`redis.Redis(...)`). |
| `capacity` | `int` | `1000000` | Default capacity for new filters. |
| `error_rate` | `float` | `0.001` | Default error rate for new filters. |
| `expansion` | `int` | `2` | Expansion rate for RedisBloom filters (`BF.RESERVE`). |
| `use_mmap` | `bool` | `False` | Enables local disk `mmap` cache acceleration. |
| `mmap_dir` | `str \| None` | `None` | Storage directory for local `.bloom` files (`./bloom_filters`). |

#### Methods

- `createFilter(name: str, capacity: int | None = None, error_rate: float | None = None) -> bool`: Reserves filter on Redis (`BF.RESERVE`).
- `add(name: str, item: str) -> bool`: Adds item (`BF.ADD`) and updates local mmap cache.
- `exists(name: str, item: str) -> bool`: Checks membership (`BF.EXISTS`). Skips network call if local mmap returns `False`.
- `rebuild(name: str, items: Iterable[str], capacity: int | None = None, error_rate: float | None = None) -> bool`: Deletes existing filter (Redis key & local mmap file), re-creates a clean filter with optional new/increased `capacity` or `error_rate`, and bulk-inserts `items` in chunks via Redis pipelines.
- `get_info(name: str) -> dict[str, Any]`: Returns `{"capacity": int, "inserted": int, "ratio": float}`.
- `load_ratio(name: str) -> float`: Returns current fill ratio (`inserted / capacity`).
- `acquire_lock(lock_name: str, ttl: int = 600) -> bool`: Acquires Redis lock key `lock:<lock_name>`.
- `release_lock(lock_name: str) -> bool`: Releases Redis lock key.
- `swap(temp_name: str, live_name: str) -> bool`: Atomically swaps temp filter to live key in Redis and renames local mmap files.

---

### Utility Functions

```python
from bloomsieve import get_optimal_m_k, get_hash_indices

# Calculate optimal bit array size (m) and hash count (k)
m, k = get_optimal_m_k(capacity=500000, error_rate=0.01)

# Generate k bit indices for an item
indices = get_hash_indices("user@example.com", m=m, k=k)
```

---

## Configuration

### Bit Array Memory Footprint

| Capacity ($n$) | Target Error Rate ($p$) | Bit Size ($m$) | Memory Size (Bytes) | Hash Count ($k$) |
| :--- | :--- | :--- | :--- | :--- |
| 10,000 | 1% (`0.01`) | 95,856 bits | ~11.7 KB | 7 |
| 100,000 | 1% (`0.01`) | 958,512 bits | ~117 KB | 7 |
| 1,000,000 | 1% (`0.01`) | 9,585,064 bits | ~1.14 MB | 7 |
| 1,000,000 | 0.1% (`0.001`) | 14,377,592 bits | ~1.71 MB | 10 |
| 10,000,000 | 0.1% (`0.001`) | 143,775,976 bits | ~17.1 MB | 10 |

---

## Examples

### 1. Bulk Rebuilding & Capacity Resizing (`rebuild`)

Flush existing filter state and repopulate it with fresh items. Pass a new `capacity` (or `error_rate`) to scale up the filter when dataset size grows:

```python
import redis
from bloomsieve import BloomFilterService

client = redis.Redis(host="localhost", port=6379, db=0)
service = BloomFilterService(redis_client=client, capacity=100000, error_rate=0.001)

# Refresh filter key and scale up capacity from 100,000 to 500,000 items
user_ids = [f"user_{i}" for i in range(250000)]

# Deletes existing filter (and local mmap file), creates filter with new capacity, and bulk-inserts items
success = service.rebuild("users:active", items=user_ids, capacity=500000, error_rate=0.001)
print(f"Filter rebuilt with increased capacity: {success}")
```

### 2. Database Query Stampede Guard

Protect databases from expensive lookups for non-existent keys:

```python
from bloomsieve import BloomFilter

db_guard = BloomFilter(capacity=500000, error_rate=0.001, filepath="db_keys.bloom")

def get_user_profile(user_id: str):
    if user_id not in db_guard:
        return None  # Guaranteed not to exist, return immediately!
    return query_database(user_id)

def query_database(user_id: str):
    return {"user_id": user_id, "name": "Alice"}
```

### 3. Production Threshold Monitoring & Zero-Downtime Swap (`rebuild` + `swap`)

Monitor filter load ratio (`load_ratio()`), dynamically scale capacity with headroom multiplier when threshold is reached, rebuild into a temporary key under a distributed lock (`acquire_lock`), and atomically rotate (`swap`):

```python
import redis
from bloomsieve import BloomFilterService

client = redis.Redis(host="localhost", port=6379, db=0)
service = BloomFilterService(redis_client=client, capacity=10000, error_rate=0.001, use_mmap=True)

def check_and_rebuild(live_key: str, items_generator, current_db_count: int, threshold: float = 0.8) -> bool:
    # 1. Check current load ratio and capacity from Redis
    info = service.get_info(live_key)
    if info["ratio"] >= threshold:
        # 2. Acquire distributed lock for rebuild safety
        lock_name = f"rebuild:{live_key}"
        if service.acquire_lock(lock_name, ttl=600):
            try:
                # 3. Calculate expanded capacity with 2.0x headroom multiplier
                new_capacity = max(1000, int(current_db_count * 2.0))
                temp_key = f"{live_key}:rebuild"

                # 4. Rebuild into temporary filter key with expanded capacity
                if service.rebuild(temp_key, items=items_generator, capacity=new_capacity):
                    # 5. Atomically swap temporary key to live filter (both in Redis & local mmap)
                    service.swap(temp_key, live_key)
                    return True
            finally:
                service.release_lock(lock_name)
    return False

# Usage: Automatically triggers rebuild when load ratio >= 80%
users_generator = (f"user_{i}@example.com" for i in range(15000))
rebuilt = check_and_rebuild("bloom:users", items_generator=users_generator, current_db_count=15000)
print(f"Filter rebuild and atomic swap executed: {rebuilt}")
```

---

## Performance Notes

- **Time Complexity**: $O(1)$ for both `add()` and `contains`/`exists` (computes 1 SHA-256 hash digest and sets/checks $k$ bits).
- **Space Efficiency**: ~9.6 bits per item for a 1% error rate ($p=0.01$), compared to hundreds of bytes per item required by standard Python `set` structures.
- **Sub-Microsecond Latency**: Disk `mmap` lookups leverage kernel page caching, executing lookups in under 1 microsecond off the Python GC heap.

---

## FAQ

### Can items be deleted from a Bloom filter?
No. Standard Bloom filters do not support deletion because bit positions are shared across elements. To reset state, use `clear()` or `BloomFilterService.rebuild()` / `swap()`.

### What happens if capacity is exceeded?
The filter continues operating, but the false positive rate increases beyond the initial target `error_rate`.

### Is `bloomsieve` thread-safe?
- `BloomFilter`: Standalone operations do not acquire GIL locks. For multi-threaded mutation, application-level locks are recommended.
- `BloomFilterService`: Thread operations are guarded internally by recursive `RLock` instances.

### Is Redis required?
No. `BloomFilter` is pure Python with zero external dependencies. Redis is only required when using `BloomFilterService`.

---

## Contributing

1. Clone repository:
   ```bash
   git clone https://github.com/deepak7448/bloomsieve.git
   cd bloomsieve
   ```
2. Install dev dependencies:
   ```bash
   pip install -e .[dev]
   ```
3. Run tests and linting:
   ```bash
   pytest
   ruff check .
   ```

---

## License

MIT License. See [LICENSE](LICENSE) for details.

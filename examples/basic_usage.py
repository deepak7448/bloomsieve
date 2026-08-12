"""Standalone Bloom filter example (no Redis required).

Run:
    python examples/basic_usage.py
"""

from bloomsieve import BloomFilter


def main():
    # 1. In-memory filter.
    print("--- 1. In-memory Bloom filter ---")
    bf = BloomFilter(capacity=1000, error_rate=0.01)
    bf.add("hello")
    print(f"'hello' in bf -> {'hello' in bf}")  # True
    print(f"'world' in bf -> {'world' in bf}")  # False
    print(f"m={bf.m} bits, k={bf.k} hash functions")

    # 2. Persistent mmap-backed filter.
    print("\n--- 2. mmap-backed (persistent) Bloom filter ---")
    path = "/tmp/bloomsieve_example.bloom"
    with BloomFilter(capacity=5000, error_rate=0.001, filepath=path) as disk_bf:
        disk_bf.add("session:abc")
        print(f"'session:abc' -> {'session:abc' in disk_bf}")
        print(f"'session:def' -> {'session:def' in disk_bf}")
        print(f"file size: {disk_bf.total_size} bytes (16-byte header + bit array)")

    # 3. Reopen the same file; data survives the process boundary conceptually.
    print("\n--- 3. Reopening the persisted filter ---")
    with BloomFilter(capacity=5000, error_rate=0.001, filepath=path) as reopened:
        print(f"'session:abc' -> {'session:abc' in reopened}")
    print(f"persisted at: {path}")


if __name__ == "__main__":
    main()

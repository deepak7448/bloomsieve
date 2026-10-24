"""Standalone in-memory Bloom filter (no Redis, no files).

Covers: sizing, add/lookup, str vs bytes, duplicate detection with ``add``'s
return value, batch inserts with ``add_many``, ``clear`` and the sizing helper.

Run:
    python examples/basic_usage.py
"""

from bloomsieve import BloomFilter, get_optimal_m_k


def main() -> None:
    # Size the filter for the number of items you expect and the false-positive
    # rate you can tolerate.  Memory is fixed up front: m bits.
    bf = BloomFilter(capacity=10_000, error_rate=0.01)
    print(f"{bf!r}: {bf.byte_size:,} bytes, {bf.k} hash functions")

    # add() returns True when the item was new, False when it was (probably) seen.
    print("add('alice') ->", bf.add("alice"))  # True
    print("add('alice') ->", bf.add("alice"))  # False: already present

    # Membership: False is a definite "never added"; True means "possibly added".
    print("'alice' in bf ->", "alice" in bf)  # True
    print("'mallory' in bf ->", "mallory" in bf)  # False (definite)

    # str and bytes are interchangeable: str is hashed as its UTF-8 encoding.
    bf.add(b"bob")
    print("'bob' in bf ->", "bob" in bf)  # True

    # Batch insert: one call, returns how many items were new.
    new = bf.add_many(f"user:{i}" for i in range(5_000))
    print(f"add_many: {new:,} new items")

    # Measure the real false-positive rate against items that were never added.
    false_positives = sum(f"other:{i}" in bf for i in range(10_000))
    print(f"observed false-positive rate: {false_positives / 10_000:.2%} (target 1%)")

    # clear() resets every bit but keeps the configuration.
    bf.clear()
    print("after clear, 'alice' in bf ->", "alice" in bf)  # False

    # The sizing maths on its own, e.g. to budget memory before creating anything.
    for capacity, error_rate in [(1_000_000, 0.01), (1_000_000, 0.001), (100_000_000, 0.001)]:
        m, k = get_optimal_m_k(capacity, error_rate)
        print(f"capacity={capacity:>11,} error_rate={error_rate:<6} -> {m // 8 / 2**20:8.1f} MiB, k={k}")


if __name__ == "__main__":
    main()

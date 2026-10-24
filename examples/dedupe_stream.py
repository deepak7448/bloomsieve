"""De-duplicate a stream (e.g. a web crawler's URL frontier) with a Bloom filter.

``add()`` returns ``True`` only for items it has not seen, so one call both checks
and records an item.  A false positive here means an unseen item is skipped, so
choose ``error_rate`` according to how costly a skipped item is.

Run:
    python examples/dedupe_stream.py
"""

import random

from bloomsieve import BloomFilter


def url_stream(n: int, seed: int = 7):
    """Yield n URLs with plenty of repeats, like links found while crawling."""
    rng = random.Random(seed)
    for _ in range(n):
        yield f"https://example.com/page/{rng.randrange(n // 3)}"


def main() -> None:
    seen = BloomFilter(capacity=50_000, error_rate=0.001)
    exact = set()  # only here to show how close the Bloom filter gets

    processed = 0
    for url in url_stream(100_000):
        if seen.add(url):  # True -> first time we see this URL
            processed += 1
        exact.add(url)

    print(f"unique URLs (exact):  {len(exact):,}")
    print(f"URLs processed:       {processed:,}")
    print(f"skipped as false positives: {len(exact) - processed:,}")
    print(f"filter memory: {seen.byte_size / 1024:.0f} KiB (a set of these strings needs several MiB)")


if __name__ == "__main__":
    main()

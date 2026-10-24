"""Putting the local pre-filter in front of a Redis filter that already has data.

A filter created outside Bloomsieve (or by an older version) has no change log,
so a local mirror cannot know which items it holds.  Bloomsieve does not trust
such a mirror: every lookup is verified in Redis (correct, just not faster).
``rebuild(name, items)`` from your source of truth puts the filter under
replication; afterwards mirrors on every host answer negatives locally.

Run (needs Redis with the Bloom module):
    pip install "bloomsieve[redis]"
    python examples/redis_adopt_existing.py
"""

import logging
import os
import tempfile

from bloomsieve import BloomFilterService

REDIS_URL = os.environ.get("BLOOMSIEVE_REDIS_URL", "redis://localhost:6379/0")


def connect():
    try:
        import redis
    except ImportError:
        print('Skipping: install the extra with  pip install "bloomsieve[redis]"')
        return None
    client = redis.from_url(REDIS_URL, socket_timeout=2)
    try:
        client.ping()
    except redis.exceptions.RedisError:
        print(f"Skipping: no Redis server reachable at {REDIS_URL}")
        return None
    return client


def main() -> None:
    logging.basicConfig(level=logging.WARNING, format="  [log] %(message)s")
    client = connect()
    if client is None:
        return

    name = "example:legacy_filter"
    items = [f"order:{i}" for i in range(5_000)]
    client.delete(name)
    # Data written to Redis before Bloomsieve's mirror existed.
    client.execute_command("BF.RESERVE", name, "0.001", "10000")
    client.execute_command("BF.MADD", name, *items)

    with BloomFilterService(
        client, capacity=10_000, error_rate=0.001, use_mmap=True, mmap_dir=tempfile.mkdtemp(prefix="bloomsieve_")
    ) as svc:
        try:
            # Not replicated yet: correct answers, verified in Redis (a warning is logged once).
            print("exists(order:1)  ->", svc.exists(name, "order:1"))  # True
            print("exists(order:x)  ->", svc.exists(name, "order:x"))  # False, but asked Redis

            # Rebuild from the source of truth.  This also publishes a snapshot, so
            # mirrors on other hosts load it instead of replaying every item.
            svc.rebuild(name, items, capacity=10_000, error_rate=0.001)

            # (Single-host deployments with replicate=False can instead fill the
            # mirror themselves and call svc.mark_synced(name).)

            print("exists(order:x)  ->", svc.exists(name, "order:x"), "(answered locally)")
        finally:
            svc.delete_filter(name)


if __name__ == "__main__":
    main()

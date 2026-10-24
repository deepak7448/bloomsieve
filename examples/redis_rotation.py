"""Zero-downtime rotation: rebuild into a temporary filter, then swap it live.

Bloom filters cannot delete items, and their false-positive rate rises once they
grow past capacity.  The fix is to periodically rebuild from your source of
truth.  This example shows the safe pattern:

1. take an advisory lock so only one worker rebuilds,
2. ``rebuild`` a *temporary* filter (the live one keeps serving reads),
3. ``swap`` it into place (Redis RENAME + local mirror rotation),
4. release the lock.

It also shows ``get_info``/``load_ratio`` as the trigger for a rebuild.

Run (needs Redis with the Bloom module):
    pip install "bloomsieve[redis]"
    python examples/redis_rotation.py
"""

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


def load_active_users():
    """Stand-in for your database: the authoritative set of items."""
    return (f"user:{i}" for i in range(0, 20_000, 2))


def rotate(svc: BloomFilterService, live: str) -> bool:
    if not svc.acquire_lock(f"rebuild:{live}", ttl=300):
        print("another worker is already rebuilding; skipping")
        return False
    try:
        temp = f"{live}:rebuilding"
        if not svc.rebuild(temp, load_active_users(), capacity=50_000, error_rate=0.001):
            return False
        return svc.swap(temp, live)
    finally:
        svc.release_lock(f"rebuild:{live}")


def main() -> None:
    client = connect()
    if client is None:
        return

    live = "example:active_users"
    client.delete(live)
    with BloomFilterService(
        client, capacity=1_000, error_rate=0.01, use_mmap=True, mmap_dir=tempfile.mkdtemp(prefix="bloomsieve_")
    ) as svc:
        try:
            # A filter that has outgrown its capacity.
            svc.create_filter(live, capacity=1_000)
            svc.add_many(live, (f"old:{i}" for i in range(3_000)))
            info = svc.get_info(live)
            print(f"before: {info['inserted']:,} items, load ratio {info['ratio']:.2f}")

            if svc.load_ratio(live) > 0.8:
                print("rotated:", rotate(svc, live))

            info = svc.get_info(live)
            print(f"after:  {info['inserted']:,} items, load ratio {info['ratio']:.2f}")
            print("exists(user:2) ->", svc.exists(live, "user:2"))  # True
            print("exists(old:1)  ->", svc.exists(live, "old:1"))  # False: gone after the rebuild
        finally:
            svc.delete_filter(live)
            svc.delete_filter(f"{live}:rebuilding")


if __name__ == "__main__":
    main()

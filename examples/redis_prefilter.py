"""Redis pre-filter: stop sending unnecessary membership checks to Redis.

Covers: ``create_filter``, ``add``/``add_many``, ``exists``/``exists_many``, and
counting how many Redis requests the local mirror saved.

Run (needs Redis with the Bloom module, e.g. Redis 8 or redis-stack):
    pip install "bloomsieve[redis]"
    python examples/redis_prefilter.py
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


def main() -> None:
    client = connect()
    if client is None:
        return

    name = "example:active_sessions"
    client.delete(name)
    with BloomFilterService(
        redis_client=client,
        capacity=100_000,
        error_rate=0.001,
        use_mmap=True,  # enable the local pre-filter
        mmap_dir=tempfile.mkdtemp(prefix="bloomsieve_"),
    ) as svc:
        try:
            # A brand-new Redis filter: the (empty) local mirror is trusted at once.
            svc.create_filter(name)

            svc.add(name, "session:1")
            svc.add_many(name, (f"session:{i}" for i in range(2, 10_001)))  # BF.MADD, one round trip

            print("exists(session:1)    ->", svc.exists(name, "session:1"))  # True, verified in Redis
            print("exists(session:9999999) ->", svc.exists(name, "session:9999999"))  # False, no Redis call

            # Batch lookups: local negatives are answered locally, the rest go to
            # Redis in a single BF.MEXISTS round trip.
            print("exists_many ->", svc.exists_many(name, ["session:5", "nope:1", "session:6", "nope:2"]))

            # Count how many Redis requests a negative-heavy workload sends.
            sent = []
            real = client.execute_command

            def counting(*args, **kwargs):
                sent.append(args[0])
                return real(*args, **kwargs)

            client.execute_command = counting
            queries = [f"session:{i}" for i in range(100)] + [f"unknown:{i}" for i in range(9_900)]
            hits = sum(svc.exists(name, q) for q in queries)
            client.execute_command = real
            print(f"{len(queries):,} lookups, {hits} hits, only {len(sent):,} Redis requests")
        finally:
            svc.delete_filter(name)


if __name__ == "__main__":
    main()

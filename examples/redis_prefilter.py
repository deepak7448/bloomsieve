"""Redis pre-filter example: stop sending unnecessary membership checks to Redis.

Run (needs Redis with the RedisBloom module on localhost:6379):
    pip install "bloomsieve[redis]"
    python examples/redis_prefilter.py
"""

import os
import tempfile

import redis

from bloomsieve import BloomFilterService

REDIS_URL = os.environ.get("BLOOMSIEVE_REDIS_URL", "redis://localhost:6379/0")


def main():
    client = redis.from_url(REDIS_URL, socket_timeout=2)
    mmap_dir = tempfile.mkdtemp(prefix="bloomsieve_")

    # The local mmap filter mirrors items as they are added through the service.
    service = BloomFilterService(
        redis_client=client,
        capacity=100_000,
        error_rate=0.001,
        use_mmap=True,
        mmap_dir=mmap_dir,
    )

    try:
        client.ping()
    except redis.exceptions.RedisError:
        print(f"Skipping Redis demo: no Redis server reachable at {REDIS_URL}.")
        return

    filter_name = "active_sessions"
    try:
        service.create_filter(filter_name)

        for session in ("session:1001", "session:1002", "session:1003"):
            service.add(filter_name, session)

        # Positive query: the local filter says "possibly present", so RedisBloom
        # verifies and returns the authoritative answer.
        print("exists('active_sessions', 'session:1001') ->",
              service.exists(filter_name, "session:1001"))  # True

        # Negative query: the local filter says "definitely absent", so the answer
        # is returned locally without a single round-trip to Redis.
        print("exists('active_sessions', 'session:9999') ->",
              service.exists(filter_name, "session:9999"))  # False, no Redis call

    finally:
        client.delete(filter_name)
        service._close_mmap(filter_name)


if __name__ == "__main__":
    main()

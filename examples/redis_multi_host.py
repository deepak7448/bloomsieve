"""Several application servers, each with its own local pre-filter.

Every server keeps a local mirror on its own disk.  Writes made on any server are
recorded in a change log in Redis (``replicate=True``, the default), and each
mirror catches up on that log before answering "absent", so no server ever
misses another server's items for longer than ``sync_interval`` seconds.

This script simulates two servers in one process by giving each service its own
``mmap_dir``: they share nothing but Redis, exactly like separate machines.

Run (needs Redis >= 6.2 with the Bloom module, e.g. Redis 8):
    pip install "bloomsieve[redis]"
    python examples/redis_multi_host.py
"""

import os
import tempfile
import time

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


def server(client, label: str) -> BloomFilterService:
    return BloomFilterService(
        client,
        capacity=100_000,
        error_rate=0.001,
        use_mmap=True,
        mmap_dir=tempfile.mkdtemp(prefix=f"bloomsieve_{label}_"),
        sync_interval=0.2,  # a local "absent" is at most 0.2 s behind other servers
    )


def main() -> None:
    client = connect()
    if client is None:
        return

    name = "example:revoked_tokens"
    web1, web2 = server(client, "web1"), server(client, "web2")
    try:
        # Every server calls this at startup; the first one creates the filter.
        web1.create_filter(name)
        web2.create_filter(name)

        print("web2: is tok_123 revoked? ", web2.exists(name, "tok_123"))  # False, answered locally

        web1.add(name, "tok_123")  # revoked on another server
        time.sleep(0.25)  # wait for longer than sync_interval

        # web2 catches up on the change log before answering, so it now knows.
        print("web2: is tok_123 revoked? ", web2.exists(name, "tok_123"))  # True

        # Bulk writes replicate the same way, and a server that starts later
        # loads a compact snapshot plus the recent log instead of every item.
        web1.add_many(name, (f"tok_{i}" for i in range(10_000)))
        web3 = server(client, "web3")
        print("web3 (new): tok_9999 revoked?", web3.exists(name, "tok_9999"))  # True
        print("web3 (new): tok_x revoked?   ", web3.exists(name, "tok_x"))  # False, locally
        web3.close()
    finally:
        web1.close()
        web2.close()
        web1.delete_filter(name)


if __name__ == "__main__":
    main()

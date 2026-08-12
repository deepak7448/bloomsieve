"""Live integration tests against a real Redis server with RedisBloom.

These tests run only when the ``BLOOMSIEVE_REDIS_URL`` environment variable is set
(e.g. ``redis://localhost:6379/0``).  Ordinary unit tests never require a live
server; this suite is opt-in.

Example:
    BLOOMSIEVE_REDIS_URL=redis://localhost:6379/0 pytest tests/test_integration_redis.py
"""

import os
import uuid

import pytest

pytest.importorskip("redis")

REDIS_URL = os.environ.get("BLOOMSIEVE_REDIS_URL")

pytestmark = pytest.mark.skipif(
    not REDIS_URL,
    reason="BLOOMSIEVE_REDIS_URL not set; skipping live Redis integration tests",
)

import redis  # noqa: E402

from bloomsieve import BloomFilter, BloomFilterService  # noqa: E402


def _unique(name):
    return f"bloomsieve:{name}:{uuid.uuid4().hex[:10]}"


@pytest.fixture
def client():
    return redis.from_url(REDIS_URL, socket_timeout=5)


@pytest.fixture
def service(client, tmp_path):
    svc = BloomFilterService(
        redis_client=client,
        capacity=1000,
        error_rate=0.01,
        use_mmap=True,
        mmap_dir=str(tmp_path / "bloom_filters"),
    )
    yield svc
    for name in list(BloomFilterService._mmaps):
        svc._close_mmap(name)


def test_live_core_mmap_roundtrip(client, tmp_path):
    path = str(tmp_path / "live.bloom")
    bf = BloomFilter(capacity=1000, error_rate=0.01, filepath=path)
    bf.add("alice")
    assert "alice" in bf
    assert "bob" not in bf
    bf.close()

    reopened = BloomFilter(capacity=1000, error_rate=0.01, filepath=path)
    assert "alice" in reopened
    assert "bob" not in reopened
    reopened.close()


def test_live_create_add_exists(client):
    key = _unique("basic")
    svc = BloomFilterService(redis_client=client, capacity=1000, error_rate=0.01)
    try:
        assert svc.create_filter(key) is True
        assert svc.add(key, "hello") is True
        assert svc.exists(key, "hello") is True
        assert svc.exists(key, "world") is False
    finally:
        client.delete(key)


def test_live_local_negative_avoids_redis_requests(client, tmp_path):
    key = _unique("prefilter")
    svc = BloomFilterService(
        redis_client=client,
        capacity=500,
        error_rate=0.01,
        use_mmap=True,
        mmap_dir=str(tmp_path / "bloom_filters"),
    )
    try:
        svc.create_filter(key, 500, 0.01)
        for i in range(300):
            assert svc.add(key, f"user:{i}") is True

        original = svc.redis.execute_command
        exists_calls = []

        def counting(*args, **kwargs):
            if args and args[0] == "BF.EXISTS":
                exists_calls.append(args)
            return original(*args, **kwargs)

        svc.redis.execute_command = counting

        # The bulk of a negative-heavy workload resolves locally: no BF.EXISTS calls.
        for i in range(300, 600):
            assert svc.exists(key, f"user:{i}") is False
        assert exists_calls == []

        # A present item is a possible positive locally and verifies against Redis.
        assert svc.exists(key, "user:0") is True
        assert ("BF.EXISTS", key, "user:0") in exists_calls
    finally:
        svc.redis.execute_command = client.execute_command
        client.delete(key)


def test_live_rebuild_and_swap(client, tmp_path):
    svc = BloomFilterService(
        redis_client=client,
        capacity=1000,
        error_rate=0.01,
        use_mmap=True,
        mmap_dir=str(tmp_path / "bloom_filters"),
    )
    live = _unique("live")
    temp = _unique("temp")
    try:
        assert svc.rebuild(temp, [f"item:{i}" for i in range(200)], 1000, 0.01) is True
        assert svc.exists(temp, "item:0") is True

        assert svc.swap(temp, live) is True
        assert svc.exists(live, "item:199") is True
        assert svc.exists(live, "missing") is False
        assert svc.exists(temp, "item:199") is False  # temp key is gone in Redis

        # The local mmap mirror followed the swap and now serves the live generation.
        bf = svc._init_mmap(live)
        assert bf is not None
        assert "item:199" in bf
    finally:
        client.delete(live)
        client.delete(temp)
        for name in list(BloomFilterService._mmaps):
            svc._close_mmap(name)


def test_live_locking(client):
    svc = BloomFilterService(redis_client=client)
    lock = _unique("lock")
    try:
        assert svc.acquire_lock(lock, ttl=60) is True
        assert svc.acquire_lock(lock, ttl=60) is False  # already held
        assert svc.release_lock(lock) is True
        assert svc.acquire_lock(lock, ttl=60) is True
        svc.release_lock(lock)
    finally:
        client.delete(f"lock:{lock}")


def test_live_get_info(client):
    key = _unique("info")
    svc = BloomFilterService(redis_client=client, capacity=1000, error_rate=0.01)
    try:
        svc.create_filter(key)
        svc.add(key, "a")
        info = svc.get_info(key)
        assert info["capacity"] >= 1000  # RedisBloom may round capacity up
        assert info["inserted"] == 1
        assert 0.0 <= info["ratio"] <= 1.0
    finally:
        client.delete(key)

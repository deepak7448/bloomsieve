"""Live integration tests against a real Redis server with RedisBloom.

These tests run only when the ``BLOOMSIEVE_REDIS_URL`` environment variable is set
(e.g. ``redis://localhost:6379/0``).  Ordinary unit tests never require a live
server; this suite is opt-in.

Example:
    BLOOMSIEVE_REDIS_URL=redis://localhost:6379/0 pytest tests/test_integration_redis.py
"""

import logging
import os
import time
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


def _drop(client, *names):
    probe = BloomFilterService(redis_client=client)
    for name in names:
        probe.delete_filter(name)


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
    svc.close()


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
        _drop(client, key)


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
        _drop(client, key)


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
        _drop(client, live)
        _drop(client, temp)
        svc.close()


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


def test_live_lock_is_released_only_by_its_owner(client):
    owner = BloomFilterService(redis_client=client)
    other = BloomFilterService(redis_client=client)
    lock = _unique("lock")
    try:
        assert owner.acquire_lock(lock, ttl=60) is True
        assert other.release_lock(lock) is False  # never acquired it
        assert client.exists(f"lock:{lock}") == 1
        client.set(f"lock:{lock}", "someone-else")  # simulate expiry + takeover
        assert owner.release_lock(lock) is False
        assert client.get(f"lock:{lock}") == b"someone-else"
    finally:
        client.delete(f"lock:{lock}")


def test_live_batch_add_and_exists(service, client):
    key = _unique("batch")
    try:
        assert service.create_filter(key, 5000, 0.01) is True
        items = [f"user:{i}" for i in range(2500)]
        assert service.add_many(key, items) is True
        probe = items[::50] + [f"nope:{i}" for i in range(50)]
        answers = service.exists_many(key, probe)
        assert answers[:50] == [True] * 50
        assert sum(answers[50:]) <= 3  # false positives only, and rare
        assert service.get_info(key)["inserted"] == 2500
    finally:
        _drop(client, key)


def test_live_swap_without_local_temp_falls_back_to_redis(service, client):
    live, temp = _unique("live"), _unique("temp")
    try:
        service.create_filter(live)
        service.add(live, "old")
        client.execute_command("BF.ADD", temp, "new")  # built by someone else
        assert service.swap(temp, live) is True
        assert service.exists(live, "new") is True
    finally:
        _drop(client, live, temp)


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
        _drop(client, key)


# --------------------------------------------------------------------------- multi-host
#
# Each "host" is a BloomFilterService with its own mmap_dir, so the hosts share
# nothing but Redis -- exactly like application servers on different machines.


@pytest.fixture
def hosts(client, tmp_path):
    created = []
    names = []

    def make(label, **kwargs):
        kwargs.setdefault("sync_interval", 0)
        svc = BloomFilterService(
            redis_client=client,
            capacity=1000,
            error_rate=0.01,
            use_mmap=kwargs.pop("use_mmap", True),
            mmap_dir=str(tmp_path / label),
            **kwargs,
        )
        created.append(svc)
        return svc

    def name(label):
        key = _unique(label)
        names.append(key)
        return key

    make.name = name
    yield make
    for svc in created:
        svc.close()
    _drop(client, *names)


def _count_bf_exists(svc):
    calls = []
    original = svc.redis.execute_command

    def counting(*args, **kwargs):
        if args and args[0] in ("BF.EXISTS", "BF.MEXISTS"):
            calls.append(args[0])
        return original(*args, **kwargs)

    svc.redis = _Proxy(svc.redis, execute_command=counting)
    return calls


class _Proxy:
    def __init__(self, target, **overrides):
        self._target = target
        self.__dict__.update(overrides)

    def __getattr__(self, attr):
        return getattr(self._target, attr)


def test_live_item_added_on_one_host_is_seen_on_another(hosts):
    # The reported bug: host B used to answer "absent" for host A's items.
    a, b = hosts("host-a"), hosts("host-b")
    key = hosts.name("users")
    assert b.create_filter(key)  # B created the filter, so it trusts its own mirror
    assert a.create_filter(key)  # every server calls this at startup
    assert b.exists(key, "alice") is False  # B answers locally

    assert a.add(key, "alice")
    assert a.add_many(key, [f"user:{i}" for i in range(500)])
    assert b.exists(key, "alice") is True
    assert all(b.exists_many(key, [f"user:{i}" for i in range(500)]))


def test_live_synced_negatives_are_answered_locally(hosts):
    a, b = hosts("host-a"), hosts("host-b", sync_interval=60)
    key = hosts.name("users")
    a.create_filter(key)
    a.add_many(key, [f"user:{i}" for i in range(300)])
    assert b.exists(key, "user:1") is True  # first lookup syncs B

    calls = _count_bf_exists(b)
    assert [b.exists(key, f"nobody:{i}") for i in range(300)].count(True) <= 3
    assert len(calls) <= 3  # only false positives reached Redis


def test_live_staleness_is_bounded_by_sync_interval(hosts):
    a, b = hosts("host-a"), hosts("host-b", sync_interval=0.3)
    key = hosts.name("users")
    a.create_filter(key)
    assert b.exists(key, "warm-up") is False  # B synced just now

    a.add(key, "late")
    # Within the interval B may still answer from its last sync...
    time.sleep(0.35)
    # ...but never after it.
    assert b.exists(key, "late") is True


def test_live_writer_without_mmap_is_replicated(hosts):
    writer = hosts("writer", use_mmap=False)
    reader = hosts("reader")
    key = hosts.name("users")
    writer.create_filter(key)
    writer.add(key, "from-a-writer-only-host")
    assert reader.exists(key, "from-a-writer-only-host") is True


def test_live_late_joiner_bootstraps_from_compacted_snapshot(hosts, client):
    a = hosts("host-a", max_log_entries=20)  # compaction threshold: 10 entries
    key = hosts.name("users")
    a.create_filter(key)
    for i in range(30):
        a.add(key, f"user:{i}")
    assert a.exists(key, "nobody") is False
    assert client.xlen(a._log_key(key)) < 10  # A compacted the log as it went

    late = hosts("host-late")
    assert all(late.exists(key, f"user:{i}") for i in range(30))
    assert late.exists(key, "nobody") is False


def test_live_host_behind_a_compaction_catches_up(hosts, client):
    a = hosts("host-a", max_log_entries=20)
    b = hosts("host-b", max_log_entries=20)
    key = hosts.name("users")
    a.create_filter(key)
    assert b.exists(key, "x") is False  # B's cursor is at the start

    for i in range(40):
        a.add(key, f"user:{i}")
    assert a.exists(key, "nobody") is False  # compacts: B's entries are trimmed
    assert all(b.exists(key, f"user:{i}") for i in range(40))


def test_live_rebuild_and_swap_on_one_host_reaches_the_others(hosts):
    a, b = hosts("host-a"), hosts("host-b")
    live, temp = hosts.name("live"), hosts.name("temp")
    a.create_filter(live)
    a.add(live, "old")
    assert b.exists(live, "old") is True

    assert a.rebuild(temp, ["new-1", "new-2"])
    assert a.swap(temp, live)
    assert b.exists(live, "new-1") is True
    calls = _count_bf_exists(b)
    assert b.exists(live, "old") is False  # gone after rotation, answered locally
    assert calls == []


def test_live_overflowing_log_disables_replication_safely(hosts, caplog):
    writer = hosts("writer", use_mmap=False, max_log_entries=5)
    reader = hosts("reader")
    key = hosts.name("users")
    writer.create_filter(key)
    with caplog.at_level(logging.ERROR):
        for i in range(10):
            writer.add(key, f"user:{i}")
    assert "replication is disabled" in caplog.text
    # No snapshot any more: the reader cannot trust its mirror and asks Redis.
    assert all(reader.exists(key, f"user:{i}") for i in range(10))


def test_live_unreachable_redis_never_yields_a_local_negative(hosts):
    a, b = hosts("host-a"), hosts("host-b")
    key = hosts.name("users")
    a.create_filter(key)
    assert b.exists(key, "x") is False

    def down(*args, **kwargs):
        raise ConnectionError("redis is down")

    b.redis = _Proxy(b.redis, pipeline=down, execute_command=down)
    assert b.exists(key, "anything") is True  # conservative, not a stale "absent"


def test_live_existing_filter_needs_a_rebuild_for_replication(hosts, client):
    key = hosts.name("legacy")
    client.execute_command("BF.RESERVE", key, "0.01", "1000")
    client.execute_command("BF.ADD", key, "pre-existing")
    a, b = hosts("host-a"), hosts("host-b")
    assert a.create_filter(key)  # already exists: not under replication
    assert b.exists(key, "pre-existing") is True  # untrusted mirror -> Redis

    assert a.rebuild(key, ["pre-existing", "more"])
    calls = _count_bf_exists(b)
    assert b.exists(key, "unknown") is False
    assert calls == []


def test_live_restart_resumes_without_downloading_the_snapshot(hosts, caplog, tmp_path):
    a = hosts("host-a")
    key = hosts.name("users")
    a.create_filter(key)
    a.add_many(key, [f"user:{i}" for i in range(100)])
    b = hosts("host-b")
    assert b.exists(key, "user:5") is True
    b.close()

    restarted = hosts("host-b")
    with caplog.at_level(logging.INFO):
        assert restarted.exists(key, "nobody") is False
    assert "loading local mirror" not in caplog.text

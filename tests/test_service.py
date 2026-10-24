import logging
import os
from unittest.mock import MagicMock

import pytest

from bloomsieve import BloomFilterService


class FakeBloomRedis:
    """Tiny in-memory stand-in for a RedisBloom server (exact sets, no false positives)."""

    def __init__(self):
        self.filters = {}
        self.strings = {}
        self.calls = []

    def execute_command(self, cmd, name, *args):
        self.calls.append(cmd)
        if cmd == "BF.RESERVE":
            if name in self.filters:
                raise Exception("ERR item exists")
            self.filters[name] = set()
            return "OK"
        if cmd == "BF.ADD":
            self.filters.setdefault(name, set()).add(args[0])
            return 1
        if cmd == "BF.MADD":
            self.filters.setdefault(name, set()).update(args)
            return [1] * len(args)
        if cmd == "BF.EXISTS":
            return int(args[0] in self.filters.get(name, ()))
        if cmd == "BF.MEXISTS":
            return [int(a in self.filters.get(name, ())) for a in args]
        raise NotImplementedError(cmd)

    def pipeline(self, transaction=True):
        return _FakePipeline(self)

    def delete(self, *names):
        return sum(self.filters.pop(n, None) is not None or self.strings.pop(n, None) is not None for n in names)

    def rename(self, src, dst):
        if src not in self.filters:
            raise Exception("ERR no such key")
        self.filters[dst] = self.filters.pop(src)
        return True


class _FakePipeline:
    def __init__(self, redis):
        self.redis = redis
        self.queued = []

    def execute_command(self, *args):
        self.queued.append(args)

    def execute(self):
        return [self.redis.execute_command(*args) for args in self.queued]


@pytest.fixture
def service(tmp_path):
    mock_redis = MagicMock()
    svc = BloomFilterService(
        redis_client=mock_redis,
        capacity=1000,
        error_rate=0.01,
        use_mmap=False,
        mmap_dir=str(tmp_path),
        replicate=False,
    )
    yield svc
    svc.close()


@pytest.fixture
def fake():
    return FakeBloomRedis()


@pytest.fixture
def mirrored(fake, tmp_path):
    """A single-host (replicate=False) service with a local mirror in front of the fake RedisBloom."""
    svc = BloomFilterService(
        fake, capacity=1000, error_rate=0.01, use_mmap=True, mmap_dir=str(tmp_path), replicate=False
    )
    yield svc
    svc.close()


def redis_exec_args(mock):
    return [call.args for call in mock.execute_command.call_args_list]


class TestCreateFilter:
    def test_creates_filter(self, service):
        service.redis.execute_command.return_value = "OK"
        assert service.create_filter("test_filter", 5000, 0.01) is True
        service.redis.execute_command.assert_called_with("BF.RESERVE", "test_filter", "0.01", "5000", "EXPANSION", "2")

    def test_already_exists_is_success(self, service):
        service.redis.execute_command.side_effect = Exception("ERR item exists")
        assert service.create_filter("test_filter") is True

    def test_failure_returns_false_and_logs(self, service, caplog):
        service.redis.execute_command.side_effect = Exception("Connection Refused")
        with caplog.at_level(logging.WARNING):
            assert service.create_filter("test_filter") is False
        assert "BF.RESERVE failed" in caplog.text

    def test_camel_case_alias_still_works(self, service):
        service.redis.execute_command.return_value = "OK"
        assert service.createFilter("legacy_filter", 5000, 0.01) is True
        assert service.createFilter == service.create_filter


class TestAdd:
    def test_add_ok(self, service):
        service.redis.execute_command.return_value = 1
        assert service.add("test_filter", "test_item") is True
        service.redis.execute_command.assert_called_with("BF.ADD", "test_filter", "test_item")

    def test_add_redis_failure_returns_false_and_logs(self, service, caplog):
        service.redis.execute_command.side_effect = Exception("Connection Refused")
        with caplog.at_level(logging.WARNING):
            assert service.add("test_filter", "test_item") is False
        assert "BF.ADD failed" in caplog.text

    def test_add_many_uses_madd_in_one_round_trip(self, mirrored, fake):
        mirrored.create_filter("users")
        items = [f"user:{i}" for i in range(2500)]
        assert mirrored.add_many("users", items) is True
        assert fake.filters["users"] == set(items)
        assert fake.calls.count("BF.MADD") == 3  # 1000 + 1000 + 500, one pipeline
        assert all(item in mirrored._init_mmap("users") for item in items)

    def test_add_many_empty_is_noop(self, service):
        assert service.add_many("users", []) is True
        service.redis.pipeline.assert_not_called()

    def test_add_many_failure_returns_false(self, service, caplog):
        service.redis.pipeline.return_value.execute.side_effect = Exception("boom")
        with caplog.at_level(logging.WARNING):
            assert service.add_many("users", ["a"]) is False
        assert "BF.MADD failed" in caplog.text


class TestExists:
    def test_redis_says_present(self, service):
        service.redis.execute_command.return_value = 1
        assert service.exists("test_filter", "hello") is True
        assert ("BF.EXISTS", "test_filter", "hello") in redis_exec_args(service.redis)

    def test_redis_says_absent(self, service):
        service.redis.execute_command.return_value = 0
        assert service.exists("test_filter", "hello") is False

    def test_redis_failure_falls_back_to_true_and_logs(self, service, caplog):
        service.redis.execute_command.side_effect = Exception("Connection Refused")
        with caplog.at_level(logging.WARNING):
            assert service.exists("test_filter", "hello") is True
        assert "BF.EXISTS failed" in caplog.text

    def test_exists_many_mixes_local_and_redis_answers(self, mirrored, fake):
        mirrored.create_filter("users")
        mirrored.add_many("users", ["a", "b"])
        fake.calls.clear()
        assert mirrored.exists_many("users", ["a", "zzz", "b", "nope"]) == [True, False, True, False]
        assert fake.calls == ["BF.MEXISTS"]  # only the possible positives went to Redis

    def test_exists_many_all_local_negatives_skip_redis(self, mirrored, fake):
        mirrored.create_filter("users")
        mirrored.add("users", "a")
        fake.calls.clear()
        assert mirrored.exists_many("users", ["x1", "x2"]) == [False, False]
        assert fake.calls == []

    def test_exists_many_redis_failure_is_conservative(self, service, caplog):
        service.redis.pipeline.return_value.execute.side_effect = Exception("down")
        with caplog.at_level(logging.WARNING):
            assert service.exists_many("users", ["a", "b"]) == [True, True]
        assert "BF.MEXISTS failed" in caplog.text


class TestCorePromise:
    """The product's central behaviour: definite-negative local answers avoid Redis."""

    def test_local_negative_lookup_skips_redis(self, mirrored, fake):
        mirrored.create_filter("users")
        for i in range(200):
            mirrored.add("users", f"user:{i}")
        fake.calls.clear()
        assert mirrored.exists("users", "user-absent") is False
        assert fake.calls == []

    def test_possible_positive_falls_back_to_redis(self, mirrored, fake):
        mirrored.create_filter("users")
        mirrored.add("users", "user:1")
        fake.calls.clear()
        assert mirrored.exists("users", "user:1") is True
        assert fake.calls == ["BF.EXISTS"]

    def test_cold_local_filter_falls_back_to_redis(self, mirrored, fake, caplog):
        fake.filters["brand_new_filter"] = {"anything"}
        with caplog.at_level(logging.WARNING):
            assert mirrored.exists("brand_new_filter", "anything") is True
        assert "not known to hold every item" in caplog.text
        assert fake.calls == ["BF.EXISTS"]

    def test_adding_to_untrusted_mirror_does_not_make_it_trusted(self, mirrored, fake):
        # Regression: Redis already holds items the brand-new local file never saw.
        fake.filters["users"] = {"alice", "bob"}
        mirrored.add("users", "carol")
        assert mirrored.exists("users", "alice") is True

    def test_existing_redis_filter_leaves_mirror_untrusted(self, mirrored, fake):
        fake.filters["users"] = {"alice"}
        assert mirrored.create_filter("users") is True  # "item exists"
        assert mirrored.exists("users", "alice") is True

    def test_trust_survives_reopen(self, fake, tmp_path):
        svc = BloomFilterService(fake, 1000, 0.01, use_mmap=True, mmap_dir=str(tmp_path), replicate=False)
        svc.create_filter("users")
        svc.add("users", "alice")
        svc.close()

        restarted = BloomFilterService(fake, 1000, 0.01, use_mmap=True, mmap_dir=str(tmp_path), replicate=False)
        fake.calls.clear()
        assert restarted.exists("users", "nobody") is False
        assert fake.calls == []
        restarted.close()

    def test_recreated_file_is_not_trusted_by_a_leftover_marker(self, mirrored, fake):
        mirrored.create_filter("users")
        mirrored.add("users", "alice")
        mirrored.close()
        os.remove(mirrored._mmap_path("users"))  # e.g. an operator deleted the file
        assert mirrored.exists("users", "alice") is True

    def test_mark_synced(self, mirrored, fake):
        fake.filters["users"] = {"alice"}
        mirrored.add("users", "alice")  # mirror now holds everything Redis has
        assert mirrored.mark_synced("users") is True
        fake.calls.clear()
        assert mirrored.exists("users", "nobody") is False
        assert fake.calls == []

    def test_mark_synced_without_mmap(self, service):
        assert service.mark_synced("users") is False

    def test_mmap_disabled_always_queries_redis(self, service):
        service.redis.execute_command.return_value = 0
        assert service.exists("test_filter", "anything") is False
        assert ("BF.EXISTS", "test_filter", "anything") in redis_exec_args(service.redis)


class TestMmap:
    def test_init_mmap_disabled_returns_none(self, service):
        assert service._init_mmap("test_filter") is None

    def test_init_mmap_enabled_creates_filter_file(self, service):
        service.use_mmap = True
        bf = service._init_mmap("test_filter")
        assert bf is not None
        assert os.path.exists(service._mmap_path("test_filter"))

    def test_safe_names_are_used_verbatim(self, service):
        assert service._mmap_path("users.v2-a_b") == os.path.join(service.mmap_dir, "users.v2-a_b.bloom")

    def test_unsafe_names_are_sanitised_without_collisions(self, service):
        path = service._mmap_path("some/path:with spaces")
        assert os.path.basename(path).startswith("some_path_with_spaces-")
        assert service._mmap_path("a/b") != service._mmap_path("a_b")

    def test_services_with_different_dirs_do_not_share_mirrors(self, fake, tmp_path):
        # Regression: the mirror cache used to be class-level and keyed by name only.
        a = BloomFilterService(fake, 1000, 0.01, use_mmap=True, mmap_dir=str(tmp_path / "a"), replicate=False)
        b = BloomFilterService(fake, 1000, 0.01, use_mmap=True, mmap_dir=str(tmp_path / "b"), replicate=False)
        a.add("f", "x")
        b.add("f", "y")
        assert os.path.exists(b._mmap_path("f"))
        assert "y" not in a._init_mmap("f")
        a.close()
        b.close()

    def test_mirror_replaced_by_another_process_is_reopened(self, fake, tmp_path):
        reader = BloomFilterService(
            fake, 1000, 0.01, use_mmap=True, mmap_dir=str(tmp_path), sync_interval=0, replicate=False
        )
        writer = BloomFilterService(fake, 1000, 0.01, use_mmap=True, mmap_dir=str(tmp_path), replicate=False)
        reader.create_filter("users")
        reader.add("users", "old")
        assert reader.exists("users", "new") is False

        writer.rebuild("users", ["new"])  # replaces the file on disk
        assert reader.exists("users", "new") is True
        reader.close()
        writer.close()

    def test_mmap_open_failure_falls_back_to_redis(self, service, monkeypatch, caplog):
        service.use_mmap = True

        def broken_init(*args, **kwargs):
            raise OSError("cannot open backing file")

        monkeypatch.setattr("bloomsieve.redis_service.BloomFilter", broken_init)
        service.redis.execute_command.return_value = 1
        with caplog.at_level(logging.WARNING):
            assert service.exists("users", "anything") is True
        assert "failed to open local mmap filter" in caplog.text
        assert ("BF.EXISTS", "users", "anything") in redis_exec_args(service.redis)

    def test_flush_and_close(self, mirrored):
        mirrored.add("users", "a")
        mirrored.flush()
        mirrored.flush("users")
        mirrored.close("users")
        assert mirrored._mirrors == {}

    def test_context_manager_closes_mirrors(self, fake, tmp_path):
        with BloomFilterService(fake, 1000, 0.01, use_mmap=True, mmap_dir=str(tmp_path), replicate=False) as svc:
            svc.add("users", "a")
            bf = svc._init_mmap("users")
        assert bf.closed


class TestRebuild:
    def test_rebuild_basic(self, service):
        service.redis.execute_command.return_value = "OK"
        assert service.rebuild("test_filter", ["a", "b", "c"], 5000, 0.01) is True
        service.redis.delete.assert_called_with("test_filter")
        assert ("BF.MADD", "test_filter", "a", "b", "c") in redis_exec_args(service.redis)

    def test_rebuild_chunks_large_item_sets(self, service):
        service.redis.execute_command.return_value = "OK"
        assert service.rebuild("test_filter", (str(i) for i in range(2500)), 5000, 0.01) is True
        madd = [args for args in redis_exec_args(service.redis) if args[0] == "BF.MADD"]
        assert [len(args) - 2 for args in madd] == [1000, 1000, 500]

    def test_rebuild_with_none_items(self, service):
        service.redis.execute_command.return_value = "OK"
        assert service.rebuild("test_filter", None) is True
        assert service.rebuild("test_filter", [None, "", None]) is True
        madd = [args for args in redis_exec_args(service.redis) if args[0] == "BF.MADD"]
        assert madd == [("BF.MADD", "test_filter", "")]  # None skipped, "" is a real item

    def test_rebuild_returns_false_when_reserve_fails(self, service, caplog):
        service.redis.execute_command.side_effect = Exception("Connection Refused")
        with caplog.at_level(logging.ERROR):
            assert service.rebuild("test_filter", ["a"]) is False
        assert "could not reserve" in caplog.text

    def test_rebuild_returns_false_when_insert_fails(self, service, caplog):
        def fail_on_madd(cmd, *args):
            if cmd == "BF.MADD":
                raise Exception("boom")
            return "OK"

        service.redis.execute_command.side_effect = fail_on_madd
        with caplog.at_level(logging.ERROR):
            assert service.rebuild("test_filter", ["a", "b"]) is False
        assert "rebuild of test_filter failed" in caplog.text

    def test_rebuild_leaves_a_trusted_complete_mirror(self, mirrored, fake):
        fake.filters["users"] = {"stale"}
        assert mirrored.rebuild("users", ["alice", "bob"]) is True
        assert fake.filters["users"] == {"alice", "bob"}
        fake.calls.clear()
        assert mirrored.exists("users", "stale") is False
        assert mirrored.exists("users", "nobody") is False
        assert fake.calls == []


class TestSwap:
    def test_swap_renames_in_redis(self, service):
        service.redis.rename.return_value = True
        assert service.swap("temp_filter", "live_filter") is True
        service.redis.rename.assert_called_with("temp_filter", "live_filter")

    def test_swap_returns_false_when_redis_rename_fails(self, service, monkeypatch, caplog):
        service.use_mmap = True
        service.redis.rename.side_effect = Exception("ERR no such key")
        replaced = []
        monkeypatch.setattr(
            "bloomsieve.redis_service.os.replace",
            lambda src, dst: replaced.append((src, dst)),
        )
        with caplog.at_level(logging.ERROR):
            assert service.swap("temp_filter", "live_filter") is False
        assert replaced == []  # local files untouched when the Redis rename failed
        assert "Redis rename" in caplog.text

    def test_swap_rotates_trusted_local_mirror(self, mirrored, fake):
        mirrored.create_filter("bloom:live")
        mirrored.add("bloom:live", "old")
        mirrored.rebuild("bloom:temp", ["alice"])

        assert mirrored.swap("bloom:temp", "bloom:live") is True
        assert not os.path.exists(mirrored._mmap_path("bloom:temp"))
        assert "alice" in mirrored._init_mmap("bloom:live")
        fake.calls.clear()
        assert mirrored.exists("bloom:live", "old") is False  # trusted, answered locally
        assert fake.calls == []

    def test_swap_reports_failure_when_local_replace_fails(self, mirrored, fake, monkeypatch, caplog):
        mirrored.create_filter("bloom:live")
        mirrored.rebuild("bloom:temp", ["new"])
        monkeypatch.setattr("bloomsieve.redis_service.os.replace", _raising_replace)
        with caplog.at_level(logging.ERROR):
            assert mirrored.swap("bloom:temp", "bloom:live") is False
        assert "failed after Redis rename succeeded" in caplog.text
        # The old live mirror must not be trusted any more.
        assert mirrored.exists("bloom:live", "new") is True

    def test_swap_without_local_temp_discards_stale_live_mirror(self, mirrored, fake, caplog):
        # Regression: the old live mirror used to stay trusted and hide new items.
        mirrored.create_filter("bloom:live")
        mirrored.add("bloom:live", "old")
        fake.filters["bloom:temp"] = {"new"}  # built elsewhere, no local mirror

        with caplog.at_level(logging.WARNING):
            assert mirrored.swap("bloom:temp", "bloom:live") is True
        assert "discarding the local mirror" in caplog.text
        assert mirrored.exists("bloom:live", "new") is True


class TestInfoAndLocks:
    def test_get_info_dict(self, service):
        service.redis.execute_command.return_value = {
            b"Capacity": 1000,
            b"Number of items inserted": 250,
        }
        info = service.get_info("test_filter")
        assert info == {"capacity": 1000, "inserted": 250, "ratio": 0.25}

    def test_get_info_list(self, service):
        service.redis.execute_command.return_value = [
            b"Capacity",
            b"2000",
            b"Number of items inserted",
            b"500",
        ]
        info = service.get_info("test_filter")
        assert info["capacity"] == 2000
        assert info["inserted"] == 500
        assert info["ratio"] == 0.25

    def test_get_info_not_found_ratio_one(self, service):
        service.redis.execute_command.side_effect = Exception("no such key")
        assert service.get_info("test_filter")["ratio"] == 1.0

    def test_get_info_redis_errors_ratio_zero(self, service, caplog):
        service.redis.execute_command.side_effect = Exception("Connection Refused")
        with caplog.at_level(logging.WARNING):
            info = service.get_info("test_filter")
        assert info["ratio"] == 0.0
        assert "BF.INFO failed" in caplog.text

    def test_load_ratio(self, service):
        service.redis.execute_command.return_value = {
            b"Capacity": 1000,
            b"Number of items inserted": 250,
        }
        assert service.load_ratio("test_filter") == 0.25

    def test_acquire_lock_uses_a_token(self, service):
        service.redis.set.return_value = True
        assert service.acquire_lock("rebuild_users", ttl=300) is True
        args, kwargs = service.redis.set.call_args
        assert args[0] == "lock:rebuild_users"
        assert len(args[1]) == 32
        assert kwargs == {"nx": True, "ex": 300}

    def test_acquire_lock_failure(self, service, caplog):
        service.redis.set.side_effect = Exception("boom")
        with caplog.at_level(logging.WARNING):
            assert service.acquire_lock("rebuild_users") is False
        assert "failed to acquire lock" in caplog.text

    def test_release_lock_compares_token(self, service):
        service.redis.set.return_value = True
        service.redis.eval.return_value = 1
        service.acquire_lock("rebuild_users")
        token = service.redis.set.call_args.args[1]
        assert service.release_lock("rebuild_users") is True
        _, numkeys, key, sent_token = service.redis.eval.call_args.args
        assert (numkeys, key, sent_token) == (1, "lock:rebuild_users", token)

    def test_release_lock_not_held(self, service, caplog):
        with caplog.at_level(logging.WARNING):
            assert service.release_lock("rebuild_users") is False
        service.redis.eval.assert_not_called()
        assert "not held by this service" in caplog.text

    def test_release_lock_taken_over(self, service, caplog):
        service.redis.set.return_value = True
        service.redis.eval.return_value = 0
        service.acquire_lock("rebuild_users")
        with caplog.at_level(logging.WARNING):
            assert service.release_lock("rebuild_users") is False
        assert "expired or was taken over" in caplog.text

    def test_release_lock_failure(self, service, caplog):
        service.redis.set.return_value = True
        service.redis.eval.side_effect = Exception("boom")
        service.acquire_lock("rebuild_users")
        with caplog.at_level(logging.WARNING):
            assert service.release_lock("rebuild_users") is False
        assert "failed to release lock" in caplog.text


def _raising_replace(src, dst):
    raise OSError("disk full")


class TestDeleteAndKeys:
    def test_delete_filter_removes_redis_keys_and_mirror(self, mirrored, fake):
        mirrored.create_filter("users")
        mirrored.add("users", "a")
        path = mirrored._mmap_path("users")
        assert os.path.exists(path)
        assert mirrored.delete_filter("users") is True
        assert "users" not in fake.filters
        assert not os.path.exists(path)
        assert not os.path.exists(path + ".synced")

    def test_delete_filter_redis_failure(self, service, caplog):
        service.redis.delete.side_effect = Exception("down")
        with caplog.at_level(logging.WARNING):
            assert service.delete_filter("users") is False
        assert "failed to delete Redis filter" in caplog.text

    @pytest.mark.parametrize("name", ["users", "{users}", "{tenant1}:users", "a{b", "user:{42}:seen"])
    def test_replication_keys_share_the_filters_cluster_slot(self, service, name):
        assert _slot(service._snapshot_key(name)) == _slot(name)
        assert _slot(service._log_key(name)) == _slot(name)
        assert service._snapshot_key(name) != service._log_key(name)

    def test_replicating_service_rejects_decoding_clients(self):
        client = MagicMock()
        client.connection_pool.connection_kwargs = {"decode_responses": True}
        with pytest.raises(ValueError, match="decode_responses"):
            BloomFilterService(client)
        BloomFilterService(client, replicate=False)  # fine without replication


def _crc16(data: bytes) -> int:
    crc = 0
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else crc << 1
            crc &= 0xFFFF
    return crc


def _slot(key: str) -> int:
    """Redis Cluster hash slot (with hash-tag rules) of a key."""
    start = key.find("{")
    if start != -1:
        end = key.find("}", start + 1)
        if end > start + 1:
            key = key[start + 1 : end]
    return _crc16(key.encode()) % 16384

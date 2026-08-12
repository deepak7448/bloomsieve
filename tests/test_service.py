import logging
import os
from unittest.mock import MagicMock

import pytest

from bloomsieve import BloomFilterService


@pytest.fixture(autouse=True)
def _clean_service_cache():
    """Class-level caches on BloomFilterService must not leak between tests."""
    yield
    for bf in BloomFilterService._mmaps.values():
        try:
            bf.close()
        except Exception:
            pass
    BloomFilterService._mmaps.clear()
    BloomFilterService._cold_warned.clear()


@pytest.fixture
def service(tmp_path):
    mock_redis = MagicMock()
    return BloomFilterService(
        redis_client=mock_redis,
        capacity=1000,
        error_rate=0.01,
        use_mmap=False,
        mmap_dir=str(tmp_path),
    )


def redis_exec_args(mock):
    return [call.args for call in mock.execute_command.call_args_list]


class TestCreateFilter:
    def test_creates_filter(self, service):
        service.redis.execute_command.return_value = "OK"
        assert service.create_filter("test_filter", 5000, 0.01) is True
        service.redis.execute_command.assert_called_with(
            "BF.RESERVE", "test_filter", "0.01", "5000", "EXPANSION", "2"
        )

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


class TestCorePromise:
    """The product's central behaviour: definite-negative local answers avoid Redis."""

    def test_local_negative_lookup_skips_redis(self, tmp_path):
        service = BloomFilterService(
            redis_client=MagicMock(),
            capacity=1000,
            error_rate=0.01,
            use_mmap=True,
            mmap_dir=str(tmp_path),
        )
        for i in range(200):
            service.add("users", f"user:{i}")

        service.redis.execute_command.reset_mock()
        assert service.exists("users", "user-absent") is False

        bf_requests = [args[0] for args in redis_exec_args(service.redis)]
        assert "BF.EXISTS" not in bf_requests

    def test_possible_positive_falls_back_to_redis(self, tmp_path):
        service = BloomFilterService(
            redis_client=MagicMock(),
            capacity=1000,
            error_rate=0.01,
            use_mmap=True,
            mmap_dir=str(tmp_path),
        )
        service.redis.execute_command.return_value = 1
        service.add("users", "user:1")
        assert service.exists("users", "user:1") is True
        assert ("BF.EXISTS", "users", "user:1") in redis_exec_args(service.redis)

    def test_cold_local_filter_falls_back_to_redis(self, tmp_path, caplog):
        service = BloomFilterService(
            redis_client=MagicMock(),
            capacity=1000,
            error_rate=0.01,
            use_mmap=True,
            mmap_dir=str(tmp_path),
        )
        service.redis.execute_command.return_value = 1
        with caplog.at_level(logging.WARNING):
            assert service.exists("brand_new_filter", "anything") is True
        assert "has not been populated" in caplog.text
        assert ("BF.EXISTS", "brand_new_filter", "anything") in redis_exec_args(service.redis)

    def test_local_filter_becomes_trusted_after_add(self, tmp_path):
        service = BloomFilterService(
            redis_client=MagicMock(),
            capacity=1000,
            error_rate=0.01,
            use_mmap=True,
            mmap_dir=str(tmp_path),
        )
        service.add("users", "user:1")
        service.redis.execute_command.reset_mock()
        assert service.exists("users", "definitely-absent") is False
        assert [args[0] for args in redis_exec_args(service.redis)] == []

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
        service._close_mmap("test_filter")

    def test_filenames_are_sanitized(self, service):
        path = service._mmap_path("some/path:with spaces")
        assert path == os.path.join(service.mmap_dir, "some_path_with_spaces.bloom")

    def test_local_add_sets_synced_flag(self, service):
        service.use_mmap = True
        service.redis.execute_command.return_value = 1
        service.create_filter("users", 500, 0.01)
        service._local_add("users", "user:1")
        assert service._init_mmap("users").synced is True
        service._close_mmap("users")

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


class TestRebuild:
    def test_rebuild_basic(self, service):
        service.redis.delete.return_value = 1
        service.redis.execute_command.return_value = "OK"
        mock_pipeline = MagicMock()
        service.redis.pipeline.return_value = mock_pipeline

        assert service.rebuild("test_filter", ["a", "b", "c"], 5000, 0.01) is True
        service.redis.delete.assert_called_with("test_filter")
        assert mock_pipeline.execute_command.call_count == 3
        mock_pipeline.execute.assert_called_once()

    def test_rebuild_chunks_large_item_sets(self, service):
        service.redis.delete.return_value = 1
        service.redis.execute_command.return_value = "OK"
        mock_pipeline = MagicMock()
        service.redis.pipeline.return_value = mock_pipeline

        assert service.rebuild("test_filter", range(2500), 5000, 0.01) is True
        assert mock_pipeline.execute.call_count == 3  # 1000 + 1000 + 500

    def test_rebuild_with_none_items(self, service):
        service.redis.delete.return_value = 1
        service.redis.execute_command.return_value = "OK"
        mock_pipeline = MagicMock()
        service.redis.pipeline.return_value = mock_pipeline
        assert service.rebuild("test_filter", None) is True
        mock_pipeline.execute.assert_not_called()

    def test_rebuild_returns_false_when_reserve_fails(self, service, caplog):
        service.redis.execute_command.side_effect = Exception("Connection Refused")
        with caplog.at_level(logging.ERROR):
            assert service.rebuild("test_filter", ["a"]) is False
        assert "could not reserve" in caplog.text

    def test_rebuild_returns_false_when_insert_fails(self, service, caplog):
        service.redis.execute_command.return_value = "OK"
        mock_pipeline = MagicMock()
        mock_pipeline.execute.side_effect = Exception("boom")
        service.redis.pipeline.return_value = mock_pipeline
        with caplog.at_level(logging.ERROR):
            assert service.rebuild("test_filter", ["a", "b"]) is False
        assert "rebuild of test_filter failed" in caplog.text


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

    def test_swap_rotates_local_mirror(self, service):
        service.use_mmap = True
        service.redis.rename.return_value = True
        service._init_mmap("bloom:temp")
        service._local_add("bloom:temp", "alice")
        assert os.path.exists(service._mmap_path("bloom:temp"))

        assert service.swap("bloom:temp", "bloom:live") is True
        assert "alice" in service._init_mmap("bloom:live")
        assert not os.path.exists(service._mmap_path("bloom:temp"))

    def test_swap_reports_failure_when_local_replace_fails(self, service, monkeypatch, caplog):
        service.use_mmap = True
        service.redis.rename.return_value = True
        service._init_mmap("bloom:temp")
        monkeypatch.setattr("bloomsieve.redis_service.os.replace", _raising_replace)
        with caplog.at_level(logging.ERROR):
            assert service.swap("bloom:temp", "bloom:live") is False
        assert "failed after Redis rename succeeded" in caplog.text

    def test_swap_without_local_temp_skips_local_step(self, service, caplog):
        service.use_mmap = True
        service.redis.rename.return_value = True
        with caplog.at_level(logging.INFO):
            assert service.swap("bloom:temp", "bloom:live") is True
        assert "skipping local mirror swap" in caplog.text


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

    def test_acquire_lock(self, service):
        service.redis.set.return_value = True
        assert service.acquire_lock("rebuild_users", ttl=300) is True
        service.redis.set.assert_called_with("lock:rebuild_users", "1", nx=True, ex=300)

    def test_acquire_lock_failure(self, service, caplog):
        service.redis.set.side_effect = Exception("boom")
        with caplog.at_level(logging.WARNING):
            assert service.acquire_lock("rebuild_users") is False
        assert "failed to acquire lock" in caplog.text

    def test_release_lock(self, service):
        service.redis.delete.return_value = 1
        assert service.release_lock("rebuild_users") is True
        service.redis.delete.assert_called_with("lock:rebuild_users")

    def test_release_lock_failure(self, service, caplog):
        service.redis.delete.side_effect = Exception("boom")
        with caplog.at_level(logging.WARNING):
            assert service.release_lock("rebuild_users") is False
        assert "failed to release lock" in caplog.text


def _raising_replace(src, dst):
    raise OSError("disk full")

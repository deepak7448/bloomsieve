import hashlib
import logging
import multiprocessing
import os
import struct
import threading

import pytest

from bloomsieve import BloomFilter, BloomFilterFileError
from bloomsieve.utils import MAX_HASHES, get_hash_indices, get_optimal_m_k

HEADER_SIZE = 16
FILE_ID_SIZE = 24  # trailer: b"BSFILEID" + 16 random bytes


def header_of(path):
    with open(path, "rb") as fh:
        return struct.unpack("<QQ", fh.read(HEADER_SIZE))


class TestMemoryFilter:
    def test_initial_state_is_absent(self):
        bf = BloomFilter(capacity=1000, error_rate=0.01)
        assert "hello" not in bf

    def test_insert_and_positive_membership(self):
        bf = BloomFilter(capacity=1000, error_rate=0.01)
        assert bf.add("hello") is True
        assert "hello" in bf

    def test_duplicate_insertion_returns_false(self):
        bf = BloomFilter(capacity=1000, error_rate=0.01)
        bf.add("hello")
        assert bf.add("hello") is False
        assert "hello" in bf

    def test_negative_membership(self):
        bf = BloomFilter(capacity=1000, error_rate=0.01)
        bf.add("hello")
        assert "world" not in bf

    def test_string_and_bytes_are_interchangeable(self):
        bf = BloomFilter(capacity=1000, error_rate=0.01)
        bf.add("hello")
        assert b"hello" in bf
        bf.add(b"world")
        assert "world" in bf

    def test_clear_resets_bits(self):
        bf = BloomFilter(capacity=1000, error_rate=0.01)
        bf.add("hello")
        assert "hello" in bf
        bf.clear()
        assert "hello" not in bf

    def test_configurable_capacity_changes_size(self):
        small = BloomFilter(capacity=100, error_rate=0.01)
        large = BloomFilter(capacity=100_000, error_rate=0.01)
        assert large.m > small.m

    def test_configurable_error_rate_changes_size(self):
        loose = BloomFilter(capacity=10_000, error_rate=0.1)
        strict = BloomFilter(capacity=10_000, error_rate=0.0001)
        assert strict.m > loose.m

    def test_invalid_capacity(self):
        with pytest.raises(ValueError):
            BloomFilter(capacity=0, error_rate=0.01)
        with pytest.raises(ValueError):
            BloomFilter(capacity=-5, error_rate=0.01)

    def test_invalid_error_rate(self):
        with pytest.raises(ValueError):
            BloomFilter(capacity=1000, error_rate=0)
        with pytest.raises(ValueError):
            BloomFilter(capacity=1000, error_rate=1)
        with pytest.raises(ValueError):
            BloomFilter(capacity=1000, error_rate=-0.1)
        with pytest.raises(ValueError):
            BloomFilter(capacity=1000, error_rate=1.5)

    def test_non_text_items_raise_typeerror(self):
        bf = BloomFilter(capacity=1000, error_rate=0.01)
        with pytest.raises(TypeError):
            bf.add(123)
        with pytest.raises(TypeError):
            assert 123 in bf
        with pytest.raises(TypeError):
            bf.add(None)

    def test_tiny_boundary_configuration(self):
        bf = BloomFilter(capacity=1, error_rate=0.99)
        assert bf.add("only_item") is True
        assert "only_item" in bf
        assert "other_item" not in bf

    def test_observed_false_positive_rate_is_bounded(self):
        capacity = 10_000
        error_rate = 0.01
        bf = BloomFilter(capacity=capacity, error_rate=error_rate)
        for i in range(capacity):
            bf.add(f"positive-{i}")
        false_positives = sum(1 for i in range(capacity, capacity * 2) if f"positive-{i}" in bf)
        # With k ~7 hashes the expected false-positive share is ~1%; allow a wide
        # margin so the check is deterministic and not flaky.
        assert false_positives < capacity * 0.05

    def test_after_close_raises(self):
        bf = BloomFilter(capacity=1000, error_rate=0.01)
        bf.add("hello")
        bf.close()
        with pytest.raises(RuntimeError):
            bf.add("world")
        with pytest.raises(RuntimeError):
            assert "hello" in bf
        with pytest.raises(RuntimeError):
            bf.clear()

    def test_close_is_idempotent(self):
        bf = BloomFilter(capacity=1000, error_rate=0.01)
        bf.close()
        bf.close()


class TestMmapFilter:
    def test_file_is_created_with_expected_size(self, tmp_path):
        path = str(tmp_path / "filter.bloom")
        bf = BloomFilter(capacity=1000, error_rate=0.01)
        expected = HEADER_SIZE + bf.m // 8 + FILE_ID_SIZE
        bf2 = BloomFilter(capacity=1000, error_rate=0.01, filepath=path)
        assert path.endswith("filter.bloom")
        assert os.path.getsize(path) == expected
        assert header_of(path) == (bf2.m, bf2.k)
        bf2.close()

    def test_missing_file_is_created(self, tmp_path):
        path = str(tmp_path / "sub" / "new.bloom")
        bf = BloomFilter(capacity=50, error_rate=0.05, filepath=path)
        assert os.path.exists(path)
        assert bf.newly_created is True
        bf.close()

    def test_persistence_across_instances(self, tmp_path):
        path = str(tmp_path / "persistent.bloom")
        bf = BloomFilter(capacity=500, error_rate=0.05, filepath=path)
        bf.add("alice")
        bf.add("bob")
        assert "alice" in bf
        bf.close()

        bf2 = BloomFilter(capacity=500, error_rate=0.05, filepath=path)
        assert "alice" in bf2
        assert "bob" in bf2
        assert "charlie" not in bf2
        bf2.close()

    def test_reopen_uses_stored_configuration(self, tmp_path):
        path = str(tmp_path / "stored.bloom")
        first = BloomFilter(capacity=500, error_rate=0.05, filepath=path)
        stored = (first.m, first.k)
        first.close()

        reopened = BloomFilter(capacity=999_999, error_rate=0.999, filepath=path)
        assert (reopened.m, reopened.k) == stored
        assert reopened.newly_created is False
        reopened.close()

    def test_duplicate_insert_across_instances(self, tmp_path):
        path = str(tmp_path / "dup.bloom")
        bf = BloomFilter(capacity=500, error_rate=0.05, filepath=path)
        assert bf.add("alice") is True
        bf.close()

        bf2 = BloomFilter(capacity=500, error_rate=0.05, filepath=path)
        assert bf2.add("alice") is False  # bit already set
        bf2.close()

    def test_clear_then_reopen(self, tmp_path):
        path = str(tmp_path / "clear.bloom")
        bf = BloomFilter(capacity=500, error_rate=0.05, filepath=path)
        bf.add("alice")
        bf.clear()
        assert "alice" not in bf
        bf.close()

        bf2 = BloomFilter(capacity=500, error_rate=0.05, filepath=path)
        assert "alice" not in bf2
        bf2.close()

    def test_truncated_below_header_is_recovered(self, tmp_path, caplog):
        path = str(tmp_path / "truncated.bloom")
        bf = BloomFilter(capacity=500, error_rate=0.05, filepath=path)
        bf.add("alice")
        bf.close()

        with open(path, "r+b") as fh:
            fh.truncate(8)
        assert os.path.getsize(path) == 8

        with caplog.at_level(logging.WARNING):
            bf2 = BloomFilter(capacity=500, error_rate=0.05, filepath=path)
        assert "truncated" in caplog.text
        assert os.path.getsize(path) == HEADER_SIZE + bf2.m // 8 + FILE_ID_SIZE
        assert "alice" not in bf2  # fresh filter after recovery
        bf2.close()

    def test_truncated_below_bit_area_raises(self, tmp_path):
        path = str(tmp_path / "half.bloom")
        bf = BloomFilter(capacity=50_000, error_rate=0.001, filepath=path)
        bf.add("alice")
        bf.close()

        original = os.path.getsize(path)
        assert original > HEADER_SIZE + 8
        with open(path, "r+b") as fh:
            fh.truncate(HEADER_SIZE + 32)

        with pytest.raises(BloomFilterFileError):
            BloomFilter(capacity=50_000, error_rate=0.001, filepath=path)

    def test_corrupt_header_raises(self, tmp_path):
        path = str(tmp_path / "corrupt.bloom")
        bf = BloomFilter(capacity=500, error_rate=0.05, filepath=path)
        bf.close()

        with open(path, "r+b") as fh:
            fh.seek(0)
            fh.write(struct.pack("<QQ", 1, 2))  # m not >= 8 and not byte-aligned

        with pytest.raises(BloomFilterFileError):
            BloomFilter(capacity=500, error_rate=0.05, filepath=path)

    def test_mmap_file_stays_exactly_sized_after_writes(self, tmp_path):
        path = str(tmp_path / "sized.bloom")
        bf = BloomFilter(capacity=1000, error_rate=0.01, filepath=path)
        for i in range(500):
            bf.add(f"item-{i}")
        bf.close()
        bf = BloomFilter(capacity=1000, error_rate=0.01, filepath=path)
        assert os.path.getsize(path) == HEADER_SIZE + bf.m // 8 + FILE_ID_SIZE
        assert "item-499" in bf
        bf.close()

    def test_context_manager_closes_handles(self, tmp_path):
        path = str(tmp_path / "ctx.bloom")
        with BloomFilter(capacity=500, error_rate=0.05, filepath=path) as bf:
            bf.add("alice")
        # The file must be reusable after the context exited, proving the handles
        # were released and the state flushed to disk.
        bf2 = BloomFilter(capacity=500, error_rate=0.05, filepath=path)
        assert "alice" in bf2
        bf2.close()


def test_optimal_params_helper():
    m, k = get_optimal_m_k(1000, 0.01)
    assert m % 8 == 0
    assert m >= 8
    assert k >= 1


def test_optimal_params_reject_nan_error_rate():
    with pytest.raises(ValueError):
        get_optimal_m_k(1000, float("nan"))


def test_error_rate_too_small_is_rejected_up_front():
    # Regression: k > 128 used to produce a file the library itself refused to reopen.
    with pytest.raises(ValueError, match="too small"):
        BloomFilter(capacity=10, error_rate=1e-40)
    _, k = get_optimal_m_k(10, 1e-38)
    assert k <= MAX_HASHES


class TestFormatCompatibility:
    def test_bit_layout_is_unchanged(self):
        # Golden value produced by bloomsieve 0.2.x.  Files written by older
        # versions are only readable if the bit layout never changes.
        bf = BloomFilter(capacity=100, error_rate=0.01)
        for item in ("alice", "bob", b"\x00\xff"):
            bf.add(item)
        assert (bf.m, bf.k) == (960, 7)
        assert hashlib.sha256(bytes(bf._bitarray)).hexdigest() == (
            "86545fb7b532104660627fb49c75e62cbaaf535bf64f794dcde02363f8080f88"
        )

    def test_bits_match_get_hash_indices(self):
        bf = BloomFilter(capacity=500, error_rate=0.001)
        bf.add("item")
        for idx in get_hash_indices("item", bf.m, bf.k):
            assert bf._bitarray[idx // 8] & (1 << (idx % 8))


class TestBatchAndIntrospection:
    def test_add_many_returns_new_item_count(self, tmp_path):
        for path in (None, str(tmp_path / "batch.bloom")):
            with BloomFilter(capacity=1000, error_rate=0.01, filepath=path) as bf:
                assert bf.add_many(["a", "b", "c"]) == 3
                assert bf.add_many(["a", "d"]) == 1
                assert all(x in bf for x in "abcd")

    def test_add_many_type_error(self):
        bf = BloomFilter(capacity=1000, error_rate=0.01)
        with pytest.raises(TypeError):
            bf.add_many(["ok", 5])
        assert "ok" in bf

    def test_closed_and_repr(self, tmp_path):
        bf = BloomFilter(capacity=1000, error_rate=0.01, filepath=str(tmp_path / "r.bloom"))
        assert not bf.closed
        assert "r.bloom" in repr(bf)
        bf.close()
        assert bf.closed
        assert "closed" in repr(bf)
        with pytest.raises(RuntimeError):
            bf.add_many(["x"])
        with pytest.raises(RuntimeError):
            bf.flush()


class TestConcurrency:
    def test_threads_sharing_one_instance_lose_no_items(self):
        bf = BloomFilter(capacity=200, error_rate=0.3)
        workers = 8

        def run(j):
            for i in range(2000):
                bf.add(f"{j}-{i}")

        threads = [threading.Thread(target=run, args=(j,)) for j in range(workers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert all(f"{j}-{i}" in bf for j in range(workers) for i in range(2000))

    def test_processes_sharing_one_file_lose_no_items(self, tmp_path):
        # Regression: unlocked read-modify-write of shared bytes lost bits.
        if not hasattr(os, "fork"):
            pytest.skip("needs fork")
        ctx = multiprocessing.get_context("fork")
        paths = [str(tmp_path / f"t{t}.bloom") for t in range(40)]
        for p in paths:
            BloomFilter(capacity=300, error_rate=0.3, filepath=p).close()
        workers = 6
        barrier = ctx.Barrier(workers)
        procs = [ctx.Process(target=_add_worker, args=(paths, barrier, j)) for j in range(workers)]
        for p in procs:
            p.start()
        for p in procs:
            p.join()
        assert all(p.exitcode == 0 for p in procs)
        missing = 0
        for t, path in enumerate(paths):
            with BloomFilter(capacity=300, error_rate=0.3, filepath=path) as bf:
                missing += sum(f"{t}-{j}-{i}" not in bf for j in range(workers) for i in range(40))
        assert missing == 0

    def test_concurrent_creation_never_clobbers_a_file(self, tmp_path):
        path = str(tmp_path / "race.bloom")
        filters, errors = [], []

        def open_and_add(j):
            try:
                bf = BloomFilter(capacity=1000, error_rate=0.01, filepath=path)
                bf.add(f"item-{j}")
                filters.append(bf)
            except Exception as exc:  # pragma: no cover - reported below
                errors.append(exc)

        threads = [threading.Thread(target=open_and_add, args=(j,)) for j in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        assert sum(bf.newly_created for bf in filters) == 1
        for bf in filters:
            bf.close()
        with BloomFilter(capacity=1000, error_rate=0.01, filepath=path) as bf:
            assert all(f"item-{j}" in bf for j in range(16))
        assert [f for f in os.listdir(tmp_path) if f.endswith(".tmp")] == []


def _add_worker(paths, barrier, j):
    for t, path in enumerate(paths):
        bf = BloomFilter(capacity=300, error_rate=0.3, filepath=path)
        barrier.wait()
        for i in range(40):
            bf.add(f"{t}-{j}-{i}")
        bf.close()


class TestFileId:
    def test_every_new_file_gets_a_unique_id(self, tmp_path):
        ids = set()
        for i in range(5):
            with BloomFilter(capacity=100, error_rate=0.01, filepath=str(tmp_path / f"f{i}.bloom")) as bf:
                assert bf.file_id is not None and len(bf.file_id) == 32
                ids.add(bf.file_id)
        assert len(ids) == 5
        assert BloomFilter(capacity=100, error_rate=0.01).file_id is None

    def test_id_survives_reopen_and_changes_when_recreated(self, tmp_path):
        path = str(tmp_path / "f.bloom")
        with BloomFilter(capacity=100, error_rate=0.01, filepath=path) as bf:
            first = bf.file_id
        with BloomFilter(capacity=100, error_rate=0.01, filepath=path) as bf:
            assert bf.file_id == first
        os.remove(path)
        with BloomFilter(capacity=100, error_rate=0.01, filepath=path) as bf:
            assert bf.file_id != first

    def test_pre_0_3_file_without_trailer(self, tmp_path):
        # A file exactly as 0.2 wrote it: header + bit array, nothing after it.
        path = str(tmp_path / "old.bloom")
        with BloomFilter(capacity=100, error_rate=0.01, filepath=path) as bf:
            bf.add("alice")
            size = bf.total_size
        with open(path, "r+b") as fh:
            fh.truncate(size)

        with BloomFilter(capacity=100, error_rate=0.01, filepath=path) as bf:
            assert bf.file_id is None
            assert "alice" in bf
            new_id = bf._ensure_file_id()
            assert bf._ensure_file_id() == new_id  # idempotent
        assert os.path.getsize(path) == size + FILE_ID_SIZE
        with BloomFilter(capacity=100, error_rate=0.01, filepath=path) as bf:
            assert bf.file_id == new_id
            assert "alice" in bf

    def test_ensure_file_id_needs_a_file(self):
        with pytest.raises(RuntimeError):
            BloomFilter(capacity=100, error_rate=0.01)._ensure_file_id()

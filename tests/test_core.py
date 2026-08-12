import logging
import os
import struct

import pytest

from bloomsieve import BloomFilter, BloomFilterFileError
from bloomsieve.utils import get_optimal_m_k

HEADER_SIZE = 16


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
        expected = HEADER_SIZE + bf.m // 8
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
        assert os.path.getsize(path) == HEADER_SIZE + bf2.m // 8
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
        assert os.path.getsize(path) == HEADER_SIZE + bf.m // 8
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

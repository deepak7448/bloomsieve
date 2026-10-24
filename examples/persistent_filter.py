"""Persistent, memory-mapped Bloom filter that survives restarts.

Covers: creating a file-backed filter, the context manager, ``flush`` for
durability, reopening (the stored configuration wins), ``newly_created``, and
handling a corrupt file with ``BloomFilterFileError``.

Run:
    python examples/persistent_filter.py
"""

import os
import tempfile

from bloomsieve import BloomFilter, BloomFilterFileError


def main() -> None:
    workdir = tempfile.mkdtemp(prefix="bloomsieve_example_")
    path = os.path.join(workdir, "sessions.bloom")

    # First run: the file does not exist yet, so it is created.
    with BloomFilter(capacity=100_000, error_rate=0.001, filepath=path) as bf:
        print(f"created: {bf.newly_created}, file size: {os.path.getsize(path):,} bytes")
        bf.add_many(f"session:{i}" for i in range(1_000))
        bf.flush()  # durable on disk now; close() (end of the with-block) also flushes

    # Later run (or another process): the same file is reopened.  Its stored
    # m/k win, so passing different sizing arguments cannot corrupt it.
    with BloomFilter(capacity=1, error_rate=0.5, filepath=path) as bf:
        print(f"reopened: newly_created={bf.newly_created}, m={bf.m:,}, k={bf.k}")
        print("'session:42' ->", "session:42" in bf)  # True: survived the restart
        print("'session:x' ->", "session:x" in bf)  # False

    # A damaged file is reported, never silently trusted.
    broken = os.path.join(workdir, "broken.bloom")
    with open(broken, "wb") as fh:
        fh.write(b"\x01" * 64)  # header claims an impossible configuration
    try:
        BloomFilter(capacity=1000, error_rate=0.01, filepath=broken)
    except BloomFilterFileError as exc:
        print("corrupt file detected:", exc)
        os.remove(broken)  # recover by deleting it and rebuilding from your data

    print("files are in", workdir)


if __name__ == "__main__":
    main()

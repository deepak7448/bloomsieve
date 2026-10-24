"""Several processes sharing one Bloom filter file.

Every process maps the same file, so an item added by one worker is immediately
visible to all others.  Writes take an exclusive file lock (POSIX), so concurrent
adds never lose bits.  On Windows use a single writer process per file.

Run:
    python examples/multiprocess_shared_file.py
"""

import multiprocessing
import os
import tempfile

from bloomsieve import BloomFilter

WORKERS = 4
ITEMS_PER_WORKER = 25_000


def worker(path: str, worker_id: int) -> None:
    # Each process opens the filter itself; never pass a BloomFilter object
    # between processes.
    with BloomFilter(capacity=200_000, error_rate=0.001, filepath=path) as bf:
        bf.add_many(f"w{worker_id}:item{i}" for i in range(ITEMS_PER_WORKER))


def main() -> None:
    path = os.path.join(tempfile.mkdtemp(prefix="bloomsieve_mp_"), "shared.bloom")

    procs = [multiprocessing.Process(target=worker, args=(path, w)) for w in range(WORKERS)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

    with BloomFilter(capacity=200_000, error_rate=0.001, filepath=path) as bf:
        missing = sum(f"w{w}:item{i}" not in bf for w in range(WORKERS) for i in range(ITEMS_PER_WORKER))
    print(f"{WORKERS} processes added {WORKERS * ITEMS_PER_WORKER:,} items; missing afterwards: {missing}")


if __name__ == "__main__":
    main()

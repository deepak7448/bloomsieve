# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-08-08

### Added
- Modular package layout under `src/bloomsieve/`.
- High-performance standalone local `BloomFilter` supporting both in-memory and disk-backed `mmap` mode.
- Optimized hashing using the Kirsch-Mitzenmacher technique to reduce hashing operations from $O(k)$ to $O(1)$.
- Comprehensive test coverage for local Bloom filters and service integration.
- Distributed locking and thread synchronization in `BloomFilterService` for concurrent use cases.

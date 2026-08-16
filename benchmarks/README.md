# Benchmark scenarios

This directory contains end-to-end measurements that combine multiple modules.
Benchmarks for one module remain inside that module's `benchmarks/` directory,
while shared measurement mechanics belong in `support/benchmarking/`.

Read `docs/benchmarking-methodology.md` before publishing a number. Shared
GitHub-hosted runners are suitable for compilation smoke tests, not for
enforcing latency thresholds.

Register cross-module scenarios explicitly in `benchmarks/scenarios/`.

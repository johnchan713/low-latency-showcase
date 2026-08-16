# Benchmarks

This directory will hold shared measurement infrastructure only when the first
real benchmark needs it. Individual technique benchmarks stay beside their
snippet or component so that code, context, and results remain discoverable.

Read `docs/benchmarking-methodology.md` before adding a harness or publishing a
number. Shared GitHub-hosted runners are suitable for compilation smoke tests,
not for enforcing latency thresholds.

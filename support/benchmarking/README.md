# Benchmarking support

Shared measurement mechanics belong here: runner control, warm-up, timing
calibration, statistics, environment capture, and CPU-placement helpers.

Infrastructure placed here is used by module and scenario benchmarks, but it is
not a production dependency. Workload-specific benchmark code stays beside the
module being measured or in `benchmarks/scenarios/` for cross-module tests.

# Technique name

## Summary

Explain the technique and the specific latency problem in a few sentences.

## Use when

- List the workload, topology, and operational conditions that make it useful.

## Avoid when

- List conditions where it wastes resources, harms throughput, becomes unsafe,
  or loses to the simpler baseline.

## Requirements

Document the minimum C++ version, compiler, OS, architecture, privileges,
hardware properties, and dependencies.

## Correctness contract

Describe ownership, lifetime, thread-safety, memory ordering, progress
guarantees, overflow/full/empty behaviour, and failure handling.

## How it works

Explain the mechanism. Distinguish what the source expresses from what the
compiler and hardware are expected to do.

## API and example

Show the smallest useful example, including required setup and cleanup.

## Trade-offs

Cover CPU usage, memory, throughput, fairness, portability, complexity, and
operational consequences.

## Benchmark

Follow `docs/benchmarking-methodology.md`. Include the baseline, environment,
commands, raw results, distribution, and interpretation.

## Limitations

State what has not been tested and where conclusions must not be generalized.

## References

Prefer standards, vendor manuals, source code, and primary research.

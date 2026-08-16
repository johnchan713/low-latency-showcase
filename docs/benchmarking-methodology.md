# Benchmarking methodology

Low-latency measurements are unusually easy to distort. This document defines
the minimum evidence needed for a result to be useful rather than decorative.

## 1. Define the question

State one falsifiable question. For example: "Does padding two independently
written counters reduce p99 update latency with two pinned producer threads?"

Avoid broad questions such as "Which queue is fastest?" A queue result depends
on producers, consumers, capacity, payload size, contention, batching, waiting
strategy, CPU topology, and full/empty behaviour.

## 2. Establish a baseline

Compare the technique against the simplest correct implementation that solves
the same problem. Keep semantics equivalent. Faster code that drops work or
weakens ordering is not the same experiment.

## 3. Record the environment

At minimum, record:

- CPU model, core topology, cache hierarchy, and NUMA layout;
- operating system and kernel;
- compiler version and complete build configuration;
- power governor, turbo policy, and relevant BIOS settings;
- CPU affinity and whether sibling hyperthreads are in use;
- memory placement and huge-page configuration when relevant; and
- meaningful background workload.

Run `tools/collect-system-info.sh` to capture a starting snapshot. Review its
output before publishing because machines may expose environment-specific data.

## 4. Control execution

- Use a release or native benchmark build, never a sanitizer build.
- Pin latency-sensitive threads to declared CPUs.
- Warm the code and data before collecting steady-state measurements.
- Separate setup, allocation, logging, and validation from the timed region.
- Prevent the compiler from deleting or precomputing the measured operation.
- Repeat enough independent runs to expose run-to-run variation.
- Explain whether caches are intended to be hot, cold, or mixed.

CPU isolation, real-time scheduling, frequency locking, NUMA binding, and busy
polling can improve repeatability, but they also change the system being
measured. Record them; do not quietly treat them as free speed.

## 5. Use an appropriate clock

Document the clock source and its overhead. Measure that overhead separately.
If using a hardware counter such as the x86 timestamp counter, document
serialization, counter invariance, core migration controls, and conversion from
cycles to time.

## 6. Report the distribution

For latency, report at least:

- sample count;
- minimum;
- median (p50);
- p90, p95, p99, and p99.9;
- maximum; and
- throughput when it helps interpret contention or batching.

An average can hide the pauses that a low-latency system exists to avoid.
Include confidence intervals or repeated-run dispersion where possible.

## 7. Check correctness separately

Run unit, stress, sanitizer, and concurrency checks outside the timed build.
Benchmark success does not establish correctness, especially for relaxed atomic
operations and lock-free structures.

## Result record

Every published result should include:

```text
Question:
Baseline:
Technique:
Workload:
Hardware and topology:
OS and kernel:
Compiler and flags:
Affinity and system tuning:
Clock and measured overhead:
Warm-up and sample count:
Latency distribution:
Throughput:
Correctness checks:
Interpretation:
Limitations:
Reproduction command:
```

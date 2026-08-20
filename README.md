# Low Latency Showcase

Measured and documented C++ building blocks for latency-sensitive systems.

The project does not claim that a technique is universally fastest. Hardware,
contention, workload shape, operating-system behaviour, compiler output, and
acceptable trade-offs determine whether an optimization helps. Each accepted
technique must therefore include a baseline, reproducible measurements, and an
explicit **use when / avoid when** explanation.

## Performance at a glance

The first capsule moves small batched events at **hundreds of millions per
second** while keeping an unbatched producer-to-consumer handoff in the **tens
to low hundreds of nanoseconds**.

Representative results from the shared AMD EPYC development VM, using GCC 13,
`-O3`, LTO, native CPU tuning, pinned producer/consumer threads, exact sequence
validation, and full-payload checks. These are five-run medians; the shared VM
has substantial scheduling dispersion.

| Disruptor workload | Throughput | Handoff p50 | Handoff p99 |
|---|---:|---:|---:|
| 8-byte event, batch 1 | **83M events/s** | **80 ns** | **140 ns** |
| 8-byte event, batch 16 | **478M events/s** | **485 ns** | **511 ns** |
| 64-byte event, batch 1 | **78M events/s** | **220 ns** | **436 ns** |
| 64-byte event, batch 16 | **334M events/s** | **2,529 ns** | **2,869 ns** |
| 8-byte batch-16 multicast to 3 consumers | **121M source events/s** | **846 ns** | **1,061 ns** |

Batching raises throughput by sharing publication work; it does not mean that
one event crosses the ring in two nanoseconds. Throughput and handoff latency
measure different boundaries.

The 64-byte cases now write and verify all 64 bytes. Earlier figures that only
touched the first word were faster but represented cache-line stride rather
than complete payload processing.

### Latest optimization delta

The latest pass improves fixed-index consumption without weakening the
release/acquire synchronization contract:

| Targeted path | Previous | Optimized | Change |
|---|---:|---:|---:|
| 8-byte publish/drain 64/64 | 812M events/s | **904M events/s** | **+11%** |
| Batch-1 handoff p50 | 65 ns | **65 ns** | unchanged |
| Batch-1 handoff p99 | 126 ns | **125 ns** | effectively unchanged |

The throughput gain uses the thread-owned `make_consumer<Index>()` handle,
which reuses an already acquired publication range. When no range can be
reused, the advantage can shrink or disappear.

### Wait-strategy comparison

Seven paired runs compared equivalent polling loops with 5,000,000 events for
throughput and 200,000 post-warm-up handoff samples:

| Polling mode | Throughput | p50 | p99 |
|---|---:|---:|---:|
| Empty spin | 88M events/s | 85 ns | 145 ns |
| x86 `PAUSE` | 84M events/s | 95 ns | 145 ns |
| Adaptive: 64 pauses, then yield | **96M events/s** | **81 ns** | **136 ns** |

Adaptive waiting won this particular shared-VM handoff scenario: approximately
9% more throughput, 5% lower p50, and 6% lower p99 than empty spinning. It is a
selectable policy, not a universal default; an isolated physical core or a
different workload can reverse the result.

Build and reproduce the measurements with `benchmark-native`; see the
[`single-producer-disruptor` benchmark notes](modules/concurrency/disruptor-single-producer/README.md#benchmarks)
and [benchmarking methodology](docs/benchmarking-methodology.md) for workload
details, limitations, and interpretation.

## Current status

The repository is at its foundation stage. It provides:

- a C++23 entry executable for the currently supported Linux toolchains;
- target-scoped CMake configuration;
- Ninja-based development, release, sanitizer, and native benchmark presets;
- GCC and Clang continuous-integration builds;
- a capsule-first hierarchy for reusable modules and integrations;
- an experimental single-producer multicast Disruptor capsule with correctness,
  sanitizer, throughput, and handoff-latency coverage;
- explicit busy-spin, yield, and adaptive spin-wait policies for x86 polling;
- dedicated locations for future test support, benchmark support, and
  benchmark scenarios; and
- scripts for recording system information and pinning a process to CPUs.

The first reusable concurrency capsule is the
[`single-producer-disruptor`](modules/concurrency/disruptor-single-producer/).
It is intentionally narrow: one producer, a fixed set of multicast consumers,
bounded storage, and explicit backpressure.

## Requirements

- CMake 3.28 or newer
- Ninja
- Linux with GCC or Clang and C++23 support

The tested platform is Linux x86-64. Other architectures are currently
untested. Windows and MSVC are intentionally unsupported at this stage.

## Build and test

Configure, compile, and test the strict development build:

```sh
cmake --preset dev
cmake --build --preset dev
ctest --preset dev
```

Run the entry executable:

```sh
./build/dev/lls_showcase
```

Build profiles:

| Preset | Intended use | Timing results valid? |
|---|---|---:|
| `dev` | Debugging and strict warnings | No |
| `release` | Portable optimized build | Only with recorded context |
| `asan-ubsan` | Memory and undefined-behaviour checks | No |
| `tsan` | Concurrency checks | No |
| `benchmark-native` | Host-tuned measurements with LTO | Yes, for that host |

`benchmark-native` may emit instructions unavailable on another CPU. Never
distribute its binaries as portable release artifacts.

## Repository design

| Path | Responsibility |
|---|---|
| `apps/lls-info/` | Small project entry, compiler diagnostic, and its tests |
| `include/lls/` | Narrow repository-wide foundation headers only |
| `modules/` | Self-contained, independently reusable capability capsules |
| `examples/` | Runnable integrations combining multiple modules |
| `support/testing/` | Shared test-only infrastructure |
| `support/benchmarking/` | Shared benchmark mechanics, never production code |
| `benchmarks/scenarios/` | Cross-module and end-to-end measurements |
| `docs/` | Methodology, glossary, templates, and decision guidance |
| `cmake/` | Target-scoped warnings, sanitizers, and build options |
| `tools/` | Linux environment and CPU-affinity helpers |

A module folder represents a capability that another programmer may reasonably
integrate, not every private helper class. Each module owns its README, CMake
target, public headers, optional implementation, examples, correctness tests,
and benchmarks. Unneeded directories are omitted.

The root `include/lls/` directory is deliberately narrow. It currently exposes
compiler and language-mode metadata for applications and future benchmark
result reporting. Other reusable capabilities and their public headers belong
inside their module capsules.

Every module is registered explicitly in `modules/CMakeLists.txt`. Source
globbing is not accepted because new code must be visible in review and CI.

## Project rules

1. Correctness comes before speed. A wrong answer in two nanoseconds is still a
   wrong answer.
2. Compare against a clear ordinary baseline.
3. Report distributions, including tail latency, rather than only averages.
4. Record the hardware, OS, compiler, flags, topology, and workload.
5. Keep architecture- and OS-specific code isolated and labelled.
6. Never use sanitizer or shared cloud-runner timings as performance evidence.
7. Prefer understandable code until measurement proves that complexity pays.

Read the [benchmarking methodology](docs/benchmarking-methodology.md),
[Disruptor optimization tournament](docs/disruptor-optimization-tournament.md),
[glossary](docs/glossary.md), and [contribution guide](CONTRIBUTING.md) before
adding a technique.

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE).

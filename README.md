# Low Latency Showcase

Measured and documented C++ building blocks for latency-sensitive systems.

## C++ versus Java LMAX Disruptor 4

The current producer-session/two-span C++ implementation was measured directly
against the official
[Java LMAX Disruptor 4.0.0](https://github.com/LMAX-Exchange/disruptor) on the
same Intel Xeon Platinum 8370C host. Both languages use individually pinned
producer and consumer threads. Throughput validates count, sequence, value,
checksum, and affinity; latency validates count, sequence checksum, ordering,
positive samples, and affinity. `P` is the exact producer claim size; `D` is
the consumer acknowledgement cap.

### Completion throughput

The primary workload matches C++ `try_publish_batch(P)` against Java
`tryNext(P)`: both make an all-or-nothing nonblocking claim and retry at the
caller. Each process retains one 65,536-slot ring and the same worker threads
across two 100-million-event warm-ups and a two-billion-event measured phase.
Seven pairs alternate which language runs first, and every accepted phase lasts
at least one second. Rates cover publication through the producer's observation
of the final consumer acknowledgement.

| Workload | C++ median | Java median | Paired geometric C++ / Java (95% CI) | Order effect |
|---|---:|---:|---:|---:|
| `P=1, D=1` | **213M events/s** | 44.5M events/s | **4.96× (4.37–5.63)** | 1.047 |
| `P=1, D=65,536` | **165M events/s** | 67.0M events/s | **2.09× (1.25–3.50)** | 1.528 |
| `P=16, D=16` | **873M events/s** | 117M events/s | **7.43× (7.01–7.88)** | 1.016 |
| `P=16, D=65,536` | **1.47B events/s** | 224M events/s | **6.49× (6.04–6.97)** | 0.983 |
| `P=64, D=64` | **1.60B events/s** | 181M events/s | **8.95× (7.82–10.24)** | 1.059 |
| `P=64, D=65,536` | **1.60B events/s** | 261M events/s | **6.07× (5.61–6.57)** | 1.034 |

Java's idiomatic blocking `next(P)` was also run as a separately labelled
sensitivity across the same six modes. Its paired ratios ranged from 1.89× to
6.84× and every individual lower confidence bound exceeded parity. It is not
the workload-matched headline because Java blocks inside `next(P)` while the
C++ API returns failure for caller-side retry.

All 168 throughput rows passed validation. Every one of the twelve individual
per-mode confidence intervals excluded parity, but these are not a
multiple-comparison-adjusted family-wide guarantee. Two primary and four
blocking-sensitivity order effects fell outside the predeclared 0.95–1.05 band,
so the results remain qualified same-host observations rather than a universal
runtime ranking. See the tracked
[paired benchmark, raw-output schema, and exact reproduction commands](benchmarks/comparisons/disruptor/README.md).

### Batch-1 handoff latency

A separate serialized `P=1, D=1` workload measures each event from the
post-claim producer timestamp to the consumer handler-entry timestamp. The
producer waits for that event's released consumer sequence before claiming the
next event, preventing backlog behind earlier events from inflating the
distribution. Sixteen exactly balanced pairs each use 1,000,000 warm-up and
2,000,000 measured events.

| Percentile | C++ median | Java median | Paired geometric C++ / Java (95% CI; lower is better) | Order effect |
|---:|---:|---:|---:|---:|
| p50 | **127 ns** | 164 ns | **0.721 (0.589–0.882)** | 0.890 |
| p99 | **232 ns** | 394 ns | **0.555 (0.387–0.797)** | 1.130 |
| p99.9 | **280 ns** | 848 ns | **0.365 (0.193–0.692)** | 1.262 |

The five measured percentile intervals from p50 through p99.9 remain below
parity, so this pass supports a statistically resolved C++ latency advantage
for this workload on this host. Maximum latency remains unresolved, and every
percentile order effect falls outside the 0.95–1.05 diagnostic band; this is
not a portable guarantee. The comparator report preserves p90, p95, maximum,
the exact summary, and full qualifications.

This audit measures the current module's producer-session and two-span hot
paths. The benchmarked source tree was clean, both native binaries
were identical to the independently audited candidate binaries, and all
correctness, sanitizer, rollover, multicast, and concurrency checks remained
green.

The project does not claim that a technique is universally fastest. Hardware,
contention, workload shape, operating-system behaviour, compiler output, and
acceptable trade-offs determine whether an optimization helps. Each accepted
technique must therefore include a baseline, reproducible measurements, and an
explicit **use when / avoid when** explanation.

## Historical module microbenchmarks

The first capsule moves small batched events at **hundreds of millions per
second** while keeping an unbatched producer-to-consumer handoff in the **tens
to low hundreds of nanoseconds**.

These earlier results come from the shared AMD EPYC development VM and predate
the current producer-session optimization. They use GCC 13, `-O3`, LTO, native
CPU tuning, pinned producer/consumer threads, exact sequence validation, and
full-payload checks. They remain useful for workload-shape comparisons, not as
the current C++/Java headline. Values are five-run medians; the shared VM has
substantial scheduling dispersion.

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

### Earlier consumer-handle optimization delta

An earlier pass improved fixed-index consumption without weakening the
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

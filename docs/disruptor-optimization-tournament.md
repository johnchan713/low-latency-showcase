# Disruptor optimization tournament

This record bounds and evaluates optimization techniques for the experimental
single-producer Disruptor. It is exhaustive for techniques that preserve the
current contract: a bounded preallocated ring, one producer, a fixed number of
multicast consumers, ordered lossless delivery, Linux x86-64, and C++23.

Results came from a shared AMD EPYC 9V74 virtual machine with nine reported
single-threaded vCPUs, one NUMA node, 64-byte cache lines, GCC 13.3, native CPU
tuning, `-O3`, LTO, and pinned threads. Scheduling noise is visible, so decisions
use repeated-run medians and reject narrow or order-dependent wins.

## Decision rule

A production change must preserve the complete contract, pass checksums and
sanitizers, improve relevant throughput without materially worsening p50 or p99,
repeat across run order, and justify its API and state complexity. A configuration
knob may remain caller-selected when no universal optimum exists.

## Algorithm and layout tournament

| Technique | Evidence | Decision |
|---|---|---|
| One consumer acknowledgement per handled batch | Already implemented | Keep |
| Cached capacity plus last-blocking consumer | Full-ring retry median improved from 440M to 1,193M rejects/s with 3 consumers and from 172M to 1,186M with 8 | Keep |
| One release-store publishing the final batch sequence | Already implemented | Keep |
| 64-byte ring and sequence alignment | Previously improved the 64-byte-event median about 15% | Keep |
| Construction-time allocation and touching | Already implemented for in-ring storage | Keep |
| Explicit `ConsumerCount == 1` branch | GCC emitted byte-identical throughput and latency binaries | Reject as redundant |
| Manual four-event consumer unroll | Initial batch-16 gain collapsed to about 1% when run order reversed; no stable latency win | Reject as non-repeatable |
| Stateless consumer-local cursor | Hurt several modes by about 3–7%; improved two batched throughput modes about 24%; multicast p50/p99 worsened slightly | Reject as inconsistent |
| Thread-owned consumer handle caching position and acquired range | Publication/drain 64/64 improved from a five-run median of 812M to 904M events/s; batch-1 p50 stayed 65 ns and p99 moved from 126 to 125 ns | Keep as preferred fixed-index hot-loop API |
| Unsigned end-exclusive positions | Optimized, ASan/UBSan, TSan, capacity-one, batch-wrap, and near-`UINT64_MAX` tests pass | Keep for defined rollover correctness |
| Two contiguous-span callbacks around ring wrap | Improved single-consumer batch-16 throughput about 15–28%, but worsened its p50/p99 about 3–7% and did not improve multicast consistently | Reject from the latency-first API |
| 128-byte sequence spacing | Helped selected batched cases, but reduced small-event batch-1 throughput about 8% and was inconsistent elsewhere | Reject on this 64-byte-line CPU |
| Software prefetch | Previously reduced the 64-byte batch-16 median about 28% | Reject |
| Empty, `PAUSE`, yield, and adaptive polling | Measured separately; the winner changes with workload and core sharing | Keep as caller-selected policies |

## Batch and capacity sweep

`lls_disruptor_single_producer_configuration_sweep` tests batch sizes 1, 2, 4,
8, 16, 32, 64, and 128; capacities 1,024, 65,536, and 1,048,576; payloads of 8
and 64 bytes; and one- and three-consumer paths. Each latency case records
50,000 post-warm-up samples. Every throughput run verifies each consumer's
exact sequence order and payload before reporting `PASS`.

Consumers now keep validation state thread-local, affinity failures abort,
threads rendezvous before timing, order is checked exactly, and every byte of a
64-byte event is written and verified. Five-run medians after that benchmark
hardening were:

| Workload | Batch | Throughput | p50 | p99 |
|---|---:|---:|---:|---:|
| 8-byte, capacity 65,536 | 1 | 83M events/s | 80 ns | 140 ns |
| 8-byte, capacity 65,536 | 8 | 279M events/s | 280 ns | 330 ns |
| 8-byte, capacity 65,536 | 16 | 478M events/s | 485 ns | 511 ns |
| 8-byte, capacity 65,536 | 64 | 713M events/s | 2,528 ns | 2,889 ns |
| 64-byte, capacity 65,536 | 1 | 78M events/s | 220 ns | 436 ns |
| 64-byte, capacity 65,536 | 8 | 303M events/s | 1,547 ns | 1,718 ns |
| 64-byte, capacity 65,536 | 16 | 334M events/s | 2,529 ns | 2,869 ns |
| 64-byte, capacity 65,536 | 32 | 395M events/s | 4,051 ns | 5,163 ns |
| 8-byte multicast to 3, capacity 65,536 | 1 | 14M source events/s | 296 ns | 696 ns |
| 8-byte multicast to 3, capacity 65,536 | 16 | 121M source events/s | 846 ns | 1,061 ns |
| 8-byte multicast to 3, capacity 65,536 | 64 | 179M source events/s | 2,364 ns | 3,480 ns |

Latency uses capacity 1,024 because the producer waits for every complete batch;
the measured boundary isolates handoff rather than queue residence. Throughput
capacity changes the permitted producer lead and cache footprint. A 1,048,576
slot ring helped 8-byte throughput but hurt 64-byte locality; 65,536 remains the
balanced showcase capacity.

No batch size is universally best. Batch 1 minimizes handoff latency. Batch 8
offers a moderate compromise. Batch 16–64 prioritizes throughput and increases
per-event residence time. Producers should publish the events already ready,
not wait merely to fill a nominal batch.

## Compiler and deployment tournament

`-O2`, `-O3`, LTO, no LTO, generic tuning, native tuning, and `-Ofast` were run
in forward and reverse order. Winners changed by workload and run order. `-Ofast`
adds semantic risk without a stable benefit, while generic binaries sacrifice
host-specific intent. Retain `-O3`, LTO, and `-march=native` for host-local
benchmarking; portable releases must not use the native preset.

This VM exposes one NUMA node and no sibling SMT threads, so cross-NUMA memory
placement, sibling avoidance, explicit huge pages, frequency locking, and
real-time scheduling cannot be evaluated credibly here. They are deployment
experiments, not library defaults. Test them on the target machine and record
permissions, topology, page policy, and operational cost.

## Correctly excluded techniques

The tournament does not execute variants that weaken required synchronization,
overwrite unread events, skip slow consumers, drop delivery, change ordering,
or replace lossless backpressure with loss. Release publication, acquire
observation, release consumer completion, and acquire gating form the lifetime
edges that prevent concurrent reuse of an event. Removing them would benchmark
a different—and potentially data-racing—algorithm.

## Reproduce

```sh
cmake --preset benchmark-native
cmake --build --preset benchmark-native
./build/benchmark-native/benchmarks/lls_disruptor_single_producer_configuration_sweep
./build/benchmark-native/benchmarks/lls_disruptor_single_producer_next_frontier
```

Repeat the executable, retain distributions rather than a best run, and use
the repository benchmarking methodology before changing production decisions.

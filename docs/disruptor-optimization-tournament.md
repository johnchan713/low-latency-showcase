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
| Cached slowest-consumer gate | Already implemented | Keep |
| One release-store publishing the final batch sequence | Already implemented | Keep |
| 64-byte ring and sequence alignment | Previously improved the 64-byte-event median about 15% | Keep |
| Construction-time allocation and touching | Already implemented for in-ring storage | Keep |
| Explicit `ConsumerCount == 1` branch | GCC emitted byte-identical throughput and latency binaries | Reject as redundant |
| Manual four-event consumer unroll | Initial batch-16 gain collapsed to about 1% when run order reversed; no stable latency win | Reject as non-repeatable |
| Separate consumer-local cursor | Hurt several modes by about 3–7%; improved two batched throughput modes about 24%; multicast p50/p99 worsened slightly | Reject as inconsistent and larger |
| Two contiguous-span callbacks around ring wrap | Improved single-consumer batch-16 throughput about 15–28%, but worsened its p50/p99 about 3–7% and did not improve multicast consistently | Reject from the latency-first API |
| 128-byte sequence spacing | Helped selected batched cases, but reduced small-event batch-1 throughput about 8% and was inconsistent elsewhere | Reject on this 64-byte-line CPU |
| Software prefetch | Previously reduced the 64-byte batch-16 median about 28% | Reject |
| Empty, `PAUSE`, yield, and adaptive polling | Measured separately; the winner changes with workload and core sharing | Keep as caller-selected policies |

## Batch and capacity sweep

`lls_disruptor_single_producer_configuration_sweep` tests batch sizes 1, 2, 4,
8, 16, 32, 64, and 128; capacities 1,024, 65,536, and 1,048,576; payloads of 8
and 64 bytes; and one- and three-consumer paths. Each latency case records
50,000 post-warm-up samples. Every measured throughput run verifies all consumer
checksums.

Five-run medians for representative configurations were:

| Workload | Batch | Throughput | p50 | p99 |
|---|---:|---:|---:|---:|
| 8-byte, capacity 65,536 | 1 | 124M events/s | 84 ns | 150 ns |
| 8-byte, capacity 65,536 | 8 | 357M events/s | 265 ns | 285 ns |
| 8-byte, capacity 65,536 | 16 | 388M events/s | 471 ns | 495 ns |
| 8-byte, capacity 65,536 | 64 | 501M events/s | 1,728 ns | 1,813 ns |
| 64-byte, capacity 65,536 | 1 | 112M events/s | 110 ns | 130 ns |
| 64-byte, capacity 65,536 | 8 | 384M events/s | 326 ns | 426 ns |
| 64-byte, capacity 65,536 | 16 | 431M events/s | 526 ns | 705 ns |
| 64-byte, capacity 65,536 | 32 | 465M events/s | 992 ns | 1,047 ns |
| 8-byte multicast to 3, capacity 65,536 | 1 | 31M source events/s | 110 ns | 216 ns |
| 8-byte multicast to 3, capacity 65,536 | 16 | 167M source events/s | 531 ns | 586 ns |
| 8-byte multicast to 3, capacity 65,536 | 64 | 315M source events/s | 1,853 ns | 2,008 ns |

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
```

Repeat the executable, retain distributions rather than a best run, and use
the repository benchmarking methodology before changing production decisions.

# Single-producer Disruptor

Status: experimental

Target: Linux, GCC or Clang, C++23, x86-64 first

This capsule is a compact C++ implementation of the central Disruptor idea: a
preallocated ring buffer whose storage is multicast to independent consumers by
monotonic sequences. It is not an API-compatible port of the Java library.

## Use it when

- exactly one thread publishes;
- a fixed set of consumers must each observe every event in order;
- bounded memory and no allocation on the hot path matter;
- the caller can dedicate or otherwise manage the producer and consumer threads;
- backpressure is preferable to dropping or overwriting events.

Prefer a conventional queue when work should be divided among consumers rather
than multicast, blocking is desirable, consumers must be registered dynamically,
or the event stream must survive a process restart.

## Contract

`single_producer_disruptor<Event, Capacity, ConsumerCount>` constructs every
event once. `Capacity` is a power of two, enabling `sequence & (Capacity - 1)`
instead of division when a sequence maps to a ring slot.
The ring storage begins on a 64-byte cache-line boundary, preventing a
cache-line-sized event from straddling two lines because of ordinary heap
alignment.

- One thread owns all publication calls.
- One thread owns each consumer index.
- Exactly one live consumption path owns each consumer index. Prefer one
  `make_consumer<Index>()` handle per consumer thread; do not create multiple
  handles for the same index or mix handle and indexed consumption. The handle
  caches its position and last acquire-observed publication boundary.
- A consumer callback receives `const Event&` and must be `noexcept`.
- A producer callback receives `Event&` and must be `noexcept`.
- `try_publish` and `try_publish_batch` return `false` instead of blocking when
  the slowest consumer still owns a required slot.
- Batches are all-or-nothing and become visible with one release store.
- Destruction is only safe after all producer and consumer threads have stopped.

Sequence positions are unsigned and end-exclusive internally. Modular unsigned
distance keeps rollover defined when a long-running stream crosses
`UINT64_MAX`. The optional constructor position supports deterministic replay
and rollover testing; normal streams start at zero.

Construction value-initializes every in-ring event, so trivial in-ring storage
is allocated and touched before the hot path. If an event owns separate dynamic
buffers, the caller remains responsible for allocating and touching those
buffers before starting latency-sensitive threads.

The producer writes an event before release-publishing its sequence. Consumers
acquire that sequence before reading and release their completed sequences. The
producer acquire-reads the slowest consumer before reusing storage. These edges
both publish event data and prevent overwrite while any consumer can still read.
When backpressured, the producer checks the consumer that blocked the previous
attempt first. If it remains behind, the producer safely rejects without
touching every other cursor. Successful reuse still acquire-checks all consumers.

## Deliberate exclusions

The first version excludes multiple producers, dynamic consumer registration,
consumer dependency graphs, blocking wait strategies, dropping or overwriting,
persistence, networking, exception recovery, and a Java-compatible facade.

## Integration

Copy this directory and add it with `add_subdirectory`, or include it through the
repository registry. Link the header-only target:

```cmake
target_link_libraries(my_target PRIVATE lls::disruptor_single_producer)
```

See `examples/basic.cpp` for thread ownership and backpressure handling. Build
benchmarks with the `benchmark-native` preset; results are environment-sensitive
and are intentionally not latency-threshold CTest tests.

## Benchmarks

Build and run the native benchmark executables:

```sh
cmake --preset benchmark-native
cmake --build --preset benchmark-native
./build/benchmark-native/benchmarks/lls_disruptor_single_producer_benchmark
./build/benchmark-native/benchmarks/lls_disruptor_single_producer_latency_benchmark
./build/benchmark-native/benchmarks/lls_disruptor_single_producer_configuration_sweep
./build/benchmark-native/benchmarks/lls_disruptor_single_producer_next_frontier
```

The throughput benchmark transfers 5,000,000 events through a 65,536-slot ring.
It compares batch sizes 1 and 16, 8-byte and 64-byte events, a mutex-protected
ring, a specialized SPSC ring, and three independent SPSC queues. The multicast
case sends every source event to three consumers.

Hardened five-run medians from the development AMD EPYC virtual machine, using
GCC 13, `-O3`, LTO, native architecture tuning, pinned threads, exact sequence
validation, and complete payload checks:

| Workload | Observed result |
|---|---:|
| 8-byte event, batch 1 | 83 million events/s |
| 8-byte event, batch 16 | 478 million events/s |
| 64-byte event, batch 1 | 78 million events/s |
| 64-byte event, batch 16 | 334 million events/s |
| 8-byte batch-16 multicast to 3 consumers | 121 million source events/s |
| Blocking single-slot latency p50 | roughly 17–27 us |

The configuration latency sweep records 50,000 post-warm-up samples per
consumer. Median percentiles across five native, pinned runs were:

| Workload | Handoff p50 | Handoff p99 |
|---|---:|---:|
| 8-byte event, batch 1 | 80 ns | 140 ns |
| 8-byte event, batch 16 | 485 ns | 511 ns |
| 64-byte event, batch 1 | 220 ns | 436 ns |
| 64-byte event, batch 16 | 2,529 ns | 2,869 ns |
| 8-byte event, batch 16, multicast to 3 consumers | 846 ns | 1,061 ns |

The multicast distribution combines all three complete consumer handoffs. Each
run verifies exact sequence order. The 64-byte cases write and verify every
payload word; older measurements that touched only the first eight bytes are
not directly comparable.

The cache-line-aligned ring changed the collected 64-byte-event median from
about 83 million to 96 million events/s, approximately a 15% improvement on
this host. It does not enlarge an 8-byte event to 64 bytes; it aligns the start
of the ring so a cache-line-sized event does not unnecessarily span two lines.

Throughput is not individual-event latency. For example, 100 million events/s
means an average pipeline completion interval of 10 ns, while an event's
complete producer-to-consumer handoff still takes tens or hundreds of
nanoseconds. Batching increases throughput by sharing publication overhead, but
can increase latency if a producer waits to assemble a batch.

`consume_available` already performs adaptive consumer batching: it handles up
to the requested maximum but never waits for that many events. Producers can do
the same by passing only the number of source events that are already ready to
`try_publish_batch`; they should not wait merely to fill a larger batch.

For a fixed consumer index, `make_consumer<Index>()` is the preferred hot-loop
API. In the next-frontier sweep, publication/drain 64/64 improved from a
five-run median of 812M events/s through the indexed API to 904M through the
handle, about 11%. Batch-1 latency remained effectively unchanged: p50 65 ns
and p99 125–126 ns. The handle is not magic; if no acquired range can be reused,
its gain may vanish.

The cross-module wait-strategy scenario compares empty polling, x86 `PAUSE`,
and adaptive pause-then-yield from the [`spin-wait`](../spin-wait/) capsule. On
the shared development VM, its seven-run median was approximately:

| Polling policy | Throughput | Handoff p50 | Handoff p99 |
|---|---:|---:|---:|
| Empty loop | 88 million events/s | 85 ns | 145 ns |
| `PAUSE` | 84 million events/s | 95 ns | 145 ns |
| Adaptive: 64 pauses, then yield | 96 million events/s | 81 ns | 136 ns |

Adaptive waiting improved median throughput by about 9%, p50 by about 5%, and
p99 by about 6% on this shared VM. This is not a universal winner: on a truly
isolated physical core, yielding can worsen tails, so retain the policy choice
at the call site.
An explicit software prefetch experiment was rejected after reducing the
64-byte batch-16 median by about 28%; sequential hardware prefetch was already
sufficient.

These figures are observations, not portable guarantees. Shared virtual-machine
scheduling produced occasional throughput drops and 250–470 ns latency tails.
Repeat runs, retain the distribution, record the system configuration, and use
the repository's benchmarking methodology before making performance claims.
The full [optimization tournament](../../../docs/disruptor-optimization-tournament.md)
records tested winners, rejected variants, batch/capacity sweeps, and the
techniques that require dedicated deployment hardware.

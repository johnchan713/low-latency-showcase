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
- A consumer callback receives `const Event&` and must be `noexcept`.
- A producer callback receives `Event&` and must be `noexcept`.
- `try_publish` and `try_publish_batch` return `false` instead of blocking when
  the slowest consumer still owns a required slot.
- Batches are all-or-nothing and become visible with one release store.
- Destruction is only safe after all producer and consumer threads have stopped.

Construction value-initializes every in-ring event, so trivial in-ring storage
is allocated and touched before the hot path. If an event owns separate dynamic
buffers, the caller remains responsible for allocating and touching those
buffers before starting latency-sensitive threads.

The producer writes an event before release-publishing its sequence. Consumers
acquire that sequence before reading and release their completed sequences. The
producer acquire-reads the slowest consumer before reusing storage. These edges
both publish event data and prevent overwrite while any consumer can still read.

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

Build and run the two native benchmark executables:

```sh
cmake --preset benchmark-native
cmake --build --preset benchmark-native
./build/benchmark-native/benchmarks/lls_disruptor_single_producer_benchmark
./build/benchmark-native/benchmarks/lls_disruptor_single_producer_latency_benchmark
```

The throughput benchmark transfers 5,000,000 events through a 65,536-slot ring.
It compares batch sizes 1 and 16, 8-byte and 64-byte events, a mutex-protected
ring, a specialized SPSC ring, and three independent SPSC queues. The multicast
case sends every source event to three consumers.

Representative observations from the development AMD EPYC virtual machine,
using GCC 13, `-O3`, LTO, native architecture tuning, and pinned threads:

| Workload | Observed result |
|---|---:|
| 8-byte event, batch 1 | typically 120–140 million events/s |
| 8-byte event, batch 16 | typically 350–470 million events/s |
| 64-byte event, batch 1 | typically 85–140 million events/s |
| Source event multicast to 3 consumers | roughly 65–195 million events/s |
| Busy-spin handoff latency p50 | typically 60–100 ns |
| Busy-spin handoff latency p99 | typically 80–160 ns |
| Blocking single-slot latency p50 | roughly 17–27 us |

The cache-line-aligned ring changed the collected 64-byte-event median from
about 83 million to 96 million events/s, approximately a 15% improvement on
this host. It does not enlarge an 8-byte event to 64 bytes; it aligns the start
of the ring so a cache-line-sized event does not unnecessarily span two lines.

Throughput is not individual-event latency. For example, 135 million events/s
means an average pipeline completion interval of about 7.4 ns, while an event's
complete producer-to-consumer handoff still takes tens or hundreds of
nanoseconds. Batching increases throughput by sharing publication overhead, but
can increase latency if a producer waits to assemble a batch.

`consume_available` already performs adaptive consumer batching: it handles up
to the requested maximum but never waits for that many events. Producers can do
the same by passing only the number of source events that are already ready to
`try_publish_batch`; they should not wait merely to fill a larger batch.

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

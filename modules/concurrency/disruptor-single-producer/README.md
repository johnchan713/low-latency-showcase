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

These figures are observations, not portable guarantees. Shared virtual-machine
scheduling produced occasional throughput drops and 250–470 ns latency tails.
Repeat runs, retain the distribution, record the system configuration, and use
the repository's benchmarking methodology before making performance claims.

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

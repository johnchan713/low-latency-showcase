# Cross-module benchmark scenarios

Each directory here measures a declared end-to-end workload involving more
than one reusable module, or a complete operating-system path that precedes a
reusable module. It must identify the ordinary baseline, measured boundary,
environment, reproduction command, results, and limitations.

A benchmark concerning only one module belongs beside that module instead.

## Wait-strategy scenario

`lls_wait_strategy_benchmark` combines the single-producer Disruptor and the
spin-wait capsule. It compares equivalent empty-spin, `PAUSE`, and adaptive
pause-then-yield polling loops using pinned producer and consumer threads,
5,000,000 events for throughput, and 200,000 post-warm-up latency samples.

```sh
cmake --preset benchmark-native
cmake --build --preset benchmark-native
./build/benchmark-native/benchmarks/lls_wait_strategy_benchmark
```

The executable validates a checksum for every policy. Performance thresholds
are deliberately not CTest gates because scheduler behaviour and CPU topology
change the result.

## Socket-latency scenario

`lls_socket_latency_benchmark` measures validated single-request IPv4 loopback
RTT for TCP and UDP at 8, 64, 256, and 1,400 bytes. Its literal baseline makes
no transport-policy `setsockopt()` calls. Named profiles isolate receive spin,
`TCP_NODELAY`, `TCP_QUICKACK`, `SO_BUSY_POLL`, and connected UDP, while a
separate runner performs balanced profile tournaments and fresh baseline
confirmation.

```sh
cmake --preset benchmark-native
cmake --build --preset benchmark-native --target lls_socket_latency_benchmark
./build/benchmark-native/benchmarks/lls_socket_latency_benchmark --self-test
```

See [`socket-latency/README.md`](socket-latency/README.md) for the measurement
boundary, profile definitions, audited results, CPU cost, and loopback-only
interpretation limits.

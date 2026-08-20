# Spin wait

Status: experimental

Target: Linux, GCC or Clang, C++23, x86-64

This capsule supplies explicit waiting policies for nonblocking polling loops.
It does not own threads or hide when the scheduler may run.

## Policies

- `busy_spin_wait` executes x86 `PAUSE` after every unsuccessful poll. Use it on
  a dedicated physical core when latency is more important than CPU usage.
- `yield_wait` immediately yields to the scheduler. Use it on shared cores when
  additional and less predictable wake-up latency is acceptable.
- `adaptive_spin_wait<N>` pauses for `N` unsuccessful polls, then yields until
  useful work calls `reset()`.

```cpp
lls::concurrency::busy_spin_wait wait;

while (!stream.try_publish(writer)) {
    wait.wait();
}
wait.reset();
```

`PAUSE` is not a sleep and does not make an algorithm correct. It is a processor
hint inside an already-correct polling loop. Whether it improves throughput or
latency depends on contention, core sharing, microarchitecture, and workload;
measure it against an empty loop on the deployment hardware.

The repository's `lls_wait_strategy_benchmark` performs that comparison using
the single-producer Disruptor. Seven paired runs on the shared development VM
favoured `adaptive_spin_wait<64>`: its medians improved throughput by about 9%,
p50 handoff latency by about 5%, and p99 by about 6% versus an empty loop. Plain
`PAUSE` was not faster in that environment. These are host-specific observations,
not defaults that override measurement on an isolated production core.

## Integration

Copy this directory and add it with `add_subdirectory`, or include it through
the repository registry. Link:

```cmake
target_link_libraries(my_target PRIVATE lls::spin_wait)
```

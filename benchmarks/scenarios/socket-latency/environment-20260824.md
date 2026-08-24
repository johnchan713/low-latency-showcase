# Default socket baseline environment: 2026-08-24

## Result identity

- Base revision: `b93016c2539a3cc49aeb426d63de9436739f951b`
- Working branch: `agent/socket-baseline-benchmark-20260824`
- Benchmark source SHA-256: `e9efb07188a80990d9f73dd673e0c9753e6bc4292c83cd0b82e22b348e43ae43`
- Summarizer source SHA-256: `4c9872e64340fadcbcc0f01b67eb3f83bced05595d1775140989d7a887d1615f`
- Benchmark binary SHA-256: `11d1d05486f9a2b538ed30f54e6eaca1761ae0d69903a74449e255386417739a`
- Raw-result SHA-256: `c07ceb51a26073fbad552db0f9c21dc47449859f8b1eeeb5e1b37acee7ffc7fc`
- Summary SHA-256: `ecd8e64fb92145115c81c7159400723d852b387553ce83ea97b68e507daec5c4`
- Start: `2026-08-24T13:40:24+03:00`
- Finish: `2026-08-24T13:48:44+03:00`
- Wall duration: 500 seconds

The source tree was not committed at measurement time, so the source hashes
above, rather than the base revision alone, identify the measured program.

## Hardware and operating system

- CPU: Intel Xeon Platinum 8573C, exposed by a KVM virtual machine
- Visible topology: 9 online vCPUs (`0-8`), one socket, one NUMA node, one
  visible thread per core
- Measured vCPUs: client `2`, server `4`; each had a one-CPU affinity mask and
  a distinct visible core with no exposed SMT sibling
- CPU cgroup: CPUs `0-8` allowed, quota `800000/100000` (eight CPU equivalents)
- OS: Ubuntu 24.04.3 LTS
- Kernel: Linux 6.18.35 x86-64
- Current clocksource: TSC
- CPU frequency governor and turbo policy: not exposed
- CPU isolation, real-time scheduling, frequency locking, and NUMA binding:
  not applied
- Background host workload and steal time: not controlled from this guest

The benchmark interval increased cgroup CPU usage by 497,925,075 microseconds.
The shared cgroup was throttled in 4 periods for a total of 99,677 microseconds
during that interval. These cgroup counters include any concurrent work in the
same container and are environment diagnostics, not benchmark-thread metrics.

## Build

Compiler: GCC 13.3.0 (`g++ (Ubuntu 13.3.0-6ubuntu2~24.04)`). CMake, Ninja, and
Clang were unavailable in this environment, so the source was built directly
with the same strict/native intent as the benchmark preset:

```sh
g++ -std=c++23 -O3 -DNDEBUG -march=native -flto -pthread \
  -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion -Wshadow -Werror \
  benchmarks/scenarios/socket-latency/socket_latency.cpp \
  -o /tmp/lls_socket_latency_benchmark
```

## Workload

```sh
/tmp/lls_socket_latency_benchmark \
  --warmup 20000 --samples 200000 --runs 8 \
  --client-cpu 2 --server-cpu 4 --udp-timeout-ms 100 \
  > raw-results-20260824.csv

python3 benchmarks/scenarios/socket-latency/summarize.py \
  raw-results-20260824.csv --expected-runs 8 \
  > baseline-summary-20260824.csv
```

- Scope: default, untuned Linux IPv4 sockets over `127.0.0.1`
- Clock: `CLOCK_MONOTONIC_RAW`; median back-to-back read overhead 19 ns, not
  subtracted
- Order: eight-row Williams balance over TCP/UDP and 8/64/256/1400-byte wire
  messages
- Attempts: 200,000 measured RTTs after 20,000 warm-ups in every row; eight
  rows per treatment, 12.8 million measured attempts overall
- UDP measured response deadline: 100 ms
- Socket tuning: none; both TCP endpoints reported `TCP_NODELAY=0`
- Default buffers observed in every run: TCP send 3,939,840 bytes and receive
  131,072 bytes at both endpoints; UDP send/receive 212,992 bytes at both
  endpoints

## Validation and boundary

All 64 raw rows reported `PASS`. The independent summarizer accepted exactly
eight treatments in each of eight contiguous runs, every treatment in every
position, all 56 directed predecessor pairs, monotonic percentile/accounting
invariants, default socket state, and stable no-deadline-miss checksums.

This is a same-process, two-thread loopback measurement inside a shared virtual
environment. It includes application work, socket syscalls, scheduling, and the
local kernel network stack. It excludes a physical NIC, DMA, interrupts,
cabling, switches, a remote kernel, and a remote scheduler. It must not be
presented as two-machine latency, and its maxima should be read as shared-VM
scheduler diagnostics rather than hardware-network bounds.

# Socket-profile tuning environment: 2026-08-24

## Result identity

- Base revision: `b93016c2539a3cc49aeb426d63de9436739f951b`
- Working branch: `agent/socket-baseline-benchmark-20260824`
- Benchmark source SHA-256: `9784b7565a2a67b46ae2237ac0282e694e3cfc4bbeae250af3ba0556c11a6424`
- Tournament runner SHA-256: `18022081ec19c5be3a204e8fc1985ce1d12ae1e7be1ab48570b2ac7fcda0a9cd`
- Benchmark binary SHA-256: `c7b9dc208e3ca75b38440f71261c1934ab1655bf0164deb1a796991726eb684b`
- TCP screen raw/summary SHA-256: `8457a33584cb702270a4de1298fe3c92cb4eaf435222f92e1983d055967c58a7` / `9100d58c45955f1187d25845c069ee21611387ca2fdf6d048b6ea785e23c6657`
- UDP screen raw/summary SHA-256: `b16500dd283a6445dca8e29822f0fb120b457ffebe507d8452671b111095c037` / `0d692aadca9312c782d945f7adf91dc2f85d6fdc60521f7482c2e422201fc56c`
- TCP confirmation raw/summary SHA-256: `204469cc62001e91e1a74d525e909189a3955792cf6cc2fb158ead5d2eb6d987` / `02e762d0bcffc701ecdd53efd58d76256cdf0aa9a9b2c088233512dc673efc51`
- UDP confirmation raw/summary SHA-256: `10015b382a6a00e97fcc4c80f020498922d8a0a450dd3602fc8faa2a6fd35ee0` / `af86a528becb754b7cb0ba45afa0dc33fd5b21debde4763e66470260f3bd3c0c`
- Measured interval: `2026-08-24T12:56:29Z` through `2026-08-24T13:12:58Z`

The source tree was not committed at measurement time. The source and binary
hashes above identify the measured program in addition to the base revision.

## Hardware and operating system

- CPU: Intel Xeon Platinum 8573C, exposed by a KVM virtual machine
- Visible topology: 9 online vCPUs (`0-8`), one socket, one NUMA node, one
  visible thread per core
- Measured vCPUs: client `2`, server `4`; each thread was pinned to its own
  visible core
- CPU cgroup: CPUs `0-8` allowed, quota `800000/100000` (eight CPU equivalents)
- OS: Ubuntu 24.04.3 LTS
- Kernel: Linux 6.18.35 x86-64
- Clocksource: TSC
- CPU isolation, real-time scheduling, frequency locking, and NUMA binding:
  not applied
- Background host workload and steal time: not controlled from this guest

Cgroup throttle deltas in the raw data cover every isolated invocation, but
the counters include concurrent work in the same cgroup and are environment
diagnostics rather than benchmark-thread metrics.

## Build

Compiler: GCC 13.3.0 (`g++ (Ubuntu 13.3.0-6ubuntu2~24.04)`). CMake, Ninja, and
Clang were unavailable in this environment, so the benchmark was built
directly with strict warnings and host-native optimization:

```sh
g++ -std=c++23 -O3 -DNDEBUG -march=native -flto -pthread \
  -Imodules/concurrency/spin-wait/include \
  -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion -Wshadow -Werror \
  benchmarks/scenarios/socket-latency/socket_latency.cpp \
  -o /tmp/lls_socket_options_benchmark_reviewed
```

## Tournament workload

The 64-byte screens used 10,000 warm-ups and 50,000 measured attempts in each
isolated invocation. TCP compared eight profiles over eight macro-runs; UDP
compared six profiles over six macro-runs. A Williams-derived cyclic order
placed every condition in every ordinal position once.

```sh
python3 benchmarks/scenarios/socket-latency/run_profile_tournament.py \
  --binary /tmp/lls_socket_options_benchmark_reviewed \
  --output-dir /tmp/lls-tcp-options-reviewed-20260824 \
  --protocol tcp --payloads 64 \
  --profiles baseline tcp-nodelay tcp-spin tcp-nodelay-spin \
    tcp-quickack tcp-nodelay-quickack tcp-nodelay-quickack-spin \
    tcp-nodelay-busy-poll-50 \
  --observations 8 --warmup 10000 --samples 50000 \
  --client-cpu 2 --server-cpu 4

python3 benchmarks/scenarios/socket-latency/run_profile_tournament.py \
  --binary /tmp/lls_socket_options_benchmark_reviewed \
  --output-dir /tmp/lls-udp-options-reviewed-20260824 \
  --protocol udp --payloads 64 \
  --profiles baseline udp-connected udp-spin udp-connected-spin \
    busy-poll-50 udp-connected-busy-poll-50 \
  --observations 6 --warmup 10000 --samples 50000 \
  --client-cpu 2 --server-cpu 4
```

The confirmation used fresh interleaved baselines, not the historical baseline
CSV. For each protocol it compared the baseline with the selected profile at
8, 64, 256, and 1,400 bytes. The eight conditions occupied every position once
over eight macro-runs. Each condition therefore represents eight independent
invocations and 1.6 million measured attempts after 20,000 warm-ups per
invocation.

The TCP confirmation recorded 116 baseline and 81 tuned throttled periods
(18,108,277 and 16,481,875 microseconds respectively). The UDP confirmation
recorded 100 baseline and zero tuned periods (13,797,838 and zero
microseconds). These uneven overlaps contaminate exact tail and throughput
ratios, although the run-level p50 ranges remained separated by large margins.

```sh
python3 benchmarks/scenarios/socket-latency/run_profile_tournament.py \
  --binary /tmp/lls_socket_options_benchmark_reviewed \
  --output-dir /tmp/lls-tcp-full-reviewed-20260824 \
  --protocol tcp --payloads 8 64 256 1400 \
  --profiles baseline tcp-spin \
  --observations 8 --warmup 20000 --samples 200000 \
  --client-cpu 2 --server-cpu 4

python3 benchmarks/scenarios/socket-latency/run_profile_tournament.py \
  --binary /tmp/lls_socket_options_benchmark_reviewed \
  --output-dir /tmp/lls-udp-full-reviewed-20260824 \
  --protocol udp --payloads 8 64 256 1400 \
  --profiles baseline udp-connected-spin \
  --observations 8 --warmup 20000 --samples 200000 \
  --client-cpu 2 --server-cpu 4
```

RTT uses `CLOCK_MONOTONIC_RAW`. The requested `TCP_NODELAY` and `SO_BUSY_POLL`
values were read back before measurement; baseline rows confirmed zero for
both. `TCP_QUICKACK` is a transient per-exchange rearm rather than a persistent
state and is recorded as such. Every row passed message, sequence, checksum,
accounting, option-state, affinity, and anomaly validation.

## Interpretation boundary

This is a same-process, two-thread loopback experiment in a shared virtual
environment. It includes application work, socket syscalls, scheduling, and
the local kernel network stack. It excludes a physical NIC, DMA, interrupts,
cabling, switches, a remote kernel, and a remote scheduler. It cannot establish
two-machine latency or the benefit of NAPI-backed busy polling on a physical
interface.

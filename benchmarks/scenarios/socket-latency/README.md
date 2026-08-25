# Linux IPv4 socket latency baseline and profiles

This scenario measures application-to-application round-trip time through
Linux IPv4 kernel sockets. The literal untuned baseline establishes the control
workload; named profiles then isolate socket options and receive-wait policies.
Reusable versions of the selected primitives live in the
[`low-latency-sockets` module](../../../modules/networking/low-latency-sockets/).
This scenario remains the audited measurement harness.

The benchmark runs a client and echo server on `127.0.0.1` and keeps one
request in flight. Baseline TCP uses persistent blocking `send`/`recv`;
baseline UDP uses `sendto`, `poll`, and `recvmsg` with a declared response
deadline. The server
also polls with a 10 ms cancellation check so every error path has a bounded
join instead of depending on a best-effort stop datagram. Readiness calls are
part of every UDP RTT, so the TCP and UDP point estimates are separate
baselines rather than a controlled protocol race.

The `baseline` profile does not call `setsockopt()`. It uses `SOCK_CLOEXEC` for
descriptor lifetime hygiene and `MSG_NOSIGNAL` for process safety; neither
changes socket transport policy. Client and server buffer sizes, busy-poll
settings, and both TCP endpoints' `TCP_NODELAY` state are queried outside the
timed region to prove the active configuration.

Every exact-size wire message contains an eight-byte header:

- 16-bit magic;
- 8-bit format version;
- 8-bit protocol identifier; and
- 32-bit sequence number.

The remaining bytes follow a deterministic pattern. After each RTT timestamp,
the client validates the complete echoed message and updates an aggregate
sequence checksum. TCP compares that aggregate with an independently generated
expected value. The 8-byte case therefore remains exactly eight bytes rather
than silently growing to fit benchmark metadata.

Reported per-run metrics include attempted/successful samples, min, p50, p90,
p95, p99, p99.9, maximum, validated round trips per second, two logical
application messages per completed round trip, client/server/process CPU,
CPU nanoseconds per attempt, context switches, and UDP deadline misses,
requests not observed by the server before cancellation, response deadline
misses after an observed request, and combined client/server duplicate,
reordering, invalid-message, and truncation events. A deadline miss is an
application-level failure to complete within the configured deadline; it is
not proof that a packet was lost on the wire. The CSV records that deadline in
every row. The single TCP connect observation is diagnostic only, not a
connection-latency distribution.

The configured UDP deadline applies only to measured requests. Warm-up uses the
greater of that value and a five-second safety timeout, so a scheduler pause can
delay conditioning without silently changing or aborting the measured SLA.

The rate covers header preparation and post-RTT full-payload validation; each
individual RTT covers only send through complete echo receipt. Process CPU is
the combined client/server process consumption and can approach 200% with two
busy cores. RTT uses `CLOCK_MONOTONIC_RAW`; separately measured clock-read
overhead is reported but not subtracted. Server CPU accounting starts after the
last warm-up echo and normally ends after the final measured request is echoed;
if that final request is unobserved, it ends at bounded cancellation.

## Named tuning profiles

The executable supports a curated set of auditable profiles rather than
arbitrary option switches:

- TCP profiles isolate `TCP_NODELAY`, transient per-exchange `TCP_QUICKACK`,
  nonblocking receive spin, and 50 microseconds of `SO_BUSY_POLL`, plus the
  compatible combinations used by the tournament.
- UDP profiles isolate a connected datagram socket, nonblocking receive spin,
  their combination, and 50 microseconds of `SO_BUSY_POLL` with and without a
  connected socket.

Spin profiles call `recv` or `recvmsg` with `MSG_DONTWAIT` and use the project's
`PAUSE`-based spin primitive between unsuccessful receives. The UDP client
still enforces the declared absolute response deadline and the server retains
bounded cancellation. TCP spin has a five-second per-exchange safety watchdog;
it prevents an indefinitely live but stalled peer from burning a core and is
not reported as a TCP latency SLA. A spin profile can consume both pinned CPUs;
that cost is recorded rather than treated as a free socket optimization.

`TCP_NODELAY` and `SO_BUSY_POLL` are set on both endpoints and read back before
measurement. `TCP_QUICKACK` is intentionally recorded as a rearm policy because
Linux does not expose it as a permanent mode. Connected UDP changes peer/error
semantics and therefore remains a separately named profile.

## Reproduce

Build the host-tuned benchmark:

```sh
cmake --preset benchmark-native
cmake --build --preset benchmark-native --target lls_socket_latency_benchmark
```

Run the initial matrix with the declared client/server affinity (CPUs 2 and 4):

```sh
./build/benchmark-native/benchmarks/lls_socket_latency_benchmark \
  --warmup 20000 --samples 200000 --runs 8 \
  --client-cpu 2 --server-cpu 4 --udp-timeout-ms 100
```

Run one selected treatment directly:

```sh
./build/benchmark-native/benchmarks/lls_socket_latency_benchmark \
  --profile tcp-spin --protocol tcp --payload 64 \
  --warmup 20000 --samples 200000 --runs 1 \
  --client-cpu 2 --server-cpu 4
```

Run a balanced representative tournament. The condition count must be one or
even, and the observation count must be a multiple of that count so every
condition occupies every ordinal position equally:

```sh
python3 benchmarks/scenarios/socket-latency/run_profile_tournament.py \
  --binary ./build/benchmark-native/benchmarks/lls_socket_latency_benchmark \
  --output-dir /tmp/socket-tcp-screen \
  --protocol tcp --payloads 64 \
  --profiles baseline tcp-nodelay tcp-spin tcp-nodelay-spin \
  --observations 4 --warmup 10000 --samples 50000 \
  --client-cpu 2 --server-cpu 4
```

The runner rejects an existing output directory and emits `raw.csv`,
`summary.csv`, and `manifest.json` only after validating the captured rows.

When affinity is omitted, the harness selects the first two CPUs in its allowed
set; audited runs should pass both CPU indices explicitly. `--self-test` runs a
small correctness matrix.

The eight workloads use an eight-row Williams order: each workload occupies
each ordinal position once and every directed predecessor pair appears once.
Headline values should be medians of the eight per-run statistics; raw samples
must not be pooled. With 200,000 samples, p99.9 has roughly 200 tail observations
per run, so this is an initial baseline rather than a universal claim. The
summary also retains per-run minima and maxima for p50, p99, p99.9, validated
throughput, and combined process CPU to expose run dispersion.

Summarize a captured raw CSV without pooling latency samples:

```sh
python3 benchmarks/scenarios/socket-latency/summarize.py \
  raw.csv --expected-runs 8
```

The summarizer rejects truncated data or mixed sample, warm-up, and configured
deadline values. It independently checks all eight treatments, contiguous run
IDs, position balance, all 56 directed predecessor pairs, sample/deadline
accounting, monotonic percentiles, finite rates, default TCP Nagle state,
socket buffers, and stable checksums when every request completes on time.

## Audited loopback baseline: 2026-08-24

This pass used eight runs, 20,000 warm-ups and 200,000 measured attempts per
row on pinned vCPUs 2 and 4. Each treatment therefore has 1.6 million measured
attempts. Percentile values below are medians of per-run statistics; they do
not pool raw samples. `Min` is the median run minimum and `Max` is the global
maximum. Latencies through p99.9 are microseconds; maximum is milliseconds.

| Protocol | Bytes | Min (us) | p50 (us) | p90 (us) | p95 (us) | p99 (us) | p99.9 (us) | Max (ms) | Median RTT/s | Process CPU | UDP deadline misses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TCP | 8 | 13.601 | 21.463 | 25.757 | 50.987 | 203.719 | 2,012.415 | 664.590 | 27,045 | 83.933% | - |
| TCP | 64 | 13.724 | 21.417 | 24.781 | 44.091 | 148.050 | 949.931 | 100.336 | 34,483 | 92.898% | - |
| TCP | 256 | 13.837 | 21.430 | 22.782 | 40.341 | 170.408 | 1,422.529 | 437.146 | 31,417 | 91.678% | - |
| TCP | 1,400 | 14.406 | 22.128 | 23.792 | 42.584 | 151.260 | 997.982 | 505.544 | 33,429 | 91.828% | - |
| UDP | 8 | 12.737 | 20.785 | 22.483 | 39.812 | 156.790 | 1,520.660 | 99.382 | 30,317 | 90.091% | 6 / 1,600,000 |
| UDP | 64 | 12.876 | 20.741 | 22.537 | 35.803 | 146.528 | 1,178.583 | 88.789 | 34,489 | 92.030% | 46 / 1,600,000 |
| UDP | 256 | 13.039 | 20.968 | 23.823 | 42.413 | 166.803 | 1,604.370 | 84.748 | 31,051 | 90.904% | 0 / 1,600,000 |
| UDP | 1,400 | 13.019 | 21.432 | 23.174 | 41.883 | 161.041 | 1,273.838 | 99.099 | 31,356 | 91.078% | 15 / 1,600,000 |

Median p50 RTT stayed between 20.741 and 22.128 us across this matrix, but p99
was 146.528-203.719 us and scheduler-sensitive maxima reached 84.748-664.590
ms. The shared cgroup also recorded four throttled periods, so the tail is a
qualified property of this virtual environment rather than a portable socket
bound.

The 67 UDP deadline misses all followed requests observed and echoed by the
server; none were classified as an unobserved request. No duplicate,
reordering, invalid-message, or truncation event was observed at either
endpoint. This only establishes failure to complete within 100 ms and does not
establish wire packet loss.

Exact values and run dispersion are retained in
[`baseline-summary-20260824.csv`](baseline-summary-20260824.csv); all 64 rows,
including context switches, buffer defaults, TCP connect diagnostics, endpoint
CPU, and checksums, are in
[`raw-results-20260824.csv`](raw-results-20260824.csv). The build, hashes,
topology, cgroup observations, and measurement boundary are recorded in
[`environment-20260824.md`](environment-20260824.md).

## Audited profile tournament: 2026-08-24

The fast screen used a representative 64-byte message: it is a small message
without being the header-only 8-byte edge case or the near-MTU 1,400-byte edge
case. TCP and UDP were screened separately because their applicable options
and syscall paths differ. The screen used fresh controls, 10,000 warm-ups,
50,000 samples per isolated invocation, and a balanced cyclic order.

The dominant method was nonblocking receive spin. In the expanded TCP screen,
spin alone had the lowest median p50 at 5.184 microseconds, compared with 5.196
microseconds for `TCP_NODELAY` plus spin and 21.353 microseconds for baseline.
The paired p50 ratio favored the `TCP_NODELAY` variant by only 0.3% in five of
eight runs, while its paired p99 ratio was 14% higher and paired throughput 3%
lower. The data therefore does not establish a separate `TCP_NODELAY` benefit.
`TCP_QUICKACK` plus spin regressed p50 to 5.836 microseconds. Both endpoints
accepted and reported `SO_BUSY_POLL=50`, but it was neutral on loopback; this
does not test a physical NIC's NAPI busy-poll path.

For UDP, the connected-spin profile had the lowest screen p50 at 3.274
microseconds, versus 3.367 microseconds for spin alone and 20.663 microseconds
for baseline. Relative to spin alone, connected spin improved paired p50 by
about 2.9% and p99 in this pass but regressed paired p99.9 by about 19%.
Connected UDP without spin improved p50 by less than one percent. The selected
profiles were therefore `tcp-spin` for TCP and `udp-connected-spin` for UDP
under the declared lowest-p50 objective. They are the fastest among the tested
profiles, not a claim over every legal socket-option combination.

The finalists were then compared with fresh, interleaved baselines at all four
payloads using the original 20,000-warm-up, 200,000-sample scale. Each table
cell is the median of eight run-level statistics representing 1.6 million
attempts. `Change` uses the median of the eight same-macro-run p50 ratios, not
the ratio of pooled samples. Latencies are microseconds.

| Protocol | Bytes | p50 baseline -> tuned | p50 change | p99 baseline -> tuned | p99.9 baseline -> tuned | RTT/s baseline -> tuned | CPU baseline -> tuned | Deadline misses baseline -> tuned |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| TCP | 8 | 21.394 -> 5.189 | -75.7% | 187.195 -> 55.948 | 967.631 -> 337.136 | 33,071 -> 129,746 | 91.5% -> 199.9% | - |
| TCP | 64 | 21.446 -> 5.192 | -75.8% | 190.018 -> 56.631 | 1,003.913 -> 305.138 | 31,553 -> 131,458 | 91.3% -> 199.8% | - |
| TCP | 256 | 21.490 -> 5.261 | -75.6% | 199.765 -> 54.897 | 1,067.403 -> 293.838 | 32,342 -> 131,903 | 90.7% -> 199.8% | - |
| TCP | 1,400 | 22.151 -> 6.257 | -71.9% | 186.947 -> 64.179 | 1,020.309 -> 287.770 | 31,540 -> 109,693 | 91.2% -> 199.9% | - |
| UDP | 8 | 20.662 -> 3.251 | -84.3% | 201.248 -> 9.975 | 1,673.804 -> 201.564 | 30,589 -> 225,215 | 92.1% -> 199.9% | 28 -> 0 |
| UDP | 64 | 20.647 -> 3.266 | -84.2% | 186.698 -> 13.317 | 1,171.666 -> 217.807 | 33,442 -> 212,022 | 92.7% -> 199.8% | 74 -> 0 |
| UDP | 256 | 20.683 -> 3.318 | -84.1% | 171.525 -> 15.098 | 850.233 -> 203.976 | 34,962 -> 214,770 | 92.9% -> 199.8% | 93 -> 0 |
| UDP | 1,400 | 21.262 -> 4.035 | -81.0% | 171.120 -> 32.767 | 891.168 -> 256.709 | 33,743 -> 159,124 | 93.5% -> 199.9% | 44 -> 0 |

The tuned profile won every one of the 32 TCP and 32 UDP same-macro-run p50
comparisons. UDP recorded zero tuned and 239 baseline response-deadline misses
out of 6.4 million attempts per profile. Every request was observed by the
server, and neither profile produced a duplicate, reordering, invalid-message,
or truncation event. This is not evidence of wire packet loss. Several TCP
conditions and the 64/256-byte UDP baselines overlapped shared-cgroup
throttling, so exact tail and throughput ratios are contaminated. Maximum
latency remains a shared-VM scheduler diagnostic and was not used to select a
winner.

Selection-screen evidence is retained in
[`tcp-profile-screen-raw-20260824.csv`](tcp-profile-screen-raw-20260824.csv),
[`tcp-profile-screen-summary-20260824.csv`](tcp-profile-screen-summary-20260824.csv),
[`udp-profile-screen-raw-20260824.csv`](udp-profile-screen-raw-20260824.csv),
and [`udp-profile-screen-summary-20260824.csv`](udp-profile-screen-summary-20260824.csv).
Full-matrix evidence is in
[`tcp-profile-confirmation-raw-20260824.csv`](tcp-profile-confirmation-raw-20260824.csv),
[`tcp-profile-confirmation-summary-20260824.csv`](tcp-profile-confirmation-summary-20260824.csv),
[`udp-profile-confirmation-raw-20260824.csv`](udp-profile-confirmation-raw-20260824.csv),
and [`udp-profile-confirmation-summary-20260824.csv`](udp-profile-confirmation-summary-20260824.csv).
Build identity, hashes, exact commands, and environment limitations are in
[`tuning-environment-20260824.md`](tuning-environment-20260824.md).

## Follow-up receive-path tournament: 2026-08-25

A second tournament tested pause cadence, amortized control checks,
per-exchange `TCP_QUICKACK`, connected UDP `recv(MSG_TRUNC)`, and a peerless
`recvmsg` sensitivity. TCP retained its existing profile. For UDP, connected
`recv(MSG_TRUNC)` reduced paired p50 by 1.23-4.97% and paired p99 by
0.85-4.04% across 8-1,400 bytes, with zero correctness anomalies or deadline
misses in the selected 6.4-million-attempt confirmation. It did not improve
every p99.9 result, so it is exposed as a connected-socket path rather than
presented as a universal replacement.

The candidate list, qualifications, exact environment, summaries, raw rows,
and manifests are in [`tuning-20260825`](tuning-20260825/README.md).

## Interpretation boundary

Loopback traverses application code, socket syscalls, and the local kernel
network stack, but bypasses physical NICs, DMA, interrupt moderation, cables,
switches, and a remote scheduler. These numbers must not be presented as
two-machine latency, and `RTT / 2` is not a measured one-way latency.

## Reusable-code boundary

The tracked evidence records hashes for this exact benchmark implementation.
The reusable module was added after those measurements, so this scenario has
not been refactored to call it: doing so would change the measured source and
invalidate reconstruction of the uncommitted evidence snapshot. The module has
loopback correctness and concurrency tests, while its performance should be
remeasured under a new source identity before benchmark results are attributed
to it.

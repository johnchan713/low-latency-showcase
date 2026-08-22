# Paired C++ / LMAX Disruptor comparators

This subtree contains the reproducible comparator source and tracked summaries
for a same-machine, one-producer / one-consumer comparison. Each run emits raw
rows and manifests; published values are derived from measurements rather than
external documentation. The runners do not update the repository's headline
table automatically.

The throughput and latency comparators are separate workloads. Throughput
measures a sustained phase and permits multiple events in flight; latency
permits only one outstanding event, excluding backlog behind earlier events.
Do not combine their numbers or infer latency from an events-per-second rate.

## Completion-throughput workload contract

| Item | Contract |
|---|---|
| Ring | 65,536 preallocated entries |
| Logical event | One 64-bit value (8 bytes) |
| Producer batch `P` | Every claim is exactly `P`; event counts must be divisible by `P` |
| Producer claim | C++ `try_publish_batch(P)` versus Java `tryNext(P)` by default; Java `next(P)` is an explicitly labelled sensitivity |
| Drain limit `D` | Maximum events handled before one consumer sequence acknowledgement |
| Strict family | `D=P` |
| Opportunistic family | `D=65,536`, so the consumer drains the acquired available range |
| Wait | Busy spin; C++ uses x86 `PAUSE`, Java uses `BusySpinWaitStrategy` / `Thread.onSpinWait()` |
| Lifecycle | Within one benchmark process, one retained ring and the same two worker TIDs across every warm-up and measured phase |
| Throughput interval | Starts immediately before the first measured publish; stops when the producer acquire-observes the final consumer acknowledgement |

`P` and `D` are separate command-line fields and separate CSV columns. The named families intentionally constrain their relationship so a row cannot claim “strict” or “opportunistic” while running another policy. Supported `P` values are 1, 16, and 64; supported `D` specializations are 1, 16, 64, and 65,536.

The C++ program dispatches outside the timed region to compile-time `<P,D>`
specializations and uses the preferred thread-owned `make_producer()` and
`make_consumer<0>()` handles. Java uses separate literal `P=1`, `P=16`, and
`P=64` publication methods. Its drain cap remains the official runtime
`BatchEventProcessorBuilder.setMaxBatchSize(D)` path in LMAX Disruptor 4.0.0.
This real API/code-generation difference is part of the implementations being
compared; the runner records both build profiles and all execution flags.

The primary Java claim policy is `tryNext(P)`: like C++
`try_publish_batch(P)`, it makes one all-or-nothing nonblocking claim with
caller-side retry. C++ reports insufficient capacity with `false`, while Java
reports it with `InsufficientCapacityException`; that real API cost remains in
the timed comparison. `--java-claim-policy blocking-next` instead tests Java's
idiomatic blocking `next(P)` API. That sensitivity is recorded in every raw,
paired, summary, and manifest result and must not be described as the
workload-matched nonblocking-claim comparison.

## Equivalent validation contract

Each phase resets its logical value to zero while the retained ring's native sequence continues. For every callback, both programs perform the same branchless validation arithmetic:

```text
mismatch |= event.value XOR expected_logical
mismatch |= callback_sequence XOR (phase_first_sequence + expected_logical)
checksum += event.value
expected_logical += 1
```

After the timed boundary they require the exact event count, zero mismatch, and the exact wrapping 64-bit triangular checksum `N*(N-1)/2`. This catches payload corruption, gaps, duplicates, and reordering independently; a checksum alone would not catch a permutation.

Neither handler contains a per-event terminal test. After publishing the phase, each producer spins on the implementation's consumer sequence (`Sequence.get()` in Java and `consumer_position(0)` in C++) until an acquire load observes the known final released position, then timestamps immediately. Java subsequently halts its `BatchEventProcessor`; C++ lets its consumer phase finish naturally. Processor halt, phase-completion signalling, thread join, and scheduler wake-up time are excluded in both.

## Dependency lock

The only Java runtime dependency is:

```text
com.lmax:disruptor:4.0.0
SHA-256 c2ba80841541272bc815bcadab910d2d716aa563eca15762450ab4c889440505
size 85,721 bytes
```

[`dependencies.lock`](dependencies.lock) pins the URL, version, byte size, and digest. [`fetch_dependencies.sh`](fetch_dependencies.sh) refuses a jar that does not match both size and SHA-256. Maven and Gradle are not required.

## Producer-session optimization A/B: 2026-08-22

The current C++ path was measured directly against base revision `165eda81` on
the same pinned Intel Xeon Platinum 8370C host. The implementation commit in
this branch combines two changes: a thread-owned producer handle keeps its
cursor, conservative capacity cache, blocker index, and ring pointer private to
the producer session; producer and consumer batches traverse at most two
physical ring spans instead of masking every event index. The measurements
therefore apply to the combined change, not to either mechanism in isolation.

Every one of the 84 measured throughput rows passed affinity, event-count,
checksum, payload, and exact-order validation. Each configuration used seven
alternating-order pairs and two 100-million-event warm-ups per process; the
extra first-position assignment was alternated across configurations. P1 used
300 million measured events per run, while P16 and P64 used two billion.

| `P` | `D` | Family | Base median | Candidate median | Paired candidate/base (95% CI) |
|---:|---:|---|---:|---:|---:|
| 1 | 1 | strict | 80M/s | 216M/s | 2.70× (1.69–4.30) |
| 1 | 65,536 | opportunistic | 61M/s | 137M/s | 2.17× (1.24–3.81) |
| 16 | 16 | strict | 677M/s | 908M/s | 1.35× (1.29–1.40) |
| 16 | 65,536 | opportunistic | 864M/s | 1.32B/s | 1.46× (1.24–1.71) |
| 64 | 64 | strict | 962M/s | 1.43B/s | 1.53× (1.39–1.69) |
| 64 | 65,536 | opportunistic | 924M/s | 1.46B/s | 1.54× (1.40–1.69) |

All six paired confidence intervals exclude parity. P1 was especially
scheduler-sensitive: its base-first/candidate-first order effects were 1.61 and
2.10, respectively. Several batched order effects also exceeded the 0.95–1.05
ideal band. These are therefore qualified same-host results, not portable
throughput guarantees. The paired ratios are calculated from within-pair ratios
and are not quotients of the rounded medians.

The candidate throughput executable's text grew from 86,314 to 102,634 bytes
(18.9%). In return, GCC 13 keeps producer session state in registers in the P1
success loop, while the batched common path uses adjacent ring accesses and
isolates the rare physical wrap. This code-size/performance tradeoff is part of
the result.

The single-in-flight latency audit used 15 alternating pairs, one million
warm-up events, and two million measured events per process. All 30 rows passed
the same correctness and affinity gates. Lower ratios favour the candidate.

| Percentile | Base median | Candidate median | Paired candidate/base (95% CI) |
|---:|---:|---:|---:|
| p50 | 143 ns | 140 ns | 0.858 (0.609–1.209) |
| p90 | 170 ns | 159 ns | 0.918 (0.652–1.293) |
| p95 | 186 ns | 164 ns | 0.854 (0.603–1.209) |
| p99 | 265 ns | 221 ns | 0.855 (0.576–1.268) |
| p99.9 | 509 ns | 348 ns | 0.603 (0.340–1.071) |

Every displayed latency median decreased, but every confidence interval still
crosses parity. The defensible conclusion is **no statistically resolved
latency shift**, not a proven latency improvement or non-inferiority result.
Maximum latency remains a scheduler-sensitive diagnostic and is retained only
in the machine-readable data.

The exact summaries, order effects, pair-win counts, revisions, and event counts
are preserved in
[`producer-handle-throughput-20260822.csv`](producer-handle-throughput-20260822.csv)
and
[`producer-handle-latency-20260822.csv`](producer-handle-latency-20260822.csv).
The audited candidate binaries had SHA-256
`9655e049a713c9573eb6b919d1b67b23c4226c129880ed968f8b1e9c5f99f283`
(throughput) and
`6b34ae5d606eecfcfb78408bf65af621145422ee0155294df34b9863795a8a49`
(latency).

## Audited completion-throughput pass: 2026-08-21

The final pass used GCC 13.3.0, OpenJDK 17.0.19, and two individually pinned
physical cores on an AMD EPYC 9V74 development VM. All 112 measured rows passed
count, sequence, value, checksum, and affinity checks; the locked dependency
was verified for each run. Each configuration has seven alternating-order pairs
(a 4/3 split, with the extra first-language assignment alternated across
configurations), two 100-million-event warm-ups per process, and a minimum
accepted measured duration of one second.
[`audited-results-20260821.csv`](audited-results-20260821.csv) preserves the
exact summary values below in machine-readable form. The language columns are
seven-run medians; the ratio and confidence interval are calculated from the
seven within-pair ratios and are not the quotient of the displayed medians.

Primary nonblocking-claim comparison:

| `P` | `D` | C++ median | Java `tryNext` median | Paired geometric ratio (95% CI) | Order effect |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 186M/s | 97M/s | 2.20× (1.68–2.87) | 0.927 |
| 1 | 65,536 | 181M/s | 110M/s | 1.62× (1.30–2.02) | 1.318 |
| 16 | 16 | 742M/s | 275M/s | 2.85× (2.47–3.29) | 1.083 |
| 16 | 65,536 | 927M/s | 379M/s | 2.37× (1.82–3.09) | 1.155 |

Java blocking-claim sensitivity:

| `P` | `D` | C++ median | Java `next` median | Paired geometric ratio (95% CI) | Order effect |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 189M/s | 100M/s | 1.89× (1.70–2.10) | 0.988 |
| 1 | 65,536 | 205M/s | 136M/s | 1.47× (1.15–1.88) | 0.718 |
| 16 | 16 | 670M/s | 274M/s | 2.44× (1.60–3.71) | 0.846 |
| 16 | 65,536 | 748M/s | 428M/s | 1.66× (1.06–2.61) | 0.792 |

The order effect is the geometric-mean C++ / Java ratio in C++-first pairs
divided by the corresponding ratio in Java-first pairs. A value of 1.0 is
ideal; values outside 0.95–1.05 flag shared-VM drift. The lower confidence bound
remains above parity in all eight cases, but the flagged order effects are why
these are qualified same-machine results rather than a universal ranking.
Batch-1 phases used one billion measured events. The initial one-billion-event
batch-16 C++ phases completed in under one second and were therefore rejected;
the reported batch-16 phases use two billion events.

The four audited invocations were:

```bash
benchmarks/comparisons/disruptor/run_paired.py \
  --producer-cpu 2 --consumer-cpu 4 \
  --producer-batches 1 --families strict opportunistic \
  --events 1000000000 --warmup-events 100000000 --warmup-runs 2 \
  --samples 7 --minimum-duration-ms 1000 \
  --java-claim-policy try-next \
  --output-dir /tmp/lls-final-try-p1-both-20260821

benchmarks/comparisons/disruptor/run_paired.py \
  --producer-cpu 2 --consumer-cpu 4 \
  --producer-batches 16 --families strict opportunistic \
  --events 2000000000 --warmup-events 100000000 --warmup-runs 2 \
  --samples 7 --minimum-duration-ms 1000 \
  --java-claim-policy try-next \
  --output-dir /tmp/lls-final-try-p16-both-20260821

benchmarks/comparisons/disruptor/run_paired.py \
  --producer-cpu 2 --consumer-cpu 4 \
  --producer-batches 1 --families strict opportunistic \
  --events 1000000000 --warmup-events 100000000 --warmup-runs 2 \
  --samples 7 --minimum-duration-ms 1000 \
  --java-claim-policy blocking-next \
  --output-dir /tmp/lls-final-blocking-p1-both-20260821

benchmarks/comparisons/disruptor/run_paired.py \
  --producer-cpu 2 --consumer-cpu 4 \
  --producer-batches 16 --families strict opportunistic \
  --events 2000000000 --warmup-events 100000000 --warmup-runs 2 \
  --samples 7 --minimum-duration-ms 1000 \
  --java-claim-policy blocking-next \
  --output-dir /tmp/lls-final-blocking-p16-both-20260821
```

Choose two equivalent permitted physical cores on another host and a fresh `--output-dir`; an existing directory is rejected deliberately.

## Workload-matched handoff latency

[`run_paired_latency.py`](run_paired_latency.py) is a separate reproducible
latency workload. It uses one producer, one consumer, a retained 65,536-entry
ring, an 8-byte logical event containing only its publication timestamp, and
fixed `P=1`, `D=1`. Both producers make one nonblocking all-or-nothing claim,
timestamp only after that claim succeeds, and then publish. The consumer reads
the clock at handler entry. After every event, the producer acquire-waits for
the released consumer sequence before claiming the next event, so queue
residence is controlled rather than measured under backlog.

The C++ callback and Java `BatchEventProcessor` handler perform the same
straight-line sequence XOR, sequence checksum, latency calculation, and sample
store. Neither handler tests for the final event. A run is rejected unless its
count and triangular sequence checksum are exact, its order mismatch is zero,
all measured latencies are positive, and both hot threads prove their exact
one-CPU affinity masks. The existing launcher also keeps JVM helpers and its
own monitor off both hot cores.

Each process retains the same ring and worker TIDs across 1,000,000 warm-up and
1,000,000 measured events. Seven pairs alternate C++-first and Java-first
order. Percentile index `floor((N - 1) * p)` is applied within each run; the
table reports medians of the seven per-run percentiles rather than pooling
samples across runs. C++ uses `std::chrono::steady_clock`, Java uses
`System.nanoTime`, and no clock-read overhead is calibrated out; the differing
clock APIs remain part of the implementation comparison. This is therefore a
workload-matched implementation comparison, not a claim that the runtimes
execute identical clock instructions.

Audited shared-VM pass on CPUs 2 and 4, 2026-08-21:

| Percentile | C++ median | Java median | Paired C++ / Java geometric ratio (95% CI) | Order effect |
|---:|---:|---:|---:|---:|
| p50 | 75 ns | 95 ns | 0.640 (0.263–1.557) | 1.591 |
| p90 | 95 ns | 140 ns | 0.678 (0.289–1.593) | 0.976 |
| p95 | 125 ns | 150 ns | 0.751 (0.321–1.755) | 1.192 |
| p99 | 135 ns | 156 ns | 0.814 (0.355–1.864) | 0.998 |
| p99.9 | 145 ns | 186 ns | 0.757 (0.310–1.846) | 1.043 |
| maximum | 685,864 ns | 619,281 ns | 1.629 (0.602–4.407) | 0.394 |

The paired ratio is `C++ latency / Java latency`, so lower than 1.0 favours
C++ latency. The order effect is the geometric-mean ratio in C++-first pairs
divided by the ratio in Java-first pairs; 1.0 means no observed order effect.
p50, p95, and maximum order effects fall outside 0.95–1.05, and every
confidence interval crosses parity. These are descriptive medians from an
especially noisy shared-VM pass, not evidence of a statistically resolved
latency lead or a universal latency ranking. The exact values are retained in
[`audited-latency-results-20260821.csv`](audited-latency-results-20260821.csv).
Undocumented historical Java p50/p99 values are not reproducible evidence and
should not be kept in a comparison table.

Reproduce the workload with:

```bash
benchmarks/comparisons/disruptor/run_paired_latency.py \
  --producer-cpu 2 --consumer-cpu 4 \
  --warmup-events 1000000 --events 1000000 --samples 7 \
  --output-dir /tmp/lls-paired-latency-audit
```

Its output directory contains the exact compiled binary and Java classes,
`raw.csv`, `paired_ratios.csv`, `summary.csv`, `manifest.json`, and the tracked
source patch. The manifest records dependency, binary, class, source, topology,
clocksource, toolchain, flag, policy, and order hashes/metadata. Performance is
never a correctness pass gate. Use a fresh `--output-dir`; existing paths are
rejected.

## Run the throughput comparator

First choose two distinct, permitted physical cores. Prefer cores on the same NUMA node that are not SMT siblings.

```bash
taskset -pc $$
lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE
```

Then run from the repository root:

```bash
benchmarks/comparisons/disruptor/run_paired.py \
  --producer-cpu 2 \
  --consumer-cpu 4
```

Defaults are deliberately substantial: two 100-million-event untimed warm-ups,
one-billion-event measured phases, fifteen alternating-order samples, all three
`P` values, and both families. A measured phase shorter than one second is
rejected as methodologically too short; increase `--events` rather than
publishing the short result.

The runner:

1. verifies and downloads the locked jar;
2. compiles C++ with GCC or Clang, strict warnings, native tuning, and LTO;
3. compiles Java 17 with `-Xlint:all -Werror` (using the JDK compiler module if no `javac` launcher exists);
4. alternates C++-first and Java-first pair order;
5. rejects SMT-sibling worker choices, reserves all siblings of both hot cores, pins each Java worker TID individually, and moves JVM/helper/monitor threads to remaining housekeeping CPUs;
6. verifies affinity again inside both hot workers before every phase; and
7. emits raw records before computing any summary.

The default JVM uses a fixed one-gigabyte heap and Serial GC. It deliberately does not use `AlwaysPreTouch`: the JVM starts on housekeeping CPUs, while the retained ring is constructed only after the producer has been pinned and verified.

Useful narrower audit run (still requiring at least three pairs):

```bash
benchmarks/comparisons/disruptor/run_paired.py \
  --producer-cpu 2 \
  --consumer-cpu 4 \
  --producer-batches 16 \
  --families strict \
  --samples 3
```

Run the separately labelled Java blocking-claim sensitivity with:

```bash
benchmarks/comparisons/disruptor/run_paired.py \
  --producer-cpu 2 \
  --consumer-cpu 4 \
  --java-claim-policy blocking-next
```

Use `--cxx clang++` for the independent Clang strict-warning build/run. Java and its jar remain opt-in; normal CMake configuration never downloads them.

## Throughput output and acceptance

Each run directory contains:

- `build/`: the exact native binary and Java class files used by that run;
- `manifest.json`: toolchains, flags, CPU topology/sets, git state, hashes of every comparator source, the production header, built binary and Java classes, dependency hash, sample policy, and pair-order policy;
- `raw.csv`: one value-preserving metadata/result row per implementation and pair;
- `paired_ratios.csv`: per-pair C++ / Java throughput ratios; and
- `summary.csv`: medians, paired geometric-mean ratio, Student-t 95% confidence interval in log space, lower-CI parity/5%-margin flags, and an order-effect ratio.

Configuration, affinity, dependency, duration, count, ordering, value, or checksum failures stop the run. A throughput loss does not fail the build or CI: performance is evidence to review, not a correctness assertion.

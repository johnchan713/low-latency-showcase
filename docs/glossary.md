# Glossary

## Critical path

The sequence of work that directly determines when an operation completes.
Moving work away from the critical path can reduce latency even when total work
does not decrease.

## Jitter

Variation between observed latencies. Low average latency with high jitter can
be unsuitable when deadlines or predictable response times matter.

## Latency percentile

The value below which a percentage of observations fall. For example, p99 is
the latency met or beaten by 99% of samples; the slowest 1% are above it.

## Tail latency

The slower end of a latency distribution, commonly represented by p99, p99.9,
or higher percentiles. Tail latency often exposes scheduling, contention,
allocation, page-fault, or queueing effects hidden by an average.

## Throughput

The amount of work completed per unit of time. Improving throughput does not
necessarily improve the latency of an individual operation, and batching often
trades one for the other.

## False sharing

Independent values sharing a cache line while different cores write them. The
values are logically unrelated, but cache-coherence traffic makes the cores
interfere.

## Lock-free

A progress guarantee that the system as a whole continues to make progress even
if an individual participating thread stalls. It does not mean wait-free,
contention-free, automatically faster, or automatically correct.

## Wait-free

A progress guarantee that each participating operation completes in a bounded
number of its own steps. Real elapsed time can still be affected by scheduling,
interrupts, page faults, and hardware behaviour.

## TSC

The x86 timestamp counter. It can provide low-overhead cycle measurements on
suitable systems, but ordering, invariance, migration, calibration, and
virtualization must be handled explicitly.

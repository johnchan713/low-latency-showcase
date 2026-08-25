# Socket receive follow-up: 2026-08-25

This follow-up tested whether the already selected busy-spin socket profiles
could be improved without changing the one-request/one-response workload or
weakening message validation. It ran on Linux 6.18.35 in KVM on an AMD EPYC
9V74 with GCC 13.3.0. Client and server were pinned to vCPUs 0 and 1.

## Candidates

- TCP: execute `PAUSE` every fourth empty receive, omit `PAUSE`, and rearm
  `TCP_QUICKACK` for each exchange.
- UDP: execute `PAUSE` every fourth empty receive or omit it; amortize
  cancellation/deadline checks over 64 empty receives; receive a connected
  datagram with `recv(MSG_TRUNC)` instead of address-returning `recvmsg`; and
  combine the receive and polling variants.
- A later sensitivity used `recvmsg(MSG_TRUNC)` without a peer-address output.

Batching, UDP segmentation, and zero-copy were not candidates because they
change this serialized small-message workload or add completion-management
semantics. `SO_BUSY_POLL` and `TCP_NODELAY` were not repeated because the
2026-08-24 tournament had already found no repeatable loopback benefit.

[Linux `recv(2)`](https://man7.org/linux/man-pages/man2/recvfrom.2.html)
documents that per-call `MSG_DONTWAIT` does not change the socket's open file
description and that `MSG_TRUNC` returns an Internet datagram's original wire
length. The selected path therefore keeps descriptor mode unchanged and can
reject an undersized receive instead of silently accepting it.

## Selection

The TCP pause variants traded wins between central and tail metrics, while
per-exchange `TCP_QUICKACK` was clearly slower. The existing `tcp-spin` profile
is retained.

For UDP, connected `recv(MSG_TRUNC)` was the reproducible improvement. The
balanced confirmation used 32 observations per payload/profile, 10,000 warm-up
round trips, and 50,000 measured attempts per invocation: 1.6 million attempts
per table cell. Values are medians of run-level statistics; paired changes are
medians of same-position ratios, not ratios of pooled samples.

| Bytes | Existing p50 -> `recv` p50 | Paired p50 change | Existing p99 -> `recv` p99 | Paired p99 change | p99.9 result |
|---:|---:|---:|---:|---:|---:|
| 8 | 2.324 -> 2.244 us | -2.22% | 3.130 -> 3.135 us | -4.04% | -3.60% paired |
| 64 | 2.354 -> 2.253 us | -3.39% | 3.350 -> 3.145 us | -0.85% | +0.53% paired |
| 256 | 2.413 -> 2.303 us | -4.97% | 3.350 -> 3.105 us | -3.71% | -3.40% paired |
| 1,400 | 2.503 -> 2.463 us | -1.23% | 3.791 -> 3.481 us | -3.28% | +13.49% paired |

The connected-`recv` profile won 96 of 128 paired p50 observations. It recorded
zero UDP deadline misses, cgroup-throttled periods, duplicate, reordered,
invalid, or truncated messages across 6.4 million measured attempts. A
separate 1,400-byte run at 200,000 samples per observation again improved p50
and p99, but p99.9 was 4.86% worse. There is therefore no honest claim that the
new path improves every tail percentile.

The peerless-`recvmsg` sensitivity was rejected: a high-resolution rerun
overlapped cgroup throttling, and another cleanly scheduled retry still showed
intermittent deadline misses and a worse 1,400-byte tail. Those captured rows
remain in `udp-selected-confirmation` as negative evidence.

## Evidence and reproduction

- `tcp-screen`: 64-byte TCP candidate screen.
- `udp-screen`: 64-byte UDP candidate screen.
- `udp-confirmation`: selected connected-`recv` all-payload confirmation.
- `udp-1400-tail`: higher-resolution near-MTU tail check.
- `udp-selected-confirmation`: rejected peerless-`recvmsg` sensitivity.

Each directory contains validated `raw.csv`, `summary.csv`, and `manifest.json`
files. The manifest records the exact invocation, binary and runner hashes, and
ordering. Reproduce the selected comparison with:

```sh
python3 benchmarks/scenarios/socket-latency/run_profile_tournament.py \
  --binary ./build/benchmark-native/benchmarks/lls_socket_latency_benchmark \
  --output-dir /tmp/socket-udp-recv-confirmation \
  --protocol udp --payloads 8 64 256 1400 \
  --profiles udp-connected-spin udp-connected-recv-spin \
  --reference-profile udp-connected-spin \
  --observations 32 --warmup 10000 --samples 50000 \
  --client-cpu 0 --server-cpu 1
```

These are local-kernel loopback results from a shared VM. They do not include a
physical NIC, network path, remote scheduler, or measured one-way latency.

# Low-latency sockets

Status: experimental

Target: Linux, GCC or Clang, C++23, x86-64

This capsule provides small, allocation-free primitives for the receive paths
that won the project's Linux IPv4 loopback socket tournament. It is deliberately
non-owning: callers retain descriptor, buffer, thread, affinity, deadline, and
backpressure ownership. The module creates no threads and performs no hidden
allocation.

Link `lls::low_latency_sockets`. The target depends only on
`lls::spin_wait` and the Linux socket API.

## Selected profiles

The measured lowest-p50 profiles were:

- TCP: persistent sockets with a `MSG_DONTWAIT` receive loop and x86 `PAUSE`;
- UDP: sockets connected with POSIX `connect()`, followed by connected send and
  a `MSG_DONTWAIT` receive loop with x86 `PAUSE`.

In the audited benchmark implementation, TCP busy spin reduced median p50 RTT
by 71.9-75.8% across 8-1,400-byte payloads. Connected UDP busy spin reduced it
by 81.0-84.3%. Both tuned paths consumed approximately two logical CPU cores
across the client and echo server. These are workload-specific loopback results,
not measurements of this subsequently extracted header, remote-host results, or
one-way latency claims. See the
[benchmark evidence](../../../benchmarks/scenarios/socket-latency/README.md).

`TCP_NODELAY` did not add a repeatable benefit to TCP spin in that experiment,
and `SO_BUSY_POLL=50` was neutral on loopback. They remain explicit opt-in
helpers because another workload or a physical NIC can behave differently.
`TCP_QUICKACK` is exposed as a rearm operation rather than a permanent mode.

## API and behaviour

| Operation | Behaviour |
|---|---|
| `send_all` | Blocking exact TCP-style send with partial-write and `EINTR` handling. |
| `receive_exact_blocking` | Blocking exact stream receive; reports partial progress and peer close. |
| `receive_exact_busy_spin` | Per-call nonblocking stream receive using `MSG_DONTWAIT` and `PAUSE`. |
| `send_connected_datagram` | One possibly blocking datagram send through a socket previously connected with `connect()`. |
| `send_datagram_to` | One possibly blocking datagram send to an explicit IPv4 peer. |
| `receive_datagram_blocking` | Blocking `recvmsg`; preserves peer, flags, and original wire length. |
| `try_receive_datagram` | One nonblocking `recvmsg`; returns `not_ready` for `EAGAIN`. |
| `receive_datagram_busy_spin` | Repeats the nonblocking receive with `PAUSE` until data or caller stop. |
| option helpers | Observe/apply `TCP_NODELAY` and `SO_BUSY_POLL`; rearm `TCP_QUICKACK`. |

The busy-spin functions accept a caller-supplied stop predicate. The `_until`
convenience overloads adapt any `std::chrono` clock deadline. The predicate is
checked after an unsuccessful or interrupted receive, so cancellation is
cooperative; it does not reject data already returned successfully by the
kernel. `MSG_DONTWAIT` applies only to each receive call and does not set
`O_NONBLOCK` on the descriptor. Exceptions from a caller-provided predicate
propagate; use a `noexcept` predicate when the calling path must not throw.

```cpp
#include <lls/networking/low_latency_sockets.hpp>

std::array<std::byte, 64> response{};
const auto result = lls::networking::receive_exact_busy_spin_until(
    tcp_fd,
    response,
    std::chrono::steady_clock::now() + std::chrono::milliseconds{5});

if (!result.complete()) {
    // Inspect result.state, result.bytes_received, and result.error.
}
```

For the selected UDP profile, connect both endpoint sockets during setup and
keep that policy out of the timed receive path:

```cpp
// POSIX connect(udp_fd, peer, peer_length) is performed during setup.
const auto sent = lls::networking::send_connected_datagram(udp_fd, request);
const auto received = lls::networking::receive_datagram_busy_spin_until(
    udp_fd, response, deadline);
```

## Failure, blocking, and backpressure

- Socket operations return status, byte progress, and `std::error_code`; they
  do not throw internally. A caller-provided spin stop predicate remains caller
  code and may propagate its own exception.
- `send_all` and the blocking receive can wait indefinitely on a blocking
  descriptor. Use transport-level flow control and an application shutdown
  policy appropriate for the workload.
- Both datagram send functions inherit the descriptor's blocking mode. A
  blocking UDP send can wait for local buffer space; a nonblocking one returns
  the kernel error such as `EAGAIN`. A successful datagram is atomic rather than
  a partial-write retry loop.
- A stream peer close is distinct from an error and retains the number of bytes
  already received.
- Datagram receive uses Linux `MSG_TRUNC`, so an undersized buffer exposes both
  truncation and the original datagram size instead of silently accepting it.
- Connected UDP changes peer filtering and asynchronous error delivery. Treat
  it as a transport-policy choice, not merely a syscall shortcut.
- Busy spin intentionally keeps a core runnable. Use it only with an isolated
  or otherwise budgeted CPU; shared cores, oversubscription, VM scheduling, and
  cgroup throttling can make tail latency worse.
- `SO_BUSY_POLL` support and privileges are kernel/environment dependent. The
  configuration helper verifies the value by reading it back.

The option observation and application functions are separate. A literal
baseline can query state without calling `setsockopt()`; false or zero still
means an explicit configuration request when passed to an apply helper.

## Evidence boundary

The tracked 2026-08-24 CSVs identify the exact benchmark source that produced
them. This reusable capsule was added afterward, so the benchmark remains an
unchanged audited snapshot rather than being refactored to call this header.
The module's semantics are covered by loopback regression tests, but its
performance must be remeasured after a production integration or any benchmark
switch-over. Historical benchmark numbers are not silently attributed to a
new binary.

## Integration

Through the repository registry:

```cmake
target_link_libraries(my_target PRIVATE lls::low_latency_sockets)
```

When copying this capsule independently, also copy `spin-wait`, register it
first, and then add this directory with `add_subdirectory()`.

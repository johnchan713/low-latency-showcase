# Networking modules

Socket configuration, protocol helpers, buffering strategies, and other
independently reusable networking capabilities belong here.

Every module must document operating-system requirements, blocking behaviour,
backpressure, failure handling, and the workload used for latency claims.

Available capsules:

- [`low-latency-sockets`](low-latency-sockets/) provides non-owning Linux IPv4
  stream/datagram I/O, explicit socket-option helpers, and busy-spin receive
  policies selected by the benchmark.

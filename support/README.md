# Repository support

Support code provides shared development infrastructure for this repository. It
is not part of the stable production API and production modules must not depend
on it for their core behaviour.

Tests may use `support/testing/`. Benchmarks may use
`support/benchmarking/`. A support facility that becomes independently useful,
documented, and stable can later be promoted into `modules/tooling/`.

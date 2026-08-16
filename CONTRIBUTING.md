# Contributing

Contributions should help another programmer decide whether a low-latency
technique fits a real workload. Fast-looking code without correctness evidence
or measurement context is not ready for this repository.

## Before writing code

Open an issue or proposal describing:

- the problem and expected workload;
- the ordinary baseline implementation;
- the proposed technique;
- the expected latency mechanism;
- portability requirements; and
- the trade-off being accepted.

## Module capsule

Place an independently reusable capability under
`modules/<category>/<module>/`. A complete capsule can contain:

- `README.md` based on `docs/technique-template.md`;
- `CMakeLists.txt` defining independent, predictably named targets;
- `include/lls/<category>/` containing the public API;
- optional `src/` containing private implementation;
- `examples/` showing the smallest useful standalone usage;
- `tests/` checking correctness and concurrency guarantees; and
- `benchmarks/` comparing the capability with an ordinary baseline.

Omit directories that are not needed. A folder represents one capability a
user may integrate, not every private implementation class.

Register every capsule explicitly in `modules/CMakeLists.txt`. Do not use
automatic source globbing: a visible list makes build and review scope
deterministic. Directory names use kebab-case; C++ files, target names, and
identifiers use snake_case.

Use `lls_<name>` for the concrete library target and `lls::<name>` for its
alias. Tests, examples, and benchmarks use the suffixes `_test`, `_example`,
and `_benchmark` respectively.

A production module must not depend on `support/testing/` or
`support/benchmarking/`. It must configure when copied into a small external
CMake project using only its documented dependencies. Header-only targets
expose `cxx_std_23` directly with `INTERFACE` scope; their compiled tests and
examples receive strict repository options through `lls_apply_project_options()`.
Compiled module targets expose their public language requirement directly and
may apply repository options when that command is available.

Single-module examples and benchmarks remain beside their module. Root
`examples/` and `benchmarks/scenarios/` are reserved for integrations involving
multiple modules.

Every module must apply the repository build gates inside its own
`CMakeLists.txt`:

```cmake
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

if(LLS_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(LLS_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()
```

Omit a block when the corresponding directory does not exist. These conditions
keep production-only builds small and prevent benchmark targets from depending
on support infrastructure that was deliberately disabled.

Before a module is marked integration-ready, add a smoke test proving that a
minimal external CMake project can include the capsule with `add_subdirectory()`
and link its documented `lls::<name>` target.

## Code expectations

- Target Linux with GCC or Clang. Windows and MSVC are not currently supported.
- Use portable C++23 within that platform scope unless a documented compiler,
  operating-system, or architecture feature is central to the technique.
- Keep compiler options target-scoped.
- Avoid hidden global state and undocumented allocation.
- State thread-safety and memory-ordering guarantees.
- Add correctness tests before benchmark claims.
- Format C++ using the repository `.clang-format` file.
- Keep warnings clean with the `dev` preset.

## Validation

Run:

```sh
cmake --preset dev
cmake --build --preset dev
ctest --preset dev
```

For applicable changes, also run `asan-ubsan` and `tsan`. Sanitizer builds are
correctness tools; their timings must not appear in benchmark results.

Follow `docs/benchmarking-methodology.md` for performance experiments. Include
raw output or a machine-readable result where practical.

## Pull requests

A pull request should explain what changed, why the technique may reduce
latency, where it should and should not be used, and how correctness and
performance were checked.

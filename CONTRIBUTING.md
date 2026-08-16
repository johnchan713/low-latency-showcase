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

## Snippet capsule

Place a focused demonstration under `snippets/<category>/<technique>/` with:

- `README.md` based on `docs/technique-template.md`;
- `main.cpp` containing the minimal demonstration;
- `benchmark.cpp` comparing it with a baseline;
- `test.cpp` checking correctness; and
- `CMakeLists.txt` defining independent targets.

Register every capsule explicitly in `snippets/CMakeLists.txt`. Register every
reusable component in `components/CMakeLists.txt`. Do not use automatic source
globbing: a visible list makes build and review scope deterministic.

Do not make a snippet depend on another snippet. Shared code belongs in a
component only after its interface and ownership are clear.

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

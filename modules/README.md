# Modules

Each directory below a category is a self-contained capsule representing one
independently reusable capability. A module owns its public API, implementation,
documentation, examples, tests, benchmarks, and build targets.

Use this shape, omitting directories that the module does not need:

```text
<category>/<module>/
├── README.md
├── CMakeLists.txt
├── include/lls/<category>/
├── src/
├── examples/
├── tests/
└── benchmarks/
```

A folder boundary represents something a user may reasonably integrate. Small
private helper classes stay with their owning module instead of becoming
separate capsules.

Directory names use kebab-case, while C++ filenames and identifiers use
snake_case. Register every module explicitly in `modules/CMakeLists.txt`; source
globbing is not accepted.

Production targets must not require repository-only testing or benchmarking
support. A copied module must configure through `add_subdirectory()` using only
its documented production dependencies.

Each module's `CMakeLists.txt` gates its own `tests/`, `examples/`, and
`benchmarks/` directories with `BUILD_TESTING`, `LLS_BUILD_EXAMPLES`, and
`LLS_BUILD_BENCHMARKS` respectively.

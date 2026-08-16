# Snippets

Snippets are small, independent demonstrations organized as
`snippets/<category>/<technique>/`. Each capsule owns its documentation,
example, correctness test, benchmark, and CMake targets.

Examples must not depend on other snippets. If implementation is genuinely
reusable, promote it into `components/` after its interface and guarantees have
stabilized.

Register every capsule explicitly in `snippets/CMakeLists.txt`. The root build
loads that registry, ensuring registered snippets participate in configuration
and CI.

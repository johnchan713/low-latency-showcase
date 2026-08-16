# Integration examples

This directory is for runnable examples that combine multiple modules into a
larger scenario. A demonstration of one module stays in that module's
`examples/` directory.

Examples may depend on modules. Modules must never depend on examples. Register
each integration example explicitly in `examples/CMakeLists.txt`.

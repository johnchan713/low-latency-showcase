# Applications

Applications are repository-level executables rather than reusable modules.
They may depend on shared headers and multiple modules, but modules must never
depend on an application.

Register each application explicitly in `apps/CMakeLists.txt`.

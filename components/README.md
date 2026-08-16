# Components

Components are reusable implementations rather than demonstrations. A component
must have a clear public interface, documented ownership and concurrency rules,
correctness tests, representative benchmarks, and explicit limitations.

Keep each component independently targetable. Architecture-specific and
operating-system-specific implementations must be isolated from their portable
interfaces.

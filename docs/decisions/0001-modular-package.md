# ADR 0001: One modular package and staged dependencies

**Status:** Accepted for Phase 0 scaffolding.

**Date:** 2026-10-10.

## Context

The repository began with an MIT license and no implementation. The long-term
system needs multiple execution modes, but its first engine must be easy to
inspect and verify. Creating services or frameworks now would make the smallest
correctness case depend on unnecessary infrastructure.

## Decision

Use Python 3.12+ with a `src/distributed_backtester/` package, setuptools packaging,
pytest, Ruff, strict mypy, and GitHub Actions. Phase 0 contains a docstring-only
package and an installation smoke test. Keep simulation contracts in documents.

Use modules with explicit inputs/outputs. Add backend abstractions in Phase 2
when sequential and multiprocessing implementations provide a real reason for
the interface. Make Ray an optional dependency in Phase 3. Add storage libraries
in Phase 5 and Docker in Phase 7. NumPy and pandas are preferred available tools,
but are not automatic dependencies for CSV parsing or exact accounting.

## Alternatives

- Independent services provide deployment boundaries but require serialization,
  networking, monitoring, and recovery before a working simulator exists.
- A large plugin framework promises extensibility but makes simple cases harder
  to trace and encourages speculative interfaces.
- A pandas-first engine is convenient for research; strategy and accounting
  timing still need explicit event semantics, and the dependency is unnecessary
  for the first small synthetic cases.

## Consequences

The same simulation function can be invoked in-process, in a process worker, or
remotely. Module boundaries must remain disciplined because there are no service
boundaries enforcing them. The package will evolve incrementally, with no
performance or distributed-readiness claim from this decision alone.

# Architecture Decision Records

Each ADR records context, a decision, alternatives, and consequences. Phase 0
decisions establish the repository foundation. Simulation and distributed
decisions become accepted only when the relevant implementation phase is approved.

| Record | Status | Scope |
| --- | --- | --- |
| [0001: Modular package](0001-modular-package.md) | Accepted for Phase 0 | Repository shape and dependency introduction |
| [0002: Simulation semantics](0002-simulation-semantics.md) | Accepted and implemented in Phase 1 | Data policy, event order, sizing, accounting |
| [0003: Reproducibility and results](0003-reproducibility-and-results.md) | Phases 1-5 implemented; see ADRs 0006 and 0007 | Semantic results, identity, execution and analytical guarantees |
| [0004: Bounded local execution](0004-bounded-local-execution.md) | Accepted and implemented in Phase 2 | Spawn initialization, streaming export, source identity, benchmark scope |
| [0005: Ray actor execution](0005-ray-actor-execution.md) | Accepted for Phase 3 | Optional actor pool, resources, data placement, ownership, measurement scope |
| [0006: Durable scheduling](0006-durable-scheduling.md) | Accepted for Phase 4 | Coordinator SQLite journal, bounded attempts, fencing, recovery, and export repair |
| [0007: Versioned storage and analytics](0007-versioned-storage-and-analytics.md) | Accepted for Phase 5 | Exact Parquet, verified cache, analytical snapshots, and measurement boundaries |
| [0008: Measured performance](0008-measured-performance.md) | Accepted and implemented in Phase 6 | Scoped profiling, immutable wheel comparisons, full economics verification |

Update status when a checkpoint is approved. Supersede a consequential decision
with a new ADR and link the records; preserve the reasoning for interview review.

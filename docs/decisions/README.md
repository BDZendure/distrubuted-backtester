# Architecture Decision Records

Each ADR records context, a decision, alternatives, and consequences. Phase 0
decisions establish the repository foundation. Simulation and distributed
decisions become accepted only when the relevant implementation phase is approved.

| Record | Status | Scope |
| --- | --- | --- |
| [0001: Modular package](0001-modular-package.md) | Accepted for Phase 0 | Repository shape and dependency introduction |
| [0002: Simulation semantics](0002-simulation-semantics.md) | Accepted and implemented in Phase 1 | Data policy, event order, sizing, accounting |
| [0003: Reproducibility and results](0003-reproducibility-and-results.md) | Phases 1-2 implemented; durable guarantees proposed | Semantic results, identity, execution guarantees |

| [0004: Bounded local execution](0004-bounded-local-execution.md) | Accepted and implemented in Phase 2 | Spawn initialization, streaming export, source identity, benchmark scope |

Update status when a checkpoint is approved. Supersede a consequential decision
with a new ADR and link the records; preserve the reasoning for interview review.

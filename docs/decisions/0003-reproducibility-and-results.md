# ADR 0003: Separate semantic results from execution observations

**Status:** Semantic-result separation, stable identity, and streaming local
backends accepted and implemented through Phase 2. Durable commits for Phases 4-5
remain proposed.

**Date:** 2026-10-10.

## Context

Parallel workers finish in different orders and have different timestamps and
process IDs. Comparing entire execution reports would make identical simulations
appear different. Worker retries can also produce duplicate submissions.

## Decision

Semantic results contain simulation inputs/provenance, fills, order outcomes,
equity, and metrics. Operational metadata contains elapsed time, host, backend,
worker, attempt, and observed timestamps. Compare canonical semantic results
across backends; keep operational observations for diagnostics and benchmarks.

Phase 1 exports sorted canonical JSON with exact money and explicitly rounded
ratios. Release/semantics labels identify the implemented model but do not hash
editable source code. Phase 2 includes an installed-source fingerprint in job/manifest identity and
records Git revision/status plus the complete environment separately as
operational observations. [ADR 0004](0004-bounded-local-execution.md) details this. Only the built-in SMA has fully captured strategy parameters in
Phase 1; custom testing hooks cannot export results without supported provenance.

In Phase 2, derive stable job IDs from canonical config, dataset byte digest and
policy, engine/strategy identity, installed-source fingerprint, and schema version. Deliver outcomes incrementally;
the experiment manager exports per-job artifacts and returns a compact index
sorted by job ID. Do not accumulate all full equity paths in coordinator memory.
Preserve duplicate parameter-grid membership in the manifest while executing
only one copy of an identical job.

In Phase 4, prefer at-least-once execution and transactional idempotent result
commits. One authoritative coordinator owns lifecycle and persistence. A SQLite
journal is a candidate standard-library implementation; finalize that choice at
the Phase 4 gate. Workers submit results through the coordinator. Phase 5 uses
DuckDB to query committed outcomes and experiment metadata.

## Alternatives

- Random IDs alone identify submissions but do not identify repeated simulations.
- Including wall time in semantic results prevents stable equality checks.
- Exactly-once execution cannot be assumed when a worker's acknowledgment is lost.
- Concurrent writes from every worker to analytical storage broaden coordination
  requirements without helping simulation correctness.

## Consequences

Retries may repeat computation, but identical outcomes must not create multiple
committed successes. Conflicting duplicate results indicate nondeterminism or
an identity error and must surface. Attempt fencing prevents stale failures or
expired attempts from overwriting authoritative state.

A single coordinator is a deliberate availability limit. Recovery guarantees
apply to its durable store, not to disk loss or arbitrary network filesystems.
Failure-injection tests in Phase 4 are required before any durability claim.

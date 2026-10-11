# ADR 0004: Bounded local execution with explicit provenance

**Status:** Accepted and implemented in the approved Phase 2.

**Date:** 2026-10-10.

## Context

Independent simulations can run concurrently. Submitting every job at once or
retaining all detailed equity paths would move the memory bottleneck into the
coordinator. Release versions alone also fail to identify changes in an editable
installation. Small workloads may spend more time starting processes than doing
simulation work.

## Decision

- Keep one sequential backend as the semantic reference. Use an explicit spawn
  context for `ProcessPoolExecutor`, with importable top-level worker functions.
- Parse and validate CSV once in the coordinator. Each worker receives the same
  immutable dataset through its initializer once, rather than on every job.
  These are separate process copies. Shared memory is not implemented.
- Bound submitted, unconsumed futures to `max_in_flight`, defaulting to twice
  the requested workers. Deliver completed outcomes to the experiment manager.
- Publish detailed artifacts as outcomes arrive; retain compact records and
  finalize a job-ID-sorted index. Job specs, grid membership, and the index still
  use O(jobs) memory; detailed result buffering uses O(in-flight jobs * bars).
- Hash canonical schema/config/data policy and byte digest plus installed source
  identity for job IDs. Hash sorted relative Python filenames and source bytes
  using length framing. Record Git revision/status and environment separately.
- Refuse an existing experiment directory. Finalize `index.json` last, after
  pool shutdown, detailed export, and operational report publication. Per-job
  exceptions become failures; broken pools, protocol violations, and export
  errors abort the experiment without a finalized index. Partial files can remain.
- Add only optional psutil for measurement. Every benchmark trial uses a fresh
  coordinator process, includes startup/loading/export/shutdown, and samples its
  live process tree at 50 ms. Report observed RSS sums and sampling limitations.
- Verify detailed artifact digests and semantic equality before using timings.
  Publish five rotated timed repetitions after one warm-up per configuration.

## Alternatives and consequences

Fork inheritance could reduce copying on some operating systems, but explicit
spawn makes serialization and initialization visible and works across platforms.
Memory mapping can be evaluated after data-loading or representation measurements.

Waiting in submission order can delay export behind one slow job. Sorting only
the compact index preserves deterministic final output while allowing completion
order delivery. The result publication protocol does not supply recovery or
transactional distributed idempotency; those guarantees require Phase 4.

A whole-package source hash conservatively changes IDs when benchmark or CLI code
changes. This is simple and auditable; a narrower dependency closure would add
complexity. Keep installed source fixed during a run. Wheels can be fingerprinted
without a repository, while Git context is explicitly unavailable there.

Detailed JSON export and IPC are included in all comparisons. They may limit
speedup. Component profiling and changes to representation belong to Phase 6;
Phase 2 makes no optimization claims.

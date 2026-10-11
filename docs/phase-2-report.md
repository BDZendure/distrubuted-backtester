# Phase 2: Local parallel execution

Implementation and local verification are in progress on
`feat/phase-2-local-parallelism`, release `0.3.0`. Final measurements and the
completion checkpoint will be recorded here after verification.

## Implemented

Serializable stable jobs, duplicate-aware parameter sweeps, sequential and spawn
process backends, bounded submissions, one initialized dataset per worker,
structured failures, streamed detailed artifacts, and a sorted compact index.
A fresh-process benchmark runner verifies semantic equality and records timing,
RSS samples, dependency environment, hardware, and source identity.

## Architecture

The Phase 1 engine is unchanged. `jobs.py` and `provenance.py` define identity;
`worker.py` executes a fresh simulation; `backends/` controls execution;
`experiments.py` exports results and publishes the index last. `benchmark.py`,
`measurement.py`, and `workloads.py` keep measurement outside the core engine.
See [ADR 0004](decisions/0004-bounded-local-execution.md).

## Verification

The most recent full run passed 577 tests in 6.08 seconds on Python 3.14.6.
An additional CLI failure test then passed with the eight-test CLI sweep suite.
Ruff and strict mypy passed. Final fresh checks and installation evidence follow
the benchmark run. Local Python 3.12 is unavailable; CI is configured for 3.12
and 3.14 but has not been observed remotely.

## Performance

Pilots selected 32 jobs / 64 bars and 64 jobs / 4,096 bars. Their single timed
samples are workload-selection evidence only. Five rotated repetitions per
configuration are pending. See [benchmark record](benchmarks/phase-2/results.md).

## Known Limitations

Separate dataset copies per worker; O(jobs) job/manifest/index metadata; bounded
O(in-flight jobs * bars) detailed buffering. No Ray, retries, resume, durable
scheduler, database, or crash recovery. RSS is sampled and may double-count shared
pages or miss peaks. OS filesystem cache is uncontrolled. The Phase 1 economic
limitations continue to apply.

## Reproduction Instructions

Use the [README](../README.md) for setup, examples, sweeps, and checks.
Install `.[dev,benchmark]` to run the full verification and measurement surface.

## Engineering Lessons

1. Deterministic output can be independent of completion order.
2. Bounded submission and streamed export control detailed-result retention.
3. Spawn makes serialization, initialization, and data copies explicit.
4. Per-job failure differs from experiment-wide failure.
5. End-to-end measurements include overhead and require verified equal output.

## Next Phase

Phase 3 would add an optional Ray adapter, resource declarations, data placement,
and the same equivalence checks. Its runtime compatibility and measurements must
be reviewed before dependency adoption.

## Approval Required

Stop after the final Phase 2 report. Phase 3 requires explicit user approval.

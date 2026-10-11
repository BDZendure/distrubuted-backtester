# Phase 3 implementation plan

**Authorization:** The user explicitly approved Phase 3 after the Phase 2 report.
Implement this phase only. Stop for approval before Phase 4.

**Goal:** Run the same deterministic experiment through sequential, process, and
Ray backends, with reproducible local measurements and cluster instructions.

**Architecture:** A fixed pool of Ray actors implements `ExecutionBackend`.
Each actor checks the installed implementation and holds one immutable dataset.
The coordinator submits a bounded number of jobs and streams completed results
through the existing artifact collector. Ray remains an optional dependency.

**Stack:** Python >=3.12, Ray 2.59.0, existing pytest/Ruff/mypy, optional psutil.
The published CPython 3.14 macOS arm64 wheel was resolved before adoption.

## Design review

Three approaches were considered:

1. Independent Ray tasks: straightforward, but repeated Python dataset decoding
   and less direct control over worker count on an existing cluster.
2. Fixed Ray actors, selected: explicit worker count, one initialization per
   worker, reuse across jobs, and lifetime CPU/custom resource reservations.
3. A custom scheduler/service: unnecessary in Phase 3; durability is Phase 4.

Use `ray.put` once for the dataset, passed as an actor constructor argument.
Python dataclasses still deserialize into each actor heap; this is not zero copy.
Use SPREAD scheduling as a preference, not a guarantee of equal distribution.
Actor restarts and task retries are explicitly zero. Ordinary job errors remain
structured outcomes; infrastructure errors abort without a final result index.

The default creates an owned local cluster using the explicit address `local`.
An explicit GCS address or `auto` attaches to an existing cluster. Refuse an
already initialized driver connection rather than taking over its ownership.
Always kill only experiment actors and disconnect this driver; Ray shutdown
stops an owned local cluster but does not stop an externally started cluster.
Existing clusters require the same Python, Ray, installed package, and source
fingerprint on every node. No implicit code or dependency deployment.

Validate finite positive per-worker CPU/custom resource requests. Verify live
node capacity before creating actors, and bound actor readiness waits. A busy
cluster can still delay jobs; job deadlines remain Phase 4. Reject Ray Client
addresses because this phase uses a driver connected directly to a GCS cluster.

## Task 1: Preserve the baseline and implement the Ray backend

Files: `backends/ray.py`, `tests/test_ray_backend.py`, `pyproject.toml`.

- [x] Inspect repository and Phase 2 contracts; create Phase 3 feature branch.
- [x] Resolve/install the pinned optional Ray wheel on the current interpreter.
- [x] Investigate the baseline recursive RSS test failure without weakening it.
- [x] Write failing backend validation, optional import, actor, and integration tests.
- [x] Add typed boundary protocols for Ray's dynamic actor API.
- [x] Implement fixed actor initialization, identity check, bounded submission,
      outcome ID validation, resource configuration, operational diagnostics,
      and cleanup on success, early iterator close, and failure.
- [x] Verify real remote jobs and equivalence, including structured failures,
      worker reuse, source mismatch, resource errors, and external ownership.

Interface: `RayBackend(workers, *, max_in_flight=None, address=None,
cpus_per_worker=1.0, resources=None, object_store_memory=None,
startup_timeout_seconds=60.0)`; existing `iter_outcomes` signature unchanged.
Expose `diagnostics` as JSON-safe operational metadata, never semantic identity.

## Task 2: CLI, experiment metadata, and local distributed validation

Files: `cli.py`, `experiments.py`, CLI tests, CI, cluster instructions.

- [x] Write failing CLI tests for Ray flags and invalid option combinations.
- [x] Add `--backend ray`, `--ray-address`, `--cpus-per-worker`, and repeatable
      `--worker-resource NAME=QUANTITY`. Local object-store budget is configurable.
- [x] Record backend diagnostics in `execution.json` outside semantic artifacts.
- [x] Run all three user-facing sweeps and compare verified artifact digests.
- [x] Validate an externally started cluster, resource-constrained scheduling,
      and two local Ray nodes if this platform permits it. Record actual node IDs.
- [x] Add optional Ray integration CI and preserve dependency-free wheel checks.

## Task 3: Reproducible Phase 3 benchmark and documentation

Files: dedicated local Ray benchmark module/tests, benchmark results, ADR,
README, architecture/roadmap/dependencies/testing/benchmarking docs, beginner and
technical explanations, Phase 3 report.

- [ ] Write failing benchmark orchestration/measurement tests.
- [ ] Compare sequential, process, and owned local Ray on identical code,
      interpreter, data, configurations, and artifact verification.
- [ ] Include Ray import/startup, actor readiness, dataset placement, export,
      shutdown, and process exit in cold command timings. Record component times.
- [ ] Warm up once per configuration, rotate order, retain repeated raw samples.
- [ ] Inventory all owned local Ray services/actors, including non-driver
      descendants. Memory observations must have explicit process scope and
      missing/denied coverage. Shared object-store pages must not be added once
      per worker: report private memory separately from logical store usage.
      Do not claim external cluster memory coverage from a local driver sample.
- [ ] Capture environment, dependency versions, source identity, and clean revision.
- [ ] Measure small and substantial workloads; retain unfavorable results.
- [ ] Explain timing, object-store serialization, locality, scheduling, and
      limitations in both requested explanation documents.
- [ ] Run full pytest, Ruff, mypy, pip check, build, fresh wheel and archive checks.
- [ ] Independently review implementation; fix findings with regression tests.
- [ ] Commit focused milestones and produce the required Phase 3 report.

## Checkpoint

No multiple-machine scaling claim without a real multiple-machine run. Local
multi-node validation is a functional test sharing one physical machine.
No durable scheduler, retry policy, journal, recovery, Parquet, Docker environment,
or optimization in this phase. Completion is not approval for Phase 4.

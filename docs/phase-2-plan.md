# Phase 2 implementation plan

**Goal:** Run reproducible parameter sweeps sequentially or with local processes,
report failures, and measure complete execution including detailed result export.

**Architecture:** Serializable immutable jobs call the existing simulation.
Backends stream outcomes; the experiment manager publishes per-job artifacts and
a compact stable index. Benchmark sampling is an optional adapter around a fresh
experiment process. The user explicitly approved Phase 2 after the Phase 1 report.

**Tech stack:** Python 3.12+ standard library; optional pinned psutil 7.2.2 for
process-tree RSS sampling. Existing pytest/Ruff/mypy/build tools.

## Constraints and design review

- Phase 2 only; stop before Ray/Phase 3. Preserve daily SMA and accounting rules.
- Use the clean existing checkout on `feat/phase-2-local-parallelism`.
- No remote push, history rewriting, retry scheduler, database, or shared memory.
- Explicit spawn `ProcessPoolExecutor`, fresh pool per experiment, positive worker
  count, and a configurable bounded number of submitted/unconsumed jobs.
- Read CSV once per experiment. Initialize each worker with one immutable dataset
  copy; pass small job specifications thereafter. Measure this duplication.
- Full semantic result equality across backends. Operational timings and worker
  identifiers must not enter semantic manifests, job artifacts, or index.
- Fingerprint actual installed package source bytes in addition to release/semantic
  identity. Record exact Git revision/status and dependency snapshot operationally.
- Hash canonical full configuration, data metadata, source/engine identity, and
  schema to identify jobs. Preserve requested duplicate-grid membership while
  executing each unique configuration once. Reject invalid grid pairs explicitly.
- Export each result immediately and retain only compact records. Sorted job IDs
  determine manifest/index order; arrival order never determines semantic output.
- Structured ordinary job failures; a broken pool or export/protocol failure aborts
  the experiment. No success index may be published for incomplete collection.
- Fresh output directory required. Partial per-job artifacts can remain after
  failure, without a finalized index; recovery is deferred to Phase 4.
- Tests precede implementation. Real spawn processes, CLI subprocesses, and files
  establish behavior. Label narrowly injected failures in tests.

Alternatives considered: threads are unsuitable for CPU-bound Python under the
usual GIL; fork depends on platform state and does not provide spawn portability;
spawn with one dataset initialization per worker provides a clear reference.
Shared memory and batching require future measurement and ownership semantics.
An in-memory list of all results would scale as jobs times bars; streamed artifacts
plus a compact index bound retained detailed results by in-flight work.

## Interfaces and file ownership

| File | Responsibility |
| --- | --- |
| `provenance.py` | Installed-source fingerprint and operational environment/revision snapshot |
| `jobs.py` | Immutable identity, JSON round trips, unique parameter-grid expansion |
| `worker.py` | Execute one job against matching initialized data/implementation; structured failure |
| `backends/base.py` | `ExecutionBackend` protocol and outcome contract |
| `backends/sequential.py` | Streaming reference execution |
| `backends/process.py` | Spawn initialization and bounded future collection |
| `experiments.py` | Manifest, per-job export, complete index, operational report |
| `benchmark.py`, `measurement.py`, `workloads.py` | Optional fresh-process driver, RSS observation, and deterministic synthetic workloads |
| `cli.py` | Existing backtest plus sweep command; the benchmark module has its own CLI |
| `requirements/benchmark.lock` | Exact dependency pins for measurement environment |
| `tests/test_*.py` | Jobs/provenance/worker/backends/experiments/CLI/measurement evidence |
| `docs/phase-2-report.md`, `docs/benchmarks/` | Verification, raw measured evidence, analysis, next gate |

Core interfaces:

```text
ImplementationIdentity(engine_id: str, source_sha256: str)
JobSpec(job_id: str, config: BacktestConfig, dataset: DatasetMetadata,
        implementation: ImplementationIdentity)
create_job(config, metadata, implementation) -> JobSpec
job.to_dict() -> dict[str, object]
JobSpec.from_dict(record: object) -> JobSpec
expand_grid(fast: Sequence[int], slow: Sequence[int], initial_cash: int,
            fee_bps: int, metadata, implementation) -> tuple[tuple[JobSpec, ...], tuple[str, ...]]
JobFailure(error_type: str, message: str)
JobOutcome(job_id: str, result: BacktestResult | None, failure: JobFailure | None)
execute_job(job: JobSpec, dataset: MarketDataset,
            implementation: ImplementationIdentity) -> JobOutcome
ExecutionBackend.iter_outcomes(jobs: Sequence[JobSpec], dataset: MarketDataset,
                              implementation: ImplementationIdentity) -> Iterator[JobOutcome]
SequentialBackend()
ProcessBackend(workers: int, max_in_flight: int | None = None)
run_experiment(data: Path, *, symbol: str, fast: Sequence[int], slow: Sequence[int],
               initial_cash: int, fee_bps: int, output: Path,
               backend: ExecutionBackend) -> ExperimentSummary
```

## Task 1: Serializable identity and jobs

- [ ] Write tests for canonical IDs, source fingerprints, JSON/pickle round trips,
      unknown/malformed fields, bool numeric rejection, changed config/data/code,
      duplicate membership, and invalid/empty grids.
- [ ] Observe missing implementation failures with
      `.venv/bin/python -m pytest tests/test_jobs.py tests/test_provenance.py`.
- [ ] Implement frozen validated identities/job specifications and strict decoding.
      Source fingerprint hashes sorted package-relative Python filenames and bytes,
      independent of installation path; fail if source cannot be read.
- [ ] Verify focused pytest, Ruff, strict mypy and commit the identity milestone.

Representative contract:

```python
job = create_job(BacktestConfig(), metadata, implementation)
assert JobSpec.from_dict(json.loads(json.dumps(job.to_dict()))) == job
assert pickle.loads(pickle.dumps(job)) == job
```

## Task 2: Worker and execution backends

- [ ] Write worker/reference/process tests, including ordinary dataset/identity
      mismatch failures mixed with successes, spawn reuse, distinct worker counts,
      immutable data, bounded outstanding jobs, and missing/duplicate outcomes.
- [ ] Observe red with `.venv/bin/python -m pytest tests/test_worker.py tests/test_backends.py`.
- [ ] Implement a worker without IO/persistence and a sequential streaming adapter.
      Catch ordinary `Exception` as a failure; do not swallow `BaseException`.
- [ ] Implement top-level spawn initializer and worker entry. Validate installed
      identity once per initialized process. Submit at most `max_in_flight` jobs,
      yield completed outcomes, and refill only after the consumer resumes.
      Close/cancel pending futures and shut down the pool when iteration stops.
- [ ] Compare complete serialized semantic outcomes with the sequential reference
      using real 1/2/4/8-worker pools where supported, and commit after verification.

Representative equivalence:

```python
reference = {
    outcome.job_id: outcome
    for outcome in SequentialBackend().iter_outcomes(jobs, dataset, identity)
}
parallel = {
    outcome.job_id: outcome
    for outcome in ProcessBackend(2).iter_outcomes(jobs, dataset, identity)
}
assert parallel == reference
```

## Task 3: Experiment orchestration and CLI

- [ ] Write tests before implementation for sorted canonical manifest/index,
      per-job digests and relative references, retained duplicate membership,
      result/failure export, incomplete/invalid backend output, and output protection.
- [ ] Implement source load, identity/grid creation, fresh output directory,
      manifest, incremental artifacts, and final compact index after pool shutdown.
      Verify each result matches its job metadata/config/engine identity and every
      expected job has exactly one outcome. Export failure aborts the experiment.
- [ ] Keep execution information in `execution.json`, outside semantic comparison.
      Report counts/timing, source path, Git status/revision, and environment.
- [ ] Add `sweep` CLI options for data/symbol/raw basis, fast/slow lists, cash/fees,
      sequential/process backend, workers, in-flight limit, and output directory.
      Partial job failure exits 1 with structured artifacts; input/fatal errors exit
      2 without a finalized success index. Preserve the backtest command.
- [ ] Run actual CLI subprocesses, full reference equivalence, lint/types, and commit.

Example:

```bash
.venv/bin/python -m distributed_backtester sweep --data examples/sma.csv \
  --symbol SYNTH --price-basis raw --fast 1 2 --slow 3 4 --fee-bps 10 \
  --backend process --workers 2 --output-dir artifacts/sweep-process
```

## Task 4: Reproducible measurements

- [ ] Add optional psutil extra and a complete pinned benchmark environment; record
      dependency metadata and version compatibility. CI installs the measurement
      extra for measurement correctness tests, without enforcing timing thresholds.
- [ ] Write tests for deterministic synthetic CSV/config grids, measurements,
      process-tree sample sums, summary medians/ranges/speedup/efficiency, failed
      samples, and digest equality verification before comparing timings.
- [ ] Implement benchmark runner spawning a fresh CLI experiment for every trial.
      Start timing before process creation; include source loading, startup,
      job/result serialization, artifacts/index/report, and shutdown. Sample root
      plus descendants' simultaneous RSS at 50 ms; record missed/denied samples,
      intervals, coordinator RSS, and raw timestamped observations. This is an RSS
      sum, not unique physical memory. Keep sampling overhead consistent.
- [ ] Run one declared unmeasured warm-up per configuration, then five repetitions
      in rotating configuration order, for small and substantial distinct-job grids.
      Pilot sizes before fixing the final workloads. Measure sequential and 2/4/8
      workers on supported hardware, preserving identical semantic artifact hashes.
- [ ] Measure increasing job-count coordinator memory with fixed bar count;
      document compact index/job metadata growth and bounded detailed-result memory.
- [ ] Commit source before final measurements, record revision and clean/dirty state,
      retain all raw observations and failures, and summarize only observed results.

Benchmark runs label process-cold / OS cache uncontrolled. No cache flushing or
host changes. Five samples support median/range summaries, not precise percentiles.

## Task 5: Review, documentation, release, checkpoint

- [ ] Independent correctness review of jobs, bounded process collection, output
      validation/publication, sampling boundaries, and benchmark reproducibility.
      Reproduce/fix substantive findings and re-review within this phase.
- [ ] Update README, architecture, roadmap, ADR 0003, dependencies/testing/
      benchmarking, beginner/technical guides, and complete Phase 2 report.
- [ ] Run pytest, Ruff lint/format, strict mypy, pip check, isolated distributions,
      fresh-wheel sweep CLI outside checkout, semantic equality, document links,
      source archive completeness, and Git whitespace checks.
- [ ] Commit focused milestones and report actual test/measurement results.
- [ ] Stop before Phase 3 and ask explicit approval. No remote push.

Final commands:

```bash
.venv/bin/python -m pytest
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
.venv/bin/python -m mypy
.venv/bin/python -m pip check
.venv/bin/python -m build
```

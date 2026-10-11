# Phase 6 performance engineering implementation plan

> **For agentic workers:** Use subagent-driven-development or executing-plans
> task by task. This document is the durable progress ledger.

**Authorization:** The user approved Phase 6 after the verified Phase 5 checkpoint.
Work on `feat/phase-6-performance`; stop before Phase 7.

**Goal:** Identify measured CPU/allocation costs, evaluate focused changes without
altering economics, and publish reproducible before/after and scaling evidence.

**Architecture:** Keep the existing domain records, execution backends, scheduler,
and detailed exports. Add standard-library profiling and a narrow cross-revision
comparison over existing verified benchmark artifacts. Preserve each producer's
implementation/job identity while comparing identical input definitions and full
simulation results. Use archived wheel installations with the same interpreter
and dependencies for final comparisons.

**Stack:** Python >=3.12; cProfile, pstats, tracemalloc; existing pytest/Ruff/mypy,
Ray and psutil. No new runtime dependency or native extension is planned.

## Global constraints

- Phase 6 only. No Docker, operational configuration layer, frontend, or Phase 7 work.
- Preserve chronological close decisions, next supplied open execution, rounded
  transaction fees, whole shares, raw USD prices, validation, and deterministic outputs.
- Never omit detailed outputs only for one variant/backend to improve timings.
- Keep current CSV/Parquet inputs, worker representation, and durable commit protocol.
- Freeze all package source during real backend tests, profiles, and timed runs.
- CPU/instrumented durations and Python allocation traces are not unprofiled throughput.
- Report native/shared/private/cluster memory only within actually supported scope.
- Read current bytes and verify source/workload/artifact identities before comparisons.
- Document successful, ineffective, and rejected optimization experiments honestly.
- No new dependencies unless actual profiling justifies them. No em dash or attribution.
- Make focused local commits; never discard user changes or push remotely.

## Context and design

- [x] Inspect Phase 5 status, source, contracts, benchmark methodology, and roadmap.
- [x] Reuse the accepted in-place feature branch workflow; checkout began clean.
- [x] Baseline full suite: 1,213 passed in 130.39 seconds, package SHA
  `dfef1b1edee02fde072a4173750d25b8a39f9bc279772c69a184d98c9ed7c53c`.
- [x] Present alternatives: bounded Python changes, representation changes, native
  acceleration. Prefer the smallest profile-supported Python change first.
- [x] Initial read-only cProfile of the existing 65,536-bar/four-job complete CLI:
  SMA on_close cumulative 1.655 s, history indexing 1.090 s, metrics 0.691 s;
  simulation 3.370 s. These are instrumented observations, not speedup evidence.
- [x] Independent reuse/design audit approved this approach. Worker counts
  apply to both process and Ray configurations; record wheel build provenance
  separately because installed wheels have no automatic Git history.

## Task 1: Reproducible profiling and timing tool

Files: create `src/distributed_backtester/profiling.py`, `tests/test_profiling.py`.
Main owns `pyproject.toml` version 0.7.0 and editable reinstall separately.

Public API:

```python
profile_workload(output: Path, *, bars: int, jobs: int,
                 kind: Literal["cpu", "memory", "timing"],
                 scope: Literal["simulation", "experiment"] = "simulation",
                 repetitions: int = 1, seed: int = 1729) -> dict[str, Any]
```

CLI: `python -m distributed_backtester.profiling run --bars N --jobs N
--kind cpu|memory|timing --scope simulation|experiment --repetitions N
--output-dir DIR`. CPU/memory require one repetition; timing accepts positive N.
Use the existing synthetic generator and `job_configs`, fee 10 bps, BENCH/raw.
Generate before measurement. Simulation loads CSV before measurement, runs fresh
`run_backtest` calls and retains the returned results. Experiment measures loading,
job construction, sequential simulation, detailed export and finalization through
existing APIs. Independent result hashing follows each scoped measurement.

- [x] Write failing API/CLI tests before production code. Cover invalid exact
  integers/kind/scope, fresh/protected output, CPU records/raw stats, tracemalloc
  retained/peak distinctions, existing tracer rejection, repeats, whole-result
  equality, source freeze checks, failure persistence and cleanup.
- [x] Run the tests and record failures caused by the missing feature.
- [x] Implement minimum CLI/API with lazy instrumentation. Capture environment,
  implementation, workload/config/data hashes, scoped timings and own-process
  lifetime RSS through existing `peak_rss`. Timing must remain uninstrumented.
- [x] CPU records retain filename/line/function, primitive/total calls, self and
  cumulative seconds plus a pstats file. Allocation records retain current/peak
  traced bytes and before/after live allocation differences, never total allocation
  frequency or native/private memory claims. Stop only the tracer this run started.
- [x] For simulation, fingerprint complete `result_to_dict` records in canonical
  config order. For experiments, verify complete snapshots before hashing all
  successful result records. All repetitions must agree; job IDs may differ across
  implementations but result semantics cannot. Persist terminal errors before
  re-raising, with no successful fingerprint claim on failure.
- [x] Run scoped tests, Ruff, format, mypy; obtain independent task review and commit.

Task 1 evidence: missing-feature and boundary red runs preceded implementation; independent review reproduced active caller instrumentation and combined-failure cleanup gaps. New regressions observed 32 failures before fixes. Main verification: 112 scoped tests passed in 0.39 s; scoped Ruff/format/mypy passed. Final real 32-bar/four-job smoke agreed across all six kind/scope combinations. Failure publication is attempted; an unwritable destination cannot guarantee a report.

## Task 2: Cross-revision comparison over existing benchmark reports

Files: create `src/distributed_backtester/performance.py`,
`tests/test_performance.py`; add documentation after actual runs.

Public API:

```python
compare_benchmarks(baseline: Path, candidate: Path, output: Path) -> dict[str, Any]
```

CLI: `python -m distributed_backtester.performance compare --baseline DIR
--candidate DIR --output FILE.json`. Consume retained `ray_benchmark` report roots,
including sequential, process-2/4/8 and Ray-2/4/8 configurations. Reuse the existing
owned local harness without duplicating scheduling, startup or memory observation.

- [x] Write failing tests for complete output equality across distinct code/job IDs,
  changed fills/equity/config/data, tampered/missing artifacts, incorrect identities,
  failed/incomplete runs, mismatched environments/configurations/repetitions,
  invalid timings and protected output. Test known ratio calculations.
- [x] Run failures before implementation.
- [x] Read reports and recompute raw-sample medians/ranges, jobs/sec, speedup and
  parallel efficiency. Require identical dataset and config definitions, method,
  declared resource/backend settings and relevant interpreter/dependency/hardware
  environment. Preserve the two implementation identities separately.
- [x] Verify every retained trial using canonical manifest/index/full snapshots,
  exact producer identities and declared generated configs. Compare all result
  fields, sorting by canonical configuration, excluding only validated engine_id
  when release identity differs. Keep dataset provenance and execution/accounting
  policy. Reject every failed outcome and semantic difference.
- [x] Publish actual before/after ratios, variability, sampling limitations and
  observed memory fields without manufacturing unavailable values. Refuse existing
  or source-alias output and publish atomically only after all checks.
- [x] Run scoped tests/static checks; obtain independent task review and commit.

Task 2 evidence: initial 32-test missing-feature red run; expanded full-trace and metadata regressions observed red before fixes. Final scoped run: 44 passed in 9.95 s. Scoped Ruff/mypy passed; independent review identified missing child environment verification, then confirmed the four new rejection cases and valid release-normalized comparisons after the fix. Formal process/Ray artifact verification remains in Task 3.

## Task 3: Measured optimization experiments

Main owns narrow changes and expanded regressions in `models.py`/`metrics.py`,
`tests/test_models.py`/`tests/test_metrics.py`, and independent reference checks.
No change is selected until the accepted profiles establish its cost.

- [x] Commit/build a 0.7.0 measurement baseline with unchanged production algorithm;
  archive/install the wheel to a private site, verifying exact package bytes/SHA.
- [x] Capture CPU and Python allocation profiles for simulation and complete
  sequential experiments; preserve raw JSON and pstats evidence.
- [x] Capture uninstrumented repeated timing baselines for each candidate scope.
- [x] Candidate A: use bounded native list/tuple slicing in CloseHistory only if
  profiles support it. Preserve every slice, future bounds and arbitrary Sequence
  fallback. Write deterministic operation-count and exhaustive semantic tests;
  observe the old implementation fail the operation-count expectation first.
  Evaluate a generator over bounded backing indices before native slicing as a
  plausible allocation-saving alternative; measure both and retain the winner.
- [x] Measure A, verify complete results and review before acceptance. Reject or
  revert it if measurements do not support a useful improvement.
- [x] Candidate B: defer Fraction construction in drawdown calculation using exact
  integer cross products only if supported. Preserve validation and rational output;
  write a Fraction reference, huge values, ties, zero equity and late-invalid cases,
  plus a failing deterministic allocation-count regression before implementation.
- [x] Measure B separately against A, verify complete results and review.
- [x] Evaluate at least one profile-supported alternative and retain measured
  evidence whether accepted or rejected. Do not introduce C++ without justification.
- [x] Freeze the accepted candidate, archive/install its wheel and verify source.
- [x] Run small and substantial fresh-process comparisons with one warmup and three
  timed repetitions/configuration. Use sequential, process-2/4/8 and Ray-2/4/8 where
  available, same workload/seed/resources and complete output. Retain detailed
  artifacts until cross-revision verification; source identity changes are explicit.
- [x] Compare baseline/candidate economic results and raw times using Task 2.
- [x] Run complete correctness/fault/backend suite, example CLI, lint/format/mypy,
  dependency and distribution checks, isolated installation and documentation links.

## Task 4: Evidence and checkpoint

Files: `docs/benchmarks/phase-6/`, `requirements/performance-benchmark.lock`,
`docs/performance.md`, `docs/phase-6-report.md`, README, architecture, roadmap,
beginner/technical guides, testing/benchmarking and decisions index/ADR 0008.

- [x] Record hardware/software, workload sizes/seed, full source/config digests,
  raw repetitions, CPU and retained/peak allocation evidence, runtime/jobs per
  second/speedup/efficiency, and memory availability with exact scope.
- [x] Explain successful and unsuccessful experiments without extrapolated claims.
- [x] Synchronize both beginner and technical explanations and reproduction commands.
- [x] Obtain final independent implementation/evidence review, fix actual findings,
  rebuild/verify archives and make local milestone commits.
- [x] Publish the nine-section Phase 6 completion report and stop.
- [ ] Obtain explicit approval before Phase 7.

Optimization evidence before formal backend comparisons: all 24 CPU/memory/timing reports and 56 repetitions agree on complete results and workload hashes. Five-repeat simulation medians: baseline 0.975629 s, bounded generator 0.871920 s, native history 0.805127 s, combined exact drawdown 0.669989 s. Complete experiment medians: 1.903224 / 1.849313 / 1.766037 / 1.652131 s respectively. Both narrow changes received independent acceptance; traced memory peaks did not materially improve. Full optimized native suite: 1,397 passed in 139.17 s.

Final controlled measurements: 112 full trial snapshots verified across both revisions
and all backends; 8,960 executions including warmups/repetitions, with 128 or 32
unique jobs per experiment. Sequential small/substantial median reductions 18.30% /
30.20%; parallel changes were small and mixed. All primary private USS
peaks were unavailable. The first small pair was excluded after exact environment
verification caught Ray's bundled Colorama metadata exposure; identical vendor
path pre-exposure passed preflight without source/dependency changes. Final native
repeat: 1,397 passed in 139.41 s; Ruff/format/mypy/pip check and fresh bare-wheel
core plus six profile modes passed. Raw reports, pstats, identities and the rejected
alternative were kept.

Final independent integration/evidence review accepted Phase 6 without blocking
findings. The minor ADR index table rendering issue was corrected. Final archives
were checked against current package/document bytes, and local links resolved.
The nine-section report is [published](phase-6-report.md); Phase 7 requires a
separate explicit approval.

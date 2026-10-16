# Phase 2: Parallel backtesting on one machine

**Status:** Complete, verified locally; Phase 3 awaits explicit approval.
**Release:** `0.3.0`. **Branch:** `feat/phase-2-local-parallelism`.

## Implemented

- Immutable JSON/pickle-compatible job specifications and content-derived IDs.
- Full config, data-policy/digest, and installed-source identity in each job.
- Parameter sweeps with strict pair validation, duplicate collapse, and preserved
  requested grid membership.
- Streaming sequential reference and spawn-based process backends.
- CSV loaded once per experiment; immutable data initialized once per worker.
- Bounded submitted/unconsumed futures, defaulting to twice the workers.
- Structured per-job failures and explicit experiment-wide failure behavior.
- Incremental detailed artifacts, content digests, and a job-ID-sorted compact index.
- `sweep` CLI, operational execution reports, deterministic workload generation,
  and a fresh-process timing/RSS benchmark CLI.
- Updated CI installation/wheel sweep checks, dependency snapshot, ADRs, README,
  architecture, roadmap, testing/benchmark methodology, and both explanation files.

Important files: `jobs.py`, `provenance.py`, `worker.py`, `backends/`,
`experiments.py`, `workloads.py`, `benchmark.py`, `measurement.py`, and their tests.
`cli.py` and `results.py` extend IO and reuse complete-file publication. The
Phase 1 simulation, strategy, accounting, and metrics semantics remain unchanged.

## Architecture

`JobSpec` hashes canonical schema/config, dataset metadata and source digest, and
`ImplementationIdentity`. The latter combines release/engine semantics with
length-framed hashes of sorted package-relative Python filenames and source bytes.
Paths, clocks, worker count, and completion order do not enter job identity.

The sequential backend is the semantic reference. The process backend uses an
explicit spawn context and top-level worker functions. Each worker checks source
identity and receives the immutable dataset through its initializer once, then
handles small specs. Workers invoke a fresh built-in SMA simulation per job.
Each worker still owns a separate data copy; no shared memory is implemented.

The collector writes `manifest.json`, publishes each `results/<job_id>.json` as
it arrives, then writes `execution.json` and publishes `index.json` last. The index
contains compact summaries, references, and digests. Semantic files are independent
of backend/completion order for identical jobs and membership. Operational metadata
holds timings, backend/workers, source path, installed versions, and Git context.

An ordinary job exception becomes a failure artifact. Broken pools, initializer
failure, unknown/duplicate/missing IDs, mismatched result config/provenance/schema,
and export failure abort without a finalized index. Partial files can remain.
Existing experiment directories are refused. Exit codes: 0 for all-success,
1 for completed job failures, 2 for input/fatal errors.

Detailed retention is bounded by in-flight work, rather than total jobs.
Definitions, membership, manifest serialization, and compact index remain O(jobs).
Simulation/export uses O(bars) memory per retained result. Early iterator closure
cancels pending work where possible and waits for running tasks during shutdown.
See [ADR 0004](decisions/0004-bounded-local-execution.md).

## Verification

Executed on macOS arm64 with Python 3.14.6. All final commands exit 0 unless an
expected failure is explicitly stated. The final full suite passes **578 tests**.

| Exact command | Actual result |
| --- | --- |
| `.venv/bin/python -m pytest --tb=short` | 578 passed in 5.83 s; no failures/skips |
| `.venv/bin/ruff check .` | All checks passed |
| `.venv/bin/ruff format --check .` | 62 files already formatted |
| `.venv/bin/mypy` | No issues in 42 source/test files |
| `.venv/bin/python -m pip check` | No broken requirements |
| `.venv/bin/python -m build` | Isolated source distribution and wheel built, setuptools 84.0.0 |
| `.venv/bin/python -m pip install --no-cache-dir --no-index --no-deps --requirement requirements/benchmark.lock` | Every pinned version already satisfied |
| `git diff --check` | No whitespace errors |

New tests establish JSON/pickle round trips, strict job decoding, changed identity,
duplicate grids, invalid pairs, real spawn equivalence at 1/2/4/8 workers, mixed
success/failure, output protection, reversed completion order, publication/protocol
failures, broken initialization, and early closure. An instrumented real pool
independently counts paused-consumer submissions, records initialization once per
worker and repeated worker use, and checks eventual release of future references.

Real subprocess sampler tests cover recursive descendant RSS, output capture,
nonzero status, and prompt completion. Narrow injected boundaries cover denied
observations, disappeared processes, hardware errors, missing optional psutil,
interruption cleanup, and CLI job failures. Those injections are labeled in tests;
they do not substitute for real spawn/measurement execution.

Independent review found and reproduced unsupported result schemas being accepted
and trial/report identity divergence. Both were fixed with regression tests.
Final review approved with no blocking findings, while retaining RSS and component
measurement limitations. Intermediate failures were investigated and fixed; no
production assertions were weakened.

### CLI demonstrations

Executed from the repository, each with a new destination:

```bash
.venv/bin/distributed-backtester backtest --data examples/sma.csv \
  --symbol SYNTH --price-basis raw --fast 2 --slow 3 --initial-cash 1000 \
  --fee-bps 10 --output artifacts/phase2-example.json
.venv/bin/distributed-backtester sweep --data examples/sma.csv \
  --symbol SYNTH --price-basis raw --fast 1 2 --slow 3 4 --initial-cash 1000 \
  --fee-bps 10 --backend sequential --output-dir artifacts/phase2-demo-sequential
.venv/bin/distributed-backtester sweep --data examples/sma.csv \
  --symbol SYNTH --price-basis raw --fast 1 2 --slow 3 4 --initial-cash 1000 \
  --fee-bps 10 --backend process --workers 4 --output-dir artifacts/phase2-demo-process
```

Both sweeps submitted/succeeded in all four unique jobs, with zero failures.
A Python assertion script called `semantic_digest` on both directories and checked
all artifact digests and equality. Experiment ID:
`183d87956abfdd8794b01ce8a9d0d29a560f5e1a1df34e1d2d146aae79962c25`.
Verified semantic digest:
`ca386c30546fb94d165243fd0b8b27a6e24eff2589a119c683dabf0b60c11402`.

The example retains Phase 1's reference: buy 83 shares at $12, fee $0.996;
sell at $9, fee $0.747; final equity $749.257, return -25.0743%, drawdown 25.0743%,
and total fees $1.743. The ledger and equity explanation is in the
[Phase 1 report](phase-1-report.md).

### Fresh wheel outside the checkout

Executed `python3 -m venv .venv-wheel-phase2`, followed by:

```bash
.venv-wheel-phase2/bin/python -m pip install --no-cache-dir --no-index \
  --no-deps --force-reinstall dist/distributed_backtester-0.3.0-py3-none-any.whl
```

Using absolute paths to the wheel's Python/CLI and committed CSV, with working
directory `/private/tmp`, executed isolated import/identity checks, `backtest`
with 10 bps to `/private/tmp/phase2-wheel-example.json`, and sequential/process-2
four-job sweeps to `/private/tmp/phase2-wheel-sequential` and
`/private/tmp/phase2-wheel-process`. These are the same sweep options above,
with process workers set to 2. All exited 0; the wheel environment had no psutil.

An isolated Python script asserted final example equity, equal verified sweep
outputs across both wheel backends and the editable installation, equal installed
source fingerprint, and unavailable Git revision for the wheel. Thus core/process
execution works without benchmark dependencies and without checkout-path imports.
A final archive inspection confirms source/tests/examples/docs and the benchmark
version snapshot are included.

Local Python 3.12 is unavailable. CI is configured for Linux/Python 3.12 and 3.14,
including benchmark dependencies and fresh-wheel sweep checks; no remote CI run
was observed and no cross-machine execution was attempted.

## Performance

Measured on an Apple M3 Pro, 11 physical/logical cores, 18 GiB RAM, macOS 26.6.1,
CPython 3.14.6. All primary reports record clean source commit
`dc01e0611d0b268c260139ba5068b4f12f97471d`. Installed-source fingerprint:
`5ac64d80388b3ce3eadb36eaf1a7b3306408e43a269040c2a713d62260571843`.

Five rotated timed repetitions follow one warm-up per configuration. Each trial
starts a fresh coordinator/pool; timing includes imports, loading, startup,
simulation/serialization, detailed export/fsync, finalization, and shutdown.
All semantic output digests were verified before interpreting results.

| Backend | Small median seconds | Substantial median seconds | Substantial jobs/s | Substantial speedup | Substantial tree RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Sequential | 0.159 | 2.069 | 30.9 | 1.000x | 36.72 |
| Process 2 | 0.268 | 1.321 | 48.4 | 1.566x | 125.02 |
| Process 4 | 0.268 | 1.453 | 44.0 | 1.423x | 199.83 |
| Process 8 | 0.300 | 1.685 | 38.0 | 1.228x | 346.12 |

Small: 32 jobs x 64 bars. Substantial: 64 jobs x 4,096 bars. Two workers are
fastest for this substantial workload; sequential is fastest for small tasks.
The measured exporter and startup/first-outcome intervals help explain limited
scaling, but do not isolate worker CPU bottlenecks. No optimization is claimed.

Fixed-64-bar memory checks also complete 2,048 distinct jobs per trial. Median
coordinator sampled peak RSS increases from 30.80 to 37.98 MiB sequentially and
31.05 to 38.52 MiB with two workers, from 32 to 2,048 jobs. Metadata still grows
with jobs; memory is not constant. Full [tables, ranges, efficiency, raw samples,
component timings, and exact benchmark commands](benchmarks/phase-2/results.md)
provide the measurement evidence. No values in these tables are estimates.

## Known Limitations

- Local single-machine execution only; no Ray, scheduler, retries, timeouts,
  resume, transactional result store, or crash recovery.
- Worker data copies, detailed IPC/export, per-job O(bars) work/storage, and
  O(jobs) manifest/index memory can limit scale. Streaming is not constant memory.
- RSS sampling at 50 ms is approximate, can miss brief processes/peaks, and can
  count shared pages repeatedly. Denials invalidate published peak values.
- OS cache, thermals, and background host activity are uncontrolled. Five samples
  support observed medians/ranges, not statistical significance or universal speedup.
- Component intervals are coordinator observations, not CPU profiles. CPU
  utilization, isolated worker service time, and latency percentiles are unmeasured.
- Installed source must stay fixed during a run; restart Python after source edits.
  Whole-package hashes conservatively change all job IDs for auxiliary code edits.
- JSON publication relies on local hard-link/replace semantics; there is no
  directory fsync, recovery journal, or distributed exactly-once guarantee.
- The Phase 1 single-asset/raw-USD/daily/whole-share/long-only model still lacks
  splits/dividends, exchange calendar, intraday liquidity/impact, and real universe
  completeness. Synthetic tests/benchmarks do not establish investment performance.

## Reproduction Instructions

Use Terminal or VS Code's terminal from the repository directory:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --editable ".[dev,benchmark]"
python -m pytest --tb=short
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m pip check
python -m build
```

Run the demonstrations above with new output paths; existing destinations are
protected. Inspect `index.json` using `python -m json.tool`. The committed
`examples/sma.csv` is ready to use. A separate benchmark module supplies generated
CSV workloads; reproduce the four exact measurement commands in the
[benchmark record](benchmarks/phase-2/results.md), using the full version snapshot
for the recorded platform. Fresh wheel reproduction is also in the README.

## Engineering Lessons

1. **Separate semantics from execution.** Money/trades/equity/provenance compare
   exactly across backends; clocks and worker resources remain operational context.
2. **Bound both submission and retention.** A fixed queue is insufficient if the
   collector retains every detailed result. Stream artifacts and sort compact records.
3. **Make process costs explicit.** Spawn requires serialization and worker
   initialization; once-per-worker data transfer still creates separate copies.
4. **Define failure boundaries.** A failed simulation can yield a complete
   failure record; a broken pool or incomplete export invalidates experiment completion.
5. **Measure the user-visible operation.** Include startup, loading, IPC, export,
   and shutdown. Verify equal output and report overhead/variability before any claim.

## Next Phase

Phase 3 would add an optional Ray adapter to the same job/outcome contract. First
verify Ray/Python compatibility and pin an appropriate extra. Major challenges
are dataset placement and node availability, serialization/network overhead,
worker resource declarations/lifecycle, and cleanup. Run the same manifest through
sequential, process, and Ray backends; measure local Ray against this baseline.
Multiple-machine claims require actual multiple-machine evidence. Durable retries
and scheduling remain Phase 4, so they are not added prematurely in Phase 3.

## Approval Required

Phase 2 ends here. Do you want to proceed to Phase 3: distributed execution?
No Phase 3 implementation or remote push has been performed.

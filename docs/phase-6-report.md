# Phase 6 completion report

Phase 6 was explicitly approved and implemented on `feat/phase-6-performance`.
Phase 7 remains unapproved. Measurements describe this Mac and synthetic SMA
workloads; they are separate from strategy profitability and production readiness.

## Implemented

- Standard-library CPU, Python allocation and uninstrumented timing CLI/API.
- Complete retained-artifact comparison across producer source/job identities.
- Bounded native close-history slicing with reverse/empty/future-view safeguards.
- Exact drawdown comparisons with integer cross products and final Fractions.
- Raw profiles/pstats, archived wheel identity records, two controlled backend
  matrices, a measured rejected generator alternative and synchronized guides.

No new dependency or compiled extension was introduced. Important changed files:
`profiling.py`, `performance.py`, `models.py`, `metrics.py`, their regression tests,
`pyproject.toml` (0.7.0), `MANIFEST.in`, documentation and the environment snapshot.

## Architecture

The simulation, worker, backend, scheduler and storage contracts retain their
existing semantics. Native slicing applies only to the exact base history and
built-in list/tuple backing, normalized against the captured prefix. Custom
integer access keeps its fallback. Metrics validate every observation and use
positive denominators to compare rational values without per-point reduction.

Profiling distinguishes simulation from complete sequential experiment scope.
Independent result hashing follows measurement. Owned instrumentation is released
on failure; secondary observation/publication failures preserve the primary
exception. The comparer revalidates actual inputs, producer manifests, every
successful snapshot, commands, environments and resources, then compares every
result field in canonical configuration order. Only a validated release label
may be normalized. Source-dependent IDs are preserved separately.

Baseline clean commit `58837ac10216dcc2379d0b7ef0f5f3779466e4f7`; candidate
`df294d4788673e0180d0dc67715c955407a95372`. Each offline-installed wheel had all
42 package files verified against checkout bytes. Source hashes are respectively
`ab0c29cffd82af10c03146da8182e025fa11d114c328c09a3bb0b06eb303a789` and
`16a1a54602a060cea2fddaaae827b9aff99d87ccd5fbb6761a8b1731a5b834ad`.
Build/wheel provenance is recorded separately from unavailable installed-wheel Git
metadata. See [ADR 0008](decisions/0008-measured-performance.md).

## Verification

All commands ran from the repository unless noted. Actual recorded results:

| Command | Result |
| --- | --- |
| `PHASE5_STORAGE_CHILD_TESTS=1 .venv/bin/python -m pytest -q` | Final repeat: 1,397 passed in 139.41 s; native Ray, recovery and storage child tests included. Earlier optimized run: 139.17 s. |
| `.venv/bin/ruff check .` | All checks passed. |
| `.venv/bin/ruff format --check .` | All files already formatted; final repeat covered 123 files. |
| `.venv/bin/mypy` | No issues in 84 source files. |
| `.venv/bin/python -m pip check` | No broken requirements found. |
| `.venv/bin/python -m distributed_backtester backtest --data examples/sma.csv --symbol SYNTH --price-basis raw --fast 2 --slow 3 --initial-cash 1000 --fee-bps 10 --output /private/tmp/phase6-example.json` | Exit 0; hand-calculated metrics, both fills and all five equity values verified. |

Initial Phase 5 baseline: 1,213 passed in 130.39 s. Measurement-tool baseline:
1,331 passed in 137.58 s before review-fix regressions were added. Final scoped
profiler/comparer run: 156 passed in 10.19 s. New history operation-count tests
first failed twice on the old code; metrics construction count first failed
8 versus 2. Existing semantic assertions were preserved. Profiler review-fix
tests first exposed 32 failures; the corrected 112-case scope passed.

Actual profiling calls used `python -m distributed_backtester.profiling run`
with `--bars 65536 --jobs 4 --seed 1729`, each `--kind cpu|memory|timing` and
`--scope simulation|experiment`. CPU/memory had one repetition; timing used five.
Fresh roots were `/private/tmp/phase6-evidence/{baseline,generator,native-history,optimized}-{kind}-{scope}`.
All 24 reports and 56 samples match complete-result and workload hashes.

Actual controlled matrices used the same `.venv/bin/python` with
`PYTHONPATH=<verified-site>:<Ray bundled thirdparty_files>` and:

```bash
python -m distributed_backtester.ray_benchmark run --bars 256 --jobs 128 \
  --workers 2 4 8 --repetitions 3 --seed 1729 --output-dir ABS_SMALL_ROOT
python -m distributed_backtester.ray_benchmark run --bars 16384 --jobs 32 \
  --workers 2 4 8 --repetitions 3 --seed 1729 --output-dir ABS_SUBSTANTIAL_ROOT
python -m distributed_backtester.performance compare \
  --baseline ABS_BASELINE_ROOT --candidate ABS_CANDIDATE_ROOT \
  --output ABS_NEW_COMPARISON_FILE
```

The exact roots were `/private/tmp/phase6-evidence/baseline-controlled-small`,
`candidate-controlled-small`, `baseline-controlled-substantial` and
`candidate-controlled-substantial`. Each comparison exited 0 and verified 28
baseline plus 28 candidate full snapshots, including warmups: 112 trials total,
8,960 executions including repetition, 128 or 32 distinct jobs per experiment.
The first small pair was excluded after the comparer caught Ray's bundled
Colorama metadata exposure. Identical vendor-path pre-exposure passed import
preflight and fixed the environment mismatch without changing code or installing
a dependency. [Pilot diagnosis](benchmarks/phase-6/pilots/environment-preflight.json)
and all raw observations are retained.

Local checks do not establish that remote GitHub Actions or another Python/OS
combination passed. The wheel and source archive built with
`.venv/bin/python -m build --no-isolation`. The core wheel installed
with `--no-index --no-deps` in `/private/tmp/phase6-core-wheel`.

Actual isolated commands:

```bash
/private/tmp/phase6-core-wheel/bin/python -I /private/tmp/phase5-core-wheel-check.py \
  "$PWD/examples/sma.csv" /private/tmp/phase6-isolated-core-results
```

The reused local verification helper confirmed no Ray/psutil/Arrow/DuckDB were
installed, and four-job sequential, spawned-process and completed-journal
exports had one semantic digest. Source SHA matched the accepted candidate.
A second `python -I -` probe called `profile_workload` for all six kind/scope
combinations, 32 bars/four jobs, timing two repetitions. All six complete-result
hashes matched without optional dependencies. These local helpers are verification
artifacts, not runtime dependencies; public reproduction commands above and in
[performance](performance.md) use supported CLIs/APIs.

Final archive/link checks used `.venv/bin/python /private/tmp/phase6-final-verify.py`:
42 wheel package byte matches, 201 source-archive byte matches including all 50
Phase 6 evidence files, and 39 Markdown documents with 260 local links. Git
whitespace checks passed. The helper is local verification; builds include raw
JSON, pstats and the rejected alternative patch through `MANIFEST.in`.

Independent final integration/evidence review accepted Phase 6 with no blocking
findings. The minor ADR index table rendering issue was corrected. Changes remain
on the local feature branch; no remote push or Phase 7 implementation occurred.

## Performance

Host: Apple M3 Pro, 11 available/physical cores, 18 GiB RAM, macOS 26.6.1 arm64,
CPython 3.14.6; Ray 2.59.0 and psutil 7.2.2. Workloads use seeded synthetic raw
BENCH prices, 10 bps fees, identical unique configurations and complete exports.

| Measured operation | Before median s | After median s | Reduction |
| --- | ---: | ---: | ---: |
| Scoped simulation, 65,536 bars/four jobs/five repetitions | 0.975629 | 0.669989 | 31.33% |
| Scoped sequential experiment, same inputs | 1.903224 | 1.652131 | 13.19% |
| Fresh sequential small, 256 bars/128 jobs/three repetitions | 0.377949 | 0.308797 | 18.30% |
| Fresh sequential substantial, 16,384 bars/32 jobs/three repetitions | 3.821517 | 2.667269 | 30.20% |

Substantial throughput increased from 8.374 to 11.997 jobs/s for sequential
execution. Multiprocessing was effectively unchanged; Ray medians increased
1.30-7.05%. Parallel changes were small and mixed. Two processes remained
fastest for substantial inputs at 2.369008 s; higher worker counts did not scale
linearly. The bounded generator improved the baseline but lost to native slicing,
so it was not retained. Exact source patches, raw samples, all backend speedups/
efficiencies, ranges, CPU evidence and memory observations are in
[results](benchmarks/phase-6/results.md) and [performance instructions](performance.md).

## Known Limitations

- One host, synthetic data, three primary samples per configuration and consecutive
  revision order. Cache, frequency, scheduling and background activity were not
  controlled; observed ranges are not confidence intervals.
- Profilers perturb timing. CPU seconds and tracing cover this interpreter only;
  RSS is a process lifetime high water. Traced peaks barely changed. Private
  aggregate USS was unavailable; null is preserved. No memory saving is claimed.
- Cold local Ray includes fresh startup and is not network or steady-state scaling.
  Component wall timings overlap and do not isolate worker CPU.
- Detailed temporary profile exports were removed after full-result verification
  to free benchmark space; [removal records](benchmarks/phase-6/profile-export-cleanup.json)
  and raw reports/pstats remain. Formal backend details remained through comparison.
- Comparisons require absolute canonical producer roots from the start and retained
  detailed results. Relative captured commands or symlink parents are rejected.
- Existing modeling/recovery limits remain: one long-only asset, raw prices, no
  market impact/liquidity/intraday model, no dividends/splits/calendar conventions,
  and one local durable coordinator without disk-loss failover.

## Reproduction Instructions

Use Terminal or VS Code's terminal in the repository. Example CSV is included.
Install with an existing Python >=3.12 and activate the environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --editable ".[dev,benchmark,distributed,storage]"
PHASE5_STORAGE_CHILD_TESTS=1 python -m pytest -q
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m pip check
python -m build --no-isolation
python -m distributed_backtester backtest --data examples/sma.csv \
  --symbol SYNTH --price-basis raw --fast 2 --slow 3 --initial-cash 1000 \
  --fee-bps 10 --output artifacts/phase6-example.json
python -m distributed_backtester.profiling run --bars 65536 --jobs 4 \
  --kind timing --scope simulation --repetitions 5 \
  --output-dir /private/tmp/phase6-user-profile
```

Every output must be fresh. Full historical-wheel build/byte-fingerprint preflight,
Ray metadata control and exact paired matrix commands are in
[performance reproduction](performance.md#reproduce-the-wheel-comparison-with-retained-benchmark-roots).
The version snapshot is local, not a universal platform lock. The example equity
curve stays 1000.000000, 1000.000000, 1000.000000, 750.004000, 749.257000 USD.
January 7 buys 83 shares at 12.000000 with 0.996000 fee; January 8 sells at
9.000000 with 0.747000 fee. Total fees are 1.743000, return -25.0743%, drawdown
25.0743%. The intentional loss is the reference answer.

## Engineering Lessons

1. Profiling identifies costs; separate uninstrumented complete-workload timings
   establish observed performance. Cumulative call times overlap.
2. Exact ratios can be compared by integer cross products when denominators stay
   positive. Removing repeated reductions preserves precision and validation.
3. Optimized bounded slices need explicit reverse sentinels and empty-view guards;
   returning the right values now is insufficient if future data becomes visible.
4. Faster simulation does not imply faster distributed execution. Startup, result
   transport, serialization and export can dominate; worker count is not speedup.
5. Producer identity differs from economic equality. Keep source/job identities
   and validate every full result before comparing revisions or interpreting time.

## Next Phase

Phase 7 would add Docker-based reproducibility, structured operational logs,
configuration files, status reporting and developer documentation. The important
challenge is keeping the same semantics, ownership/recovery rules and measurement
boundaries in a reproducible operational environment. No Phase 7 work is started.

## Approval Required

Do you want to proceed to Phase 7?

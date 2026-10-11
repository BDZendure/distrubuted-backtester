# Testing strategy

## Current checks

Phases 1 and 2 check exact money/contracts, strict CSV validation, SMA decisions,
execution, metrics, causal chronology, complete JSON publication, and real CLI
subprocesses, serializable jobs, source fingerprints, common-data initialization,
real spawn execution with 1/2/4/8 workers, bounded buffering, deterministic
experiment artifacts, and explicit failure/finalization behavior. The packaging test imports from an unrelated directory in isolated
mode; CLI tests also run outside the repository without `PYTHONPATH` workarounds.
CI builds wheel/source artifacts and runs the wheel's installed CLI in a fresh
environment against the committed synthetic example.

Phase 3 adds actual Ray actors and CLI backend equivalence, immutable dataset and
code identity checks, actor reuse, bounded submission, early close, custom resource
validation, readiness, initialized-driver refusal, and owned shutdown. Narrow
unit tests also cover the pinned reaper-pipe cleanup adapter without suppressing
warnings. Real cluster/process measurement tests require process enumeration and
local sockets; the macOS tool sandbox blocks those APIs, so local verification
uses an unrestricted execution context. This does not weaken the tests.

Run pytest, Ruff lint/format, strict mypy, `pip check`, and `python -m build` using
the [README commands](../README.md). Development checks are configured centrally
in `pyproject.toml`. Counts and commands are recorded in the
[Phase 2 report](phase-2-report.md). A count alone does not prove correctness;
independent expectations and boundary cases provide the evidence.

## Phase 1 correctness evidence

Use small literal synthetic datasets with expected outcomes computed separately
from the engine. Tests should explain the event chronology and expected ledger.
Do not calculate expected values by calling the production functions under test.

| Area | Cases and evidence |
| --- | --- |
| Input | Empty file/header-only; malformed date; missing/duplicate/unknown columns; missing fields; unsorted/duplicate sessions; NaN/infinity; nonpositive/incoherent OHLC; invalid volume; precision rejection |
| Strategy | Invalid windows; warm-up; initial ready entry; crossover exit; equality preserves target; no repeated investment/rebalancing; history ends at current close |
| Execution | Decision T, fill next supplied open; overnight gaps; no pending first-bar order; terminal cancellation; invalid targets/sequences/timing; insufficient cash; zero affordable shares; all-holdings exit |
| Fees/accounting | Zero/positive/max configured fees; rounding ties; affordability boundary after rounded fee; buy/sell ledger; cash/holdings never negative; final open position |
| Metrics | No trades; flat path; losses below initial cash; monotonic rise; peak-to-trough drawdown; denominator/initial cash rules |
| Integration | Full synthetic backtest; exact ledger reference; repeated runs; CLI export and invalid-input exit; no success artifact on failure |

Lookahead tests mutate a future suffix and assert earlier decisions/fills are
unchanged. A separate test changes T+1's open and checks that T's signal stays
unchanged while the fill price and cash-constrained quantity change appropriately.
For truncation tests, compare completed decisions/fills only: truncating the dataset
necessarily changes whether the final pending order is canceled.

Cross-check cash independently: initial cash minus purchase notionals plus sale
notionals minus charged fees equals final cash. For every equity observation,
`equity = cash + shares * close`. Return/drawdown are exact `Fraction` values;
JSON rounds those ratios half-even to twelve places. Accounting remains exact.

The hand-calculated SMA case is in the [technical explanation](technical-explanation.md)
and [Phase 1 report](phase-1-report.md). `test_simulation.py` asserts its complete
ledger and equity path. Affordability additionally checks 2,928 small combinations
against exhaustive quantities and independent `Fraction` rounding. This bounded
enumeration checks fee boundaries, not every possible large integer input.

## Backend and reliability checks

Phase 2 compares complete canonical semantic outputs across sequential and process
execution, varying worker count and deliberately varying completion order. Include
ordinary failed jobs among successes and verify one outcome per unique job ID.
Verify incremental export and a stable compact index without retaining all detailed
results. Increase sweep size and inspect coordinator memory as well as worker memory.
Tests use real subprocesses and real spawn pools. Narrow injection at an
exception/export boundary is explicitly labeled, as are instrumented pool wrappers.
A paused consumer test independently counts submissions, records initialization
once per worker and multiple jobs per worker, and checks futures are released.
The benchmark tests run actual fresh-process sequential/process trials. Optional
psutil is installed for CI; measurement tests label injected denial/race cases.
No mocked runtime substitutes for actual process execution.

Phase 3 runs the same manifest through real Ray actors. Distinguish local remote-worker coverage
from true multiple-machine behavior. Pin and record the environment for each run.

Phase 4 uses real persistent transactions and controlled failure injection at job,
acknowledgment, and commit boundaries. Assert bounded retries, fenced stale attempts,
unique durable successes, divergent duplicate detection, and restart reconciliation.

Phase 5 round-trips exact values through Parquet and compares economic result
fields after establishing bar/config/policy equality. Source artifact digests and
format provenance are checked separately, including conversion lineage. Same-source
backend equivalence still includes all provenance. Verify query/commit reconciliation.
Phase 6 runs semantic checks unchanged around optimizations.
Phase 7 adds full CLI/container/status/logging smoke coverage.

## Reporting discipline

Record exact commands, versions, exit outcomes, test counts, and execution blockers
in each phase report. Distinguish configured CI jobs from CI runs actually observed.
Do not delete or weaken a failing assertion to fit generated output. Investigate
the underlying chronology or contract first; document justified contract changes.


## Phase 3 failure and measurement evidence

`tests/test_ray_faults.py` preserves pure transport-boundary checks for unexpected
outcome types/IDs, actor errors, readiness timeout, export failure, early close,
and independent bounded-reference release. Those cases are explicitly labeled
boundary injection, not a replacement for actual Ray execution. Real subprocess
cases crash an actor during execution and fail artifact export while actors are
alive, then inspect owned process exit and absence of a final index.

`tests/test_ray_cluster.py` starts real one-node and two-node local clusters,
attaches separate drivers, checks active resource reservations and distinct node
placement, and verifies external service survival after success, early close,
and impossible per-node placement. Local nodes share hardware. The harness uses
Ray's pinned private Cluster helper for test setup and records the limitation.

The owned benchmark inventory tests use fake process boundaries to reproduce
PID/group reuse, denied discovery/memory, shared memory accounting, and safe
ownership certification. Benchmark tests verify initial workload/source identity,
all-success semantic artifacts, repeated order, failure preservation, optional
imports, and explicit post-verification deletion. Real cold smoke commands prove
the full subprocess path separately. Preserve negative controls; do not reduce
assertions to the backend's self-reported counters.


## Phase 4 verification boundaries

The journal tests use real SQLite subprocess termination during claims and full
terminal transactions, plus a kill after commit before acknowledgment. Coordinator
unit tests use an explicitly fake transport/clock around that real journal for
reproducible deadlines and retry behavior. Ray transport unit tests validate the
actual runtime against a labeled in-memory transport, including wrapped exceptions
and readiness ordering. They do not establish actual worker-crash recovery.

`tests/test_scheduler_faults.py` contains six actual Ray cases: death before dispatch,
death during real simulation steps, lost reply after calculation, live timeout,
repeated structured failure, and abrupt coordinator restart. Run after freezing
package source. These cases need local sockets and process control and are
subject to the environment's process permissions. After explicit user approval,
all six cases passed in the focused run with CLI tests: 17 passed in 40.47 seconds.
The final complete suite passed all 897 tests in 105.18 seconds, including stronger
timeout assertions, those same fault scenarios, and the existing
cluster/resource/ownership tests.
See [the verification report](phase-4-report.md) for observed commands and outcomes.

## Phase 5 verification boundaries

Parquet tests round-trip exact values and hand-calculated simulation economics.
They reject float prices, timestamp dates, nullable/extra/missing fields, invalid
prices/volumes, global ordering violations across row groups, and inconsistent
provenance. Corruption fixtures distinguish self-consistent descriptor changes
from actual content changes, so version checks cannot mask bar-hash checks.
CSV serialization retains its exact five-field metadata contract.

Cache tests verify format/symbol separation, least recent use, immutable reuse,
disabled-cache decoding, and same-size/same-timestamp replacement. Analytics
tests exercise real DuckDB transactions and real SQLite snapshots, complete
classic verification, damaged exports, distinct terminal snapshots, idempotent
imports, exact raw metrics, bounded numeric projections, and rollback after a
late invalid artifact. Native dependency failures must be clear CLI errors.

`tests/test_storage_ray.py` compares complete same-Parquet semantics across
sequential, spawn processes, classic Ray, and durable Ray, including completed
resume without extra attempts. Run it after freezing package source. Storage
measurement unit tests cover reported boundaries; actual subprocess benchmark
runs establish the measured evidence separately. See [Phase 5 report](phase-5-report.md).


The final accepted Phase 5 source passed **1,213 tests in 347.97 seconds** on
CPython 3.14.6/macOS, with `PHASE5_STORAGE_CHILD_TESTS=1` enabling the real storage
child case. Ruff, formatting, strict mypy, and dependency checks passed.
CI enables that case in the main checks job, while a separate job installs Ray
and exercises its integration/fault tests. Remote CI outcomes are not claimed
from local execution. See the [completion report](phase-5-report.md) for exact
commands, isolated wheel checks, and source identity.


## Phase 6 optimization and measurement validation

`test_models.py` checks 21,294 bounded-slice oracle cases, reverse/empty old views
with appended future data, custom sequence/history overrides and a deterministic
recursive-access count. `test_metrics.py` compares exact fractions on seeded
paths, ties, zero equity, huge integers and late invalid values. Fraction
construction counts are asserted without a timing threshold.

`test_profiling.py` checks measurement boundaries, complete-result hashes,
repetition equality, input/source drift, conflicting caller instrumentation,
primary/secondary failures and owned tracer cleanup. `test_performance.py` uses
explicitly synthetic durations over real exported artifacts for ratio math,
full-trace differences despite equal final metrics, release normalization,
metadata/resources, completeness and protected publication. These fixture times
are not benchmark evidence. Real measurements and full backend verification are
[recorded separately](benchmarks/phase-6/results.md).

# Testing strategy

## Current checks

Phase 1 checks exact money/contracts, strict CSV validation, SMA decisions,
execution, metrics, causal chronology, complete JSON publication, and real CLI
subprocesses. The packaging test imports from an unrelated directory in isolated
mode; CLI tests also run outside the repository without `PYTHONPATH` workarounds.
CI builds wheel/source artifacts and runs the wheel's installed CLI in a fresh
environment against the committed synthetic example.

Run pytest, Ruff lint/format, strict mypy, `pip check`, and `python -m build` using
the [README commands](../README.md). Development checks are configured centrally
in `pyproject.toml`. Counts and commands are recorded in the
[Phase 1 report](phase-1-report.md). A count alone does not prove correctness;
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

## Later backend and reliability checks

Phase 2 compares complete canonical semantic outputs across sequential and process
execution, varying worker count and deliberately varying completion order. Include
ordinary failed jobs among successes and verify one outcome per unique job ID.
Verify incremental export and a stable compact index without retaining all detailed
results. Increase sweep size and inspect coordinator memory as well as worker memory.
Use real subprocesses; mocks cannot establish spawn/serialization behavior.

Phase 3 runs the same manifest through Ray. Distinguish local remote-task coverage
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

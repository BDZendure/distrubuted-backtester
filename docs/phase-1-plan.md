# Phase 1 Implementation Plan

**Goal:** Deliver a deterministic daily, single-asset, long-only backtester with
strict CSV validation, a CLI, exact accounting, and independently checked examples.

**Architecture:** Validated immutable domain records feed a pure simulation loop.
The strategy sees only completed closes; execution handles cash-constrained targets
at the next open. File loading and JSON publication remain outside that loop.

**Tech stack:** Python 3.12+ standard library, existing pytest/Ruff/mypy/build tools.

## Global constraints

- Implement Phase 1 only. Stop for explicit approval before Phase 2.
- No runtime dependencies, concurrency, distributed infrastructure, or database.
- Preserve the existing user files and Git history; do not push remotely.
- Whole shares, USD micro-dollar integers, raw unadjusted daily prices.
- Strict ISO session dates and chronological ordering; no silent data repair.
- Observe T close, execute at the next supplied open, then mark at the close.
- Proportional integer fee basis points in [0, 10000], half-even to micro-dollars.
- Buy the greatest affordable whole-share quantity including the rounded fee.
- Initial SMA target flat; warm-up through the slow window; strict comparisons
  change target; equality retains it; emit only changes and no daily rebalancing.
- Cancel final pending intent without a next open; retain final holdings.
- Exact money strings with six places, exact ratios with twelve places, sorted
  deterministic JSON; no timestamps, paths, or process IDs in semantic output.
- Tests precede production behavior and use independent expected values.
- Keep the beginner and technical explanations synchronized with actual behavior.

## Files and interfaces

| File | Responsibility |
| --- | --- |
| `src/distributed_backtester/money.py` | Exact text-to-micro conversion, half-even integer division, fixed-place formatting |
| `src/distributed_backtester/models.py` | Frozen bars, dataset/config, portfolio, intents, fills/outcomes, equity, metrics/results |
| `src/distributed_backtester/data.py` | Parse a single hashed UTF-8 CSV snapshot and reject invalid rows |
| `src/distributed_backtester/strategy.py` | Prefix-only strategy protocol and SMA regime state |
| `src/distributed_backtester/execution.py` | Fees, maximum affordable quantity, immutable portfolio updates, rejected intents |
| `src/distributed_backtester/metrics.py` | Exact total return and initial-anchored maximum drawdown |
| `src/distributed_backtester/simulation.py` | Daily chronology, fresh default strategy state, fills/equity/terminal outcomes |
| `src/distributed_backtester/results.py` | Canonical JSON and complete-file publication |
| `src/distributed_backtester/cli.py`, `__main__.py` | `backtest` command, user-facing errors, optional output file |
| `examples/sma.csv` | Tracked synthetic scenario, including both entry and exit |
| `tests/test_*.py` | Unit, causal, reference, CLI, packaging, and integration checks |
| `docs/phase-1-report.md` | Actual verification, sample ledger, limits, reproduction, and Phase 2 gate |

Public core: `load_csv(path: Path, *, symbol: str, price_basis: Literal["raw"]) -> MarketDataset`
and `run_backtest(config: BacktestConfig, dataset: MarketDataset) -> BacktestResult`.
An optional testable strategy implements
`on_close(history: CloseHistory, portfolio: Portfolio) -> TargetIntent | None`.
Only observed dates/closes are supplied; the whole bar dataset is never exposed.
The read-only history captures prefix bounds over an append-only observed buffer;
constructing a view does not copy every prior observation on every session.

## Task 1: Exact units and validated immutable contracts

**Files:** `money.py`, `models.py`, `tests/test_money.py`, `tests/test_models.py`.

- [ ] Write tests for exact parsing (including trailing zeros and ambient Decimal
      context independence), invalid/nonfinite precision, half-even ties, and formats.
- [ ] Run the focused tests and observe failure from missing implementation.
- [ ] Implement integer/tuple-backed frozen contracts and strict constructor
      validation, rejecting bool as integer input, invalid OHLC, invalid windows,
      negative cash/holdings, and invalid metadata.
- [ ] Verify focused tests, lint, and types; retain clear error reasons.

Reference assertions:

```python
assert parse_money("12.0000000") == 12_000_000
assert round_half_even(25, 10) == 2
assert round_half_even(35, 10) == 4
assert format_money(3_004_000) == "3.004000"
```

Run: `.venv/bin/python -m pytest tests/test_money.py tests/test_models.py`.

## Task 2: Strict data loading and isolated SMA decisions

**Files:** `data.py`, `strategy.py`, `tests/test_data.py`, `tests/test_strategy.py`.

- [ ] Write and observe failing validation and signal tests before implementation.
- [ ] Load one `read_bytes()` snapshot, hash the same bytes, and strictly parse
      UTF-8 CSV with the exact six-column schema and source row/field diagnostics.
- [ ] Reject empty/header-only files, duplicate/unknown/missing headers, malformed
      dates/rows, blank fields/rows, inconsistent/nonfinite prices, invalid volume,
      precision loss, duplicates, and unsorted sessions. Dates are session labels.
- [ ] Implement SMA state using integer sums and cross-multiplication. Test warm-up,
      first ready entry, equality, exit, repeated regime, and separate instances.
- [ ] Verify both components independently against the common frozen contracts.

Run: `.venv/bin/python -m pytest tests/test_data.py tests/test_strategy.py`.

## Task 3: Execution and metrics

**Files:** `execution.py`, `metrics.py`, `tests/test_execution.py`, `tests/test_metrics.py`.

- [ ] Write failing tests for gaps, fee rounding, maximal affordable sizing,
      zero-share rejections, invalid timing/targets, no-op orders, and liquidation.
- [ ] Calculate each fee with integer half-even division. Find maximal quantity
      against final fees with a bounded binary search and test neighboring quantities.
- [ ] Return an immutable portfolio and explicit filled/rejected/no-op outcome.
      Invalid order intent cannot mutate cash/holdings or charge a fee.
- [ ] Write metric tests for no trades, rises, losses below initial cash, complete
      loss, and peak-to-trough decline. Use exact `Fraction` ratios internally.
- [ ] Verify the independent cash reconciliation and equity identity.

Reference ledger: cash 1000, open 12, 10 bps -> 83 shares, fee 0.996, cash 3.004;
close 11 -> equity 916.004; next open 9 sale -> fee 0.747 and cash 749.257.

Run: `.venv/bin/python -m pytest tests/test_execution.py tests/test_metrics.py`.

## Task 4: Simulation chronology and canonical CLI output

**Files:** `simulation.py`, `results.py`, `cli.py`, `__main__.py`,
`tests/test_simulation.py`, `tests/test_results.py`, `tests/test_cli.py`,
`examples/sma.csv`, `pyproject.toml`, `MANIFEST.in`.

- [ ] Write and observe failing complete-simulation/CLI/export tests.
- [ ] Implement open fill, close mark, prefix strategy call, and terminal cancellation.
      Use fresh strategy state for every normal invocation. Reject non-past intents.
- [ ] Test future-suffix mutation, next-open changes, last-bar cancellation, one-bar
      input, insufficient cash, retained holdings, no trades, repeated runs, and
      independently calculated entry/exit paths.
- [ ] Serialize all config/provenance, fills/outcomes, equity, final state, and
      exact metrics without operational metadata. Record release and semantics identity.
- [ ] Publish a complete JSON file from a temporary sibling; protect existing
      output unless overwrite is explicit. Reject input/output aliasing. Failed
      validation cannot overwrite an earlier result or publish a new success file.
- [ ] Expose installed `distributed-backtester backtest` and module invocation;
      parse/validate options and report errors with nonzero exits and no traceback.
- [ ] Add the synthetic example and include it in source distributions.

Example CLI:

```bash
.venv/bin/python -m distributed_backtester backtest --data examples/sma.csv \
  --symbol SYNTH --price-basis raw --fast 2 --slow 3 \
  --initial-cash 1000 --fee-bps 10 --output artifacts/example.json
```

Expected output is computed from the committed synthetic rows before comparing
with the engine. The CLI is tested in real subprocesses and fresh wheel installation.

## Task 5: Documentation, review, checks, and checkpoint

**Files:** README, architecture/roadmap/ADRs, both explanations, testing,
dependencies, `docs/phase-1-report.md`, this progress plan, CI as needed.

- [ ] Mark Phase 1 design accepted and distinguish implemented modules from future
      contracts. Preserve Phase 0 report as historical evidence.
- [ ] Explain every synthetic-example signal, trade, fee, and equity observation.
- [ ] Run pytest, Ruff lint/format, strict mypy, `pip check`, wheel/source build,
      installed CLI/wheel smoke, repeated-byte comparison, and document links.
- [ ] Obtain an independent review and resolve substantive findings within Phase 1.
- [ ] Record exact commands and actual results, limitations, and interview concepts.

Run:

```bash
.venv/bin/python -m pytest
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
.venv/bin/python -m mypy
.venv/bin/python -m pip check
.venv/bin/python -m build
```

Make focused local commits after verified milestones. Present the Phase 1 report
and request explicit approval before any Phase 2 work. No throughput claim will
be inferred from test durations or a demonstration run.

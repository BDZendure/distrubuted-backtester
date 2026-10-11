# Distributed Backtesting Engine

A portfolio engineering project for reproducible strategy simulations, local CPU
parallelism, and eventually distributed execution. The focus is correct event
timing, exact accounting, explicit failure behavior, and measured performance.

**Current stage: Phase 1, single-process engine.** Run a deterministic daily
single-asset SMA backtest from CSV, with exact cash/fee accounting, next-open fills,
equity, metrics, and JSON output. Phase 2 requires explicit approval.

## Start with the explanations

- [Clear explanation for beginners](docs/clear%20explanation.md): what a backtest
  does, how the pieces fit, and why correctness comes first.
- [Technical explanation](docs/technical-explanation.md): event timing, exact
  accounting, data contracts, backend equivalence, and recovery tradeoffs.
- [Architecture](docs/architecture.md): full system design, proposed interfaces,
  directory structure, and engineering risks.
- [Roadmap and phase checkpoints](docs/roadmap.md): acceptance and validation for
  all nine phases.
- [Architecture decisions](docs/decisions/README.md), [testing strategy](docs/testing.md),
  [benchmark methodology](docs/benchmarking.md), and [dependency plan](docs/dependencies.md).
- [Phase 1 completion report](docs/phase-1-report.md), with actual verification and
  the sample ledger; [Phase 0 report](docs/phase-0-report.md) records the foundation.

## Development setup

Requires Python 3.12 or newer. From the repository directory:

```bash
python3 --version
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --editable ".[dev]"
```

The virtual environment isolates project tools. Editable installation makes
changes under `src/` available through the installed package. The `dev` extra
installs pytest, Ruff, mypy, and build; there are no runtime dependencies.

## Run a backtest

```bash
distributed-backtester backtest --data examples/sma.csv \
  --symbol SYNTH --price-basis raw --fast 2 --slow 3 \
  --initial-cash 1000 --fee-bps 10 --output artifacts/example.json
python -m json.tool artifacts/example.json
```

`python -m distributed_backtester backtest` accepts the same options. Omit
`--output` to write JSON to stdout. Existing output is protected; add `--overwrite`
to explicitly replace it. Invalid inputs exit with status 2 and an error on stderr.
Run `distributed-backtester backtest --help` for available options.

The committed example is synthetic. With these settings, its SMA signal enters
after January 6's close and buys **83 shares at January 7's $12 open**. The buy fee
is **$0.996**, leaving **$3.004** cash. January 7's $9 close gives **$750.004** equity
and an exit signal. January 8 sells at **$9**, charges **$0.747**, and leaves
**$749.257**. Total return is **-25.0743%**, maximum drawdown **25.0743%**, and total
fees **$1.743**. The [phase report](docs/phase-1-report.md) explains every equity point.
These losses are expected reference outcomes, not a claim about real trading.

## Data and modeling rules

CSV requires exactly `date,open,high,low,close,volume` (header order may vary).
Dates are unique increasing `YYYY-MM-DD` session labels. OHLC must be positive,
coherent, and exactly representable in micro-dollars; volume is a nonnegative
integer. Empty, malformed, duplicated, and unsorted input is rejected without repair.
`--price-basis raw` declares unadjusted USD prices. For real data, the caller must
establish that the interval has no splits or dividends; CSV validation cannot prove it.

The strategy observes closes through T, queues a target change, and executes at
the next supplied open. It warms up through the slow window; a higher fast SMA
requests investment, a lower one cash, and equality retains the prior target.
Already-invested holdings are not rebalanced; rejected entries are not retried
until a new target change. Final pending intents are canceled, and holdings stay
marked to the final close without forced liquidation.

Money uses integer micro-dollars, whole shares, and fees of 0..10,000 integer
basis points rounded half-even to a micro-dollar. JSON money has six decimal
places; return/drawdown are ratios with twelve places. Output includes source
SHA-256, policy, config, release/semantics identity, fills/outcomes, equity, and
metrics. It excludes clocks and paths. Release labels do not hash editable code;
retain the Git revision and environment when reproducing a result.

## Run the current checks

```bash
python -m pytest
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m pip check
python -m build
```

Check the built wheel in a fresh environment:

```bash
python -m venv .venv-wheel-phase1
.venv-wheel-phase1/bin/python -m pip install --no-index --no-deps --force-reinstall \
  dist/distributed_backtester-0.2.0-py3-none-any.whl
wheel_python="$PWD/.venv-wheel-phase1/bin/python"
wheel_cli="$PWD/.venv-wheel-phase1/bin/distributed-backtester"
sample_data="$PWD/examples/sma.csv"
(cd /tmp && "$wheel_python" -I -c "import distributed_backtester")
(cd /tmp && "$wheel_cli" backtest --data "$sample_data" --symbol SYNTH \
  --price-basis raw --fee-bps 10)
```

These shell commands target macOS/Linux. GitHub Actions is configured to run the
checks on Linux with Python 3.12 and 3.14. The [phase report](docs/phase-1-report.md)
records what was actually executed locally and what remains unobserved remotely.

## Scope and limitations

Later phases add parameter sweeps, multiprocessing, Ray, durable retries and
idempotent result commits, Parquet/DuckDB, profiling, and operational tooling.
These capabilities are plans, not existing functionality or performance claims.

The initial execution model has no market impact, volume-based liquidity limits,
intraday execution, dividends, splits, leverage, short selling, or fractional
shares. Synthetic data establishes correctness. An incomplete real-data universe
cannot establish freedom from survivorship bias or investment performance.

Phase 1 has no exchange calendar, annualized/Sharpe conventions, scheduler,
database, or performance baseline. Local complete-file export requires filesystem
hard links/atomic replacement; it does not provide distributed commits or crash
recovery. Custom strategies are a Python testing/extension hook, with caller-owned
state; their results cannot be exported without supported parameter provenance.
The CLI runs the fully described SMA.

## Development workflow

Keep phase changes focused on feature branches, verify before committing, and
update docs alongside implementation. Complete one phase, report evidence, and
wait for explicit approval before the next. Remote pushes require authorization.

Licensed under the existing [MIT license](LICENSE).

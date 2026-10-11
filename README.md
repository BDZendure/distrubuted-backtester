# Distributed Backtesting Engine

A portfolio engineering project for reproducible strategy simulations, local CPU
parallelism, and eventually distributed execution. The focus is correct event
timing, exact accounting, explicit failure behavior, and measured performance.

**Current stage: Phase 2, local parallel execution.** Run a deterministic daily
SMA backtest, or a parameter sweep through sequential and spawn process backends.
Stable jobs, bounded submissions, explicit failures, incremental result export,
and reproducible benchmark measurements extend the Phase 1 engine.
Phase 3 requires explicit approval.

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
- [Phase 2 completion report](docs/phase-2-report.md) and
  [local scaling results](docs/benchmarks/phase-2/results.md).
- [Phase 1 report](docs/phase-1-report.md), with the sample ledger, and
  [Phase 0 report](docs/phase-0-report.md) record earlier milestones.

## Development setup

Requires Python 3.12 or newer. From the repository directory:

```bash
python3 --version
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --editable ".[dev,benchmark]"
```

The virtual environment isolates project tools. Editable installation makes
changes under `src/` available through the installed package. The `dev` extra
installs pytest, Ruff, mypy, and build. `benchmark` adds psutil 7.2.2 for process
measurements. The core and process backend have no third-party dependencies; omit
`,benchmark` if you only want backtests and sweeps.

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

## Run a parameter sweep

Use Terminal, or the terminal in VS Code, from the repository directory:

```bash
distributed-backtester sweep --data examples/sma.csv --symbol SYNTH \
  --price-basis raw --fast 1 2 --slow 3 4 --initial-cash 1000 --fee-bps 10 \
  --backend sequential --output-dir artifacts/sweep-sequential

distributed-backtester sweep --data examples/sma.csv --symbol SYNTH \
  --price-basis raw --fast 1 2 --slow 3 4 --initial-cash 1000 --fee-bps 10 \
  --backend process --workers 4 --output-dir artifacts/sweep-process

python -m json.tool artifacts/sweep-process/index.json
```

This runs four distinct combinations of fast and slow SMA windows. Every pair
must satisfy `1 <= fast < slow`; an invalid pair rejects the entire grid. Repeated
pairs execute once, with the requested grid membership preserved in the manifest.
The committed CSV is ready to use; a dashboard is not required.

Each run requires a **new output directory**. No experiment overwrite or resume
is implemented. Choose a new name for another run.

| File | Contents |
| --- | --- |
| `manifest.json` | Dataset/config/code identity, sorted jobs, requested grid membership |
| `results/<job_id>.json` | Job definition and detailed successful result or explicit failure |
| `index.json` | Sorted compact outcomes, metrics, artifact digests, and success/failure counts |
| `execution.json` | Backend/workers, observed timings, path, environment, Git revision/status |

Semantic files are identical across backends for identical inputs and installed
source. `execution.json` records operational differences. The index is published
last. A fatal pool, collection, or export error can leave partial files without an
index; they are not a completed experiment. Exit codes: 0 for all jobs successful,
1 for a completed experiment with job failures, 2 for input or fatal errors.

The process backend sends immutable market data once during each worker's
initialization, then sends small job specs. It retains at most `--max-in-flight`
unconsumed futures, defaulting to twice the workers. Each worker has its own data
copy. Detailed results are exported as they finish; job specs and compact outcome
records still grow with the job count.

## Reproduce the benchmarks

```bash
python -m distributed_backtester.benchmark run --bars 64 --jobs 32 \
  --workers 2 4 8 --repetitions 5 --output-dir artifacts/benchmark-small
```

The harness generates deterministic synthetic CSV data and unique jobs. It runs
one warm-up per configuration, then five timed repetitions in rotating order.
Each trial starts a fresh coordinator and pool. End-to-end timing includes imports,
CSV loading, worker startup, simulation, serialization, detailed export, and shutdown.
All detailed artifact hashes and semantic equality are verified after each trial.

`report.json` retains environment, workloads, raw RSS samples, timings, and summaries;
`observations.json` retains observations if the driver aborts. Memory is sampled
process-tree RSS at 50 ms, including the coordinator and descendants. It can miss
brief peaks and count shared pages more than once. OS cache state is uncontrolled.
See [methodology](docs/benchmarking.md) and [recorded results](docs/benchmarks/phase-2/results.md)
for the fixed substantial workload, variability, limitations, and exact commands.
The [version snapshot](requirements/benchmark.lock) captures the observed macOS
arm64/Python 3.14.6 environment; it is not a universal platform lock.

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
retain the Git revision and environment when reproducing a single result. Phase 2
job envelopes add the actual installed-source fingerprint and experiment execution
reports capture revision/status and the complete installed environment.

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
python -m venv .venv-wheel-phase2
.venv-wheel-phase2/bin/python -m pip install --no-index --no-deps --force-reinstall \
  dist/distributed_backtester-0.3.0-py3-none-any.whl
wheel_python="$PWD/.venv-wheel-phase2/bin/python"
wheel_cli="$PWD/.venv-wheel-phase2/bin/distributed-backtester"
sample_data="$PWD/examples/sma.csv"
(cd /tmp && "$wheel_python" -I -c "import distributed_backtester")
(cd /tmp && "$wheel_cli" backtest --data "$sample_data" --symbol SYNTH \
  --price-basis raw --fee-bps 10)
```

These shell commands target macOS/Linux. GitHub Actions is configured to run the
checks on Linux with Python 3.12 and 3.14. The [Phase 2 report](docs/phase-2-report.md)
records what was actually executed locally and what remains unobserved remotely.

## Scope and limitations

Later phases add Ray, durable retries and
idempotent result commits, Parquet/DuckDB, profiling, and operational tooling.
These later capabilities remain plans. Local measurements describe this host and
workloads; they do not establish cross-machine scaling or production readiness.

The initial execution model has no market impact, volume-based liquidity limits,
intraday execution, dividends, splits, leverage, short selling, or fractional
shares. Synthetic data establishes correctness. An incomplete real-data universe
cannot establish freedom from survivorship bias or investment performance.

The current engine has no exchange calendar, annualized/Sharpe conventions,
durable scheduler, retries, or database. Local complete-file export requires filesystem
hard links/atomic replacement; it does not provide distributed commits or crash
recovery. Custom strategies are a Python testing/extension hook, with caller-owned
state; their results cannot be exported without supported parameter provenance.
The CLI runs the fully described SMA.

## Development workflow

Keep phase changes focused on feature branches, verify before committing, and
update docs alongside implementation. Complete one phase, report evidence, and
wait for explicit approval before the next. Remote pushes require authorization.

Licensed under the existing [MIT license](LICENSE).

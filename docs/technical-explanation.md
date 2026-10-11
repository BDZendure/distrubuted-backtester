# Technical explanation

For a focused systems discussion, read the [engineering deep dive](engineering-deep-dive.md).
For measurement scope and interview claims, use the [benchmark synthesis](benchmark-results.md)
and [portfolio summary](portfolio-summary.md). This document is the detailed implementation reference.

## Implemented scope and verification

The Phase 1 engine implements a deterministic daily backtester for one asset,
USD, whole shares, and long-only targets. Phase 2 adds immutable jobs, parameter
sweeps, sequential and local process backends, incremental detailed export, a
compact result index, and a benchmark runner. Phase 3 adds optional Ray actors.
Version `0.8.0` retains strict CSV
loading, the SMA strategy, next-open execution, exact accounting, return and
drawdown metrics, and versioned JSON export. Runtime dependencies in
`pyproject.toml` are empty. CSV parsing, integer arithmetic, `Fraction`, command
parsing, hashing, JSON, and process execution use Python's standard library.
The optional `benchmark` extra pins `psutil==7.2.2` for memory sampling.

The [Phase 1 plan](phase-1-plan.md) defines the engine scope, and the
[Phase 2 plan](phase-2-plan.md) defines local experiment execution. The
[Phase 1 verification report](phase-1-report.md) records the engine evidence;
the [Phase 2 report](phase-2-report.md) records its parallel execution checks,
environment, packaging evidence, and limitations. Current operational checks
are recorded in the [Phase 7 report](phase-7-report.md). This guide
describes implemented contracts; it does not infer verification from a plan or
quote a provisional test count. See the
[beginner guide](clear%20explanation.md) for setup and plain-language definitions.

Ray execution is implemented in Phase 3, with identical engine and job contracts.
Phase 4 adds a local durable scheduler around Ray. Classic Phase 3 execution
still has no recovery protocol or multiple-machine performance claim.
Phase 5 adds optional exact Parquet, a verified explicit cache, and DuckDB import
of complete result snapshots. See [storage commands](storage.md).

**Phase 7 complete:** configuration, operational logging/status, signal
handling, and installed Docker targets passed native, installed-wheel and Linux
ARM64 release checks. The [Phase 7 report](phase-7-report.md) and
[verification record](phase-7-verification.json) record the final checks and
limits. Remote CI and Linux AMD64 execution remain unobserved. The Phase 6
measurements remain historical results from archived `0.7.0` wheels and their
recorded source identities. Phase 8 was explicitly approved for final portfolio documentation.

## Actual module boundaries

| Module | Implemented responsibility |
| --- | --- |
| `models.py` | Frozen domain records for bars, metadata, dataset, configuration, portfolio, intents, fills/outcomes, equity, metrics, and results; bounded `CloseHistory` views |
| `money.py` | Exact decimal-text parsing to integer micro-USD, integer half-even rounding, and fixed-place money/ratio formatting |
| `data.py` | Read and hash one CSV byte snapshot, validate it, and return immutable data |
| `market_data.py`, `dataset_identity.py`, `parquet_data.py` | Format-aware loading, explicit verified LRU, normalized data versions, exact Arrow storage |
| `strategy.py` | `Strategy` protocol and stateful `MovingAverageCrossover` target decisions |
| `execution.py` | Validate intents, calculate fees, find affordable quantity, and return an updated portfolio plus an order outcome |
| `simulation.py` | Session chronology, pending intent and sequence validation, fill/equity collection, terminal cancellation, and result assembly |
| `metrics.py` | Exact return and initial-capital-anchored maximum drawdown from recorded equity |
| `results.py` | Canonical result dictionaries/JSON and complete local-file publication |
| `provenance.py` | Installed-source fingerprint and separate environment/Git snapshot |
| `jobs.py` | Canonical immutable job identity, strict decoding, and grid deduplication with retained membership |
| `worker.py` | Execute one job against matching data/implementation and return a result or structured ordinary failure |
| `backends/base.py`, `sequential.py`, `process.py` | Streaming outcome protocol, reference execution, and bounded spawn execution |
| `experiments.py` | Load once, create jobs, export detailed outcomes, and finalize semantic index plus operational execution record |
| `experiment_snapshot.py`, `analytics.py` | Verify complete classic/journal snapshots and import/query an owned DuckDB analytical copy |
| `workloads.py` | Deterministic synthetic CSV and distinct SMA benchmark configurations |
| `measurement.py` | Optional process-tree RSS sampling, fresh child-process timing, and host metadata |
| `benchmark.py` | Fresh-process trials, artifact verification, rotating repetitions, raw observations, and summaries |
| `storage_measurement.py`, `storage_benchmark.py` | Format loads, repeated experiments, cache reuse, serialization/transfer, high-water RSS, raw verified repetitions |
| `configuration.py`, `cli_options.py` | Strict command-specific TOML defaults and argument preparation |
| `operational_logging.py` | Scoped JSON observations and serializable worker logging context |
| `operational_status.py` | Validated read-only journal or complete classic-export status |
| `cli_lifecycle.py` | Scoped SIGINT/SIGTERM translation into existing cleanup paths |
| `cli.py`, `__main__.py` | Command options, input/output checks, invocation, export, and user-facing errors |

The implemented boundaries are:

```text
load_csv(path: Path, *, symbol: str, price_basis: Literal["raw"]) -> MarketDataset
run_backtest(config: BacktestConfig, dataset: MarketDataset,
             *, strategy: Strategy | None = None) -> BacktestResult
Strategy.on_close(history: CloseHistory, portfolio: Portfolio) -> TargetIntent | None
execute_intent(intent: TargetIntent, execution_session: date,
               open_price: int, portfolio: Portfolio,
               fee_bps: int) -> tuple[Portfolio, OrderOutcome]
calculate_metrics(initial_cash: int, equity: Sequence[EquityPoint],
                  fills: Sequence[Fill] = ()) -> Metrics
serialize_result(result: BacktestResult) -> str
write_result(result: BacktestResult, path: Path, *, overwrite: bool = False) -> None
```

`run_backtest` consumes already validated immutable data and configuration. It
does not load files, save results, schedule jobs, use randomness, or collect
wall-clock metadata. Each invocation owns its portfolio, pending intent,
observed-close buffer, fills, outcomes, and equity path. Loaded data can be reused.

With `strategy=None`, the engine creates a fresh SMA instance for every call.
With `strategy=...`, the caller owns that instance and must provide fresh or
reset state for each simulation. The engine does not clone or reset it. An exact
built-in `MovingAverageCrossover` instance must have configuration matching
`BacktestConfig.strategy`; a mismatch is rejected. A matching instance retains
the supported SMA identity and parameters for export.

Arbitrary custom hooks, including subclasses with different behavior, may
implement the protocol for in-memory reference simulations. Their results are
simulation-only: semantic JSON export refuses unsupported custom identity until
a full provenance contract is designed. The CLI exposes only the built-in SMA.

A programmatic call using the included input is:

```python
from pathlib import Path

from distributed_backtester.data import load_csv
from distributed_backtester.models import BacktestConfig, StrategyConfig
from distributed_backtester.money import parse_money
from distributed_backtester.simulation import run_backtest

dataset = load_csv(Path("examples/sma.csv"), symbol="SYNTH", price_basis="raw")
config = BacktestConfig(
    strategy=StrategyConfig(fast_window=2, slow_window=3),
    initial_cash=parse_money("1000"),
    fee_bps=10,
)
result = run_backtest(config, dataset)
```

## Chronology and the strategy protocol

For each supplied bar, the engine:

1. Executes the prior close's pending intent at this bar's open.
2. Records `cash + holdings * close` and the associated cash, holdings, and close.
3. Appends this session's date and close to the observed buffer.
4. Calls `on_close` with a bounded history view and the current immutable portfolio.
5. Validates and retains an emitted intent for the next supplied open.

There is no pending order on the first bar. A valid intent emitted on the final
bar is recorded as `canceled` with reason `end_of_data`. Final holdings remain
open and are marked at the final close, without an implicit sale or sale fee.

`CloseHistory` is a read-only sequence of `CloseObservation(session, close)`.
It exposes neither future observations nor the full OHLCV dataset through its
supported API. Each view captures its length, so a previously retained view
cannot grow when later closes are appended. Creating a view does not copy the
entire prefix; slices return tuples of the requested observations. Python private
attributes and caller-supplied custom code are not a security sandbox.

A `TargetIntent` contains target `flat` or `invested`, a decision session, and
a positive integer sequence. The simulation requires the decision date to equal
the current close's session and accepted sequences to increase. Execution also
requires the decision date to precede the execution date. Invalid targets,
sequences, or timing produce explicit rejections. No intent can legitimately
fill using its own session's open or close.

If a rejected intent's decision date is malformed rather than a valid `date`,
its diagnostic JSON field is `null`. A valid date is formatted as ISO text.
This safe diagnostic does not make arbitrary custom results exportable.

Causal verification must distinguish information used for a signal from
information used for execution. Changing a future suffix must leave earlier
completed decisions and fills unchanged. Changing the next open can change fill
price and affordable quantity while leaving the preceding signal unchanged.
Truncating input may change a terminal cancellation, so compare completed events
within the shared prefix. Actual verification belongs in the phase report.

## SMA target semantics

`StrategyConfig` requires integer windows with `1 <= fast_window < slow_window`.
The target starts `flat`. Before `slow_window` closes, there is no intent. Once
ready, the strategy compares the latest window sums exactly:

```text
comparison = sum_fast * slow_window - sum_slow * fast_window
positive: invested
negative: flat
zero:     preserve the previous target
```

This implements an SMA target regime. The first ready positive comparison can
enter immediately, without a prior ready negative comparison. Equality emits
nothing. A strict comparison emits an intent only if its target differs from the
remembered target; repeated invested states do not rebalance existing holdings.

The remembered target changes when the signal is emitted, independently of
whether execution succeeds. An entry rejected for insufficient cash is therefore
not retried on every later positive comparison. A new target change is required.
Custom strategies may emit other valid target sequences; they remain subject to
the engine's timing/sequence validation and execution's cash rules.

## Exact accounting, sizing, and fees

One USD equals 1,000,000 integer ledger units, called micro-USD or micro-dollars.
Prices, cash, trade notionals, fees, and equity use Python integers. Whole-share
holdings and nonnegative integer volume are separate quantities. Domain input
validation rejects booleans and other numeric types where an integer is required.

`parse_money` accepts ordinary finite decimal text, with optional sign and
surrounding whitespace, and converts it without binary floats or an ambient
Decimal context. Scientific notation is rejected. A value must be exactly
representable in micro-USD: `12.0000000` is accepted because its extra digits are
zero, while `0.0000001` is rejected rather than rounded. Initial cash is positive;
OHLC prices are positive; portfolio cash and holdings are nonnegative.

Fees accept integer basis points in `[0, 10000]`. One basis point is 0.01%, so
10 bps is 0.1%. For each actual fill:

```text
notional = quantity * open_price
fee = round_half_even(notional * fee_bps / 10000)  # to one micro-USD
buy cash change  = -(notional + fee)
sell cash change =  notional - fee
```

Integer division implements nearest rounding with ties to the even integer.
The fee rounds once on the complete fill notional, not once per share. There is
no fixed fee, minimum charge, or broker-specific cent rounding.

An invested intent with no holdings selects the largest integer `q >= 0` with
`q * open_price + fee(q * open_price) <= cash`. A binary search uses monotone
total cost and the final rounded fee. Quantity is chosen at the execution open,
so an overnight gap cannot create a purchase sized using an obsolete close.
Zero affordable shares produce `rejected / insufficient_cash`.

An invested intent while already holding shares produces
`no_change / already_invested`. A flat intent sells all holdings, or produces
`no_change / already_flat` if none exist. Rejected, canceled, and no-change
outcomes charge no fee. Every successful transition preserves `cash >= 0` and
`holdings >= 0`; every equity mark equals `cash + holdings * close`.

This is an idealized next-open cash-target model. It has no slippage, spread,
auction allocation, volume participation, partial fills, leverage, shorting,
fractional shares, or intraday execution.

## Independent reconciliation of examples/sma.csv

The [tracked synthetic CSV](../examples/sma.csv) contains these five sessions in
2026. Run it with fast 2, slow 3, $1,000 cash, and 10 bps:

| Session | Open | Close | Fast SMA | Slow SMA | Close decision | Open fill | Cash | Holdings | Close equity |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Jan 02 | 10 | 10 | Not ready | Not ready | None | None | 1000.000000 | 0 | 1000.000000 |
| Jan 05 | 11 | 11 | 10.5 | Not ready | None | None | 1000.000000 | 0 | 1000.000000 |
| Jan 06 | 12 | 12 | 11.5 | 11 | Invested, sequence 1 | None | 1000.000000 | 0 | 1000.000000 |
| Jan 07 | 12 | 9 | 10.5 | 32/3 | Flat, sequence 2 | Buy 83, fee 0.996000 | 3.004000 | 83 | 750.004000 |
| Jan 08 | 9 | 9 | 9 | 10 | None; remain flat | Sell 83, fee 0.747000 | 749.257000 | 0 | 749.257000 |

The SMA values in the table are explanatory arithmetic. The implementation
returns during warm-up and uses integer comparisons once both windows are ready.

Sequence 1 is decided at January 6's close and executes at January 7's $12 open.
83 shares require `996 + 0.996 = 996.996`; 84 require `1008 + 1.008 = 1009.008`
and are unaffordable. Cash is 3.004 and the $9 close marks equity at
`3.004 + 83 * 9 = 750.004`. Sequence 2 is decided after that close and executes
at January 8's $9 open, returning `747 - 0.747 = 746.253` to cash.

Independent cash and profit reconciliation is:

```text
final cash = 1000 - 996 - 0.996 + 747 - 0.747 = 749.257
profit     = 83 * (9 - 12) - 0.996 - 0.747    = -250.743
close equity path = [1000, 1000, 1000, 750.004, 749.257]
total fees = 1.743; fill count = 2; final holdings = 0
```

A negative result reflects the strategy buying at 12 and selling at 9 with fees.
It is compatible with a correct simulation. Correctness requires matching signals,
event order, constraints, and ledger arithmetic; profitability is not a software
acceptance criterion.

## Metrics and canonical JSON

Metrics use exact `fractions.Fraction` ratios over integer ledger values:

```text
total_return = (final_equity - initial_cash) / initial_cash
peak starts at initial_cash
maximum_drawdown = max((running_peak - close_equity) / running_peak)
```

The running peak includes initial capital and all observed close equity. This
captures losses below initial cash even before a new peak. `fill_count` counts
executed buy/sell fills, and `total_fees` sums their recorded fees. Annualized
return and Sharpe ratio are not implemented.

`result_to_dict` exports schema version 1, engine identity, settings and policies,
dataset metadata, ordered fills/outcomes/equity, final portfolio, and metrics.
Money is six-place decimal text; ratios are twelve-place decimal text rounded
half-even from the exact numerator/denominator. The example exports:

```json
{
  "fill_count": 2,
  "final_equity": "749.257000",
  "maximum_drawdown": "0.250743000000",
  "total_fees": "1.743000",
  "total_return": "-0.250743000000"
}
```

Those ratio strings mean -25.0743% return and 25.0743% drawdown, not
-0.250743% and 0.250743%. Money strings preserve the full ledger; a rounded ratio
string is a report representation of an exact internal fraction.

`serialize_result` uses sorted keys, two-space indentation, no NaN values, and
one trailing newline. The semantic JSON excludes source paths, timestamps,
process/worker IDs, and runtime measurements. For supported SMA results, given
identical validated input, configuration, installed engine, and fresh strategy
state, serialization is deterministic.

The built-in strategy ID is `sma-crossover-v1`, with its two windows exported as
parameters, including an explicitly supplied exact built-in SMA with matching
configuration. In-memory custom results identify the strategy's module-qualified
class name, but `result_to_dict`, `serialize_result`, and `write_result` refuse
unsupported custom strategy identities. There is no custom semantic JSON export
with null parameters. The SMA configuration in `BacktestConfig` does not describe
a custom strategy's parameters or hidden state, and a class name alone cannot
establish reproducibility. Full custom provenance remains a future design.

## Data validation and provenance

`load_csv` performs one `read_bytes()`, computes SHA-256 on that exact snapshot,
and parses the same snapshot. It decodes UTF-8 with optional BOM handling, but the
digest includes original bytes, including BOM and line endings. No second read
can accidentally hash a different revision of the file.

The header must contain exactly the six unique names
`date,open,high,low,close,volume`; column order may vary. Empty input, header-only
input, unknown/missing/duplicate columns, blank rows/fields, wrong field counts,
invalid UTF-8, and malformed CSV are rejected. Diagnostics identify source,
physical row, and field; a multiline CSV record uses its ending physical line.

Dates must be valid ASCII `YYYY-MM-DD` session labels, unique and strictly
increasing. They are dates, not timestamps. Prices must meet the exact positive
money rules and `low <= open <= high`, `low <= close <= high`. Volume must be
ASCII digits representing a nonnegative integer. The loader performs no sorting,
deduplication, imputation, forward filling, or price rounding.

Metadata records a nonempty symbol without surrounding whitespace, currency
`USD`, price basis `raw`, source SHA-256, and validation version 1. The symbol
labels the whole dataset; it is not a filter or download instruction.

The next session is the next supplied row. Without an exchange calendar, missing
trading sessions cannot be inferred reliably. Raw OHLC validation cannot prove
adjustment provenance or the absence of corporate actions. The caller must
establish a consistently raw interval with no splits/dividends and the intended
session sequence. Synthetic data supplies known assumptions. Adjusted closes
mixed with raw opens, corporate-action accounting, and claims about a complete
historical universe are outside Phase 1's economic validation.

## CLI and complete local-file publication

After installation, the two invocation forms are
`distributed-backtester backtest` and `python -m distributed_backtester backtest`.
The complete example command is:

```bash
.venv/bin/python -m distributed_backtester backtest \
  --data examples/sma.csv --symbol SYNTH --price-basis raw \
  --fast 2 --slow 3 --initial-cash 1000 --fee-bps 10 \
  --output artifacts/example.json
```

`--data`, `--symbol`, and `--price-basis raw` are required, either explicitly or
through `backtest --config`. Defaults are fast 2,
slow 3, initial cash 1000, and fee 0 bps. Positive windows require fast < slow;
cash must be positive exact micro-USD; fees are integers from 0 to 10000.
`--output` selects a JSON file, otherwise JSON goes to stdout.
`--overwrite` permits replacing that file. `backtest --help` explains options;
top-level `--version` reports installed package metadata; the working version is
`0.8.0`. Reinstall after a metadata change to update an existing environment. The
[beginner guide's option table](clear%20explanation.md#run-the-included-example)
explains each argument.

The CLI rejects resolved input/output path equality and existing same-file
aliases, including hard links. Configuration and CSV validation precede export.
Known validation/file errors and argument errors exit with status 2 and readable
diagnostics; successful invocation returns 0. A simulation failure must not be
represented as a successful empty result.

Phase 2 also exposes `distributed-backtester sweep` and
`python -m distributed_backtester sweep`. For the committed
[examples/sma.csv](../examples/sma.csv), a four-job process sweep is:

```bash
python -m distributed_backtester sweep \
  --data examples/sma.csv --symbol SYNTH --price-basis raw \
  --fast 1 2 --slow 3 4 --initial-cash 1000 --fee-bps 10 \
  --backend process --workers 2 --max-in-flight 4 \
  --output-dir artifacts/sweep-process
```

Use `--backend sequential --workers 1`, omit `--max-in-flight`, and choose a
different new directory for the reference run. Sweep fast/slow lists and
`--output-dir` are required, either explicitly or through `sweep --config`.
The default backend is sequential with one worker;
the process backend requires positive workers and a positive pending limit.
Every Cartesian pair must satisfy `1 <= fast < slow`. Any invalid pair rejects
the whole grid before directory creation. Duplicate configurations execute once,
with their original grid membership retained in the manifest.

The destination directory must not exist, including as an empty directory;
sweeps have no overwrite mode. Exit `0` means complete collection with all jobs
successful. Exit `1` means complete collection with structured job failures.
Argument/validation/file errors and reported fatal backend/protocol/export errors
exit `2`. Fatal incomplete collection does not publish `index.json`; partial
artifacts can remain. These semantics differ from an ordinary rejected trading
intent, which is part of a successful simulation's ledger.

`write_result` serializes first, creates parent directories as needed, and writes
a complete temporary sibling in the destination directory. It flushes and
`fsync`s the file before publication:

- Default: `os.link(temporary, destination)` atomically refuses an existing
  destination while publishing the complete file, on supporting filesystems.
- Overwrite: `os.replace(temporary, destination)` publishes the complete
  replacement. This is explicit replacement, without conflict detection between
  competing overwrite writers.
- Successful link/replacement is the publication commit point. Cleanup then
  attempts to remove the temporary sibling. A cleanup failure logs a warning;
  it does not turn an already published result into CLI exit status 2. Before
  publication, the original write/publication error remains the failure reason.

These classic operations avoid exposing partially written JSON at the destination.
They do not establish the Phase 4 durable job commit protocol described below. There is no directory
`fsync`, transactional lifecycle, crash recovery, distributed idempotency, or
network-filesystem guarantee. A crash can leave a temporary file and does not
have a promised recovery procedure. Hard-link publication requires filesystem
support; this implementation does not claim universal filesystem behavior.

## Release identity and reproducibility limits

For an installation of version `0.8.0`, the engine ID is
`distributed-backtester/0.8.0:daily-long-only-v1`. The release component comes
from installed package metadata; the suffix identifies the declared simulation
semantics. It is not a source-code hash. Editing code in an editable installation
can leave the release and engine ID unchanged, so that ID alone does not identify
an exact implementation revision.

The dataset byte digest identifies input bytes, not execution environment.
The standalone backtest JSON retains the Phase 1 provenance contract. Phase 2
job envelopes add `ImplementationIdentity(engine_id, source_sha256)` so editable
source changes affect identity even when the installed version is unchanged.

`source_fingerprint()` reads the installed package directory, independent of the
working directory. It hashes all `.py` files in sorted package-relative POSIX
name order. For each file it feeds SHA-256 an unsigned 8-byte big-endian filename
length, the UTF-8 filename, an unsigned 8-byte big-endian content length, and the
exact file bytes. Framing prevents ambiguous concatenations. Absolute installation
paths, non-Python files, and cache directories are excluded. Directory symlinks,
missing sources, and traversal/read failures are rejected. This fingerprints the
whole installed Python package, including orchestration and benchmark code.

Operational environment capture records Python implementation/version, OS/platform,
architecture, logical/available cores, every unique installed distribution
name/version pair, known BLAS/OpenMP thread controls, and Git revision, porcelain
status, and dirty state when available. Automatic Git capture recognizes this
package's source checkout; an unrelated enclosing repository is not used for a
wheel installation. Unavailable Git fields are `null`. These records supplement
the source hash but do not archive dirty file contents or reconstruct an environment
by themselves. The benchmark dependency snapshot is an exact version list, not a
universal solver or artifact-hash lock. Custom strategies still need explicit
parameter and state provenance before semantic export is supported.

## Implemented jobs, backends, and experiment collection

`JobSpec` is frozen and carries its validated `BacktestConfig`, `DatasetMetadata`,
`ImplementationIdentity`, schema version 1, and job ID. `create_job` hashes the
UTF-8 canonical payload: JSON with sorted keys, compact separators, and NaN
disallowed. Its inputs are:

- Schema version.
- Configuration: SMA ID, fast/slow windows, initial cash in integer micro-USD,
  money unit, and fee basis points.
- Dataset metadata: symbol, source-byte SHA-256, USD currency, raw price basis,
  and validation version.
- Implementation: installed release/engine semantics ID and installed-source SHA-256.

The lowercase SHA-256 job ID excludes paths, timestamps, backend, workers, and
runtime measurements. `JobSpec.from_dict` rejects unknown/missing fields and
unsupported schema/strategy/unit values, rebuilds validated domain records, and
verifies the supplied ID. Grid expansion retains requested membership in fast-list
then slow-list order, including duplicates, but returns unique jobs sorted by ID.
The experiment ID hashes the semantic manifest payload, including that membership;
changing requested membership can change the experiment ID without changing jobs.

`ExecutionBackend.iter_outcomes(jobs, dataset, implementation)` yields
`JobOutcome(job_id, result, failure)` with exactly one result or failure.
`execute_job` checks dataset metadata and implementation identity, then invokes
the same `run_backtest` core with a fresh built-in SMA. Ordinary `Exception`s
become `JobFailure(error_type, message)` records; `BaseException` is not swallowed.
Dataset and implementation mismatches also become structured job failures.

`SequentialBackend` validates the current installed identity and yields one job
at a time. `ProcessBackend(workers, max_in_flight=None)` creates a fresh
`ProcessPoolExecutor` using explicit `multiprocessing.get_context("spawn")`.
Its top-level initializer checks installed identity once per worker and retains
one immutable dataset copy in that process. Subsequent submissions carry small
job specifications, not the full bar table. The coordinator and each worker
still have separate dataset copies; there is no actual shared memory.

The process backend bounds submitted, unconsumed futures by `max_in_flight`,
defaulting to `2 * workers`. The bound includes running/queued jobs and completed
results awaiting consumption. It waits for `FIRST_COMPLETED`, checks each outcome
against its submitted ID, yields completed outcomes, and refills after the consumer
resumes. Completed futures retain results until their references are released.
On closure/error, it attempts to cancel pending futures and shuts down the pool;
already running jobs may finish during shutdown.

`run_experiment` loads the dataset once, fingerprints the implementation, and expands
the grid before `collect_experiment` creates a fresh directory. Collection checks
outcome IDs for unknowns/duplicates and each successful result for matching config,
dataset, engine, supported SMA identity, and exact supported result schema. Its files are:

| File | Contract |
| --- | --- |
| `manifest.json` | Canonical semantic dataset/implementation identity, sorted unique jobs, requested grid membership, and experiment ID |
| `results/<job_id>.json` | Incrementally published job envelope with status and complete semantic result or structured failure |
| `execution.json` | Operational backend/workers, configured and observed in-flight limits, elapsed time before finalization, source path, and environment |
| `index.json` | Final sorted compact outcomes with relative artifact paths, content SHA-256, metrics/failure, and submitted/succeeded/failed counts |

The collector exports each detailed outcome as it arrives and releases it,
retaining compact records. After every expected job has exactly one exported
outcome and backend shutdown completes, it writes the execution record and
publishes `index.json` last. A complete index may include ordinary job failures.
A broken pool, initializer failure, protocol violation such as missing/duplicate
outcomes, or export failure aborts the experiment without an index. Existing
partial artifacts are not resumed. The opt-in Phase 4 mode below provides retries and durable recovery.

Streaming export bounds retained detailed outcomes by backend buffering; it does
not make total memory constant. Job specifications, membership, and compact index
records use `O(jobs)` memory. Each simulation and detailed result/export uses
`O(bars)` memory, and process execution adds dataset copies and up to the bounded
number of retained detailed outcomes. Coordinator growth with job count and
worker dataset duplication require measurements before scaling claims.

Manifest, detailed artifacts, and index are semantic outputs independent of the
backend and completion order for identical inputs, membership, and installed
source. Operational fields belong in `execution.json` and benchmark observations.
Same-source backend comparison covers complete canonical semantic results,
including provenance, one artifact at a time.

**Phase 5 cross-format checks are a separate contract:** converting CSV
to Parquet changes source bytes, digest, and job identity. First establish equal
validated bars, configuration, and policy; then compare economic fields: fills,
order outcomes, equity, final portfolio, and metrics. Validate each artifact's
provenance and conversion lineage separately. This preserves complete equality
for backends using the same source artifact.

## Implemented local benchmark methodology

Install the optional sampling dependency and invoke the benchmark module directly:

```bash
python -m pip install --editable ".[benchmark]"
python -m distributed_backtester.benchmark run \
  --bars 1000 --jobs 16 --workers 2 4 8 --repetitions 5 \
  --output-dir artifacts/benchmark-example
```

These are illustrative workload inputs, not performance observations. The
runner always includes sequential execution and rejects worker counts above
available cores. The destination must be fresh. `workloads.py` generates exact
synthetic prices with a local seeded RNG, consecutive synthetic dates including
weekends, and prefix-stable rows. Its distinct configurations use bounded SMA
windows and cash increments after each window-pair batch, preserving the requested
unique job count. The workload record includes bars/jobs/seed, data digest, symbol,
configuration-generator identity, and fees.

Every warm-up and timed trial starts a fresh experiment process; process
configurations also create a fresh pool. Timing uses `perf_counter` from before
process creation through child exit, including
imports, CSV loading, job creation, pool startup, simulation, serialization,
collection, detailed export, execution/index finalization, and shutdown. Synthetic
CSV generation, the observer's own RSS, and artifact verification after child exit
are excluded. Operational reports also measure CSV loading, job preparation, backend iterator
advancement, detailed export, and first-outcome latency. The latter overlaps
backend time; those intervals cannot isolate worker CPU costs.
The sweep's `elapsed_before_finalization_seconds` has a narrower
boundary and is not the primary benchmark duration.

There is one warm-up per configuration, retained separately from comparison
summaries, followed by five repetitions in the example. Configuration order rotates
each repetition. Trials are labeled `process-cold / OS cache uncontrolled`;
a fresh process does not establish a cold filesystem cache. Raw observations,
stdout/stderr, per-trial artifacts, and environment/hardware records are retained.
`observations.json` records progress, including failed trials; a successful run
finalizes `report.json` with median/range, throughput, speedup, and efficiency
summaries. No particular speedup is implied by this machinery.

The runner checks the submitted unique job count against the requested workload.
`semantic_digest` requires an all-success index, reads each detailed artifact, and
verifies its SHA-256 against the index. It then hashes an 8-byte manifest length,
the manifest bytes, and the index bytes. Since the index contains the verified
artifact digests, equality covers manifest, index, and detailed
semantic outputs. The runner requires the same digest across warm-ups and timed
trials before completing the report, and validates each trial implementation
against the report fingerprint; `execution.json` is excluded.

Optional `psutil==7.2.2` sampling uses a nominal 50 ms interval
(`--interval-seconds 0.05`). Raw process-tree RSS readings cover the experiment
coordinator and its live
descendants, including worker/runtime children. Each sampling round sums readings
aligned approximately in time; peak tree RSS is the highest of those sums, not a
sum of independent per-process peaks. Coordinator RSS is reported separately.
Raw rounds record elapsed timestamps, RSS sums, successful process reads, and
vanished/denied observation counts. Actual intervals include sampling work.
Any access denial invalidates both reported peaks (`null`); an entirely unobserved
peak is also `null`. Raw sums can be partial when processes disappear or access is
denied. RSS can count shared pages repeatedly, and samples can miss short-lived children
or brief peaks. This is a sampled RSS sum, with no claim to unique physical memory.
Observer overhead is present consistently in trials.

The [benchmark results](benchmarks/phase-2/results.md) and
[Phase 2 report](phase-2-report.md) record measured observations and
verification evidence. See [benchmark methodology](benchmarking.md) for the wider
measurement contract. This explanation makes no measured performance claims.

## Implemented Ray execution

`backends/ray.py` implements the same `ExecutionBackend` iterator with a fixed
actor pool. Ray is imported only when nonempty Ray execution begins. The
`distributed` extra pins Ray 2.59.0; another version fails with explicit guidance.
Each actor reserves CPU/custom resources for its lifetime, validates
`current_implementation()` in its constructor, and holds one immutable dataset.
Its `execute` method calls the existing `execute_job`, which constructs fresh
strategy state per simulation. No simulation mechanics are duplicated.

The coordinator puts the dataset once and passes that ObjectRef as a top-level
constructor argument. Ray resolves it before constructor invocation and transfers
it to the destination node if necessary. Python dataclasses/tuples deserialize
into each actor's heap. This is not zero-copy data sharing. Readiness diagnostics
record worker PID, node ID, dataset/code digests, and completed-job counts.

Submissions are bounded by `max_in_flight`, default twice worker count. Calls are
assigned round robin; actor methods execute serially. The driver uses `ray.wait`
for a ready reference and verifies the returned outcome ID before yielding it.
The collector validates full result contracts, exports each artifact, and sorts
the compact final index. Idle actors can coexist with queues on busy actors;
SPREAD is a placement preference, not a load-balancing guarantee.

The default explicitly creates an owned `local` cluster, with integer CPU budget,
no GPU reservation, disabled dashboard, and configurable object-store capacity
(128 MiB by default). Fractional per-actor CPU quantities remain possible. Local CPU/custom capacity
is rounded up to whole units. Requests have at most four decimal places and
are at least 0.0001, so unsupported precision fails before artifact creation. Live
node capacity is checked per node across all requested resources; summing CPU
and custom resources independently could admit an impossible combination.
Readiness waits are bounded; individual jobs have no deadline yet.

An explicit GCS address or `auto` attaches to an existing cluster. An already
initialized driver is rejected. Actors and driver connection are owned by the
backend; an external cluster is owned by its administrator. Cleanup kills the
experiment actors and disconnects. Ray shutdown also stops an owned local cluster.
Actor restarts and task retries are explicitly zero. Ordinary engine errors are
structured job failures; infrastructure errors abort without a final index.

The pin also bounds one private API adapter: `_shutdown_ray` retains Ray's reaper
Popen across shutdown and closes its leaked stdin only after the process exits.
Direct strict-warning tests reproduced the leak in Ray 2.59.0. Closing a live
reaper pipe would trigger fate sharing, so the adapter checks exit first. This
adapter must be reviewed when updating Ray. Tests keep warnings as errors.

All nodes require compatible Python/Ray versions and identical project source.
Automatic deployment and Ray Client are absent. Operational diagnostics live in
`execution.json`, outside job identity and semantic equality. See
[ADR 0005](decisions/0005-ray-actor-execution.md) and
[cluster setup](ray-clusters.md) for ownership and deployment contracts.

## Ray measurement boundaries

Its object store does not remove serialization, transfer, eviction, or spill
costs. A local run cannot establish performance across physical machines.

The implemented local comparison uses `ray_benchmark.py` and `ray_measurement.py`.
It times fresh subprocesses including cluster creation and cleanup; warmed actor
pool throughput is a separate boundary not measured here. Exact artifact digests
and installed source identity are verified before accepting timings. Optional
detail-file discard happens after exit and verification, outside the timer.

Inventory ownership is established by a private process group, exact published
session path, and PID/creation-time identities. Process names classify roles but
never authorize cleanup. Workers and services outside the driver tree can be
included. Raw per-process RSS is retained without summing shared mappings. USS
is private resident memory only; aggregate values stay null when reads are
denied. On this Mac, self USS is available while the observer's owned child USS
reads are denied. Live object-store occupancy is not observed. Neither the
logical serialized dataset size nor reserved store capacity is physical usage.

Publication certifies actor identities only with independent original-driver
group or exact owned-session proof, plus creation-time/liveness revalidation.
New process-group admissions require a surviving original group member. A reused
PID/group cannot authorize cleanup. On this host, actual actors lacked these
group/session proofs, so they were not adopted by the observer. Primary samples
report missing actors and zero complete private-memory observations. Backend
actor-handle cleanup is separately validated; memory coverage is not established.

The parent captures the parsed CSV snapshot and complete canonical jobs before
the first trial. Every trial must match that manifest, implementation, expected
experiment ID and job set before establishing semantic equality or deleting details.
This prevents a changed first trial from silently becoming the input reference.

The [Phase 3 measurements](benchmarks/phase-3/results.md) use a clean source
revision, three timed repetitions and one excluded warmup per configuration.
All 56 trials succeeded. Cold Ray was slower on both workloads; two processes
gave a 1.466x speedup for the 64-job, 4,096-bar workload. No warm-cluster or
multiple-machine speedup is inferred.

Ray measurements must preserve the declared end-to-end timing boundary and
identical successful job sets/output detail. Fresh-process and reused-pool
measurements need distinct declared boundaries, with raw samples, failures, and
exact environment records retained.

For a future multiple-machine benchmark, sample every declared node's drivers, workers,
runtime services, and object-store observations, including processes outside the
driver's child tree. Align timestamped node samples and report clock skew,
missing data, and alignment uncertainty. Never sum independent process or node
peaks as if they occurred simultaneously.

Report object-store used bytes/capacity separately, avoiding double-counting
shared mappings already included in RSS. RSS sums can count shared pages more
than once and miss brief peaks; declare those limitations. The
[benchmark methodology](benchmarking.md) defines the planned measurement scope.
The five-bar example and test duration do not establish throughput or speedup.
The current owned-local harness does not implement remote sampling or clock alignment.

## Analytical storage

Phase 4 scheduling and its limits are described below.
Phase 5 provides DuckDB analysis over verified complete outcomes. Its import
transaction is separate from the scheduler's authoritative SQLite commit.

See [architecture](architecture.md), [roadmap](roadmap.md), and
[testing strategy](testing.md) for the future contracts and phase checkpoints.


## Phase 4: durable scheduling

The asynchronous `AttemptRuntime` boundary exposes start, submit, bounded poll,
cancel, and close. A shared actor pool retains the Phase 3 resource and ownership
rules. Durable mode limits active attempts to the lesser of workers and requested
in-flight capacity, with one active call per actor. A failed or timed-out actor is
retired. Its replacement is created on demand and must finish within the new
attempt's deadline. Ray task retries and actor restarts remain zero.

The scheduler's `RetryPolicy`, `AttemptSpec`, and `AttemptReply` are immutable
validated contracts. The coordinator owns monotonic deadlines and retry decisions.
`Journal` owns transactional state and canonical terminal artifact bytes. Shared
`experiment_artifacts.py` produces the same successful manifest, details, and index
as classic collection. Operational attempts, clocks, and generations stay outside
semantic identity.

SQLite WAL with synchronous FULL is the authoritative persistence boundary. A
terminal transition and full result content commit in one transaction. The job ID
is a primary key in committed results. There is no independent file/metadata commit
gap: JSON is rebuilt from the journal afterward. File publication fsyncs each file
and parent directory, with the index last. A failure before publication completes
can leave committed calculations that need export repair rather than recomputation.

The writer holds a POSIX flock for the full session. Resume first verifies schema,
manifest, dataset metadata, source fingerprint, and committed digests. It then
advances the generation and reconciles abandoned running attempts against the
persisted retry budget. Status uses a separate read-only snapshot. The generation,
attempt number, and job ID fence stale leases. Identical duplicate commits receive
an acknowledgment; conflicting content for the same completed attempt is rejected.

This is bounded retry orchestration with at-least-once computation as the intended
model and at most one committed terminal outcome within one journal. A finite
budget can be exhausted before an attempt starts. The journal is not replicated,
and flock does not make coordinators highly available across machines. Local
process-kill evidence does not establish arbitrary power-loss, network-partition,
or disk-loss recovery. Full artifact duplication and one transactional writer are
explicit storage and throughput tradeoffs, with no new performance claim.

See [reliability](reliability.md), [ADR 0006](decisions/0006-durable-scheduling.md),
and the [Phase 4 report](phase-4-report.md) for commands and actual verification.

## Phase 5: exact storage and analytical snapshots

### Storage boundary

`load_dataset` selects a parser before passing a frozen `MarketDataset` to the
unchanged engine. CSV metadata serializes to its original five keys. Parquet
adds `source_format`, `dataset_version`, `parent_source_sha256`, and `bars_sha256`.
All three added hashes are mandatory together; unknown or partial metadata is
rejected by domain records and strict job decoding.

Parquet uses exactly six nonnullable columns: `date` as Arrow `date32`, OHLC as
signed `int64` micro-USD, and volume as signed `int64`. No inferred float or
timestamp conversion is accepted. The reader checks provenance, row count,
global ordering, bar domain constraints, and the canonical normalized bar digest.
It decodes the same bytes used for physical SHA-256, avoiding a separate
hash/read race. Arrow's page checksums supplement domain and digest checks.

The logical version hashes a canonical descriptor containing schema version,
symbol, USD/raw policy, validation version, micro-USD unit, original CSV digest,
normalized bar digest, and row count. Each normalized row hashes canonical JSON
`[date, open, high, low, close, volume]` followed by LF. Compression and row-group
layout are excluded from this descriptor. The physical source digest still
participates in jobs and manifests, so recompression creates new jobs even when
the logical version stays the same. Expected-version checking is optional at
load time; durable resume also requires exact source and implementation identity.

Conversion validates the int64 range before creating destination parents. It
writes configurable bounded row groups to a sibling temporary file, closes and
fsyncs it, then publishes with no-overwrite linking or explicit replacement.
Cleanup failure is logged without obscuring the primary operation's outcome.
This does not claim directory-fsync or arbitrary power-loss durability.

### Explicit cache

`DatasetCache(max_entries=1)` is an entry-bounded LRU, keyed by selected format,
symbol, and SHA-256 of current bytes. Each request validates arguments and rereads
and hashes the file. A hit reuses immutable decoded bars and avoids parsing and
domain validation; it never trusts only path, size, or timestamp. Expected-version
checking also applies to hits. There is no process-global cache, cross-worker
shared heap, byte budget, or persistent cache service.

### Verified snapshots and DuckDB

`read_experiment_snapshot` validates the canonical experiment definition and all
terminal outcomes. Classic mode verifies index membership and streamed detailed
artifacts. Durable mode uses a read-only consistent SQLite transaction and the
journal's existing invariants. A present journal takes priority over exports;
corrupt authority cannot be replaced by a convenient JSON fallback. Import does
not acquire the coordinator's writer lock, resume work, or change journal schema.

Detailed validation reconciles declared fills with initial cash, whole-share
target sizing, rounded fees, and every recorded cash/holdings transition. Fill
dates must match the next supplied equity session. Orders have strict field
domains and status/reason rules, with one matching order per fill and no orphan
or duplicate links. Nullable malformed-intent diagnostics remain valid for
explicit rejections. This checks internal consistency, without rerunning the
strategy or independently verifying a vendor's open prices.
An ordered walk reserves known intent closes, then assigns unknown immediate
diagnostics to their earliest feasible unused closes between recorded events.
Invalid-timing diagnostics need a possible emitting close different from their
declared date. An exhaustive small-calendar reference checks this matcher.

Snapshot identity hashes canonical `{experiment_id, outcomes}` where outcomes
are sorted `{job_id, content_sha256}` pairs. Input definition identity and terminal
result identity are distinct: the same experiment can produce different terminal
failures in different runs. Identical snapshots deduplicate; divergent snapshots
coexist without overwriting historical answers.

DuckDB stores experiment manifests, job definitions, full canonical artifact
bytes, compact records, snapshot membership, and separate canonical source
observations. Membership is unique per snapshot/job. Imports are transactional,
including verification of late artifacts, so errors cannot leave a partially
accepted snapshot. The adapter checks an owned schema/version and refuses
unrelated databases. Queries are read-only, bounded, parameterized, and sorted.
The owned catalogue includes custom macros/functions; built-in catalogue readers
are qualified to prevent a local macro from shadowing the checks. See the
[DuckDB catalogue reference](https://duckdb.org/docs/current/sql/meta/duckdb_table_functions).

Money and ratios retain their original exact strings. The `results_report` SQL
view adds `TRY_CAST` projections to bounded decimals (`DECIMAL(38,6)` for money,
`DECIMAL(38,12)` for ratios) and integer fill counts. Out-of-range projections are
explicitly null; raw text remains available. The CLI returns original metrics,
avoiding conversion through Python floating point. DuckDB is an analytical
projection, separate from the coordinator's authoritative result commit.

### Measurement and limits

The storage harness measures fresh-process load through child exit, repeated
in-process load, verified cache reuse, repeated full sequential experiments,
protocol-5 pickle serialization, and same-host spawned transfer. It records raw
repetitions, workload identities, bar/economic digests, file/payload sizes,
environment, and own-process high-water RSS. Serialization uses the existing
Python object representation; Parquet does not make workers share memory.

Fresh-process measurements include native imports and setup. Filesystem cache
state is uncontrolled. High-water RSS is not retained dataset size or aggregate
cluster memory. Spawned transfer includes process startup and is not a network
measurement. Default row groups contain 65,536 rows; files contain one asset's
history, without a multi-asset catalogue or fine-grained date partitions. Read
[storage instructions](storage.md), [ADR 0007](decisions/0007-versioned-storage-and-analytics.md),
and the [Phase 5 report](phase-5-report.md) for evidence and reproduction.


The [recorded Phase 5 storage measurements](benchmarks/phase-5/results.md) contain
108 verified observations. On the 65,536-bar workload, default Parquet reduced
file bytes from 4,054,324 to 2,056,839; complete four-job medians were 8.633964 s
(CSV) and 8.653589 s (Parquet). Warm-read and cache measurements are separate
operations, not substitutes for end-to-end experiment time. No CPU or allocation
bottleneck follows from those storage measurements alone. Phase 6 now provides
scoped CPU and live-allocation evidence; see the
[profiling and optimization section](#phase-6-profiling-and-measured-python-optimizations)
and [performance instructions](performance.md). Its final backend comparison
is complete.

## Phase 6: profiling and measured Python optimizations

Phase 6 provides standard-library profiling, uninstrumented scoped timing, and
verified comparison of retained fresh-process benchmark roots. Two focused
Python changes are accepted after independent review. Final local verification
recorded 1,397 tests passing in 139.41 seconds and full Ruff/format/mypy checks
passing. Final fresh-process matrices passed complete-result comparison.

| Phase 6 module | Responsibility |
| --- | --- |
| [`profiling.py`](../src/distributed_backtester/profiling.py) | Deterministic scoped CPU, traced-live-allocation, and timing reports with complete-result verification |
| [`performance.py`](../src/distributed_backtester/performance.py) | Cross-revision verification and recomputed summaries over retained `ray_benchmark` roots |
| `models.py` | Bounded native history slicing with custom-sequence/subclass fallback |
| `metrics.py` | Exact integer drawdown comparisons with final rational construction |

### Profiling API and module CLI

```python
profile_workload(
    output: Path, *,
    bars: int,
    jobs: int,
    kind: Literal["cpu", "memory", "timing"],
    scope: Literal["simulation", "experiment"] = "simulation",
    repetitions: int = 1,
    seed: int = 1729,
) -> dict[str, Any]

compare_benchmarks(
    baseline: Path, candidate: Path, output: Path
) -> dict[str, Any]
```

```bash
python -m distributed_backtester.profiling run \
  --bars 65536 --jobs 4 --seed 1729 \
  --kind cpu --scope simulation \
  --output-dir /private/tmp/phase6-technical-cpu-simulation

python -m distributed_backtester.profiling run \
  --bars 65536 --jobs 4 --seed 1729 \
  --kind memory --scope experiment \
  --output-dir /private/tmp/phase6-technical-memory-experiment

python -m distributed_backtester.profiling run \
  --bars 65536 --jobs 4 --seed 1729 \
  --kind timing --scope experiment --repetitions 5 \
  --output-dir /private/tmp/phase6-technical-timing-experiment
```

CPU and memory require one repetition; timing accepts positive repetitions.
The final output directory must be fresh and outside the source checkout and
installed package. The profiler intentionally rejects checkout-contained output.
Use absolute canonical private paths. With `tempfile.mkdtemp`, resolve the
created parent using `Path(...).resolve()` and pass a new child path to the tool.
Neither the package nor the generated CSV may change during a measurement.

| Scope | Measured work | Work outside the measurement |
| --- | --- | --- |
| `simulation` | Fresh sequential `run_backtest` calls; complete results retained through measurement | Synthetic generation, CSV load, independent result conversion/verification/hash, report publication |
| `experiment` | CSV load, job construction, sequential simulation, detailed export, finalization | Synthetic generation, independent complete-snapshot verification/result hash, report publication |

Each repetition uses the same interpreter. Startup/import time and process/Ray
execution are outside both scopes. Caches are uncontrolled; the profiling CLI
has no separately excluded warmup. Timing uses `perf_counter` with no cProfile
or tracemalloc instrumentation. Active caller instrumentation is rejected for
the relevant kind, including a trace callback for timing.

All kinds record `process_time` as `cpu_seconds` for this interpreter's process,
with the corresponding own-process scope. The RSS observation is
`resource.getrusage(RUSAGE_SELF).ru_maxrss` normalized where supported: an
own-process lifetime high water, not a workload-only peak or worker aggregate.

CPU instrumentation records raw `run-1.pstats` and functions with filename,
line, name, primitive/total calls, self seconds, and cumulative seconds.
Cumulative time includes subcalls. History access is nested in SMA evaluation,
and both are nested in the simulation. Those durations overlap and cannot be
summed; instrumented durations are not unprofiled throughput.

Memory instrumentation captures `current_traced_bytes` and `peak_traced_bytes`
before the final snapshot, plus before/after `live_differences` grouped by
traceback. Differences retain net `size_diff_bytes`/`count_diff` and the final
live size/count. They describe surviving traced Python allocations, not all
allocation traffic, native allocations, private memory, object-store usage,
or aggregate worker memory. Only the tracer started by this run is stopped.

Simulation retains all complete result objects for hashing after measurement,
but tracing starts after CSV loading. Experiment result retention follows the
existing sequential export/release path. Current-byte counts across these
scopes therefore answer different questions. RSS includes interpreter setup,
instrumentation, and earlier work; it cannot be treated as an operation delta.

### Verification and failure contract

Reports retain implementation/environment, method, workload/config/data
digests, raw runs, source/data guards, status, and complete-result digests.
Simulation hashes complete `result_to_dict` records in canonical config order.
Experiment consumes a validating complete snapshot, checks expected success,
identity, data/config membership, and hashes those same complete result records.

Every repetition must produce the same complete result; source and generated
data are rechecked outside the timed operation. A terminal failure attempts
atomic failed-report publication without claiming a successful result hash.
The primary exception survives instrumentation cleanup or report-write errors;
secondary diagnostics are retained where possible. An unwritable output can
prevent report publication. Known CLI validation/file/runtime errors return 2;
successful completion returns 0.

### Selected history and metric changes

For exact `CloseHistory` with exact built-in list/tuple backing, slicing uses:

```python
start, stop, step = index.indices(self._length)
indices = range(start, stop, step)
if not indices:
    return ()
end = None if step < 0 and stop == -1 else stop
return tuple(self._observations[start:end:step])
```

Normalization uses the view's captured prefix length, preserving negative
indices, strides, reverse slices, empty slices, and retained-view future bounds.
The before-zero reverse stop needs `None` for backing-sequence slicing; a
literal `-1` stop would be interpreted relative to that sequence's end.
Custom sequences, backing subclasses, and `CloseHistory` subclasses retain
`tuple(self[i] for i in indices)`. This is a focused fast path for built-in
operations, without changing the supported strategy boundary.

The evaluated generator alternative used the normalized bounded backing indices
and avoided repeated view indexing. It improved scoped timing over the original
but was slower than native slicing on the measured workload, so it was not
selected. "Native" refers to Python's built-in list/tuple operation; no C++
extension or new runtime dependency was introduced.

Drawdown retains a positive running peak anchored to initial cash and a best
numerator/denominator. For each validated equity point, use the exact comparison
`loss * best_denominator > best_numerator * peak`, retaining the winning pair.
Only the final maximum drawdown needs `Fraction` reduction. Total return still
uses `Fraction`, and the public rational/money formatting, per-point validation,
fee totals, and fill counts preserve their existing contracts.

### Scoped measurements and revision identities

Four variants, three kinds, and two scopes yield 24 reports at
`/private/tmp/phase6-evidence/{baseline,generator,native-history,optimized}-{cpu,memory,timing}-{simulation,experiment}/report.json`.
All record source unchanged per run and the same workload/full-result hashes.
The shared complete-result digest is
`ab780eb44724aa83ec754ab5c0552ee845bbde2a9486d090a5a8e09797dbbad9`.
The workload has 65,536 bars, four distinct generated jobs, seed 1729, 10 bps,
and `BENCH/raw`; each timing report retains five uninstrumented repetitions.

| Variant | Simulation median seconds | Sequential experiment median seconds |
| --- | --- | --- |
| Original algorithm | 0.975629291991936 | 1.903223999979673 |
| Bounded generator alternative | 0.8719195839948952 | 1.8493130419810768 |
| Native history slicing | 0.8051267090195324 | 1.7660370000230614 |
| Native slicing plus integer drawdown comparisons | 0.6699892920150887 | 1.6521307920047548 |

These medians use in-process scoped boundaries. They are not final
fresh-process sequential/process/Ray medians. Single traced-memory observations
show nearly unchanged retained/peak bytes, so there is no material retained-memory
or total-allocation reduction claim.

The accepted clean baseline is `58837ac10216dcc2379d0b7ef0f5f3779466e4f7`,
with package SHA-256
`ab0c29cffd82af10c03146da8182e025fa11d114c328c09a3bb0b06eb303a789`.
The accepted clean candidate is `df294d4788673e0180d0dc67715c955407a95372`,
with package SHA-256
`16a1a54602a060cea2fddaaae827b9aff99d87ccd5fbb6761a8b1731a5b834ad`,
matching the optimized scoped reports. Both wheels contain 42 package files,
use release `0.7.0`, and were installed offline into separate sites with package
bytes matching the archives. Retain build-commit/clean-state records separately;
automatic Git fields are unavailable for these wheel installations.

### Fresh-process cross-revision comparison

Use the same Python executable and dependency environment for both archived
wheels. Select their separate target sites via `PYTHONPATH`, and invoke
`python -m distributed_backtester.ray_benchmark run`. Both producer commands
must receive **absolute canonical output roots**. Use a fresh parent resolved
with `Path(tempfile.mkdtemp(dir="/private/tmp")).resolve()`, with new child roots.

The comparer rejects symlink parents, including macOS's `/tmp` alias, and checks
each retained trial command against its root. `ray_benchmark` accepts relative
paths for general use, but a root produced with relative captured commands is
not valid for this cross-revision comparison. Resolving only at comparison time
does not repair that command mismatch.

The first small pair is excluded pilot evidence at
`docs/benchmarks/phase-6/pilots/environment-preflight.json`. Ray import inserts its bundled
`thirdparty_files` path into `sys.path`, exposing `colorama==0.4.6` metadata
after the parent header capture. The comparer correctly rejected the resulting
parent/child distribution mismatch. This was an environment-record mismatch,
not evidence of an economic-output difference.

For controlled formal reruns, derive Ray's vendor path without importing Ray:
`Path(importlib.util.find_spec("ray").origin).resolve().parent / "thirdparty_files"`,
then resolve the directory. Include that identical directory on `PYTHONPATH`
alongside either immutable wheel site before all parent/child processes start.
Verify `capture_environment()["distributions"]` is identical before/after Ray
import for each site. No package source changes or dependency installs were
introduced for this control; the `ab0c29...` and `16a1a5...` package hashes and
accepted commits remain unchanged. Final evidence uses
`baseline-controlled`/`candidate-controlled` roots with absolute canonical
producer paths.

The formal matrix uses 256 bars/128 jobs and 16,384 bars/32 jobs, with seed 1729,
sequential plus process-2/4/8 and Ray-2/4/8. Each root has seven configurations,
one excluded warmup and three timed trials per configuration: 28 trials per
root. Both comparisons verified all 112 complete trials. The sequential median fell
18.30% for small and 30.20% for substantial inputs; substantial multiprocessing
was effectively unchanged and Ray medians increased 1.30-7.05%. These are local
observations with three samples/configuration and consecutive revision order,
not proof of improved parallel throughput.

The fresh-process timer includes startup/imports, data load, jobs, worker/owned
local cluster setup, placement/readiness, simulation, full export/finalization,
backend shutdown, and child exit. It excludes synthetic generation, parent
expected-manifest capture, post-exit validation and orphan cleanup. Memory
inventory excludes the observer; concurrent sampling can affect wall time. Rotating order and fresh processes do not establish a cold OS cache,
prestarted-cluster throughput, or multiple-machine performance.

Compare the retained roots with
`python -m distributed_backtester.performance compare --baseline ABS_BASELINE_ROOT --candidate ABS_CANDIDATE_ROOT --output ABS_COMPARISON_FILE`.
Use new canonical output outside both roots and the installed package.
Do not discard trial details; the comparer requires all warmup/timed artifacts.

Before publishing, comparison rereads input bytes, expected manifests, all
successful full snapshots, and artifact digests. It verifies workload/method,
repetition/config membership, resource policies, captured commands, producer
identity, and parent/child interpreter/dependency environment. Each producer's
implementation-dependent job and experiment IDs are independently validated.
Complete results are compared by canonical configuration; only a validated
engine release identifier is normalized when needed. All economic records,
dataset provenance, strategy/accounting policies, and detailed output must agree.

Summaries are recomputed from raw timed samples. Jobs/sec is jobs divided by
median duration; backend speedup uses that revision's sequential median;
parallel efficiency divides backend speedup by workers. Optimization speedup
compares baseline/candidate medians for the same configuration. These are
different ratios. Failed/incomplete, discarded, tampered, mismatched, or
semantically divergent inputs are refused.

The harness retains per-process RSS without summing shared mappings. Aggregate
private USS requires complete owned inventory and successful reads; missing
actor coverage or denied reads leave it null. Sampled peaks can miss brief
services, live object-store usage is unobserved, and reserved capacity or
serialized dataset size is not physical memory. Preserve unavailable coverage
rather than filling it with own-process profiler RSS.

See [performance instructions and raw evidence](performance.md) for exact
ranges, CPU/memory observations, canonical wheel/matrix reproduction commands,
and the completed backend table, plus
[ADR 0008](decisions/0008-measured-performance.md) for the accepted design.

## Phase 7: operational interfaces

**Phase 7 complete, version `0.8.0`.** These are the verified source
contracts. Final native, installed-wheel and Linux ARM64 runtime/dev checks
passed; the [completion report](phase-7-report.md) records the evidence.
Configured CI is not an observed CI result. Phase 8
was explicitly approved for final portfolio documentation. This section introduces no performance or memory measurements;
the Phase 6 evidence above retains its archived `0.7.0` source and commit IDs.

### Flat TOML defaults and command preparation

```python
load_configuration(path: Path, command: str) -> dict[str, object]
configuration_defaults(argv: Sequence[str]) -> tuple[str | None, dict[str, object]]
```

`configuration.py` uses standard-library `tomllib`. A document is a flat table
with required integer `schema_version = 1` and string
`command = "backtest"`, `"sweep"`, or `"resume"`, matching the invoked subcommand.
Nested sections, unknown keys, wrong types and unsupported schema versions are
errors. Version 1 is the configuration schema, independent of package `0.8.0`
and result/journal schemas. Only those three run commands accept `--config`.

Keys use CLI option names with underscores. Supported field groups are:

| Group | Keys | Commands |
| --- | --- | --- |
| Input and logging | `data`, `data_format`, `dataset_version`, `log_level` | backtest, sweep, resume |
| Simulation | `symbol`, `price_basis`, `fast`, `slow`, `initial_cash`, `fee_bps` | backtest, sweep |
| Single-result output | `output`, `overwrite` | backtest |
| Worker/Ray settings | `workers`, `max_in_flight`, `ray_address`, `cpus_per_worker`, `worker_resource`, `object_store_memory` | sweep, resume |
| Sweep scheduling/output | `backend`, `durable`, `max_attempts`, `job_timeout`, `retry_delay`, `output_dir` | sweep |
| Existing durable output | `output_dir` | resume |

Integers require exact integer types, excluding booleans. Backtest `fast`/`slow`
are integers; sweep windows are nonempty arrays of positive integers.
`overwrite`/`durable` require TOML booleans. `cpus_per_worker`, `job_timeout` and
`retry_delay` accept finite integers/floats and convert to floats. Other scalar
fields are strings, including `initial_cash`, which preserves decimal text for
`parse_money`. `worker_resource` is a nonempty string array of `NAME=QUANTITY`
entries. The existing backend constructor validates quantities, resource names
and capacity; money, windows, fees and retry policy use existing domain validators.

Choices are exact and case-sensitive: data format `auto|csv|parquet`, price basis
`raw`, backend `sequential|process|ray`, and log level
`OFF|ERROR|WARNING|INFO|DEBUG`. NaN/infinity and bool-as-int coercion are rejected.
Required run fields can come from the file or CLI; the loader itself requires
only the schema and command headers. Parsing/type/domain validation precedes
application output creation. The shell can independently create redirected files.

`data`, `output`, and `output_dir` in TOML resolve against the configuration
file's resolved parent. Explicit CLI paths retain working-directory semantics.
The configuration path itself is interpreted from the working directory.
Output/config aliases are rejected; existing input/output alias checks remain.
The bundled files beside `sma.csv` omit destinations so each invocation can
choose a fresh file or experiment directory.

`cli_options.py` prepares defaults before building the ordinary argparse parser.
Explicit scalar, path and window-list arguments replace file defaults.
`BooleanOptionalAction` provides `--overwrite`/`--no-overwrite` for backtest and
`--durable`/`--no-durable` for sweep, so explicit false can override TOML true.
Both default to false without configuration. Disabling durable mode does not
remove configured retry-only options; those are invalid when durable is false.
Durable sweeps require Ray; resume selects Ray and reads the saved policy rather
than accepting a new grid or retry policy.

`--worker-resource` uses `ExplicitAppend`: its first explicit occurrence replaces
the entire configured array, and subsequent occurrences append to that new CLI
list. With no explicit occurrence, the configured array is retained. Parsing
copies explicit values without mutating configuration/parser defaults, so reusing
the parser does not carry a previous invocation's resources into a later one.
Duplicate resource names are rejected. This follows the same explicit-value
override contract as the other configuration fields.

```bash
python -m distributed_backtester backtest --config examples/backtest.toml \
  --fee-bps 10 --no-overwrite --output artifacts/phase7-configured.json
python -m distributed_backtester sweep --config examples/sweep.toml \
  --backend process --workers 2 --max-in-flight 4 --fee-bps 10 \
  --no-durable --output-dir artifacts/phase7-configured-process
```

### Scoped logging and explicit worker context

`operational_logging.py` owns the dedicated
`distributed_backtester.operations` logger. `log_session(level)` temporarily
installs its JSON handler and sets level, propagation and disabled state, then
restores the caller's handlers and settings on every exit. It does not configure
the root logger. Context-variable tokens restore level/context in nested scopes.
Concurrent logger reconfiguration from different threads is unsupported.

Logging defaults to `OFF`. Opt-in records are JSON lines on stderr, with
`schema_version = 1`, a UTC `timestamp`, `level`, `event`, and process `pid`.
Applicable context includes `experiment_id`, `job_id`, `attempt_id`,
`generation`, and event-specific fields such as `attempt_generation` and
`attempt_number`. Context fields are JSON scalars with finite numbers; reserved
record names cannot be supplied as context. Severity thresholds filter events.
stdout retains existing result/summary JSON. A backtest using `--output` writes
the result file instead of a success document on stdout. Argument-parser
diagnostics and unrelated library messages are not part of this JSON schema.
Native Ray/Abseil can print failure diagnostics or stack traces on stderr beside
application records. Only the application's operational records follow the
JSON logging schema; the complete stderr stream is not guaranteed to be JSON.

```python
LogState(level: str, context: dict[str, object])
capture_logging() -> LogState
worker_session(state: LogState | None)
event_context(**values: object)
emit_event(event: str, *, level: str = "INFO", **values: object) -> None
```

`LogState` captures serializable settings, without handler or stream objects.
The process backend explicitly passes it to its spawn initializer, then recreates
a local logging session around job execution. Ray pool startup captures the
driver context and passes it to actor construction; durable execution adds
attempt context. OFF retains the original worker invocation shape. Neither
spawn nor Ray is assumed to inherit the driver's context variables or handlers.

Ray uses `log_to_driver=False`. Actor events stay in worker-local Ray session
logs. The driver emits readiness, collection/export, attempt claim/failure and
durable outcome observations through its own stderr. `result_committed` is
emitted only after `Journal.commit` returns; its disposition distinguishes a
new commit from an identical duplicate acknowledgment. A stale reply produces
`result_rejected`. These observations do not replace the journal's authoritative
transactions or establish final JSON publication.

Handler serialization/write/flush failures are swallowed as best-effort
delivery. A closed/full log destination must not change results or replace a
workload exception. Missing, delayed or reordered logs do not imply journal
state. Worker PIDs/node IDs are observational metadata outside economic/job
identity. Log timing is not performance evidence.

### Validated read-only status

```python
read_status(output: Path, *, job_id: str | None = None) -> dict[str, object]
```

`operational_status.py` implements the CLI status boundary. It starts no runtime,
creates no experiment, resumes no work and takes no coordinator writer lock.
The path must have no traversal or symlink components. Its source selection is:

| Source | Validation and published fields |
| --- | --- |
| Present `journal.sqlite3` | Existing read-only journal transaction validates saved state; preserves schema, experiment ID, generation, policy, publication flag, counts, jobs, attempts and events; adds `source_kind = "journal"` |
| No journal | Complete `read_experiment_snapshot` classic export, exhausting every detailed artifact and the final metadata recheck; reports `source_kind = "classic"`, publication complete, terminal jobs and aggregate counts |

A corrupt journal never falls back to files. Classic status checks that no
journal appeared during the read and that the source remained classic. Missing,
incomplete or corrupt classic exports fail explicitly. They are not resumable
or live scheduler snapshots. Classic counts have zero pending/running jobs.
Journal status can observe pending/running jobs while a coordinator is active;
the publication flag describes derived export completion, not worker liveness.

`--job-id` must be a lowercase 64-character SHA-256 digest belonging to the
experiment. Validation covers the whole source before filtering, including
unselected artifacts/state. Selection filters `jobs` and durable `attempts`;
durable `events` retain selected-job events and experiment-wide events with no
job ID. `selected_job_id` identifies the selection. `counts`, publication
metadata and any worker observations remain experiment-wide. Unknown membership
is an error rather than an empty successful selection.

Classic `execution.json` can contribute `worker_observations` with
`source = "execution.json"`, `kind = "last_observed"`, and retained workers.
This is historical readiness/identity metadata, not an alive/dead probe. The
journal status reader does not retain worker identities, so durable status omits
that field. A historical running attempt is not proof that its worker survives.

Read-only journal access can maintain SQLite's SHM or create/remove an empty
WAL. This is permitted bookkeeping, without changing authoritative results.
The smoke comparison still checks the database, coordinator lock, exports and
nonempty WAL, which can contain authoritative committed frames.

### Interruption, cleanup and durable recovery

`cli_lifecycle.py` installs scoped SIGINT/SIGTERM handlers around CLI execution
on the main thread and restores previous handlers even on failure. The first
signal raises `CommandInterrupted`, a `KeyboardInterrupt` subclass, using
`128 + signum`: controlled SIGTERM returns 143 and controlled SIGINT/Ctrl-C
returns 130. `SignalState` retains the signal when a native library translates
the exception into plain `KeyboardInterrupt`, preserving its exit mapping.
Ray initialization is wrapped to restore driver signal handlers afterward.
Further shutdown signals during this scope let cleanup continue. The first
signal arriving during owned cleanup is deferred until cleanup finishes;
secondary cleanup failures are retained as notes on the primary interruption.
A non-main-thread library caller keeps
the process-wide handlers unchanged. No IO or scheduler operation runs inside
the signal handler.

The CLI exit/cleanup contract includes startup. Ray 2.59.0 exposes no public
opt-out for its startup SIGTERM registration. The version-bound adapter in
`backends/ray.py` preserves the active main-thread CLI bridge through
`ray._private.utils.set_sigterm_handler`, delegates platform registration and
restores that private helper on every exit. Ordinary library calls and other
threads retain Ray's registration behavior. Review the adapter with the Ray
pin when upgrading dependencies.

Actual native probes during Ray signal registration and immediately after
CoreWorker construction returned 143, restored caller handlers and left Ray
disconnected. These checks exercise concrete startup windows; they do not
establish every timing interleaving or external-cluster behavior. Native failure
diagnostics can accompany application JSON on stderr.

Unwinding uses existing collector/runtime/journal cleanup: cancel pending
futures, close owned actors/connections, and release the coordinator lock.
Disconnecting from an external Ray cluster does not stop it. Process-pool exit
can wait for already-running jobs. Cleanup has no guaranteed time bound; the
CLI signal bridge introduces no cleanup deadline. SIGKILL bypasses Python
cleanup, and Docker can send it after its stop timeout. See the
[Docker stop behavior](https://docs.docker.com/reference/cli/docker/container/stop/).

Classic partial exports have no recovery guarantee. Durable resume requires the
complete local experiment directory and matching dataset bytes/metadata,
installed source fingerprint and engine identity. Use the same built image by
image ID/digest and the same input; a reused mutable tag is insufficient.
Changing package source, including operational modules, changes implementation
and job identity. Phase 6 `0.7.0` journals cannot be assumed compatible with
Phase 7 `0.8.0`. Resume advances generation, fences abandoned attempts and
preserves the saved retry budget under single-coordinator exclusion. Resource
settings may change. See [reliability](reliability.md) and
[container recovery procedures](operations.md).

### Installed Docker targets and dependency locks

All Docker stages share this immutable official Python index:

```text
python:3.12.15-slim-bookworm@sha256:34386ef0cb081344d7ec1c103ba398e6e9f64e9ab3a1509accc92a4e24a07258
```

The [official image metadata](https://raw.githubusercontent.com/docker-library/repo-info/master/repos/python/remote/3.12.15-slim-bookworm.md)
includes Linux amd64 and arm64. Matching builder/runtime stages build and
install the project wheel into `/opt/venv`; runtime includes the existing
benchmark/distributed/storage extras. Default `dev` adds development/build tools
and checkout fixtures under `/workspace`, using the same installed wheel with
no editable install or `PYTHONPATH=src`. Both run as UID/GID `10001:10001`, with
home `/home/backtester`, working directory `/outputs`, an exec-form
`distributed-backtester` entrypoint, and `STOPSIGNAL SIGTERM`.

`container-build.lock`, `container-runtime.lock` and `container-dev.lock` are
actual hash-enforced binary-wheel locks for CPython 3.12.15 on Debian bookworm
Linux amd64/arm64. They pin transitive versions and permitted SHA-256 artifacts;
the dev lock includes the runtime closure. Docker installs with
`--require-hashes`, then installs the project wheel with `--no-deps` to avoid
unpinned re-resolution. Earlier macOS benchmark snapshots are observed version
lists, not universal hash locks. Published metadata and resolved hashes are
separate from downloaded-wheel verification and successful Linux execution.

Mount inputs read-only and the output parent writable by the container UID.
Durable persistence needs the entire experiment tree, including journal WAL/SHM
and coordinator lock, on a filesystem meeting the existing local guarantees.
Use `--init` for forwarding/reaping. Example resource limits in the runbook are
starting settings, not measured capacity or memory minima. The container is a
CLI packaging/development boundary; it adds no API, dashboard or scheduler
service. Core wheel installations still need no runtime extras.

Final native verification passed 1,928 tests in 231.48 seconds, including five
actual operational native cases. Ruff checks passed with 147 files formatted,
mypy checked 102 source files and pip reported no broken requirements. The
isolated installed core helper returned `ok=true`.

Actual Linux ARM64 builds and installed runtime/dev checks used Python 3.12.15,
glibc 2.36 and UID 10001. The final runtime helper returned `ok=true` with
Ray/storage/signals; the dev installed-core helper returned `ok=true`, and its
Ruff, 143-file format, 102-source mypy and pip checks passed. The verified package
identity is:

```text
distributed-backtester/0.8.0:daily-long-only-v1
source_sha256=a3edda1731d8694fda5d6987e570e0cca365bbee22d18bbfc0dbd116003f618f
```

Final images retain that package identity:

```text
runtime=sha256:5932b2b6dd36792bfe563be5f00f17ffb771fe33829a807193211b7d13e3dc5e
dev=sha256:664b3494bfcafddbe94b311c0e3c85f7591074593944ba53dafb25f9c714297a
```

Actual Docker stop/INT/KILL drills returned 143/130/137 with OOM false. Each
same-image resume advanced generation 1 to 2, retained a unique committed result
per job and validated the derived exports. SIGKILL recovery uses the journal;
it does not run graceful cleanup. See the [Phase 7 report](phase-7-report.md) and
[verification record](phase-7-verification.json) for exact lifecycle evidence
and the images used for those drills.

Remote CI, Linux AMD64 execution, the complete Linux dev-image test suite,
Docker Desktop bind-filesystem durability, power-loss recovery and
external-cluster behavior remain unobserved. Published AMD64 lock/base support
is separate from an observed AMD64 run. Test/smoke durations add no performance
or memory claim. Consult
[development](development.md) for lock regeneration and required checks and
[operations](operations.md) for mounts, logs, status and recovery commands.

# Technical explanation

## Implemented scope and verification

The Phase 1 engine implements a deterministic daily backtester for one asset,
USD, whole shares, and long-only targets. Phase 2 adds immutable jobs, parameter
sweeps, sequential and local process backends, incremental detailed export, a
compact result index, and a benchmark runner. Phase 3 adds optional Ray actors.
Release `0.4.0` retains strict CSV
loading, the SMA strategy, next-open execution, exact accounting, return and
drawdown metrics, and versioned JSON export. Runtime dependencies in
`pyproject.toml` are empty. CSV parsing, integer arithmetic, `Fraction`, command
parsing, hashing, JSON, and process execution use Python's standard library.
The optional `benchmark` extra pins `psutil==7.2.2` for memory sampling.

The [Phase 1 plan](phase-1-plan.md) defines the engine scope, and the
[Phase 2 plan](phase-2-plan.md) defines local experiment execution. The
[Phase 1 verification report](phase-1-report.md) records the engine evidence;
the [Phase 2 report](phase-2-report.md) records the current commands,
checks, environment, packaging evidence, and limitations. This guide
describes implemented contracts; it does not infer verification from a plan or
quote a provisional test count. See the
[beginner guide](clear%20explanation.md) for setup and plain-language definitions.

Ray execution is implemented in Phase 3, with identical engine and job contracts.
Durable scheduling, retries, and databases remain later work. Phase 3 has no
recovery protocol or multiple-machine performance claim.

## Actual module boundaries

| Module | Implemented responsibility |
| --- | --- |
| `models.py` | Frozen domain records for bars, metadata, dataset, configuration, portfolio, intents, fills/outcomes, equity, metrics, and results; bounded `CloseHistory` views |
| `money.py` | Exact decimal-text parsing to integer micro-USD, integer half-even rounding, and fixed-place money/ratio formatting |
| `data.py` | Read and hash one CSV byte snapshot, validate it, and return immutable data |
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
| `workloads.py` | Deterministic synthetic CSV and distinct SMA benchmark configurations |
| `measurement.py` | Optional process-tree RSS sampling, fresh child-process timing, and host metadata |
| `benchmark.py` | Fresh-process trials, artifact verification, rotating repetitions, raw observations, and summaries |
| `cli.py`, `__main__.py` | Backtest/sweep options, input/output checks, invocation, export, and user-facing errors |

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

`--data`, `--symbol`, and `--price-basis raw` are required. Defaults are fast 2,
slow 3, initial cash 1000, and fee 0 bps. Positive windows require fast < slow;
cash must be positive exact micro-USD; fees are integers from 0 to 10000.
`--output` selects a JSON file, otherwise JSON goes to stdout.
`--overwrite` permits replacing that file. `backtest --help` explains options;
top-level `--version` reports the installed release, `0.4.0` for this version. The
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
`--output-dir` are required. The default backend is sequential with one worker;
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

These operations avoid exposing partially written JSON at the destination.
They do not establish a durable job commit protocol. There is no directory
`fsync`, transactional lifecycle, crash recovery, distributed idempotency, or
network-filesystem guarantee. A crash can leave a temporary file and does not
have a promised recovery procedure. Hard-link publication requires filesystem
support; this implementation does not claim universal filesystem behavior.

## Release identity and reproducibility limits

For an installation of release `0.4.0`, the engine ID is
`distributed-backtester/0.4.0:daily-long-only-v1`. The release component comes
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

`run_experiment` loads the CSV once, fingerprints the implementation, and expands
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
partial artifacts are not resumed. Retries and durable recovery belong to Phase 4.

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

**Phase 5 cross-format checks are a separate planned contract:** converting CSV
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

## Planned recovery and storage guarantees

**Phase 4, not implemented:** Lost acknowledgments may cause a calculation to
repeat. Planned scheduling permits at-least-once computation, with at most one
durable successful result per stable job ID inside one authoritative coordinator
and store. This is not an exactly-once execution claim.

A single coordinator will own lifecycle/commit decisions, with transactions,
unique commits, bounded retries, attempt identifiers, leases/fencing, and explicit
terminal failures. A single-writer SQLite journal is a candidate to review in
Phase 4. Workers submit outcomes to the coordinator. Identical duplicates can be
accepted safely; divergent content for one ID is a correctness error. Expired
attempts and still-running work require defined timeout handling.

The planned initial guarantee excludes coordinator high availability,
network-filesystem assumptions, and recovery after disk loss. Real failure
injection and persistent-transaction evidence must precede recovery claims.
**Phase 5, not implemented:** DuckDB provides analysis over committed outcomes;
it does not supply the scheduler's commit protocol by itself.

See [architecture](architecture.md), [roadmap](roadmap.md), and
[testing strategy](testing.md) for the future contracts and phase checkpoints.

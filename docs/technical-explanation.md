# Technical explanation

## Implemented scope and verification

Phase 1 implements a deterministic, single-process, daily backtester for one
asset, USD, whole shares, and long-only targets. Release `0.2.0` includes strict
CSV loading, an SMA strategy, next-open execution, exact accounting, return and
drawdown metrics, a CLI, and versioned JSON export. Runtime dependencies in
`pyproject.toml` are empty. CSV parsing, integer arithmetic, `Fraction`, command
parsing, hashing, and JSON use Python's standard library.

The [Phase 1 plan](phase-1-plan.md) defines the accepted scope. The
[Phase 1 verification report](phase-1-report.md) is the place for actual commands,
check results, environment, packaging evidence, and limitations. This guide
describes implemented contracts; it does not infer verification from a plan or
quote a provisional test count. See the
[beginner guide](clear%20explanation.md) for setup and plain-language definitions.

All experiment management, execution backends, multiprocessing, Ray, durable
scheduling, retries, databases, and benchmark machinery described later in this
document are **planned**, with approval required before Phase 2.

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
| `cli.py`, `__main__.py` | Options, configuration, input/output checks, loading, invocation, export, and user-facing errors |

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
top-level `--version` reports the installed release. The
[beginner guide's option table](clear%20explanation.md#run-the-included-example)
explains each argument.

The CLI rejects resolved input/output path equality and existing same-file
aliases, including hard links. Configuration and CSV validation precede export.
Known validation/file errors and argument errors exit with status 2 and readable
diagnostics; successful invocation returns 0. A simulation failure must not be
represented as a successful empty result.

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

For an installation of release `0.2.0`, the engine ID is
`distributed-backtester/0.2.0:daily-long-only-v1`. The release component comes
from installed package metadata; the suffix identifies the declared simulation
semantics. It is not a source-code hash. Editing code in an editable installation
can leave the release and engine ID unchanged, so that ID alone does not identify
an exact implementation revision.

The dataset byte digest identifies input bytes, not execution environment.
Phase 1 JSON does not record an exact repository revision or complete environment.
**Planned Phase 2** experiment and benchmark manifests must record exact source
revision, clean/dirty state and any local changes needed to reconstruct it,
Python/OS and relevant dependency versions, workload identity, and environment
settings. Direct development-tool pins do not constitute a complete transitive
environment lock. Custom strategy provenance needs its own explicit parameters
and state contract before being claimed reproducible.

## Planned parallel execution and result identity

**Phase 2, not implemented:** A sequential reference backend and an explicit
spawn-based `ProcessPoolExecutor` will call the same core. Each process will
initialize common immutable data once per sweep; jobs carry references/settings.
This still duplicates the dataset per process.

Bounded submissions alone do not bound memory retained from completed jobs.
Backends will stream outcomes to the experiment manager, which exports each
detailed result and releases it, retaining a compact outcome record. The final
index sorts compact records by stable job ID. Job metadata/index memory still
grows with job count, and one full result still grows with its bar count. Measure
coordinator memory as sweep size increases before claiming this design scales.

Planned stable job IDs hash canonical configuration, dataset byte digest and
policy, strategy/engine identity, and schema version. Paths, timestamps, workers,
and attempt IDs are excluded. Duplicate configurations collapse to one job;
the manifest retains their original grid membership. Exact revision/environment
records supplement these identities.

Same-source backend checks will compare complete canonical semantic results,
including provenance, against the sequential reference one artifact at a time.
**Phase 5 cross-format checks are a separate planned contract:** converting CSV
to Parquet changes source bytes, digest, and job identity. First establish equal
validated bars, configuration, and policy; then compare economic fields: fills,
order outcomes, equity, final portfolio, and metrics. Validate each artifact's
provenance and conversion lineage separately. This preserves complete equality
for backends using the same source artifact.

## Planned Ray execution and measurement

**Phase 3, not implemented:** Ray will be an optional adapter with compatibility
checked at adoption. Its object store does not make data shared across machines
without transfer, serialization, spill, eviction, and locality costs. A local Ray
run cannot establish performance across physical machines.

Planned end-to-end timing includes startup, loading, submission, serialization,
collection, detailed artifact export, compact index creation, and shutdown.
Fresh-process and reused-pool measurements need distinct declared boundaries.
Compare identical successful job sets and output detail, retaining raw samples,
failures, and exact environment records.

For local execution, peak memory is the highest sampled simultaneous RSS sum of
coordinator and workers. For Ray, sample every declared node's drivers, workers,
runtime services, and object-store observations, including processes outside the
driver's child tree. Align timestamped node samples and report clock skew,
missing data, and alignment uncertainty. Never sum independent process or node
peaks as if they occurred simultaneously.

Report object-store used bytes/capacity separately, avoiding double-counting
shared mappings already included in RSS. RSS sums can count shared pages more
than once and miss brief peaks; declare those limitations. The
[benchmark methodology](benchmarking.md) defines the planned measurement scope.
No throughput or speedup is established by Phase 1's example or test duration.

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

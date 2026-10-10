# Technical explanation

## Review of the agent-generated handoff

The phased roadmap and correctness requirements are a sound basis. The main
improvement is to turn broad goals into observable contracts and defer mechanisms
until they serve those contracts.

| Original direction | Refinement | Engineering reason |
| --- | --- | --- |
| Many logical components | One modular package; create modules as needed | Boundaries are useful without a service protocol for every responsibility |
| NumPy and pandas preferred | No runtime dependencies in Phase 0; evaluate numerical/tabular tools when useful | CSV parsing and exact accounting have straightforward standard-library solutions |
| Observe T close, fill T+1 open | Define prefix-only input and the full open/close/decision event order | A timing statement alone does not prevent strategy access to future bars |
| Available cash and fees | Choose maximum affordable whole shares at execution, including rounded fees | Close-priced sizing can overspend after an opening gap |
| Avoid floating-point inconsistencies | Exact decimal parsing and integer micro-dollar ledger | Explicit units and rounding make invariants testable |
| Consistent adjusted-price policy | Raw OHLC plus a caller-established interval without corporate actions initially | Corporate actions cannot be inferred or solved by changing one price column |
| Deterministic worker results | Separate semantic output from operational metadata; canonical identity/order | Completion time and worker identifiers vary with execution |
| Fault tolerance | At-least-once attempts with transactional unique commits in Phase 4 | Lost acknowledgment makes repeated computation unavoidable |
| Peak memory and scaling | Sample simultaneous coordinator/worker RSS; retain raw measurements | Parent-only memory or summed independent peaks misdescribe the workload |

These are proposed implementation decisions. Phase 0 implements the repository
foundation; the future simulator and execution backends are not present yet.

## Simulation as an independent calculation

The core boundary is `run_backtest(config, dataset) -> result`. It receives already
validated immutable data. All portfolio state, pending orders, and strategy state
belong to one invocation. It neither loads a CSV nor owns job scheduling or storage.

In Phase 1 the CLI calls that function directly. In Phase 2 a sequential backend
and a process worker call it. In Phase 3 a Ray task calls it. Scheduling changes
where a job runs; the simulation's economic rules stay in the same function.

This design supports strong backend equivalence tests. A mismatch in trades or
equity is a correctness failure even if the parallel version runs faster.

## Chronology and information availability

At each supplied session, fill the prior close's intent at the open, mark equity
at the close, and invoke the strategy with history ending at that close. Its next
intent is pending until the next supplied session. No pending order exists on the
first bar. A final intent is canceled; final holdings are valued without an
invented liquidation trade.

The strategy receives a prefix view, not a complete dataset and an index. That
limits accidental future access through an API, although Python is not a sandbox
for untrusted strategy code. Tests mutate future prices to check causal behavior.

For an SMA regime strategy, compare the fast and slow averages once the slow
window is ready. A positive comparison requests invested exposure; a negative
comparison requests flat; equality preserves the previous target. Initial target
is flat, and a positive first ready comparison can enter. State this convention
because a strategy requiring a previously observed strict crossover would enter
at different times. Repeated invested states do not rebalance existing holdings.

Integer price sums permit exact comparison without division:
`sum_fast * slow_window` versus `sum_slow * fast_window`. This removes an unnecessary
floating-point threshold ambiguity; it is not a measured optimization claim.

## Sizing, fees, and exact ledger

An invested target selects the largest integer `q >= 0` satisfying:

```text
q * next_open + fee(q * next_open) <= cash
```

The fee is proportional integer basis points, rounded half-even once per fill to
one micro-dollar. One dollar is 1,000,000 ledger units. Inputs are parsed exactly;
`price * 1_000_000` must be an integer. Trailing zeroes do not invalidate an exactly
representable price. A value like `0.0000001` is rejected instead of rounded.

Fees from 0 to 10,000 basis points are supported in the initial contract. This
model has no broker-specific cent quantum, fixed fee, or minimum fee. Cash change
on a buy is `-(q * open + fee)`. On a sell it is `q * open - fee`.
Unfilled intents charge no fee. An unaffordable entry is a recorded rejection,
and does not repeatedly retry unless a new strategy target change occurs.

The invariants are `cash >= 0`, `shares >= 0`, and
`equity = cash + shares * close`. Integer arithmetic avoids ambient decimal-context
changes and binary floating-point ledger drift. Decimal remains a valid alternative
with declared context/quantization. The chosen units trade general currency/precision
support for a small explicit model; neither approach has a proven speed advantage here.

The target instruction is decided at T's close; share sizing occurs at T+1's open.
This is an idealized cash-target execution model. It does not model broker timing,
auction allocation, spread, slippage, volume participation, or partial fills.

## Independent reference example

These are manual arithmetic expectations for hypothetical entry/exit intents,
not a backtest run or a claim about an SMA's signals on a particular dataset.

| Event | Notional and fee | Cash | Shares | Close equity |
| --- | --- | --- | --- | --- |
| T close $10; request invested | No fill | $1,000.000000 | 0 | $1,000.000000 |
| T+1 open $12; close $11; request flat | Buy 83: $996.000000 + $0.996000 | $3.004000 | 83 | $916.004000 |
| T+2 open $9 | Sell 83: $747.000000 - $0.747000 | $749.257000 | 0 | $749.257000 |

At 10 bps, 84 shares would require $1,009.008000, so they are unaffordable.
Independent reconciliation gives
`83 * (9 - 12) - 0.996 - 0.747 = -250.743` dollars.
Final return is `749.257 / 1000 - 1 = -0.250743`, or -25.0743%.
With the initial capital included as the equity anchor, maximum drawdown in this
path is 25.0743%. The final flat position makes close price irrelevant on T+2.

Total return and drawdown are ratios over exact integer ledger values. Export
money as six-place strings and these ratios as twelve-place strings with explicit
half-even rounding; canonical JSON has sorted keys. Operational timing is outside
that canonical semantic result.

## Data validity and economic validity

Reject malformed rows, missing fields, duplicate/unsorted session dates, invalid
volume, and incoherent/nonfinite prices. Never silently alter a simulation's input.
The source byte digest is calculated from the same byte snapshot that is parsed.

Session labels are dates rather than invented UTC timestamps. Without an exchange
calendar, "next session" means the next row, and omitted sessions remain a caller
responsibility. Dataset metadata declares symbol, USD, and raw price basis.

OHLC validation cannot detect all corporate actions or prove raw-price provenance.
The caller must establish an interval without splits/dividends for the initial
model. Splits require share-count changes; dividends require cash distributions.
Mixing adjusted closes with raw opens cannot establish consistent accounting.
An incomplete historical universe also cannot support a survivorship-bias claim.

## Parallel execution and result identity

Phase 2 will use spawn-based `ProcessPoolExecutor` workers, each initializing common
immutable data once for a sweep. Jobs carry configuration and data references.
This still duplicates dataset memory per process. Bounded submissions control the
number of queued futures and outputs. The backend streams outcomes to the experiment
manager, which exports each detailed result and retains a compact record. Keeping
all completed equity paths in memory would still scale as jobs times bars, despite
a bounded queue. Shared memory or file mapping needs measured
evidence and its own ownership/lifetime design.

A stable job ID hashes canonical configuration, data byte digest and policy,
strategy/engine identity, and schema version. File paths, worker IDs, attempt IDs,
and timestamps are excluded. Identical configurations deduplicate; the experiment
manifest preserves their original grid membership. Delivery order may vary; the
final compact index uses stable ID order. Same-source backend tests compare complete
semantic results against the sequential reference, one artifact at a time.

CSV-to-Parquet conversion changes artifact bytes and therefore the source hash and
job ID. Establish that bars, policy, and configuration agree, then compare economic
fields: fills, order outcomes, equity, final state, and metrics. Validate provenance
and conversion lineage separately. This does not relax full semantic equality for
backends running the same source artifact.

Ray is a later optional adapter, not a required dependency of the engine. Its object
store helps data placement, but separate machines still incur transfer, spill, and
locality costs. Local cluster tests establish narrower evidence than real multiple-
machine benchmarks. Version and Python compatibility must be checked in Phase 3.

## Recovery and analytical storage

Suppose a worker finishes but its acknowledgment is lost. The scheduler cannot
know whether the calculation finished, so it may assign another attempt. The
design permits repeated computation and prevents duplicate durable successes.

Phase 4 will use stable identity, bounded retries, leases/attempt fencing, unique
commits, and a real transactional store. A single-writer SQLite journal is a
candidate; that decision will be finalized in Phase 4. Workers submit to the
coordinator instead of racing to write an analytical database. Duplicate identical
results are safe; conflicting content for one job is a correctness error.

This is at-least-once execution with at most one committed success per job ID
inside one authoritative durable store. It does not establish coordinator high
availability or recovery after disk loss. DuckDB in Phase 5 provides analytical
queries over committed outcomes; it does not replace the commit protocol by itself.

## Measurement discipline

End-to-end runtime includes startup, loading, serialization, collection, per-job
artifact export, final index creation, and backend shutdown. Fresh-process and
warm-pool runs have different boundaries and must be
reported separately. Compare identical successful job sets and output detail.

Peak process-tree memory is the highest sampled simultaneous RSS sum, including
the coordinator and workers for local execution. Ray runs require declared per-node
coverage of drivers, workers, runtime services, and separate object-store observations,
including processes outside the driver's child tree. Align node sampling timestamps
and report alignment uncertainty. Shared pages may be double-counted, and brief peaks
may escape sampling; record those limitations. Do not sum individual process peaks
as if they occurred at once. Record environment, manifests, dependency snapshots,
all timing samples, and failures. Profile before optimizing.

The complete proposed contracts are in [architecture](architecture.md), validation
in [testing](testing.md), and measurement definitions in [benchmarking](benchmarking.md).

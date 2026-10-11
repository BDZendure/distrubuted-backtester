# CLI reference

Detailed command usage for version `0.8.0`. Start with the [README](../README.md)
or follow the [fresh-environment instructions](reproducibility.md). Benchmark
results below are historical, scoped measurements linked to their recorded versions.

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

For Ray execution, install the additional optional extra:

```bash
python -m pip install --editable ".[dev,benchmark,distributed]"
```

This pins Ray 2.59.0. A matching Python/OS wheel must be available. The current
Mac was verified with Python 3.14.6 and its macOS arm64 wheel. All cluster nodes
need compatible Python/Ray versions and identical installed project source.

## Run a backtest

For optional columnar inputs and a queryable results database:

```bash
python -m pip install --editable ".[dev,benchmark,storage]"
distributed-backtester convert-data --data examples/sma.csv --symbol SYNTH \
  --price-basis raw --output artifacts/sma.parquet
distributed-backtester backtest --data artifacts/sma.parquet --symbol SYNTH \
  --price-basis raw --fee-bps 10 --output artifacts/parquet-example.json
```

See [storage instructions](storage.md) for sweeps, imports, queries, version
checks, and the reproducible storage benchmark. The optional libraries are pinned
to PyArrow 25.0.1 and DuckDB 1.5.6. CSV works without either library.

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

### Configuration and observations

The bundled TOML file uses the adjacent synthetic CSV. Select an output yourself:

```bash
mkdir -p artifacts
distributed-backtester backtest --config examples/backtest.toml \
  --fee-bps 10 --output artifacts/configured-reference.json --log-level INFO \
  2> artifacts/reference-operations.jsonl
distributed-backtester sweep --config examples/sweep.toml \
  --backend process --workers 2 --fee-bps 10 \
  --output-dir artifacts/configured-process --log-level INFO \
  > artifacts/process-summary.json 2> artifacts/process-operations.jsonl
distributed-backtester status --output-dir artifacts/configured-process
```

Config paths are relative to the TOML file. Explicit CLI values replace file
defaults, including the first explicit worker-resource list; repeated explicit
resource options collect together. Explicit CLI paths retain current-directory
semantics. Unknown fields, wrong types and invalid domains fail before execution.
Successful result JSON remains on stdout or in the selected output file. Opt-in
application logs go to stderr; default logging is OFF. The JSON schema applies
to application records. Native Ray/Abseil failure traces can also appear on
stderr, and argparse usage errors remain human-readable. Observations do not
prove a result was committed.

Status validates the journal when present, otherwise every completed classic
artifact. `--job-id SHA256` selects one job while retaining experiment-wide
counts. Retained worker IDs describe previous observations, without asserting
current liveness. Partial classic exports require a new run; durable journals
support same-identity resume. See the runbook for Docker mounts and recovery.

The committed example is synthetic. With these settings, its SMA signal enters
after January 6's close and buys **83 shares at January 7's $12 open**. The buy fee
is **$0.996**, leaving **$3.004** cash. January 7's $9 close gives **$750.004** equity
and an exit signal. January 8 sells at **$9**, charges **$0.747**, and leaves
**$749.257**. Total return is **-25.0743%**, maximum drawdown **25.0743%**, and total
fees **$1.743**. The [phase report](phase-1-report.md) explains every equity point.
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

Each run requires a **new output directory**. Classic sweeps cannot resume. The durable mode below supports recovery from an
existing journal. Choose a new name for another classic run.

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

## Recoverable Ray experiments

On macOS/Linux, start a sweep with a local SQLite journal:

```bash
distributed-backtester sweep --data examples/sma.csv --symbol SYNTH \
  --price-basis raw --fast 1 2 --slow 3 4 --fee-bps 10 \
  --backend ray --workers 2 --durable --max-attempts 3 \
  --job-timeout 60 --retry-delay 0.1 --output-dir artifacts/durable-demo

distributed-backtester status --output-dir artifacts/durable-demo
distributed-backtester resume --data examples/sma.csv --workers 2 \
  --output-dir artifacts/durable-demo
```

The policy includes the first attempt. Resume requires identical CSV bytes and
installed source; completed jobs are not rerun. Worker settings may change.
One coordinator holds an exclusive lock. SQLite commits complete results and
terminal state together; resume repairs JSON exports from that journal. Calculations
can repeat, but each job has at most one committed terminal record per journal.
This is process crash recovery, not coordinator high availability or disk-loss
recovery. See [reliability and recovery](reliability.md),
[the scheduling ADR](decisions/0006-durable-scheduling.md), and
[Phase 4 evidence](phase-4-report.md).

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
See [methodology](benchmarking.md) and [recorded results](benchmarks/phase-2/results.md)
for the fixed substantial workload, variability, limitations, and exact commands.
The [version snapshot](../requirements/benchmark.lock) captures the observed macOS
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

## Run with Ray

```bash
distributed-backtester sweep --data examples/sma.csv --symbol SYNTH \
  --price-basis raw --fast 1 2 --slow 3 4 --fee-bps 10 \
  --backend ray --workers 2 --output-dir artifacts/sweep-ray
```

This starts a local Ray cluster and stops it after collection. Each actor checks
the installed source and receives the dataset once. Jobs use the same engine and
fresh strategy state. `--ray-address HOST:PORT` attaches to an existing cluster;
cleanup leaves that cluster running. Resource options and reproducible deployment
instructions are in [Ray clusters](ray-clusters.md).

Compare all three backends with a cold local benchmark:

```bash
python -m distributed_backtester.ray_benchmark run \
  --bars 4096 --jobs 64 --workers 2 4 8 --repetitions 3 \
  --discard-trial-results --output-dir artifacts/ray-comparison
```

Each configuration has one warmup and three timed fresh-process trials. Ray
trials include local cluster and actor startup, detailed result export, shutdown,
and process exit. Identical semantic output is verified before accepting a trial.
`--discard-trial-results` removes only newly generated detailed result files after
verification, retaining metadata, digests, logs, and raw observations. Omit it to
retain all artifacts. Memory inventory covers the owned local services and workers;
denied or unavailable private-memory reads produce null measurements. It does not
measure external cluster nodes. Use available worker counts on smaller machines.
See [benchmark methodology](benchmarking.md) for limits and scope.

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
python -m venv .venv-wheel-phase7
.venv-wheel-phase7/bin/python -m pip install --no-index --no-deps --force-reinstall \
  dist/distributed_backtester-0.8.0-py3-none-any.whl
wheel_python="$PWD/.venv-wheel-phase7/bin/python"
wheel_cli="$PWD/.venv-wheel-phase7/bin/distributed-backtester"
sample_data="$PWD/examples/sma.csv"
(cd /tmp && "$wheel_python" -I -c "import distributed_backtester")
(cd /tmp && "$wheel_cli" backtest --data "$sample_data" --symbol SYNTH \
  --price-basis raw --fee-bps 10)
```

These shell commands target macOS/Linux. Historical Phase 7 verification passed
1,928 tests in 231.48 seconds, including five operational native cases. Ruff
checks passed with 147 files formatted, mypy checked 102 source files, and pip
reported no broken requirements. The installed core helper returned `ok=true`.
These are correctness and release checks, not performance measurements.

Actual Linux ARM64 runtime/dev builds used Python 3.12.15, glibc 2.36 and the
installed wheel as UID 10001. The final runtime helper passed Ray, storage and
signal checks; the dev installed-core helper and static/dependency checks passed.
Actual Ray startup probes returned 143, restored caller handlers and disconnected
Ray. Docker stop/INT/KILL drills returned 143/130/137 without OOM; all resumed
generation 1 to 2 with unique committed results and validated exports. The
[Phase 7 report](phase-7-report.md) records source/image identities and
reproduction details.

GitHub Actions is configured for Linux Python 3.12 and 3.14. Remote CI, Linux
AMD64 execution, the complete Linux dev-image test suite, Docker Desktop
bind-filesystem durability, power-loss recovery and external-cluster behavior
remain unobserved. Phase 7 adds no performance or
memory result; historical Phase 6 evidence retains its archived source identities.

## Scope and limitations

The implemented application remains version `0.8.0`. Its portfolio guides
organize the verified engineering and recorded measurements.
Local measurements describe this host and
workloads; they do not establish cross-machine scaling or production readiness.

The initial execution model has no market impact, volume-based liquidity limits,
intraday execution, dividends, splits, leverage, short selling, or fractional
shares. Synthetic data establishes correctness. An incomplete real-data universe
cannot establish freedom from survivorship bias or investment performance.

The current engine has no exchange calendar or annualized/Sharpe conventions.
Durable recovery requires a local POSIX journal and one coordinator; it does not
provide replication or failover after disk loss. The analytical database does not
replace that journal. Local complete-file export requires filesystem hard
links/atomic replacement. Custom strategies are a Python testing/extension hook, with caller-owned
state; their results cannot be exported without supported parameter provenance.
The CLI runs the fully described SMA.


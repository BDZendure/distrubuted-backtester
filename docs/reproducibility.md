# Reproduce the system

Use Terminal or the VS Code terminal from a checkout. Commands below target
macOS/Linux with Python 3.12 or newer. A fresh virtual environment and fresh output
directories make the steps repeatable. The application version is `0.8.0`.

## 1. Run the dependency-free core

```bash
python3 -m venv .venv-core
.venv-core/bin/python -m pip install --editable .
.venv-core/bin/distributed-backtester --version
.venv-core/bin/distributed-backtester backtest \
  --config examples/backtest.toml --fee-bps 10
```

The included [synthetic CSV](../examples/sma.csv) requires no download or account.
With these settings, expect two fills, final equity `749.257000`, fees `1.743000`,
total return `-0.250743000000` and maximum drawdown `0.250743000000`.
These are decimal ratios, not percentages. This is a correctness reference,
not an investment result. See the [hand calculation](technical-explanation.md#independent-reconciliation-of-examplessmacsv).

Export a result and run four independent configurations:

```bash
mkdir -p artifacts
.venv-core/bin/distributed-backtester backtest \
  --config examples/backtest.toml --fee-bps 10 --output artifacts/reference.json
.venv-core/bin/distributed-backtester sweep --config examples/sweep.toml \
  --fee-bps 10 --backend process --workers 2 --output-dir artifacts/process
.venv-core/bin/distributed-backtester status --output-dir artifacts/process
```

Expect four submitted/successful jobs and no failures. An experiment needs a
new child directory: reruns should use another name. Inspect `manifest.json`,
`index.json`, `execution.json` and `results/JOB_ID.json`. The index is compact;
each detail file retains fills, decisions and equity. Execution clocks/worker
metadata are observations, while economic output is deterministic.

## 2. Run the full development checks

The existing extras add developer tools, memory observation, Ray and storage:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --editable '.[dev,benchmark,distributed,storage]'
PHASE5_STORAGE_CHILD_TESTS=1 .venv/bin/python -m pytest -q
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
.venv/bin/python -m mypy
.venv/bin/python -m pip check
.venv/bin/python -m build
```

The suite includes explicitly injected local worker/process failures and journal
recovery. It requires permission to start local processes/sockets and inspect
owned processes. Freeze package source during these tests: experiments and
workers validate the actual source fingerprint. The variable enables additional
storage child checks. A missing optional dependency causes its tests to skip;
use all extras for the full check and report skips separately from passes.

Native verification and GitHub Actions execution are separate. The configured
workflow covers Python 3.12/3.14 and installed Docker checks; remote CI has not
been observed here. Current results belong in the [Phase 8 report](phase-8-report.md).

## 3. Check the shipped core wheel outside the checkout

After building, preserve absolute paths before changing directories:

```bash
python3 -m venv .venv-wheel
.venv-wheel/bin/python -m pip install --no-index --no-deps \
  dist/distributed_backtester-0.8.0-py3-none-any.whl
wheel_python="$PWD/.venv-wheel/bin/python"
smoke_script="$PWD/scripts/smoke.py"
sample_data="$PWD/examples/sma.csv"
smoke_parent="$(mktemp -d /tmp/backtester-core-smoke.XXXXXX)"
cd "$smoke_parent"
"$wheel_python" -I -m pip check
"$wheel_python" -I "$smoke_script" --data "$sample_data" \
  --output-parent "$smoke_parent" --timeout 180 --signals
```

Expect a JSON report with `ok: true`. `-I` isolates Python from source-path
environment settings. The helper verifies WHEEL/RECORD hashes and actual imported
package paths, the reference calculation, configured paths, explicit overrides,
process parity, status, logs and signals. It retains per-command outputs and
`report.json` even on failure. Deadlines bound verification; they are not
benchmark thresholds.

## 4. Exercise actual Ray, durable recovery and storage

From the repository again, use a separate optional-wheel environment:

```bash
python3 -m venv .venv-runtime-wheel
.venv-runtime-wheel/bin/python -m pip install \
  'dist/distributed_backtester-0.8.0-py3-none-any.whl[benchmark,distributed,storage]'
runtime_python="$PWD/.venv-runtime-wheel/bin/python"
smoke_script="$PWD/scripts/smoke.py"
sample_data="$PWD/examples/sma.csv"
runtime_parent="$(mktemp -d /tmp/backtester-runtime-smoke.XXXXXX)"
cd "$runtime_parent"
"$runtime_python" -I -m pip check
"$runtime_python" -I "$smoke_script" --data "$sample_data" \
  --output-parent "$runtime_parent" --timeout 180 --ray --storage --signals
```

The helper starts real local Ray actors and durable attempts, verifies complete
semantic parity, signal recovery and generation fencing, converts Parquet and
imports/queries DuckDB. Ray needs compatible binary wheels, local sockets,
resource/process access and sufficient memory. See the
[failure demonstration](fault-tolerance-demo.md), [Ray guide](ray-clusters.md)
and [storage guide](storage.md). Platform-dependent optional installation must
actually succeed before claiming a complete run.

## 5. Use the reproducible container environment

See [development](development.md) for immutable base/hash locks and build commands,
and [operations](operations.md) for nonroot UID, read-only inputs, writable named
outputs, resource quotas, stop/kill/resume and retained artifacts. Linux ARM64
runtime/dev checks were observed in Phase 7. Linux AMD64 and remote Actions are
configured but unobserved. The named-volume recovery evidence does not establish
host bind-filesystem or power-loss guarantees.

## 6. Reproduce historical measurements

[Benchmark synthesis](benchmark-results.md) links each experiment's raw data,
workload, measured release, timer boundary and exact reproduction procedure.
Use the recorded historical revisions for before/after optimization comparisons.
Running a benchmark on current source is a new experiment and must report its own
identity; it cannot reproduce a historical revision by changing a version label.
Run one benchmark trial at a time without tests/profilers, retain complete details,
verify results, and disclose hardware and unavailable measurements.

For resume/start, keep dataset bytes, installed source and engine identity
unchanged. Operational settings may change, but retry budgets stay journaled.
Use the same built image or verified matching source fingerprint and retain the
whole experiment directory. See [identity contracts](decisions/0003-reproducibility-and-results.md).

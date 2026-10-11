# Phase 1 completion report

## Implemented

The approved Phase 1 delivers a working single-process daily backtester:

- Strict UTF-8 OHLCV CSV loading, immutable chronological bars, and SHA-256 of
  the exact parsed source bytes. No silent sorting, deduplication, or imputation.
- Configurable SMA target regimes using exact sums and only observed closes.
- Next-supplied-open fills, maximal affordable whole shares, explicit order
  outcomes, proportional fees, and exact cash/position/equity accounting.
- Total return, initial-capital-anchored maximum drawdown, fill count, fees, and
  final portfolio state.
- Installed `distributed-backtester backtest` and module CLI, canonical versioned
  JSON to stdout or a protected complete-file destination, and a tracked example.
- Unit, causal, hand-calculated integration, CLI, and packaging checks, with
  synchronized architecture, ADRs, roadmap, and both explanation documents.

Important files: `src/distributed_backtester/{models,money,data,strategy,execution,
simulation,metrics,results,cli}.py`, `__main__.py`, `tests/test_*.py`,
`examples/sma.csv`, `pyproject.toml`, `MANIFEST.in`, and `.github/workflows/ci.yml`.

The work is on `feat/phase-1-engine`; no remote push or Phase 2 implementation.
Foundation commit `dda83b9` adds contracts/data/strategy/accounting; `cb5d908`
adds simulation, CLI, export, example, and integration tests.
Follow-up commit `1eb07b5` fixes export cleanup, malformed diagnostics,
strategy provenance, and source-distribution test-package completeness.

## Architecture

`load_csv` handles input IO and validation. `run_backtest` receives validated
immutable data/config and produces a result; it does not load files or persist
results. `results.py` handles canonical serialization and local publication.
The CLI connects these boundaries. No runtime dependencies were added.

Each session executes the prior intent at the open, marks equity at the close,
then asks the strategy for its next target. The strategy gets a read-only view
whose captured prefix cannot grow when later observations arrive. It receives
frozen dates/closes and portfolio state, rather than the full future dataset.
Python private attributes do not provide a security sandbox for hostile code.

The default SMA instance is fresh on each call. SMA comparisons cross-multiply
integer sums, warm up through the slow window, start flat, and emit only changes
in target. Equality retains the regime. Execution uses a monotone binary search
for the largest quantity affordable after the final rounded fee. Cash and shares
are nonnegative immutable portfolio fields; equity is exactly cash plus shares
times close. Metrics use exact `Fraction` values.

JSON money has six places and ratio strings twelve, rounded half-even. Canonical
results exclude paths, clocks, and process information. `engine_id` records release
0.2.0 and daily semantics; it is not a source hash. Record the Git revision and
environment separately. Built-in SMA parameters are fully described; custom
Python strategy hooks are simulation-only until parameter provenance has a
supported contract; semantic export refuses them. A supplied built-in SMA must
match the reported configuration and retains its windows in output.

Output is written to a temporary sibling and flushed/fsynced before publication.
No-overwrite publication uses a hard link; explicit overwrite uses `os.replace`.
This prevents normal readers from seeing a partially written final JSON file.
Publication is the commit point; later cleanup failure logs a warning and does
not incorrectly mark the completed export as failed.
It is not a durable distributed result commit or recovery protocol.

## Verification

Local environment: macOS **26.6.1** arm64, Python **3.14.6**, pytest **9.1.1**, Ruff **0.17.0**,
mypy **2.4.0**, build **1.6.1**, setuptools **84.0.0**. Package version **0.2.0**.
The configured Linux/Python 3.12 and 3.14 CI matrix was not run remotely here.

| Exact command | Observed result |
| --- | --- |
| `.venv/bin/python -m pip install --no-build-isolation --no-deps --editable .` | Installed 0.2.0 with existing pinned build tooling |
| `.venv/bin/python -m pytest` | 344 passed |
| `.venv/bin/python -m ruff check .` | All checks passed |
| `.venv/bin/python -m ruff format --check .` | 38 files already formatted |
| `.venv/bin/python -m mypy` | No issues in 22 source files |
| `.venv/bin/python -m pip check` | No broken requirements |
| `.venv/bin/python -m build` | Wheel and source build succeeded after permitted network retry; initial restricted-network attempt could not resolve PyPI |
| `cmp artifacts/example.json artifacts/example-repeat.json` | Exit 0, identical bytes |
| `.venv/bin/python -m json.tool artifacts/example.json` | Valid JSON; inspected every fill and equity point |
| `git diff --check` | Exit 0, no whitespace errors |
| `.venv/bin/python -m venv .venv-wheel-phase1` | Created a fresh wheel-only environment |
| `.venv-wheel-phase1/bin/python -m pip install --no-index --no-deps dist/distributed_backtester-0.2.0-py3-none-any.whl` | Installed 0.2.0 without a package-index lookup |
| `.venv-wheel-phase1/bin/python -m pip check` | No broken requirements |
| `cmp artifacts/example.json artifacts/wheel-example.json` | Exit 0, wheel CLI output identical to editable output |

Both example commands in the next section were executed with `--overwrite` to
allow repeatable local validation. The first used the installed console entry
point; the second used the module entry point. Both exited 0.

Tests were written before their implementations. Missing-module red runs were
observed, and strategy behavioral tests also failed before completed behavior.
One export test initially omitted the sell fee from its independent expectation:
111 shares at $9 have $999 notional and $0.999 fee on each side, giving $1.998
total fees and $998.002 final cash. It was corrected with this arithmetic, rather
than changing the fee model or removing an assertion.

The suite covers malformed/empty data, dates/schema/precision/volume/OHLC,
warm-up/regimes/equality, fee ties and affordability, invalid orders and gaps,
terminal cancellation/retained holdings, deterministic repeat runs, and metric
references. Affordability additionally enumerates **2,928** bounded small cases
using independent exhaustive quantities and `Fraction` rounding. Causal tests
mutate future closes, change next-open prices, retain old prefix views, and truncate
input. Real CLI subprocesses check errors, output protection, source aliases
(same path, symlink, hard link), and execution outside the checkout.

Independent review reproduced three edge cases, then regression tests observed
the expected failures before fixes: cleanup after publication wrongly failed the
CLI, malformed rejected dates broke export, and supplied strategies could omit
actual parameters. The fixes preserve the publication commit point, normalize
malformed diagnostic types to null, validate supplied SMA configuration, and
refuse arbitrary custom exports without provenance. Failure injection changes
only the relevant cleanup/fsync/replace syscall; the remaining IO and simulation
are real. The source manifest also now includes `tests/__init__.py`, required for
unambiguous imported test modules in mypy.

Final independent review confirmed all three findings resolved, with **32 targeted
tests passing** and separate malformed-field, provenance, and publication probes.
No substantive issue remained in that focused scope. This complements, rather
than replaces, the full 344-test suite.

The fresh wheel's isolated import resolved inside its environment's `site-packages`,
and its installed console CLI ran from `/tmp`. The command in the wheel section
was also executed with `--output` pointing to the checkout's
`artifacts/wheel-example.json --overwrite`; the result matched the editable output.
Wheel Python modules match checkout bytes, `py.typed` and the console entry point
are included, and source-archive docs, example, and test files match checkout.
All **16 Markdown documents** and **50 local file links** were inspected; no em
dashes were found. The exact inspection script appears below.

Strict mypy was also run from the extracted source distribution:

```bash
(cd /tmp/backtester-phase1-sdist/distributed_backtester-0.2.0 && \
  /Users/blakezhang/Documents/Dev/distrubuted-backtester/.venv/bin/python -m mypy)
```

Observed result: no issues in 22 source files. This specifically validates that
the source distribution contains the test-package marker needed by imports.

## Performance

No throughput, memory, parallel scaling, or optimization benchmark was performed.
Test durations are not performance evidence. Phase 2 introduces reproducible
sequential/process baselines with startup/loading/export overhead and environment
capture. There are no claimed speedups or production-readiness claims.

## Known Limitations

- One asset, USD, whole shares, long-only; no leverage, borrowing, fractional
  shares, or explicit quantity-order API.
- Exact opening fills assume unconstrained liquidity; volume is validated but
  does not limit fills. No spread, slippage, market impact, partial fills, intraday
  events, or broker-specific fee quantum/minimum.
- Raw prices only. No split/dividend processing; caller must establish a suitable
  interval. Validation cannot establish provenance or eliminate survivorship bias.
- No exchange calendar: next session means next supplied row; missing calendar
  sessions are not inferred.
- No final forced sale, automatic retry of rejected entry, or daily rebalancing.
  No annualized return/Sharpe convention.
- Data, fills, and equity for one simulation are retained in memory. SMA sums
  traverse configured windows; no profiling-based optimization has been attempted.
- Custom hooks require caller-owned fresh state; their results cannot be exported
  without supported provenance. Release/semantics identity is not immutable
  source identity.
- Local filesystem hard-link/replace support is required for file export. No
  directory fsync, crash cleanup/recovery, hostile filesystem-race protection,
  job idempotency, retries, scheduler, database, or worker backend.
  A cleanup failure can leave a temporary sibling, with a warning; a published
  result still represents successful export.
- Local Python 3.12, remote CI, and multiple operating systems are unobserved.

## Reproduction Instructions

From the repository directory with Python 3.12 or newer:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --editable ".[dev]"
python -m pytest
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m pip check
python -m build
```

Run and compare the sample (add `--overwrite` if the files already exist):

```bash
.venv/bin/distributed-backtester backtest --data examples/sma.csv \
  --symbol SYNTH --price-basis raw --fast 2 --slow 3 \
  --initial-cash 1000 --fee-bps 10 --output artifacts/example.json --overwrite
.venv/bin/python -m distributed_backtester backtest --data examples/sma.csv \
  --symbol SYNTH --price-basis raw --fast 2 --slow 3 \
  --initial-cash 1000 --fee-bps 10 --output artifacts/example-repeat.json --overwrite
cmp artifacts/example.json artifacts/example-repeat.json
.venv/bin/python -m json.tool artifacts/example.json
```

### Sample trades and equity curve

The five synthetic sessions give these independently checked outcomes:

| Session | Open / close | Close decision | Cash after open | Shares | Close equity |
| --- | --- | --- | --- | --- | --- |
| 2026-01-02 | $10 / $10 | Warm-up | $1,000.000000 | 0 | $1,000.000000 |
| 2026-01-05 | $11 / $11 | Warm-up | $1,000.000000 | 0 | $1,000.000000 |
| 2026-01-06 | $12 / $12 | Invested | $1,000.000000 | 0 | $1,000.000000 |
| 2026-01-07 | $12 / $9 | Flat | $3.004000 | 83 | $750.004000 |
| 2026-01-08 | $9 / $9 | Remain flat | $749.257000 | 0 | $749.257000 |

January 6: fast SMA `(11 + 12)/2 = 11.5` exceeds slow `(10 + 11 + 12)/3 = 11`.
This decision does not fill that day. January 7 buys 83 shares: $996 notional plus
$0.996 fee. 84 shares would cost $1,009.008 and exceed cash. Closing equity is
`3.004 + 83 * 9 = 750.004`. Fast SMA `(12 + 9)/2 = 10.5` is below slow
`(11 + 12 + 9)/3 = 32/3`, so it requests flat exposure after that close.

January 8 sells 83 shares for $747 minus $0.747 fee. Final cash is
`3.004 + 747 - 0.747 = 749.257`. Independent P&L reconciliation:
`83 * (9 - 12) - 0.996 - 0.747 = -250.743` dollars. The curve is flat through
January 6, falls on January 7, then falls by the sell fee on January 8.
The initial $1,000 is the peak, so return is **-25.0743%** and maximum drawdown
**25.0743%**. There are **2 fills**, **$1.743 fees**, and **0 final shares**.
The exported ratio strings are `-0.250743000000` and `0.250743000000`.

Sample source SHA-256:
`b06a3a1bf428a3141064d4a1510fe3542889a1e564c6fa6345c11f91a5097c60`.
Changing line endings changes this digest even if parsed economic bars agree.
Artifacts under `artifacts/` are ignored local output, not tracked results.

Fresh wheel CLI reproduction:

```bash
.venv/bin/python -m venv .venv-wheel-phase1
.venv-wheel-phase1/bin/python -m pip install --no-index --no-deps --force-reinstall \
  dist/distributed_backtester-0.2.0-py3-none-any.whl
wheel_python="$PWD/.venv-wheel-phase1/bin/python"
wheel_cli="$PWD/.venv-wheel-phase1/bin/distributed-backtester"
sample_data="$PWD/examples/sma.csv"
(cd /tmp && "$wheel_python" -I -c "import distributed_backtester")
(cd /tmp && "$wheel_cli" backtest --data "$sample_data" --symbol SYNTH \
  --price-basis raw --fee-bps 10)
```

These commands target macOS/Linux. Build isolation needs access to the configured
package index; the local restricted sandbox initially prevented that access.

### Artifact and document inspection

This standard-library script was executed after the build. It compares packaged
source/docs/example/tests with checkout bytes, checks local links, and extracts
the trusted locally built archive for the additional mypy check above:

```bash
.venv/bin/python - <<'PY'
from pathlib import Path
import re
import tarfile
from urllib.parse import unquote
from zipfile import ZipFile

root = Path.cwd()
docs = [root / 'README.md', *sorted((root / 'docs').rglob('*.md'))]
links = 0
for document in docs:
    source = document.read_text()
    assert '\u2014' not in source, document
    for link in re.findall(r'\[[^\]]+\]\(([^\)]+)\)', source):
        if '://' in link or link.startswith('#'):
            continue
        target = unquote(link.split('#', 1)[0])
        assert (document.parent / target).exists(), (document, target)
        links += 1
with ZipFile('dist/distributed_backtester-0.2.0-py3-none-any.whl') as wheel:
    assert 'distributed_backtester/py.typed' in wheel.namelist()
    for path in (root / 'src/distributed_backtester').glob('*.py'):
        assert wheel.read(f'distributed_backtester/{path.name}') == path.read_bytes(), path
    entry_points = wheel.read('distributed_backtester-0.2.0.dist-info/entry_points.txt').decode()
    assert 'distributed-backtester = distributed_backtester.cli:main' in entry_points
with tarfile.open('dist/distributed_backtester-0.2.0.tar.gz') as archive:
    prefix = 'distributed_backtester-0.2.0/'
    for path in [*docs, root / 'examples/sma.csv', *sorted((root / 'tests').glob('*.py'))]:
        name = prefix + str(path.relative_to(root))
        stream = archive.extractfile(name)
        assert stream is not None, name
        assert stream.read() == path.read_bytes(), name
    archive.extractall('/tmp/backtester-phase1-sdist', filter='data')
print(f'Validated {len(docs)} Markdown documents and {links} local links; wheel source/typing/entry point and sdist docs/example/tests match checkout.')
PY
```

## Engineering Lessons

1. **Causality is an interface and event-order property.** Passing only observed
   closes and separating close decisions from next-open fills makes future-price
   use detectable. Causal mutation tests are stronger evidence than a plausible
   final return.
2. **Rounding belongs to the economic contract.** Fixed integer units and one
   declared fee-rounding boundary make exact reconciliation possible. Affordability
   must include the charged fee, especially after gaps.
3. **Separate signals from fills.** A target can be rejected or canceled. The
   signal regime and actual portfolio need not match; automatic retries would
   change the strategy's behavior.
4. **Keep calculation separate from orchestration.** One engine can become the
   sequential reference called by future process/Ray workers. Mutable state belongs
   to each invocation; immutable data can be reused.
5. **Reproducibility has several boundaries.** Deterministic economic output,
   source-byte provenance, source revision, and environment are related but distinct.
   Build/install tests check deployment boundaries; they do not prove economic validity.

## Next Phase

Phase 2 will add serializable job specifications, exact revision/environment
manifests, parameter sweeps, a sequential reference backend, and a spawn-based
process backend. Major challenges are startup/serialization overhead, one data
copy per process, bounded outstanding work, explicit per-job failures, and
incremental per-job export with a compact stable index. The simulation semantics
should remain unchanged. Benchmarks will compare small/substantial workloads and
supported worker counts, including total time and simultaneous process-tree memory.

## Approval Required

Phase 2 has not started. Do you want to proceed with Phase 2: parallel backtesting
on one machine?

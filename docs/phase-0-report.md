# Phase 0 completion report

## Implemented

The repository initially had one commit, an MIT license, and a clean working tree.
Phase 0 adds an architecture, all-phase roadmap, dependency plan, testing strategy,
benchmark methodology, three ADRs, and the requested learning documents:

- [Clear explanation](clear%20explanation.md) for beginners.
- [Technical explanation](technical-explanation.md) for engineering reasoning.

The scaffold includes `pyproject.toml`, a docstring-only typed package under `src/`,
one packaging smoke test, `.gitignore`, source-document inclusion via `MANIFEST.in`,
README installation/check instructions, and a GitHub Actions workflow. Work is on
the local `chore/phase-0-foundation` branch. The existing license is preserved.

## Architecture

Use one modular package. The future simulation consumes validated immutable data
and configuration, maintains private per-run state, and returns a semantic result.
Loading, scheduling, and persistence surround that function. Phase 1 calls it
directly; later backends run the same function on workers.

The agent-generated handoff was reviewed and refined:

- Stage dependencies according to need; no runtime dependencies are installed now.
- Keep strategy information limited to the current historical prefix.
- Declare exact daily event order and cash-constrained next-open target sizing.
- Propose exact micro-dollar accounting and an explicit micro-dollar fee quantum.
- Require raw-price provenance and caller-established absence of corporate actions;
  distinguish session ordering from calendar completeness.
- Separate semantic output from operational observations; define future stable IDs,
  transactional duplicate protection, and honest process-tree memory measurements.
- Stream later job outcomes to per-job artifacts and return a compact sorted index,
  rather than accumulating all full equity paths in coordinator memory.
- Define Ray measurement scope across node services/workers and distinguish
  cross-format economic equivalence from same-source provenance equality.

Phase 1 semantics and later distributed/persistence decisions are proposals, with
their status recorded in [ADRs](decisions/README.md). They are not implemented.

## Verification

Local environment: macOS 26.6.1, arm64, Python 3.14.6, pip 26.1.2.
Direct tool versions: setuptools 84.0.0, pytest 9.1.1, Ruff 0.17.0,
mypy 2.4.0, build 1.6.1. Commands below were run from the repository unless noted.

| Command | Actual result |
| --- | --- |
| `python3 -m venv .venv` | Environment created |
| `.venv/bin/python -m pip install pytest==9.1.1 ruff==0.17.0 mypy==2.4.0 build==1.6.1 setuptools==84.0.0` | Development/build tools installed |
| `.venv/bin/python -m pytest tests/test_packaging.py -q`, before package creation | Expected failure: package not installed, `ModuleNotFoundError`; one failed test |
| `.venv/bin/python -m pip install --editable '.[dev]'` | Editable package installed as version 0.1.0 |
| `.venv/bin/python -m pytest` | One test passed |
| `.venv/bin/python -m ruff check .` | All checks passed |
| `.venv/bin/python -m ruff format --check .` | Files already formatted |
| `.venv/bin/python -m mypy` | No issues in two Python source files |
| `.venv/bin/python -m pip check` | No broken requirements |
| `.venv/bin/python -m build` | Source distribution and wheel built successfully, including wheel build from source archive |
| `python3 -m venv .venv-wheel` | Fresh environment created |
| `.venv-wheel/bin/python -m pip install --no-deps dist/distributed_backtester-0.1.0-py3-none-any.whl` | Wheel installed successfully |

The isolated wheel demonstration command, run with `/tmp` as working directory:

```bash
/Users/blakezhang/Documents/Dev/distrubuted-backtester/.venv-wheel/bin/python -I -c 'import distributed_backtester; from importlib.metadata import version; print(version("distributed-backtester")); print(distributed_backtester.__file__)'
```

It printed `0.1.0` and the module path inside `.venv-wheel/lib/python3.14/site-packages/`.
This demonstrates a real wheel installation, independent of the checkout import path.

Additional inspection checked wheel metadata for the type marker, license,
Python >=3.12 declaration, and absence of runtime requirements. Independent Decimal
arithmetic checked the documentation's 83-share scenario: final cash $749.257,
return -25.0743%. This is a documentation calculation, not a simulation run.

The CI YAML was parsed with Ruby and its triggers/matrix inspected. The first
check exposed Ruby's YAML 1.1 interpretation of the unquoted `on` key as a boolean.
Quoting that key preserves the string and the same check passed:

```bash
ruby -ryaml -e 'workflow = YAML.load_file(".github/workflows/ci.yml"); abort "Invalid Python matrix" unless workflow.dig("jobs", "checks", "strategy", "matrix", "python-version") == ["3.12", "3.14"]; abort "Missing workflow triggers" unless workflow.key?("on"); puts "CI YAML parsed; Python matrix and triggers verified"'
```

The workflow uses read-only repository permissions and verified pinned checkout/
setup-python v6 revisions. Local parsing is not a remote GitHub Actions run.
The configured Linux Python 3.12/3.14 matrix has not been observed remotely;
Python 3.12 was not available locally. No backtest demonstration is possible yet.

Document inspection checked 14 Markdown files and confirmed that all 35 local
link targets exist. No em dashes or unresolved TODO/TBD/FIXME markers were found.
Source-archive inspection confirmed inclusion of all 13 Markdown design/learning
documents. `git diff --cached --check` passed for the staged Phase 0 files.
An independent read-only review found no packaging or CI defects. Its substantive
documentation findings about result accumulation, Ray memory scope, and cross-format
identity were incorporated into the architecture, roadmap, and validation methodology.

## Performance

No simulation performance was measured. No simulation exists yet. Tool execution
times and the packaging test duration are not backtesting benchmarks. The
[benchmark methodology](benchmarking.md) specifies future workloads, measurement
boundaries, raw samples, worker counts, memory accounting, and environment records.

## Known Limitations

- No engine, strategy, market-data loader, backtest CLI, result store, or backend.
- Passing packaging checks is not evidence of financial/simulation correctness.
- Only the local macOS/Python 3.14 environment was exercised; remote CI remains
  unobserved and no remote push was performed.
- Direct dependencies are pinned; transitive dependency resolution is not fully
  locked. A complete benchmark environment snapshot is required before performance
  comparisons in Phase 2.
- Proposed Phase 1 assumptions exclude corporate actions, liquidity constraints,
  market impact, intraday execution, leverage, shorting, and fractional shares.
- The price/date schema cannot establish adjustment provenance, omitted exchange
  sessions, or survivorship-bias freedom from incomplete real data.

## Reproduction Instructions

From the repository directory, with Python 3.12 or newer:

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
python -m venv .venv-wheel
.venv-wheel/bin/python -m pip install --no-deps dist/*.whl
wheel_python="$PWD/.venv-wheel/bin/python"
(cd /tmp && "$wheel_python" -I -c "import distributed_backtester")
git diff --check
```

These commands validate the current foundation. Read the
[architecture](architecture.md) and explanations to review the proposed engine.
There is no sample backtest command during Phase 0.

## Engineering Lessons

1. **Information availability is part of an API.** Restrict strategy input to the
   current prefix and use explicit next-open event ordering; plausible output
   alone cannot establish the absence of lookahead.
2. **Accounting needs declared units and rounding.** Exact money and fee rules
   make gap sizing, cash constraints, and reference calculations auditable.
3. **Execution location and simulation semantics are separate responsibilities.**
   Reusing one engine enables meaningful backend equivalence checks.
4. **Retries and commits solve different problems.** Repeated work can be safe
   when one authoritative store commits a unique result with stable identity.
5. **Measurements depend on their boundaries.** Startup/loading and all live
   workers affect end-to-end cost; parent-only memory is incomplete evidence.

## Next Phase

Phase 1 builds the complete deterministic single-process engine: strict CSV data
validation, SMA strategy, next-open execution, fees, exact portfolio tracking,
metrics, CLI, JSON export, synthetic fixtures, and correctness/reference tests.

Its main challenges are causal event ordering, precise affordable sizing, input
policy, terminal behavior, and independently checked outcomes. Review the proposed
contracts and modeling limits first. It will introduce no distributed infrastructure.

## Approval Required

Phase 0 stops here. Do you approve the proposed architecture and Phase 1 semantics,
and want to proceed with Phase 1? Successful checks do not authorize that work.

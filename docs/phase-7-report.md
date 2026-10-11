# Phase 7 completion report

Phase 7 is complete and locally verified on `feat/phase-7-operations`, version `0.8.0`.
Phase 8 remains unapproved. This report records observed execution; configured
remote CI is separate from local verification.

## Implemented

- Strict, command-specific TOML defaults and explicit CLI overrides.
- Scoped JSON observations with experiment/job/attempt correlation and worker
  propagation; successful command stdout retains its existing JSON contract.
- Fully validated classic/journal status, optional job selection and clear errors.
- SIGINT/SIGTERM ownership, startup preservation, exception-safe runtime cleanup,
  retained cleanup diagnostics and durable same-identity resume.
- Installed nonroot runtime/dev Docker targets, Linux binary hash locks, isolated
  wheel smoke, extended pinned CI and operational/development documentation.
- Beginner and technical explanations synchronized with the operational surface.

No runtime dependency, service, frontend, new scheduling guarantee or simulation
semantics was introduced. Core wheels remain dependency-free.

Important files: `configuration.py`, `operational_status.py`,
`operational_logging.py`, `cli_options.py`, `cli_lifecycle.py`, CLI/backend/worker
and coordinator integration, regression tests, `scripts/smoke.py`, Dockerfile,
Linux locks, CI, examples, and the guides linked below.

## Architecture

The journal remains authoritative for durable state. Observations follow actual
claim/commit APIs and do not constitute commit acknowledgements. Worker settings
are explicitly serialized; libraries retain caller logging/signal ownership.
Classic status consumes every artifact. Durable reads can maintain SQLite SHM
and an empty WAL; QA preserves DB/lock/export and nonempty WAL byte checks.

Configuration paths belong to the TOML directory; explicit CLI paths belong to
CWD. First explicit resources replace the configured list, and repeated explicit
resources collect together. Config aliases to mutable outputs are rejected.

The existing Ray 2.59.0 pin gates narrow startup-registration and reaper cleanup
adapters. First cleanup signals are deferred until owned cleanup finishes.
No time bound is promised. See [ADR 0009](decisions/0009-operational-interface.md),
[architecture](architecture.md), and [operations](operations.md).

Package identity is `distributed-backtester/0.8.0:daily-long-only-v1`, source SHA
`a3edda1731d8694fda5d6987e570e0cca365bbee22d18bbfc0dbd116003f618f`.
All 46 Python files plus `py.typed` matched the built wheel byte-for-byte.
Docker imports matched this identity under `/opt/venv` as UID 10001, with
root-owned installed code. Linux ARM64: Python 3.12.15, glibc 2.36.

## Verification

Commands ran from the checkout unless otherwise stated. Native source was frozen
through worker executions. The [verification record](phase-7-verification.json)
contains exact installed child commands, reports, tested image IDs, limits and
signal/recovery observations. Remote CI has not been executed here.

| Executed command or check | Observed result |
| --- | --- |
| `PHASE5_STORAGE_CHILD_TESTS=1 .venv/bin/python -m pytest -q` | Final frozen-source release run: 1,928 passed in 231.48 s; exit 0. |
| `.venv/bin/python -m pytest tests/test_operational_native.py -q` | 5 passed in 54.08 s: actual process/Ray parity and active durable signals/resume. |
| `.venv/bin/python -m ruff check .` | All checks passed. |
| `.venv/bin/python -m ruff format --check .` | 147 files already formatted. |
| `.venv/bin/python -m mypy` | No issues in 102 source files. |
| `.venv/bin/python -m pip check` | No broken requirements found. |
| `.venv/bin/python -m build --no-isolation --outdir /private/tmp/phase7-release-dist` | Final wheel and source archive built; 47 package files and 227 expected archive files byte-verified. |
| Fresh wheel installation with `--no-index --no-deps`, outside-checkout `python -I` configured reference | Exit 0; equity `749.257000`, fees `1.743000`, both detailed fills matched. |
| Final installed configured process sweep (`--workers 2`) and `status` | Exit 0; all four jobs succeeded and complete artifacts validated. Exact commands are in the verification record. |
| Installed core helper with `--timeout 180 --signals` | `ok=true`; real process execution, full parity, config/status and CLI signals. |
| Actual pinned Docker runtime/dev builds, Linux ARM64 | Both succeeded; pip archive hashes verified during installation. |
| Dev image Ruff/format/mypy/pip checks and installed core helper | All passed: 143 formatted files, 102 typed source files, installed helper `ok=true`; exit 0. |
| Final runtime helper with `--timeout 180 --ray --storage --signals` | `ok=true`; actual actors, journal recovery, Parquet and DuckDB; exit 0. |
| Actual `docker stop --timeout 30`, `docker kill --signal INT`, `docker kill --signal KILL` after persisted claims | Exits 143/130/137, OOM false. Each resumed generation 1 to 2, with one interrupted and one successful attempt and one committed job. |
| Closed WAL journal `status`, physically read-only versus writable output mount | Reproduced exit 2 with absent sidecars; corrected command exited 0 and matched the original completed journal snapshot exactly. |

The installed helper's exact core invocation was:

```bash
/private/tmp/phase7-release-wheel-env/bin/python -I \
  /Users/blakezhang/Documents/Dev/distrubuted-backtester/scripts/smoke.py \
  --data /Users/blakezhang/Documents/Dev/distrubuted-backtester/examples/sma.csv \
  --output-parent /private/tmp/phase7-release-wheel-smoke --timeout 180 --signals
```

Docker used `--init --cpus=2 --memory=4g --memory-swap=4g --shm-size=256m`,
read-only inputs, and named volume `phase7-outputs-20261011-1747`. The complete
runtime helper ran with `--ray --storage --signals`. Recovery drills used exact
image `sha256:38839087d89e2f3518f8317078508cfec7b106c2373d2789040bbd17045b147f`,
one 262,144-bar seeded synthetic job per signal, three attempts maximum, and a
180-second job deadline. Actual native startup probes inside Ray initialization
and immediately after native CoreWorker construction returned 143, restored
caller handlers, and disconnected Ray. Native failure diagnostics can accompany
application JSON on stderr.

The final tested runtime image is
`sha256:5932b2b6dd36792bfe563be5f00f17ffb771fe33829a807193211b7d13e3dc5e`;
dev is `sha256:664b3494bfcafddbe94b311c0e3c85f7591074593944ba53dafb25f9c714297a`.
They were built with `VCS_REF=0dab66a-dirty` from frozen package/helper bytes
before completion-document edits. Later rebuilds can have different IDs; the
record identifies the actual executed images. The three recovery exports were
independently read from a read-only volume and fully validated, with identical
index SHA `e3cfac40a84e8a2734168f640a29a47dbf3e61b4039790db5e8664f71c8fede1`.

Failures were investigated and retained as history. Initial execution signals
exposed native exception translation and vendor handler replacement. Independent
review reproduced eight CLI/lifecycle defects, each fixed with regressions first.
A complete run then had 1,904 passes and six pure fault-fixture failures: the
fixture lacked the new registration interface. The fixture was extended while
all pre-existing test definitions/assertions and the real-import guard remained
unchanged. Independent delta review accepted it; the 1,926-case repeat passed.
First Linux smoke rejected allowed SQLite sidecars. Four red bookkeeping cases
preceded a narrow exception for top-level SHM/empty WAL, with nine negative
controls retaining authoritative-write detection. Corrected Linux smoke passed.
The helper's launch-time Ctrl-C ownership follow-up was fixed with an actual
sleeping-child regression. Independent review passed all eight final signal
cases and all 13 bookkeeping cases; the final full suite and rebuilt images
passed on those helper bytes. No review findings remain open.
Final runbook review also found that physical read-only mounts can block SQLite
bookkeeping for a valid closed journal. An actual backed-up journal reproduced
exit 2. The two durable status commands now allow directory bookkeeping; the
same command with a writable mount returned 0 and the identical validated status.

## Performance

No Phase 7 optimization or throughput claim is made. Test/smoke durations are
execution observations, not benchmarks. Existing deterministic operation-count
regressions remain; no noisy CI timing threshold was added. Historical measured
[Phase 6 results](benchmarks/phase-6/results.md) retain their original identities.

## Known Limitations

- Remote GitHub Actions and Linux AMD64 execution have not been observed locally.
  The workflow is configured, not claimed to have run successfully.
- Graceful shutdown has no fixed bound. Process pools can wait for active jobs;
  forced termination relies on journal recovery and validated filesystem behavior.
- Local Docker-managed volume behavior was exercised. Docker Desktop bind-mount
  durability, power-loss recovery and external-cluster routing are not established.
- Logging is best effort. Ray worker logs remain in native session files, and
  native runtime diagnostics can appear on stderr. Worker observations are not
  current liveness. Partial classic exports cannot resume.
- Durable status needs output-directory write permission when SQLite must create
  WAL/SHM bookkeeping, although it does not change authoritative job/result state.
- Same-identity resume requires matching data/source/engine and the complete
  experiment directory. Older-phase journals cannot be silently upgraded.
- Concurrent logging reconfiguration is unsupported. Pins/private adapters and
  binary hash locks require deliberate review when upgrading dependencies.
- The daily single-asset, long-only execution and market-model limitations remain.

## Reproduction Instructions

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --editable '.[dev,benchmark,distributed,storage]'
PHASE5_STORAGE_CHILD_TESTS=1 .venv/bin/python -m pytest -q
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
.venv/bin/python -m mypy
.venv/bin/python -m pip check
mkdir -p artifacts
.venv/bin/distributed-backtester backtest --config examples/backtest.toml \
  --fee-bps 10 --output artifacts/phase7-reference.json --log-level INFO \
  2> artifacts/phase7-reference.jsonl
.venv/bin/distributed-backtester sweep --config examples/sweep.toml \
  --backend process --workers 2 --output-dir artifacts/phase7-process
.venv/bin/distributed-backtester status --output-dir artifacts/phase7-process
```

Use new output names on repeated runs. [Development](development.md) gives fresh
wheel and runtime/dev build commands; [operations](operations.md) gives mounted
smoke, stop/forced termination, resume and retained-output procedures. The
[beginner guide](clear%20explanation.md) explains these commands in plain language;
the [technical guide](technical-explanation.md) explains ownership and guarantees.

## Engineering Lessons

1. Operational logs are observations; the journal is the state authority.
2. Process ownership starts at creation, so interruption during launch must defer
   until the child is known and can be cleaned up.
3. Cleanup must retain the primary interruption while reporting secondary errors.
4. Read-only database behavior can include filesystem bookkeeping. Checks must
   distinguish it from authoritative committed writes.
5. An installed-wheel check must verify distribution records and actual imports;
   source proximity alone cannot prove the packaging boundary.

## Next Phase

Phase 8 organizes the verified architecture, scaling/failure demonstrations,
engineering deep dive, interview summary and evidence-based resume candidates.
It should explain the measured limits and the source identity tradeoff without
adding unsupported production-readiness or performance claims.

## Approval Required

Phase 7 stops here, as required by the roadmap. Do you approve proceeding to
Phase 8, final portfolio polish?

# Phase 4 implementation and verification report

Status: Implementation and pure/storage verification complete. Real Ray failure
validation is pending explicit user approval required by automatic approval review.
Phase 4 acceptance is incomplete; Phase 5 is not authorized.

## Implemented

An opt-in durable Ray sweep, read-only status, and resume commands. The scheduler
persists jobs, immutable retry policy, attempt history, generation, structured
failures, and complete terminal artifacts. It supports bounded retries, deadlines,
actor retirement/replacement, stale-lease fencing, duplicate commit protection,
and repair of derived JSON exports. Classic simulation and backend APIs remain.

## Architecture

One coordinator owns a local SQLite WAL journal and holds a POSIX writer lock.
Terminal state and full artifact content commit in one transaction. Workers send
attempt replies, never database writes. Resume verifies original data/source,
advances the coordinator generation, and reconciles interrupted leases. Retry
policy includes the first attempt and cannot be changed on resume.

The shared Ray actor pool preserves resource and ownership controls. Ray automatic
retries/restarts remain disabled. Successful semantic JSON matches the sequential
reference; operational retry history is stored separately. See
[ADR 0006](decisions/0006-durable-scheduling.md) and [reliability](reliability.md).

## Verification

The pre-change baseline was `.venv/bin/python -m pytest -q`: **695 passed in
70.17 seconds**, before Phase 4 changes. Final observed checks on macOS 26.6.1,
Apple M3 Pro, CPython 3.14.6, release 0.5.0:

| Command executed | Actual result |
| --- | --- |
| `.venv/bin/python -m pytest -q --ignore-glob='tests/test_ray*.py' --ignore=tests/test_scheduler_faults.py` | 715 passed in 7.23 seconds, with process-inspection access outside the sandbox |
| `.venv/bin/python -m pytest -q tests/test_ray_runtime.py tests/test_ray_backend.py tests/test_ray_faults.py tests/test_ray_cli.py -k 'not real and not refuses'` | 102 passed, 9 deselected in 0.43 seconds; pure transport boundary tests |
| `.venv/bin/python -m pytest -q tests/test_ray_benchmark.py tests/test_ray_measurement.py -k 'not real'` | 62 passed, 1 deselected in 2.82 seconds |
| `.venv/bin/python -m pytest -q tests/test_ray_measurement.py::test_real_subprocess_timing_includes_exit_and_preserves_arguments` | 1 passed in 0.16 seconds; ordinary Python subprocess, no Ray runtime |
| `.venv/bin/ruff check .` | Passed |
| `.venv/bin/ruff format --check .` | 94 files already formatted |
| `.venv/bin/python -m mypy` | No issues in 65 source files |
| `.venv/bin/python -m pip check` | No broken requirements |
| `.venv/bin/python -m build --no-isolation` | Wheel and source archive built successfully |
| `.venv/bin/python /tmp/phase4-package-check.py` | 33 package files match wheel; 100 repository files match source archive |
| `/tmp/phase4-core-wheel/bin/python -I /tmp/phase4-wheel-check.py "$PWD/examples/sma.csv" /tmp/phase4-final-core-wheel-results` | Four jobs match across sequential, process, and journal-derived exports; no Ray or psutil installed |
| `/tmp/phase4-core-wheel/bin/distributed-backtester backtest ... --fee-bps 10` | Sample final equity 749.257000, two fills, fees 1.743000 |
| `git diff --check` | Passed |

A collection-only run found 897 test cases. The four disjoint executed suites
passed 880 cases; 17 real Ray cases remain unexecuted. This is not a passing full
final suite.

The temporary verification scripts inspect exact archive bytes and install the
wheel outside the checkout. Portable wheel installation and demonstration commands
are in the README; the committed packaging and completed-resume tests cover these
boundaries. The wheel checks retained no invented performance values.

The tested source fingerprint is
`1052d7e4de199ea21c1c0e1900bbb64dc57b82e184b1b9ba67eb24a8d8e87c0d`.
The four-job wheel semantic digest is
`35e6d0d5058bcdfc8b99e517cb581af30236bfe327cc21acf233ba402b64e141`.
A prior wheel path assertion initially failed because macOS resolves `/tmp` to
`/private/tmp`; canonical-path verification then passed. An initial sandboxed
recursive RSS test lacked process-inspection coverage and passed when rerun with
that access. Neither issue was hidden by weakening project tests.

Independent reviews found and reproduced deadline/receipt rounding, mutable policy,
wrapped programming errors, late replacement readiness, hidden cleanup failures,
SQLite CLI errors, weak manifest digest comparison, and accumulated lease history.
Regression tests passed after fixes. Active leases now have a capacity bound;
historical duplicate/conflict checks resolve attempts through the journal.

Automatic approval review rejected the real fault-test command twice. Its stated
reason was that the source-freeze reports were insufficient trusted authorization
for intentional Ray worker crashes. A user approval request remains pending. No
real Ray crash test was run after that rejection, and no full final suite is claimed.
A sandboxed ordinary Ray CLI test also failed during startup with denied sysctl
access; it remains to be rerun with the approved real Ray verification.

| Required scenario | Observed status |
| --- | --- |
| Worker dies before a job starts | Real Ray injector written; execution pending |
| Worker dies during simulation | Injector terminates after two real strategy/engine close steps; execution pending |
| Result finishes but acknowledgment is lost | SQLite post-commit process kill passed; completed-worker/lost-reply Ray test pending |
| Job/result delivered more than once | Real journal uniqueness/divergence tests and pure transport tests passed |
| Job repeatedly fails | Bounded exhaustion in real journal/pure coordinator passed; real Ray case pending |
| Results partially written | SQLite kills during claim/success/failure transactions and derived export interruption/repair passed |
| Scheduler restarts | Journal reconciliation/fencing passed; real Ray driver restart and sequential parity pending |

The six real Ray cases collect successfully. The test injectors are explicitly
labeled and are not production simulation behavior. Process kills establish only
the recovery actually observed, not power-loss or multi-machine partition safety.

## Performance

No throughput or speedup improvement is claimed. This phase establishes recovery
behavior. Test durations are verification timings, not benchmark measurements.
Phase 3 comparisons remain historical measurements of the prior release.

## Known limitations

Durable mode uses Ray and a local POSIX journal, with one writer and no replication.
It does not provide coordinator high availability, Windows writer support, journal
recovery after disk loss, or network-filesystem safety. Process termination does
not establish power-loss safety. Actual multi-machine partitions remain untested.
Finite attempts can be exhausted before a job starts. Full artifacts are duplicated
in SQLite and JSON. There is no exactly-once calculation guarantee.

## Reproduction instructions

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --editable ".[dev,benchmark,distributed]"
python -m pytest -q
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m pip check
python -m build

distributed-backtester sweep --data examples/sma.csv --symbol SYNTH \
  --price-basis raw --fast 1 2 --slow 3 4 --fee-bps 10 \
  --backend ray --workers 2 --durable --max-attempts 3 \
  --job-timeout 60 --retry-delay 0.1 --output-dir artifacts/phase4-demo
distributed-backtester status --output-dir artifacts/phase4-demo
distributed-backtester resume --data examples/sma.csv --workers 2 \
  --output-dir artifacts/phase4-demo
```

Start requires a fresh directory. For focused failure injection, run
`python -m pytest -q tests/test_journal.py tests/test_coordinator.py tests/test_scheduler_faults.py`.
Package source must remain unchanged during real Ray tests and between start and
resume. A completed resume repairs exports and does not start workers.

## Engineering lessons

1. Execution and persistence have different guarantees: a calculation can repeat
   while a database uniqueness constraint prevents duplicate terminal records.
2. A lease needs an attempt number and generation so a restarted coordinator can
   reject old work without depending on message arrival order.
3. Atomic full-result transactions remove the gap between a result file and a
   separate lifecycle update. Derived files can be repaired afterward.
4. Timeouts require both monotonic measurement and action on still-running workers.
   A finite retry budget is a policy, not a guarantee of eventual success.
5. Failure injection must reach actual worker and storage boundaries. Process
   termination proves only the recovery behavior observed in those scenarios.

## Next phase

Phase 5 evaluates Parquet market data, dataset versioning, and DuckDB queries over
committed outcomes. Cross-format comparison must preserve price/accounting
semantics while recording changed source provenance. Storage/query performance
needs new measurements; DuckDB must not replace the journal commit protocol.

## Approval required

Automatic approval review requires explicit approval to execute bounded local Ray
failure tests that intentionally terminate test workers/coordinator processes.
Complete that verification before requesting approval for Phase 5. Do not treat
pure tests, successful packaging, or this implementation report as phase acceptance.

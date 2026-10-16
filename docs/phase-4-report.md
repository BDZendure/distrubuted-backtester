# Phase 4 completion report

Status: Phase 4 implemented and verified locally, including actual Ray failures and
coordinator restart. The complete suite passed all 897 tests. Phase 5 requires new
explicit approval.

## Implemented

An opt-in durable Ray sweep, read-only status, and resume commands. The scheduler
persists jobs, immutable retry policy, attempt history, generation, structured
failures, and complete terminal artifacts. It supports bounded retries, deadlines,
actor retirement/replacement, stale-lease fencing, duplicate commit protection,
and repair of derived JSON exports. Classic simulation and backend APIs remain.

Important files: `src/distributed_backtester/scheduler/` owns durable execution;
`experiment_artifacts.py` shares canonical artifact serialization; `cli.py` exposes
the commands; `.github/workflows/ci.yml` includes Ray failure tests. Journal,
coordinator, runtime, and actual fault tests accompany the implementation. The
beginner and technical explanations, reliability guide, ADR, architecture, and
roadmap describe the implementation and its limits.

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
| `.venv/bin/python -m pytest -q` | Final run: 897 passed in 105.18 seconds; complete suite, including actual local Ray processes and strengthened timeout assertions |
| `.venv/bin/python -m pytest -q tests/test_scheduler_faults.py tests/test_ray_cli.py` | 17 passed in 40.47 seconds; all six new actual Ray fault cases and CLI tests |
| `.venv/bin/python -m pytest -q tests/test_scheduler_faults.py -k timeout` | Strengthened timeout assertions: 1 passed, 5 deselected in 10.46 seconds |
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
| `/tmp/phase4-core-wheel/bin/python -I /tmp/phase4-wheel-check.py /Users/blakezhang/Documents/Dev/distrubuted-backtester/examples/sma.csv /tmp/phase4-final-verified-core-wheel-results` | Freshly rebuilt/reinstalled wheel: four jobs match across sequential, process, and journal-derived exports; no Ray or psutil installed |
| `.venv/bin/python /tmp/phase4-cli-check.py` | Actual durable CLI exports match sequential/process; deleted index and one result repaired byte for byte; attempts remain four; completed resume starts no workers |
| `.venv/bin/python /tmp/phase4-doc-link-check.py` | 138 local file links resolve across 29 Markdown files |
| `git diff --check` | Passed |

The earlier four disjoint suites passed 880 cases. After the user approved bounded
local process failure tests, the complete suite exercised all 897 cases, with no
skips or failures. Remote GitHub Actions has not been observed for this branch.
The first full run passed in 105.69 seconds; after strengthening timeout assertions,
the final full run passed in 105.18 seconds.

The actual CLI demonstration ran these commands successfully, each with four
successful jobs and no failures:

```bash
.venv/bin/distributed-backtester sweep --data examples/sma.csv --symbol SYNTH --price-basis raw --fast 1 2 --slow 3 4 --fee-bps 10 --backend sequential --output-dir /tmp/phase4-approved-demo/sequential
.venv/bin/distributed-backtester sweep --data examples/sma.csv --symbol SYNTH --price-basis raw --fast 1 2 --slow 3 4 --fee-bps 10 --backend process --workers 2 --output-dir /tmp/phase4-approved-demo/process
.venv/bin/distributed-backtester sweep --data examples/sma.csv --symbol SYNTH --price-basis raw --fast 1 2 --slow 3 4 --fee-bps 10 --backend ray --workers 2 --durable --max-attempts 3 --job-timeout 60 --retry-delay 0.1 --output-dir /tmp/phase4-approved-demo/durable
.venv/bin/distributed-backtester status --output-dir /tmp/phase4-approved-demo/durable
```

The temporary CLI script then deleted the generated index and one result, invoked
`resume --data /Users/blakezhang/Documents/Dev/distrubuted-backtester/examples/sma.csv --workers 2 --output-dir /tmp/phase4-approved-demo/durable`, and read status again.
Both repair runs restored the original bytes and retained the same four attempts.
The final generation was three; there were no worker startup timings or in-flight
attempts on completed resume. The script initially expected an empty worker list;
inspection showed that this diagnostic field is absent until workers start. The
script was corrected, then passed; project code and tests were unchanged.

The installed core wheel also ran the unchanged sample successfully: final equity
749.257000, two fills, and fees 1.743000. Exact command:

```bash
/tmp/phase4-core-wheel/bin/distributed-backtester backtest --data examples/sma.csv --symbol SYNTH --price-basis raw --fast 2 --slow 3 --initial-cash 1000 --fee-bps 10 --output /tmp/phase4-approved-core-wheel-results/example.json
```

The temporary verification scripts inspect exact archive bytes and install the
wheel outside the checkout. Portable wheel installation and demonstration commands
are in the README; the committed packaging and completed-resume tests cover these
boundaries. The wheel checks retained no invented performance values.

The tested source fingerprint is
`1052d7e4de199ea21c1c0e1900bbb64dc57b82e184b1b9ba67eb24a8d8e87c0d`.
The four-job wheel and actual CLI semantic digest is
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

Final acceptance review found a weak assertion in the actual timeout test. It now
requires a started worker, recorded `JobTimeout` with reason `timeout`, successful
retry, one actor retirement, and one replacement. To verify that it detects the
wrong failure mode, a temporary test-only mutation replaced the worker sleep with
an immediate exit. The focused command failed as intended (`worker_failure` was
not `timeout`; 1 failed, 5 deselected in 5.39 seconds). Restoring the sleep produced
the passing result above. No production source changed for this check.

Automatic approval review initially rejected real fault tests because source-freeze
reports did not provide sufficient trusted authorization for intentional worker
crashes. The user then explicitly approved bounded local failure tests. The focused
run and full suite passed with the required process/socket access. An earlier
sandboxed ordinary Ray CLI run failed during startup with denied sysctl access;
the unchanged test passed in the approved runs.

| Required scenario | Observed status |
| --- | --- |
| Worker dies before a job starts | Actual ready Ray actor killed before the first claim; replacement recovered and semantic results matched sequential |
| Worker dies during simulation | Worker terminated after two real strategy/engine close steps; second attempt recovered and matched sequential |
| Result finishes but acknowledgment is lost | Worker completed a real calculation, then died before returning it; retry matched sequential. SQLite post-commit process kill also preserved the commit |
| Job/result delivered more than once | Lost-reply recovery calculated the same stable job twice and committed one outcome; real journal identical/conflicting duplicate and pure transport tests passed |
| Job repeatedly fails | Actual Ray worker returned an explicitly injected structured failure; exactly three attempts exhausted the configured budget |
| Results partially written | SQLite kills during claim/success/failure transactions and derived export interruption/repair passed |
| Scheduler restarts | Driver exited after one real job committed and another was claimed; generation-two resume recovered both outcomes and matched sequential |

The additional actual timeout case retired a sleeping worker and completed a
replacement attempt with sequential parity. All six Ray fault cases passed in the
focused run and both complete suites. The test injectors are explicitly
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

Do you approve proceeding to Phase 5, efficient data processing and storage?
Stop here until explicit approval. Passing tests do not authorize the next phase.

# Fault tolerance demonstration

This guide exercises existing tests that terminate actual Ray workers and SQLite
coordinator subprocesses. The production simulation has no crash switch.
Test injectors deliberately reach failure boundaries while retaining the real
engine, scheduler, journal, and local runtime where specified.

The contract is at-least-once calculation with bounded attempts and idempotent
fenced commits: at most one committed terminal outcome per job in one journal.
Success is not guaranteed within a finite budget. Durable recovery requires a
local POSIX filesystem, one coordinator writer, and the complete experiment
directory. These scenarios do not establish power-loss, disk-loss,
network-partition, or high-availability behavior. See [reliability](reliability.md).

## Prepare a separate local run

Use Terminal at the repository root with Python >=3.12 and a working local Ray
runtime. The tests require process creation, local sockets, process inspection,
and POSIX process-group signals. Windows durable execution is unsupported.
Install the pinned development and distributed extras in your environment:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --editable '.[dev,distributed]'
```

Keep all package Python source unchanged during worker execution and between
start and resume. Run these commands separately from other native test or
benchmark sessions. The Ray cases skip if Ray is absent; a skip is not successful
failure evidence. The commands below are reproduction instructions, not a claim
of a fresh run for this documentation change.

## 1. Real Ray worker failures and coordinator restart

Run all six modes from [test_scheduler_faults.py](../tests/test_scheduler_faults.py):

```bash
.venv/bin/python -m pytest -q \
  tests/test_scheduler_faults.py::test_real_fault_and_recovery
```

To select only a finished calculation whose reply is lost:

```bash
.venv/bin/python -m pytest -q \
  'tests/test_scheduler_faults.py::test_real_fault_and_recovery[lost-reply]'
```

| Parameter | Actual fault injection | Invariants asserted by the test |
| --- | --- | --- |
| `before` | Kill an already ready Ray actor before the first job claim. | Two attempts, one replacement, completed publication, and full semantic digest equal to sequential. |
| `during` | A strategy subclass calls `os._exit(24)` during the second real close step. | Marker proves two bars observed inside simulation; two attempts, publication complete, and sequential parity. |
| `lost-reply` | Finish the real calculation, record a `finished` marker, then call `os._exit(25)` before returning the reply. | Marker proves calculation finished; a second attempt recovers with completed publication and sequential parity. |
| `timeout` | Sleep for 30 seconds on attempt one under a five-second attempt deadline. | Started marker, first attempt reason `timeout` with `JobTimeout`, successful second attempt, one retirement and one replacement. |
| `repeated` | An actual actor returns an explicitly injected structured `Injected` failure every time. | Exactly three attempts, one failed job, and completed publication. |
| `restart` | Coordinator calls `os._exit(77)` after one job commits and a second is submitted. | Before resume: one succeeded, one running, no index. After resume: two successes, generation two, and sequential parity. |

The ordinary recovery modes use one actor, three attempts maximum, zero retry
delay, and a five-second deadline. They check driver disconnection and empty
cleanup diagnostics. The restart child's deliberate exit bypasses its cleanup;
the parent verifies the saved state and resumes through the real Ray backend.
Each harness child has a 120-second wait bound and process-group cleanup on timeout.

All six cases should pass when local runtime prerequisites are met. The repeated
failure case passes by reaching the expected failed outcome. These assertions
cover full exported semantic results through `semantic_digest`, not just final
profit or a count printed by the CLI. [Phase 4's report](phase-4-report.md)
records historical execution of these scenarios, including strengthened timeout
checks; its source fingerprint differs from the current release.

## 2. Actual SQLite process kills inside and after transactions

This selector runs four cases in [test_journal.py](../tests/test_journal.py).
It uses real SQLite and subprocess termination, with fixed fixture identities;
it requires no Ray runtime.

```bash
.venv/bin/python -m pytest -q \
  tests/test_journal.py::test_abrupt_process_kill_rolls_back_partial_transactions_and_lost_ack
```

| Parameter | Kill boundary | Checked recovery behavior |
| --- | --- | --- |
| `claim` | Temporary SQLite trigger pauses after attempt insertion, before transaction completion. | Partial claim rolls back; attempt count remains zero; resume can claim attempt one. |
| `commit` | Trigger pauses after success-artifact insertion inside its transaction. | No partial success survives; job remains running; resume can claim attempt two. |
| `failure` | Trigger pauses after terminal-failure artifact insertion inside its transaction. | Partial failure rolls back; resume detects the abandoned one-attempt job and commits `Interrupted` failure. |
| `ack` | Pause after successful `Journal.commit` returns, before the child completes its protocol. | Success survives termination; resume preserves one commit and claims no further work. |

The child first resumes generation one into generation two. The parent waits for
`TRANSACTION_READY`, kills that process, then resumes into generation three.
This synchronization distinguishes a kill at the intended boundary from an
arbitrary early exit. Process termination exercises SQLite recovery under the
tested filesystem behavior; it does not simulate loss of power or storage.

## 3. Duplicate commits, conflicts, and stale replies

These tests use the real local journal with constructed outcomes. They inject
commit calls and old leases directly, without Ray or a network failure:

```bash
.venv/bin/python -m pytest -q \
  tests/test_journal.py::test_commit_duplicate_lost_ack_and_sorted_exports \
  tests/test_journal.py::test_conflicting_same_completed_attempt_raises \
  tests/test_journal.py::test_deadline_and_full_lease_fencing \
  tests/test_journal.py::test_resume_interrupts_and_fences_old_reply_and_clamps_wait
```

Identical successful commits return `duplicate` and retain the original artifact.
Conflicting content raises an error. Forged generation/deadline and expired or
interrupted leases return `stale`. Resume spends abandoned attempts and advances
generation. The [coordinator duplicate tests](../tests/test_coordinator.py)
separately inject transport deliveries through a fake runtime while retaining
real journal logic; they are protocol coverage, not actual network evidence.

## 4. Classic Ray failure stops publication and cleans owned processes

Classic execution has no durable resume contract. These two real-runtime cases
in [test_ray_faults.py](../tests/test_ray_faults.py) demonstrate its failure boundary:

```bash
.venv/bin/python -m pytest -q \
  tests/test_ray_faults.py::test_real_fault_cleans_owned_workers_and_services
```

`actor-death` injects `os._exit(23)` in an actual worker; `export-failure` raises
`OSError` on the second result export while actors remain live. Both assert no
final index or execution record, driver disconnection, no cleanup errors, and
exit of owned actors/services. The export case leaves one partial result file.
A surviving manifest or result alone is not a completed experiment.

## 5. Historical Phase 7 Docker STOP / INT / KILL evidence

The [Phase 7 report](phase-7-report.md) and
[verification record](phase-7-verification.json) retain actual Linux ARM64 drills,
including argv, before/after status, image identity, and export validation.
Signals were delivered after a persisted claim for one 262,144-bar synthetic job,
with one worker, three attempts maximum, and a 180-second attempt deadline.

| Recorded action | Container exit | Observed same-image recovery |
| --- | ---: | --- |
| `docker stop --timeout 30` | 143 | Generation one to two; interrupted then succeeded; one committed job. |
| `docker kill --signal INT` | 130 | Generation one to two; interrupted then succeeded; one committed job. |
| `docker kill --signal KILL` | 137 | Generation one to two; interrupted then succeeded; one committed job. |

All three recorded OOM flags were false and derived exports validated. The drill
image was `sha256:38839087d89e2f3518f8317078508cfec7b106c2373d2789040bbd17045b147f`,
using `0.8.0` package source
`a3edda1731d8694fda5d6987e570e0cca365bbee22d18bbfc0dbd116003f618f`.
Its image ID differs from the later final release images in the same record.
KILL recovery relies on the journal because graceful cleanup does not run.
These are historical observations, not newly executed drills or Docker Desktop
bind-filesystem durability evidence. [Operations](operations.md) provides the
mounted-container procedures. Use the same immutable image and dataset to resume.

## Optional CLI illustration: status and completed export repair

For a small run without invoking pytest, use a fresh output path:

```bash
.venv/bin/distributed-backtester sweep --data examples/sma.csv \
  --symbol SYNTH --price-basis raw --fast 1 2 --slow 3 4 --fee-bps 10 \
  --backend ray --workers 2 --durable --output-dir artifacts/fault-demo
.venv/bin/distributed-backtester status --output-dir artifacts/fault-demo
.venv/bin/distributed-backtester resume --data examples/sma.csv \
  --workers 2 --output-dir artifacts/fault-demo
.venv/bin/distributed-backtester status --output-dir artifacts/fault-demo
```

Expect four successes, four attempts, and completed publication. Completed resume
advances generation one to two, rebuilds semantic exports identically, and adds
no calculation attempts. Operational `execution.json` can change. This example
illustrates idempotent export publication; it contains no injected crash.
The [completed-resume test](../tests/test_coordinator.py) also checks repair of
truncated/missing exports without starting its injected runtime.
`status` needs no Ray import, but SQLite may need permission to create WAL/SHM
bookkeeping. Preserve the whole directory and matching data/source for recovery.

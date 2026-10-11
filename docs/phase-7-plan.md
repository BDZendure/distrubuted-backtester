# Phase 7 operations implementation plan

> **For agentic workers:** Use subagent-driven-development or executing-plans
> task by task. This document is the durable progress ledger.

**Authorization:** The user approved Phase 7 after the verified Phase 6 checkpoint.
Work on `feat/phase-7-operations`; stop before Phase 8. The approved roadmap and
[operations design](phase-7-design.md) define scope.

**Goal:** Make existing correct executions easy to configure, observe, deploy,
recover and maintain, without changing their economic or durability contracts.

**Architecture:** Extend the existing CLI using standard-library TOML and scoped
JSON logging. Reuse authoritative journal reads and complete export validation.
Build installed wheels into a nonroot Docker CLI environment and extend existing
CI with actual operational smoke checks. No frontend or service is introduced.

**Stack:** Python >=3.12, tomllib, logging, signal, existing pytest/Ruff/mypy,
optional Ray/PyArrow/DuckDB/psutil, Docker and GitHub Actions.

## Global constraints

- Phase 7 only. Phase 8 needs explicit approval. No push or history rewrite.
- Preserve chronological decisions, next supplied open execution, exact accounting,
  source/job identities, retries, fencing and idempotent commits.
- No new runtime dependency. Core installations remain independent of all extras.
- CLI explicit values override TOML defaults; config paths are file-relative.
- Reject unknown/mistyped/nonfinite configuration before output creation.
- Operational JSON goes to stderr; stdout remains existing machine-readable JSON.
- Default logging is OFF. Scope handlers/context and propagate settings explicitly
  to spawned workers. Never treat operational log delivery as a commit guarantee.
- Status reads are validated and read-only. Observed worker IDs are not liveness.
- Freeze package source during native tests, profiles and smoke executions.
- Report actual local/container/CI evidence separately; do not invent outcomes.
- No noisy timing CI gate. Preserve deterministic operation-count regression tests.
- Keep edits focused, documentation synchronized, no em dash or attribution.

## Context and design

- [x] Inspect clean Phase 6 checkpoint `19f1afb`, CLI, storage, worker lifecycle,
  journal, existing CI, roadmap and project/global guidance.
- [x] Continue the established in-place feature branch workflow.
- [x] Evaluate direct standard-library CLI extensions, external frameworks and
  API/dashboard alternatives; select the direct extension with existing contracts.
- [x] Run frozen baseline native suite:
  `PHASE5_STORAGE_CHILD_TESTS=1 .venv/bin/python -m pytest -q`;
  1,397 passed in 138.99 seconds.
- [x] Record independent container/CI design audit and actual daemon availability.
  Docker CLI 29.5.2 initially had absent sockets. Starting the installed Desktop
  app made `docker info --format '{{.ServerVersion}}'` return 29.5.2. No other
  images or containers are modified. Container build/runtime validation follows.

## Task 1: Validated TOML configuration and read-only status

Files: create `configuration.py`, `operational_status.py`,
`tests/test_configuration.py`, `tests/test_operational_status.py`,
`examples/backtest.toml` and `examples/sweep.toml`.

Interfaces:

```python
load_configuration(path: Path, command: str) -> dict[str, object]
read_status(output: Path, *, job_id: str | None = None) -> dict[str, object]
```

Configuration maps CLI option names with underscores, requires `schema_version=1`
and matching `command`, permits only backtest/sweep/resume fields, and preserves
money as decimal text. Resolve data/output/output_dir paths against the file's
directory. Validate exact integers, boolean switches, string choices, finite
numerical values, nonempty window/resource arrays and known keys. Reuse domain
constructors for semantic validation in the CLI.

- [x] Write failing configuration tests for a file-relative CSV, strict field/type
  errors, wrong command/version, malformed TOML, missing file and exact choices.
  Example assertion:

  ```python
  assert load_configuration(config, "backtest")["data"] == config.parent / "sma.csv"
  ```

- [x] Run `.venv/bin/python -m pytest tests/test_configuration.py -q` and record
  actual missing-feature failures before implementation.
- [x] Implement loader and two usable examples with no output path embedded.
- [x] Write failing status tests for complete real sequential exports, corrupted
  full artifacts, incomplete exports, journal pending/running/terminal states,
  active-writer reads, job filtering and nonexistent output without creation.
- [x] Run `.venv/bin/python -m pytest tests/test_operational_status.py -q`, then
  implement against `read_experiment_snapshot` and the existing journal reader.
  Consume every artifact for classic validation before returning any status.
  Preserve durable status fields; reject unknown job membership. Report retained
  worker readiness metadata as observed, never as current alive/dead state.
- [x] Run both files, scoped Ruff/format/mypy, independent task review, and commit.

Task 1 evidence: configuration 308 tests and scoped static checks passed;
independent spec/quality review accepted the loader/examples. Status initially
passed 77 tests. Independent review reproduced a journal appearing during final
classic artifact validation; five new red regressions preceded a final presence
check. Corrected status: 82 tests and scoped static checks passed; independent
re-review accepted the correction with no remaining findings.
Initial module-missing red runs were collection errors, not behavioral assertions.

## Task 2: Scoped structured logging and CLI lifecycle

Files: create `operational_logging.py`, `cli_options.py`, `cli_lifecycle.py`,
`tests/test_operational_logging.py`, `tests/test_operational_cli.py`;
modify `cli.py`, `worker.py`, `experiments.py`, process/Ray backend files,
`scheduler/ray_runtime.py`, `scheduler/coordinator.py`,
`scheduler/experiments.py` and package version `0.8.0`.

Interfaces: captured logging state is serializable and passed explicitly to
worker initialization; context managers restore caller logging/signal state.
`emit_event` publishes JSON with schema version, UTC timestamp, level, event,
pid and applicable experiment/job/attempt/generation identifiers.

- [ ] Write failing API tests for JSON line escaping, severity filtering, OFF,
  nested context restoration, existing logger ownership and failed stderr writes.
- [ ] Run `.venv/bin/python -m pytest tests/test_operational_logging.py -q`.
- [ ] Implement scoped standard-library JSON logging and explicit worker context
  capture/initialization. Keep default backend call arity unchanged when OFF.
- [ ] Write failing real CLI tests for `--config`, explicit precedence and CWD,
  output alias protection, stdout/stderr separation, clear config errors and
  status filtering. Example:

  ```python
  assert main(["backtest", "--config", str(config), "--fast", "1"]) == 0
  ```

- [ ] Run `.venv/bin/python -m pytest tests/test_operational_cli.py -q`.
- [ ] Integrate file defaults through argparse without weakening required fields
  or existing choices. Existing commands and exit codes retain their behavior.
- [ ] Emit worker start/finish/failure, experiment collection/export, and durable
  attempt claim/failure/commit/complete events. Journal commit event emission
  follows the returned durable commit; stdout contains only command results.
- [ ] Add scoped SIGTERM interruption returning 143 and Ctrl-C returning 130;
  restore signal handlers on every exit and retain runtime cleanup/lock release.
  Test with actual subprocess signals and same-identity durable resume.
- [ ] Run frozen real sequential/process/logged-Ray/durable smoke tests, relevant
  CLI/backend/coordinator regressions and static checks. Obtain review and commit.

## Task 3: Installed Docker environment, CI and smoke verification

Files: create `Dockerfile`, `.dockerignore`, `scripts/smoke.py`,
`tests/test_smoke.py`, `docs/development.md`, `docs/operations.md`;
modify `.github/workflows/ci.yml` and `MANIFEST.in`.

- [ ] Select and document an official supported Python slim image with immutable
  digest, matching builder/runtime stages and a fixed development dependency
  environment. Record Docker daemon/image availability from real commands.
- [ ] Write failing smoke tests before `scripts/smoke.py`. Its CLI runs a real
  reference backtest and unique sequential/process sweeps via installed module
  commands, checks expected fees/equity and complete-result equality, validates
  JSON logs/status and reports failures with nonzero status.
- [ ] Implement smoke helper, optional actual Ray/durable smoke and fresh outputs.
  It runs outside the checkout against an installed package, never mocked runtime.
- [ ] Build wheel in builder stage, install core/optional extras in runtime, use
  nonroot user, writable mount/temp paths, exec-form entrypoint and SIGTERM.
  Development target installs pinned existing tooling and includes tests.
  Exclude host venv, Git state, caches, local artifacts and benchmark bulk.
- [ ] Extend CI to run configuration/log/status/signal smoke outside checkout,
  installed wheel and actual Docker build/run; retain native Ray/storage tests.
  Keep pinned actions and clear job timeouts. Do not add wall-clock pass criteria.
- [ ] Execute actual Docker build/run and shutdown/restart where possible. Record
  exact failed commands if daemon/registry/runtime access is unavailable; provide
  manual commands rather than claiming unobserved success.
- [ ] Run helper tests, packaging checks, review container/CI changes and commit.

## Task 4: Final verification, guides and phase checkpoint

Files: `docs/phase-7-report.md`, operational/development guides, beginner/technical
explanations, README, architecture, roadmap, dependencies, testing/benchmarking,
decisions index and `docs/decisions/0009-operational-interface.md`.

- [ ] Freeze source and run all native tests, Ruff lint/format, strict mypy, pip
  check, wheel/source build and fresh isolated installation verification.
- [ ] Execute actual example/config/process/Ray/durable/status/log/signal commands;
  report outcomes accurately, including unavailable container or remote CI steps.
- [ ] Verify documentation links and final source/archive bytes.
- [ ] Obtain final independent integration/evidence review and resolve findings.
- [ ] Synchronize simple/technical guides, publish nine-section completion report,
  make focused local commits and confirm clean feature branch.
- [ ] Stop and obtain explicit approval before Phase 8.

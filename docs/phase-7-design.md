# Phase 7 operations design notes

Authorization: explicit user proceed after Phase 6 checkpoint. Base 19f1afb,
local branch feat/phase-7-operations. Preserve phase boundary and existing economic,
retry, artifact and source identity contracts. No new runtime dependencies.

Alternatives: (1) extend the existing CLI with standard-library TOML, logging
and status readers (selected); (2) add a configuration/logging framework (more
runtime dependencies without a useful capability); (3) build an API/dashboard
(additional state and services outside this phase). Containerization is a CLI
packaging/development surface, with existing Ray worker code and mounted journal.

Configuration: load_configuration(path: Path, command: str) -> dict[str, object].
Flat TOML schema_version=1 and command=backtest|sweep|resume. Keys correspond to
CLI option names with underscores. Strict command-specific field/type checks,
no unknown keys, no bool-as-int, no nonfinite numbers, exact choice checks.
USD initial_cash remains text. fast/slow integers for backtest, nonempty integer
arrays for sweeps. worker_resource is nonempty NAME=QUANTITY string array.
Paths resolve relative to the config's directory; explicit CLI paths keep CWD
semantics. Command must agree with the CLI; CLI values override file defaults.
Parsing/domain validation happens before output creation. Reuse existing domain
validators/backend constructors; protect config from output aliases. Only these
three run commands accept --config. Examples backtest.toml and sweep.toml next
to the bundled CSV. No embedded output path in default examples: user chooses.

Logging: structured JSON lines only to stderr, opt-in --log-level
OFF|ERROR|WARNING|INFO|DEBUG, default OFF for compatibility. Scoped handler
ownership preserves library caller logging handlers/levels/context on all exits.
Schema includes schema_version, UTC timestamp, level, event, process pid,
experiment_id, job_id, attempt_id, generation when applicable. Operational
context is captured explicitly into spawned worker initializers/Ray actors.
Ray workers retain local session log files, log_to_driver=False; coordinator
emits authoritative attempt/outcome/commit events on driver stderr. No scheduler
state transition changes. Emit committed events only after journal commit APIs
return. Log processing/output errors must not mutate simulation results or
replace primary workload exceptions. New events are informational, not durability
or delivery guarantees. Backend observational worker identities are separate
from economic/job identity. Logging is not timed performance evidence.

Status: read_status(output: Path, *, job_id: str | None = None) -> dict[str, object]
in a focused new operational_status module. Durable path uses existing validated
read_status journal API; preserve existing fields and add truthful observation
metadata only. Classic path uses read_experiment_snapshot and consumes every
artifact for validation before status publication. Classic incomplete exports are
explicit errors, not resumable/live scheduler status. Optional job filter must
validate lowercase digest and reject unknown membership. Worker readiness info,
when retained, is last-observed metadata, never a current liveness claim. Read
only: no coordinator lock, runtime start, resume or output creation.

CLI shutdown: scoped SIGTERM handler at main run boundary raises a controlled
interruption, restores prior handler even on errors, returns 143; Ctrl-C returns
130. Existing runtime finally blocks close owned pools/actors/connections and
release coordinator locks. Process pool may wait for already-running jobs;
container stop timeout can force SIGKILL. Durable journals are resumable within
the same source/data/engine identity; ordinary exports have no resume guarantee.
Respect single coordinator exclusion and bounded retry rules. No new guarantee
of immediate or bounded in-process graceful shutdown.

Validation: test first, actual spawned worker JSON and complete sequential/process
output equality, actual Ray/durable correlations/recovery where available. Run
full existing native tests and static checks against frozen package source.
Build source/wheel, isolated core smoke, example config and CLI logs/status from
outside repository. Docker daemon/image availability must be recorded, no mocked
container claims. CI can run actual container build/smoke on Linux; local remote
CI remains unobserved. Add deterministic correctness/operation count regression
checks, no noisy wall-clock gate without stable measurement evidence.

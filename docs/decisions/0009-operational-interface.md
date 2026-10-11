# ADR 0009: Operational interface and installed execution

Status: accepted for the explicitly approved Phase 7. Verification is recorded
in the phase plan and completion report. Phase 8 remains a separate checkpoint.

## Context

The engine already has deterministic economics, three execution backends,
durable retries and versioned storage. Operators need reproducible run settings,
inspectable failures and a deployable CLI. New state stores or services would
introduce competing sources of truth without improving these requirements.

## Decision

Extend the existing standard-library CLI. A strict, versioned, flat TOML document
supplies command-specific defaults. Relative file paths belong to its directory;
explicit CLI options replace defaults and explicit paths retain CWD semantics.
Reject mistyped fields and aliases of mutable destinations before execution.
Resume takes its economic configuration from the authoritative manifest.

Opt-in JSON observations use a dedicated scoped logger with explicitly captured
worker context. Restore caller handlers, level, propagation, disabled state and
context. Ordinary publication failures remain best effort; invalid event fields
still raise. Commit observations follow durable commit returns. Log delivery is
not a persistence guarantee. Ray worker observations remain in Ray session logs.

Status validates an existing journal or consumes every classic export artifact.
It creates no experiment and acquires no coordinator lock. SQLite read-only
transactions may maintain WAL/SHM sidecars. Historical worker readiness is an
observation, not a live health check. Partial classic exports are explicit errors.

Own CLI SIGINT/SIGTERM scopes, preserve interruption identity through cleanup,
and restore caller handlers. Cleanup may wait for active processes or native
runtime shutdown. SIGKILL bypasses cleanup; durable recovery relies on the
existing journal, filesystem semantics, generation fencing and bounded retries.

Pinned Ray 2.59.0 replaces Python SIGTERM handlers during initialization and has
no public switch to preserve the CLI bridge. A narrow adapter substitutes its
private registration helper only during an active main-thread CLI startup,
delegates actual registration to the original helper, and restores the helper
on every exit. Other threads and library calls retain vendor behavior. The Ray
pin already gates its private reaper-pipe cleanup adapter; an upgrade must review
both boundaries. This coupling buys a consistent startup interruption contract
without replacing Python's global signal API or claiming bounded shutdown.
The pinned native constructor also installs failure handlers; actual startup
checks verify its chaining to the CLI bridge. See the primary
[Ray 2.59.0 constructor](https://github.com/ray-project/ray/blob/ray-2.59.0/python/ray/_raylet.pyx#L3058)
and [native initialization](https://github.com/ray-project/ray/blob/ray-2.59.0/src/ray/core_worker/core_worker_process.cc#L802).

Build installed wheels into matching immutable Python base stages. Runtime uses
UID/GID 10001, root-owned installed code, a writable home/output mount, read-only
inputs and an exec entrypoint. The dev target adds existing pinned tools and
checkout files, with imports still coming from the installed wheel. Separate
Linux hash locks cover existing runtime/development/build dependency closures.

## Alternatives

- Configuration/logging frameworks: additional dependencies without a required
  capability for this small CLI.
- Dashboard/API services: new lifecycle, security and state boundaries beyond
  Phase 7 requirements.
- Editable container installs: useful for interactive changes but insufficient
  evidence that the actual distributed package works outside a checkout.
- Wall-clock CI regression ratios: unstable across shared runners. Preserve
  deterministic operation-count tests and report timing measurements separately.

## Consequences

The core wheel remains dependency-free. Native and installed-wheel smoke checks
validate the same economic and artifact contracts. Local Docker and remote CI
results need separate evidence; a configured workflow is not an observed pass.
Container resource limits, filesystem behavior and signal forwarding require
actual checks. Pinned versions and hashes require deliberate maintenance.

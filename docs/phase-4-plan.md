# Phase 4 implementation plan

Authorization: The user explicitly said "proceed" after the Phase 3 checkpoint.
Implement Phase 4 only and stop before Phase 5.

## Design

See [ADR 0006](decisions/0006-durable-scheduling.md). Preserve the simulation and
classic backend APIs. Add a SQLite journal, asynchronous Ray attempt runtime,
coordinator, and CLI status/resume commands. No new dependency is required.

## Work and validation ledger

- [x] Inspect existing contracts, preserve a clean Phase 3 baseline on a feature branch.
- [x] Run baseline tests: 695 passed in 70.17 seconds.
- [x] Review storage, retry, ownership, and fencing alternatives.
- [x] Share manifest and outcome serialization without changing classic artifacts.
- [x] Implement validated retry and attempt contracts and durable journal.
- [x] Implement asynchronous Ray execution with actor replacement and cleanup.
- [x] Implement deadlines, bounded retries, resume, and derived export repair.
- [x] Add durable sweep, resume, and read-only status commands.
- [ ] Verify all seven required failure scenarios with real storage and workers.
- [x] Independently review changes and address findings with regression tests.
- [x] Run non-Ray suite, pure transport tests, lint, formatting, typing, and core wheel demonstrations.
- [ ] Run full final suite and actual Ray failure demonstrations after explicit approval.
- [x] Update beginner and technical explanations, architecture, roadmap, and report.
- [x] Commit implementation and documentation milestones, recording pending real Ray validation.
- [ ] Complete Phase 4 acceptance and request explicit approval before Phase 5.

## Boundaries

The journal is authoritative. Only the coordinator writes it. Full artifacts and
terminal state commit together; exported JSON can be repaired. Policy is immutable
on resume, while operational Ray worker settings can change. Pending work is
ordered by job ID; timestamps and attempt history are operational metadata.
Successful semantic results stay independent of retries and completion order.

Actual multiple-machine partitions and disk power loss remain unverified unless
we execute those scenarios. Process termination must not be described as either.

## Verification hold

Automatic approval review rejected the real Ray fault-test run, requiring explicit
user approval for intentional worker crashes despite the recorded source freeze.
The approval request is pending. No Phase 5 work has begun.

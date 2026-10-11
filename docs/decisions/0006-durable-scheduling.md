# ADR 0006: Coordinator journal and bounded Ray retries

Status: Accepted for Phase 4. The user approved this phase on 2026-10-10.

## Decision

Use a local SQLite journal, one coordinator, and a bounded asynchronous Ray
actor runtime. SQLite is part of Python's standard library. Store the complete
canonical result and terminal job state in the same database transaction.
The existing manifest, result files, and compact index are derived exports.
Recovery can rebuild them without executing a committed job again.

The journal uses WAL, synchronous FULL, foreign keys, and a POSIX advisory lock
held for the entire coordinator session. Status reads use a separate read-only
connection and transaction. Durable execution initially supports Ray only;
sequential and process backends remain simulation correctness references.

Jobs progress from pending to running, then succeeded, failed, or pending for
another attempt. Each attempt has a job ID, increasing number, and coordinator
generation. Claiming an attempt is committed before remote submission. Resume
increments the generation and records abandoned running attempts as interrupted.
An interrupted attempt consumes its original retry budget. Old replies cannot
commit through a newer lease.

Retries have a fixed delay and a finite maximum number of attempts, including
the first. Ray actor restarts and task retries remain disabled. The coordinator
alone decides retries. Simulation failures, worker failures, and deadlines can
consume this budget. Configuration and protocol errors abort the coordinator.

Job deadlines use a monotonic clock while the coordinator is alive. A reply must
be observed before its deadline, including submission and transport time. Initial
pool startup has a separate bounded readiness wait. Replacement startup consumes
the next attempt's deadline. A timeout retires the actor before retrying.

Commit uniqueness is enforced by a primary key on job ID. An identical repeated
commit is acknowledged without another result. A conflicting repeated commit is
an error. A reply from an expired or interrupted lease is ignored and recorded.
Workers never access the journal. Resume requires the exact original dataset
bytes, validation policy, and installed source identity. Retry policy is immutable;
worker count and cluster resource settings may change.

## Alternatives

1. JSON event log plus separate result files: requires a second crash recovery
   protocol across metadata and artifacts. SQLite provides atomic transactions.
2. External database or queue service: adds deployment and failure dependencies
   that this phase does not need. The journal interface leaves room for one later.
3. Rely only on Ray retries: does not persist experiment progress across driver
   restarts and cannot make local result publication idempotent.

## Guarantees and limits

Calculations can repeat. We provide bounded retries with at-least-once delivery
semantics, not exactly-once execution or guaranteed eventual success. Within one
journal, each job has at most one committed terminal result. A lost acknowledgment
after commit cannot duplicate that result. A lost reply before commit can require
recomputation. A timeout can overlap briefly with a remote worker during a network
partition; lease fencing still prevents a late commit.

This is process crash recovery on a supported local POSIX filesystem. It is not
coordinator high availability, journal replication, Windows durable scheduling,
or recovery from disk loss. SQLite's durability depends on filesystem and device
behavior. We do not claim that process-kill tests prove power-loss safety.
The journal duplicates result bytes with exported JSON, trading disk space for
a simple authoritative commit. One writer and full result transactions may become
a throughput limit; measure before changing them.

SQLite documents [WAL restrictions and durability](https://www.sqlite.org/wal.html)
and [atomic commit assumptions](https://www.sqlite.org/atomiccommit.html).
Ray documents [actor failure behavior](https://docs.ray.io/en/latest/ray-core/fault_tolerance/actors.html).

## Validation

Use real SQLite transactions and process termination, plus real Ray actor failures.
Cover worker death before and during a job, lost acknowledgment, duplicate delivery,
retry exhaustion, partial writes, and coordinator restart. Also test deadlines,
stale generations, conflicting results, lock exclusion, input mismatch, and export
repair. Compare all successful semantic artifacts with sequential execution.

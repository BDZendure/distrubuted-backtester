# Development roadmap

Implement one phase at a time. Each phase ends with a report of implemented
behavior, architecture, actual checks, measured performance if applicable,
limitations, reproduction commands, engineering lessons, and the next proposal.
Explicit user approval is required before starting the following phase.

**Current status:** Phases 0 and 1 completed. Phase 1 was explicitly approved and
its [report](phase-1-report.md) records validation. Phase 2 was explicitly approved and is complete.
The [Phase 2 report](phase-2-report.md) records its verification and measurements.
Phase 3 was explicitly approved and is verified locally; its
[report](phase-3-report.md) and [comparisons](benchmarks/phase-3/results.md) record
the evidence and unavailable memory coverage. Phase 4 was explicitly approved
and is verified locally, including actual Ray fault recovery and coordinator
restart. Its [report](phase-4-report.md) records 897 passing tests and CLI export
parity/repair evidence. Phase 5 was explicitly approved and is verified locally.
Its [report](phase-5-report.md), [storage measurements](benchmarks/phase-5/results.md),
and [completed plan](phase-5-plan.md) record the evidence. Phase 6 remains unapproved.

## Phase 0: Architecture and repository setup

**Dependency:** Existing repository only.

**Deliverables:** Architecture and contracts, all-phase roadmap, ADRs, dependency
plan, testing and benchmark methodology, beginner and technical explanations,
README, `pyproject.toml`, minimal typed package, packaging smoke test, and CI.

**Acceptance:** The package installs and imports outside the checkout. Tests,
lint, format, type, and build checks run successfully or their execution blockers
are reported. Future interfaces and current capabilities are clearly distinguished.
No simulator, runtime dependencies, or distributed infrastructure is implemented.

**Validation:** Editable installation, pytest, Ruff lint/format, strict mypy,
`pip check`, wheel/source build, isolated wheel installation and import, document
link inspection, and Git whitespace checks. Remote CI status is reported separately
from local validation. No simulation throughput measurements are possible yet.

**Checkpoint:** Present the architecture, directory structure, and report.
Stop before Phase 1.

## Phase 1: Correct single-process engine

**Dependency:** Approved Phase 0 design, including execution/accounting semantics.

**Deliverables:** Strict CSV validation, immutable market data, configurable SMA
target strategy, chronological daily engine, next-open fills, fees, exact cash and
whole-share holdings, fills/order outcomes, close equity, total return/drawdown,
CLI, versioned JSON export, and deterministic synthetic data.

**Acceptance:** A complete backtest runs from the CLI. Identical inputs yield
identical semantic output. Hand-calculated reference cases agree. The full test
suite and development checks pass. Installation and sample commands are in the
README. Concurrency and distributed libraries remain absent.

**Validation:** Tests for empty/malformed/unsorted/duplicated CSVs, timestamp and
OHLC/volume rules, precision/fee boundaries, SMA warm-up/equality/entry/exit,
insufficient cash, invalid execution intents, gap sizing, fees on buys/sells,
last-bar cancellation, open final positions, and metric formulas. Mutation of
future bars must leave earlier decisions and fills unchanged. Run repeated full
simulations and CLI export/error cases. Inspect and explain every sample trade
and equity point using independent reference calculations.

**Checkpoint:** Full tests, example run, accurate report, exact reproduction
commands, and limitations. Stop before Phase 2.

## Phase 2: Parallel backtesting on one machine

**Dependency:** Approved and verified Phase 1 engine.

**Deliverables:** Serializable job specs, canonical job IDs, parameter sweeps and
manifests, sequential reference backend, multiprocessing backend using spawn,
bounded submissions, common-data worker initialization, incremental per-job
artifact export, a compact deterministic result index, and structured failures.
Ordinary strategy exceptions are
isolated; durable recovery and retry orchestration remain Phase 4 work.

**Acceptance:** Sequential and process outputs are semantically equivalent;
worker count and completion order do not change results. Failed jobs are explicit.
Benchmark inputs, raw samples, and environment are reproducible. Explain startup,
serialization, data copies, and scaling overhead without exaggerated claims.

**Validation:** Serialization round trips; mixed success/failure sweeps; duplicate
configuration handling; same engine result across 1, 2, 4, and 8 workers where
supported; deterministic index sorting; dataset mutation checks; coordinator
memory as job count increases without accumulating every equity path. Compare small and
substantial workloads including pool startup and loading, jobs/sec and sampled
process-tree peak RSS. Record unsupported worker counts, not invented results.

**Checkpoint:** Publish measured local scaling and report. Stop before Phase 3.

## Phase 3: Distributed execution

**Dependency:** Approved Phase 2 contracts and baseline.

**Deliverables:** Optional Ray backend, remote invocation of the same engine,
resource configuration, data placement/resolution, job submission/collection,
worker initialization/lifecycle, and reproducible cluster execution instructions.
Validate Ray support for the selected Python/runtime versions before adopting it.

**Acceptance:** One manifest runs through all three backends with equivalent
semantic outcomes. Distributed execution works in a documented environment;
measure against the local baseline and identify locality/network/scheduling costs.

**Validation:** Local Ray run, resource constraints, data identity checks,
backend equivalence, worker initialization, and cluster cleanup. Exercise local
multi-node execution if feasible. Real multiple-machine runs are required for
cross-machine claims; otherwise state that validation limit explicitly. Include
Ray services, workers, and object-store observations on every declared cluster
node, even when those processes are not driver descendants. Measure
serialization, data movement, startup, and task-granularity overhead. Record setup
scripts now; the full developer Docker environment remains Phase 7.

**Checkpoint:** Report environment, equivalence, overhead, and distributed test
coverage. Stop before Phase 4.

## Phase 4: Fault tolerance and scheduling

**Dependency:** Approved distributed backend and stable job identity.

**Deliverables:** Durable lifecycle, attempt identifiers, leases/fencing, timeout
semantics, bounded retries, worker failure observations, interrupted-experiment
recovery, transactional idempotent commits, duplicate/divergence protection,
experiment status, and structured terminal failures. SQLite is the selected local
single-writer journal; analytical storage does not replace scheduling durability.

**Acceptance:** No silent corruption or duplicate successful commits. Retry
behavior is deterministic and bounded. Timeout handling addresses live expired
work. State execution and persistence guarantees and recovery limits precisely.

**Validation:** Inject a crash before start, during a job, and after completion
before acknowledgment. Deliver a job/result twice, repeatedly fail a job, interrupt
a write, and restart the scheduler. Exercise late results from expired attempts,
retry exhaustion, and conflicting duplicate content. After recovery, compare the
committed semantic set with the sequential reference. Prove transaction rollback
and uniqueness behavior using real durable storage, not a mocked database.

**Checkpoint:** Explain at-least-once computation and at most one committed
success per stable ID within the coordinator's durable store. Stop before Phase 5.

## Phase 5: Data processing and storage

**Dependency:** Approved durable commit protocol and correctness baseline.

**Deliverables:** PyArrow/Parquet storage, measured partitioning choices, versioned
datasets, provenance/experiment manifests, DuckDB analytical queries over committed
results and metadata, and justified caching. Choose partitions using dataset size
and query shape; avoid many tiny files by default.

**Acceptance:** CSV and Parquet loaders produce identical validated bars and
economic results (fills, order outcomes, equity, final state, and metrics). Their
artifact byte digests/job IDs may differ; validate provenance and conversion lineage
separately. Same-source backend tests still compare complete semantic results.
Each snapshot preserves one verified outcome per stable job ID; queries can
return multiple snapshots.
Experiments resolve the intended dataset version. Storage efficiency and
complexity tradeoffs are documented with measurements; analytical query
correctness is verified separately. Query latency is not measured in Phase 5.

**Validation:** Storage round trips, exact-price schema preservation, corrupted
metadata/version rejection, commit-to-analytics reconciliation, and cache invalidation.
Measure CSV versus Parquet loading, repeated reads/experiments, memory, serialization,
and transfer. Distinguish process-cold reads from actually cold filesystem reads;
do not label an uncleared OS cache as a cold disk benchmark.

**Checkpoint:** Validate and report. Stop before Phase 6.

## Phase 6: Performance engineering

**Dependency:** Approved stable semantics and data representation.

**Deliverables:** Reproducible benchmark suite, CPU and allocation profiles,
measured baseline, focused optimizations, and before/after raw samples. Consider
event loops, arrays, serialization, initialization, batching, caching, and data
locality in the order supported by profiles. Consider C++ only for a demonstrated
bottleneck after comparing simpler options.

**Acceptance:** Every performance claim has recorded workload/environment and
measurements. Optimized outputs pass unchanged semantic reference/equivalence
tests. Successful and unsuccessful experiments are documented.

**Validation:** Profile substantial workloads; benchmark both baseline and changed
code with the same manifest, accounting/execution semantics, and environment.
Report runtime, jobs/sec, speedup, efficiency, process-tree memory, and variability.
Avoid semantic changes disguised as optimizations, including same-close vectorized
fills or dropping detailed output only on one backend.

**Checkpoint:** Review gains, regressions, and limits. Stop before Phase 7.

## Phase 7: Production-style reliability and developer experience

**Dependency:** Approved correctness, recovery, and performance behavior.

**Deliverables:** Docker environment, configuration files, structured logging,
experiment/job/attempt status, clear errors, developer/runbook documentation,
end-to-end smoke tests, and stable performance regression checks where appropriate.
Extend the basic Phase 0 CI to the completed operational surface.

**Acceptance:** A documented fresh environment can run examples, tests, and
experiments. Logs connect worker activity with jobs and attempts. Container
shutdown/resource limits have explicit behavior. Checks are useful and stable.

**Validation:** Build/run the container; execute CLI and backend smoke experiments;
verify config validation and logs; exercise shutdown/restart and failure reporting;
run CI lint/type/tests. Keep noisy timing measurements out of hard CI gates until
their variability is established. A frontend is unnecessary for these goals.

**Checkpoint:** Validate and report. Stop before Phase 8.

## Phase 8: Portfolio polish

**Dependency:** Verified prior phases and their recorded measurements.

**Deliverables:** Professional README, architecture diagram, design document,
benchmark/scaling analysis, failure demonstration, reproduction instructions,
tradeoffs/limitations, `docs/engineering-deep-dive.md`, interview summary, and
three resume bullet candidates based on implemented facts and measured results.

**Acceptance:** A new reader can reproduce the documented system and trace each
claim to code, tests, or recorded data. No unsupported production-readiness,
exactly-once, profitability, survivorship, or scaling claim appears.

**Validation:** Follow reproduction instructions from a clean environment;
review benchmark artifacts and reliability demonstration; inspect documentation
links and consistency with the final implementation; review each resume claim
against verified evidence.

**Checkpoint:** Present the final report and request approval for any additional
work or publication. A push or public release requires explicit authorization.

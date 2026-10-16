# Portfolio summary

Use this project to discuss simulation correctness, bounded execution, recovery,
and measurement. The implemented package is `0.8.0`; performance figures below
belong to archived `0.7.0` wheels. [Benchmark results](benchmark-results.md)
identify the measured versions, workload, machine, repetitions, and limitations.

## About 90 seconds

I built a Python backtester that runs the same daily SMA experiment sequentially,
in a bounded spawn pool, or through Ray actors. I started with an explicit
simulation contract: decisions use completed closes, orders fill at the next
open, and accounting uses integer micro-USD with exact rational metrics. The
tests check the full equity path and that changing future prices cannot change
earlier decisions or fills.

The systems problem was separating a calculation from its durable outcome.
Workers can calculate again after failure. One coordinator records attempts and
full terminal artifacts in a local SQLite journal. Attempt identities and
coordinator generations fence stale replies, and duplicate commits are
idempotent. Real local failure tests cover worker death, lost replies, timeouts,
and coordinator restart. Recovery depends on the surviving journal and available
retry budget.

I also measured where parallelism helped. In Phase 6, archived `0.7.0` wheels
ran 32 synthetic jobs over 16,384 bars on an M3 Pro Mac. Profiling guided bounded
history slicing and exact integer drawdown comparisons. The sequential median
fell from 3.821517 to 2.667269 seconds, 30.20%, over three timed repetitions per
configuration. Parallel changes were small and mixed, and cold Ray was slower.
The project demonstrates engineering behavior on declared workloads; it has
single-coordinator, simulation, and measurement limits.

Evidence: [simulation and tests](../tests/test_simulation.py),
[bounded process tests](../tests/test_backends.py),
[journal tests](../tests/test_journal.py),
[actual Ray failure tests](../tests/test_scheduler_faults.py), and
[Phase 6 raw comparison](benchmarks/phase-6/substantial-comparison.json).

## Extended interview narrative

**Define semantics before parallelizing.** In
[the simulation](../src/distributed_backtester/simulation.py), each bar first
executes the prior intent at its open, then marks equity at its close, then asks
the strategy for its next decision. A final unfilled intent is canceled. The
strategy receives a bounded prefix of completed close observations. Integer
money and specified fee rounding make accounting reproducible; rational return
and drawdown avoid floating-point ambiguity. The
[`test_hand_calculated_sma_entry_exit_and_full_equity_path` and future-suffix tests](../tests/test_simulation.py)
provide a reference that checks fills, fees, portfolio, and every equity point.
The supported history API prevents future access; Python private-attribute
introspection is outside that contract.

**Make work and outputs identifiable.** A
[job specification](../src/distributed_backtester/jobs.py) hashes canonical
configuration, dataset provenance, and implementation identity. Grid membership
preserves requested duplicates while execution uses unique jobs. Package source
fingerprints distinguish implementations even when release labels match.
[Identity tests](../tests/test_jobs.py) and [source fingerprint tests](../tests/test_provenance.py)
cover changes to semantic inputs and installed source. Execution timestamps and
retry history are separate from deterministic economic results.

**Bound the work that can accumulate.** The
[process backend](../src/distributed_backtester/backends/process.py) initializes
the dataset once per spawned worker, limits pending futures, yields outcomes,
and releases completed references. The
[`test_real_worker_reuse_bounds_and_future_release` test](../tests/test_backends.py)
observes a real pool. Ray uses an
[actor pool](../src/distributed_backtester/backends/ray.py) with explicit resource
settings and bounded references, covered by
[transport and real cleanup tests](../tests/test_ray_faults.py). Bounded pending
work does not make the whole program constant-memory: manifests and job
membership grow with jobs, and full simulation detail grows with bars.

**Make recovery a persistence protocol.** The
[journal](../src/distributed_backtester/scheduler/journal.py) has one local writer,
SQLite WAL transactions, and one committed terminal record per job. Full artifact
content and lifecycle state commit together; JSON exports can be repaired from
that authoritative record. A lease includes job, attempt, generation, and
deadline. Resume checks original identity, advances generation, and reconciles
interrupted attempts. An identical completed-attempt reply is a duplicate;
conflicting content raises an error; invalid or expired active leases are stale.
This supports at-least-once calculation with idempotent fenced commits under
a finite retry policy. The [Phase 4 report](phase-4-report.md),
[journal process-kill tests](../tests/test_journal.py), and
[real Ray fault tests](../tests/test_scheduler_faults.py) document observed
recovery. They do not establish power-loss, disk-loss, or network-partition safety.

**Measure before choosing an optimization.** Scoped profiles compared a bounded
index generator, built-in slicing, and slicing with integer drawdown comparisons.
The generator improved on the original but lost to built-in slicing. History
prefix bounds and exact metrics stayed covered by
[history tests](../tests/test_models.py) and [independent Fraction-reference tests](../tests/test_metrics.py).
The Phase 6 comparer checked complete economic results across both revisions
and all backends, including warmups. The substantial sequential improvement was
1.433x; two-process optimization speedup was only 1.001x, and cold Ray medians
rose 1.30% to 7.05%. Startup, transport, and serial export still matter. The
[raw comparison](benchmarks/phase-6/substantial-comparison.json) and
[profiling evidence](performance.md#scoped-evidence-and-the-alternatives) define
the separate scopes. Three samples on one synthetic workload do not establish
general scaling or current `0.8.0` performance.

**Separate installed verification from capacity claims.** Phase 7 recorded
installed `0.8.0` execution in Linux ARM64 containers as UID 10001, with code
under `/opt/venv`. TERM, INT, and KILL drills after persisted claims resumed
generation 1 to 2 with one interrupted attempt, one successful attempt, and one
committed job each. The [retained verification record](phase-7-verification.json)
contains commands, source identity, and outcomes. These are local correctness
and recovery observations. Remote CI and Linux AMD64 runtime were unobserved;
no container throughput benchmark was performed.

## Three resume candidates

Keep a numerical bullet's version, workload, host, and sample qualification in
the submitted resume. Use the alternative sentence below when space cannot
carry that qualification.

- Built a deterministic Python daily backtester with integer accounting,
  fills at the next open, bounded process/Ray execution, and job IDs that include
  source identity;
  checked complete outcomes against a sequential reference.
  Evidence: [simulation](../src/distributed_backtester/simulation.py),
  [jobs](../src/distributed_backtester/jobs.py),
  [accounting/timing tests](../tests/test_simulation.py),
  [real process parity](../tests/test_backends.py), and
  [Ray parity tests](../tests/test_ray_backend.py).
- Implemented a local SQLite journal for at-least-once calculation with
  idempotent commits, attempt/generation fencing, bounded retries, and repairable
  exports; verified worker-loss and coordinator-restart recovery in local Ray
  failure tests.
  Evidence: [journal](../src/distributed_backtester/scheduler/journal.py),
  [duplicate/stale commit tests](../tests/test_journal.py),
  [actual failure tests](../tests/test_scheduler_faults.py), and
  [historical observed results](phase-4-report.md).
- Optimized bounded history slicing and exact drawdown comparisons, reducing
  sequential median runtime 30.20% (3.821517 to 2.667269 s) between archived
  `0.7.0` wheels for 32 synthetic SMA jobs x 16,384 bars on an M3 Pro Mac,
  with three timed trials per configuration and full economic-result equality.
  Evidence: [raw comparison](benchmarks/phase-6/substantial-comparison.json),
  [baseline trials](benchmarks/phase-6/baseline-substantial.json),
  [candidate trials](benchmarks/phase-6/candidate-substantial.json),
  [history implementation/tests](../tests/test_models.py), and
  [metrics implementation/tests](../tests/test_metrics.py).

Alternative for the third bullet: "Used CPU and allocation profiles to optimize
bounded history access and exact drawdown calculations, then validated complete
economic-result equality across sequential, process, and local Ray execution."
It uses the same [comparison evidence](benchmarks/phase-6/substantial-comparison.json)
without a performance number.

## Interview questions and defensible answers

| Question | Answer and evidence |
| --- | --- |
| How do you prevent look-ahead? | The supported strategy input contains completed closes only, with captured prefix bounds; a decision fills at the next open. Future-suffix, changed-next-open, and retained-history tests check those specific invariants. Custom strategy state belongs to the caller. [Simulation tests](../tests/test_simulation.py), [history tests](../tests/test_models.py). |
| Why include source identity in a job ID? | Identical parameters and data under changed code can produce different results. Canonical IDs bind configuration, dataset, engine label, and installed source hash. Cross-revision comparison validates each identity before comparing complete results by configuration. [Jobs](../src/distributed_backtester/jobs.py), [comparer](../src/distributed_backtester/performance.py). |
| What happens if a reply is lost? | Calculation can repeat within the retry budget. SQLite uniqueness and completed-attempt content checks prevent a second terminal record; a repeated identical commit is acknowledged as duplicate. A worker lost before returning a calculated result and a driver killed after DB commit test different boundaries. [Ray faults](../tests/test_scheduler_faults.py), [journal tests](../tests/test_journal.py). |
| What fences a restarted coordinator? | Resume advances the persisted generation. Active commits must match the job, attempt number/identity, generation, and deadline; interrupted old replies cannot create a new commit. The journal has one POSIX-locked writer. [Journal](../src/distributed_backtester/scheduler/journal.py), [resume/fencing tests](../tests/test_journal.py). |
| Why did more workers fail to help? | On the declared local workloads, startup, full-result transfer, export, and coordination remain in the timer. Two processes were fastest on the substantial sets; cold Ray paid cluster lifecycle costs. Intervals overlap and do not isolate worker CPU. A warm cluster needs another measurement. [Benchmark synthesis](benchmark-results.md). |
| What does the 30.20% figure mean? | It is reduction in median sequential end-to-end runtime between two archived `0.7.0` wheels, with three timed repetitions per configuration on one M3 Pro: 32 jobs x 16,384 bars. It is 1.433x optimization speedup, not a parallel speedup or `0.8.0` result. [Raw comparison](benchmarks/phase-6/substantial-comparison.json). |
| Did memory improve? | Scoped Python allocation peaks were essentially unchanged. Complete private USS was unavailable in Phases 3/6 due to denied reads and incomplete ownership coverage. Phase 2 sampled RSS sums can double-count shared pages and miss peaks. [Memory evidence](benchmark-results.md#memory-evidence-and-missing-coverage). |
| Was Parquet faster? | The default substantial file was smaller and had a lower warm-read median, but a higher fresh-process wall median; complete-experiment ranges overlapped. A primed cache hit still reads/hashes bytes and is a separate operation. [Phase 5 raw samples](benchmarks/phase-5/substantial.json). |
| What deployment behavior was actually observed? | Installed execution and TERM/INT/KILL recovery in local Linux ARM64 containers, preserving a journal volume. Remote CI, Linux AMD64, physical multi-machine throughput, and power-loss safety were not observed. [Phase 7 verification](phase-7-verification.json). |

## Shortcomings and future changes

**Current scope.** Daily single-asset, long-only, whole-share simulation uses raw
USD prices and a fixed fee model. It has no market-impact, partial-fill, corporate
action, or live-broker model. Histories and detailed outputs grow with bars.
Durable mode has one local coordinator and an unreplicated SQLite journal; disk
loss removes the recovery authority. Finite attempts can be exhausted before
useful work starts. Source changes intentionally invalidate same-identity resume.
[Simulation](../src/distributed_backtester/simulation.py),
[execution](../src/distributed_backtester/execution.py), and
[reliability limits](reliability.md) define those boundaries.

**Proposed changes, unmeasured.** I would first isolate export and transport costs
and measure batching on prestarted clusters, preserving canonical results and
bounded retention. Larger deployments would require explicit data placement,
complete per-node memory/ownership accounting, partition tests, and a replicated
commit authority with a defined fencing protocol. Richer market behavior would
need new simulation contracts and reference cases. These are design directions,
not implemented features or demonstrated gains.

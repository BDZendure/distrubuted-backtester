# Benchmark methodology

Phase 2 implements the local harness and publishes
[recorded results and raw observations](benchmarks/phase-2/results.md). Phase 3 adds
the owned local Ray comparison harness below. Multiple-machine Ray
metrics and optimization procedures below remain planned requirements.
Run the harness with `python -m distributed_backtester.benchmark run --help`.
Optional psutil 7.2.2 is required only for measurement.

## Workload definition

Version a deterministic synthetic dataset and experiment manifest. Record dataset
digest, bar count, job count, SMA parameter grid, initial cash, fee policy, execution
semantics, and output detail. Use enough distinct valid configurations for the
declared unique job count; do not claim thousands of independent jobs by silently
deduplicating the same configuration.

Use at least two workloads: small tasks where overhead may dominate, and substantial
tasks where computation is meaningful. Choose actual sizes after measuring the
Phase 1 engine; do not promise that an arbitrary bar count is CPU-heavy.

All backends run the same jobs and produce the same detailed semantic results.
Validate equality before interpreting timing. Keep complete raw observations in
machine-readable artifacts; plots and tables derive from those observations.

## Measurement boundary and repetitions

Primary runtime begins before backend creation and dataset loading, and ends after
all per-job outcomes are exported, the compact result index is finalized, and the
backend is shut down. Include process/worker startup, data loading, serialization,
collection, and artifact export in the primary comparison. Optional timing of
compute without export is a separately labeled component measurement.

Operational trial reports separately measure CSV loading, identity/job
preparation, manifest preparation/export, time to the first outcome, time spent
advancing the backend iterator, and detailed artifact conversion/export. Backend
iterator time combines startup, simulation, IPC/collection waits, and shutdown;
it is not isolated worker CPU time. Workers can compute while the coordinator
exports, so these intervals cannot attribute all costs independently. Finalization,
imports, and process exit also remain in the external end-to-end timer. Report reused
pools as a separate steady-state scenario, with initial setup cost recorded.
For Ray report cluster provisioning/bootstrap separately and also present a cold
total including it; reusing an already running cluster must not hide startup costs.

Record at least five timed runs per configuration where feasible, after one declared
unmeasured warm-up. Each primary multiprocessing run still creates a fresh backend.
Alternate or deterministically rotate configuration order to reduce systematic cache
and thermal effects. Record every sample, median, range, and failed run. Tiny samples
do not justify precise percentile or statistical significance claims.

Use a monotonic high-resolution clock. Diagnostics, memory sampling, and profilers
introduce overhead; collect profiling separately and state which instruments were
active during timed runs. Include collection overhead consistently.

## Metrics

| Metric | Definition |
| --- | --- |
| End-to-end runtime | Elapsed wall seconds over the declared boundary |
| Throughput | Successfully completed unique jobs / total elapsed seconds; report failures and submitted count alongside it |
| Speedup | Median sequential runtime / median parallel runtime for the identical all-success workload |
| Parallel efficiency | Speedup / worker count |
| Job latency | Submission-to-outcome duration, including queue wait; distinguish service time |
| Local peak memory | Highest sampled simultaneous total RSS of coordinator plus live descendant workers |
| Ray cluster memory | Highest aligned-sample RSS sum over declared drivers, workers, and Ray services on all benchmark nodes; object-store use/capacity reported separately |
| CPU utilization | CPU time over elapsed time, with normalization per available core explicitly stated |

Do not sum each process's individual maximum RSS: those peaks may occur at different
times. Sample simultaneous process-tree RSS at a declared interval (for example
50 ms), include child processes, and record the sampling method/tool/version.
Short-lived workers and peaks between samples may be missed. Shared mapped pages
can be counted in multiple RSS values; label this an RSS sum, not unique physical
memory. Record PSS or cgroup memory separately when supported rather than silently
equating the two. Phase 2 uses optional psutil 7.2.2. Denied process observations invalidate
published peak memory values; raw samples retain denial and vanished-process
counts. Sequential sampling within each bin is approximate, not atomic. The
spawn resource tracker is included when visible as a descendant. The observer
process itself is outside the timed child process tree.

Compare sequential with 2, 4, and 8 workers when the available hardware supports
them. Record physical/logical CPU counts and any quotas. Unsupported configurations
are explicitly omitted. Additional oversubscription experiments require clear labels.

Ray requires per-node measurement covering drivers, workers, and declared runtime
services (including raylet and head-node services), regardless of process ancestry.
Ray distinguishes worker heap, runtime, and shared object-store memory; shared
object mappings complicate RSS accounting. See the
[Ray memory documentation](https://docs.ray.io/en/latest/ray-core/scheduling/memory-management.html).

Record object-store used bytes and capacity as separate observations, without
adding them again to a process RSS sum that may already include shared mappings.
Align timestamped node samples in declared sampling bins and record clock skew,
missing samples, and alignment tolerance. Label the resulting cluster peak as a
sampled estimate. Never sum independently observed node peaks. Include baseline
cluster use and ensure other workloads are absent or explicitly included in scope.

The number of queued jobs is not the only memory bound. Measure coordinator
memory as the job count increases; retain only compact outcome records while
exporting detailed equity paths incrementally. Full per-job result creation still
uses memory proportional to one job's bar count. Artifact export costs are part of
the primary end-to-end measurement, consistently across backends.

## Data-cache and environment controls

Record CPU model, cores, RAM, OS/kernel, architecture, Python/dependency versions,
Git commit and clean/dirty state, backend/version, worker resources, BLAS/OpenMP
thread settings, data location, workload digest, and other relevant running load.
Archive an exact dependency snapshot with benchmark artifacts. Direct version pins
alone do not fully fix the transitive environment.

A fresh process does not imply a cold operating-system filesystem cache. Use
`process-cold / OS cache uncontrolled` when the cache was not controlled. Describe
warm reads and repeated experiment runs separately. Avoid clearing shared caches
or changing host settings without authorization.

## Interpretation and optimization

Explain observed limits using measured startup, IPC, serialization, data copies,
job-size imbalance, memory bandwidth, I/O, and coordination costs. Larger worker
counts can reduce throughput. Do not infer linear scaling from CPU count.

Before an optimization, capture a baseline/profile with one stable manifest.
Change one meaningful factor, preserve semantic output, and repeat measurements.
Document regressions and ineffective changes as well as improvements. C++ or
vectorization needs both profile evidence and semantic-equivalence checks.


## Phase 3 owned local Ray harness

Run `python -m distributed_backtester.ray_benchmark run --help`. Install
`.[benchmark,distributed]` first. The runner compares sequential, process, and
Ray configurations on the same interpreter, installed source, CSV, and unique
jobs. The bounded SMA window/cash generator and deterministic seed are shared
with Phase 2. Each configuration has one recorded warmup excluded from summaries;
repeated fresh-process trials rotate configuration order.

The outer timer starts before subprocess launch and stops at child exit. Ray
trials include Ray import, owned cluster startup, dataset placement, actor
readiness, simulation, IPC, detailed conversion/export, fsync, final index,
backend shutdown, and exit. Generation and post-exit digest verification are
excluded. Component times in execution diagnostics overlap coordinator timings;
do not add overlapping intervals or equate them with worker CPU service time.
No prestarted cluster throughput or CPU profile is inferred from cold runs.

The parent parses and hashes the CSV snapshot and captures complete canonical
jobs before starting any child. Every successful trial must match that declared
manifest and experiment/job identities, then verify expected source identity,
the complete all-success index, every detail digest, and cross-trial equality.
Failure observations are persisted before abort. The optional
`--discard-trial-results` flag unlinks only newly created indexed detailed result
files after verification. Metadata, logs, digests, removed paths/bytes, and all
raw observations remain. Default runs retain details. This option limits disk
use during repeated export-heavy workloads, without changing timed execution.

Memory inventory uses the trial's private POSIX process group and exact owned
Ray session/PID-creation identities, rather than the driver child tree alone.
Service classification does not establish ownership. Records retain per-process
RSS, USS, errors, roles, coverage, and sampling times. RSS is never summed across
Ray mappings. Aggregate USS excludes shared pages and is a private-memory
quantity, not total physical cluster memory. It remains null when observations
are denied or incomplete; samples can miss transient peaks. Sampling also perturbs
the timed command and is not a profiler.

Logical serialized dataset size and configured object-store capacity are recorded
separately. Live object-store occupancy and unique resident shared-store pages
are not measured and are never added to private memory. Local observations cover
one physical machine only. Remote cluster memory needs per-node agents and
aligned observations; this runner deliberately has no external cluster option.
A denied macOS process read is an environment limitation, not evidence of zero
memory use. In the recorded Mac runs, actors also lacked independent group/session
proof and were excluded by the observer. Complete actor/cluster coverage was not
established. A target platform needs both supported memory reads and trustworthy
actor ownership evidence before publishing private-memory peaks. See the
[actual Phase 3 observations](benchmarks/phase-3/results.md).

Primary runs should use a clean committed source revision and record hardware,
Python, all installed dependencies, thread settings, sample interval, seed, data
hash, workload generator, repetitions, and completion digests. Keep historical
Phase 2 measurements unchanged. New comparisons are the Phase 3 baseline, even
when a workload happens to match Phase 2.


## Phase 5 storage comparison

Run the separate `distributed_backtester.storage_benchmark` module with frozen
package source. It compares CSV and strict integer Parquet layouts over identical
synthetic bars, using sequential full experiments and six operations per layout.
Rotate layout order each repetition and retain raw observations, source/workload
identities, exact results digests, file sizes, and software environment.

Read timing includes snapshot reading, hashing, parsing, and loader validation.
Fresh-process import/setup and parent wall time through child exit are separate
from the child API read. Warm reads decode again; primed content-verified cache
hits still read and hash. Complete experiment time includes data loading, job
construction, simulation, detailed export, and finalization. The harness verifies
complete economic results after timing, excluding only source lineage for the
cross-format comparison.

Protocol-5 serialization and deserialization have separate observations.
Same-host spawned transfer reports startup, Pipe delivery, and decoding through
an acknowledgment before independent verification; end-to-end includes verified
child exit. It does not measure network transfer. Own-process lifetime high-water
RSS uses `resource.getrusage(RUSAGE_SELF)`, normalized to bytes with explicit
availability and scope. Parent and child values remain separate. It is not the
Phase 3 memory observer and cannot establish dataset live size or cluster memory.

OS cache state is uncontrolled. A fresh interpreter is not a cold-disk read.
Report medians and ranges for small and substantial workloads, and inspect whether
row-group settings actually produce different files. File compression does not
imply smaller Python worker objects or faster complete simulations. See
[storage commands](storage.md) and [actual Phase 5 results](benchmarks/phase-5/results.md).

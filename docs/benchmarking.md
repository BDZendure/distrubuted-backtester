# Benchmark methodology

Phase 0 defines the method. There is no simulation workload, harness, or measured
throughput yet. The first execution comparison belongs to Phase 2.

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

Measure component costs separately to explain the end-to-end result. Report reused
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
equating the two. A small measurement dependency such as psutil can be evaluated
in Phase 2; it is not installed in Phase 0.

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

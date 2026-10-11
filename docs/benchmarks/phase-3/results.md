# Phase 3: Cold local Ray comparisons

## Environment and identity

Measured on 2026-10-10: Apple M3 Pro, 11 physical/logical cores, 18 GiB RAM,
macOS 26.6.1 arm64, CPython 3.14.6, Ray 2.59.0, and psutil 7.2.2.
Both primary reports capture a clean checkout at
`7ab3ff9d5261b30b3dcca57fedde84d040cc11c3`.

Installed source SHA-256:
`02d6288f4a007b3d4c7c143b5cab53a91b0ca08711ede192b5ab20f305165fe8`.
Engine: `distributed-backtester/0.4.0:daily-long-only-v1`.
Reports retain all installed versions, hardware, Git state, and thread environment.
The [observed version snapshot](../../../requirements/ray-benchmark.lock) is
specific to this Python/OS environment, not a universal dependency lock.

## Workload and method

| Workload | Unique jobs | Bars per job | Bar visits per trial |
| --- | ---: | ---: | ---: |
| Small | 32 | 64 | 2,048 |
| Substantial | 64 | 4,096 | 262,144 |

Synthetic raw USD data uses seed 1729, consecutive dates including weekends,
and `bounded-sma-windows-cash-batches-v1` configs with 10 bps fees. The parent
parses the generated CSV byte snapshot and captures the complete expected jobs
before launching any trial. Every trial must match that manifest and experiment
identity, then verify every detailed artifact hash and the semantic digest.

Seven configurations each have one excluded warmup and three timed repetitions,
rotating order. Every trial starts a fresh Python coordinator; process pools and
Ray clusters are created afresh. `perf_counter` starts before `Popen` and ends
after driver exit. Imports, CSV load, jobs, Ray startup, dataset placement, actor
readiness, simulation, detailed export/fsync, finalization, and shutdown are included.
Data generation, parent expected-manifest construction, post-exit verification,
explicit detail deletion, and post-exit helper cleanup are excluded. Observer
memory is excluded; its inventory work can perturb command timing.

All **42 timed trials and 14 warmups succeeded**. Their **2,688 simulations**
matched declared input/config/source identity and equal artifacts within each
workload. Detailed artifacts were explicitly deleted only after verification:
896 small and 1,792 substantial. Raw reports retain hashes, paths and byte counts;
these deleted contents cannot now be reverified independently. Omit the discard
flag when reproducing if you want to retain them.

One trial ran at a time. No tests, builds, profilers or other benchmarks were
intentionally run concurrently. These are process-cold measurements with OS caches,
background activity, CPU frequency and thermals uncontrolled. Three samples support
median/range summaries, not precise percentiles or significance claims.

## End-to-end results

Jobs/s = unique successful jobs / median seconds. Speedup = sequential median /
backend median. Efficiency = speedup / workers, not measured CPU utilization.
Seconds are rounded for display. Private-memory peaks are unavailable for all runs.

### Small workload

[Verbatim raw report](small.json), [captured manifest](small-manifest.json).

| Backend | Median s | Observed range s | Jobs/s | Speedup | Efficiency |
| --- | ---: | ---: | ---: | ---: | ---: |
| sequential | 0.162 | 0.148..0.162 | 197.78 | 1.000x | 100.00% |
| process-2 | 0.234 | 0.231..0.247 | 136.99 | 0.693x | 34.63% |
| process-4 | 0.253 | 0.252..0.256 | 126.45 | 0.639x | 15.98% |
| process-8 | 0.291 | 0.282..0.292 | 110.07 | 0.557x | 6.96% |
| ray-2 | 4.148 | 4.146..4.338 | 7.71 | 0.039x | 1.95% |
| ray-4 | 4.343 | 4.319..4.346 | 7.37 | 0.037x | 0.93% |
| ray-8 | 4.430 | 4.412..4.498 | 7.22 | 0.037x | 0.46% |

### Substantial workload

[Verbatim raw report](substantial.json), [captured manifest](substantial-manifest.json).

| Backend | Median s | Observed range s | Jobs/s | Speedup | Efficiency |
| --- | ---: | ---: | ---: | ---: | ---: |
| sequential | 2.013 | 2.005..2.013 | 31.79 | 1.000x | 100.00% |
| process-2 | 1.374 | 1.327..1.453 | 46.59 | 1.466x | 73.28% |
| process-4 | 1.442 | 1.437..1.447 | 44.40 | 1.397x | 34.91% |
| process-8 | 1.620 | 1.618..1.803 | 39.51 | 1.243x | 15.53% |
| ray-2 | 5.371 | 5.351..5.562 | 11.92 | 0.375x | 18.74% |
| ray-4 | 5.486 | 5.366..5.514 | 11.67 | 0.367x | 9.17% |
| ray-8 | 5.641 | 5.620..5.750 | 11.34 | 0.357x | 4.46% |

Sequential wins on the small workload. On the substantial workload, two
processes reduce median runtime from **2.013 s to 1.374 s**, a **1.466x speedup**.
Four and eight processes also beat sequential but take longer than two. Owned
cold Ray takes **5.371..5.641 s** by configuration median and is slower than
sequential and process execution on both workloads. More Ray actors did not
improve end-to-end runtime here. These observations do not establish an optimal
worker count for other workloads, warm clusters, or multiple physical machines.

## Component observations and interpretation

Substantial workload medians, recorded inside each coordinator:

| Backend | CSV load s | Backend iterator s | Detail export s | First outcome s |
| --- | ---: | ---: | ---: | ---: |
| sequential | 0.019 | 1.254 | 0.602 | 0.013 |
| process-2 | 0.019 | 0.343 | 0.867 | 0.160 |
| process-4 | 0.019 | 0.385 | 0.900 | 0.238 |
| process-8 | 0.019 | 0.703 | 0.769 | 0.398 |
| ray-2 | 0.019 | 4.131 | 0.636 | 2.405 |
| ray-4 | 0.019 | 4.304 | 0.633 | 2.562 |
| ray-8 | 0.019 | 4.473 | 0.624 | 2.712 |

Ray-specific substantial workload medians:

| Backend | Ray import s | Startup s | Dataset put s | Actors ready s | Shutdown s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| ray-2 | 0.365 | 2.321 | 0.021 | 0.032 | 1.319 |
| ray-4 | 0.338 | 2.474 | 0.021 | 0.036 | 1.341 |
| ray-8 | 0.335 | 2.600 | 0.021 | 0.050 | 1.409 |

Backend iterator time includes startup, compute/IPC waits, and shutdown.
First-outcome latency overlaps it; Ray-specific times are also nested components.
Dataset-put timing includes the diagnostic serialization-size calculation.
Do not add these overlapping intervals. Component medians need not sum to the
median end-to-end total. Export can overlap worker computation.

**Inference from these intervals:** cluster startup and shutdown plausibly explain
much of the cold Ray overhead, while coordinator export and increasing process
coordination costs constrain local scaling. This is not a CPU profile proving a
dominant bottleneck. Warm-cluster throughput, task batching, and optimization
require separately scoped measurements in later phases.

## Memory scope and missing coverage

The sampler uses a private POSIX process group anchored to original live process
identities, exact published local-session arguments, and certified PID/creation
identities. Names classify roles, never authorize cleanup. Recycled PIDs/groups
are rejected. Discovery uncertainty or a scoped unavailable USS read invalidates
a run peak. Per-process RSS is retained but never summed across shared mappings.

| Workload | Samples | Complete memory samples | Aggregate private USS peak |
| --- | ---: | ---: | --- |
| small | 1037 | 0 | unavailable |
| substantial | 1713 | 0 | unavailable |

On this Mac, self USS is supported, but the observer cannot read owned child
USS. Raw scoped `AccessDenied` evidence is retained. Ray actors also could not
be independently certified by the current group/session checks on this host:
their published worker PIDs lacked those ownership proofs. The sampler therefore
does not adopt them, and reports missing actor roles/PIDs. This is an additional
coverage limitation, not merely a short-sampling-interval problem. Driver, GCS,
raylet, runtime/dashboard agents, monitor/log-monitor and generic Ray-worker
roles appear in the raw traces, but **complete actor or cluster memory coverage
is not established**. Functional actor execution and cleanup are tested separately.

The dataset diagnostic serialized sizes are **3,286 bytes** small and
**196,349 bytes** substantial. Each local Ray store reserves **134,217,728 bytes**.
Payload length and capacity are logical quantities, not resident memory or
live occupancy, and are never added to USS/RSS. Actors decode separate Python
dataset objects. Physical shared-store usage, remote memory, network traffic,
and CPU utilization were not measured.

All post-exit cleanup error lists are empty. Two small process-2 trials and
two substantial process-4/process-8 trials left an inventoried owned helper,
which was killed only after creation-time/liveness revalidation. These events
and PIDs are retained in raw reports and excluded from the timing boundary.
No Ray primary trial required post-exit kills; backend cleanup diagnostics are empty.

## Reproduction

Use the measured revision or a checkout with the same source hash. Install
the optional extras; keep source fixed while drivers/actors are running.
Choose fresh output directories and worker counts supported by the host.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --editable ".[dev,benchmark,distributed]"
python -m pip check

python -m distributed_backtester.ray_benchmark run --bars 64 --jobs 32 \
  --workers 2 4 8 --repetitions 3 --discard-trial-results \
  --output-dir artifacts/phase3-small
python -m distributed_backtester.ray_benchmark run --bars 4096 --jobs 64 \
  --workers 2 4 8 --repetitions 3 --discard-trial-results \
  --output-dir artifacts/phase3-substantial
```

The two benchmark commands above were executed with `.venv/bin/python` on the
declared clean revision. Raw reports are verbatim copies. Local compact manifests,
indexes, execution records, CSV, ownership records, and stdout/stderr remain under
`artifacts/phase3-*`, ignored by Git. Captured manifests are also copied verbatim
alongside these reports. Regenerate details without the discard flag.

Historical [Phase 2 results](../phase-2/results.md) retain their original source,
observer, repetitions and RSS definition. They are not a controlled before/after
comparison with Phase 3. Use the sequential/process baselines in these new reports.
A prestarted cluster or multiple-machine deployment needs a separately declared
benchmark, including node-level scope. Current missing actor proof and denied USS
coverage must be resolved on the target platform before publishing memory peaks.

See [methodology](../../benchmarking.md), [cluster instructions](../../ray-clusters.md),
and the [Phase 3 report](../../phase-3-report.md).

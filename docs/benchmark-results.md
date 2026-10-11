# Benchmark results

This synthesis covers retained measurements from releases `0.3.0`, `0.4.0`,
`0.6.0`, and `0.7.0`. Phase 8 adds documentation for the implemented `0.8.0`
package; it introduces no benchmark or performance claim for that release.
Historical results identify the measured source, workload, host, and timer.
Comparisons across phases are context, not controlled release comparisons.

The clearest optimization result is the Phase 6 substantial sequential median:
**3.821517 to 2.667269 seconds, 1.433x speedup, 30.20% lower runtime**. It applies
to archived `0.7.0` wheels, 32 synthetic SMA jobs over 16,384 bars, and three
timed trials per configuration on one Mac. Parallel changes were small and
mixed; cold local Ray regressed on this workload. See the complete matrix below
and its [raw comparison](benchmarks/phase-6/substantial-comparison.json).

## Measured identities and workloads

All primary measurements used an Apple M3 Pro, 18 GiB RAM, macOS 26.6.1 arm64,
and CPython 3.14.6. Hardware reports record 11 physical/logical cores; available
cores were 11. Data was synthetic raw USD pricing, seed 1729, with consecutive
dates including weekends and 10 bps fees. Jobs were distinct configurations.
These are engineering workloads, not evidence of investment returns.

| Phase and release | Source revision | Small workload | Substantial workload | Timed repetitions |
| --- | --- | --- | --- | ---: |
| 2, `0.3.0` | `dc01e0611d0b268c260139ba5068b4f12f97471d` | 32 jobs x 64 bars | 64 jobs x 4,096 bars | 5 |
| 3, `0.4.0` | `7ab3ff9d5261b30b3dcca57fedde84d040cc11c3` | 32 jobs x 64 bars | 64 jobs x 4,096 bars | 3 |
| 5, `0.6.0` | `14d24c282b5fa6cb71403c153383c05e23e78572` | 4 jobs x 256 bars | 4 jobs x 65,536 bars | 3 per layout/operation |
| 6, `0.7.0` | Baseline `58837ac10216dcc2379d0b7ef0f5f3779466e4f7`; candidate `df294d4788673e0180d0dc67715c955407a95372` | 128 jobs x 256 bars | 32 jobs x 16,384 bars | 3 per revision/backend |

Package source SHA-256 values, distinct from dataset or wheel archive hashes:

| Measurement | Installed package source SHA-256 | Retained identity evidence |
| --- | --- | --- |
| Phase 2 | `5ac64d80388b3ce3eadb36eaf1a7b3306408e43a269040c2a713d62260571843` | [Small](benchmarks/phase-2/small.json), [substantial](benchmarks/phase-2/substantial.json) |
| Phase 3 | `02d6288f4a007b3d4c7c143b5cab53a91b0ca08711ede192b5ab20f305165fe8` | [Small](benchmarks/phase-3/small.json), [substantial](benchmarks/phase-3/substantial.json) |
| Phase 5 | `dfef1b1edee02fde072a4173750d25b8a39f9bc279772c69a184d98c9ed7c53c` | [Environment](benchmarks/phase-5/environment.json), [small](benchmarks/phase-5/small.json), [substantial](benchmarks/phase-5/substantial.json) |
| Phase 6 baseline | `ab0c29cffd82af10c03146da8182e025fa11d114c328c09a3bb0b06eb303a789` | [Installation](benchmarks/phase-6/baseline-installation.json) |
| Phase 6 candidate | `16a1a54602a060cea2fddaaae827b9aff99d87ccd5fbb6761a8b1731a5b834ad` | [Installation](benchmarks/phase-6/candidate-installation.json) |

Phases 2/3 record clean checkouts. Phase 5 records pending documentation changes
and frozen package source. Phase 6 installation records identify clean builds
and verify 42 wheel package files each. Installed-wheel Git fields are null;
`revision_clean: false` in those reports means checkout cleanliness was
unavailable. Source fingerprints and build records establish their provenance.

Exact observed environments: [Phase 2 lock](../requirements/benchmark.lock),
[Phase 3 lock](../requirements/ray-benchmark.lock),
[Phase 5 lock](../requirements/storage-benchmark.lock), and
[Phase 6 lock](../requirements/performance-benchmark.lock). Ray was 2.59.0 and
psutil 7.2.2 where used; Phase 5 used PyArrow 25.0.1 and DuckDB 1.5.6.

## Method and comparison boundaries

Phases 2/3/6 exclude one warmup per configuration, then rotate configuration
order across timed repetitions. Each trial starts a fresh coordinator and fresh
process pool or owned local Ray cluster. The end-to-end timer starts before
`Popen` and ends after child exit: imports, CSV load, job preparation, startup,
simulation, transport, detailed JSON export/finalization, and shutdown are
included. Dataset generation and independent post-exit validation are excluded.
Phases 3/6 also exclude parent expected-manifest construction and post-exit
helper cleanup. Observer memory is excluded; observation work can affect timing.

Phase 5 separates complete experiments, first reads in fresh interpreters,
warm reads in an existing interpreter, primed cache hits, serialization, and
local spawned transfer. Its complete-experiment timer includes loading through
export/finalization but excludes interpreter launch and independent verification.
It cannot be compared directly with the child-exit timer above. Scoped Phase 6
profiles also exclude interpreter startup and use different workloads.

Primary benchmark runs were serial, with no intentionally concurrent tests,
builds, or profilers. OS caches, background activity, scheduling, frequency, and
thermals were uncontrolled. Phase 6 revisions ran consecutively, not in randomized
order. Three or five samples support medians and observed ranges, not confidence
intervals, precise tail percentiles, or stable regression thresholds.

Tables show seconds rounded to six decimals. Arithmetic uses unrounded samples:

| Quantity | Definition |
| --- | --- |
| Median and range | Median, minimum, maximum of timed `runs[].elapsed_seconds`, excluding `warmups` |
| Backend speedup | Same revision's sequential median / backend median |
| Parallel efficiency | Backend speedup / worker count; not measured CPU utilization |
| Optimization speedup | Baseline median / candidate median for the same backend |
| Runtime reduction | `100 * (1 - candidate median / baseline median)`; negative means slower |
| Throughput | Distinct successful jobs / median end-to-end seconds |

## Phase 2: local process scaling, `0.3.0`

[Small raw trials](benchmarks/phase-2/small.json),
[substantial raw trials](benchmarks/phase-2/substantial.json), and
[original method and results](benchmarks/phase-2/results.md).

| Backend | Small median [min, max] s | Substantial median [min, max] s | Substantial backend speedup | Substantial efficiency |
| --- | ---: | ---: | ---: | ---: |
| sequential | 0.158785 [0.157366, 0.189632] | 2.068505 [2.023388, 2.092944] | 1.000x | 100.00% |
| process-2 | 0.268439 [0.231581, 0.407908] | 1.321220 [1.310640, 1.349555] | 1.566x | 78.28% |
| process-4 | 0.268113 [0.239397, 0.287840] | 1.453282 [1.427851, 1.501252] | 1.423x | 35.58% |
| process-8 | 0.300300 [0.286312, 0.366687] | 1.684559 [1.639156, 1.819439] | 1.228x | 15.35% |

Sequential wins on the small workload. Two processes are fastest on the
substantial workload; adding workers raises runtime and sampled tree RSS.
That worker count is an observation for this workload and machine.

Substantial coordinator detail-export medians were 0.678 s sequentially and
0.880 s with two processes. Eight processes had 0.400 s median time to first
outcome versus 0.013 s sequentially, recomputed from `component_timings` in the
substantial raw report. Export includes conversion, hashing, metrics, JSON, and
publication. Iterator time includes startup and compute/IPC waits. Workers can
compute during export; first-outcome latency overlaps iterator time. These
component medians must not be added as independent CPU costs. Serial export and
startup/transfer costs plausibly limit scaling here, an inference from intervals
and implementation rather than a CPU bottleneck proof.

## Phase 3: cold local Ray, `0.4.0`

[Small raw trials](benchmarks/phase-3/small.json),
[substantial raw trials](benchmarks/phase-3/substantial.json),
[small manifest](benchmarks/phase-3/small-manifest.json),
[substantial manifest](benchmarks/phase-3/substantial-manifest.json), and
[original results](benchmarks/phase-3/results.md).

| Backend | Small median s | Substantial median [min, max] s | Substantial backend speedup | Substantial efficiency |
| --- | ---: | ---: | ---: | ---: |
| sequential | 0.161798 | 2.013178 [2.005463, 2.013242] | 1.000x | 100.00% |
| process-2 | 0.233586 | 1.373655 [1.327089, 1.453183] | 1.466x | 73.28% |
| process-4 | 0.253064 | 1.441516 [1.437230, 1.447352] | 1.397x | 34.91% |
| process-8 | 0.290718 | 1.620030 [1.617501, 1.803292] | 1.243x | 15.53% |
| ray-2 | 4.147764 | 5.371002 [5.351067, 5.561692] | 0.375x | 18.74% |
| ray-4 | 4.343309 | 5.486458 [5.365881, 5.514440] | 0.367x | 9.17% |
| ray-8 | 4.429971 | 5.641429 [5.620008, 5.749968] | 0.357x | 4.46% |

Cold Ray is slower than both sequential and process execution. Substantial Ray-2
startup and shutdown medians were 2.321 s and 1.319 s; its detail export median
was 0.636 s. These nested component intervals help explain the cold cost but
do not isolate worker service time. Prestarted-cluster throughput, remote
network traffic, and physical multi-machine scaling were not measured.
Phase 2 and Phase 3 have different sources, observers, and repetitions; use
each phase's own sequential baseline.

## Phase 5: storage operations, `0.6.0`

[Small samples](benchmarks/phase-5/small.json),
[substantial samples](benchmarks/phase-5/substantial.json), and
[original results and operation definitions](benchmarks/phase-5/results.md).
Three layouts x six operations x three repetitions x two workloads yielded
108 verified observations, including 18 complete sequential experiments.
Cross-format economic equality excludes changed dataset lineage explicitly.

| Workload | Layout | File bytes | Full experiment median [min, max] s |
| --- | --- | ---: | ---: |
| small | CSV | 15,878 | 0.221555 [0.203727, 0.312949] |
| small | Parquet, 8,192 rows/group | 9,409 | 0.224977 [0.207460, 0.241395] |
| small | Parquet, 65,536 rows/group | 9,409 | 0.191968 [0.190452, 0.262783] |
| substantial | CSV | 4,054,324 | 8.633964 [8.527414, 8.681617] |
| substantial | Parquet, 8,192 rows/group | 1,930,921 | 8.544690 [8.348733, 9.680249] |
| substantial | Parquet, 65,536 rows/group | 2,056,839 | 8.653589 [8.379305, 8.769441] |

| Workload | Layout | Fresh API read s | Fresh process through exit s | Warm read s | Primed cache hit s |
| --- | --- | ---: | ---: | ---: | ---: |
| small | CSV | 0.004941 | 0.576745 | 0.005108 | 0.000222 |
| small | Parquet, 8,192 | 0.007172 | 0.755306 | 0.005296 | 0.000205 |
| small | Parquet, 65,536 | 0.007759 | 0.822438 | 0.005752 | 0.000322 |
| substantial | CSV | 1.297167 | 2.587710 | 1.253122 | 0.005310 |
| substantial | Parquet, 8,192 | 1.197246 | 3.014289 | 1.077567 | 0.002939 |
| substantial | Parquet, 65,536 | 1.153379 | 2.999106 | 1.040920 | 0.002975 |

All entries in the second table are medians. Fresh API reads include bytes,
hashing, decoding, and loader validation; import/setup is separate. Fresh
process wall time includes interpreter startup, imports, independent verification,
and reporting through child exit. Substantial default-Parquet import/setup was
0.669402 s versus CSV's 0.032678 s. Warm reads decode again in the same
interpreter. A primed one-entry cache avoids decoding/validation after reading
and hashing current bytes; separate CLI processes do not share that cache.
"Fresh" describes interpreter state, not cold physical disk. OS cache was
uncontrolled in every operation.

Default substantial Parquet used about half the CSV bytes and had a lower
warm-read median, but a higher fresh-process wall median. Complete-experiment
ranges overlap, so these observations do not establish an experiment speedup
or optimal row-group size. Both small Parquet layouts are the same one-group
file; their timing difference is variability. The raw reports separately retain
protocol-5 serialization and one local spawned transfer through decode
acknowledgment; that timer excludes subsequent independent child validation.
Those observations do not measure Ray object-store or network transfer.

## Phase 6: optimization of archived `0.7.0` wheels

The controlled small pair links [baseline trials](benchmarks/phase-6/baseline-small.json),
[candidate trials](benchmarks/phase-6/candidate-small.json), and
[comparison](benchmarks/phase-6/small-comparison.json). The substantial pair links
[baseline trials](benchmarks/phase-6/baseline-substantial.json),
[candidate trials](benchmarks/phase-6/candidate-substantial.json), and
[comparison](benchmarks/phase-6/substantial-comparison.json). Captured manifests
and the full original tables are linked in [Phase 6 results](benchmarks/phase-6/results.md).

### Small: 128 jobs, 256 bars

| Backend | Baseline median s | Candidate median [min, max] s | Optimization speedup | Runtime reduction | Candidate backend speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| sequential | 0.377949 | 0.308797 [0.305728, 0.309118] | 1.224x | 18.30% | 1.000x |
| process-2 | 0.375537 | 0.374458 [0.361784, 0.382012] | 1.003x | 0.29% | 0.825x |
| process-4 | 0.373069 | 0.359827 [0.359292, 0.360463] | 1.037x | 3.55% | 0.858x |
| process-8 | 0.433005 | 0.415561 [0.413949, 0.417218] | 1.042x | 4.03% | 0.743x |
| ray-2 | 4.622344 | 4.563762 [4.485495, 4.600401] | 1.013x | 1.27% | 0.068x |
| ray-4 | 4.564438 | 4.669421 [4.595741, 4.737209] | 0.978x | -2.30% | 0.066x |
| ray-8 | 4.849729 | 4.789417 [4.779253, 5.097599] | 1.013x | 1.24% | 0.064x |

### Substantial: 32 jobs, 16,384 bars

| Backend | Baseline median s | Candidate median [min, max] s | Optimization speedup | Runtime reduction | Candidate backend speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| sequential | 3.821517 | 2.667269 [2.600845, 2.844892] | 1.433x | 30.20% | 1.000x |
| process-2 | 2.371537 | 2.369008 [2.284186, 2.392777] | 1.001x | 0.11% | 1.126x |
| process-4 | 2.474945 | 2.499210 [2.495846, 2.633546] | 0.990x | -0.98% | 1.067x |
| process-8 | 2.862338 | 2.841476 [2.788111, 2.878244] | 1.007x | 0.73% | 0.939x |
| ray-2 | 6.676220 | 7.146931 [7.097124, 7.191520] | 0.934x | -7.05% | 0.373x |
| ray-4 | 6.733161 | 7.026772 [6.895134, 7.060065] | 0.958x | -4.36% | 0.380x |
| ray-8 | 7.050624 | 7.142382 [6.978246, 7.165010] | 0.987x | -1.30% | 0.373x |

The substantial sequential ratio is `3.821516875002999 / 2.6672691670246422`
= `1.4327451170839005`; runtime reduction is `30.203914982776336%`.
This optimization speedup is distinct from candidate process-2 backend speedup
of 1.126x and efficiency of 56.30%. Sequential optimization made the reference
faster while parallel startup, result transfer, export, and coordination remained.
Substantial process changes stayed below 1%; cold Ray medians were 1.30% to
7.05% slower. Consecutive revision order and few samples prevent assigning small
parallel differences or Ray regressions to the code change itself.

The comparer recorded full economic equality for all 112 trials including
warmups, or 8,960 backtest executions. Each pair has 28 trials per revision;
repeated executions are not additional distinct configurations. It validated
each producer's identity, artifacts, and complete results in configuration order,
including fills, outcomes, equity, portfolio, and metrics. Only a validated engine
release label may be normalized; job IDs can differ with implementation identity.
[Comparer source](../src/distributed_backtester/performance.py) and
[comparison tests](../tests/test_performance.py) define the checks. The first
[pilot pair](benchmarks/phase-6/pilots/environment-preflight.json) was excluded
because Ray import changed visible bundled distribution metadata. Controlled
runs exposed the same vendor path before parent/child capture.

### Profiles and alternatives

Separate uninstrumented scoped runs used 4 jobs x 65,536 bars and five
repetitions in one interpreter per report. Simulation scope starts after loading
the CSV and retains complete results for hashing afterward. Experiment scope
includes loading, job creation, sequential execution, and export/finalization;
independent validation follows timing. Neither includes interpreter startup.

| Variant | Simulation median s and raw report | Experiment median s and raw report |
| --- | --- | --- |
| Baseline | [0.975629](benchmarks/phase-6/profiles/baseline-timing-simulation/report.json) | [1.903224](benchmarks/phase-6/profiles/baseline-timing-experiment/report.json) |
| Bounded index generator | [0.871920](benchmarks/phase-6/profiles/generator-timing-simulation/report.json) | [1.849313](benchmarks/phase-6/profiles/generator-timing-experiment/report.json) |
| Native history slicing | [0.805127](benchmarks/phase-6/profiles/native-history-timing-simulation/report.json) | [1.766037](benchmarks/phase-6/profiles/native-history-timing-experiment/report.json) |
| Native slicing and integer drawdown comparisons | [0.669989](benchmarks/phase-6/profiles/optimized-timing-simulation/report.json) | [1.652131](benchmarks/phase-6/profiles/optimized-timing-experiment/report.json) |

The generator improved scoped timing but was slower than built-in slicing, so
it was not selected. [CPU baseline](benchmarks/phase-6/profiles/baseline-cpu-simulation/report.json)
and [optimized CPU](benchmarks/phase-6/profiles/optimized-cpu-simulation/report.json)
reports show history indexing calls falling from 2,101,158 to 528,364 and
metrics cumulative time from 0.649799 to 0.091127 s. Instrumented cumulative
entries overlap; these are diagnostic observations, not the uninstrumented
speedup timer. Native slicing means Python built-ins, not a new native extension.
[History implementation](../src/distributed_backtester/models.py) preserves prefix
bounds and custom-sequence fallbacks; [metrics](../src/distributed_backtester/metrics.py)
uses exact integer cross products before constructing final rational values.
[History tests](../tests/test_models.py) and [metrics tests](../tests/test_metrics.py)
cover those contracts. [The profiling guide](performance.md) links all 24 reports,
their source fingerprints, ranges, and method.

## Memory evidence and missing coverage

Phase 2 sampled coordinator plus live descendant RSS at a nominal 50 ms interval.
The table reports medians of per-trial sampled peaks, in MiB. Shared pages may
be counted repeatedly, inventory work lengthens intervals, and short processes
or peaks may be missed. It is an approximate RSS sum, not unique physical memory.

| Jobs, each 64 bars | Sequential coordinator MiB | Process-2 coordinator MiB | Process-2 tree RSS sum MiB | Raw report |
| --- | ---: | ---: | ---: | --- |
| 32 | 30.80 | 31.05 | 110.09 | [Small](benchmarks/phase-2/small.json) |
| 128 | 31.34 | 31.59 | 110.27 | [128 jobs](benchmarks/phase-2/memory-128.json) |
| 2,048 | 37.98 | 38.52 | 117.23 | [2,048 jobs](benchmarks/phase-2/memory-2048.json) |

Larger job sets add SMA windows and eventually cash batches. These memory
observations do not compare constant work per job or prove constant memory.
Submission/result buffering is bounded, but jobs, membership, and compact indexes
remain O(jobs); individual simulation detail remains O(bars).

Phases 3/6 require complete owned-process inventory and readable private USS.
All primary aggregate USS peaks were unavailable. Child reads were denied and
Ray actor ownership coverage was incomplete on this host. Per-process RSS is
retained without summing shared mappings, and cannot replace unavailable USS.
The Ray store's configured 134,217,728 bytes is logical capacity, not measured
resident memory or live occupancy. Remote memory, network traffic, and CPU
utilization were not measured.

Phase 5 uses own-process lifetime high-water RSS from `resource.getrusage`,
normalized to bytes on macOS. Parent peaks include earlier setup and validation;
fresh readers/receivers are separate processes. These values cannot be summed
or treated as operation-specific deltas. Phase 6 `tracemalloc` measures traced
Python allocations, not all native memory or worker heaps. Simulation peaks
were [39,037,616 bytes before](benchmarks/phase-6/profiles/baseline-memory-simulation/report.json)
and [39,037,140 after](benchmarks/phase-6/profiles/optimized-memory-simulation/report.json);
experiment peaks were [68,127,508 before](benchmarks/phase-6/profiles/baseline-memory-experiment/report.json)
and [68,126,749 after](benchmarks/phase-6/profiles/optimized-memory-experiment/report.json).
There is no material memory-reduction claim. End current bytes and snapshot
live differences also do not count every allocation made and freed.

## Evidence retention and next measurements

Committed JSON retains observations, methods, identities, and digests; retained
Markdown provides historical interpretation. Phases 3/5 explicitly deleted
detailed trial result files after verification. Their hashes cannot reconstruct
the deleted content, so full artifacts cannot now be independently rechecked
from committed reports alone. Phase 6 retained detail through its validating
comparison, but report paths reference external local artifact roots rather
than packaging all trial content in Git. This synthesis rechecks retained raw
arithmetic and recorded validation outcomes; it does not rerun experiments or
certify the present availability of those external files.

Future work would measure `0.8.0` separately, randomize revision order, collect
more repetitions, isolate export and transport costs, compare prestarted Ray
clusters, and establish complete USS ownership coverage on the target platform.
Physical multi-machine tests would need declared nodes, network, data placement,
and failure scope. These are proposals, not observed results. Historical
reproduction procedures remain in the linked phase results and [performance
guide](performance.md); source and dependency identities must match the intended
measurement.

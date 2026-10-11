# Phase 5 storage measurements

These are observed comparisons on one Apple M3 Pro machine, not optimization or
production-capacity claims. Both workloads completed with **108 verified raw
observations** in total: 3 layouts x 6 operations x 3 repetitions x 2 workloads.
The 18 complete experiments matched full economic results per configuration,
excluding only dataset lineage. Other operations verified bar and serialized
identities and reference that verified economic baseline.

## Environment and provenance

- macOS 26.6.1, arm64, Apple M3 Pro, 11 available logical cores, 18 GiB RAM.
- CPython 3.14.6, PyArrow 25.0.1, DuckDB 1.5.6; exact installed versions in
  [storage-benchmark.lock](../../../requirements/storage-benchmark.lock).
- Seed 1729, deterministic synthetic bars, 4 bounded SMA configurations,
  sequential execution. Small: 256 bars. Substantial: 65,536 bars.
- Row-group limits: 8,192 and 65,536. Layout order rotates each repetition.
- OS filesystem cache uncontrolled; no cold-disk, network, or scaling claim.
- Package source committed at `14d24c2` and frozen throughout both runs.
  Documentation edits were pending, as the raw `git_dirty` records disclose.
- Installed package source SHA-256:
  `dfef1b1edee02fde072a4173750d25b8a39f9bc279772c69a184d98c9ed7c53c`.

[Small raw samples](small.json), [substantial raw samples](substantial.json), and
[hardware environment](environment.json) retain exact observations, identities,
software environment, wall-clock boundaries, and artifact/payload digests.
[CLI demonstration evidence](demonstration.json) separately records actual
sequential/process/Ray/durable-Ray parity and idempotent analytical import.

## File size and complete experiments

All times are seconds. Time includes load, identity-bound job creation, simulation,
detailed JSON export, and finalization. Independent artifact verification follows
timing. Jobs/s is 4 divided by the median time, not a CPU scaling result.

| Workload | Layout | File bytes | Median full experiment | Min-max | Jobs/s |
| --- | --- | ---: | ---: | ---: | ---: |
| small | csv | 15,878 | 0.221555 | 0.203727-0.312949 | 18.054 |
| small | parquet-rg8192 | 9,409 | 0.224977 | 0.207460-0.241395 | 17.780 |
| small | parquet-rg65536 | 9,409 | 0.191968 | 0.190452-0.262783 | 20.837 |
| substantial | csv | 4,054,324 | 8.633964 | 8.527414-8.681617 | 0.463 |
| substantial | parquet-rg8192 | 1,930,921 | 8.544690 | 8.348733-9.680249 | 0.468 |
| substantial | parquet-rg65536 | 2,056,839 | 8.653589 | 8.379305-8.769441 | 0.462 |

## Loading and verified reuse

The API read includes reading bytes, hashing, parsing, and loader validation.
Fresh-process import/setup is timed separately. Parent wall time through child
exit also includes interpreter startup, verification, and reporting. Warm reads
use the same interpreter and decode again. Cache hits use an explicitly primed
one-entry cache, but still read and hash current file bytes.

| Workload | Layout | Fresh API median | API min-max | Import/setup median | Process through exit median | Warm median | Warm min-max | Cache median | Cache min-max |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small | csv | 0.004941 | 0.004491-0.005578 | 0.030211 | 0.576745 | 0.005108 | 0.004924-0.005185 | 0.000222 | 0.000183-0.000272 |
| small | parquet-rg8192 | 0.007172 | 0.006470-0.025540 | 0.210016 | 0.755306 | 0.005296 | 0.004994-0.010014 | 0.000205 | 0.000195-0.000236 |
| small | parquet-rg65536 | 0.007759 | 0.007235-0.007938 | 0.226563 | 0.822438 | 0.005752 | 0.005583-0.006881 | 0.000322 | 0.000272-0.000348 |
| substantial | csv | 1.297167 | 1.019807-1.388598 | 0.032678 | 2.587710 | 1.253122 | 1.160015-1.299273 | 0.005310 | 0.005101-0.008354 |
| substantial | parquet-rg8192 | 1.197246 | 1.066519-1.221222 | 0.648233 | 3.014289 | 1.077567 | 0.943376-1.282677 | 0.002939 | 0.002237-0.003017 |
| substantial | parquet-rg65536 | 1.153379 | 0.974570-1.154664 | 0.669402 | 2.999106 | 1.040920 | 0.843631-1.083386 | 0.002975 | 0.001976-0.003262 |

## Serialization and local spawned transfer

The dataset is the same immutable Python bar representation for both formats.
Protocol 5 uses in-band serialization. Spawn time runs from process start through
Pipe delivery and decode acknowledgment; independent child verification happens
after that acknowledgment. Parent wall time continues through verified child exit.
This is one local child, not Ray object-store or network transfer.

| Workload | Layout | Pickle bytes | Serialize median | Deserialize median | Spawn through decode median | Spawn min-max | Through child exit median |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| small | csv | 12,493 | 0.001615 | 0.001153 | 0.357463 | 0.350712-0.384581 | 0.619767 |
| small | parquet-rg8192 | 12,695 | 0.002273 | 0.001400 | 0.322324 | 0.311509-0.350351 | 0.552985 |
| small | parquet-rg65536 | 12,695 | 0.001805 | 0.001449 | 0.338868 | 0.316662-0.366736 | 0.578297 |
| substantial | csv | 3,137,933 | 0.578159 | 0.422780 | 0.811039 | 0.798928-1.021851 | 1.653964 |
| substantial | parquet-rg8192 | 3,138,135 | 0.555866 | 0.420731 | 0.824377 | 0.790993-0.836682 | 1.675922 |
| substantial | parquet-rg65536 | 3,138,135 | 0.545466 | 0.402303 | 0.789273 | 0.756019-0.836759 | 1.515387 |

## Memory scope

Values below are MiB (1,048,576 bytes). The source is macOS
`resource.getrusage(RUSAGE_SELF).ru_maxrss`, normalized to bytes in each sample.
It is own-process lifetime high-water RSS. Fresh readers and receivers are separate
processes; the parent maximum accumulates earlier work across layouts and trials.
Never sum these numbers or infer a per-format live-memory saving from the parent.

| Workload | Layout | Fresh reader median MiB | Spawn receiver median MiB |
| --- | --- | ---: | ---: |
| small | csv | 26.70 | 31.61 |
| small | parquet-rg8192 | 52.73 | 31.69 |
| small | parquet-rg65536 | 52.72 | 31.67 |
| substantial | csv | 67.64 | 75.44 |
| substantial | parquet-rg8192 | 95.44 | 75.31 |
| substantial | parquet-rg65536 | 100.17 | 75.38 |

The small parent lifetime high-water maximum was **57.22 MiB**. This includes setup, conversion, exports, and validation outside timed operations.

The substantial parent lifetime high-water maximum was **266.77 MiB**. This includes setup, conversion, exports, and validation outside timed operations.

## Interpretation and limits

The 256-row Parquet files have identical bytes: both limits produce one row group.
Their timing differences are run variability, not evidence of a layout benefit.
For 65,536 bars, 8,192-row groups produced a smaller file (1,930,921 bytes)
than 65,536-row groups (2,056,839 bytes). Warm-read medians were 1.077567 and
1.040920 seconds respectively, with overlapping ranges. Full experiment medians
were 8.544690 and 8.653589 seconds, also with overlapping ranges. CSV's full
median was 8.633964 seconds. These runs do not establish a complete-experiment
speedup from Parquet. The default remains 65,536
rows as a simple bounded layout for complete single-asset histories. Three trials
on one synthetic workload do not establish an optimal group size or platform rule.
Files are partitioned by asset, with no fine date partitions or multi-asset catalogue.

The default substantial Parquet file used about half the CSV storage. Its
fresh-process wall median was 2.999106 seconds versus CSV's 2.587710 seconds;
native import/setup and independent verification are visible in this boundary.
Warm-read medians were 1.040920 seconds versus 1.253122 seconds. Primed cache-hit
medians were 0.002975 and 0.005310 seconds, while still reading and hashing.
These observations concern different operations, so one must not substitute a
cache-hit time or API read time for complete experiment time.

A smaller Parquet file does not remove Python decoding, strict semantic checks,
per-worker objects, simulation, or detailed-result export. Verified caching avoids
decoding and validation after content checks; separate CLI invocations do not
reuse the previous process's cache. Startup dominates some small operations.

Three repetitions give medians and observed ranges, not statistical confidence
intervals. Workload generation, conversion setup, and independent harness
verification are outside individual operation timings. Fresh-process wall time
includes verification, whereas API read time excludes the independent check.
Local timestamps and raw operation boundaries are retained for that distinction.
No profiler was used and no simulation optimization was introduced in Phase 5.

Detailed trial result files were removed only after full verification using the
explicit `--discard-trial-results` option. Raw samples retain removed byte counts,
artifact hashes, indexes, and economic digests. Default runs retain full results.
Synthetic data and results can be regenerated; machine-specific timings vary.

## Reproduce

From the repository directory with the virtual environment active:

```bash
python -m pip install --editable ".[dev,benchmark,distributed,storage]"
python -m distributed_backtester.storage_benchmark run \
  --bars 256 --jobs 4 --repetitions 3 --row-group-sizes 8192 65536 \
  --discard-trial-results --output-dir artifacts/storage-small
python -m distributed_backtester.storage_benchmark run \
  --bars 65536 --jobs 4 --repetitions 3 --row-group-sizes 8192 65536 \
  --discard-trial-results --output-dir artifacts/storage-substantial
```

Use fresh output directories and freeze package source for the entire run.
The observed invocations used the same parameters and canonical destinations
`/private/tmp/phase5-storage-small` and `/private/tmp/phase5-storage-substantial`.
Full observations are in `report.json`; failures or interruption retain
`observations.json` with terminal status. See [storage instructions](../../storage.md).

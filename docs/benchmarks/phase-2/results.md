# Phase 2: Local scaling measurements

## Environment and workload

Measured on 2026-10-10: Apple M3 Pro, 11 physical/logical cores, 18 GiB RAM,
macOS 26.6.1 arm64, CPython 3.14.6, psutil 7.2.2. All four primary reports
record a clean source checkout at
`dc01e0611d0b268c260139ba5068b4f12f97471d`.

Installed source SHA-256:
`5ac64d80388b3ce3eadb36eaf1a7b3306408e43a269040c2a713d62260571843`.
Engine: `distributed-backtester/0.3.0:daily-long-only-v1`.
The [complete observed version snapshot](../../../requirements/benchmark.lock)
is specific to macOS arm64/Python 3.14.6. Reports also retain every installed
version, available cores, platform, hardware errors, and thread environment.

| Workload | Unique jobs | Bars per job | Simulated bar visits per trial |
| --- | ---: | ---: | ---: |
| Small | 32 | 64 | 2,048 |
| Substantial | 64 | 4,096 | 262,144 |

CSV generation uses a local RNG with seed 1729, raw synthetic USD prices and
consecutive dates, including weekends. Jobs use $1,000 initial cash, 10 bps fees,
and `bounded-sma-windows-cash-batches-v1`: fast windows 1..16, slow windows up to
80, and an additional dollar of cash after each batch of 1,024 window pairs.
The primary workloads use only the first 32 or 64 configs, with slow windows
at most 18 or 20. Each declared job count is distinct; no silent deduplication
inflates throughput. This measures synthetic engineering workloads.

## Measurement method

Each configuration has one warm-up, excluded from comparison summaries, and five
timed repetitions. Order rotates deterministically. Every trial starts a fresh
coordinator subprocess and, for process execution, a fresh spawn pool. CSV loads
once in that coordinator, and the immutable dataset initializes each worker once.
In-flight limits use the default twice the workers.

`perf_counter` starts before process creation and stops after child exit. Imports,
CSV loading, job preparation, worker startup, simulation, IPC, detailed JSON
export/fsync, index/report finalization, and shutdown are included. Dataset
creation and artifact verification afterward are excluded. The observer's RSS is
outside the child tree; its sampling overhead is present in all configurations.

All 60 primary timed trials and 12 warm-ups succeeded. Every artifact digest was
verified, and manifest/index/detailed results were equivalent across every trial
of its workload. This includes 2,048 unique jobs per large-count trial. There were
no failed trials or denied memory observations. Vanished-process observations
are retained and expected near exits: 12/4/0/1 in the timed small/substantial/
memory-128/memory-2048 reports respectively.

Memory is sampled every nominal 50 ms: the highest approximate simultaneous sum
of coordinator and live descendants' RSS, including visible worker/resource-tracker
and operational child processes. Sampling work adds to the actual interval.
Short processes and brief peaks may be missed; shared pages can be counted more
than once. This is an RSS sum, not unique physical memory. Reported memory tables
use the median of each trial's sampled peak; raw traces retain every observation.

Trials are **process-cold / OS cache uncontrolled**. One trial ran at a time;
no tests, builds, or profilers were intentionally run concurrently with the primary
comparisons. Other host background activity, CPU frequency, thermals, and OS cache
were not controlled. Five samples support median/range summaries, not precise
percentiles or statistical significance claims. No CPU utilization or isolated
worker service-time measurement is claimed.

## End-to-end results

Seconds and MiB are rounded for display. Throughput is unique successful jobs /
median end-to-end seconds. Speedup is sequential median / backend median;
efficiency is speedup / workers.

### Small workload

[Raw observations](small.json).

| Backend | Median seconds | Observed range | Jobs/s | Speedup | Efficiency | Peak tree RSS MiB | Peak coordinator MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| sequential | 0.159 | 0.157..0.190 | 201.5 | 1.000x | 100.0% | 31.27 | 30.80 |
| process-2 | 0.268 | 0.232..0.408 | 119.2 | 0.592x | 29.6% | 110.09 | 31.05 |
| process-4 | 0.268 | 0.239..0.288 | 119.4 | 0.592x | 14.8% | 170.91 | 31.27 |
| process-8 | 0.300 | 0.286..0.367 | 106.6 | 0.529x | 6.6% | 289.67 | 31.77 |

### Substantial workload

[Raw observations](substantial.json).

| Backend | Median seconds | Observed range | Jobs/s | Speedup | Efficiency | Peak tree RSS MiB | Peak coordinator MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| sequential | 2.069 | 2.023..2.093 | 30.9 | 1.000x | 100.0% | 36.72 | 36.72 |
| process-2 | 1.321 | 1.311..1.350 | 48.4 | 1.566x | 78.3% | 125.02 | 39.23 |
| process-4 | 1.453 | 1.428..1.501 | 44.0 | 1.423x | 35.6% | 199.83 | 44.72 |
| process-8 | 1.685 | 1.639..1.819 | 38.0 | 1.228x | 15.3% | 346.12 | 52.17 |

Sequential wins for the small workload. On this substantial workload, two
workers reduce median runtime from 2.069 s to 1.321 s, a 1.566x speedup.
Four/eight workers remain faster than sequential but are slower than two. More
workers also increase process-tree and coordinator RSS. These observations do
not establish that two workers are optimal for other workloads or machines.

## Component observations and interpretation

The substantial runs separately record coordinator intervals:

| Backend | CSV load s | Backend iterator s | Detailed export s | First outcome s |
| --- | ---: | ---: | ---: | ---: |
| sequential | 0.019 | 1.242 | 0.678 | 0.013 |
| process-2 | 0.019 | 0.293 | 0.880 | 0.158 |
| process-4 | 0.019 | 0.399 | 0.920 | 0.242 |
| process-8 | 0.019 | 0.708 | 0.827 | 0.400 |

Backend iterator time includes startup, compute/IPC waits, collection, and
shutdown while the coordinator advances the iterator. Detailed export includes
result conversion, JSON, hashing, metrics, and file publication. First-outcome
latency overlaps backend time, so it must not be added again. Component medians
need not sum to the median end-to-end total; imports/finalization also contribute.
Workers can compute during export, so these intervals do not isolate CPU costs.

The measured serial exporter costs about 0.68..0.92 s for the substantial job
set, and first-outcome delay grows from 0.013 s sequentially to 0.400 s with eight
workers. The code also transfers full detailed outcomes through IPC and keeps
separate dataset copies. **Inference:** serial coordinator work and increasing
startup/transfer/coordination costs plausibly limit scaling here. This is not a
CPU profile identifying a dominant bottleneck. Isolating those causes and changing
representation or batching belongs to the measured Phase 6 optimization work.

## Coordinator memory versus job count

Fixed at 64 bars; five repetitions after warm-up for sequential and process-2.
The 32-job values come from the small workload. Config prefixes are stable, but
larger counts use additional SMA windows and then cash batches; these are memory
observations, not a constant-work-per-job speedup comparison.

| Jobs | Sequential coordinator MiB | Process-2 coordinator MiB | Process-2 tree MiB |
| --- | ---: | ---: | ---: |
| 32 | 30.80 | 31.05 | 110.09 |
| 128 | 31.34 | 31.59 | 110.27 |
| 2,048 | 37.98 | 38.52 | 117.23 |

[128-job raw observations](memory-128.json),
[2,048-job raw observations](memory-2048.json).
Coordinator memory grows because jobs, membership, manifests, and compact index
records remain O(jobs). Detailed results are streamed and buffered within the
in-flight bound; each simulation/export still uses O(bars) memory. These finite
samples do not prove constant memory or replace the submission/retention tests.

## Pilot evidence

[Pilot small](pilot-small.json) and [pilot substantial](pilot-substantial.json)
retain one timed sample after each configuration's warm-up. They selected the
fixed workload sizes; they are excluded from the five-sample tables above.
Their recorded checkout is dirty and revision precedes the source milestone;
installed-source identity is recorded explicitly. No failed pilot trials occurred.

## Reproduction

Use this source revision, or the Phase 2 checkout with the same installed-source
hash. For the recorded tool environment use CPython 3.14.6 on macOS arm64:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --requirement requirements/benchmark.lock
python -m pip install --no-build-isolation --no-deps --editable .
python -m pip check

python -m distributed_backtester.benchmark run --bars 64 --jobs 32 \
  --workers 2 4 8 --repetitions 5 --output-dir artifacts/phase2-small
python -m distributed_backtester.benchmark run --bars 4096 --jobs 64 \
  --workers 2 4 8 --repetitions 5 --output-dir artifacts/phase2-substantial
python -m distributed_backtester.benchmark run --bars 64 --jobs 128 \
  --workers 2 --repetitions 5 --output-dir artifacts/phase2-memory-128
python -m distributed_backtester.benchmark run --bars 64 --jobs 2048 \
  --workers 2 --repetitions 5 --output-dir artifacts/phase2-memory-2048
```

The four benchmark commands were executed with `.venv/bin/python` on the declared
host, after checking the exact version snapshot with
`pip install --no-cache-dir --no-index --no-deps --requirement requirements/benchmark.lock`.
Every destination must be fresh. Other platforms can install `.[dev,benchmark]`,
record their own versions/hardware, and choose supported workers; their timings
are separate measurements. Python/OS versions and workload/source digests matter
for reproducing semantic artifacts. Restart Python after source edits and keep
source fixed during a run. Timing values will vary.

Full local per-trial artifacts/stdout/stderr remain under `artifacts/phase2-*` and
are ignored by Git. The tracked JSON reports are verbatim copies of final reports,
retaining all raw samples and warm-ups. They include workload parameters/digests,
implementation identity, experiment IDs, semantic hashes, environment/hardware,
component timings, ranges, and memory traces. Regenerate detailed artifacts with
the commands above. See [Phase 2 report](../../phase-2-report.md).

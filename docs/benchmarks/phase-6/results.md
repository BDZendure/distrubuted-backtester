# Phase 6 measured results

Measurements were collected on CPython 3.14.6, macOS 26.6.1 arm64, Apple M3 Pro, 11 available/physical cores and 18 GiB RAM. Ray 2.59.0 and psutil 7.2.2 are pinned; [the environment snapshot](../../../requirements/performance-benchmark.lock) records the other versions.

Baseline clean commit `58837ac10216dcc2379d0b7ef0f5f3779466e4f7`; candidate `df294d4788673e0180d0dc67715c955407a95372`. [Baseline](baseline-installation.json) and [candidate](candidate-installation.json) records verify all 42 wheel package files and separately record build/wheel/source identities. Installed-wheel Git fields are null; `revision_clean: false` in benchmark reports means Git cleanliness was unavailable, not that the clean recorded build was dirty.

Every workload uses seven configurations, one excluded warmup and three timed fresh-process repetitions each. All detailed outputs remained available through the independently validating comparison. That is 112 verified trials and 8,960 backtest executions including warmups, with 128 or 32 distinct jobs per experiment. Repetitions are not extra distinct strategy configurations.

The timer includes imports, CSV load, setup, simulation, result transport, detailed export/finalization, backend shutdown and child exit. Generation and post-exit validation are excluded. Configuration order rotates; revisions run consecutively, not randomized. OS caches, scheduling and frequency are uncontrolled. Three samples provide observed medians/ranges, not confidence intervals or stable regression thresholds.

The [environment preflight](pilots/environment-preflight.json) explains why the first small pair was excluded: Ray import exposes bundled Colorama metadata after header capture. Controlled runs pre-expose the same existing Ray bundled path in every process, keeping exact parent/child metadata checks. No package or dependency was changed. Pilot reports are retained under `pilots/` and excluded from every final table.

## Small: 256 bars, 128 jobs

[Baseline raw trials](baseline-small.json), [candidate raw trials](candidate-small.json), [fully verified comparison](small-comparison.json), [baseline manifest](baseline-small-manifest.json), [candidate manifest](candidate-small-manifest.json). Seed 1729, BENCH/raw, 10 bps fees, unchanged complete result detail.

| Backend | Before median s [min, max] | After median s [min, max] | Before jobs/s | After jobs/s | Optimization speedup | After backend speedup | After efficiency |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| sequential | 0.377949 [0.373837, 0.393008] | 0.308797 [0.305728, 0.309118] | 338.670 | 414.512 | 1.224x | 1.000x | 100.00% |
| process-2 | 0.375537 [0.371223, 0.375807] | 0.374458 [0.361784, 0.382012] | 340.845 | 341.827 | 1.003x | 0.825x | 41.23% |
| process-4 | 0.373069 [0.370000, 0.383048] | 0.359827 [0.359292, 0.360463] | 343.100 | 355.726 | 1.037x | 0.858x | 21.45% |
| process-8 | 0.433005 [0.411457, 0.433326] | 0.415561 [0.413949, 0.417218] | 295.608 | 308.017 | 1.042x | 0.743x | 9.29% |
| ray-2 | 4.622344 [4.609822, 4.632044] | 4.563762 [4.485495, 4.600401] | 27.692 | 28.047 | 1.013x | 0.068x | 3.38% |
| ray-4 | 4.564438 [4.509723, 4.644633] | 4.669421 [4.595741, 4.737209] | 28.043 | 27.412 | 0.978x | 0.066x | 1.65% |
| ray-8 | 4.849729 [4.805896, 5.018103] | 4.789417 [4.779253, 5.097599] | 26.393 | 26.726 | 1.013x | 0.064x | 0.81% |

Optimization speedup compares the same backend before/after. Backend speedup uses that revision's sequential median; efficiency divides it by workers. These ratios do not measure CPU utilization.

Full economic SHA-256: `8343f4a28d57d48210895bdbb62b91fc0fab08e846c75e6dcded09df32cb112c`. Dataset SHA-256: `11cf8aaa3575ec6b47b16f05a52bda92f50806593727d0880df816219f9100fc`.

## Substantial: 16,384 bars, 32 jobs

[Baseline raw trials](baseline-substantial.json), [candidate raw trials](candidate-substantial.json), [fully verified comparison](substantial-comparison.json), [baseline manifest](baseline-substantial-manifest.json), [candidate manifest](candidate-substantial-manifest.json). Seed 1729, BENCH/raw, 10 bps fees, unchanged complete result detail.

| Backend | Before median s [min, max] | After median s [min, max] | Before jobs/s | After jobs/s | Optimization speedup | After backend speedup | After efficiency |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| sequential | 3.821517 [3.818267, 3.837216] | 2.667269 [2.600845, 2.844892] | 8.374 | 11.997 | 1.433x | 1.000x | 100.00% |
| process-2 | 2.371537 [2.353554, 2.372341] | 2.369008 [2.284186, 2.392777] | 13.493 | 13.508 | 1.001x | 1.126x | 56.30% |
| process-4 | 2.474945 [2.467189, 2.508615] | 2.499210 [2.495846, 2.633546] | 12.930 | 12.804 | 0.990x | 1.067x | 26.68% |
| process-8 | 2.862338 [2.850932, 2.865852] | 2.841476 [2.788111, 2.878244] | 11.180 | 11.262 | 1.007x | 0.939x | 11.73% |
| ray-2 | 6.676220 [6.665053, 6.768143] | 7.146931 [7.097124, 7.191520] | 4.793 | 4.477 | 0.934x | 0.373x | 18.66% |
| ray-4 | 6.733161 [6.673501, 6.777959] | 7.026772 [6.895134, 7.060065] | 4.753 | 4.554 | 0.958x | 0.380x | 9.49% |
| ray-8 | 7.050624 [6.861357, 7.087791] | 7.142382 [6.978246, 7.165010] | 4.539 | 4.480 | 0.987x | 0.373x | 4.67% |

Optimization speedup compares the same backend before/after. Backend speedup uses that revision's sequential median; efficiency divides it by workers. These ratios do not measure CPU utilization.

Full economic SHA-256: `b964fe76aac3caa8af0724ad864f1a55728fe0fa17d2b2122261af664dbd5fa9`. Dataset SHA-256: `b07d8ee9be0d47f5d0d67d7588224e0498d8a51794ad8853e2b56e3152e396de`.

## Interpretation

The sequential median fell 18.30% on the small input and 30.20% on the substantial
input. Substantial multiprocessing changed by less than 1%; cold Ray was
1.30-7.05% slower in these samples. Parallel changes were small and mixed. Two processes were fastest for substantial inputs; four
and eight were slower. On small inputs the optimized sequential backend beat
all parallel configurations.

The reduced CPU work is supported by scoped profiles and deterministic access/
Fraction-construction tests. Complete parallel runs still pay startup, data/result
transport and detailed export costs. Their component wall timings overlap and do
not isolate worker CPU. Cache/scheduling/order variation prevents assigning a
small measured Ray change to the optimization itself.

## Memory and CPU limits

All primary trial private USS peaks were unavailable. Scoped denial or incomplete ownership coverage invalidates the aggregate peak; null is retained rather than replaced with RSS or zero. Per-process RSS inventories remain in the raw trials; shared mappings are not summed. Ray object-store configured capacity was 134,217,728 bytes, a logical reservation rather than physical consumption. Live plasma occupancy was not observed.

The [profiling guide](../../performance.md) contains all 24 CPU/allocation/timing reports, own-process CPU seconds and lifetime RSS, traced current/peak/live differences, exact source/workload/result hashes, and the measured generator alternative. Scoped simulation median fell from 0.975629 to 0.669989 seconds; complete sequential experiment median fell from 1.903224 to 1.652131 seconds. These scopes exclude process startup and independent verification. Traced peaks were essentially unchanged; there is no material memory reduction claim.

## Reproduce and interpret

Use [performance instructions](../../performance.md) to build both historical wheels, verify their fingerprints, expose the identical Ray vendor path, run both absolute canonical roots without discarding details, and compare every result. The core [sample CSV](../../../examples/sma.csv) still finishes with 749.257000 USD, two fills and 1.743000 USD fees.

Measurements establish this host and these synthetic SMA jobs. They do not establish linear scaling, network performance, steady-state cluster throughput, real investment performance or production readiness. Full results agree across all measured revisions and backends.

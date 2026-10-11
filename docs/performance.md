# Phase 6 performance: profiling and before/after comparison

## Current checkpoint

The two focused Python optimizations are accepted after independent review.
The clean measurement baseline is commit
`58837ac10216dcc2379d0b7ef0f5f3779466e4f7`; the clean accepted candidate is
`df294d4788673e0180d0dc67715c955407a95372`. Both archived wheels contain
42 package files and use release `0.7.0`. The candidate's installed package
source hash matches the `optimized` scoped reports below.

Final local verification recorded 1,397 tests passing in 139.41 seconds, plus
Ruff lint/format, strict mypy and dependency checks. Both fresh-process backend
matrices passed full-result comparison; raw runtimes, throughput, speedups,
efficiency and memory coverage are in [recorded results](benchmarks/phase-6/results.md).
The scoped timings below describe their separate in-process boundaries.

## Run the standalone profiling CLI

From the repository directory, use the Python environment where the package is
installed. Profiling uses standard-library tools and needs no new runtime
dependency. It is a separate module CLI:

```bash
python -m distributed_backtester.profiling --help

for scope in simulation experiment
do
  python -m distributed_backtester.profiling run \
    --bars 65536 --jobs 4 --seed 1729 \
    --kind cpu --scope "$scope" \
    --output-dir "/private/tmp/phase6-profile-example-cpu-$scope"

  python -m distributed_backtester.profiling run \
    --bars 65536 --jobs 4 --seed 1729 \
    --kind memory --scope "$scope" \
    --output-dir "/private/tmp/phase6-profile-example-memory-$scope"

  python -m distributed_backtester.profiling run \
    --bars 65536 --jobs 4 --seed 1729 \
    --kind timing --scope "$scope" --repetitions 5 \
    --output-dir "/private/tmp/phase6-profile-example-timing-$scope"
done
```

Use these commands to reproduce the scoped measurements.
Each output directory must be new. Do not create the final output directory in
advance or rerun against an existing path. Use fresh names if these already exist.
The profiler intentionally rejects output inside the source checkout, inside
the installed package, and protected version-control paths. Use an absolute,
canonical path outside the checkout, such as a fresh child of `/private/tmp`.
For a generated temporary parent, use
`Path(tempfile.mkdtemp(dir="/private/tmp")).resolve()` and then a new child path.

CPU and memory each require one repetition, which is the default. Timing accepts
a positive repetition count. Bars/jobs must be positive integers; the default
seed is 1729. The generator supplies synthetic `BENCH`, raw USD prices, 10 bps
fees, and distinct SMA configurations. This is generated input rather than the
five-row example CSV or downloaded market data. Keep installed package source
fixed for the entire run.

| Scope | Inside the measured operation | Outside the measured operation |
| --- | --- | --- |
| `simulation`, the default | Fresh sequential `run_backtest` calls for all jobs; their complete results are retained through measurement | Dataset generation and CSV loading; independent complete-result conversion, verification, and hashing; report publication |
| `experiment` | CSV loading, job construction, sequential simulation, detailed JSON export, and experiment finalization | Dataset generation; independent complete-snapshot verification and complete-result hashing; report publication |

Neither scope includes Python process startup, package imports before the call,
a process pool, or Ray startup. Repeated timings use the same interpreter.
OS caches are uncontrolled. There is no separately excluded warmup in this
profiling CLI; all five timing repetitions enter the median.

## Read each kind of observation

| Kind or field | Interpretation |
| --- | --- |
| `cpu` | cProfile instruments calls and writes `run-1.pstats` plus per-function filename, line, name, primitive/total call counts, self time, and cumulative time |
| `memory` | tracemalloc observes traced Python allocations in this interpreter |
| `timing` | No cProfile or tracemalloc instrumentation is enabled by the tool; `elapsed_seconds` uses `perf_counter` |
| `cpu_seconds` | `process_time` for the running process, this interpreter only; instrumentation overhead is present in CPU/memory runs |
| `memory.peak_rss_bytes` | The running process's lifetime RSS high water from `RUSAGE_SELF`, normalized to bytes where supported |

**cProfile cumulative entries overlap.** A caller's cumulative time contains its
callees' time. SMA `on_close` calls history indexing, and `run_backtest` contains
strategy and metric work. Do not add those cumulative entries to estimate total
time. Self time excludes subcalls. Instrumented elapsed durations identify costs;
the separate uninstrumented timing runs provide scoped runtime comparisons.

**tracemalloc current, peak, and live differences describe different things.**
`current_traced_bytes` is memory still traced at the end of the operation.
`peak_traced_bytes` is the highest traced live-byte count since the peak reset
before the operation. Both counters are captured before the final snapshot.
`live_differences` compares the before/after live snapshots by traceback:
`size_diff_bytes` and `count_diff` are net live changes. They do not count all
allocations made and freed along the way. They do not measure all native
allocations, worker heaps, Ray's object store, or private physical memory.

Simulation deliberately retains every complete result for subsequent hashing.
Its CSV dataset was loaded before tracing. Experiment tracing includes loading,
but results follow the existing sequential export/release behavior. Lower current
bytes after an experiment do not mean it allocated only that many bytes.

Own-process CPU and lifetime RSS are not worker aggregates. RSS may include work
before the measured operation and memory used by instrumentation; it is not a
workload-only peak or an operation delta. Unsupported observations retain
`available: false`, `null`, and a reason. Missing coverage is not zero usage.

## Reports, identity, and failure handling

Each root retains `data.csv` and `report.json`. CPU also retains raw pstats;
experiment scope retains `experiment-N/` with manifest, detailed results,
execution record, and index. The report records implementation/environment,
workload/config/data hashes, method, raw repetitions, and complete-result hashes.

A successful profile requires unchanged package/data bytes and equal complete
results across repetitions. Simulation hashes all `result_to_dict` records in
canonical configuration order. Experiment validates a complete successful
snapshot and hashes the same complete records. That equality covers fills,
order outcomes, the equity path, final portfolio, metrics, policies, and dataset
provenance. It does not prove behavior on every possible input.

Known validation/file/runtime errors return CLI status 2; success returns 0.
The tool rejects conflicting active caller instrumentation, including a profiler,
tracemalloc, or trace callback for timing. After a workload failure it attempts to
publish a failed report and preserves the primary exception; an unwritable
destination can prevent report publication. A failed run makes no successful
complete-result fingerprint claim.

## Scoped evidence and the alternatives

The 24 raw reports cover four variants, three kinds, and two scopes. All report
success with source unchanged during each run, the same workload definition,
and the same complete-result digest:

- Bars/jobs: 65,536 bars and four jobs, seed 1729, 10 bps fees, `BENCH/raw`.
- SMA windows: `(1, 2)`, `(2, 3)`, `(3, 4)`, `(4, 5)`; each starts with
  `1000000000` micro-USD ($1,000).
- Data SHA-256: `380f916704ad8012e48e77129620a55b25d5c37126e1f9c67cca59267f94869e`.
- Config SHA-256: `38fc4566d1c14677c3932678d2c5e63a7bf7b1d7ab26e029578a7fcf3109094e`.
- Workload SHA-256: `d8e5ffa851bee93a461d43208c2c68ebfc4ff77d7df9857731686c9537660e2e`.
- Complete-result SHA-256:
  `ab780eb44724aa83ec754ab5c0552ee845bbde2a9486d090a5a8e09797dbbad9`.

All reports record CPython 3.14.6, arm64 macOS 26.6.1, 11 available/logical cores,
identical installed distribution records, and identical thread environment.
Git observations differ as expected: the baseline wheel has no automatic checkout
Git record; the exploratory variants record their dirty checkout. The source
hash identifies each measured variant independently of its then-current commit.

| Variant | Installed package source SHA-256 |
| --- | --- |
| baseline | `ab0c29cffd82af10c03146da8182e025fa11d114c328c09a3bb0b06eb303a789` |
| generator | `d2c7a10ae7fd4d57c0345fe094ebb39faf74c0a8693d4b7365caa1939842b261` |
| native-history | `ee5d2381dcb8d83904110739f2731f239866e96264da92b2fcf6fd6eeb624562` |
| optimized | `16a1a54602a060cea2fddaaae827b9aff99d87ccd5fbb6761a8b1731a5b834ad` |

### Uninstrumented scoped timing

Each median is recomputed from five raw repetitions in its own interpreter.
Medians below retain the reported Python float values; ranges are rounded to
six decimal places, in seconds.

| Variant | Simulation median (s) | Simulation min to max (s) | Experiment median (s) | Experiment min to max (s) |
| --- | --- | --- | --- | --- |
| Original algorithm | 0.975629291991936 | 0.972241 to 1.211939 | 1.903223999979673 | 1.886281 to 1.942950 |
| Bounded backing-index generator | 0.8719195839948952 | 0.867644 to 0.881137 | 1.8493130419810768 | 1.829913 to 1.858415 |
| Native history slicing | 0.8051267090195324 | 0.802683 to 0.809550 | 1.7660370000230614 | 1.757131 to 1.819325 |
| Native slicing + integer drawdown comparisons | 0.6699892920150887 | 0.660115 to 0.673648 | 1.6521307920047548 | 1.630219 to 1.727610 |

The generator alternative improved both scoped medians relative to the original,
but native history slicing had lower medians than the generator. Native slicing
was selected. The combined candidate then added integer drawdown comparisons.
The original-to-combined scoped ratios are about 1.456x for simulation and 1.152x
for experiment, corresponding to about 31.33% and 13.19% lower scoped median
runtime. These are observations from this synthetic in-process workload. They
are not final fresh-process backend results or evidence of parallel scaling.

### CPU observations that motivated the changes

These entries come from the instrumented simulation reports and are seconds.
The strategy and history cumulative columns overlap.

| Variant | History total calls | History cumulative (s) | SMA cumulative (s) | Metrics cumulative (s) |
| --- | --- | --- | --- | --- |
| baseline | 2,101,158 | 1.066036 | 1.606581 | 0.649799 |
| generator | 528,364 | 0.643885 | 1.177914 | 0.648614 |
| native-history | 528,364 | 0.395181 | 0.941043 | 0.664398 |
| optimized | 528,364 | 0.424348 | 1.000544 | 0.091127 |

1. **Bounded native slicing in `models.py`.** `CloseHistory` normalizes slices
   against its captured prefix length. For exact built-in list/tuple backing
   and an exact `CloseHistory`, it uses Python's built-in slice operation and
   returns a tuple. An empty normalized range returns immediately; reverse
   slices translate the before-zero stop correctly. Retained views therefore
   cannot expose subsequently appended observations. Custom sequences and
   subclasses retain the per-element fallback. The generator alternative used
   bounded backing indices to avoid repeated view indexing, but retained more
   Python iteration work than built-in slicing.
2. **Integer cross products in `metrics.py`.** Instead of constructing and
   reducing a `Fraction` at every equity point, track the largest loss/peak pair
   and compare it with integer multiplication. For positive peaks,
   `loss / peak > best_loss / best_peak` is exactly equivalent to
   `loss * best_peak > best_loss * peak`. Construct the final `Fraction` once.
   Initial-capital anchoring, per-point validation, return, fees, and rational
   output remain part of the contract.

Here, "native slicing" means Python's existing built-in list/tuple operation.
No C++ extension, compiler toolchain, new runtime dependency, array
representation, or worker-sharing mechanism was introduced.

### Memory observations

These are single instrumented observations per variant/scope, in bytes.

| Variant | Scope | Current traced Python bytes | Peak traced Python bytes | Own-process lifetime peak RSS bytes |
| --- | --- | --- | --- | --- |
| baseline | simulation | 34,748,360 | 39,037,616 | 259,424,256 |
| baseline | experiment | 75,364 | 68,127,508 | 208,273,408 |
| generator | simulation | 34,748,312 | 39,037,568 | 259,473,408 |
| generator | experiment | 76,135 | 68,127,149 | 198,180,864 |
| native-history | simulation | 34,748,476 | 39,037,732 | 260,079,616 |
| native-history | experiment | 75,485 | 68,127,095 | 208,388,096 |
| optimized | simulation | 34,747,884 | 39,037,140 | 251,265,024 |
| optimized | experiment | 76,048 | 68,126,749 | 198,328,320 |

Current/peak traced bytes are nearly unchanged across these variants. This
evidence does not establish a material reduction in retained memory or total
allocation frequency. Lifetime RSS observations are not per-operation memory
savings and must not be substituted for worker or cluster memory.

### Raw report authority

These archived reports retain the raw observations and full result digests.
CPU directories also contain the raw pstats files. Temporary detailed profile
exports were removed after full-result verification; the removal record is
[retained separately](benchmarks/phase-6/profile-export-cleanup.json).

| Variant | CPU reports | Memory reports | Timing reports |
| --- | --- | --- | --- |
| baseline | [simulation](benchmarks/phase-6/profiles/baseline-cpu-simulation/report.json) / [experiment](benchmarks/phase-6/profiles/baseline-cpu-experiment/report.json) | [simulation](benchmarks/phase-6/profiles/baseline-memory-simulation/report.json) / [experiment](benchmarks/phase-6/profiles/baseline-memory-experiment/report.json) | [simulation](benchmarks/phase-6/profiles/baseline-timing-simulation/report.json) / [experiment](benchmarks/phase-6/profiles/baseline-timing-experiment/report.json) |
| generator | [simulation](benchmarks/phase-6/profiles/generator-cpu-simulation/report.json) / [experiment](benchmarks/phase-6/profiles/generator-cpu-experiment/report.json) | [simulation](benchmarks/phase-6/profiles/generator-memory-simulation/report.json) / [experiment](benchmarks/phase-6/profiles/generator-memory-experiment/report.json) | [simulation](benchmarks/phase-6/profiles/generator-timing-simulation/report.json) / [experiment](benchmarks/phase-6/profiles/generator-timing-experiment/report.json) |
| native-history | [simulation](benchmarks/phase-6/profiles/native-history-cpu-simulation/report.json) / [experiment](benchmarks/phase-6/profiles/native-history-cpu-experiment/report.json) | [simulation](benchmarks/phase-6/profiles/native-history-memory-simulation/report.json) / [experiment](benchmarks/phase-6/profiles/native-history-memory-experiment/report.json) | [simulation](benchmarks/phase-6/profiles/native-history-timing-simulation/report.json) / [experiment](benchmarks/phase-6/profiles/native-history-timing-experiment/report.json) |
| optimized | [simulation](benchmarks/phase-6/profiles/optimized-cpu-simulation/report.json) / [experiment](benchmarks/phase-6/profiles/optimized-cpu-experiment/report.json) | [simulation](benchmarks/phase-6/profiles/optimized-memory-simulation/report.json) / [experiment](benchmarks/phase-6/profiles/optimized-memory-experiment/report.json) | [simulation](benchmarks/phase-6/profiles/optimized-timing-simulation/report.json) / [experiment](benchmarks/phase-6/profiles/optimized-timing-experiment/report.json) |

## Reproduce the wheel comparison with retained benchmark roots

The accepted build records are
[baseline installation](benchmarks/phase-6/baseline-installation.json)
and [candidate installation](benchmarks/phase-6/candidate-installation.json).

| Identity | Baseline | Accepted candidate |
| --- | --- | --- |
| Clean build commit | `58837ac10216dcc2379d0b7ef0f5f3779466e4f7` | `df294d4788673e0180d0dc67715c955407a95372` |
| Package files | 42 | 42 |
| Engine release | `distributed-backtester/0.7.0:daily-long-only-v1` | `distributed-backtester/0.7.0:daily-long-only-v1` |
| Wheel SHA-256 | `c836b2856e63ecdf5d89a5ebb7b395cd1cb301fa2d95f6d464b9800f4145c77c` | `48ee685432f04d123472e27dee566d9c6f98553ddf839986bed40b42d890a62c` |

The installed baseline/candidate package source hashes are the `baseline` and
`optimized` hashes above. Keep wheel archives and installed bytes immutable
through all trials; do not rebuild from changed source halfway through a matrix.
Wheel installations have no automatic build-commit provenance, so retain the
separate build/installation records and the package-byte verification.

### Controlled Ray metadata environment

The first small baseline/candidate pair was a pilot and is excluded from final
before/after results. The strict comparer correctly rejected it because importing
Ray adds its bundled `ray/thirdparty_files` directory to `sys.path`. That exposed
bundled `colorama==0.4.6` distribution metadata only after the parent benchmark
header had been captured, so the parent's header and child environment differed.
The raw pilot reports remain at `docs/benchmarks/phase-6/pilots/environment-preflight.json`
as environment diagnostics.

The formal reruns use `baseline-controlled` and `candidate-controlled` roots.
Before starting either parent or any child, expose the same Ray vendor directory
on `PYTHONPATH` alongside the selected wheel site. Derive it from
`importlib.util.find_spec("ray").origin`, taking the package parent and
`thirdparty_files`, then resolve the path. A preflight compares the actual
distribution metadata before and after Ray import and requires equality on both
sites. This is an environment control: no package source changes or dependency
installs were introduced to address the pilot rejection. The accepted commit
and package source hashes remain unchanged.

Use **one Python executable and one dependency environment** for both versions.
Install the archived project wheels into separate new target sites with no
dependency installation. Select the project site plus the same Ray vendor directory using `PYTHONPATH` and invoke
the module CLI with that same interpreter. Existing installed console scripts
can point at a different interpreter.

**Absolute canonical benchmark roots are mandatory for before/after comparison,
from the moment the benchmark is produced.** The comparer binds every captured
child command to its root and rejects symlink parents. A relative root accepted
by `ray_benchmark` cannot be repaired for this comparer merely by resolving its
path afterward. On macOS, use resolved `/private/tmp` paths rather than the
`/tmp` symlink alias. General relative-path benchmark commands remain valid when
no cross-revision comparison is requested.

The following macOS example starts in the repository directory with
`.venv` already containing the matching benchmark/Ray dependencies. It creates
only a canonical container parent; all sites, benchmark roots, and comparison
files beneath it are fresh. Build the historical wheels from the recorded commits first; wheel archive
hashes can change with build timestamps, while package source fingerprints
remain the identity check.

```bash
PYTHON_BIN="$PWD/.venv/bin/python"
REPRO_ROOT="$("$PYTHON_BIN" -c 'from pathlib import Path; import tempfile; print(Path(tempfile.mkdtemp(prefix="phase6-repro-", dir="/private/tmp")).resolve())')"
BASELINE_SITE="$REPRO_ROOT/baseline-site"
CANDIDATE_SITE="$REPRO_ROOT/candidate-site"
RAY_VENDOR="$("$PYTHON_BIN" -c 'from importlib.util import find_spec; from pathlib import Path; spec = find_spec("ray"); assert spec is not None and spec.origin; print((Path(spec.origin).resolve().parent / "thirdparty_files").resolve())')"

mkdir "$REPRO_ROOT/baseline-src" "$REPRO_ROOT/candidate-src"
git archive 58837ac10216dcc2379d0b7ef0f5f3779466e4f7 | tar -x -C "$REPRO_ROOT/baseline-src"
git archive df294d4788673e0180d0dc67715c955407a95372 | tar -x -C "$REPRO_ROOT/candidate-src"
"$PYTHON_BIN" -m build --wheel --no-isolation \
  --outdir "$REPRO_ROOT/baseline-dist" "$REPRO_ROOT/baseline-src"
"$PYTHON_BIN" -m build --wheel --no-isolation \
  --outdir "$REPRO_ROOT/candidate-dist" "$REPRO_ROOT/candidate-src"

"$PYTHON_BIN" -m pip install --no-index --no-deps \
  --target "$BASELINE_SITE" \
  "$REPRO_ROOT/baseline-dist/distributed_backtester-0.7.0-py3-none-any.whl"
"$PYTHON_BIN" -m pip install --no-index --no-deps \
  --target "$CANDIDATE_SITE" \
  "$REPRO_ROOT/candidate-dist/distributed_backtester-0.7.0-py3-none-any.whl"

for SITE in "$BASELINE_SITE" "$CANDIDATE_SITE"
do
  PHASE6_SELECTED_SITE="$SITE" PYTHONPATH="$SITE:$RAY_VENDOR" "$PYTHON_BIN" - <<'PY'
import os
from pathlib import Path
import distributed_backtester
from distributed_backtester.provenance import capture_environment, current_implementation

site = Path(os.environ["PHASE6_SELECTED_SITE"]).resolve()
expected = {
    "baseline-site": "ab0c29cffd82af10c03146da8182e025fa11d114c328c09a3bb0b06eb303a789",
    "candidate-site": "16a1a54602a060cea2fddaaae827b9aff99d87ccd5fbb6761a8b1731a5b834ad",
}
assert Path(distributed_backtester.__file__).resolve().is_relative_to(site)
identity = current_implementation()
assert identity.engine_id == "distributed-backtester/0.7.0:daily-long-only-v1"
assert identity.source_sha256 == expected[site.name]
print("Verified imported package/source:", identity)

before = capture_environment()["distributions"]
import ray
after = capture_environment()["distributions"]
if before != after:
    raise SystemExit("Ray import changed distribution metadata")
print("Distribution metadata stable before/after Ray import")
PY
done

PYTHONPATH="$BASELINE_SITE:$RAY_VENDOR" "$PYTHON_BIN" -m distributed_backtester.ray_benchmark run \
  --bars 256 --jobs 128 --seed 1729 --workers 2 4 8 --repetitions 3 \
  --interval-seconds 0.05 --output-dir "$REPRO_ROOT/baseline-controlled-small"
PYTHONPATH="$CANDIDATE_SITE:$RAY_VENDOR" "$PYTHON_BIN" -m distributed_backtester.ray_benchmark run \
  --bars 256 --jobs 128 --seed 1729 --workers 2 4 8 --repetitions 3 \
  --interval-seconds 0.05 --output-dir "$REPRO_ROOT/candidate-controlled-small"

PYTHONPATH="$BASELINE_SITE:$RAY_VENDOR" "$PYTHON_BIN" -m distributed_backtester.ray_benchmark run \
  --bars 16384 --jobs 32 --seed 1729 --workers 2 4 8 --repetitions 3 \
  --interval-seconds 0.05 --output-dir "$REPRO_ROOT/baseline-controlled-substantial"
PYTHONPATH="$CANDIDATE_SITE:$RAY_VENDOR" "$PYTHON_BIN" -m distributed_backtester.ray_benchmark run \
  --bars 16384 --jobs 32 --seed 1729 --workers 2 4 8 --repetitions 3 \
  --interval-seconds 0.05 --output-dir "$REPRO_ROOT/candidate-controlled-substantial"

PYTHONPATH="$CANDIDATE_SITE:$RAY_VENDOR" "$PYTHON_BIN" -m distributed_backtester.performance compare \
  --baseline "$REPRO_ROOT/baseline-controlled-small" \
  --candidate "$REPRO_ROOT/candidate-controlled-small" \
  --output "$REPRO_ROOT/comparison-small.json"
PYTHONPATH="$CANDIDATE_SITE:$RAY_VENDOR" "$PYTHON_BIN" -m distributed_backtester.performance compare \
  --baseline "$REPRO_ROOT/baseline-controlled-substantial" \
  --candidate "$REPRO_ROOT/candidate-controlled-substantial" \
  --output "$REPRO_ROOT/comparison-substantial.json"
```

Verify the imported package path and complete source fingerprint against the
installation record before measuring. Require the metadata preflight to pass on both sites. Use
the same dependency versions, Python
path, hardware, core availability, thread controls, seed, configuration generator,
worker/resource policy, observer interval, and output detail. Unsupported worker
counts are rejected; a reduced worker matrix must be declared and match both
versions.

Every root includes sequential, process-2/4/8, and Ray-2/4/8: seven configurations.
Each has one excluded warmup and three timed trials, with rotating order.
That is 28 trials per root, 56 per workload pair, and 112 across both pairs.
Retain all detailed outputs, including warmups, until comparison finishes.
Do not pass `--discard-trial-results`. The comparer accepts retained
`ray_benchmark` roots, not the six profiler roots or a bare `report.json` path.

The backend timer covers fresh process startup/imports, CSV loading, jobs, pool
or local cluster setup, data placement/readiness, calculation, detailed export,
finalization, shutdown, and child exit. Dataset generation, parent expected
manifest construction, post-exit verification and orphan cleanup are outside
that timer. Memory inventory excludes the observer; concurrent sampling can
affect measured wall time. OS caches remain uncontrolled. Cold local Ray does not
measure a prestarted cluster or a multiple-machine deployment.

## What the comparer certifies

`distributed_backtester.performance compare` rereads the reports, input CSV,
captured expected manifests, each warmup/timed experiment, and every detailed
artifact. It verifies the producer identity, commands, child/parent environment,
resource settings, successful complete membership, artifact digests, and full
snapshot validity before publishing.

Implementation-dependent job/experiment IDs may differ across revisions.
Each producer is validated against its own identity. Complete results are then
compared in canonical configuration order, with only a validated engine release
identifier normalized if needed. Fills, outcomes, equity, portfolio, metrics,
configuration, dataset provenance, and accounting/strategy policies must agree.
No final-metric-only shortcut is used. The comparison retains both identities.

The comparer recomputes summaries from raw timed samples, excluding warmups:

| Metric | Definition |
| --- | --- |
| Median/range | Median, minimum, and maximum of each configuration's elapsed samples |
| Jobs per second | Unique job count / median seconds |
| Backend speedup | Same revision's sequential median / configuration median |
| Parallel efficiency | Backend speedup / worker count |
| Optimization speedup | Baseline configuration median / candidate configuration median |
| Runtime reduction (%) | `100 * (1 - candidate median / baseline median)` |

The output file must be new, outside both input roots and the installed package,
and use canonical parents without symlinks. Failed, incomplete, tampered,
discarded, semantically different, or mismatched-method/environment runs are
rejected. Publication happens only after verification.

Memory keeps the owned-local harness's limits: per-process RSS is retained
without a shared-page sum; aggregate private USS requires complete inventory
and successful reads. Missing actor coverage or denied reads leave it null.
Sampling can miss brief peaks; object-store reserved capacity and serialized
dataset bytes are logical quantities, and live object-store usage is unobserved.
Unavailable memory remains unavailable in the comparison.

## Final controlled matrix

Both comparisons passed: all 112 warmup/timed trials agree on every economic
result across revisions and all seven backends. [Raw trials and tables](benchmarks/phase-6/results.md)
include medians/ranges, throughput, optimization/backend speedups, efficiency,
and unavailable private-memory observations.

| Workload | Sequential before s | Sequential after s | Runtime reduction | Optimization speedup |
| --- | ---: | ---: | ---: | ---: |
| Small: 256 bars, 128 jobs | 0.377949 | 0.308797 | 18.30% | 1.224x |
| Substantial: 16,384 bars, 32 jobs | 3.821517 | 2.667269 | 30.20% | 1.433x |

Multiprocessing changed little; substantial process-4 was 0.98% slower. Cold
Ray medians on the substantial workload were 1.30-7.05% slower. These observations
show small and mixed parallel changes. Two processes were fastest
on the substantial workload; more workers did not give linear scaling. Startup,
result transfer, detailed export and scheduling costs still matter when the
simulation becomes cheaper. Component wall timings overlap and do not isolate
worker CPU, so they must not be added as independent costs.

Three timed repetitions and consecutive revision order limit causal conclusions
about the small parallel differences and Ray regressions. This is one host and
synthetic SMA workload, not a network or prestarted-cluster benchmark. Aggregate
private USS was unavailable in every primary trial; scoped Python allocation
peaks were essentially unchanged. There is no material memory saving claim.

See the [benchmark methodology](benchmarking.md),
[measured-performance decision](decisions/0008-measured-performance.md),
[beginner guide](clear%20explanation.md), and
[technical guide](technical-explanation.md) for related contracts.

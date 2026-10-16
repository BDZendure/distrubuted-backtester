# Phase 3: Distributed execution

**Status:** Complete, verified locally; Phase 4 awaits explicit approval.
**Release:** `0.4.0`. **Branch:** `feat/phase-3-distributed-execution`.

## Implemented

- Optional pinned Ray backend using the existing deterministic simulation worker.
- Fixed actor pool with explicit CPU and custom resource reservations.
- One dataset object reference and one decoded immutable dataset per actor.
- Bounded outstanding jobs, streamed exports, and backend-independent artifacts.
- Local cluster ownership and connection to an externally started GCS cluster.
- Worker/node diagnostics, source checks, readiness timeout, and fatal cleanup.
- Ray CLI options and instructions for matching installations across nodes.
- Cold comparison harness for sequential, process, and owned local Ray execution.
- Process ownership checks, raw memory coverage, declared-input verification, and
  artifact verification before optional detailed-result deletion.
- Optional Ray CI, dependency snapshot, ADR 0005, and synchronized beginner and
  technical explanations.

Main new files: `backends/ray.py`, `ray_benchmark.py`, `ray_measurement.py`,
`tests/test_ray*.py`, [cluster instructions](ray-clusters.md), and
[ADR 0005](decisions/0005-ray-actor-execution.md).

## Architecture

Each actor verifies its installed source identity during initialization, retains
the immutable dataset, and runs a fresh strategy/portfolio for each job. `ray.put`
places the dataset once, but Python dataclasses deserialize into each actor heap.
This does not provide zero-copy simulation data.

CPU/custom resources reserve actor lifetime capacity. The coordinator checks
complete actor slots per live node, waits for readiness, submits round-robin with
at most twice the worker count outstanding by default, and consumes whichever
result becomes ready. `SPREAD` is a placement preference. Round-robin queues can
leave another actor idle on uneven jobs; adaptive scheduling is future work.

Existing canonical jobs and the collector establish determinism. Worker count,
node IDs, paths, completion order, and timings do not enter semantic identity.
Operational diagnostics belong in `execution.json`. A successful final index is
published only after collection and backend shutdown. Ordinary simulation errors
produce structured job failures. Infrastructure/protocol/export errors abort with
partial files and no final index.

Actor restarts and task retries are explicitly zero. This phase has no durable
scheduler, resume, or recovery. The backend kills its actors and disconnects its
driver. It stops only a local cluster it started. Existing external clusters remain
owned by their original launcher. An already initialized driver is refused.

Ray 2.59.0 requires a small version-bound private adapter to close a terminated
macOS reaper's stdin after shutdown. Tests enforce that a live pipe is never closed.
Changing the Ray pin requires reviewing this adapter. Local node CPU/custom
capacity uses integral ceilings, while actor reservations may be fractional.

## Verification

Executed on macOS 26.6.1 arm64, CPython 3.14.6, Ray 2.59.0, and psutil 7.2.2.
The final full suite passes **695 tests**, with no failures or skips.

| Exact command | Actual result |
| --- | --- |
| `.venv/bin/python -m pytest --tb=short` | 695 passed in 67.43 s |
| `.venv/bin/python -m ruff check .` | All checks passed |
| `.venv/bin/python -m ruff format --check .` | 76 files already formatted after adding measured documentation |
| `.venv/bin/python -m mypy` | No issues in 51 source/test files |
| `.venv/bin/python -m pip check` | No broken requirements |
| `.venv/bin/python -m build` | Isolated wheel and source distribution built successfully |
| `.venv/bin/python /tmp/phase3-archive-check.py` | One-off local audit: 26 wheel files and 93 source/archive files exactly match the checkout; extras metadata correct |
| `git diff --check` | No whitespace errors |

Primary measurements and packaging checks are described below. Intermediate
failures were investigated rather
than weakening assertions. The baseline recursive process
memory test passes outside the sandbox, where macOS process enumeration is allowed.
Ray socket/process tests also require that execution context on this machine.

Real tests cover remote execution and equivalent artifacts, actor reuse, source
mismatch, fractional resources, external ownership, and placement on two local
Ray nodes. The two nodes share one physical Mac. Tests additionally kill a running
actor with `os._exit(23)` and inject a writer failure while actors are alive, then
check that no final index exists and owned actors/services have exited.

Pure transport injections cover malformed outcomes, timeouts, early close, and
bounded submission/reference release. These complement actual Ray runs. Five
in-memory negative controls independently showed the fault tests reject missing
protocol checks, missing cleanup/timeout, excessive submissions, and retained refs.

Independent benchmark review reproduced four defects: certification of a reused
actor PID, admission through a reused process group, incomplete discovery being
treated as complete memory coverage, and accepting changed first-trial input as
the experiment reference. All four were fixed with repository regression tests.
Independent re-review reran its seven original proofs unchanged: **7 passed**.
It also reran the focused pure subset: **61 passed, 2 deselected**, and found no
remaining important finding. The final full suite includes both subprocess tests.

### Installed wheel and example

Created `.venv-wheel-phase3-core` using `.venv/bin/python -m venv`, installed
`dist/distributed_backtester-0.4.0-py3-none-any.whl` with `--no-index --no-deps`,
and ran its isolated Python with working directory `/private/tmp`. Import paths
pointed into that environment; both `ray` and `psutil` were absent. The sample
backtest produced the expected **$749.257** final equity. Sequential and real
spawn sweeps each completed four jobs successfully.

Reinstalled the same wheel with `--no-index --no-deps --force-reinstall` into
`.venv-wheel-phase3`, which already had the pinned Ray extra, and ran a real
two-actor Ray sweep from `/private/tmp`. All four jobs succeeded. `semantic_digest`
verified every detailed artifact in all three wheel sweeps and returned:
`5e0819b5a2d5cc40dc6d19f51e00e23a4a7d27b11964393f6867c0a4797b0997`.
Experiment ID: `759ba8ba1e995b91fea3e3982c5be70f6be32b9e72ade924d371fe7c28c4803b`.
The installed source identity agrees with the checkout:
`02d6288f4a007b3d4c7c143b5cab53a91b0ca08711ede192b5ab20f305165fe8`.

After rebuilding with the measured reports, both wheel environments were
reinstalled with `--no-index --no-deps --force-reinstall`. Ran the one-off audit
`/Users/blakezhang/Documents/Dev/distrubuted-backtester/.venv-wheel-phase3-core/bin/python -I /tmp/phase3-wheel-final-check.py`
from `/private/tmp`: the reference example and all three sweeps passed again,
with the same verified digest and source identity above. These two `/tmp` scripts
are local audits, not repository tools. The archive audit opens the wheel/source
archive and compares every package, README/config/license, documentation, raw
report, test, CSV, and lockfile byte with the corresponding checkout file.
It also verifies all dependencies are conditional on extras and Ray is pinned.
Raw reports and captured manifests match local observations byte for byte.
The final Markdown audit checked 25 files and 120 local links, with none missing.
CI YAML parsed successfully, and both published numerical summaries recomputed
exactly from their raw runs. Remote CI and complete memory coverage are not claimed.

The final real harness smoke command was:

```bash
.venv/bin/python -m distributed_backtester.ray_benchmark run --bars 8 --jobs 3 \
  --workers 2 --repetitions 1 --interval-seconds 0.02 --discard-trial-results \
  --output-dir /tmp/phase3-ray-final-smoke
```

All six trials matched the captured parent manifest and semantic digest. Eighteen
verified details were explicitly removed; compact evidence remains. No Ray trial
needed post-exit kills. One process warmup left an individually revalidated owned
helper, which was killed after exit; cleanup is outside timing. Private USS peaks
were null with scoped denial evidence. This smoke is excluded from primary tables.

## Performance

The [measured results](benchmarks/phase-3/results.md) retain raw reports and
captured manifests from clean revision
`7ab3ff9d5261b30b3dcca57fedde84d040cc11c3` on the declared Mac. All **42 timed
trials and 14 warmups**, totaling **2,688 simulations**, succeeded and verified.
Workers were 2, 4 and 8; each configuration had three repetitions and one warmup.

| Workload | Sequential median | Process-2 median | Ray-2 median |
| --- | ---: | ---: | ---: |
| 32 jobs x 64 bars | 0.162 s | 0.234 s | 4.148 s |
| 64 jobs x 4,096 bars | 2.013 s | 1.374 s | 5.371 s |

Two processes achieve a measured **1.466x speedup** on the substantial workload.
Cold Ray is slower on both workloads. Reports include 4/8-worker observations,
ranges, jobs/s, parallel efficiency, first-outcome latency, and startup/export/
shutdown components. Startup and shutdown plausibly explain much of Ray's cold
cost; this inference is not a CPU profile. Warm-cluster and multiple-machine
performance remain unmeasured. Historical Phase 2 evidence is unchanged.

All private-memory peaks are unavailable: child USS reads were denied and actor
PIDs lacked independent group/session ownership proof for observer adoption.
There were zero complete memory samples. Raw per-process RSS is not summed.
Store capacity and payload length are logical quantities, not physical usage.
The conservative observer leaves missing actor coverage explicit; backend actor
execution/cleanup is validated separately. See the results for cleanup and scope.

## Known Limitations

- No multiple-machine run, network scaling measurement, or deployment automation.
- Nodes must already have matching Python, Ray, package, and installed source.
- No retries, job deadlines, durable commits, recovery, or resume.
- Actor readiness is bounded; Ray startup uses Ray's own internal timeouts.
- Every actor has a Python dataset copy and returns full detailed outcomes.
- No adaptive load balancing; CPU reservations are logical scheduling resources.
- Memory sampling can miss short-lived processes and peaks. This Mac denies
  child USS reads. Observer actor ownership proof also failed on this host, so
  complete actor coverage and private-memory peaks are unavailable.
- Object-store capacity and serialized payload size are logical measurements;
  live occupancy, physical shared memory, and remote memory are not measured.
- Local multi-node success is platform-specific; Ray describes macOS multi-node
  support as generally untested.
- All partial cluster-initialization failure modes have not been exercised.
- CI configuration is committed, but no remote CI run is claimed.
- Existing single-asset, long-only daily-bar and synthetic-universe limits remain.

## Reproduction Instructions

From the repository directory, create a fresh Python 3.12+ environment and install
`.[dev,benchmark,distributed]`. The observed version snapshot is
[requirements/ray-benchmark.lock](../requirements/ray-benchmark.lock); it is not
a portable cross-platform lock.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --editable ".[dev,benchmark,distributed]"
python -m pytest --tb=short
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m pip check
python -m build

distributed-backtester sweep --data examples/sma.csv --symbol SYNTH \
  --price-basis raw --fast 1 2 --slow 3 4 --initial-cash 1000 --fee-bps 10 \
  --backend ray --workers 2 --output-dir artifacts/phase3-demo-ray

python -m distributed_backtester.ray_benchmark run --bars 64 --jobs 32 \
  --workers 2 4 8 --repetitions 3 --discard-trial-results \
  --output-dir artifacts/phase3-small
python -m distributed_backtester.ray_benchmark run --bars 4096 --jobs 64 \
  --workers 2 4 8 --repetitions 3 --discard-trial-results \
  --output-dir artifacts/phase3-substantial
```

Use fresh output directories and worker counts supported by your host. Keep source
fixed and run one comparison at a time. Timing varies with hardware/background
activity. Detailed benchmark artifacts are verified before explicit deletion;
omit the discard flag to retain them for later independent verification.

### Reproduce the installed-wheel demonstration

Build first, then use a new environment and temporary directory. From the repo:

```bash
python -m build
python -m venv .venv-wheel-review
.venv-wheel-review/bin/python -m pip install --no-deps \
  dist/distributed_backtester-0.4.0-py3-none-any.whl
phase3_repo="$PWD"
phase3_demo="$(mktemp -d)"
cd "$phase3_demo"

"$phase3_repo/.venv-wheel-review/bin/distributed-backtester" backtest \
  --data "$phase3_repo/examples/sma.csv" --symbol SYNTH --price-basis raw \
  --fast 2 --slow 3 --initial-cash 1000 --fee-bps 10 --output example.json
"$phase3_repo/.venv-wheel-review/bin/distributed-backtester" sweep \
  --data "$phase3_repo/examples/sma.csv" --symbol SYNTH --price-basis raw \
  --fast 1 2 --slow 3 4 --fee-bps 10 --backend sequential --workers 1 \
  --output-dir sequential
"$phase3_repo/.venv-wheel-review/bin/distributed-backtester" sweep \
  --data "$phase3_repo/examples/sma.csv" --symbol SYNTH --price-basis raw \
  --fast 1 2 --slow 3 4 --fee-bps 10 --backend process --workers 2 \
  --output-dir process

"$phase3_repo/.venv-wheel-review/bin/python" -m pip install \
  "$phase3_repo/dist/distributed_backtester-0.4.0-py3-none-any.whl[distributed]"
"$phase3_repo/.venv-wheel-review/bin/distributed-backtester" sweep \
  --data "$phase3_repo/examples/sma.csv" --symbol SYNTH --price-basis raw \
  --fast 1 2 --slow 3 4 --fee-bps 10 --backend ray --workers 2 --output-dir ray
"$phase3_repo/.venv-wheel-review/bin/python" -I - <<'PY'
from pathlib import Path
from distributed_backtester.benchmark import semantic_digest
digests = [semantic_digest(Path(name)) for name in ("sequential", "process", "ray")]
assert len(set(digests)) == 1
print(digests[0])
PY
```

## Engineering Lessons

1. **Determinism crosses process boundaries.** Stable input/config/source identity
   and exact accounting allow complete artifact comparisons across all backends.
2. **Placement is constrained per node.** Summing CPU and custom resources across
   nodes can claim capacity even when no node can fit a complete actor request.
3. **Ownership controls cleanup.** A process name, bare PID, or recycled group
   number is insufficient. Lifetime identity and independent ownership evidence
   prevent cleanup from touching unrelated processes.
4. **A measurement needs a defined scope.** Shared mappings make summed RSS
   misleading. Unavailable coverage must remain unavailable; process startup,
   export, shutdown, and observer overhead belong in the measured end-to-end path.
5. **Execution and commit guarantees differ.** A worker can fail after computation
   but before a final commit. Retries alone cannot prevent duplicate persistence.

## Next Phase

Phase 4 introduces stable lifecycle states, bounded retry/timeout policies,
failure detection, durable experiment progress, recovery, and idempotent commits.
Design the journal and commit protocol around at-least-once execution with a
single committed result per job. Recovery must validate dataset/source identity
and distinguish acknowledged, completed, and committed work. Test lost
acknowledgments, repeated delivery, partial writes, and scheduler restart.

## Approval Required

Would you like to proceed to Phase 4? No Phase 4 implementation is authorized
until the user explicitly approves it.

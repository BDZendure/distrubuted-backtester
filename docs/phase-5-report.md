# Phase 5 completion report

Status: completed and verified locally on `feat/phase-5-data-storage`.
Phase 6 remains unapproved.

## Implemented

- Exact versioned Parquet conversion/loading alongside the unchanged CSV contract.
- Optional expected-version checks and an explicit content-verified bounded LRU.
- CSV/Parquet backtests and sweeps through the existing execution backends.
- Complete classic or authoritative journal snapshots imported transactionally
  into optional DuckDB, with idempotent membership and exact original results.
- CLI conversion, import, and query commands; a reproducible storage harness.
- Synchronized architecture, ADR, roadmap, beginner/technical guides, and CI.

Important files changed:

| Files | Responsibility |
| --- | --- |
| `dataset_identity.py`, `parquet_data.py`, `market_data.py` | Exact version descriptors, columnar adapter, format-aware loading and explicit cache |
| `experiment_snapshot.py`, `analytics.py`, `scheduler/journal.py` | Verified classic/journal reads and transactional analytical snapshots |
| `cli.py`, `experiments.py`, `scheduler/experiments.py` | Conversion, format/version options, import/query, and existing backend integration |
| `storage_benchmark.py`, `storage_measurement.py`, storage tests and CI | Reproducible observations and regression coverage |
| README, architecture/roadmap/ADR, beginner/technical guides, storage instructions | Interface, tradeoffs, evidence, and reproduction |

## Architecture

The simulation still consumes immutable validated domain records. Arrow is a
storage adapter; its int64 range is enforced before conversion, without reducing
CSV's supported integer range. Logical versions identify normalized bars,
policy/schema, and CSV lineage; physical file digests remain part of job identity.
Compression/layout changes can preserve logical identity while creating new jobs.

The cache reads and hashes current bytes before reuse. Entries are explicit,
immutable, and evicted by recency. It saves decoding work, not file reading.
Entry count is bounded; bytes are not. There is no shared global cache service.

SQLite remains the durable coordinator authority. A consistent read-only snapshot
can supply complete outcomes even when JSON exports are missing. Classic sources
need verified final exports. DuckDB commits an entire snapshot atomically and
stores canonical artifacts, exact metric strings, definitions, membership, and
separate observations. Snapshot identity distinguishes different terminal answer
sets for identical experiment definitions. Decimal report projections are bounded
and explicitly nullable on overflow.

See [ADR 0007](decisions/0007-versioned-storage-and-analytics.md) and
[storage instructions](storage.md). One history per asset/file avoids small-file
proliferation; configurable row groups provide a measured layout choice without
introducing a premature data-lake catalogue.

## Verification

The Phase 4 baseline passed 897 tests before changes. The accepted Phase 5 source
ran the complete suite, including real Parquet sequential/process/Ray and durable
Ray equivalence, plus the real storage child-process harness:

```bash
PHASE5_STORAGE_CHILD_TESTS=1 .venv/bin/python -m pytest -q
```

Result: **1,213 passed in 347.97 seconds**, exit 0. Package source was frozen for
this run. A separate final scope executed:

```bash
.venv/bin/python -m pytest -q tests/test_experiment_snapshot.py \
  tests/test_analytics.py tests/test_journal.py
```

Result: **222 passed in 13.12 seconds**, exit 0. These are correctness-check
durations, not throughput measurements. The plan records earlier scoped checks.

Independent review reproduced malformed orders, inconsistent fill/equity
accounting, SQL macro shadowing, and invalid diagnostic timelines. Regressions
preceded their corrections. A process-local negative control restoring earlier
validation failed 32 added cases; no files or tests were weakened for that check.
Final ledger review found zero mismatches in 15,391 exhaustive timeline cases.
The accepted validator proves internal ledger consistency and possible diagnostic
timing, not original market prices or strategy authenticity.

Final static commands were executed successfully:

```text
.venv/bin/python -m ruff check .                 All checks passed
.venv/bin/python -m ruff format --check .        114 files already formatted
.venv/bin/python -m mypy                         No issues in 80 source files
.venv/bin/python -m pip check                    No broken requirements
```

The read-only cache-directory warning from pip did not prevent its check.
Frozen installed-source digest:
`dfef1b1edee02fde072a4173750d25b8a39f9bc279772c69a184d98c9ed7c53c`.

The accepted-source wheel and source archive built with
`.venv/bin/python -m build --no-isolation`. Final archive verification executed
`.venv/bin/python /private/tmp/phase5-artifact-check.py`:
40 wheel package files and 136 source-archive files matched current bytes;
all wheel dependencies remained optional; 34 Markdown documents had 181 local
links with no missing targets or em dashes. `git diff --check` also passed.
The archive checker is a local verification helper, not a runtime dependency.

Two independent virtual environments were created under `/private/tmp` and invoked
outside the checkout with `-I`.
The core wheel was installed with `--no-deps`; probes confirmed Arrow, DuckDB,
Ray, and psutil absent. CSV sequential/process exports and a manually populated
completed-journal fixture had matching digests
`673571df60985c39b28f73ac5396df95041a56ff41849cce2db7ef04af0fdda8`.
The fixture called `execute_job` and `Journal.commit` directly. Status and
completed resume worked without starting Ray, and the core CLI backtest also
exported the reference sample. Actual durable Ray execution was verified by
the separate complete suite and CLI demonstration below.

The storage wheel installed the two pinned binary dependencies from downloaded
wheels. Conversion/backtest reproduced final equity `749.257000` and two fills.
Its four-job sweep, idempotent import, and query passed without Ray or psutil.
Both environments reported the frozen source digest above. The temporary helper
invocations were:

```bash
/private/tmp/phase5-core-wheel/bin/python -I /private/tmp/phase5-core-wheel-check.py \
  "$PWD/examples/sma.csv" /private/tmp/phase5-accepted-core-wheel-results
/private/tmp/phase5-storage-wheel/bin/python -I /private/tmp/phase5-storage-wheel-check.py \
  "$PWD/examples/sma.csv" /private/tmp/phase5-accepted-storage-wheel-results
```

The helpers are local verification artifacts. Public reproduction commands are
in this report and the README's wheel-installation section.

Historical import also succeeded: `results-import` read the completed Phase 4
durable journal into a fresh analytical database, reporting four successful jobs
and snapshot `cae3f87fab723325fbb46a0db756b8b008f805436f62b641d7dc95473048c91d`.
The first attempt using `/tmp` was rejected by the no-symlink path contract;
the canonical `/private/tmp` invocation succeeded. No source journal was changed.

The actual CLI demonstration ran conversion, CSV/Parquet reference backtests,
four-job sweeps through sequential/process-2/Ray-2, durable Ray-2, status,
journal import with its index deliberately absent, repeated classic import,
query, and completed resume. All four sweep digests agreed:
`90be4b1fbfd84927f7ee494bcb939be23d81e36d2815125b9784b3b0a5ec14fe`.
Four queried jobs, two sample fills, final equity `749.257000`, fees `1.743000`,
and export repair without new attempts were verified. The complete command list
and identifiers are retained in [demonstration.json](benchmarks/phase-5/demonstration.json).
The first helper attempt incorrectly parsed empty stdout from `backtest --output`;
the helper was corrected to inspect the exported file and rerun in a fresh
location. No application change was needed.

## Performance

Both measured workloads completed: 256 and 65,536 bars, four jobs, three
repetitions, CSV plus Parquet row groups of 8,192 and 65,536. **108 raw observations**
passed identity checks; 18 complete experiments matched all economic results per
configuration except dataset lineage. No simulation optimization or profiler was
introduced. The [storage comparison](benchmarks/phase-5/results.md) retains every
sample, range, size, identity, and environment.

For the 65,536-bar workload on Apple M3 Pro/macOS/CPython 3.14.6:

| Layout | File bytes | Warm read median | Cache-hit median | Complete 4-job median |
| --- | ---: | ---: | ---: | ---: |
| CSV | 4,054,324 | 1.253122 s | 0.005310 s | 8.633964 s |
| Parquet, 8,192-row groups | 1,930,921 | 1.077567 s | 0.002939 s | 8.544690 s |
| Parquet, 65,536-row groups | 2,056,839 | 1.040920 s | 0.002975 s | 8.653589 s |

Default Parquet used about half the file storage; the complete runs did not
establish a speedup. Fresh-process wall medians were 2.587710 s for CSV and
2.999106 s for default Parquet, including startup/imports, verification, and exit.
The 256-row Parquet files were identical across both settings, so their timing
variation cannot show a row-group advantage. The simple 65,536-row default remains;
three synthetic trials do not establish a universal optimum.

Own-process lifetime high-water parent RSS reached 57.22 MiB for the small workload
and 266.77 MiB for the substantial workload. This includes setup and earlier work;
it is not live dataset size, private RAM, or total cluster memory. Fresh reader and
local receiver RSS are reported separately. Larger protocol-5 payloads were
3,137,933 CSV bytes and 3,138,135 Parquet bytes, showing that file compression did
not replace the Python worker representation. Local spawned transfer is not a
network test. OS cache state remained uncontrolled.

## Known Limitations

- Single asset, USD, raw daily bars, long only, whole shares, next supplied open;
  no market impact, liquidity constraints, intraday execution, or corporate actions.
- No real historical-universe coverage or survivorship-bias guarantee.
- Full files are read into memory before decoding; workers retain Python objects
  and separate copies. Parquet does not introduce shared-memory simulation.
- Logical identity includes original CSV bytes; identifiers do not authenticate
  data vendors. Conversion needs int64-compatible values.
- The cache has an entry limit rather than a memory quota; hits still read/hash.
- DuckDB is a local analytical copy with one import writer, not replicated
  scheduling infrastructure. Imported sources must be complete and terminal.
  Query correctness is verified; query latency and large-table scaling are unmeasured.
- Resume requires the original installed source, including release identity;
  upgrading does not migrate an existing journal to a new engine implementation.
- Local atomic publication and process-kill checks do not establish arbitrary
  power-loss or network-filesystem durability. Durable journals require POSIX.
- Fresh-process reads have uncontrolled filesystem cache. RSS is own-process
  lifetime high water. Spawn transfer is local, not a network benchmark.
- Local Python 3.14.6/macOS results do not prove remote Linux CI or Python 3.12
  outcomes. CI configuration is documented separately from observed runs.

## Reproduction Instructions

From a fresh checkout with Python 3.12 or newer:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --editable ".[dev,benchmark,distributed,storage]"
PHASE5_STORAGE_CHILD_TESTS=1 python -m pytest -q
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m pip check
python -m pip install setuptools==84.0.0
python -m build --no-isolation
```

The observed tool snapshot is [storage-benchmark.lock](../requirements/storage-benchmark.lock).
It records versions, not artifact hashes or a universal platform resolution.
Follow [storage commands](storage.md) for conversion, sweeps, import/query, and version
checks. The [measured comparison](benchmarks/phase-5/results.md#reproduce) includes
the exact two benchmark parameter sets and destinations. Use fresh output directories on repeat runs.

## Engineering Lessons

1. **Separate data meaning from file bytes.** Layout changes need physical
   provenance without pretending prices changed. Both identities have a purpose.
2. **Exact types are part of correctness.** Integer storage prevents a float
   round trip; strict null/type/range checks reject ambiguous input at the boundary.
3. **Cache invalidation needs evidence.** Current content hashes establish reuse;
   paths and timestamps alone are insufficient. Cache hits still have costs.
4. **Analytical copies need a commit boundary.** Transactional snapshot import
   prevents partial answers, while authoritative journals remain responsible for
   execution recovery. Result-set identity differs from input identity.
5. **Name measurement scope precisely.** Process startup, OS cache, retained
   allocations, and local transfer influence observations. Report those boundaries
   before drawing a performance conclusion.

## Next Phase

Phase 6 profiles substantial workloads to identify dominant CPU/allocation costs,
then measures each proposed optimization against this stable semantic baseline.
No engine representation rewrite, vectorization, task batching, or C++ work is
implemented in Phase 5. Any Phase 6 change must preserve verified timing,
accounting, provenance, and backend equivalence.

## Approval Required

Phase 5 is complete. Proceed to Phase 6 performance engineering?
Stop at this checkpoint until explicit approval is received.

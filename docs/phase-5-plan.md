# Phase 5 data storage implementation plan

Authorization: The user approved Phase 5 after the verified Phase 4 checkpoint.
Implement this phase on `feat/phase-5-data-storage`; stop before Phase 6.
Use test-driven development and independent code review. This document is the
durable progress ledger. See [ADR 0007](decisions/0007-versioned-storage-and-analytics.md).

**Goal:** Exact versioned Parquet input, verified caching, queryable committed
results, and reproducible storage measurements.

**Architecture:** Preserve simulation and worker representations. Add format-aware
loading and conversion, extend metadata for Parquet provenance, and import complete
outcome snapshots into DuckDB without changing the journal's commit protocol.

**Stack:** Python >=3.12; optional storage extra `pyarrow==25.0.1`,
`duckdb==1.5.6`; existing pytest, Ruff, mypy, Ray, and benchmark tooling.

## Global constraints

- Phase 5 only. No Phase 6 profiling, engine rewrites, or optimization claims.
- No simulation timing, fee, integer-accounting, or long-only rule changes.
- Existing CSV metadata JSON remains byte-compatible in shape.
- Parquet columns are nonnullable date32 and int64 micro-USD/volume.
- Reject overflow, float prices, malformed metadata, nulls, unordered data,
  mismatched expected versions, and changed dataset snapshots.
- Core CSV/status/completed-resume paths need no Arrow, DuckDB, or Ray installation.
- The SQLite journal remains authoritative; its schema does not change.
- Full artifact parity for identical sources; economic parity across CSV/Parquet.
- No fabricated checks, benchmarks, cold-disk claims, or multi-machine measurements.
- Never use an em dash. Never push remotely without explicit authorization.

## Context and design review

- [x] Inspect current loaders, metadata, manifests, results, CLI, journal, and CI.
- [x] Compare strict files, inferred schemas, and partitioned lake approaches.
- [x] Define exact types, physical/logical identity, cache, and analytical snapshots.
- [x] Verify pinned packages and APIs against primary documentation.
- [x] Clean baseline: 897 passed in 104.77 seconds, source unchanged from Phase 4.
- [ ] Complete independent review and all acceptance checks.

## Task 1: Exact Parquet and dataset versions

Files: `models.py`, `jobs.py`, `results.py`, `data.py`; new
`dataset_identity.py`, `parquet_data.py`, `market_data.py`; tests
`test_parquet_data.py`, `test_market_data.py`, existing metadata/job/result tests;
`pyproject.toml` version 0.6.0 and optional storage extra.

Public contracts:

```python
load_dataset(path: Path, *, symbol: str, price_basis: Literal["raw"],
             data_format: Literal["auto", "csv", "parquet"] = "auto",
             expected_version: str | None = None,
             cache: DatasetCache | None = None) -> MarketDataset
load_parquet(path: Path, *, symbol: str, price_basis: Literal["raw"])
    -> MarketDataset
write_parquet(dataset: MarketDataset, path: Path, *, row_group_size: int = 65536,
              overwrite: bool = False) -> DatasetMetadata
bars_digest(bars: Sequence[Bar]) -> str
DatasetCache(max_entries: int = 1)  # bounded LRU, hits/misses, clear()
```

DatasetMetadata adds `source_format` (csv default), and optional `dataset_version`,
`parent_source_sha256`, `bars_sha256`. All three hashes are required for Parquet;
CSV forbids them and retains its legacy serialized fields. The normalized bar
digest hashes each canonical `[date, open, high, low, close, volume]` row with an LF
separator. The version descriptor contains schema_version=1, symbol, currency=USD,
price_basis=raw, validation_version=1, money_unit=micro-USD, parent_source_sha256,
bars_sha256, and row_count. Its canonical JSON hash is the dataset version.

- [ ] Write failing tests for exact six-decimal round trips and overflow rejection.
- [ ] Add metadata fields and legacy/extended canonical serialization; test JobSpec
  round trips and malformed/partial provenance rejection.
- [ ] Factor CSV snapshot parsing so cache hashing and parsing use identical bytes.
- [ ] Implement batch Parquet reading, strict schema/descriptor verification,
  atomic no-overwrite writing, count/order/null/price validation, and path safety.
- [ ] Test int64 boundaries, unexpected symbol/policy/version, corrupted files,
  tampered metadata/bar hashes, cross-row-group ordering, empty input, protected
  outputs, and identical economics of all fills/equity/metrics/final state.
- [ ] Implement content-verified bounded cache; test same-size/mtime rewrites,
  symbol/format separation, eviction, no-cache behavior, and invalid hit inputs.
- [ ] Run `pytest -q tests/test_parquet_data.py tests/test_market_data.py
  tests/test_data.py tests/test_models.py tests/test_jobs.py tests/test_results.py`.
- [ ] Run Ruff and mypy; obtain task review; commit focused changes.

## Task 2: Verified analytical snapshots

Files owned by the analytics implementer: new `experiment_snapshot.py`,
`analytics.py`, `tests/test_experiment_snapshot.py`, `tests/test_analytics.py`;
add only the read-only committed-snapshot API to `scheduler/journal.py` and
associated tests. No CLI, loader, metadata, dependency, or schema edits.

Public contracts:

```python
import_experiment(experiment: Path, database: Path) -> dict[str, object]
query_results(database: Path, *, snapshot_id: str | None = None,
              limit: int = 20) -> list[dict[str, object]]
```

`import_experiment` returns experiment_id, snapshot_id, submitted/succeeded/failed,
and whether the semantic snapshot was newly inserted. `query_results` returns
stable ordered report rows containing snapshot_id, experiment_id, job_id, symbol,
source_sha256, fast_window, slow_window, status, metrics, and failure. Preserve
canonical full artifact bytes and compact outcome records in DuckDB. Snapshot ID
hashes canonical `{experiment_id, outcomes:[{job_id,content_sha256},...]}` in sorted
job order. Store manifest definitions, snapshot membership, outcomes, and source
observations separately. Repeated imports must not duplicate outcome rows;
different terminal sets of the same experiment must coexist. Source observations
include source kind/path and optional execution metadata or journal attempt status,
deduplicated by their canonical content hash.

- [ ] Write tests for classic and journal snapshots and an initial failing import.
- [ ] Implement a public read-only consistent journal snapshot reader which uses
  existing schema/hash/invariant validation, without writer locking or resume.
- [ ] Require all expected jobs terminal. Validate classic canonical manifest,
  exact final index, paths, hashes, outcome/job/config/dataset/status/metric links.
  Stream full artifacts with one retained at a time; compact headers may be O(jobs).
- [ ] Prefer the journal when present, including missing/truncated exports; reject
  a corrupt journal instead of falling back to JSON.
- [ ] Implement lazy DuckDB loading, owned database schema/version checks,
  transactional import, snapshot deduplication, divergence rejection, and rollback
  when the last artifact fails validation. Preserve exact monetary text and source
  hashes. Add a decimal SQL report view with explicit nullable bounded casts.
- [ ] Test failed outcomes, duplicate membership, multiple snapshots, different
  experiments/datasets, incomplete runs, tampering, symlinks/traversal, large exact
  amounts, malformed database schema, and readonly parameterized query limits.
- [ ] Run `pytest -q tests/test_experiment_snapshot.py tests/test_analytics.py
  tests/test_journal.py`; run Ruff and mypy; obtain task review and report.

## Task 3: CLI and execution integration

Files: `cli.py`, `experiments.py`, `scheduler/experiments.py`, CLI tests,
new `tests/test_storage_cli.py`, `tests/test_storage_ray.py`, CI optional extras.

Add `--data-format {auto,csv,parquet}` and optional `--dataset-version` to backtest,
sweep, and resume. Existing APIs gain corresponding optional arguments. CSV keeps
its existing timing key; Parquet uses `parquet_load_seconds`. CLI conversion is
`convert-data --data CSV --symbol SYMBOL --price-basis raw --output FILE.parquet
--row-group-size N` with explicit `--overwrite`. It prints physical and logical
identifiers. Analytics commands are `results-import --experiment-dir DIR --database
FILE.duckdb` and `results-query --database FILE.duckdb [--snapshot-id ID] [--limit N]`.

- [ ] Write failing CLI tests and implement loader/expected-version integration.
- [ ] Check real sequential/process/Ray parity on the same Parquet source, plus
  cross-format economic equivalence and durable Parquet resume/export repair.
- [ ] Map optional dependency and storage errors to clear exit-2 diagnostics;
  refuse conversions that alias the input and protect existing outputs.
- [ ] Test source mutation before resume, unknown versions, protected source paths,
  imports after partial publication, report query output, and no-extra wheel paths.
- [ ] CI installs storage extras and runs storage/Ray integration tests alongside
  existing tests, without making runtime dependencies mandatory.
- [ ] Run all applicable integration checks, review, and commit.

## Task 4: Measurements and completion

Files: new `storage_benchmark.py`, `storage_measurement.py`, associated tests;
`docs/benchmarks/phase-5/`, `requirements/storage-benchmark.lock`; README,
architecture, roadmap, dependencies, testing, beginner and technical guides,
`docs/storage.md`, `docs/phase-5-report.md`, decisions index.

CLI: `python -m distributed_backtester.storage_benchmark run --bars N --jobs N
--repetitions N --row-group-sizes 8192 65536 --output-dir DIR`. Validate arguments
and require a fresh directory. Generate seeded synthetic CSV, convert each layout,
and retain manifests and raw samples. Each sample reports workload and source IDs,
verified economic/bar digests, elapsed time, file or payload size where applicable,
own-process peak RSS with explicit units/source, and environment.

- [ ] Test benchmark parsing, raw sample schemas, digests, timing boundaries,
  failure propagation, measurement availability, and protected destinations.
- [ ] Measure fresh-process reads (including import/setup and child exit), repeated
  reads, content-verified cache hits, repeated complete experiments, protocol-5
  pickle, and same-host spawned-process dataset transfer. Keep verification outside
  read timing and expose separate end-to-end elapsed measurements.
- [ ] Run small and larger workloads, multiple repetitions and row-group choices.
  Record OS-cache state as uncontrolled; no cold-disk or network scaling claim.
- [ ] Compare row-group sizes and document the chosen default and partition limits.
- [ ] Freeze package source before final Ray tests and timed measurements.
- [ ] Run complete pytest, lint, format, strict typing, pip check, wheel/sdist builds,
  isolated core/storage wheel demonstrations, archive byte and documentation checks.
- [ ] Independently review the whole phase; fix findings and rerun affected checks.
- [ ] Record actual results, limitations, reproduction commands, and 3-5 interview
  lessons in the completion report. Commit local milestones; stop before Phase 6.
- [ ] Obtain explicit approval before Phase 6.

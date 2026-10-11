# Versioned data and results analysis

Phase 5 adds optional PyArrow and DuckDB while keeping CSV simulation dependency
free. Use Terminal or VS Code's terminal from the repository directory.

## Install and convert the sample

```bash
python -m pip install --editable ".[dev,benchmark,storage]"
distributed-backtester convert-data --data examples/sma.csv --symbol SYNTH \
  --price-basis raw --output artifacts/sma.parquet
distributed-backtester backtest --data artifacts/sma.parquet --symbol SYNTH \
  --price-basis raw --fast 2 --slow 3 --initial-cash 1000 --fee-bps 10 \
  --output artifacts/parquet-example.json
```

The converter prints the dataset metadata. Copy its `dataset_version` into
`--dataset-version HASH` to require that version on subsequent loads. Existing
output is protected; `--overwrite` deliberately replaces it. Input/output aliases,
including hard links, are rejected. Automatic selection recognizes `.parquet` and
`.pq`; `--data-format csv` or `--data-format parquet` overrides the suffix.

The sample remains synthetic: final equity is $749.257, with two fills and
$1.743 total fees. Converting the input changes provenance, not these economics.

## Run and query an experiment

```bash
distributed-backtester sweep --data artifacts/sma.parquet --symbol SYNTH \
  --price-basis raw --fast 1 2 --slow 3 4 --fee-bps 10 \
  --backend sequential --output-dir artifacts/parquet-sweep
distributed-backtester results-import --experiment-dir artifacts/parquet-sweep \
  --database artifacts/results.duckdb
distributed-backtester results-query --database artifacts/results.duckdb --limit 20
```

Each sweep needs a new directory. Process and Ray backends consume the same
validated immutable dataset. The optional `distributed` extra is needed for Ray.
Durable Ray start/resume supports both formats, with the same exact input and
installed-source identity requirements as Phase 4.
An experiment created with an earlier release cannot resume under changed source;
use its original installation for recovery. Historical complete results can still
be imported for analysis because their recorded implementation is validated as
part of their own manifest rather than compared to the currently installed engine.

Import verifies a complete terminal experiment before accepting it. Classic
experiments need a valid manifest, index, and matching detailed artifacts.
For durable experiments, a consistent read-only SQLite journal snapshot is
authoritative. Missing or damaged JSON exports do not override that journal;
a damaged journal is an error. Incomplete experiments are rejected.

An import transaction commits all rows together. Importing identical outcomes
again reuses the same snapshot. Different terminal outcomes for the same
experiment definition receive different snapshot identifiers. DuckDB is an
analytical copy; workers and the scheduler continue to use the original stores.
Keep databases separate from input datasets and experiment directories.
Analytical source/database paths reject symlinks and `..` traversal components.
Use their canonical paths; on macOS, `/private/tmp` is the canonical spelling of
the `/tmp` symlink.

## Exact representation and identity

| Value | Representation or guarantee |
| --- | --- |
| Session date | Non-null Arrow `date32`, one row per observed session |
| OHLC price | Non-null signed 64-bit integer micro-USD |
| Volume | Non-null signed 64-bit integer, nonnegative |
| Physical source identity | SHA-256 of the exact file bytes that were decoded |
| Logical Parquet version | Hash of schema/policy, normalized bars, row count, symbol, and original CSV digest |
| Result snapshot identity | Hash of experiment ID and sorted job/artifact digest membership |

Prices are exact integers, avoiding a float round trip. CSV retains its original
arbitrary Python integer range; conversion rejects values exceeding signed int64
before creating output directories. Parquet files with wrong types, nulls,
unsupported policies, malformed metadata, inconsistent hashes, or invalid ordering
are rejected. Publication writes a sibling temporary file then atomically creates
or replaces the destination. This is not a power-loss durability guarantee.

Changing compression or row-group layout can preserve the logical version while
changing the physical digest and job IDs. Changing the original CSV's whitespace
changes its lineage digest and thus can change the version even if prices agree.
Neither identifier authenticates an untrusted data vendor. The declared raw price
policy still requires the user to assess corporate actions and universe bias.

Files hold one asset's complete history. Row groups are configurable; the default
is 65,536 rows. There is no multi-asset catalogue or date-partitioned data lake.
This avoids thousands of small files for the initial single-asset engine.

## Verified cache

`load_dataset(..., cache=DatasetCache(max_entries=1))` offers explicit in-process
reuse. Every request reads and hashes the current file. A hit skips parsing and
validation only after matching format, symbol, and content. Cached objects are
immutable and eviction follows least recent use. There is no global cache.

The entry limit is not a byte budget. A large history can still consume substantial
memory. File reads and hashing remain on hits, so repeated-load measurements must
include that work. Replacing a file at the same size and timestamp cannot produce
a stale hit.

## Storage measurements

```bash
python -m distributed_backtester.storage_benchmark run \
  --bars 256 --jobs 4 --repetitions 3 --row-group-sizes 8192 65536 \
  --discard-trial-results --output-dir artifacts/storage-small
```

Use a fresh destination for each run. The harness compares formats, fresh-process
reads, repeated reads, cache hits, repeated full experiments, serialization, and
same-host spawned-process transfer. It verifies bars and economic outputs before
accepting observations. Read the measured results in
[Phase 5 storage comparisons](benchmarks/phase-5/results.md).

A fresh process does not imply a cold filesystem cache. The OS cache is not
evicted. Process high-water RSS includes imports and retained allocations; it
does not isolate live dataset bytes or total cluster memory. Local transfer is
not a network or multiple-machine benchmark.

See [ADR 0007](decisions/0007-versioned-storage-and-analytics.md) and the
[technical explanation](technical-explanation.md#phase-5-exact-storage-and-analytical-snapshots).

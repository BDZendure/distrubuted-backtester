# ADR 0007: Versioned Parquet and committed-result analytics

Status: Accepted for the explicitly approved Phase 5.

## Context

The engine accepts immutable daily bars with exact integer micro-USD prices.
CSV input hashes, canonical job IDs, and the SQLite journal already protect
reproducibility. Phase 5 must improve storage and querying without changing
simulation timing, accounting, or the journal's authority.

## Alternatives

1. **Project-defined Parquet files and a DuckDB projection, selected.** A strict
   schema makes price units and dates unambiguous. Embedded conversion lineage
   and verified hashes connect the CSV and Parquet representations. Completed
   outcomes can be imported transactionally for queries.
2. **Infer arbitrary third-party Parquet schemas.** More flexible, but implicit
   timestamp, adjustment, and floating-price conventions weaken the boundary.
3. **Partitioned data lake and catalog.** Useful for a large multi-asset universe,
   but the current single-asset reader consumes an entire history. One versioned
   file with configurable row groups avoids directory scans and tiny daily files.

## Data contracts

Use nonnullable Arrow `date32` for sessions and `int64` for OHLC micro-USD and
volume. CSV retains its arbitrary-size Python integer contract. Parquet conversion
explicitly rejects values outside int64 instead of narrowing or rounding them.
Loaders validate every bar, cross-batch ordering, schema, nulls, counts, and hashes.

Parquet metadata records a schema version, symbol, USD/raw policy, money unit,
original CSV byte hash, normalized bar hash, row count, and dataset version.
The dataset version hashes that canonical descriptor. It identifies logical
content and conversion lineage; the physical Parquet byte hash separately
identifies the exact simulation input. Recompression may preserve the version
while changing the physical hash and job IDs. CSV's version identifier remains
its existing byte hash. Optional expected-version checks reject an unintended
dataset at a reused path.

Parquet provenance extends DatasetMetadata only for Parquet. Existing CSV job and
result metadata retains its exact field set. Format-independent correctness tests
compare economic fields and bars; same-source backend tests compare full artifacts.

## Cache

A bounded, explicit in-process LRU caches immutable validated datasets. Every
lookup rereads and hashes the input snapshot; format, symbol, and exact byte hash
form the key. A hit avoids decoding and validation, not file reads. Rewriting a
file without changing its size or timestamp cannot serve stale data. There is no
global cache, persistent cache, or unverified mtime shortcut.

## Analytics

DuckDB is a rebuildable analytical copy. Import a complete classic experiment
only after validating its manifest, index, all job IDs, paths, hashes, and outcome
contracts. If a journal exists, read a consistent verified SQLite snapshot and
use committed artifacts even when exported files are missing or damaged. Reject
partial experiments. Never fall back to JSON after a journal validation failure.

An experiment ID describes inputs. Different operational executions of those
inputs can terminate differently, so a content-derived snapshot ID identifies a
complete set of outcome hashes. Identical snapshots deduplicate; distinct outcome
sets coexist. Each snapshot has one row per stable job ID. Preserve source
observations, attempt history, and execution metadata separately from semantic
identity. One DuckDB transaction imports each snapshot; a late validation failure
rolls back the entire import. Money and reported ratios remain exact text. SQL
views expose explicitly bounded decimal casts without replacing the source text.

The CLI provides parameterized report queries. Users can also open DuckDB directly
for SQL analysis. Analytics does not submit jobs, retry simulations, or overwrite
the source journal. DuckDB process locking and a single importing writer are
documented limits, not a distributed database guarantee.

## Measurements and limits

Measure file sizes, row groups, fresh-process loading, repeated loads, verified
cache hits, repeated experiments, pickle serialization, and local process transfer.
Report own-process peak RSS and environment. Fresh-process reads do not imply a
cold OS page cache; no cache eviction or multi-machine network benchmark is claimed.
Preserve raw samples and compare economics before reporting measurements. Phase 6
profiling and engine representation changes remain outside this phase.

## Primary references

- [Arrow Parquet reading and writing](https://arrow.apache.org/docs/python/parquet.html)
- [PyArrow 25.0.1 release](https://pypi.org/project/pyarrow/25.0.1/)
- [DuckDB 1.5.6 release](https://pypi.org/project/duckdb/1.5.6/)
- [DuckDB transactions](https://duckdb.org/docs/stable/sql/statements/transactions)
- [DuckDB numeric types](https://duckdb.org/docs/stable/sql/data_types/numeric)

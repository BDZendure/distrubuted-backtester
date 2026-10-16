"""Transactional DuckDB analytical copies of verified experiment snapshots.

SQLite remains authoritative. DuckDB is imported only by analytical operations,
and money/ratio text and original artifact bytes are retained without conversion.
"""

import importlib
import json
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

from distributed_backtester.experiment_snapshot import (
    ExperimentSnapshot,
    read_experiment_snapshot,
)
from distributed_backtester.jobs import canonical_json

_OWNER = "distributed-backtester-analytics"
_VERSION = 1
_TABLES = (
    """CREATE TABLE analytics_metadata (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        owner VARCHAR NOT NULL, schema_version INTEGER NOT NULL
    )""",
    """CREATE TABLE experiments (
        experiment_id VARCHAR PRIMARY KEY, manifest VARCHAR NOT NULL
    )""",
    """CREATE TABLE job_definitions (
        job_id VARCHAR PRIMARY KEY, spec VARCHAR NOT NULL,
        symbol VARCHAR NOT NULL, source_sha256 VARCHAR NOT NULL,
        fast_window VARCHAR NOT NULL, slow_window VARCHAR NOT NULL
    )""",
    """CREATE TABLE snapshots (
        snapshot_id VARCHAR PRIMARY KEY,
        experiment_id VARCHAR NOT NULL REFERENCES experiments(experiment_id),
        submitted BIGINT NOT NULL CHECK (submitted > 0),
        succeeded BIGINT NOT NULL CHECK (succeeded >= 0),
        failed BIGINT NOT NULL CHECK (failed >= 0),
        CHECK (submitted = succeeded + failed)
    )""",
    """CREATE TABLE outcomes (
        job_id VARCHAR NOT NULL REFERENCES job_definitions(job_id),
        content_sha256 VARCHAR NOT NULL, artifact BLOB NOT NULL,
        record VARCHAR NOT NULL, status VARCHAR NOT NULL
            CHECK (status IN ('succeeded', 'failed')),
        metrics VARCHAR, failure VARCHAR,
        PRIMARY KEY (job_id, content_sha256)
    )""",
    """CREATE TABLE snapshot_membership (
        snapshot_id VARCHAR NOT NULL REFERENCES snapshots(snapshot_id),
        job_id VARCHAR NOT NULL, content_sha256 VARCHAR NOT NULL,
        PRIMARY KEY (snapshot_id, job_id),
        FOREIGN KEY (job_id, content_sha256)
            REFERENCES outcomes(job_id, content_sha256)
    )""",
    """CREATE TABLE source_observations (
        observation_sha256 VARCHAR PRIMARY KEY,
        snapshot_id VARCHAR NOT NULL REFERENCES snapshots(snapshot_id),
        content VARCHAR NOT NULL
    )""",
)

_VIEW = """CREATE VIEW results_report AS
    SELECT m.snapshot_id, s.experiment_id, m.job_id,
           j.symbol, j.source_sha256, j.fast_window, j.slow_window,
           o.status, o.metrics, o.failure,
           TRY_CAST(system.main.json_extract_string(o.metrics, '$.final_equity')
                    AS DECIMAL(38,6)) AS final_equity,
           TRY_CAST(system.main.json_extract_string(o.metrics, '$.total_fees')
                    AS DECIMAL(38,6)) AS total_fees,
           TRY_CAST(system.main.json_extract_string(o.metrics, '$.total_return')
                    AS DECIMAL(38,12)) AS total_return,
           TRY_CAST(system.main.json_extract_string(o.metrics, '$.maximum_drawdown')
                    AS DECIMAL(38,12)) AS maximum_drawdown,
           TRY_CAST(system.main.json_extract_string(o.metrics, '$.fill_count')
                    AS BIGINT) AS fill_count
      FROM snapshot_membership m
      JOIN snapshots s ON s.snapshot_id = m.snapshot_id
      JOIN job_definitions j ON j.job_id = m.job_id
      JOIN outcomes o ON o.job_id = m.job_id
                     AND o.content_sha256 = m.content_sha256
"""


def _duckdb() -> Any:
    try:
        return importlib.import_module("duckdb")
    except ImportError as error:
        raise RuntimeError(
            "analytics requires DuckDB; install distributed-backtester[storage]"
        ) from error


def _path(path: Path) -> Path:
    if not isinstance(path, Path) or ".." in path.parts:
        raise ValueError("database must be a Path without traversal")
    absolute = path.absolute()
    if any(item.is_symlink() for item in (absolute, *absolute.parents)):
        raise ValueError("database paths must not be symlinks")
    return absolute


def _create_schema(db: Any) -> None:
    for statement in _TABLES:
        db.execute(statement)
    db.execute(_VIEW)
    db.execute("INSERT INTO analytics_metadata VALUES (1, ?, ?)", [_OWNER, _VERSION])


def _schema_signature(db: Any) -> tuple[object, ...]:
    # Catalog SQL preserves types, nullability, checks, keys and view expressions.
    # Compare against a schema built by this DuckDB version, avoiding assumptions
    # about its normalization of SQL syntax across engine releases.
    # Qualify system readers so local table macros cannot conceal divergence.
    tables = db.execute(
        "SELECT schema_name, table_name, sql FROM system.main.duckdb_tables() "
        "WHERE database_name = system.main.current_database() AND NOT internal "
        "ORDER BY schema_name, table_name"
    ).fetchall()
    views = db.execute(
        "SELECT schema_name, view_name, sql FROM system.main.duckdb_views() "
        "WHERE database_name = system.main.current_database() AND NOT internal "
        "ORDER BY schema_name, view_name"
    ).fetchall()
    indexes = db.execute(
        "SELECT schema_name, index_name, sql FROM system.main.duckdb_indexes() "
        "WHERE database_name = system.main.current_database() "
        "ORDER BY schema_name, index_name"
    ).fetchall()
    functions = db.execute(
        "SELECT schema_name, function_name, function_type, parameters, "
        "parameter_types, varargs, return_type, macro_definition "
        "FROM system.main.duckdb_functions() "
        "WHERE database_name = system.main.current_database() AND NOT internal "
        "ORDER BY schema_name, function_name, function_type, "
        "parameters, parameter_types"
    ).fetchall()
    sql_objects = tuple(
        tuple(
            (schema, name, re.sub(r"DECIMAL\((\d+),\s*(\d+)\)", r"DECIMAL(\1,\2)", sql))
            for schema, name, sql in group
        )
        for group in (tables, views, indexes)
    )
    return (*sql_objects, tuple(functions))


def _validate_schema(db: Any) -> None:
    with _duckdb().connect(":memory:") as expected:
        _create_schema(expected)
        signature = _schema_signature(expected)
    if _schema_signature(db) != signature:
        raise RuntimeError("analytics database schema is corrupt, unknown or unowned")
    if db.execute("SELECT * FROM analytics_metadata").fetchall() != [
        (1, _OWNER, _VERSION)
    ]:
        raise RuntimeError("unsupported analytics database schema version or ownership")


@contextmanager
def _import_database(database: Path) -> Iterator[tuple[Any, bool]]:
    duckdb = _duckdb()
    if database.exists():
        db = duckdb.connect(str(database))
        try:
            yield db, False
        finally:
            db.close()
    else:
        # A first import is published only after validation and commit. Failure
        # leaves no empty or partly initialized database at the requested path.
        with TemporaryDirectory(prefix=".analytics-", dir=database.parent) as directory:
            temporary = Path(directory) / "import.duckdb"
            db = duckdb.connect(str(temporary))
            try:
                yield db, True
            finally:
                db.close()
            os.link(temporary, database)


def _ensure_row(
    db: Any, table: str, keys: dict[str, object], row: dict[str, object]
) -> bool:
    # Identifiers come exclusively from internal schema constants. Data is bound.
    where = " AND ".join(f"{key} = ?" for key in keys)
    columns = ", ".join(row)
    found = db.execute(
        f"SELECT {columns} FROM {table} WHERE {where}", list(keys.values())
    ).fetchall()
    values = tuple(row.values())
    if found:
        if found != [values]:
            raise RuntimeError(f"analytics {table} content divergence")
        return False
    placeholders = ", ".join("?" for _ in row)
    db.execute(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", list(values))
    return True


def _import_snapshot(db: Any, snapshot: ExperimentSnapshot) -> bool:
    experiment_id, snapshot_id = snapshot.experiment_id, snapshot.snapshot_id
    _ensure_row(
        db,
        "experiments",
        {"experiment_id": experiment_id},
        {"experiment_id": experiment_id, "manifest": canonical_json(snapshot.manifest)},
    )
    for job in snapshot.jobs:
        _ensure_row(
            db,
            "job_definitions",
            {"job_id": job.job_id},
            {
                "job_id": job.job_id,
                "spec": canonical_json(job.to_dict()),
                "symbol": job.dataset.symbol,
                "source_sha256": job.dataset.source_sha256,
                "fast_window": str(job.config.strategy.fast_window),
                "slow_window": str(job.config.strategy.slow_window),
            },
        )
    inserted = _ensure_row(
        db,
        "snapshots",
        {"snapshot_id": snapshot_id},
        {
            "snapshot_id": snapshot_id,
            "experiment_id": experiment_id,
            "submitted": snapshot.submitted,
            "succeeded": snapshot.succeeded,
            "failed": snapshot.failed,
        },
    )
    membership = [
        (record["job_id"], record["content_sha256"]) for record in snapshot.records
    ]
    if (
        not inserted
        and db.execute(
            "SELECT job_id, content_sha256 FROM snapshot_membership "
            "WHERE snapshot_id = ? ORDER BY job_id",
            [snapshot_id],
        ).fetchall()
        != membership
    ):
        raise RuntimeError("analytics snapshot membership divergence")
    stream = snapshot.iter_artifacts()
    try:
        while True:
            item = next(stream, None)
            if item is None:
                break
            job_id, content, record = item
            digest = record["content_sha256"]
            _ensure_row(
                db,
                "outcomes",
                {"job_id": job_id, "content_sha256": digest},
                {
                    "job_id": job_id,
                    "content_sha256": digest,
                    "artifact": content,
                    "record": canonical_json(record),
                    "status": record["status"],
                    "metrics": canonical_json(record["metrics"])
                    if record["metrics"] is not None
                    else None,
                    "failure": canonical_json(record["failure"])
                    if record["failure"] is not None
                    else None,
                },
            )
            _ensure_row(
                db,
                "snapshot_membership",
                {"snapshot_id": snapshot_id, "job_id": job_id},
                {
                    "snapshot_id": snapshot_id,
                    "job_id": job_id,
                    "content_sha256": digest,
                },
            )
            del item, content, record
    finally:
        stream.close()
    observation = canonical_json({"snapshot_id": snapshot_id, **snapshot.observation})
    digest = sha256(observation.encode()).hexdigest()
    _ensure_row(
        db,
        "source_observations",
        {"observation_sha256": digest},
        {
            "observation_sha256": digest,
            "snapshot_id": snapshot_id,
            "content": observation,
        },
    )
    return inserted


def _import_experiment(experiment: Path, database: Path) -> dict[str, object]:
    """Import a complete verified source atomically and deduplicate semantics."""
    path = _path(database)
    with (
        read_experiment_snapshot(experiment) as snapshot,
        _import_database(path) as (db, new),
    ):
        db.execute("BEGIN TRANSACTION")
        try:
            if new:
                _create_schema(db)
            else:
                _validate_schema(db)
            inserted = _import_snapshot(db, snapshot)
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
        result: dict[str, object] = {
            "experiment_id": snapshot.experiment_id,
            "snapshot_id": snapshot.snapshot_id,
            "submitted": snapshot.submitted,
            "succeeded": snapshot.succeeded,
            "failed": snapshot.failed,
            "inserted": inserted,
        }
    return result


def _query_results(
    database: Path, *, snapshot_id: str | None = None, limit: int = 20
) -> list[dict[str, object]]:
    """Return deterministic rows from an owned database using a read-only handle.

    Metrics are the original exact decimal text. The SQL results_report view
    additionally exposes nullable DECIMAL(38,6)/(38,12) casts for aggregation.
    """
    if type(limit) is not int or limit < 1:
        raise ValueError("limit must be a positive integer")
    if snapshot_id is not None and not isinstance(snapshot_id, str):
        raise ValueError("snapshot_id must be text or None")
    path = _path(database)
    if not path.is_file():
        raise FileNotFoundError(f"analytics database does not exist: {path}")
    with _duckdb().connect(str(path), read_only=True) as db:
        db.execute("BEGIN TRANSACTION")
        try:
            _validate_schema(db)
            rows = db.execute(
                "SELECT snapshot_id, experiment_id, job_id, symbol, source_sha256, "
                "fast_window, slow_window, status, metrics, failure "
                "FROM results_report WHERE (? IS NULL OR snapshot_id = ?) "
                "ORDER BY snapshot_id, job_id LIMIT ?",
                [snapshot_id, snapshot_id, limit],
            ).fetchall()
        finally:
            db.execute("ROLLBACK")
    names = (
        "snapshot_id",
        "experiment_id",
        "job_id",
        "symbol",
        "source_sha256",
        "fast_window",
        "slow_window",
        "status",
        "metrics",
        "failure",
    )
    result = []
    for row in rows:
        record = dict(zip(names, row, strict=True))
        record["fast_window"] = int(record["fast_window"])
        record["slow_window"] = int(record["slow_window"])
        for key in ("metrics", "failure"):
            record[key] = json.loads(record[key]) if record[key] is not None else None
        result.append(cast(dict[str, object], record))
    return result


def import_experiment(experiment: Path, database: Path) -> dict[str, object]:
    """Import a verified snapshot; expose storage diagnostics as RuntimeError."""
    duckdb = _duckdb()
    try:
        return _import_experiment(experiment, database)
    except duckdb.Error as error:
        raise RuntimeError(f"DuckDB import failed: {error}") from error


def query_results(
    database: Path, *, snapshot_id: str | None = None, limit: int = 20
) -> list[dict[str, object]]:
    """Read JSON-ready reports; expose storage diagnostics as RuntimeError."""
    duckdb = _duckdb()
    try:
        return _query_results(database, snapshot_id=snapshot_id, limit=limit)
    except duckdb.Error as error:
        raise RuntimeError(f"DuckDB query failed: {error}") from error

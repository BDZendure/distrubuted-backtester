import importlib
import importlib.util
import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from distributed_backtester.jobs import canonical_json
from tests.test_experiment_snapshot import (
    classic,
    durable,
    load_json,
    other_data,
    rewrite_artifact,
)

pytest.importorskip("duckdb")


def analytics_module() -> Any:
    name = "distributed_backtester.analytics"
    assert importlib.util.find_spec(name) is not None, "analytics module is missing"
    return importlib.import_module(name)


def db_connect(database: Path, *, readonly: bool = False) -> Any:
    import duckdb

    return duckdb.connect(str(database), read_only=readonly)


@pytest.mark.parametrize("source", ["classic", "journal"])
def test_import_public_contract_exact_bytes_and_idempotence(
    tmp_path: Path, source: str
) -> None:
    output, database = tmp_path / "run", tmp_path / "analytics.duckdb"
    jobs = (classic if source == "classic" else durable)(output, failed=True)
    api = analytics_module()
    result = api.import_experiment(output, database)
    assert result == {
        "experiment_id": result["experiment_id"],
        "snapshot_id": result["snapshot_id"],
        "submitted": 2,
        "succeeded": 1,
        "failed": 1,
        "inserted": True,
    }
    assert api.import_experiment(output, database) == {**result, "inserted": False}
    rows = api.query_results(database)
    assert [row["job_id"] for row in rows] == [job.job_id for job in jobs]
    assert all(
        set(row)
        == {
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
        }
        for row in rows
    )
    assert rows[0]["metrics"]["final_equity"].endswith("000000")
    assert rows[1]["failure"]["error_type"] == "WorkerFailure"
    with db_connect(database, readonly=True) as db:
        assert db.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 2
        assert db.execute("SELECT count(*) FROM snapshot_membership").fetchone()[0] == 2
        assert db.execute("SELECT count(*) FROM source_observations").fetchone()[0] == 1
        artifacts = db.execute(
            "SELECT job_id, artifact, record FROM outcomes ORDER BY job_id"
        ).fetchall()
    for job_id, content, record in artifacts:
        assert isinstance(content, bytes)
        assert json.loads(content)["job"]["job_id"] == job_id
        assert canonical_json(json.loads(record)) == record
        if source == "classic":
            assert content == (output / "results" / f"{job_id}.json").read_bytes()


def test_same_definition_different_outcomes_and_sources_coexist(tmp_path: Path) -> None:
    api, database = analytics_module(), tmp_path / "analytics.duckdb"
    classic(tmp_path / "first")
    classic(tmp_path / "second", failed=True)
    durable(tmp_path / "third")
    first = api.import_experiment(tmp_path / "first", database)
    second = api.import_experiment(tmp_path / "second", database)
    third = api.import_experiment(tmp_path / "third", database)
    assert first["experiment_id"] == second["experiment_id"] == third["experiment_id"]
    assert first["snapshot_id"] != second["snapshot_id"]
    assert first["snapshot_id"] == third["snapshot_id"] and not third["inserted"]
    assert len(api.query_results(database)) == 4
    assert len(api.query_results(database, snapshot_id=second["snapshot_id"])) == 2
    with db_connect(database, readonly=True) as db:
        assert db.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 3
        assert db.execute("SELECT count(*) FROM experiments").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM source_observations").fetchone()[0] == 3


def test_other_experiments_and_datasets_and_duplicate_membership(
    tmp_path: Path,
) -> None:
    api, database = analytics_module(), tmp_path / "analytics.duckdb"
    classic(tmp_path / "first", duplicates=True)
    classic(tmp_path / "second", data=other_data())
    one = api.import_experiment(tmp_path / "first", database)
    two = api.import_experiment(tmp_path / "second", database)
    assert one["experiment_id"] != two["experiment_id"]
    rows = api.query_results(database)
    assert len(rows) == 4
    assert {row["source_sha256"] for row in rows} == {"a" * 64, "d" * 64}
    assert [(row["snapshot_id"], row["job_id"]) for row in rows] == sorted(
        (row["snapshot_id"], row["job_id"]) for row in rows
    )


def test_last_artifact_failure_rolls_back_every_import_table(tmp_path: Path) -> None:
    api, database = analytics_module(), tmp_path / "analytics.duckdb"
    classic(tmp_path / "good")
    api.import_experiment(tmp_path / "good", database)
    jobs = classic(tmp_path / "bad", data=other_data())
    rewrite_artifact(tmp_path / "bad", jobs[-1], "metrics")
    with db_connect(database, readonly=True) as db:
        tables = [row[0] for row in db.execute("SHOW TABLES").fetchall()]
        before = {
            table: db.execute(f'SELECT * FROM "{table}" ORDER BY ALL').fetchall()
            for table in tables
        }
    with pytest.raises((ValueError, RuntimeError)):
        api.import_experiment(tmp_path / "bad", database)
    with db_connect(database, readonly=True) as db:
        after = {
            table: db.execute(f'SELECT * FROM "{table}" ORDER BY ALL').fetchall()
            for table in tables
        }
    assert before == after


def test_unbounded_money_is_preserved_with_nullable_decimal_report(
    tmp_path: Path,
) -> None:
    api, database = analytics_module(), tmp_path / "analytics.duckdb"
    classic(tmp_path / "large", cash=10**60 + 123456)
    api.import_experiment(tmp_path / "large", database)
    rows = api.query_results(database)
    expected = load_json(tmp_path / "large" / "index.json")["outcomes"][0]["metrics"][
        "final_equity"
    ]
    assert rows[0]["metrics"]["final_equity"] == expected
    with db_connect(database, readonly=True) as db:
        values = db.execute(
            "SELECT final_equity, total_return FROM results_report"
        ).fetchall()
    assert all(
        equity is None and isinstance(total_return, Decimal)
        for equity, total_return in values
    )


@pytest.mark.parametrize("limit", [0, -1, True, 1.5, "2"])
def test_invalid_query_limits_rejected_without_database_creation(
    tmp_path: Path, limit: object
) -> None:
    database = tmp_path / "missing.duckdb"
    with pytest.raises(ValueError, match="limit"):
        analytics_module().query_results(database, limit=limit)
    assert not database.exists()


def test_queries_are_readonly_parameterized_and_limited(tmp_path: Path) -> None:
    api, database = analytics_module(), tmp_path / "analytics.duckdb"
    classic(tmp_path / "run")
    api.import_experiment(tmp_path / "run", database)
    before = database.read_bytes()
    assert len(api.query_results(database, limit=1)) == 1
    assert api.query_results(database, snapshot_id="' OR TRUE --") == []
    assert database.read_bytes() == before
    missing = tmp_path / "missing.duckdb"
    with pytest.raises(FileNotFoundError):
        api.query_results(missing)
    assert not missing.exists()


@pytest.mark.parametrize(
    "change", ["foreign", "version", "column", "view", "constraint"]
)
def test_owned_schema_checked_before_import_or_query(
    tmp_path: Path, change: str
) -> None:
    api, database = analytics_module(), tmp_path / "analytics.duckdb"
    classic(tmp_path / "run")
    if change == "foreign":
        with db_connect(database) as db:
            db.execute("CREATE TABLE unrelated (x INTEGER)")
    else:
        api.import_experiment(tmp_path / "run", database)
        with db_connect(database) as db:
            if change == "version":
                db.execute("UPDATE analytics_metadata SET schema_version=999")
            elif change == "column":
                db.execute("ALTER TABLE outcomes ADD COLUMN extra INTEGER")
            elif change == "view":
                db.execute("CREATE OR REPLACE VIEW results_report AS SELECT 1 AS x")
            else:
                db.execute(
                    "CREATE TABLE replacement AS SELECT * FROM source_observations"
                )
                db.execute("DROP TABLE source_observations")
                db.execute("ALTER TABLE replacement RENAME TO source_observations")
    with pytest.raises(RuntimeError, match="schema"):
        api.query_results(database)
    with pytest.raises(RuntimeError, match="schema"):
        api.import_experiment(tmp_path / "run", database)


@pytest.mark.parametrize("operation", ["import", "query"])
@pytest.mark.parametrize(
    "macro_sql",
    [
        pytest.param(
            "CREATE MACRO json_extract_string(value, key) AS '42'",
            id="json-extraction-shadow",
        ),
        pytest.param("CREATE MACRO extra(value) AS value", id="scalar-macro"),
        pytest.param(
            "CREATE MACRO extra() AS TABLE SELECT 1 AS value", id="table-macro"
        ),
    ],
)
def test_owned_schema_rejects_custom_macros(
    tmp_path: Path, macro_sql: str, operation: str
) -> None:
    api, database, output = (
        analytics_module(),
        tmp_path / "analytics.duckdb",
        tmp_path / "run",
    )
    classic(output)
    api.import_experiment(output, database)
    with db_connect(database) as db:
        db.execute(macro_sql)
    with pytest.raises(RuntimeError, match="schema"):
        if operation == "import":
            api.import_experiment(output, database)
        else:
            api.query_results(database)


@pytest.mark.parametrize("operation", ["import", "query"])
@pytest.mark.parametrize(
    ("catalog", "name_column", "change"),
    [
        ("duckdb_functions", "function_name", "CREATE MACRO unowned(value) AS value"),
        ("duckdb_tables", "table_name", "CREATE TABLE unowned (value INTEGER)"),
        ("duckdb_views", "view_name", "CREATE VIEW unowned AS SELECT 1 AS value"),
        (
            "duckdb_indexes",
            "index_name",
            "CREATE INDEX unowned ON job_definitions (symbol)",
        ),
    ],
    ids=("functions", "tables", "views", "indexes"),
)
def test_catalog_table_macros_cannot_hide_schema_changes(
    tmp_path: Path, catalog: str, name_column: str, change: str, operation: str
) -> None:
    api, database, output = (
        analytics_module(),
        tmp_path / "analytics.duckdb",
        tmp_path / "run",
    )
    classic(output)
    api.import_experiment(output, database)
    with db_connect(database) as db:
        db.execute(change)
        db.execute(
            "CREATE MACRO duckdb_functions() AS TABLE "
            "SELECT * FROM system.main.duckdb_functions() WHERE internal"
        )
        if catalog != "duckdb_functions":
            db.execute(
                f"CREATE MACRO {catalog}() AS TABLE "
                f"SELECT * FROM system.main.{catalog}() "
                f"WHERE {name_column} != 'unowned'"
            )
        # The local catalog macro really conceals the divergent object.
        assert (
            db.execute(
                f"SELECT {name_column} FROM {catalog}() WHERE {name_column} = ?",
                ["unowned"],
            ).fetchall()
            == []
        )
    with pytest.raises(RuntimeError, match="schema"):
        if operation == "import":
            api.import_experiment(output, database)
        else:
            api.query_results(database)


def test_report_view_json_extraction_ignores_macro_shadowing(tmp_path: Path) -> None:
    api, database, output = (
        analytics_module(),
        tmp_path / "analytics.duckdb",
        tmp_path / "run",
    )
    classic(output)
    api.import_experiment(output, database)
    report = (
        "SELECT final_equity, total_fees, total_return, maximum_drawdown, fill_count "
        "FROM results_report ORDER BY job_id"
    )
    with db_connect(database) as db:
        expected = db.execute(report).fetchall()
        db.execute("CREATE MACRO json_extract_string(value, key) AS '42'")
    with db_connect(database, readonly=True) as db:
        assert db.execute(report).fetchall() == expected


def test_existing_content_divergence_is_rejected(tmp_path: Path) -> None:
    api, database = analytics_module(), tmp_path / "analytics.duckdb"
    classic(tmp_path / "run")
    api.import_experiment(tmp_path / "run", database)
    with db_connect(database) as db:
        db.execute("UPDATE outcomes SET artifact=?", [b"tampered"])
    with pytest.raises(RuntimeError, match="diverg"):
        api.import_experiment(tmp_path / "run", database)


def test_core_imports_do_not_load_optional_dependencies(tmp_path: Path) -> None:
    script = """
import sys
class BlockOptional:
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'duckdb', 'pyarrow', 'ray'}:
            raise AssertionError(fullname)
sys.meta_path.insert(0, BlockOptional())
import distributed_backtester.analytics
import distributed_backtester.experiment_snapshot
import distributed_backtester.scheduler.journal
"""
    completed = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=15
    )
    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize("operation", ["import", "query"])
def test_native_storage_errors_wrapped_for_cli(tmp_path: Path, operation: str) -> None:
    import duckdb

    api, database = analytics_module(), tmp_path / "corrupt.duckdb"
    database.write_bytes(b"not a database")
    classic(tmp_path / "run")
    with pytest.raises(RuntimeError) as caught:
        if operation == "import":
            api.import_experiment(tmp_path / "run", database)
        else:
            api.query_results(database)
    assert isinstance(caught.value.__cause__, duckdb.Error)
    assert str(caught.value.__cause__) in str(caught.value)


def test_failed_first_import_leaves_no_database_and_retry_succeeds(
    tmp_path: Path,
) -> None:
    api, database = analytics_module(), tmp_path / "analytics.duckdb"
    jobs = classic(tmp_path / "bad")
    rewrite_artifact(tmp_path / "bad", jobs[-1], "metrics")
    with pytest.raises(ValueError):
        api.import_experiment(tmp_path / "bad", database)
    assert not database.exists()
    assert list(tmp_path.glob(".analytics-*")) == []
    classic(tmp_path / "good")
    assert api.import_experiment(tmp_path / "good", database)["inserted"]


def test_source_observation_hash_deduplicates_execution_changes(tmp_path: Path) -> None:
    from hashlib import sha256

    api, database = analytics_module(), tmp_path / "analytics.duckdb"
    output = tmp_path / "run"
    classic(output)
    index = load_json(output / "index.json")
    execution = {
        "schema_version": 1,
        "experiment_id": index["experiment_id"],
        "workers": 1,
    }
    (output / "execution.json").write_text(canonical_json(execution) + "\n")
    first = api.import_experiment(output, database)
    execution["workers"] = 2
    (output / "execution.json").write_text(canonical_json(execution) + "\n")
    second = api.import_experiment(output, database)
    assert first["snapshot_id"] == second["snapshot_id"]
    assert not second["inserted"]
    api.import_experiment(output, database)
    with db_connect(database, readonly=True) as db:
        observations = db.execute(
            "SELECT observation_sha256, content FROM source_observations"
        ).fetchall()
        assert db.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 2
    assert len(observations) == 2
    for digest, content in observations:
        assert sha256(content.encode()).hexdigest() == digest
        assert canonical_json(json.loads(content)) == content


def test_artifacts_are_read_and_inserted_one_at_a_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import distributed_backtester.experiment_snapshot as snapshots

    api, database = analytics_module(), tmp_path / "analytics.duckdb"
    jobs = classic(tmp_path / "run")
    events: list[tuple[str, str]] = []
    original_read = snapshots._read
    original_ensure = api._ensure_row

    def tracked_read(path: Path) -> bytes:
        if path.parent.name == "results":
            events.append(("read", path.stem))
        return original_read(path)

    def tracked_ensure(
        db: Any, table: str, keys: dict[str, object], row: dict[str, object]
    ) -> bool:
        inserted = original_ensure(db, table, keys, row)
        if table == "outcomes":
            events.append(("insert", str(row["job_id"])))
        return bool(inserted)

    monkeypatch.setattr(snapshots, "_read", tracked_read)
    monkeypatch.setattr(api, "_ensure_row", tracked_ensure)
    api.import_experiment(tmp_path / "run", database)
    assert events == [
        (action, job.job_id) for job in jobs for action in ("read", "insert")
    ]


@pytest.mark.parametrize("source", ["classic", "journal"])
def test_parquet_metadata_helpers_preserve_full_provenance(
    tmp_path: Path, source: str
) -> None:
    from dataclasses import replace

    from distributed_backtester.experiment_artifacts import manifest_record
    from distributed_backtester.scheduler.journal import Journal
    from distributed_backtester.scheduler.models import RetryPolicy
    from tests.test_experiment_snapshot import DATA, IDENTITY, jobs_for, result_for

    data = replace(
        DATA,
        metadata=replace(
            DATA.metadata,
            source_format="parquet",
            dataset_version="1" * 64,
            parent_source_sha256="2" * 64,
            bars_sha256="3" * 64,
        ),
    )
    api, database, output = (
        analytics_module(),
        tmp_path / "analytics.duckdb",
        tmp_path / "run",
    )
    if source == "classic":
        classic(output, data=data)
    else:
        jobs = jobs_for(data)
        with Journal.create(
            output,
            manifest_record(jobs, data.metadata, IDENTITY),
            RetryPolicy(),
            now=100,
        ) as journal:
            for _ in jobs:
                attempt = journal.claim(now=100)
                assert attempt is not None
                journal.commit(attempt, result_for(attempt.job, data), now=101)
    api.import_experiment(output, database)
    with db_connect(database, readonly=True) as db:
        manifest = json.loads(
            db.execute("SELECT manifest FROM experiments").fetchone()[0]
        )
        artifact = json.loads(
            db.execute("SELECT artifact FROM outcomes LIMIT 1").fetchone()[0]
        )
    assert manifest["dataset"]["source_format"] == "parquet"
    assert artifact["result"]["dataset"] == manifest["dataset"]
    assert artifact["job"]["dataset"]["dataset_version"] == "1" * 64


def test_distinct_successful_outcome_sets_of_same_definition_coexist(
    tmp_path: Path,
) -> None:
    from dataclasses import replace

    from tests.test_experiment_snapshot import DATA
    from tests.test_simulation import dataset

    api, database = analytics_module(), tmp_path / "analytics.duckdb"
    alternate = replace(dataset([10, 11, 12, 9, 11]), metadata=DATA.metadata)
    classic(tmp_path / "first")
    classic(tmp_path / "second", data=alternate)
    first = api.import_experiment(tmp_path / "first", database)
    second = api.import_experiment(tmp_path / "second", database)
    assert first["experiment_id"] == second["experiment_id"]
    assert first["snapshot_id"] != second["snapshot_id"]
    assert first["succeeded"] == second["succeeded"] == 2
    with db_connect(database, readonly=True) as db:
        assert db.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 4
        assert db.execute("SELECT count(*) FROM snapshots").fetchone()[0] == 2


@pytest.mark.parametrize("change", ["manifest", "record", "membership", "observation"])
def test_all_reimported_semantic_records_reject_divergence(
    tmp_path: Path, change: str
) -> None:
    api, database = analytics_module(), tmp_path / "analytics.duckdb"
    classic(tmp_path / "run")
    api.import_experiment(tmp_path / "run", database)
    with db_connect(database) as db:
        if change == "manifest":
            db.execute("UPDATE experiments SET manifest='{}'")
        elif change == "record":
            db.execute("UPDATE outcomes SET record='{}'")
        elif change == "membership":
            db.execute("DELETE FROM snapshot_membership")
        else:
            db.execute("UPDATE source_observations SET content='{}'")
    with pytest.raises(RuntimeError, match="diverg"):
        api.import_experiment(tmp_path / "run", database)


@pytest.mark.parametrize("source", ["classic", "journal"])
def test_incomplete_import_does_not_publish_database(
    tmp_path: Path, source: str
) -> None:
    from distributed_backtester.experiment_artifacts import manifest_record
    from distributed_backtester.scheduler.journal import Journal
    from distributed_backtester.scheduler.models import RetryPolicy
    from tests.test_experiment_snapshot import DATA, IDENTITY, jobs_for

    api, database, output = (
        analytics_module(),
        tmp_path / "analytics.duckdb",
        tmp_path / "run",
    )
    if source == "classic":
        classic(output)
        (output / "index.json").unlink()
    else:
        jobs = jobs_for()
        with Journal.create(
            output,
            manifest_record(jobs, DATA.metadata, IDENTITY),
            RetryPolicy(),
            now=100,
        ):
            pass
    with pytest.raises((ValueError, RuntimeError, FileNotFoundError)):
        api.import_experiment(output, database)
    assert not database.exists()


def test_database_symlinks_are_rejected_without_writing_targets(tmp_path: Path) -> None:
    api, database = analytics_module(), tmp_path / "analytics.duckdb"
    classic(tmp_path / "run")
    api.import_experiment(tmp_path / "run", database)
    before = database.read_bytes()
    alias = tmp_path / "alias.duckdb"
    alias.symlink_to(database)
    with pytest.raises(ValueError, match="symlink"):
        api.query_results(alias)
    with pytest.raises(ValueError, match="symlink"):
        api.import_experiment(tmp_path / "run", alias)
    assert database.read_bytes() == before

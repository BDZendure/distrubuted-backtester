import importlib
import importlib.util
import json
import sqlite3
from contextlib import closing
from dataclasses import replace
from hashlib import sha256
from itertools import pairwise, product
from pathlib import Path
from typing import Any, cast

import pytest

from distributed_backtester.experiment_artifacts import (
    index_record,
    manifest_record,
    outcome_artifact,
)
from distributed_backtester.jobs import JobSpec, canonical_json, create_job
from distributed_backtester.models import BacktestConfig, MarketDataset
from distributed_backtester.provenance import ImplementationIdentity
from distributed_backtester.scheduler.journal import Journal
from distributed_backtester.scheduler.models import RetryPolicy
from distributed_backtester.simulation import ENGINE_ID, run_backtest
from distributed_backtester.worker import JobFailure, JobOutcome
from tests.test_simulation import dataset

IDENTITY = ImplementationIdentity(ENGINE_ID, "b" * 64)
DATA = dataset([10, 11, 12, 9, 10])


def snapshot_module() -> Any:
    name = "distributed_backtester.experiment_snapshot"
    assert importlib.util.find_spec(name) is not None, "snapshot module is missing"
    return importlib.import_module(name)


def jobs_for(
    data: MarketDataset = DATA, *, cash: int = 1_000_000_000
) -> tuple[JobSpec, ...]:
    return tuple(
        sorted(
            (
                create_job(
                    BacktestConfig(initial_cash=cash, fee_bps=fee),
                    data.metadata,
                    IDENTITY,
                )
                for fee in (0, 10)
            ),
            key=lambda job: job.job_id,
        )
    )


def result_for(job: JobSpec, data: MarketDataset = DATA) -> JobOutcome:
    return JobOutcome(job.job_id, run_backtest(job.config, data), None)


def classic(
    output: Path,
    *,
    failed: bool = False,
    data: MarketDataset = DATA,
    cash: int = 1_000_000_000,
    duplicates: bool = False,
) -> tuple[JobSpec, ...]:
    jobs = jobs_for(data, cash=cash)
    members = [job.job_id for job in jobs]
    if duplicates:
        members += members
    manifest = manifest_record(jobs, data.metadata, IDENTITY, members)
    (output / "results").mkdir(parents=True)
    (output / "manifest.json").write_text(canonical_json(manifest) + "\n")
    records = []
    for i, job in enumerate(jobs):
        outcome = (
            JobOutcome(job.job_id, None, JobFailure("WorkerFailure", "stopped"))
            if failed and i == 1
            else result_for(job, data)
        )
        content, record = outcome_artifact(job, outcome)
        (output / str(record["artifact"])).write_text(content)
        records.append(record)
    (output / "index.json").write_text(
        canonical_json(index_record(str(manifest["experiment_id"]), records)) + "\n"
    )
    return jobs


def durable(output: Path, *, failed: bool = False) -> tuple[JobSpec, ...]:
    jobs = jobs_for()
    with Journal.create(
        output,
        manifest_record(jobs, DATA.metadata, IDENTITY),
        RetryPolicy(max_attempts=1),
        now=100,
    ) as journal:
        for i in range(len(jobs)):
            attempt = journal.claim(now=100)
            assert attempt is not None
            if failed and i == 1:
                journal.fail(attempt, JobFailure("WorkerFailure", "stopped"), now=101)
            else:
                journal.commit(attempt, result_for(attempt.job), now=101)
    return jobs


def load_json(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_bytes()))


def rewrite_artifact(output: Path, job: JobSpec, change: str) -> None:
    path = output / "results" / f"{job.job_id}.json"
    artifact = load_json(path)
    index = load_json(output / "index.json")
    record = next(r for r in index["outcomes"] if r["job_id"] == job.job_id)
    if change == "job":
        artifact["job"]["job_id"] = "f" * 64
    elif change == "config":
        artifact["result"]["config"]["fee_bps"] = 999
    elif change == "dataset":
        artifact["result"]["dataset"]["source_sha256"] = "c" * 64
    elif change == "status":
        artifact["status"] = "failed"
    elif change == "metrics":
        artifact["result"]["metrics"]["fill_count"] += 1
        record["metrics"] = artifact["result"]["metrics"]
    elif change == "metric_link":
        record["metrics"]["fill_count"] += 1
    elif change == "money":
        artifact["result"]["metrics"]["final_equity"] = 1.25
        record["metrics"] = artifact["result"]["metrics"]
    elif change == "null":
        artifact["result"]["equity"] = None
    else:
        artifact["schema_version"] = True
    content = (canonical_json(artifact) + "\n").encode()
    path.write_bytes(content)
    record["content_sha256"] = sha256(content).hexdigest()
    (output / "index.json").write_text(canonical_json(index) + "\n")


@pytest.mark.parametrize("source", ["classic", "journal"])
def test_snapshot_streams_canonical_success_and_failure(
    tmp_path: Path, source: str
) -> None:
    output = tmp_path / "run"
    jobs = (classic if source == "classic" else durable)(output, failed=True)
    with snapshot_module().read_experiment_snapshot(output) as snapshot:
        artifacts = list(snapshot.iter_artifacts())
        assert [item[0] for item in artifacts] == [job.job_id for job in jobs]
        assert len(snapshot.records) == 2
        assert snapshot.submitted == 2
        assert snapshot.succeeded == snapshot.failed == 1
        assert all(isinstance(item[1], bytes) for item in artifacts)
        expected = {
            "experiment_id": snapshot.experiment_id,
            "outcomes": [
                {"job_id": record["job_id"], "content_sha256": record["content_sha256"]}
                for record in snapshot.records
            ],
        }
        assert (
            snapshot.snapshot_id
            == sha256(canonical_json(expected).encode()).hexdigest()
        )
        assert snapshot.observation["source_kind"] == source


def test_journal_preferred_to_truncated_exports_and_corruption_never_falls_back(
    tmp_path: Path,
) -> None:
    output = tmp_path / "run"
    durable(output)
    (output / "manifest.json").write_text("{truncated")
    (output / "index.json").write_text("{truncated")
    with snapshot_module().read_experiment_snapshot(output) as snapshot:
        assert len(list(snapshot.iter_artifacts())) == 2
    (output / "journal.sqlite3").write_bytes(b"corrupt")
    with (
        pytest.raises((ValueError, RuntimeError, sqlite3.Error)),
        snapshot_module().read_experiment_snapshot(output),
    ):
        pass


@pytest.mark.parametrize(
    "change",
    [
        "job",
        "config",
        "dataset",
        "status",
        "metrics",
        "metric_link",
        "money",
        "null",
        "schema",
    ],
)
def test_rehashed_artifact_tampering_rejected(tmp_path: Path, change: str) -> None:
    jobs = classic(tmp_path / "run")
    rewrite_artifact(tmp_path / "run", jobs[-1], change)
    with (
        pytest.raises((ValueError, RuntimeError)),
        snapshot_module().read_experiment_snapshot(tmp_path / "run") as snapshot,
    ):
        list(snapshot.iter_artifacts())


@pytest.mark.parametrize(
    "change",
    [
        "hash",
        "missing",
        "duplicate",
        "counts",
        "extra",
        "traversal",
        "noncanonical",
        "bool",
    ],
)
def test_exact_final_index_required(tmp_path: Path, change: str) -> None:
    output = tmp_path / "run"
    classic(output)
    index = load_json(output / "index.json")
    if change == "hash":
        index["outcomes"][-1]["content_sha256"] = "f" * 64
    elif change == "missing":
        index["outcomes"].pop()
    elif change == "duplicate":
        index["outcomes"][1] = index["outcomes"][0]
    elif change == "counts":
        index["failed"] = 2
    elif change == "extra":
        index["extra"] = 1
    elif change == "bool":
        index["schema_version"] = True
    elif change == "traversal":
        index["outcomes"][0]["artifact"] = "../escape.json"
    text = (
        json.dumps(index, indent=2)
        if change == "noncanonical"
        else canonical_json(index)
    )
    (output / "index.json").write_text(text + "\n")
    with (
        pytest.raises((ValueError, RuntimeError)),
        snapshot_module().read_experiment_snapshot(output) as snapshot,
    ):
        list(snapshot.iter_artifacts())


@pytest.mark.parametrize("source", ["classic", "journal"])
def test_incomplete_run_rejected(tmp_path: Path, source: str) -> None:
    output = tmp_path / "run"
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
    with (
        pytest.raises((ValueError, RuntimeError, FileNotFoundError)),
        snapshot_module().read_experiment_snapshot(output),
    ):
        pass


@pytest.mark.parametrize(
    "target", ["root", "parent", "manifest", "index", "results", "artifact", "journal"]
)
def test_symlinks_rejected(tmp_path: Path, target: str) -> None:
    output = tmp_path / "run"
    jobs = durable(output) if target == "journal" else classic(output)
    if target == "parent":
        alias = tmp_path / "alias"
        alias.symlink_to(tmp_path, target_is_directory=True)
        output = alias / "run"
    else:
        path = {
            "root": output,
            "manifest": output / "manifest.json",
            "index": output / "index.json",
            "results": output / "results",
            "artifact": output / "results" / f"{jobs[0].job_id}.json",
            "journal": output / "journal.sqlite3",
        }[target]
        moved = tmp_path / "moved"
        path.rename(moved)
        path.symlink_to(moved, target_is_directory=moved.is_dir())
    with (
        pytest.raises(ValueError, match="symlink"),
        snapshot_module().read_experiment_snapshot(output) as snapshot,
    ):
        list(snapshot.iter_artifacts())


def test_duplicate_grid_membership_preserves_unique_snapshot_jobs(
    tmp_path: Path,
) -> None:
    classic(tmp_path / "run", duplicates=True)
    with snapshot_module().read_experiment_snapshot(tmp_path / "run") as snapshot:
        assert snapshot.submitted == len(snapshot.records) == 2
        assert len(snapshot.manifest["grid_membership"]) == 4


def test_manifest_must_be_canonical_and_hash_valid(tmp_path: Path) -> None:
    output = tmp_path / "run"
    classic(output)
    manifest = load_json(output / "manifest.json")
    manifest["experiment_id"] = "c" * 64
    (output / "manifest.json").write_text(canonical_json(manifest) + "\n")
    with pytest.raises(ValueError), snapshot_module().read_experiment_snapshot(output):
        pass


def other_data() -> MarketDataset:
    return replace(
        DATA, metadata=replace(DATA.metadata, symbol="OTHER", source_sha256="d" * 64)
    )


@pytest.mark.parametrize("change", ["status", "side"])
def test_malformed_fields_raise_validation_errors(tmp_path: Path, change: str) -> None:
    output = tmp_path / "run"
    jobs = classic(output)
    index = load_json(output / "index.json")
    if change == "status":
        index["outcomes"][0]["status"] = []
    else:
        path = output / "results" / f"{jobs[0].job_id}.json"
        artifact = load_json(path)
        artifact["result"]["fills"][0]["side"] = []
        content = (canonical_json(artifact) + "\n").encode()
        path.write_bytes(content)
        index["outcomes"][0]["content_sha256"] = sha256(content).hexdigest()
    (output / "index.json").write_text(canonical_json(index) + "\n")
    with (
        pytest.raises(ValueError),
        snapshot_module().read_experiment_snapshot(output) as snapshot,
    ):
        list(snapshot.iter_artifacts())


@pytest.mark.parametrize("change", ["index", "execution"])
def test_source_headers_cannot_change_during_stream(
    tmp_path: Path, change: str
) -> None:
    output = tmp_path / "run"
    classic(output)
    index = load_json(output / "index.json")
    execution = {
        "schema_version": 1,
        "experiment_id": index["experiment_id"],
        "workers": 1,
    }
    (output / "execution.json").write_text(canonical_json(execution) + "\n")
    with snapshot_module().read_experiment_snapshot(output) as snapshot:
        stream = snapshot.iter_artifacts()
        next(stream)
        if change == "index":
            index["schema_version"] = True
            (output / "index.json").write_text(canonical_json(index) + "\n")
        else:
            execution["workers"] = 2
            (output / "execution.json").write_text(canonical_json(execution) + "\n")
        with pytest.raises(ValueError, match="changed"):
            list(stream)


def test_corrupt_journal_is_rejected_even_with_valid_classic_exports(
    tmp_path: Path,
) -> None:
    output = tmp_path / "run"
    classic(output)
    (output / "journal.sqlite3").write_bytes(b"not sqlite")
    with (
        pytest.raises(sqlite3.Error),
        snapshot_module().read_experiment_snapshot(output),
    ):
        pass


def test_pretty_printed_manifest_is_rejected(tmp_path: Path) -> None:
    output = tmp_path / "run"
    classic(output)
    (output / "manifest.json").write_text(
        json.dumps(load_json(output / "manifest.json"), indent=2) + "\n"
    )
    with pytest.raises(ValueError), snapshot_module().read_experiment_snapshot(output):
        pass


def _rewrite_result(output: Path, job: JobSpec, source: str, mutation: str) -> None:
    path = output / "results" / f"{job.job_id}.json"
    if source == "journal":
        with closing(sqlite3.connect(output / "journal.sqlite3")) as db:
            row = db.execute(
                "SELECT content, record FROM commits WHERE job_id=?", [job.job_id]
            ).fetchone()
        assert row is not None
        artifact = json.loads(row[0])
        record = json.loads(row[1])
    else:
        artifact = load_json(path)
        index = load_json(output / "index.json")
        record = next(row for row in index["outcomes"] if row["job_id"] == job.job_id)
    result = artifact["result"]
    order, fill = result["order_outcomes"][0], result["fills"][0]
    unknown = {
        "sequence": None,
        "target": None,
        "decision_session": None,
        "execution_session": None,
        "status": "rejected",
        "reason": "invalid_target",
        "fill_sequence": None,
    }
    changes: dict[str, tuple[str, object]] = {
        "target": ("target", {"invalid": True}),
        "sequence": ("sequence", True),
        "decision": ("decision_session", []),
        "execution": ("execution_session", "not-a-date"),
        "reason": ("reason", {"invalid": True}),
        "status": ("status", "not-a-status"),
        "no_link": ("fill_sequence", None),
        "wrong_target": ("target", "flat"),
        "wrong_sequence": ("sequence", 99),
        "wrong_decision": ("decision_session", "2099-01-01"),
    }
    if mutation in changes:
        key, value = changes[mutation]
        order[key] = value
    elif mutation == "orphan":
        result["order_outcomes"] = []
    elif mutation == "duplicate":
        result["order_outcomes"].insert(1, dict(order))
    elif mutation == "too_many_unknown_orders":
        result["order_outcomes"].extend(
            {
                "sequence": None,
                "target": None,
                "decision_session": None,
                "execution_session": None,
                "status": "rejected",
                "reason": "invalid_target",
                "fill_sequence": None,
            }
            for _ in result["equity"]
        )
    elif mutation in ("unknown_after_twice", "valid_unknown_after"):
        result["order_outcomes"].extend(
            dict(unknown) for _ in range(2 if mutation == "unknown_after_twice" else 1)
        )
    elif mutation == "unknown_between":
        result["order_outcomes"].insert(1, unknown)
    elif mutation in ("unknown_before_three", "valid_unknown_before"):
        result["order_outcomes"][:0] = [
            dict(unknown) for _ in range(3 if mutation == "unknown_before_three" else 2)
        ]
    elif mutation in (
        "timing_current_close",
        "valid_timing_past",
        "valid_timing_null",
        "valid_zero_sequence",
    ):
        result["order_outcomes"].append(
            {
                **unknown,
                "target": "flat",
                "sequence": 0 if mutation == "valid_zero_sequence" else 99,
                "reason": "invalid_sequence"
                if mutation == "valid_zero_sequence"
                else "invalid_timing",
                "decision_session": {
                    "timing_current_close": "2026-01-05",
                    "valid_timing_past": "2026-01-03",
                    "valid_timing_null": None,
                    "valid_zero_sequence": None,
                }[mutation],
            }
        )
    elif mutation == "unknown_and_cancel":
        result["order_outcomes"].extend(
            [
                unknown,
                {
                    "sequence": 3,
                    "target": "flat",
                    "decision_session": "2026-01-05",
                    "execution_session": None,
                    "status": "canceled",
                    "reason": "end_of_data",
                    "fill_sequence": None,
                },
            ]
        )
    elif mutation == "fill_balances":
        fill.update(cash_after="9999999999.000000", holdings_after=1234)
    elif mutation == "fill_quantity":
        fill["quantity"] += 1
    elif mutation == "fill_price":
        fill["price"] = "8.000000"
    elif mutation == "fill_fee":
        fill["fee"] = "1.000000"
    elif mutation == "fill_side":
        fill["side"] = "sell"
    elif mutation == "fill_dates":
        fill.update(decision_session="2099-01-01", execution_session="2099-01-02")
    elif mutation == "unexplained_equity":
        result["equity"][0].update(cash="1001.000000", equity="1001.000000")
    elif mutation == "order_chronology":
        result["order_outcomes"].append(
            {
                "sequence": 3,
                "target": "flat",
                "decision_session": "2026-01-01",
                "execution_session": "2026-01-02",
                "status": "no_change",
                "reason": "already_flat",
                "fill_sequence": None,
            }
        )
    elif mutation in (
        "duplicate_null_date",
        "duplicate_future_date",
        "duplicate_past_date",
        "valid_duplicate_date",
        "duplicate_before_execution",
        "duplicate_current_twice",
        "duplicate_accepted_close",
    ):
        result["order_outcomes"].append(
            {
                "sequence": 1,
                "target": "flat",
                "decision_session": {
                    "duplicate_null_date": None,
                    "duplicate_future_date": "2099-01-01",
                    "duplicate_past_date": "2026-01-01",
                    "valid_duplicate_date": "2026-01-05",
                    "duplicate_before_execution": "2026-01-05",
                    "duplicate_current_twice": "2026-01-05",
                    "duplicate_accepted_close": "2026-01-04",
                }[mutation],
                "execution_session": None,
                "status": "rejected",
                "reason": "invalid_sequence",
                "fill_sequence": None,
            }
        )
        if mutation == "duplicate_before_execution":
            result["order_outcomes"].insert(1, result["order_outcomes"].pop())
        elif mutation == "duplicate_current_twice":
            result["order_outcomes"].append(dict(result["order_outcomes"][-1]))
        elif mutation == "duplicate_accepted_close":
            result["order_outcomes"].insert(1, result["order_outcomes"].pop())
    else:
        raise AssertionError(mutation)
    content = (canonical_json(artifact) + "\n").encode()
    record["content_sha256"] = sha256(content).hexdigest()
    if source == "journal":
        text = canonical_json(record)
        with closing(sqlite3.connect(output / "journal.sqlite3")) as db, db:
            db.execute(
                "UPDATE commits SET content=?, content_sha256=?, record=?, "
                "record_sha256=? WHERE job_id=?",
                [
                    content.decode(),
                    record["content_sha256"],
                    text,
                    sha256(text.encode()).hexdigest(),
                    job.job_id,
                ],
            )
    else:
        path.write_bytes(content)
        (output / "index.json").write_text(canonical_json(index) + "\n")


@pytest.mark.parametrize("source", ["classic", "journal"])
@pytest.mark.parametrize(
    "mutation",
    [
        "target",
        "sequence",
        "decision",
        "execution",
        "reason",
        "status",
        "no_link",
        "wrong_target",
        "wrong_sequence",
        "wrong_decision",
        "orphan",
        "duplicate",
        "fill_balances",
        "fill_quantity",
        "fill_price",
        "fill_fee",
        "fill_side",
        "fill_dates",
        "unexplained_equity",
        "order_chronology",
        "duplicate_null_date",
        "duplicate_future_date",
        "duplicate_past_date",
        "duplicate_before_execution",
        "duplicate_current_twice",
        "duplicate_accepted_close",
        "too_many_unknown_orders",
        "unknown_after_twice",
        "unknown_between",
        "unknown_before_three",
        "timing_current_close",
        "unknown_and_cancel",
    ],
)
def test_import_rejects_inconsistent_ledgers_and_rolls_back(
    tmp_path: Path, source: str, mutation: str
) -> None:
    duckdb = pytest.importorskip("duckdb")
    from distributed_backtester.analytics import import_experiment

    initial, corrupted = tmp_path / "initial", tmp_path / "corrupted"
    classic(initial)
    database = tmp_path / "analytics.duckdb"
    import_experiment(initial, database)
    jobs = (classic if source == "classic" else durable)(corrupted)
    _rewrite_result(corrupted, jobs[-1], source, mutation)
    tables = [
        "experiments",
        "job_definitions",
        "snapshots",
        "outcomes",
        "snapshot_membership",
        "source_observations",
    ]
    with duckdb.connect(str(database), read_only=True) as db:
        before = {
            table: db.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
            for table in tables
        }
    with pytest.raises(
        ValueError, match=r"order|fill|portfolio|execution|fee|sequence|session"
    ):
        import_experiment(corrupted, database)
    with duckdb.connect(str(database), read_only=True) as db:
        after = {
            table: db.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
            for table in tables
        }
    assert after == before


@pytest.mark.parametrize(
    "reason,sequence,target",
    [
        ("invalid_target", None, None),
        ("invalid_sequence", None, "flat"),
        ("invalid_timing", 99, "flat"),
    ],
)
def test_rejected_intent_nullable_diagnostics_remain_valid(
    reason: str, sequence: int | None, target: str | None
) -> None:
    module = snapshot_module()
    module._validate_orders(
        [
            {
                "sequence": sequence,
                "target": target,
                "decision_session": None,
                "execution_session": None,
                "status": "rejected",
                "reason": reason,
                "fill_sequence": None,
            }
        ],
        {},
        (DATA.bars[0].session,),
        [],
    )


@pytest.mark.parametrize("scenario", ["canceled", "insufficient_cash"])
def test_valid_unfilled_engine_outcomes_are_accepted(
    tmp_path: Path, scenario: str
) -> None:
    output = tmp_path / "run"
    if scenario == "canceled":
        classic(output, data=dataset([10, 11, 12]))
    else:
        classic(output, cash=1)
    with snapshot_module().read_experiment_snapshot(output) as snapshot:
        artifacts = list(snapshot.iter_artifacts())
    assert len(artifacts) == 2
    statuses = {
        order["status"]
        for _, content, _ in artifacts
        for order in json.loads(content)["result"]["order_outcomes"]
    }
    assert ("canceled" if scenario == "canceled" else "rejected") in statuses


@pytest.mark.parametrize("source", ["classic", "journal"])
def test_positive_duplicate_sequence_with_current_date_remains_valid(
    tmp_path: Path, source: str
) -> None:
    output = tmp_path / "run"
    jobs = (classic if source == "classic" else durable)(output)
    _rewrite_result(output, jobs[-1], source, "valid_duplicate_date")
    with snapshot_module().read_experiment_snapshot(output) as snapshot:
        assert len(list(snapshot.iter_artifacts())) == 2


@pytest.mark.parametrize("source", ["classic", "journal"])
@pytest.mark.parametrize(
    "mutation",
    [
        "valid_unknown_after",
        "valid_unknown_before",
        "valid_timing_past",
        "valid_timing_null",
        "valid_zero_sequence",
    ],
)
def test_public_import_preserves_feasible_rejected_diagnostics(
    tmp_path: Path, source: str, mutation: str
) -> None:
    pytest.importorskip("duckdb")
    from distributed_backtester.analytics import import_experiment, query_results

    output = tmp_path / "run"
    jobs = (classic if source == "classic" else durable)(output)
    _rewrite_result(output, jobs[-1], source, mutation)
    database = tmp_path / "analytics.duckdb"
    assert import_experiment(output, database)["succeeded"] == 2
    assert len(query_results(database)) == 2


@pytest.mark.parametrize("count", [1, 2, 3, 4])
def test_greedy_timeline_matches_exhaustive_close_assignments(count: int) -> None:
    """Pure matcher oracle, separate from complete artifact/engine validation."""
    sessions = tuple(bar.session for bar in DATA.bars[:count])
    markers = [
        *(("open", i) for i in range(1, count)),
        *(("close", i) for i in range(count)),
        ("cancel", count),
    ]
    api = snapshot_module()
    for mask, (kind, index), declared_a, declared_b in product(
        range(1 << count), markers, [None, *sessions], [None, *sessions]
    ):
        reserved = {sessions[i] for i in range(count) if mask & (1 << i)}
        marker: dict[str, object]
        if kind == "open":
            reserved.add(sessions[index - 1])
            marker = {
                "status": "filled",
                "sequence": 1,
                "execution_session": sessions[index].isoformat(),
            }
            event = 2 * index
        elif kind == "close":
            reserved.add(sessions[index])
            marker = {
                "status": "rejected",
                "reason": "invalid_sequence",
                "sequence": 1,
                "execution_session": None,
                "decision_session": sessions[index].isoformat(),
            }
            event = 2 * index + 1
        else:
            reserved.add(sessions[-1])
            marker = {"status": "canceled", "sequence": 1}
            event = 2 * count
        unknown = [
            {
                "status": "rejected",
                "reason": "invalid_timing",
                "sequence": 99,
                "execution_session": None,
                "decision_session": None if declared is None else declared.isoformat(),
            }
            for declared in (declared_a, declared_b)
        ]
        choices = [
            [
                i
                for i, session in enumerate(sessions)
                if session not in reserved and session != declared
            ]
            for declared in (declared_a, declared_b)
        ]
        for order in ((0, 1, 2), (0, 2, 1), (2, 0, 1)):
            # 0/1 name the two unknown closes; 2 names the known event.
            expected = any(
                all(a < b for a, b in pairwise(times))
                for i, j in product(*choices)
                for times in [[(2 * i + 1, 2 * j + 1, event)[item] for item in order]]
            )
            records = [(*unknown, marker)[item] for item in order]
            try:
                api._validate_diagnostic_timeline(records, sessions, reserved)
                actual = True
            except ValueError:
                actual = False
            assert actual == expected, (
                mask,
                kind,
                index,
                declared_a,
                declared_b,
                order,
            )

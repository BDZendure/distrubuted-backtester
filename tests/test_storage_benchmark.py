import importlib
import json
import pickle
import sys
from collections.abc import Callable, Sequence
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest


def module() -> ModuleType:
    return importlib.import_module("distributed_backtester.storage_benchmark")


@pytest.mark.parametrize(
    "options",
    [
        {"bars": 0},
        {"bars": True},
        {"bars": 4000000},
        {"jobs": 0},
        {"repetitions": 0},
        {"seed": -1},
        {"row_group_sizes": []},
        {"row_group_sizes": [0]},
        {"row_group_sizes": [True]},
        {"row_group_sizes": [8, 8]},
        {"row_group_sizes": [67108865]},
        {"discard_trial_results": 1},
        {"timeout_seconds": float("nan")},
    ],
)
def test_invalid_arguments_have_no_side_effects(
    tmp_path: Path,
    options: dict[str, Any],
) -> None:
    output = tmp_path / "output"
    with pytest.raises(ValueError):
        module().run_benchmarks(
            output,
            **{
                "bars": 8,
                "jobs": 2,
                "repetitions": 2,
                "row_group_sizes": [4, 8],
                **options,
            },
        )
    assert not output.exists()


def test_existing_directory_and_symlink_destinations_are_protected(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    sentinel = output / "keep"
    sentinel.write_text("owned by user")
    alias = tmp_path / "alias"
    alias.symlink_to(output, target_is_directory=True)
    for destination in (output, alias, tmp_path / ".git" / "new"):
        with pytest.raises(ValueError, match=r"fresh|protected"):
            module().run_benchmarks(destination, bars=8, jobs=2, row_group_sizes=[4])
    assert sentinel.read_text() == "owned by user"
    assert not (tmp_path / ".git").exists()


def fake_children(
    monkeypatch: pytest.MonkeyPatch,
    *,
    alter: Callable[[dict[str, Any]], None] | None = None,
    returncode: int = 0,
) -> list[list[str]]:
    benchmark = module()
    measurement = importlib.import_module("distributed_backtester.storage_measurement")
    commands: list[list[str]] = []

    def command(arguments: Sequence[str], **kwargs: Any) -> Any:
        arguments = list(arguments)
        commands.append(arguments)
        request = Path(arguments[arguments.index("--request") + 1])
        output = Path(arguments[arguments.index("--output") + 1])
        kwargs["stdout"].write_text("")
        kwargs["stderr"].write_text("failed" if returncode else "")
        if not returncode:
            measurement.read_child(json.loads(request.read_text()), output)
            if alter:
                record = json.loads(output.read_text())
                alter(record)
                output.write_text(json.dumps(record))
        return measurement.CommandMeasurement(returncode, 0.5)

    def transfer(payload: bytes, **kwargs: Any) -> Any:
        data = pickle.loads(payload)
        child = {
            "status": "verified",
            "identity": measurement.verify_dataset(data),
            "payload_bytes": len(payload),
            "payload_sha256": sha256(payload).hexdigest(),
            "receive_seconds": 0.01,
            "deserialize_seconds": 0.01,
            "pid": 123,
            "memory": measurement.peak_rss(),
            "environment": benchmark.capture_environment(),
            "implementation": asdict(benchmark.current_implementation()),
        }
        return measurement.TransferMeasurement(0.1, 0.3, child)

    monkeypatch.setattr(benchmark, "measure_command", command)
    monkeypatch.setattr(benchmark, "transfer_payload", transfer)
    # Active edits by other workers are irrelevant to these in-process tests.
    identity = benchmark.current_implementation()
    monkeypatch.setattr(benchmark, "current_implementation", lambda: identity)
    sequential = importlib.import_module("distributed_backtester.backends.sequential")
    monkeypatch.setattr(sequential, "current_implementation", lambda: identity)
    monkeypatch.setattr(
        measurement,
        "_context",
        lambda: {
            "environment": benchmark.capture_environment(),
            "implementation": asdict(identity),
        },
    )
    return commands


def test_repetitions_layout_rotation_raw_samples_and_verified_digests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyarrow")
    commands = fake_children(monkeypatch)
    output = tmp_path / "benchmark"
    report = module().run_benchmarks(
        output, bars=16, jobs=3, repetitions=2, row_group_sizes=[4, 8]
    )
    rows = report["samples"]
    assert len(rows) == 2 * 3 * 6
    assert len(commands) == 6
    assert all(
        command[:3]
        == [sys.executable, "-m", "distributed_backtester.storage_measurement"]
        for command in commands
    )
    assert [row["layout"] for row in rows if row["kind"] == "experiment"] == [
        "csv",
        "parquet-rg4",
        "parquet-rg8",
        "parquet-rg4",
        "parquet-rg8",
        "csv",
    ]
    assert {row["kind"] for row in rows} == {
        "experiment",
        "first_process_read",
        "warm_read",
        "cache_hit",
        "pickle_protocol5",
        "spawn_transfer",
    }
    assert len({row["bars_sha256"] for row in rows}) == 1
    assert len({row["economic_sha256"] for row in rows}) == 1
    assert len({row["source_sha256"] for row in rows}) == 3
    assert (
        len(
            {
                row["artifact_sha256"]
                for row in rows
                if row["kind"] == "experiment" and row["layout"] == "csv"
            }
        )
        == 1
    )
    for row in rows:
        assert row["verified"] is True
        assert row["elapsed_seconds"] >= 0
        assert row["end_to_end_seconds"] >= row["elapsed_seconds"]
        assert row["time_unit"] == "seconds"
        assert row["file_bytes"] > 0
        assert row["workload_id"] == report["workload_id"]
        assert row["implementation"]["source_sha256"]
        assert row["environment"]["python_version"]
        assert row["memory"]["scope"] == "own_process_lifetime_high_water"
        assert row["cache_state"] == "OS cache uncontrolled"
    hits = [row for row in rows if row["kind"] == "cache_hit"]
    assert all(
        row["cache_hits_delta"] == 1 and row["cache_misses_delta"] == 0 for row in hits
    )
    for row in rows:
        if row["kind"] in {"pickle_protocol5", "spawn_transfer"}:
            assert row["pickle_protocol"] == 5
            assert row["payload_bytes"] > 0
            assert len(row["payload_sha256"]) == 64
    assert report["status"] == "completed"
    assert (output / "manifest.json").is_file()
    assert json.loads((output / "report.json").read_text())["samples"] == rows
    assert (
        json.loads((output / "observations.json").read_text())["status"] == "completed"
    )
    assert len(report["summary"]) == 18


def test_bad_child_digest_preserves_observations_and_prevents_discard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyarrow")
    fake_children(
        monkeypatch, alter=lambda row: row["identity"].update(bars_sha256="0" * 64)
    )
    output = tmp_path / "output"
    with pytest.raises(RuntimeError, match="identity"):
        module().run_benchmarks(
            output, bars=8, jobs=2, row_group_sizes=[4], discard_trial_results=True
        )
    observations = json.loads((output / "observations.json").read_text())
    assert observations["status"] == "failed"
    assert observations["samples"][-1]["verified"] is False
    assert observations["samples"][-1]["error"]
    assert not (output / "report.json").exists()
    assert (output / "raw" / "run-1-csv-first_process_read.json").exists()


def test_failed_child_retains_returncode_and_stderr(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyarrow")
    fake_children(monkeypatch, returncode=7)
    output = tmp_path / "output"
    with pytest.raises(RuntimeError, match="child"):
        module().run_benchmarks(output, bars=8, jobs=2, row_group_sizes=[4])
    row = json.loads((output / "observations.json").read_text())["samples"][-1]
    assert row["returncode"] == 7
    assert row["end_to_end_seconds"] == 0.5
    assert (
        output / "raw" / "run-1-csv-first_process_read.stderr"
    ).read_text() == "failed"
    assert not (output / "report.json").exists()


def test_discard_keeps_manifests_indexes_hashes_and_unowned_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyarrow")
    fake_children(monkeypatch)
    benchmark = module()
    original = benchmark.verify_experiment

    def verify(output: Path, expected: dict[str, object]) -> dict[str, Any]:
        (output / "results" / "unowned.txt").write_text("keep")
        return cast(dict[str, Any], original(output, expected))

    monkeypatch.setattr(benchmark, "verify_experiment", verify)
    output = tmp_path / "output"
    report = benchmark.run_benchmarks(
        output,
        bars=8,
        jobs=2,
        repetitions=1,
        row_group_sizes=[4],
        discard_trial_results=True,
    )
    for row in report["samples"]:
        if row["kind"] != "experiment":
            continue
        trial = output / row["trial_directory"]
        assert (trial / "manifest.json").exists()
        assert (trial / "index.json").exists()
        assert (trial / "execution.json").exists()
        assert (trial / "results" / "unowned.txt").read_text() == "keep"
        assert not list((trial / "results").glob("*.json"))
        assert len(row["discarded_artifacts"]) == 2
        assert row["discarded_artifact_bytes"] > 0
        assert row["artifact_sha256"]
        assert row["economic_sha256"]


def test_corrupt_experiment_artifact_never_discarded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyarrow")
    fake_children(monkeypatch)
    benchmark = module()
    original = benchmark.verify_experiment

    def corrupt(output: Path, expected: dict[str, object]) -> dict[str, Any]:
        artifact = next((output / "results").glob("*.json"))
        artifact.write_text("corrupt")
        return cast(dict[str, Any], original(output, expected))

    monkeypatch.setattr(benchmark, "verify_experiment", corrupt)
    output = tmp_path / "output"
    with pytest.raises((ValueError, RuntimeError), match=r"digest|hash"):
        benchmark.run_benchmarks(
            output, bars=8, jobs=2, row_group_sizes=[4], discard_trial_results=True
        )
    assert list((output / "trials" / "run-1-csv" / "results").glob("*.json"))
    assert not (output / "report.json").exists()


def test_cli_arguments_and_explicit_discard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = module()
    calls: list[dict[str, Any]] = []

    def run(output: Path, **kwargs: Any) -> dict[str, object]:
        assert output == tmp_path / "benchmark"
        calls.append(kwargs)
        return {"summary": []}

    monkeypatch.setattr(benchmark, "run_benchmarks", run)
    assert (
        benchmark.main(
            [
                "run",
                "--bars",
                "8",
                "--jobs",
                "2",
                "--repetitions",
                "3",
                "--row-group-sizes",
                "8192",
                "65536",
                "--output-dir",
                str(tmp_path / "benchmark"),
                "--discard-trial-results",
            ]
        )
        == 0
    )
    assert calls[0]["row_group_sizes"] == [8192, 65536]
    assert calls[0]["discard_trial_results"] is True
    assert calls[0]["repetitions"] == 3


def test_cli_errors_are_nonzero_without_measurements(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert (
        module().main(
            [
                "run",
                "--bars",
                "0",
                "--jobs",
                "2",
                "--output-dir",
                str(tmp_path / "output"),
            ]
        )
        == 2
    )
    assert "error:" in capsys.readouterr().err
    assert not (tmp_path / "output").exists()


def test_transfer_failure_retains_raw_child_and_timing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyarrow")
    fake_children(monkeypatch)
    benchmark = module()
    measurement = importlib.import_module("distributed_backtester.storage_measurement")

    def fail(payload: bytes, **kwargs: Any) -> Any:
        raise measurement.MeasurementFailure(
            "exit failed",
            {
                "stage": "spawn_transfer",
                "returncode": 7,
                "end_to_end_seconds": 0.9,
                "transfer_seconds": 0.4,
                "child": {"status": "verified", "payload_bytes": len(payload)},
            },
        )

    monkeypatch.setattr(benchmark, "transfer_payload", fail)
    output = tmp_path / "output"
    with pytest.raises(RuntimeError, match="exit failed"):
        benchmark.run_benchmarks(output, bars=8, jobs=2, row_group_sizes=[4])
    row = json.loads((output / "observations.json").read_text())["samples"][-1]
    assert row["measurement_failure"]["returncode"] == 7
    assert row["end_to_end_seconds"] == 0.9
    raw = json.loads(
        (output / "raw" / "run-1-csv-spawn_transfer.failure.json").read_text()
    )
    assert raw["child"]["status"] == "verified"
    assert raw["transfer_seconds"] == 0.4


@pytest.mark.parametrize(
    "problem", ["unverified", "symlink", "changed", "traversal", "owner"]
)
def test_discard_rejects_unsafe_artifacts_before_deleting_any(
    tmp_path: Path,
    problem: str,
) -> None:
    benchmark = module()
    root = (tmp_path / "root").resolve()
    trial = root / "trials" / "run-1-csv"
    results = trial / "results"
    results.mkdir(parents=True)
    owner = {"owner_token": "token", "benchmark_root": str(root)}
    for directory in (root, trial):
        (directory / benchmark._OWNER).write_text(json.dumps(owner))
    first = results / ("1" * 64 + ".json")
    second = results / ("2" * 64 + ".json")
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    row: dict[str, Any] = {
        "verified": True,
        "discarded_artifacts": [],
        "discarded_artifact_bytes": 0,
        "artifacts": [
            {
                "artifact": str(path.relative_to(trial)),
                "bytes": path.stat().st_size,
                "content_sha256": sha256(path.read_bytes()).hexdigest(),
            }
            for path in (first, second)
        ],
    }
    if problem == "unverified":
        row["verified"] = False
    elif problem == "symlink":
        second.unlink()
        second.symlink_to(first)
    elif problem == "changed":
        second.write_bytes(b"changed")
    elif problem == "traversal":
        row["artifacts"][1]["artifact"] = "../outside.json"
    else:
        (trial / benchmark._OWNER).write_text(
            json.dumps({**owner, "owner_token": "other"})
        )
    with pytest.raises(RuntimeError):
        benchmark._discard_results(root, trial, "token", row)
    assert first.read_bytes() == b"first"
    assert row["discarded_artifacts"] == []


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
def test_interruption_records_active_error_and_terminal_status_before_reraising(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    interrupt: type[BaseException],
) -> None:
    pytest.importorskip("pyarrow")
    fake_children(monkeypatch)
    benchmark = module()

    def stop(payload: bytes, **kwargs: Any) -> Any:
        raise interrupt("operator stopped")

    monkeypatch.setattr(benchmark, "transfer_payload", stop)
    output = tmp_path / "output"
    with pytest.raises(interrupt, match="operator stopped"):
        benchmark.run_benchmarks(output, bars=8, jobs=2, row_group_sizes=[4])
    observations = json.loads((output / "observations.json").read_text())
    assert observations["status"] in {"interrupted", "failed"}
    assert interrupt.__name__ in observations["error"]
    row = observations["samples"][-1]
    assert row["kind"] == "spawn_transfer"
    assert row["verified"] is False
    assert row["error"] == f"{interrupt.__name__}: operator stopped"
    assert not (output / "report.json").exists()

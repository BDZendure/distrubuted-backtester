"""Synthetic elapsed records over real artifacts test comparison correctness."""

import json
import sys
from collections.abc import Iterator, Sequence
from dataclasses import asdict, replace
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from distributed_backtester.benchmark import semantic_digest
from distributed_backtester.data import load_csv
from distributed_backtester.experiment_artifacts import manifest_record
from distributed_backtester.experiment_snapshot import read_experiment_snapshot
from distributed_backtester.experiments import collect_experiment
from distributed_backtester.jobs import JobSpec, canonical_json, create_job
from distributed_backtester.models import MarketDataset
from distributed_backtester.provenance import ImplementationIdentity
from distributed_backtester.simulation import ENGINE_ID, run_backtest
from distributed_backtester.worker import JobOutcome, execute_job
from distributed_backtester.workloads import job_configs, write_synthetic_csv


class FixtureBackend:
    """Real sequential simulation with a declared synthetic producer identity."""

    name = "sequential"
    workers = 1

    def iter_outcomes(
        self,
        jobs: Sequence[JobSpec],
        dataset: MarketDataset,
        implementation: ImplementationIdentity,
    ) -> Iterator[JobOutcome]:
        for job in reversed(jobs):
            if implementation.engine_id == ENGINE_ID:
                yield execute_job(job, dataset, implementation)
            else:
                # Fixture-only legacy release label, not a legacy runtime claim.
                result = replace(
                    run_backtest(job.config, dataset),
                    engine_id=implementation.engine_id,
                )
                yield JobOutcome(job.job_id, result=result, failure=None)


def comparison() -> Any:
    from distributed_backtester.performance import compare_benchmarks

    return compare_benchmarks


def fixture(
    root: Path, letter: str, times: tuple[float, float], *, engine: str = ENGINE_ID
) -> dict[str, Any]:
    root.mkdir()
    source = root / "data.csv"
    write_synthetic_csv(source, bars=16)
    dataset = load_csv(source, symbol="BENCH", price_basis="raw")
    identity = ImplementationIdentity(engine, letter * 64)
    specs = tuple(
        create_job(config, dataset.metadata, identity) for config in job_configs(2)
    )
    expected = manifest_record(
        specs, dataset.metadata, identity, [job.job_id for job in specs]
    )
    (root / "expected_manifest.json").write_text(canonical_json(expected) + "\n")
    rows: list[dict[str, Any]] = []
    for label, elapsed in [
        ("warmup-sequential", 12.0),
        ("run-1-sequential", times[0]),
        ("run-2-sequential", times[1]),
    ]:
        output = root / label
        summary = collect_experiment(
            specs, dataset, identity, output, FixtureBackend(), source_path=source
        )
        rows.append(
            {
                "configuration": "sequential",
                "workers": 1,
                "label": label,
                "command": [
                    sys.executable,
                    "-m",
                    "distributed_backtester.ray_benchmark",
                    "_trial",
                    "--data",
                    str(source),
                    "--jobs",
                    "2",
                    "--backend",
                    "sequential",
                    "--workers",
                    "1",
                    "--output-dir",
                    str(output),
                    "--ownership",
                    str(root / f"{label}.ownership.json"),
                ],
                "returncode": 0,
                "elapsed_seconds": elapsed,
                "peak_private_uss_bytes": None,
                "detailed_results_retained": True,
                "discarded_artifacts": [],
                "discarded_artifact_bytes": 0,
                "experiment_id": summary.experiment_id,
                "semantic_sha256": semantic_digest(output),
                "input_validation": {
                    "expected_csv_sha256": dataset.metadata.source_sha256,
                    "actual_csv_sha256": dataset.metadata.source_sha256,
                    "expected_experiment_id": summary.experiment_id,
                    "actual_experiment_id": summary.experiment_id,
                    "changed_manifest_fields": [],
                },
            }
        )
    environment = {
        "python_version": "3.14.6",
        "python_implementation": "CPython",
        "platform": "fixture-platform",
        "machine": "fixture-machine",
        "logical_cores": 11,
        "available_cores": 11,
        "thread_environment": {},
        "distributions": [
            {
                "name": "distributed-backtester",
                "version": engine.split("/")[1].split(":")[0],
            }
        ],
    }
    record: dict[str, Any] = {
        "schema_version": 1,
        "started_utc": "2026-10-10T00:00:00+00:00",
        "finished_utc": "2026-10-10T00:01:00+00:00",
        "environment": environment,
        "hardware": {"fixture": "same host"},
        "dependencies": {"ray_version": "2.59.0", "psutil_version": "7.2.2"},
        "python_executable": sys.executable,
        "implementation": asdict(identity),
        "workload": {
            "bars": 16,
            "jobs": 2,
            "seed": 1729,
            "symbol": "BENCH",
            "source_sha256": dataset.metadata.source_sha256,
            "expected_experiment_id": expected["experiment_id"],
            "expected_manifest": "expected_manifest.json",
            "config_generator": "bounded-sma-windows-cash-batches-v1",
            "fee_bps": 10,
        },
        "method": {
            "timer": "perf_counter before Popen through child exit",
            "includes": "same complete pipeline",
            "excludes": "generation and verification",
            "cache": "OS caches uncontrolled",
            "warmups_per_configuration": 1,
            "order": "rotate one position per repetition",
            "interval_seconds": 0.05,
            "repetitions": 2,
            "memory": "same declared observer",
        },
        "warmups": rows[:1],
        "runs": rows[1:],
        "semantic_outputs_equal": True,
        "detailed_results_retained": True,
        "discard_method": None,
        "semantic_sha256": rows[0]["semantic_sha256"],
        "summary": [],
    }
    # Synthetic host metadata is bound consistently to each child fixture.
    for row in rows:
        path = root / row["label"] / "execution.json"
        execution = json.loads(path.read_text())
        execution["environment"] = environment
        execution["python_executable"] = sys.executable
        path.write_text(canonical_json(execution) + "\n")
    save(root, record)
    return record


def save(root: Path, record: dict[str, Any]) -> None:
    (root / "report.json").write_text(canonical_json(record) + "\n")


def pair(tmp_path: Path) -> tuple[Path, Path, dict[str, Any]]:
    baseline, candidate = tmp_path / "baseline", tmp_path / "candidate"
    fixture(baseline, "a", (9.0, 11.0))
    record = fixture(candidate, "b", (7.0, 9.0))
    return baseline, candidate, record


def test_distinct_job_ids_same_full_results_and_exact_ratio(tmp_path: Path) -> None:
    baseline, candidate, _ = pair(tmp_path)
    result = comparison()(baseline, candidate, tmp_path / "comparison.json")
    assert result["economic_outputs_equal"] is True
    row = result["summary"][0]
    assert row["baseline_median_seconds"] == 10
    assert row["candidate_median_seconds"] == 8
    assert row["optimization_speedup"] == 1.25
    assert row["runtime_reduction_percent"] == pytest.approx(20)
    assert row["candidate_jobs_per_second"] == 0.25
    assert row["baseline_parallel_efficiency"] == 1
    assert row["candidate_parallel_efficiency"] == 1
    assert row["candidate_median_peak_private_uss_bytes"] is None
    assert json.loads((tmp_path / "comparison.json").read_text()) == result


@pytest.mark.parametrize(
    "change",
    [
        "failed",
        "discarded",
        "missing-run",
        "duplicate-label",
        "zero-time",
        "bool-time",
        "nan-time",
        "wrong-source",
        "wrong-code",
        "wrong-engine",
        "wrong-workers",
        "missing-warmup",
        "source-file",
        "summary-only",
    ],
)
def test_rejects_invalid_or_unverified_reports(tmp_path: Path, change: str) -> None:
    baseline, candidate, record = pair(tmp_path)
    if change == "failed":
        record["runs"][0]["returncode"] = 1
    elif change == "discarded":
        record["detailed_results_retained"] = False
    elif change == "missing-run":
        record["runs"].pop()
    elif change == "duplicate-label":
        record["runs"][1]["label"] = record["runs"][0]["label"]
    elif change == "zero-time":
        record["runs"][0]["elapsed_seconds"] = 0
    elif change == "bool-time":
        record["runs"][0]["elapsed_seconds"] = True
    elif change == "nan-time":
        record["runs"][0]["elapsed_seconds"] = float("nan")
    elif change == "wrong-source":
        record["workload"]["source_sha256"] = "c" * 64
    elif change == "wrong-code":
        record["implementation"]["source_sha256"] = "c" * 64
    elif change == "wrong-engine":
        record["implementation"]["engine_id"] = ENGINE_ID.replace("v1", "v2")
    elif change == "wrong-workers":
        record["runs"][0]["workers"] = 2
    elif change == "missing-warmup":
        record["warmups"] = []
    elif change == "source-file":
        (candidate / "data.csv").write_text("broken\n")
    elif change == "summary-only":
        record["runs"] = []
    if change == "nan-time":
        (candidate / "report.json").write_text(json.dumps(record))
    else:
        save(candidate, record)
    output = tmp_path / "comparison.json"
    with pytest.raises((ValueError, RuntimeError, OSError)):
        comparison()(baseline, candidate, output)
    assert not output.exists()


@pytest.mark.parametrize(
    "key",
    [
        "python_version",
        "platform",
        "machine",
        "available_cores",
        "thread_environment",
        "distributions",
    ],
)
def test_rejects_changed_environment(tmp_path: Path, key: str) -> None:
    baseline, candidate, record = pair(tmp_path)
    record["environment"][key] = "different"
    save(candidate, record)
    with pytest.raises((ValueError, RuntimeError)):
        comparison()(baseline, candidate, tmp_path / "comparison.json")


def test_changed_method_is_not_comparable(tmp_path: Path) -> None:
    baseline, candidate, record = pair(tmp_path)
    record["method"]["includes"] = "simulation only"
    save(candidate, record)
    with pytest.raises((ValueError, RuntimeError)):
        comparison()(baseline, candidate, tmp_path / "comparison.json")


@pytest.mark.parametrize(
    "item", ["artifact", "index", "expected", "manifest", "execution"]
)
def test_rejects_tampering_and_missing_artifacts(tmp_path: Path, item: str) -> None:
    baseline, candidate, _ = pair(tmp_path)
    trial = candidate / "run-1-sequential"
    if item == "artifact":
        next((trial / "results").glob("*.json")).unlink()
    elif item == "index":
        (trial / "index.json").write_text("{}\n")
    elif item == "expected":
        (candidate / "expected_manifest.json").write_text("{}\n")
    elif item == "manifest":
        (trial / "manifest.json").write_text("{}\n")
    elif item == "execution":
        (trial / "execution.json").unlink()
    with pytest.raises((ValueError, RuntimeError, OSError)):
        comparison()(baseline, candidate, tmp_path / "comparison.json")


def test_existing_output_is_protected(tmp_path: Path) -> None:
    baseline, candidate, _ = pair(tmp_path)
    output = tmp_path / "comparison.json"
    output.write_text("original")
    with pytest.raises(FileExistsError):
        comparison()(baseline, candidate, output)
    assert output.read_text() == "original"


def test_input_alias_and_symlinks_are_rejected(tmp_path: Path) -> None:
    baseline, candidate, _ = pair(tmp_path)
    with pytest.raises((ValueError, FileExistsError)):
        comparison()(baseline, candidate, baseline / "report.json")
    link = tmp_path / "linked"
    link.symlink_to(candidate, target_is_directory=True)
    with pytest.raises((ValueError, RuntimeError)):
        comparison()(baseline, link, tmp_path / "comparison.json")


def test_wrong_workload_even_with_matching_header_fields(tmp_path: Path) -> None:
    baseline, candidate, record = pair(tmp_path)
    record["workload"]["jobs"] = 3
    save(candidate, record)
    with pytest.raises((ValueError, RuntimeError)):
        comparison()(baseline, candidate, tmp_path / "comparison.json")


def test_comparison_does_not_modify_inputs(tmp_path: Path) -> None:
    baseline, candidate, _ = pair(tmp_path)
    before = {
        str(p): p.read_bytes()
        for root in [baseline, candidate]
        for p in root.rglob("*.json")
    }
    comparison()(baseline, candidate, tmp_path / "comparison.json")
    assert before == {
        str(p): p.read_bytes()
        for root in [baseline, candidate]
        for p in root.rglob("*.json")
    }


def test_cli_help_and_errors() -> None:
    from distributed_backtester.performance import main

    with pytest.raises(SystemExit) as help_exit:
        main(["--help"])
    assert help_exit.value.code == 0
    assert (
        main(
            [
                "compare",
                "--baseline",
                "/missing/baseline",
                "--candidate",
                "/missing/candidate",
                "--output",
                "/missing/comparison.json",
            ]
        )
        == 2
    )


def test_valid_changed_trace_with_same_metrics_is_rejected(tmp_path: Path) -> None:
    baseline, candidate, report = pair(tmp_path)
    for row in [*report["warmups"], *report["runs"]]:
        trial = candidate / row["label"]
        index_path = trial / "index.json"
        index = json.loads(index_path.read_text())
        entry = index["outcomes"][0]
        artifact_path = trial / entry["artifact"]
        artifact = json.loads(artifact_path.read_text())
        before_metrics = artifact["result"]["metrics"].copy()
        point = artifact["result"]["equity"][0]
        # No shares are held on this first close, so this remains a valid ledger.
        point["close"] = (
            "100.000001" if point["close"] != "100.000001" else "100.000002"
        )
        content = (canonical_json(artifact) + "\n").encode()
        artifact_path.write_bytes(content)
        entry["content_sha256"] = sha256(content).hexdigest()
        index_path.write_text(canonical_json(index) + "\n")
        with read_experiment_snapshot(trial) as snapshot:
            list(snapshot.iter_artifacts())
        assert artifact["result"]["metrics"] == before_metrics
        row["semantic_sha256"] = semantic_digest(trial)
    report["semantic_sha256"] = report["warmups"][0]["semantic_sha256"]
    save(candidate, report)
    with pytest.raises(ValueError, match="economic"):
        comparison()(baseline, candidate, tmp_path / "comparison.json")


@pytest.mark.parametrize("field", ["hardware", "dependencies", "python_executable"])
def test_missing_host_metadata_is_rejected(tmp_path: Path, field: str) -> None:
    baseline, candidate, _ = pair(tmp_path)
    for root in (baseline, candidate):
        report = json.loads((root / "report.json").read_text())
        report.pop(field)
        save(root, report)
    with pytest.raises(ValueError):
        comparison()(baseline, candidate, tmp_path / "comparison.json")


def test_changed_interpreter_command_is_rejected(tmp_path: Path) -> None:
    baseline, candidate, report = pair(tmp_path)
    report["runs"][0]["command"][0] = "/different/python"
    save(candidate, report)
    with pytest.raises(ValueError):
        comparison()(baseline, candidate, tmp_path / "comparison.json")


def test_changed_queue_bound_is_rejected(tmp_path: Path) -> None:
    baseline, candidate, _ = pair(tmp_path)
    path = candidate / "run-1-sequential" / "execution.json"
    execution = json.loads(path.read_text())
    execution["max_in_flight"] = 20
    path.write_text(canonical_json(execution) + "\n")
    with pytest.raises(ValueError):
        comparison()(baseline, candidate, tmp_path / "comparison.json")


def test_only_validated_release_label_is_normalized(tmp_path: Path) -> None:
    baseline, candidate = tmp_path / "baseline", tmp_path / "candidate"
    fixture(
        baseline,
        "a",
        (9.0, 11.0),
        engine="distributed-backtester/0.6.0:daily-long-only-v1",
    )
    fixture(candidate, "b", (7.0, 9.0))
    result = comparison()(baseline, candidate, tmp_path / "comparison.json")
    assert result["economic_outputs_equal"] is True
    assert (
        result["baseline"]["implementation"]["engine_id"]
        != result["candidate"]["implementation"]["engine_id"]
    )


@pytest.mark.parametrize(
    "field",
    ["environment", "python_executable", "python_version", "thread_environment"],
)
def test_child_environment_must_match_parent(tmp_path: Path, field: str) -> None:
    baseline, candidate, _ = pair(tmp_path)
    path = candidate / "run-1-sequential" / "execution.json"
    execution = json.loads(path.read_text())
    if field in ("environment", "python_executable"):
        execution.pop(field)
    elif field == "python_version":
        execution["environment"][field] = "3.12.0"
    else:
        execution["environment"][field] = {"OMP_NUM_THREADS": "99"}
    path.write_text(canonical_json(execution) + "\n")
    with pytest.raises(ValueError):
        comparison()(baseline, candidate, tmp_path / "comparison.json")


def test_ray_policy_projection_preserves_configured_resources() -> None:
    from distributed_backtester.performance import _resources

    diagnostics = {
        "local_owned": True,
        "ray_version": "2.59.0",
        "cpus_per_worker": 1.0,
        "resources_per_worker": {},
        "max_restarts": 0,
        "max_task_retries": 0,
        "object_store_capacity_bytes": 134217728,
    }
    execution = {
        "max_in_flight": 8,
        "max_observed_in_flight": 4,
        "backend_diagnostics": diagnostics,
    }
    resources = _resources(execution, {"backend_diagnostics": diagnostics}, "ray", 4)
    assert resources["workers"] == 4
    assert resources["object_store_capacity_bytes"] == 134217728
    assert resources["cpus_per_worker"] == 1.0
    with pytest.raises(ValueError, match="queue bound"):
        _resources(
            {**execution, "max_in_flight": 9},
            {"backend_diagnostics": diagnostics},
            "ray",
            4,
        )
    with pytest.raises(ValueError, match="resources"):
        changed = {**diagnostics, "cpus_per_worker": 2.0}
        _resources(
            {**execution, "backend_diagnostics": changed},
            {"backend_diagnostics": changed},
            "ray",
            4,
        )

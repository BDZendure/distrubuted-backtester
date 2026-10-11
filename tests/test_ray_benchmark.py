import importlib
import json
import os
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from tests.test_ray_measurement import FakeProcess, boundary


def module() -> ModuleType:
    return importlib.import_module("distributed_backtester.ray_benchmark")


@pytest.mark.parametrize(
    "options",
    [
        {"bars": 0},
        {"jobs": 0},
        {"repetitions": 0},
        {"bars": True},
        {"workers": [0]},
        {"workers": [True]},
        {"workers": [2, 2]},
        {"workers": []},
        {"interval_seconds": float("nan")},
        {"interval_seconds": True},
        {"seed": -1},
    ],
)
def test_invalid_parameters_create_no_output(
    tmp_path: Path, options: dict[str, Any]
) -> None:
    kwargs = {"bars": 8, "jobs": 3, "repetitions": 2, "workers": [2], **options}
    output = tmp_path / "output"
    with pytest.raises(ValueError):
        module().run_benchmarks(output, **kwargs)
    assert not output.exists()


def test_imports_are_optional_in_new_interpreter(tmp_path: Path) -> None:
    import subprocess

    script = """
import sys
class RejectOptional:
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'ray', 'psutil'}:
            raise AssertionError('optional dependency imported eagerly: ' + fullname)
sys.meta_path.insert(0, RejectOptional())
import distributed_backtester.ray_benchmark
import distributed_backtester.ray_measurement
"""
    completed = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True
    )
    assert completed.returncode == 0, completed.stderr


def fake_trials(
    monkeypatch: pytest.MonkeyPatch,
    *,
    alter: Callable[[Path, str], None] | None = None,
) -> list[list[str]]:
    benchmark = module()
    measurement = importlib.import_module("distributed_backtester.ray_measurement")
    commands: list[list[str]] = []

    def measure(command: Sequence[str], **kwargs: Any) -> Any:
        arguments = list(command)
        commands.append(arguments)

        def option(name: str) -> str:
            return arguments[arguments.index(name) + 1]

        output = Path(option("--output-dir"))
        backend = option("--backend")
        # Exercise real CSV loading, job construction, export, and digest checks.
        # Only the fresh subprocess/backend boundary is substituted in this unit test.
        benchmark._trial(
            Path(option("--data")),
            int(option("--jobs")),
            "sequential",
            1,
            output,
            Path(option("--ownership")),
        )
        execution_path = output / "execution.json"
        execution = json.loads(execution_path.read_text())
        execution["backend"] = backend
        execution["workers"] = int(option("--workers"))
        if backend == "ray":
            execution["backend_diagnostics"] = {
                "local_owned": True,
                "session_dir": "/tmp/ray/session_fake",
                "workers": [{"pid": 1, "node_id": "local", "initialized_once": True}],
                "nodes": [{"NodeID": "local"}],
                "timings": {
                    "startup_seconds": 0.2,
                    "dataset_put_seconds": 0.01,
                    "actors_ready_seconds": 0.1,
                    "shutdown_seconds": 0.1,
                },
            }
        execution_path.write_text(json.dumps(execution))
        if alter is not None:
            alter(output, backend)
        return measurement.MeasuredRun(0, 0.6, None, (), 0, 0, {})

    monkeypatch.setattr(benchmark, "measure_command", measure)
    monkeypatch.setattr(benchmark, "_preflight", lambda: {})
    return commands


def test_warmups_rotation_same_interpreter_and_verified_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    commands = fake_trials(monkeypatch)
    report = module().run_benchmarks(
        tmp_path / "output", bars=8, jobs=3, workers=[2], repetitions=2
    )
    assert [row["configuration"] for row in report["warmups"]] == [
        "sequential",
        "process-2",
        "ray-2",
    ]
    assert [row["configuration"] for row in report["runs"]] == [
        "sequential",
        "process-2",
        "ray-2",
        "process-2",
        "ray-2",
        "sequential",
    ]
    assert report["semantic_outputs_equal"] is True
    assert len({row["semantic_sha256"] for row in report["runs"]}) == 1
    assert len(commands) == 9
    assert all(
        command[:3] == [sys.executable, "-m", "distributed_backtester.ray_benchmark"]
        for command in commands
    )
    assert report["warmups"][0]["workers"] == 1
    assert all(row["component_timings"] for row in report["runs"])
    assert all(row["returncode"] == 0 for row in report["runs"])
    assert (tmp_path / "output/report.json").exists()
    assert report["detailed_results_retained"] is True
    assert list((tmp_path / "output/warmup-sequential/results").glob("*.json"))


@pytest.mark.parametrize(
    "kind", ["artifact", "implementation", "job_count", "external"]
)
def test_verification_failure_retains_raw_observations_without_final_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    def alter(output: Path, backend: str) -> None:
        if backend != "ray":
            return
        if kind == "artifact":
            index = json.loads((output / "index.json").read_text())
            (output / index["outcomes"][0]["artifact"]).write_text("corrupt")
        elif kind == "implementation":
            manifest_path = output / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["implementation"]["source_sha256"] = "f" * 64
            manifest_path.write_text(json.dumps(manifest))
        elif kind == "job_count":
            index_path = output / "index.json"
            index = json.loads(index_path.read_text())
            index["submitted"] = 900
            index_path.write_text(json.dumps(index))
        else:
            execution_path = output / "execution.json"
            execution = json.loads(execution_path.read_text())
            execution["backend_diagnostics"]["local_owned"] = False
            execution_path.write_text(json.dumps(execution))

    fake_trials(monkeypatch, alter=alter)
    output = tmp_path / "output"
    with pytest.raises(RuntimeError):
        module().run_benchmarks(output, bars=8, jobs=3, workers=[2], repetitions=1)
    observations = json.loads((output / "observations.json").read_text())
    assert len(observations["warmups"]) == 3
    assert observations["semantic_outputs_equal"] is False
    assert not (output / "report.json").exists()


def test_summary_uses_cold_medians_and_keeps_unknown_private_memory() -> None:
    rows = [
        {
            "configuration": name,
            "workers": workers,
            "elapsed_seconds": seconds,
            "peak_private_uss_bytes": private,
        }
        for name, workers, times, private in [
            ("sequential", 1, [9.0, 3.0, 6.0], 100),
            ("ray-2", 2, [4.0, 2.0, 3.0], None),
        ]
        for seconds in times
    ]
    summary = module().summarize(rows, jobs=12)
    assert summary[0]["median_seconds"] == 6.0
    assert summary[1]["median_seconds"] == 3.0
    assert summary[1]["speedup"] == 2.0
    assert summary[1]["parallel_efficiency"] == 1.0
    assert summary[1]["median_peak_private_uss_bytes"] is None


def test_cli_accepts_requested_worker_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    benchmark = module()
    calls: list[dict[str, object]] = []

    def run(output: Path, **kwargs: Any) -> dict[str, object]:
        calls.append({"output": output, **kwargs})
        return {"summary": []}

    monkeypatch.setattr(benchmark, "run_benchmarks", run)
    assert (
        benchmark.main(
            [
                "run",
                "--bars",
                "8",
                "--jobs",
                "3",
                "--workers",
                "2",
                "4",
                "--repetitions",
                "3",
                "--output-dir",
                str(tmp_path / "output"),
            ]
        )
        == 0
    )
    assert calls[0]["workers"] == [2, 4]
    assert calls[0]["repetitions"] == 3


def test_nonzero_trial_aborts_and_retains_failure_observation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    benchmark = module()
    measurement = importlib.import_module("distributed_backtester.ray_measurement")
    monkeypatch.setattr(benchmark, "_preflight", lambda: {})
    monkeypatch.setattr(
        benchmark,
        "measure_command",
        lambda *args, **kwargs: measurement.MeasuredRun(7, 0.5, None, (), 0, 0, {}),
    )
    output = tmp_path / "output"
    with pytest.raises(RuntimeError, match="failed"):
        benchmark.run_benchmarks(output, bars=8, jobs=3, workers=[2], repetitions=1)
    observed = json.loads((output / "observations.json").read_text())
    assert observed["warmups"][0]["returncode"] == 7
    assert not (output / "report.json").exists()


def test_sequential_trial_does_not_import_ray_and_publishes_components(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from distributed_backtester.benchmark import semantic_digest
    from distributed_backtester.workloads import write_synthetic_csv

    benchmark = module()
    monkeypatch.setitem(sys.modules, "ray", None)
    source = tmp_path / "data.csv"
    write_synthetic_csv(source, bars=8, seed=1729)
    output = tmp_path / "trial"
    benchmark._trial(source, 3, "sequential", 1, output, tmp_path / "ownership.json")
    assert semantic_digest(output)
    execution = json.loads((output / "execution.json").read_text())
    assert execution["workers"] == 1
    assert execution["timings"]["csv_load_seconds"] >= 0
    assert execution["timings"]["artifact_export_seconds"] >= 0
    assert execution["timings"]["finalization_seconds"] >= 0
    assert execution["timings"]["ray_import_seconds"] == 0


def test_discard_removes_only_verified_new_detailed_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_trials(monkeypatch)
    historical = tmp_path / "historical/results/keep.json"
    historical.parent.mkdir(parents=True)
    historical.write_text("historical artifact")
    output = tmp_path / "output"
    report = module().run_benchmarks(
        output,
        bars=8,
        jobs=3,
        workers=[2],
        repetitions=1,
        discard_trial_results=True,
    )
    assert report["detailed_results_retained"] is False
    assert historical.read_text() == "historical artifact"
    for row in report["warmups"] + report["runs"]:
        trial = output / row["label"]
        index = json.loads((trial / "index.json").read_text())
        expected = [outcome["artifact"] for outcome in index["outcomes"]]
        assert row["discarded_artifacts"] == expected
        assert row["discarded_artifact_bytes"] > 0
        assert row["detailed_results_retained"] is False
        assert row["semantic_sha256"]
        assert not list((trial / "results").iterdir())
        assert (trial / "manifest.json").exists()
        assert (trial / "execution.json").exists()
    assert (output / "observations.json").exists()


def test_discard_never_removes_unverified_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def corrupt(output: Path, backend: str) -> None:
        if backend == "ray":
            index = json.loads((output / "index.json").read_text())
            (output / index["outcomes"][0]["artifact"]).write_text("corrupt")

    fake_trials(monkeypatch, alter=corrupt)
    output = tmp_path / "output"
    with pytest.raises(RuntimeError, match="digest"):
        module().run_benchmarks(
            output,
            bars=8,
            jobs=3,
            workers=[2],
            repetitions=1,
            discard_trial_results=True,
        )
    assert len(list((output / "warmup-ray-2/results").iterdir())) == 3


def test_cli_discard_is_explicit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    benchmark = module()
    calls: list[dict[str, object]] = []

    def run(output: Path, **kwargs: Any) -> dict[str, object]:
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
                "3",
                "--workers",
                "2",
                "--output-dir",
                str(tmp_path / "output"),
                "--discard-trial-results",
            ]
        )
        == 0
    )
    assert calls[0]["discard_trial_results"] is True


@pytest.mark.parametrize("evidence", ["unrelated", "group", "session"])
def test_actor_publication_requires_independent_ownership_before_pid_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    evidence: str,
) -> None:
    benchmark = module()
    measurement = importlib.import_module("distributed_backtester.ray_measurement")
    driver_pid = os.getpid()
    driver = FakeProcess(driver_pid)
    actor_or_replacement = FakeProcess(
        4,
        created=102.0,
        args=("/tmp/ray/session_owned/logs",) if evidence == "session" else (),
    )
    psutil = boundary([driver, actor_or_replacement])
    monkeypatch.setattr(benchmark, "_require_psutil", lambda: psutil)
    monkeypatch.setattr(
        measurement.os,
        "getpgid",
        lambda pid: driver_pid if pid == driver_pid or evidence == "group" else 99,
    )
    backend = SimpleNamespace(
        name="ray",
        workers=1,
        diagnostics={
            "local_owned": True,
            "session_dir": "/tmp/ray/session_owned",
            "workers": [{"pid": 4, "node_id": "owned-node"}],
        },
    )
    ownership = tmp_path / "ownership.json"
    observed = benchmark._ObservedBackend(backend, ownership)
    observed.publish()
    record = json.loads(ownership.read_text())
    sampler = measurement._Inventory(
        psutil, driver_pid=driver_pid, ownership=ownership, ray=True
    )
    sample = sampler.sample()
    sampler.cleanup()
    if evidence == "unrelated":
        assert not record["processes"]
        assert record["publication_errors"]
        assert all(row["pid"] != 4 for row in sample["inventory"])
        assert not actor_or_replacement.killed
    else:
        assert record["processes"][0]["pid"] == 4
        assert actor_or_replacement.killed


def test_actor_publication_rejects_lost_original_driver_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = module()
    driver_pid = os.getpid()
    driver, actor = FakeProcess(driver_pid), FakeProcess(4)
    monkeypatch.setattr(benchmark, "_require_psutil", lambda: boundary([driver, actor]))
    monkeypatch.setattr(benchmark.os, "getpgid", lambda pid: driver_pid)
    backend = SimpleNamespace(
        name="ray",
        workers=1,
        diagnostics={
            "local_owned": True,
            "session_dir": "/tmp/ray/session_owned",
            "workers": [{"pid": 4}],
        },
    )
    ownership = tmp_path / "ownership.json"
    observed = benchmark._ObservedBackend(backend, ownership)
    driver.created = 101.0
    observed.publish()
    record = json.loads(ownership.read_text())
    assert not record["processes"]
    assert record["publication_errors"]


@pytest.mark.parametrize("replacement", ["data", "configuration", "count"])
def test_first_trial_must_match_parent_snapshot_and_declared_jobs_before_discard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement: str,
) -> None:
    from collections.abc import Iterator
    from dataclasses import replace

    from distributed_backtester.models import BacktestConfig
    from distributed_backtester.workloads import job_configs, write_synthetic_csv

    benchmark = module()
    fake_trials(monkeypatch)
    original = benchmark._trial
    replaced = False

    def trial(data: Path, jobs: int, *args: Any, **kwargs: Any) -> None:
        nonlocal replaced
        if not replaced:
            if replacement == "data":
                source = tmp_path / "replacement.csv"
                write_synthetic_csv(source, bars=8, seed=1730)
                data.write_bytes(source.read_bytes())
            elif replacement == "configuration":

                def changed_configs(count: int) -> Iterator[BacktestConfig]:
                    return iter(
                        replace(config, initial_cash=config.initial_cash + 1)
                        for config in job_configs(count)
                    )

                monkeypatch.setattr(benchmark, "job_configs", changed_configs)
            else:
                jobs += 1
            replaced = True
        original(data, jobs, *args, **kwargs)

    monkeypatch.setattr(benchmark, "_trial", trial)
    output = tmp_path / "output"
    with pytest.raises(RuntimeError):
        benchmark.run_benchmarks(
            output,
            bars=8,
            jobs=3,
            workers=[2],
            repetitions=1,
            discard_trial_results=True,
        )
    observations = json.loads((output / "observations.json").read_text())
    assert len(observations["warmups"]) == 1
    row = observations["warmups"][0]
    assert row["verification_error"]
    assert row["detailed_results_retained"] is True
    assert not row["discarded_artifacts"]
    assert list((output / "warmup-sequential/results").iterdir())
    assert not (output / "report.json").exists()


def test_unexpected_manifest_null_field_is_rejected_before_reference_and_discard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def alter(output: Path, backend: str) -> None:
        path = output / "manifest.json"
        manifest = json.loads(path.read_text())
        manifest["unexpected_field"] = None
        path.write_text(json.dumps(manifest))

    fake_trials(monkeypatch, alter=alter)
    output = tmp_path / "output"
    with pytest.raises(RuntimeError, match="captured workload"):
        module().run_benchmarks(
            output,
            bars=8,
            jobs=3,
            workers=[2],
            repetitions=1,
            discard_trial_results=True,
        )
    row = json.loads((output / "observations.json").read_text())["warmups"][0]
    assert row["input_validation"]["changed_manifest_fields"] == ["unexpected_field"]
    assert not row["discarded_artifacts"]

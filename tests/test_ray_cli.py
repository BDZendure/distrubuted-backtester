"""CLI boundary tests for optional Ray execution and resource flags."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from distributed_backtester.backends.ray import RayBackend
from distributed_backtester.benchmark import semantic_digest
from distributed_backtester.cli import main, select_backend
from tests.test_sweep_cli import arguments


@pytest.mark.parametrize(
    "extra",
    [
        ["--ray-address", "auto"],
        ["--cpus-per-worker", "2"],
        ["--worker-resource", "custom=1"],
        ["--object-store-memory", "134217728"],
        ["--backend", "ray", "--worker-resource", "custom"],
        [
            "--backend",
            "ray",
            "--worker-resource",
            "custom=1",
            "--worker-resource",
            "custom=2",
        ],
        ["--backend", "ray", "--worker-resource", "CPU=1"],
        ["--backend", "ray", "--worker-resource", "custom=0.00001"],
        ["--backend", "ray", "--cpus-per-worker", "0.00001"],
    ],
)
def test_invalid_ray_flags_create_no_output(tmp_path: Path, extra: list[str]) -> None:
    output = tmp_path / "invalid"
    assert main([*arguments(output), *extra]) == 2
    assert not output.exists()


def test_ray_selection_remains_lazy() -> None:
    backend = select_backend(
        "ray", 2, 3, cpus_per_worker=0.5, worker_resources=["custom=1"]
    )
    assert backend.name == "ray"
    assert backend.workers == 2
    assert isinstance(backend, RayBackend)
    assert backend.resources == {"custom": 1.0}


def test_real_cli_three_backends_have_identical_semantic_artifacts(
    tmp_path: Path,
) -> None:
    pytest.importorskip("ray")
    digests = []
    for backend in ["sequential", "process", "ray"]:
        output = tmp_path / backend
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "distributed_backtester",
                *arguments(output),
                "--backend",
                backend,
                "--workers",
                "1" if backend == "sequential" else "2",
            ],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=90,
        )
        assert completed.returncode == 0, completed.stderr
        summary = json.loads(completed.stdout)
        assert summary["submitted"] == summary["succeeded"] == 4
        digests.append(semantic_digest(output))
        if backend == "ray":
            execution = json.loads((output / "execution.json").read_text())
            assert execution["backend_diagnostics"]["ray_version"] == "2.59.0"
            assert len(execution["backend_diagnostics"]["workers"]) == 2
    assert len(set(digests)) == 1

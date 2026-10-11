"""Real spawned processes and Ray verify operational IO and durable shutdown."""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, cast

import pytest

from distributed_backtester.benchmark import semantic_digest
from distributed_backtester.operational_status import read_status
from distributed_backtester.workloads import write_synthetic_csv

SOURCE = Path(__file__).resolve().parents[1] / "examples/sma.csv"


@pytest.mark.parametrize(
    "backend,workers", [("sequential", 1), ("process", 2), ("ray", 2)]
)
def test_real_backend_logs_and_outputs_are_equivalent(
    tmp_path: Path, backend: str, workers: int
) -> None:
    if backend == "ray":
        pytest.importorskip("ray")
    roots = []
    observed: list[dict[str, object]] = []
    for level in ["OFF", "INFO"]:
        output = tmp_path / level
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "distributed_backtester",
                "sweep",
                "--data",
                str(SOURCE),
                "--symbol",
                "SYNTH",
                "--price-basis",
                "raw",
                "--fast",
                "1",
                "2",
                "--slow",
                "3",
                "4",
                "--fee-bps",
                "10",
                "--backend",
                backend,
                "--workers",
                str(workers),
                "--output-dir",
                str(output),
                "--log-level",
                level,
            ],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert completed.returncode == 0, completed.stderr
        summary = json.loads(completed.stdout)
        assert summary["succeeded"] == summary["submitted"] == 4
        roots.append(output)
        if level == "INFO":
            observed = [json.loads(line) for line in completed.stderr.splitlines()]
            assert any(record["event"] == "experiment_finished" for record in observed)
    assert semantic_digest(roots[0]) == semantic_digest(roots[1])
    status = cast(dict[str, Any], read_status(roots[1]))
    expected_jobs = {job["job_id"] for job in status["jobs"]}
    if backend == "ray":
        assert sum(record["event"] == "worker_ready" for record in observed) == workers
        execution = json.loads((roots[1] / "execution.json").read_text())
        session = Path(execution["backend_diagnostics"]["session_dir"])
        observed = []
        for log in (session / "logs").glob("worker-*.err"):
            for line in log.read_text(errors="replace").splitlines():
                if line.startswith('{"'):
                    record = json.loads(line)
                    if record.get("experiment_id") == status["experiment_id"]:
                        observed.append(record)
    job_records = [record for record in observed if record["event"] == "job_started"]
    assert {record["job_id"] for record in job_records} == expected_jobs
    assert all(
        record["experiment_id"] == status["experiment_id"] for record in job_records
    )
    if backend != "sequential":
        assert all(record["pid"] != os.getpid() for record in job_records)


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_actual_signal_interrupts_durable_driver_and_same_source_resume(
    tmp_path: Path, signum: signal.Signals
) -> None:
    pytest.importorskip("ray")
    data = tmp_path / "data.csv"
    write_synthetic_csv(data, bars=262144, seed=1729)
    output, stderr = tmp_path / "durable", tmp_path / "driver.log"
    command = [
        sys.executable,
        "-m",
        "distributed_backtester",
        "sweep",
        "--data",
        str(data),
        "--symbol",
        "BENCH",
        "--price-basis",
        "raw",
        "--fast",
        "2",
        "--slow",
        "3",
        "--fee-bps",
        "10",
        "--backend",
        "ray",
        "--workers",
        "1",
        "--durable",
        "--output-dir",
        str(output),
        "--log-level",
        "INFO",
    ]
    with stderr.open("w") as sink:
        process = subprocess.Popen(
            command, cwd=tmp_path, stdout=subprocess.PIPE, stderr=sink, text=True
        )
        try:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if '"event": "attempt_claimed"' in stderr.read_text():
                    break
                assert process.poll() is None, stderr.read_text()
                time.sleep(0.002)
            else:
                pytest.fail("durable driver did not claim a persisted attempt")
            process.send_signal(signum)
            stdout, _ = process.communicate(timeout=45)
            assert process.returncode == 128 + signum, stderr.read_text()
            assert not stdout.strip()
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=15)
    before = cast(dict[str, Any], read_status(output))
    assert before["counts"]["running"] == 1
    assert not before["publication_complete"]
    resumed = subprocess.run(
        [
            sys.executable,
            "-m",
            "distributed_backtester",
            "resume",
            "--data",
            str(data),
            "--output-dir",
            str(output),
            "--log-level",
            "INFO",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert resumed.returncode == 0, resumed.stderr
    assert json.loads(resumed.stdout)["succeeded"] == 1
    after = cast(dict[str, Any], read_status(output))
    assert after["generation"] == before["generation"] + 1
    assert after["counts"]["succeeded"] == 1
    assert after["publication_complete"]
    assert [attempt["status"] for attempt in after["attempts"]] == [
        "interrupted",
        "succeeded",
    ]

#!/usr/bin/env python3
"""Bounded, installed-wheel operational smoke. Retain all outputs for inspection."""

import argparse
import json
import math
import os
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager, suppress
from datetime import date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from types import FrameType
from typing import cast


class SmokeFailure(RuntimeError):
    """An actual command or its operational/artifact contract failed."""


class SmokeInterrupted(KeyboardInterrupt):
    """Wrapper termination that must unwind owned-child cleanup."""

    def __init__(self, signum: int) -> None:
        self.signum = signum
        super().__init__(f"wrapper received {signal.Signals(signum).name}")


@contextmanager
def wrapper_signals(*, defer: bool = False) -> Iterator[Callable[[], None]]:
    """Bridge TERM/INT within this operation and restore the caller's handlers."""
    interrupted: int | None = None

    def activate() -> None:
        nonlocal defer
        defer = False
        if interrupted is not None:
            raise SmokeInterrupted(interrupted)

    if threading.current_thread() is not threading.main_thread():
        yield activate
        return
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}

    def terminate(signum: int, frame: FrameType | None) -> None:
        nonlocal interrupted
        if interrupted is None:
            interrupted = signum
            if not defer:
                raise SmokeInterrupted(signum)

    try:
        for sig in previous:
            signal.signal(sig, terminate)
        yield activate
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SmokeFailure(message)


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _constant(value: str) -> object:
    raise SmokeFailure(f"nonfinite JSON: {value}")


def json_object(content: str) -> dict[str, object]:
    try:
        result = json.loads(content, object_pairs_hook=_pairs, parse_constant=_constant)
        # Reject numeric overflow such as 1e999 as well as literal NaN/Infinity.
        json.dumps(result, allow_nan=False)
    except (ValueError, TypeError) as error:
        raise SmokeFailure(f"expected one strict JSON object: {error}") from error
    require(isinstance(result, dict), "expected one strict JSON object")
    return cast(dict[str, object], result)


def json_logs(content: str) -> list[dict[str, object]]:
    records = [json_object(line) for line in content.splitlines() if line.strip()]
    require(bool(records), "expected structured stderr logs")
    for record in records:
        require(
            type(record.get("schema_version")) is int
            and record["schema_version"] == 1
            and type(record.get("pid")) is int
            and cast(int, record["pid"]) > 0
            and record.get("level") in {"ERROR", "WARNING", "INFO", "DEBUG"}
            and isinstance(record.get("event"), str)
            and bool(record["event"]),
            "invalid structured stderr log schema",
        )
        try:
            stamp = datetime.fromisoformat(str(record["timestamp"]))
            require(stamp.utcoffset() == timedelta(0), "log timestamp must be UTC")
        except (KeyError, ValueError) as error:
            raise SmokeFailure("invalid log timestamp") from error
    return records


def find_event(
    records: Sequence[dict[str, object]], event: str, *, pid: int
) -> dict[str, object] | None:
    return next(
        (r for r in records if r.get("event") == event and r.get("pid") == pid), None
    )


def verify_reference(result: dict[str, object]) -> None:
    metrics = result.get("metrics")
    config = result.get("config")
    fills = result.get("fills")
    require(
        isinstance(metrics, dict)
        and metrics.get("final_equity") == "749.257000"
        and metrics.get("total_fees") == "1.743000"
        and metrics.get("fill_count") == 2
        and isinstance(config, dict)
        and config.get("fee_bps") == 10
        and isinstance(fills, list)
        and len(fills) == 2
        and [f.get("fee") for f in fills if isinstance(f, dict)]
        == ["0.996000", "0.747000"],
        "reference equity, fees or detailed fills differ",
    )


def experiment_bytes(output: Path) -> dict[str, bytes]:
    index = json_object((output / "index.json").read_text())
    outcomes = index.get("outcomes")
    require(
        isinstance(outcomes, list)
        and bool(outcomes)
        and type(index.get("submitted")) is int
        and len(outcomes) == index["submitted"] == index.get("succeeded")
        and index.get("failed") == 0,
        "experiment must have a complete successful index",
    )
    content = {
        name: (output / name).read_bytes() for name in ("manifest.json", "index.json")
    }
    json_object((output / "execution.json").read_text())
    seen: set[str] = set()
    for outcome in cast(list[object], outcomes):
        require(isinstance(outcome, dict), "invalid outcome record")
        record = cast(dict[str, object], outcome)
        key = record.get("job_id")
        require(
            isinstance(key, str) and re.fullmatch("[0-9a-f]{64}", key) is not None,
            "invalid artifact job ID",
        )
        key = cast(str, key)
        relative = f"results/{key}.json"
        require(
            key not in seen
            and record.get("artifact") == relative
            and record.get("status") == "succeeded",
            "duplicate, unsafe or failed artifact",
        )
        seen.add(key)
        data = (output / relative).read_bytes()
        require(
            sha256(data).hexdigest() == record.get("content_sha256"),
            f"artifact digest mismatch: {relative}",
        )
        json_object(data.decode())
        content[relative] = data
    require(
        {p.name for p in (output / "results").iterdir()}
        == {f"{key}.json" for key in seen},
        "unexpected or missing result artifacts",
    )
    return content


def compare_experiments(first: Path, second: Path) -> str:
    expected = experiment_bytes(first)
    require(expected == experiment_bytes(second), "complete experiment outputs differ")
    return sha256(
        len(expected["manifest.json"]).to_bytes(8, "big")
        + expected["manifest.json"]
        + expected["index.json"]
    ).hexdigest()


def verify_durable(status: dict[str, object], *, jobs: int) -> None:
    rows, attempts = status.get("jobs"), status.get("attempts")
    require(
        status.get("publication_complete") is True
        and status.get("counts")
        == {"pending": 0, "running": 0, "succeeded": jobs, "failed": 0}
        and isinstance(rows, list)
        and len(rows) == jobs
        and isinstance(attempts, list),
        "durable jobs or publication are incomplete",
    )
    keys = [str(r.get("job_id")) for r in cast(list[dict[str, object]], rows)]
    committed = [
        str(a.get("job_id"))
        for a in cast(list[dict[str, object]], attempts)
        if a.get("status") == "succeeded"
    ]
    require(
        len(set(keys)) == jobs and sorted(keys) == sorted(committed),
        "durable jobs have missing or duplicate successful attempts",
    )


def fresh_workspace(parent: Path) -> Path:
    parent = parent.resolve()
    parent.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix="smoke-", dir=parent))


def stop_process(process: subprocess.Popen[str]) -> None:
    """Bound cleanup to this child's fresh process group, including its workers."""
    if os.name == "posix":
        for sig in (signal.SIGTERM, signal.SIGKILL):
            with suppress(ProcessLookupError):
                os.killpg(process.pid, sig)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                continue
            # The parent can exit while grandchildren remain in the group.
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            return
    else:
        process.kill()
    process.wait(timeout=5)


class Runner:
    def __init__(self, workspace: Path, *, timeout: float) -> None:
        self.workspace = workspace
        self.timeout = timeout

    def execute(
        self,
        label: str,
        command: list[str],
        *,
        expected: int = 0,
        interrupt: tuple[str, signal.Signals] | None = None,
    ) -> tuple[str, str]:
        record: dict[str, object] = {
            "argv": command,
            "cwd": str(self.workspace),
            "timeout": self.timeout,
            "expected_exit": expected,
        }
        evidence = self.workspace / f"{label}.command.json"
        evidence.write_text(json.dumps(record, indent=2) + "\n")
        stdout_path = self.workspace / f"{label}.stdout"
        stderr_path = self.workspace / f"{label}.stderr"
        started = time.monotonic()
        process: subprocess.Popen[str] | None = None
        with (
            wrapper_signals(defer=True) as activate_signals,
            stdout_path.open("w") as stdout,
            stderr_path.open("w") as stderr,
        ):
            try:
                process = subprocess.Popen(
                    command,
                    cwd=self.workspace,
                    stdout=stdout,
                    stderr=stderr,
                    text=True,
                    start_new_session=True,
                )
                record["pid"] = process.pid
                # A launch-time signal must wait until we can reap the owned child.
                activate_signals()
                if interrupt is not None:
                    event, sig = interrupt
                    deadline = started + self.timeout
                    while time.monotonic() < deadline:
                        # Only complete JSON lines are candidates while the file grows.
                        lines = stderr_path.read_text().splitlines(keepends=True)
                        records = [json_object(s) for s in lines if s.endswith("\n")]
                        observed = find_event(records, event, pid=process.pid)
                        if observed is not None:
                            record["signal_event"] = observed
                            record["signal"] = sig.name
                            process.send_signal(sig)
                            break
                        require(
                            process.poll() is None,
                            f"{label}: child exited before {event}; see {stderr_path}",
                        )
                        time.sleep(0.01)
                    else:
                        raise subprocess.TimeoutExpired(command, self.timeout)
                process.wait(
                    timeout=max(0.001, self.timeout - (time.monotonic() - started))
                )
            except subprocess.TimeoutExpired as error:
                record["deadline_exceeded"] = True
                if process is not None:
                    stop_process(process)
                raise SmokeFailure(
                    f"{label}: subprocess deadline; see {evidence}"
                ) from error
            except BaseException as error:
                if isinstance(error, KeyboardInterrupt):
                    sig = signal.Signals(getattr(error, "signum", signal.SIGINT))
                    record["wrapper_signal"] = sig.name
                if process is not None:
                    stop_process(process)
                raise
            finally:
                record["returncode"] = (
                    process.returncode if process is not None else None
                )
                record["elapsed_seconds"] = time.monotonic() - started
                evidence.write_text(json.dumps(record, indent=2) + "\n")
        assert process is not None
        require(
            process.returncode == expected,
            f"{label}: exit {process.returncode}, expected {expected}; "
            f"see {stderr_path}",
        )
        return stdout_path.read_text(), stderr_path.read_text()

    def cli(
        self, label: str, arguments: list[str], *, logs: bool = True
    ) -> dict[str, object]:
        stdout, stderr = self.execute(
            label, [sys.executable, "-I", "-m", "distributed_backtester", *arguments]
        )
        if logs:
            json_logs(stderr)
        else:
            require(not stderr.strip(), f"{label}: unexpected stderr")
        return json_object(stdout)

    def python(self, label: str, code: str, *arguments: str) -> dict[str, object]:
        stdout, stderr = self.execute(
            label, [sys.executable, "-I", "-c", code, *arguments]
        )
        require(not stderr.strip(), f"{label}: unexpected stderr")
        return json_object(stdout)


_INSTALL_PROBE = """
import base64, hashlib, importlib.metadata as m, json, pathlib, sysconfig
import distributed_backtester as p
d = m.distribution('distributed-backtester')
direct = json.loads(d.read_text('direct_url.json') or '{}')
assert not direct.get('dir_info', {}).get('editable'), 'install a wheel'
sites = {pathlib.Path(sysconfig.get_path(key)).resolve()
         for key in ('purelib', 'platlib')}
base = pathlib.Path(d.locate_file('')).resolve()
assert base in sites, 'distribution is outside interpreter installation paths'
wheel = d.read_text('WHEEL')
assert wheel and 'Wheel-Version:' in wheel, 'installed wheel WHEEL metadata required'
assert d.read_text('RECORD'), 'installed wheel RECORD metadata required'
files = list(d.files or ())
wheel_rows = [f for f in files if len(f.parts) == 2
              and f.parts[0].endswith('.dist-info') and f.name == 'WHEEL']
assert len(wheel_rows) == 1, 'WHEEL must be recorded in installed dist-info'
metadata = wheel_rows[0].parent
record_rows = [f for f in files if f == metadata / 'RECORD']
assert len(record_rows) == 1, 'RECORD must identify the installed wheel'
for f in (wheel_rows[0], record_rows[0]):
    path = pathlib.Path(d.locate_file(f)).resolve()
    assert path.is_relative_to(base) and path.is_file(), (
        'invalid installed metadata path')
    assert path.read_text() == d.read_text(f.name), 'distribution metadata differs'
package_rows = [f for f in files if f.parts[0] == 'distributed_backtester'
                and f.suffix == '.py']
assert package_rows, 'installed wheel package files must be recorded'
recorded = {}
for f in package_rows:
    path = pathlib.Path(d.locate_file(f)).resolve()
    assert path.is_relative_to(base), 'recorded package file escapes installation'
    assert f.hash and f.hash.mode in ('sha256', 'sha384', 'sha512'), (
        'package hash required')
    content = path.read_bytes()
    digest = base64.urlsafe_b64encode(
        hashlib.new(f.hash.mode, content).digest()).rstrip(b'=')
    assert digest.decode() == f.hash.value and len(content) == f.size, (
        'package RECORD mismatch')
    assert path not in recorded, 'duplicate package RECORD entry'
    recorded[path] = f
root = base / 'distributed_backtester'
imported = pathlib.Path(p.__file__).resolve()
assert imported == (root / '__init__.py').resolve() and imported in recorded, (
    'import bypasses wheel')
assert p.__spec__ and pathlib.Path(p.__spec__.origin).resolve() == imported, (
    'import origin differs')
assert {pathlib.Path(path).resolve() for path in p.__path__} == {root.resolve()}, (
    'package search path differs')
print(json.dumps({'version': d.version, 'package_file': p.__file__,
                  'record_file': str(d.locate_file(record_rows[0])),
                  'recorded_python_files': len(package_rows),
                  'uid': __import__('os').getuid()}))
"""

_SNAPSHOT_PROBE = """
import json, sys
from pathlib import Path
from distributed_backtester.experiment_snapshot import read_experiment_snapshot
with read_experiment_snapshot(Path(sys.argv[1])) as s:
    count = sum(1 for _ in s.iter_artifacts())
    assert count == s.submitted == s.succeeded and s.failed == 0
    print(json.dumps({'experiment_id': s.experiment_id, 'validated': count}))
"""


def _sweep(data: Path, output: Path, backend: str) -> list[str]:
    args = [
        "sweep",
        "--data",
        str(data),
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
        "--output-dir",
        str(output),
        "--log-level",
        "INFO",
    ]
    if backend != "sequential":
        args += ["--workers", "2", "--max-in-flight", "4"]
    if backend == "ray":
        args += ["--cpus-per-worker", "1", "--object-store-memory", "134217728"]
    return args


def _tree(output: Path) -> dict[str, str]:
    # Read-only SQLite can maintain SHM and create/remove an empty WAL. Retain
    # nonempty WAL hashes: committed frames can be authoritative before a
    # checkpoint. Every database, lock, export and unrelated file still counts.
    journal = (output / "journal.sqlite3").is_file()
    return {
        p.relative_to(output).as_posix(): sha256(p.read_bytes()).hexdigest()
        for p in output.rglob("*")
        if p.is_file()
        and not (
            journal
            and p.parent == output
            and (
                p.name == "journal.sqlite3-shm"
                or (p.name == "journal.sqlite3-wal" and p.stat().st_size == 0)
            )
        )
    }


def _status(runner: Runner, output: Path, label: str) -> dict[str, object]:
    before = _tree(output)
    status = runner.cli(
        label, ["status", "--output-dir", str(output), "--log-level", "INFO"]
    )
    require(before == _tree(output), "status mutated experiment files")
    rows = status.get("jobs")
    require(isinstance(rows, list) and bool(rows), "status missing jobs")
    job_id = cast(list[dict[str, object]], rows)[0]["job_id"]
    filtered = runner.cli(
        f"{label}-job",
        [
            "status",
            "--output-dir",
            str(output),
            "--job-id",
            str(job_id),
            "--log-level",
            "INFO",
        ],
    )
    require(
        filtered.get("jobs") == [cast(list[object], rows)[0]]
        and filtered.get("counts") == status.get("counts"),
        "job status changed aggregate counts or selected the wrong job",
    )
    require(before == _tree(output), "filtered status mutated experiment files")
    return status


def _long_data(path: Path) -> None:
    # A real 40k-bar job stays active beyond the polling interval on tiny CI grids.
    with path.open("w") as stream:
        stream.write("date,open,high,low,close,volume\n")
        for offset in range(40000):
            day = date(1900, 1, 1) + timedelta(days=offset)
            stream.write(f"{day.isoformat()},10,10,10,10,1000\n")


def _signals(runner: Runner, *, ray: bool) -> None:
    require(os.name == "posix", "signal smoke requires POSIX")
    data = runner.workspace / "signal-data.csv"
    _long_data(data)
    for sig, expected in ((signal.SIGTERM, 143), (signal.SIGINT, 130)):
        label = f"signal-{sig.name.lower()}"
        output = runner.workspace / label
        arguments = _sweep(data, output, "sequential")
        stdout, stderr = runner.execute(
            label,
            [sys.executable, "-I", "-m", "distributed_backtester", *arguments],
            expected=expected,
            interrupt=("job_started", sig),
        )
        require(not stdout.strip(), "interrupted sweep published stdout success")
        records = json_logs(stderr)
        require(
            sum(r["event"] == "command_interrupted" for r in records) == 1,
            "expected one interruption event",
        )
        require(
            not (output / "index.json").exists(), "interrupted classic sweep completed"
        )
    if ray:
        output = runner.workspace / "durable-interrupted"
        args = [*_sweep(data, output, "ray"), "--durable", "--job-timeout", "90"]
        stdout, stderr = runner.execute(
            "durable-sigterm",
            [sys.executable, "-I", "-m", "distributed_backtester", *args],
            expected=143,
            interrupt=("attempt_claimed", signal.SIGTERM),
        )
        require(
            not stdout.strip(), "interrupted durable sweep published stdout success"
        )
        records = json_logs(stderr)
        require(
            any(r["event"] == "command_interrupted" for r in records),
            "durable SIGTERM missing interruption event",
        )
        stopped = _status(runner, output, "durable-stopped-status")
        require(
            not stopped.get("publication_complete"), "durable workload raced the signal"
        )
        generation = stopped["generation"]
        runner.cli(
            "durable-recovered",
            [
                "resume",
                "--data",
                str(data),
                "--output-dir",
                str(output),
                "--workers",
                "2",
                "--max-in-flight",
                "4",
                "--object-store-memory",
                "134217728",
                "--log-level",
                "INFO",
            ],
        )
        final = _status(runner, output, "durable-recovered-status")
        verify_durable(final, jobs=4)
        require(
            cast(int, final["generation"]) > cast(int, generation),
            "resume did not fence generation",
        )
        runner.python("durable-recovered-artifacts", _SNAPSHOT_PROBE, str(output))
        experiment_bytes(output)


def run_smoke(
    data: Path,
    workspace: Path,
    *,
    timeout: float,
    ray: bool,
    storage: bool,
    signals: bool,
) -> dict[str, object]:
    runner = Runner(workspace, timeout=timeout)
    installed = runner.python("installed-package", _INSTALL_PROBE)
    args = [
        "backtest",
        "--data",
        str(data),
        "--symbol",
        "SYNTH",
        "--price-basis",
        "raw",
        "--fee-bps",
        "10",
    ]
    reference = runner.cli("reference", args, logs=False)
    verify_reference(reference)
    logged = runner.cli("reference-logged", [*args, "--log-level", "INFO"])
    require(reference == logged, "logging changed reference backtest")
    configuration = workspace / "configuration"
    configuration.mkdir()
    config = configuration / "backtest.toml"
    config.write_text(
        'schema_version = 1\ncommand = "backtest"\ndata = "input.csv"\n'
        'symbol = "SYNTH"\nprice_basis = "raw"\nfee_bps = 0\n'
    )
    (configuration / "input.csv").write_bytes(data.read_bytes())
    configured = runner.cli(
        "config",
        [
            "backtest",
            "--config",
            str(config),
            "--fee-bps",
            "10",
            "--log-level",
            "INFO",
        ],
    )
    require(configured == reference, "config path resolution or CLI override differs")
    bad = configuration / "invalid.toml"
    bad.write_text(config.read_text() + "unknown_option = true\n")
    bad_output = workspace / "invalid-output.json"
    stdout, stderr = runner.execute(
        "invalid-config",
        [
            sys.executable,
            "-I",
            "-m",
            "distributed_backtester",
            "backtest",
            "--config",
            str(bad),
            "--output",
            str(bad_output),
            "--log-level",
            "ERROR",
        ],
        expected=2,
    )
    require(
        not stdout.strip() and not bad_output.exists(),
        "invalid config created success output",
    )
    json_logs(stderr)
    for backend in ("sequential", "process", *(("ray",) if ray else ())):
        output = workspace / backend
        arguments = _sweep(data, output, backend)
        if backend == "sequential":
            sweep_config = configuration / "sweep.toml"
            sweep_config.write_text(
                'schema_version = 1\ncommand = "sweep"\ndata = "input.csv"\n'
                'symbol = "SYNTH"\nprice_basis = "raw"\nfast = [1, 2]\n'
                'slow = [3, 4]\nfee_bps = 10\nbackend = "sequential"\n'
            )
            arguments = [
                "sweep",
                "--config",
                str(sweep_config),
                "--output-dir",
                str(output),
                "--log-level",
                "INFO",
            ]
        summary = runner.cli(backend, arguments)
        require(
            summary.get("submitted") == summary.get("succeeded") == 4
            and summary.get("failed") == 0,
            "tiny sweep did not succeed completely",
        )
        runner.python(f"{backend}-artifacts", _SNAPSHOT_PROBE, str(output))
        status = _status(runner, output, f"{backend}-status")
        require(
            status.get("counts")
            == {"pending": 0, "running": 0, "succeeded": 4, "failed": 0},
            "classic status counts differ",
        )
    digest = compare_experiments(workspace / "sequential", workspace / "process")
    process_logs = json_logs((workspace / "process.stderr").read_text())
    driver = json_object((workspace / "process.command.json").read_text())["pid"]
    require(
        any(r["event"] == "job_started" and r["pid"] != driver for r in process_logs),
        "process smoke did not observe spawned worker logs",
    )
    if ray:
        compare_experiments(workspace / "sequential", workspace / "ray")
        output = workspace / "durable"
        runner.cli("durable", [*_sweep(data, output, "ray"), "--durable"])
        verify_durable(_status(runner, output, "durable-status"), jobs=4)
        runner.python("durable-artifacts", _SNAPSHOT_PROBE, str(output))
        compare_experiments(workspace / "sequential", output)
        logs = json_logs((workspace / "durable.stderr").read_text())
        commits = [
            r
            for r in logs
            if r["event"] == "result_committed" and r.get("disposition") == "committed"
        ]
        require(
            len(commits) == 4 and len({r.get("job_id") for r in commits}) == 4,
            "durable logs missing committed job correlations",
        )
        require(
            all(
                all(
                    k in r
                    for k in ("experiment_id", "job_id", "attempt_id", "generation")
                )
                for r in commits
            ),
            "durable commit log missing attempt identity",
        )
        resume_config = configuration / "resume.toml"
        resume_config.write_text(
            'schema_version = 1\ncommand = "resume"\ndata = "input.csv"\n'
            'output_dir = "../durable"\n'
        )
        runner.cli(
            "durable-resume",
            ["resume", "--config", str(resume_config), "--log-level", "INFO"],
        )
        verify_durable(_status(runner, output, "durable-resume-status"), jobs=4)
        compare_experiments(workspace / "sequential", output)
    if storage:
        parquet = workspace / "sample.parquet"
        runner.cli(
            "convert",
            [
                "convert-data",
                "--data",
                str(data),
                "--symbol",
                "SYNTH",
                "--price-basis",
                "raw",
                "--output",
                str(parquet),
                "--log-level",
                "INFO",
            ],
        )
        stored = runner.cli(
            "parquet-backtest",
            [
                "backtest",
                "--data",
                str(parquet),
                "--symbol",
                "SYNTH",
                "--price-basis",
                "raw",
                "--fee-bps",
                "10",
                "--log-level",
                "INFO",
            ],
        )
        verify_reference(stored)
        require(
            {k: v for k, v in stored.items() if k != "dataset"}
            == {k: v for k, v in reference.items() if k != "dataset"},
            "Parquet economic results differ from CSV",
        )
        database = workspace / "results.duckdb"
        imported = runner.cli(
            "results-import",
            [
                "results-import",
                "--experiment-dir",
                str(workspace / "sequential"),
                "--database",
                str(database),
                "--log-level",
                "INFO",
            ],
        )
        queried = runner.cli(
            "results-query",
            [
                "results-query",
                "--database",
                str(database),
                "--snapshot-id",
                str(imported["snapshot_id"]),
                "--log-level",
                "INFO",
            ],
        )
        require(
            isinstance(queried.get("outcomes"), list)
            and len(cast(list[object], queried["outcomes"])) == 4,
            "storage query lost outcomes",
        )
    if signals:
        _signals(runner, ray=ray)
    return {
        "schema_version": 1,
        "ok": True,
        "workspace": str(workspace),
        "installed": installed,
        "semantic_digest": digest,
        "ray": ray,
        "storage": storage,
        "signals": signals,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data", type=Path, required=True, help="bundled reference sma.csv"
    )
    parser.add_argument(
        "--output-parent", type=Path, default=Path(tempfile.gettempdir())
    )
    parser.add_argument(
        "--timeout", type=float, default=120, help="seconds per subprocess"
    )
    parser.add_argument(
        "--ray", action="store_true", help="run real Ray and durable sweeps"
    )
    parser.add_argument(
        "--storage", action="store_true", help="run real Parquet and DuckDB commands"
    )
    parser.add_argument(
        "--signals",
        action="store_true",
        help="interrupt long jobs, recover durable when --ray",
    )
    options = parser.parse_args(argv)
    workspace: Path | None = None
    report: dict[str, object] = {"schema_version": 1, "ok": False, "workspace": None}
    exit_code = 1
    with wrapper_signals():
        try:
            require(
                math.isfinite(options.timeout) and options.timeout > 0,
                "timeout must be finite and positive",
            )
            data = options.data.resolve(strict=True)
            workspace = fresh_workspace(options.output_parent)
            report = run_smoke(
                data,
                workspace,
                timeout=options.timeout,
                ray=options.ray,
                storage=options.storage,
                signals=options.signals,
            )
            exit_code = 0 if report["ok"] else 1
        except KeyboardInterrupt as error:
            sig = signal.Signals(getattr(error, "signum", signal.SIGINT))
            exit_code = 128 + sig
            report.update(
                ok=False,
                workspace=str(workspace),
                error=f"wrapper received {sig.name}",
                signal=sig.name,
                exit_code=exit_code,
            )
        except (SmokeFailure, OSError, ValueError, KeyError) as error:
            report.update(ok=False, workspace=str(workspace), error=str(error))
        finally:
            if workspace is not None:
                (workspace / "report.json").write_text(
                    json.dumps(report, indent=2, allow_nan=False) + "\n"
                )
        print(json.dumps(report, sort_keys=True, allow_nan=False))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

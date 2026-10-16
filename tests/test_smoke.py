"""Smoke harness checks without starting the installed application or workers."""

import base64
import csv
import importlib.util
import io
import json
import os
import signal
import subprocess
import sys
import time
import tomllib
import venv
import zipfile
from contextlib import suppress
from hashlib import sha256
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


@pytest.fixture
def smoke() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts" / "smoke.py"
    if not path.is_file():
        return ModuleType("missing_smoke_helper")
    spec = importlib.util.spec_from_file_location("phase7_smoke", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_smoke_helper_exists() -> None:
    assert (Path(__file__).resolve().parents[1] / "scripts" / "smoke.py").is_file()


@pytest.mark.parametrize("content", ["[]", "{}\n{}", '{"x": NaN}', '{"x":1,"x":2}'])
def test_stdout_requires_one_strict_json_object(
    smoke: ModuleType, content: str
) -> None:
    with pytest.raises(smoke.SmokeFailure):
        smoke.json_object(content)


def test_json_logs_require_schema_and_process_identity(smoke: ModuleType) -> None:
    record = {
        "schema_version": 1,
        "timestamp": "2026-10-11T07:00:00.000+00:00",
        "level": "INFO",
        "event": "job_started",
        "pid": 101,
        "job_id": "a" * 64,
    }
    assert smoke.json_logs(json.dumps(record)) == [record]
    with pytest.raises(smoke.SmokeFailure):
        smoke.json_logs(json.dumps(record) + "\nplain Ray warning")
    with pytest.raises(smoke.SmokeFailure):
        smoke.json_logs(json.dumps({**record, "pid": True}))
    with pytest.raises(smoke.SmokeFailure):
        smoke.json_logs("")


def test_reference_checks_actual_fees_and_fills(smoke: ModuleType) -> None:
    result: dict[str, Any] = {
        "config": {"fee_bps": 10},
        "metrics": {
            "final_equity": "749.257000",
            "total_fees": "1.743000",
            "fill_count": 2,
        },
        "fills": [{"fee": "0.996000"}, {"fee": "0.747000"}],
    }
    smoke.verify_reference(result)
    result["metrics"]["total_fees"] = "0.000000"
    with pytest.raises(smoke.SmokeFailure):
        smoke.verify_reference(result)


def experiment(output: Path) -> None:
    output.mkdir()
    (output / "results").mkdir()
    content = b'{"result":"complete ledger"}\n'
    job_id = "a" * 64
    (output / "results" / f"{job_id}.json").write_bytes(content)
    (output / "manifest.json").write_text('{"experiment_id":"same"}\n')
    (output / "execution.json").write_text("{}\n")
    (output / "index.json").write_text(
        json.dumps(
            {
                "experiment_id": "same",
                "submitted": 1,
                "succeeded": 1,
                "failed": 0,
                "outcomes": [
                    {
                        "job_id": job_id,
                        "status": "succeeded",
                        "artifact": f"results/{job_id}.json",
                        "content_sha256": sha256(content).hexdigest(),
                    }
                ],
            }
        )
    )


def test_parity_consumes_complete_artifacts(smoke: ModuleType, tmp_path: Path) -> None:
    first, second = tmp_path / "first", tmp_path / "second"
    experiment(first)
    experiment(second)
    assert smoke.compare_experiments(first, second)
    artifact = second / "results" / f"{'a' * 64}.json"
    artifact.write_text("truncated")
    with pytest.raises(smoke.SmokeFailure, match="digest"):
        smoke.compare_experiments(first, second)


def test_parity_rejects_incomplete_index(smoke: ModuleType, tmp_path: Path) -> None:
    output = tmp_path / "experiment"
    experiment(output)
    index = json.loads((output / "index.json").read_text())
    index["submitted"] = 2
    (output / "index.json").write_text(json.dumps(index))
    with pytest.raises(smoke.SmokeFailure, match="complete"):
        smoke.experiment_bytes(output)


def test_output_parent_uses_unique_children_and_preserves_existing_files(
    smoke: ModuleType, tmp_path: Path
) -> None:
    sentinel = tmp_path / "keep.txt"
    sentinel.write_text("keep")
    first = smoke.fresh_workspace(tmp_path)
    second = smoke.fresh_workspace(tmp_path)
    assert first != second and first.parent == tmp_path
    assert sentinel.read_text() == "keep"
    assert not (first / "sequential").exists()


def test_runner_uses_isolated_installed_module_and_deadline(
    smoke: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict[str, Any]] = []

    class Process:
        pid = 12345
        returncode = 0

        def __init__(self, command: list[str], **kwargs: Any) -> None:
            calls.append({"command": command, **kwargs})
            kwargs["stdout"].write('{"ok":true}\n')
            kwargs["stderr"].write("")

        def wait(self, timeout: float) -> int:
            calls[-1]["timeout"] = timeout
            return self.returncode

    monkeypatch.setattr(smoke.subprocess, "Popen", Process)
    runner = smoke.Runner(tmp_path, timeout=5)
    assert runner.cli("help", ["status", "--output-dir", "absent"], logs=False) == {
        "ok": True
    }
    call = calls[0]
    assert call["command"][:4] == [sys.executable, "-I", "-m", "distributed_backtester"]
    assert call["cwd"] == tmp_path
    assert 0 < call["timeout"] <= 5
    assert call["start_new_session"] is True
    assert (tmp_path / "help.stdout").read_text() == '{"ok":true}\n'
    assert (tmp_path / "help.command.json").is_file()


def test_timeout_reaps_child_and_reports_saved_evidence(
    smoke: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stops: list[int] = []

    class Process:
        pid = 12345
        returncode = -9

        def __init__(self, command: list[str], **kwargs: Any) -> None:
            pass

        def wait(self, timeout: float) -> int:
            raise subprocess.TimeoutExpired("test child", timeout)

    monkeypatch.setattr(smoke.subprocess, "Popen", Process)
    monkeypatch.setattr(
        smoke, "stop_process", lambda process: stops.append(process.pid)
    )
    runner = smoke.Runner(tmp_path, timeout=1)
    with pytest.raises(smoke.SmokeFailure, match="deadline"):
        runner.cli("timeout", ["status"], logs=False)
    assert stops == [12345]
    assert (tmp_path / "timeout.command.json").is_file()


def test_signal_event_is_driver_correlated(smoke: ModuleType) -> None:
    records = [
        {"event": "attempt_claimed", "pid": 10, "job_id": "a" * 64},
        {"event": "attempt_claimed", "pid": 20, "job_id": "b" * 64},
    ]
    assert smoke.find_event(records, "attempt_claimed", pid=20) == records[1]
    assert smoke.find_event(records, "attempt_claimed", pid=30) is None


def test_durable_requires_all_terminal_jobs_and_no_duplicate_commits(
    smoke: ModuleType,
) -> None:
    status: dict[str, Any] = {
        "counts": {"pending": 0, "running": 0, "succeeded": 1, "failed": 0},
        "publication_complete": True,
        "jobs": [{"job_id": "a" * 64, "status": "succeeded"}],
        "attempts": [{"job_id": "a" * 64, "status": "succeeded"}],
    }
    smoke.verify_durable(status, jobs=1)
    status["attempts"] *= 2
    with pytest.raises(smoke.SmokeFailure):
        smoke.verify_durable(status, jobs=1)


def probe_environment(root: Path) -> tuple[Path, Path]:
    """An isolated interpreter, with no application installation or workloads."""
    venv.EnvBuilder(with_pip=False, symlinks=os.name == "posix").create(root)
    python = root / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    site = subprocess.check_output(
        [
            str(python),
            "-B",
            "-I",
            "-c",
            "import sysconfig; print(sysconfig.get_path('purelib'))",
        ],
        text=True,
        timeout=10,
    ).strip()
    return python, Path(site)


def synthetic_wheel(path: Path) -> None:
    """A real minimal wheel containing only a synthetic package marker."""
    metadata = "distributed_backtester-0.8.0.dist-info"
    files = {
        "distributed_backtester/__init__.py": b"MARKER = 'synthetic wheel fixture'\n",
        f"{metadata}/METADATA": (
            b"Metadata-Version: 2.1\nName: distributed-backtester\nVersion: 0.8.0\n"
        ),
        f"{metadata}/WHEEL": (
            b"Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
        ),
    }
    records = io.StringIO(newline="")
    writer = csv.writer(records)
    for name, content in files.items():
        digest = base64.urlsafe_b64encode(sha256(content).digest()).rstrip(b"=")
        writer.writerow((name, f"sha256={digest.decode()}", len(content)))
    writer.writerow((f"{metadata}/RECORD", "", ""))
    files[f"{metadata}/RECORD"] = records.getvalue().encode()
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)


@pytest.fixture
def installed_probe_environment(tmp_path: Path) -> tuple[Path, Path]:
    # A legitimate installed environment may itself be inside a checkout.
    checkout = tmp_path / "checkout"
    (checkout / ".git").mkdir(parents=True)
    python, site = probe_environment(checkout / ".venv")
    wheel = tmp_path / "distributed_backtester-0.8.0-py3-none-any.whl"
    synthetic_wheel(wheel)
    subprocess.run(
        [
            sys.executable,
            "-B",
            "-I",
            "-m",
            "pip",
            "--python",
            str(python),
            "--disable-pip-version-check",
            "--no-cache-dir",
            "install",
            "--no-index",
            "--no-deps",
            str(wheel),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return python, site


def run_install_probe(
    smoke: ModuleType, python: Path
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(python), "-B", "-I", "-c", smoke._INSTALL_PROBE],
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_install_probe_accepts_real_wheel_inside_checkout(
    smoke: ModuleType, installed_probe_environment: tuple[Path, Path]
) -> None:
    python, site = installed_probe_environment
    result = run_install_probe(smoke, python)
    assert result.returncode == 0, result.stderr
    record = json.loads(result.stdout)
    assert record["version"] == "0.8.0"
    assert (
        Path(record["package_file"]).resolve()
        == (site / "distributed_backtester" / "__init__.py").resolve()
    )


@pytest.mark.parametrize("metadata_kind", ["egg-info", "dist-info"])
def test_install_probe_rejects_source_registered_through_pth(
    smoke: ModuleType, tmp_path: Path, metadata_kind: str
) -> None:
    python, site = probe_environment(tmp_path / "legacy-env")
    source = tmp_path / "source"
    source.mkdir()
    if metadata_kind == "egg-info":
        (source / "distributed_backtester").mkdir()
        (source / "distributed_backtester" / "__init__.py").write_text(
            "MARKER = 'source'\n"
        )
        metadata = source / "distributed_backtester.egg-info"
        metadata.mkdir()
        (metadata / "PKG-INFO").write_text(
            "Metadata-Version: 2.1\nName: distributed-backtester\nVersion: 0.8.0\n"
        )
    else:
        wheel = tmp_path / "distributed_backtester-0.8.0-py3-none-any.whl"
        synthetic_wheel(wheel)
        with zipfile.ZipFile(wheel) as archive:
            archive.extractall(source)
    (site / "source.pth").write_text(str(source) + "\n")
    result = run_install_probe(smoke, python)
    assert result.returncode != 0, "source metadata was accepted as an installed wheel"


@pytest.mark.parametrize(
    "defect", ["missing-wheel", "missing-record", "unrecorded-import", "changed-import"]
)
def test_install_probe_requires_wheel_records_and_matching_import_bytes(
    smoke: ModuleType,
    installed_probe_environment: tuple[Path, Path],
    defect: str,
) -> None:
    python, site = installed_probe_environment
    metadata = site / "distributed_backtester-0.8.0.dist-info"
    if defect == "missing-wheel":
        (metadata / "WHEEL").unlink()
    elif defect == "missing-record":
        (metadata / "RECORD").unlink()
    elif defect == "unrecorded-import":
        record = metadata / "RECORD"
        rows = list(csv.reader(io.StringIO(record.read_text())))
        with record.open("w", newline="") as stream:
            csv.writer(stream).writerows(
                row for row in rows if row[0] != "distributed_backtester/__init__.py"
            )
    else:
        (site / "distributed_backtester" / "__init__.py").write_text(
            "MARKER = 'changed'\n"
        )
    result = run_install_probe(smoke, python)
    assert result.returncode != 0, f"invalid wheel evidence accepted: {defect}"


def test_install_probe_rejects_import_shadowing_a_real_wheel(
    smoke: ModuleType,
    installed_probe_environment: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    python, site = installed_probe_environment
    source = tmp_path / "shadow"
    (source / "distributed_backtester").mkdir(parents=True)
    (source / "distributed_backtester" / "__init__.py").write_text(
        "MARKER = 'shadow'\n"
    )
    (site / "shadow.pth").write_text(
        f"import sys; sys.path.insert(0, {str(source)!r})\n"
    )
    result = run_install_probe(smoke, python)
    assert result.returncode != 0, "imported source bypassed the installed wheel"


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group ownership")
@pytest.mark.parametrize(
    ("mode", "sig"),
    [
        ("runner", signal.SIGTERM),
        ("main", signal.SIGTERM),
        ("main", signal.SIGINT),
        ("launch", signal.SIGTERM),
        ("launch", signal.SIGINT),
    ],
)
def test_wrapper_signal_reaps_owned_child_and_saves_evidence(
    smoke: ModuleType, tmp_path: Path, mode: str, sig: signal.Signals
) -> None:
    marker = tmp_path / "child.json"
    data = tmp_path / "data.csv"
    data.write_text("synthetic child fixture only\n")
    child_code = """
import json, os, signal, sys, time
from pathlib import Path
signal.signal(signal.SIGTERM, signal.SIG_IGN)
marker = Path(sys.argv[1])
temporary = marker.with_suffix('.tmp')
temporary.write_text(json.dumps({'pid': os.getpid(), 'pgid': os.getpgrp(),
                                 'workspace': os.getcwd()}))
temporary.replace(marker)
while True:
    time.sleep(60)
"""
    supervisor_code = f"""
import importlib.util, os, signal, sys, time
from pathlib import Path
spec = importlib.util.spec_from_file_location('fixture_smoke', {smoke.__file__!r})
helper = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = helper
spec.loader.exec_module(helper)
command = [sys.executable, '-B', '-I', '-c', {child_code!r}, {str(marker)!r}]
if {mode!r} == 'launch':
    actual_popen = helper.subprocess.Popen
    def signal_before_assignment(*args, **kwargs):
        process = actual_popen(*args, **kwargs)
        deadline = time.monotonic() + 10
        while not Path({str(marker)!r}).exists():
            if time.monotonic() >= deadline:
                process.kill()
                process.wait()
                raise AssertionError('launch fixture did not become ready')
            time.sleep(0.01)
        os.kill(os.getpid(), {int(sig)})
        return process
    helper.subprocess.Popen = signal_before_assignment
if {mode!r} in ('runner', 'launch'):
    try:
        helper.Runner(Path({str(tmp_path)!r}), timeout=30).execute('sleeper', command)
    except KeyboardInterrupt as error:
        raise SystemExit(128 + getattr(error, 'signum', signal.SIGINT))
else:
    def synthetic_run(data, workspace, **options):
        helper.Runner(workspace, timeout=30).execute('sleeper', command)
        raise AssertionError('the sleeping child should be interrupted')
    helper.run_smoke = synthetic_run
    raise SystemExit(helper.main(['--data', {str(data)!r}, '--output-parent',
                                {str(tmp_path)!r}, '--timeout', '30']))
"""
    sentinel = subprocess.Popen(
        [sys.executable, "-B", "-I", "-c", "import time; time.sleep(60)"],
        start_new_session=True,
    )
    wrapper: subprocess.Popen[str] | None = None
    child: dict[str, Any] | None = None
    try:
        wrapper = subprocess.Popen(
            [sys.executable, "-B", "-I", "-c", supervisor_code],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        deadline = time.monotonic() + 10
        while not marker.exists():
            assert wrapper.poll() is None, "wrapper exited before the child was ready"
            assert time.monotonic() < deadline, "sleeping fixture did not become ready"
            time.sleep(0.01)
        child = json.loads(marker.read_text())
        assert child["pid"] == child["pgid"]
        started = time.monotonic()
        if mode != "launch":
            wrapper.send_signal(sig)
        stdout, stderr = wrapper.communicate(timeout=15)
        elapsed = time.monotonic() - started
        evidence = Path(child["workspace"]) / "sleeper.command.json"
        command_record = json.loads(evidence.read_text())
        alive = True
        try:
            os.kill(child["pid"], 0)
        except ProcessLookupError:
            alive = False
        assert wrapper.returncode == 128 + sig, {
            "exit": wrapper.returncode,
            "child_alive": alive,
            "evidence": command_record,
            "stdout": stdout,
            "stderr": stderr,
        }
        assert not alive, "wrapper cancellation leaked its owned child"
        assert sentinel.poll() is None, "cleanup killed an unrelated process group"
        assert elapsed < 15
        assert command_record["pid"] == child["pid"]
        assert command_record["returncode"] == -signal.SIGKILL
        assert 0 < command_record["elapsed_seconds"] < 15
        assert command_record["wrapper_signal"] == sig.name
        if mode == "main":
            report = json.loads((evidence.parent / "report.json").read_text())
            assert report == json.loads(stdout)
            assert report["ok"] is False
            assert report["signal"] == sig.name
            assert report["exit_code"] == 128 + sig
    finally:
        # Each fixture owns a separate group. Never sweep other machine processes.
        if child is None and marker.exists():
            child = json.loads(marker.read_text())
        if child is not None:
            with suppress(ProcessLookupError):
                os.killpg(child["pgid"], signal.SIGKILL)
        if wrapper is not None:
            if wrapper.poll() is None:
                os.killpg(wrapper.pid, signal.SIGKILL)
            wrapper.communicate(timeout=5)
        if sentinel.poll() is None:
            os.killpg(sentinel.pid, signal.SIGKILL)
        sentinel.wait(timeout=5)


def test_spawn_failure_finalizes_command_evidence(
    smoke: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_spawn(*args: Any, **kwargs: Any) -> None:
        raise OSError("synthetic launch failure")

    monkeypatch.setattr(smoke.subprocess, "Popen", fail_spawn)
    with pytest.raises(OSError, match="synthetic launch failure"):
        smoke.Runner(tmp_path, timeout=1).execute("launch", ["missing-fixture"])
    record = json.loads((tmp_path / "launch.command.json").read_text())
    assert record["returncode"] is None
    assert record["elapsed_seconds"] >= 0


def test_runner_restores_caller_signal_handlers_on_failure(
    smoke: ModuleType, tmp_path: Path
) -> None:
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}

    def caller_handler(signum: int, frame: Any) -> None:
        raise AssertionError("caller handler must be restored, not invoked")

    try:
        for sig in previous:
            signal.signal(sig, caller_handler)
        with pytest.raises(smoke.SmokeFailure, match="exit 3"):
            smoke.Runner(tmp_path, timeout=5).execute(
                "failure", [sys.executable, "-B", "-I", "-c", "raise SystemExit(3)"]
            )
        assert all(signal.getsignal(sig) is caller_handler for sig in previous)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
def test_wrapper_restores_handler_when_first_signal_arrives_during_install(
    smoke: ModuleType, monkeypatch: pytest.MonkeyPatch, sig: signal.Signals
) -> None:
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    installed: list[tuple[int, object]] = []
    interrupted = False

    class SignalFixture:
        SIGTERM = signal.SIGTERM
        SIGINT = signal.SIGINT
        Signals = signal.Signals
        getsignal = staticmethod(signal.getsignal)

        @staticmethod
        def signal(signum: int, handler: Any) -> Any:
            nonlocal interrupted
            result = signal.signal(signum, handler)
            installed.append((signum, handler))
            if signum == sig and not interrupted:
                interrupted = True
                handler(signum, None)
            return result

    monkeypatch.setattr(smoke, "signal", SignalFixture)
    try:
        with pytest.raises(smoke.SmokeInterrupted), smoke.wrapper_signals():
            raise AssertionError("the first signal must interrupt installation")
        assert all(
            signal.getsignal(sig) == handler for sig, handler in previous.items()
        )
        assert installed[-2:] == list(previous.items())
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


@pytest.mark.parametrize("command", ["backtest", "sweep", "resume"])
def test_configured_paths_distinguish_file_directory_from_child_cwd(
    smoke: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    workspace = tmp_path / "commands"
    workspace.mkdir()
    data = tmp_path / "fixture.csv"
    data.write_bytes(b"synthetic path fixture only\n")
    calls: dict[str, list[str]] = {}
    reference = {
        "config": {"fee_bps": 10},
        "metrics": {
            "final_equity": "749.257000",
            "total_fees": "1.743000",
            "fill_count": 2,
        },
        "fills": [{"fee": "0.996000"}, {"fee": "0.747000"}],
    }

    def log(event: str, **fields: object) -> str:
        return (
            json.dumps(
                {
                    "schema_version": 1,
                    "timestamp": "2026-10-11T07:00:00+00:00",
                    "level": "INFO",
                    "pid": 2,
                    "event": event,
                    **fields,
                }
            )
            + "\n"
        )

    class RecordingRunner:
        # Replace only external application execution; generate the real fixture files.
        def __init__(self, output: Path, *, timeout: float) -> None:
            assert output == workspace

        def python(self, *arguments: str) -> dict[str, object]:
            return {}

        def execute(self, *args: Any, **kwargs: Any) -> tuple[str, str]:
            return "", log("invalid_config")

        def cli(
            self, label: str, arguments: list[str], **options: Any
        ) -> dict[str, Any]:
            calls[label] = arguments
            if arguments[0] == "backtest":
                return reference
            if "--output-dir" in arguments:
                output = Path(arguments[arguments.index("--output-dir") + 1])
                assert output.is_absolute()
                output.mkdir()
            if label == "process":
                (workspace / "process.stderr").write_text(log("job_started"))
                (workspace / "process.command.json").write_text('{"pid": 1}')
            elif label == "durable":
                (workspace / "durable.stderr").write_text(
                    "".join(
                        log(
                            "result_committed",
                            experiment_id="fixture",
                            job_id=str(index),
                            attempt_id="fixture-attempt",
                            generation=1,
                            disposition="committed",
                        )
                        for index in range(4)
                    )
                )
            return {"submitted": 4, "succeeded": 4, "failed": 0}

    status = {
        "counts": {"pending": 0, "running": 0, "succeeded": 4, "failed": 0},
        "publication_complete": True,
        "jobs": [{"job_id": str(index), "status": "succeeded"} for index in range(4)],
        "attempts": [
            {"job_id": str(index), "status": "succeeded"} for index in range(4)
        ],
    }
    monkeypatch.setattr(smoke, "Runner", RecordingRunner)
    monkeypatch.setattr(smoke, "_status", lambda *args: status)
    monkeypatch.setattr(smoke, "compare_experiments", lambda *args: "fixture-digest")
    smoke.run_smoke(data, workspace, timeout=1, ray=True, storage=False, signals=False)
    label = {"backtest": "config", "sweep": "sequential", "resume": "durable-resume"}[
        command
    ]
    arguments = calls[label]
    config = Path(arguments[arguments.index("--config") + 1])
    document = tomllib.loads(config.read_text())
    assert config.parent != workspace, "config directory equals the child CWD"
    file_input = (config.parent / document["data"]).resolve()
    cwd_input = (workspace / document["data"]).resolve()
    assert file_input != cwd_input
    assert file_input.read_bytes() == data.read_bytes()
    assert not cwd_input.exists(), "incorrect CWD resolution must not find the input"
    if command == "resume":
        file_output = (config.parent / document["output_dir"]).resolve()
        cwd_output = (workspace / document["output_dir"]).resolve()
        assert file_output == workspace / "durable"
        assert file_output.is_dir()
        assert cwd_output != file_output and not cwd_output.exists()

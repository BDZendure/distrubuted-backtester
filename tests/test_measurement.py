import importlib
import json
import sys
import time
from dataclasses import FrozenInstanceError, asdict
from pathlib import Path
from types import ModuleType

import pytest


def test_measured_run_is_frozen_and_slotted() -> None:
    measurement = importlib.import_module("distributed_backtester.measurement")
    run = measurement.MeasuredRun(0, 0.25, 30, 10, (), 0, 0)
    assert asdict(run) == {
        "returncode": 0,
        "elapsed_seconds": 0.25,
        "peak_tree_rss_bytes": 30,
        "peak_coordinator_rss_bytes": 10,
        "samples": (),
        "vanished_observations": 0,
        "denied_observations": 0,
    }
    assert not hasattr(run, "__dict__")
    with pytest.raises(FrozenInstanceError):
        run.returncode = 1


def test_real_command_samples_recursive_tree_rss(tmp_path: Path) -> None:
    measurement = importlib.import_module("distributed_backtester.measurement")
    pytest.importorskip("psutil")
    leaf = "import time; memory = bytearray(32 * 1024**2); time.sleep(0.4)"
    middle = (
        "import subprocess, sys; memory = bytearray(16 * 1024**2); "
        f"child = subprocess.Popen([sys.executable, '-c', {leaf!r}]); child.wait()"
    )
    coordinator = (
        "import subprocess, sys; memory = bytearray(8 * 1024**2); "
        f"child = subprocess.Popen([sys.executable, '-c', {middle!r}]); child.wait()"
    )
    run = measurement.measure_command(
        (sys.executable, "-c", coordinator),
        stdout=tmp_path / "stdout",
        stderr=tmp_path / "stderr",
        interval_seconds=0.02,
    )
    assert run.returncode == 0
    assert run.elapsed_seconds >= 0.4
    assert isinstance(run.samples, tuple)
    assert any(sample["processes"] == 3 for sample in run.samples)
    assert run.peak_tree_rss_bytes == max(
        sample["tree_rss_bytes"] for sample in run.samples
    )
    assert run.peak_coordinator_rss_bytes == max(
        sample["coordinator_rss_bytes"]
        for sample in run.samples
        if sample["coordinator_rss_bytes"] is not None
    )
    assert run.peak_tree_rss_bytes > run.peak_coordinator_rss_bytes + 32 * 1024**2
    assert run.denied_observations == 0
    assert run.vanished_observations == sum(
        sample["vanished_observations"] for sample in run.samples
    )
    for sample in run.samples:
        assert set(sample) == {
            "elapsed_seconds",
            "tree_rss_bytes",
            "coordinator_rss_bytes",
            "processes",
            "vanished_observations",
            "denied_observations",
        }
        assert 0 <= sample["elapsed_seconds"] <= run.elapsed_seconds
    assert json.loads(json.dumps(asdict(run), allow_nan=False))["returncode"] == 0


def test_real_command_captures_large_output_and_nonzero_exit(tmp_path: Path) -> None:
    measurement = importlib.import_module("distributed_backtester.measurement")
    pytest.importorskip("psutil")
    stdout, stderr = tmp_path / "stdout", tmp_path / "stderr"
    stdout.write_text("old stdout")
    stderr.write_text("old stderr")
    argument = "$(echo unexpected); literal *"
    script = (
        "import sys; print(sys.argv[1]); "
        "sys.stdout.write('o' * 1024**2); sys.stderr.write('e' * 1024**2); "
        "sys.exit(7)"
    )
    run = measurement.measure_command(
        [sys.executable, "-c", script, argument], stdout=stdout, stderr=stderr
    )
    assert run.returncode == 7
    assert stdout.read_text() == argument + "\n" + "o" * 1024**2
    assert stderr.read_text() == "e" * 1024**2


def test_real_command_completion_does_not_wait_full_sampling_interval(
    tmp_path: Path,
) -> None:
    measurement = importlib.import_module("distributed_backtester.measurement")
    pytest.importorskip("psutil")
    run = measurement.measure_command(
        [sys.executable, "-c", "pass"],
        stdout=tmp_path / "stdout",
        stderr=tmp_path / "stderr",
        interval_seconds=5,
    )
    assert run.returncode == 0
    assert 0 < run.elapsed_seconds < 2


@pytest.mark.parametrize(
    "interval",
    [0, -1, float("nan"), float("inf"), -float("inf"), True, False, "0.05", None],
)
def test_invalid_interval_is_rejected_before_output_creation(
    tmp_path: Path, interval: object
) -> None:
    measurement = importlib.import_module("distributed_backtester.measurement")
    stdout, stderr = tmp_path / "stdout", tmp_path / "stderr"
    with pytest.raises(ValueError, match="interval_seconds"):
        measurement.measure_command(
            [sys.executable, "-c", "pass"],
            stdout=stdout,
            stderr=stderr,
            interval_seconds=interval,
        )
    assert not stdout.exists()
    assert not stderr.exists()


def test_mock_missing_optional_psutil_fails_before_output_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    measurement = importlib.import_module("distributed_backtester.measurement")
    # Mock only the missing optional import; command execution stays real.
    monkeypatch.setitem(sys.modules, "psutil", None)
    stdout, stderr = tmp_path / "stdout", tmp_path / "stderr"
    with pytest.raises(RuntimeError, match=r"install.*\.\[benchmark\]"):
        measurement.measure_command(
            [sys.executable, "-c", "pass"], stdout=stdout, stderr=stderr
        )
    with pytest.raises(RuntimeError, match=r"install.*\.\[benchmark\]"):
        measurement.hardware_environment()
    assert not stdout.exists()
    assert not stderr.exists()


def test_real_hardware_environment_reports_observed_fields() -> None:
    measurement = importlib.import_module("distributed_backtester.measurement")
    psutil = pytest.importorskip("psutil")
    environment = measurement.hardware_environment()
    assert set(environment) == {
        "psutil_version",
        "physical_cores",
        "logical_cores",
        "ram_bytes",
        "cpu_model",
        "metadata_errors",
    }
    assert environment["psutil_version"] == psutil.__version__
    assert environment["physical_cores"] == psutil.cpu_count(logical=False)
    assert environment["logical_cores"] == psutil.cpu_count(logical=True)
    assert environment["ram_bytes"] == psutil.virtual_memory().total
    model = environment["cpu_model"]
    assert model is None or (isinstance(model, str) and model.strip())
    assert isinstance(environment["metadata_errors"], dict)
    assert json.loads(json.dumps(environment, allow_nan=False)) == environment


def test_mock_denied_memory_invalidates_peaks_with_real_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    measurement = importlib.import_module("distributed_backtester.measurement")
    psutil = pytest.importorskip("psutil")

    # Inject only the metadata boundary; the command and waiting remain real.
    class DeniedProcess:
        def __init__(self, pid: int) -> None:
            self.pid = pid

        def children(self, *, recursive: bool) -> list[object]:
            return []

        def is_running(self) -> bool:
            return True

        def memory_info(self) -> object:
            raise psutil.AccessDenied(self.pid)

    monkeypatch.setattr(psutil, "Process", DeniedProcess)
    run = measurement.measure_command(
        [sys.executable, "-c", "import time; time.sleep(0.1)"],
        stdout=tmp_path / "stdout",
        stderr=tmp_path / "stderr",
    )
    assert run.returncode == 0
    assert run.denied_observations > 0
    assert run.peak_tree_rss_bytes is run.peak_coordinator_rss_bytes is None
    assert all(sample["tree_rss_bytes"] is None for sample in run.samples)


def test_mock_process_race_records_vanished_observation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    measurement = importlib.import_module("distributed_backtester.measurement")
    psutil = pytest.importorskip("psutil")
    original = psutil.Process
    calls = 0

    def race(pid: int) -> object:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise psutil.NoSuchProcess(pid)
        return original(pid)

    boundary = ModuleType("psutil_boundary")
    boundary.__dict__.update(psutil.__dict__)
    boundary.__dict__["Process"] = race
    monkeypatch.setattr(measurement, "_require_psutil", lambda: boundary)
    run = measurement.measure_command(
        [sys.executable, "-c", "import time; time.sleep(0.1)"],
        stdout=tmp_path / "stdout",
        stderr=tmp_path / "stderr",
    )
    assert run.returncode == 0
    assert run.vanished_observations >= 1


def test_mock_hardware_errors_are_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    measurement = importlib.import_module("distributed_backtester.measurement")
    psutil = pytest.importorskip("psutil")

    def unavailable(*, logical: bool) -> object:
        raise OSError("injected hardware denial")

    monkeypatch.setattr(psutil, "cpu_count", unavailable)
    environment = measurement.hardware_environment()
    assert environment["logical_cores"] is None
    assert environment["physical_cores"] is None
    assert "injected hardware denial" in environment["metadata_errors"]["logical_cores"]


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX private-group cleanup")
def test_unexpected_sampler_error_kills_owned_process_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    measurement = importlib.import_module("distributed_backtester.measurement")
    psutil = pytest.importorskip("psutil")
    original = psutil.Process
    pid_file = tmp_path / "child.pid"
    script = (
        "import subprocess, sys; from pathlib import Path; "
        "child = subprocess.Popen([sys.executable, '-c', "
        "'import time; time.sleep(30)']); "
        f"Path({str(pid_file)!r}).write_text(str(child.pid)); child.wait()"
    )
    root_pids: list[int] = []

    def injected(pid: int) -> object:
        root_pids.append(pid)
        deadline = time.monotonic() + 5
        while not pid_file.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not pid_file.exists():
            raise AssertionError("test child did not initialize")
        raise RuntimeError("injected sampler failure")

    boundary = ModuleType("psutil_boundary")
    boundary.__dict__.update(psutil.__dict__)
    boundary.__dict__["Process"] = injected
    monkeypatch.setattr(measurement, "_require_psutil", lambda: boundary)
    with pytest.raises(RuntimeError, match="sampler failure"):
        measurement.measure_command(
            [sys.executable, "-c", script],
            stdout=tmp_path / "stdout",
            stderr=tmp_path / "stderr",
        )
    assert not psutil.pid_exists(root_pids[0])  # Direct child was reaped.
    try:
        descendant = original(int(pid_file.read_text()))
    except psutil.NoSuchProcess:
        return  # It has already exited and been reaped by the operating system.
    deadline = time.monotonic() + 5
    while True:
        try:
            if (
                not descendant.is_running()
                or descendant.status() == psutil.STATUS_ZOMBIE
            ):
                return
        except psutil.NoSuchProcess:
            return
        assert time.monotonic() < deadline
        time.sleep(0.01)

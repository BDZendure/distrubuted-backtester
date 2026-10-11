import importlib
import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, cast

import pytest


def module() -> ModuleType:
    return importlib.import_module("distributed_backtester.storage_measurement")


@pytest.mark.parametrize(
    "system,raw,expected,unit",
    [
        ("Darwin", 1024, 1024, "bytes"),
        ("Linux", 1024, 1048576, "KiB"),
    ],
)
def test_rss_normalizes_only_documented_platforms(
    monkeypatch: pytest.MonkeyPatch, system: str, raw: int, expected: int, unit: str
) -> None:
    measurement = module()
    monkeypatch.setattr(measurement.platform, "system", lambda: system)
    resource = SimpleNamespace(
        RUSAGE_SELF=0, getrusage=lambda who: SimpleNamespace(ru_maxrss=raw)
    )
    monkeypatch.setitem(sys.modules, "resource", resource)
    row = measurement.peak_rss()
    assert row["available"] is True
    assert row["peak_rss_bytes"] == expected
    assert row["raw_unit"] == unit
    assert row["unit"] == "bytes"
    assert row["scope"] == "own_process_lifetime_high_water"
    assert row["pid"] == os.getpid()
    assert "RUSAGE_SELF" in row["source"]


@pytest.mark.parametrize(
    "system,resource",
    [
        ("Windows", None),
        ("Linux", None),
        (
            "Linux",
            SimpleNamespace(
                RUSAGE_SELF=0, getrusage=lambda who: SimpleNamespace(ru_maxrss=-1)
            ),
        ),
    ],
)
def test_unavailable_rss_is_explicit(
    monkeypatch: pytest.MonkeyPatch, system: str, resource: object
) -> None:
    measurement = module()
    monkeypatch.setattr(measurement.platform, "system", lambda: system)
    monkeypatch.setitem(sys.modules, "resource", resource)
    row = measurement.peak_rss()
    assert row["available"] is False
    assert row["peak_rss_bytes"] is None
    assert row["reason"]


def test_operation_timer_excludes_memory_and_verification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    measurement = module()
    events: list[str] = []
    ticks = iter([2.0, 5.0])
    monkeypatch.setattr(measurement, "perf_counter", lambda: next(ticks))

    def operation() -> int:
        events.append("operation")
        return 7

    def memory() -> dict[str, object]:
        events.append("memory")
        return {"available": False}

    monkeypatch.setattr(measurement, "peak_rss", memory)
    result = measurement.measure_operation(operation)
    events.append("verification")
    assert result.value == 7
    assert result.elapsed_seconds == 3.0
    assert events == ["operation", "memory", "verification"]


def test_operation_propagates_failure() -> None:
    def fail() -> None:
        raise ValueError("broken read")

    with pytest.raises(ValueError, match="broken read"):
        module().measure_operation(fail)


def test_command_timer_covers_spawn_through_exit_and_preserves_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    measurement = module()
    events: list[object] = []
    ticks = iter([10.0, 14.0])
    monkeypatch.setattr(measurement, "perf_counter", lambda: next(ticks))

    class Child:
        def wait(self, *, timeout: float) -> int:
            events.append("exit")
            return 3

    def spawn(command: Sequence[str], **kwargs: Any) -> Child:
        events.append(list(command))
        assert kwargs["shell"] is False
        kwargs["stderr"].write(b"child failed")
        return Child()

    monkeypatch.setattr(measurement.subprocess, "Popen", spawn)
    result = measurement.measure_command(
        ["python", "child"], stdout=tmp_path / "stdout", stderr=tmp_path / "stderr"
    )
    assert result.returncode == 3
    assert result.elapsed_seconds == 4.0
    assert events == [["python", "child"], "exit"]
    assert (tmp_path / "stderr").read_text() == "child failed"


@pytest.mark.parametrize("timeout", [0, True, float("nan"), float("inf")])
def test_bad_timeout_does_not_launch_child(tmp_path: Path, timeout: float) -> None:
    with pytest.raises(ValueError, match="timeout"):
        module().measure_command(
            ["missing"],
            stdout=tmp_path / "out",
            stderr=tmp_path / "err",
            timeout_seconds=timeout,
        )
    assert not (tmp_path / "out").exists()


def test_timed_out_command_is_killed_and_reaped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    measurement = module()
    events: list[str] = []

    class Child:
        def wait(self, *, timeout: float | None = None) -> int:
            if timeout is not None:
                raise measurement.subprocess.TimeoutExpired("child", timeout)
            events.append("reaped")
            return -9

        def kill(self) -> None:
            events.append("killed")

    monkeypatch.setattr(
        measurement.subprocess, "Popen", lambda *args, **kwargs: Child()
    )
    with pytest.raises(RuntimeError, match="timeout") as caught:
        measurement.measure_command(
            ["child"], stdout=tmp_path / "out", stderr=tmp_path / "err"
        )
    assert events == ["killed", "reaped"]
    assert caught.value.observations["returncode"] == -9  # type: ignore[attr-defined]
    assert caught.value.observations["end_to_end_seconds"] >= 0  # type: ignore[attr-defined]


def test_first_read_separates_import_read_and_digest_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from distributed_backtester.market_data import load_dataset
    from distributed_backtester.workloads import write_synthetic_csv

    measurement = module()
    source = tmp_path / "data.csv"
    write_synthetic_csv(source, bars=8)
    expected = load_dataset(source, symbol="BENCH", price_basis="raw")
    events: list[str] = []
    original = measurement.verify_dataset

    def verify(data: Any) -> dict[str, object]:
        events.append("verify")
        return cast(dict[str, object], original(data))

    monkeypatch.setattr(measurement, "verify_dataset", verify)
    measurement.read_child(
        {
            "path": str(source),
            "data_format": "csv",
            "expected_version": expected.metadata.version_id,
        },
        tmp_path / "read.json",
    )
    row = json.loads((tmp_path / "read.json").read_text())
    assert row["identity"]["source_sha256"] == expected.metadata.source_sha256
    assert row["identity"]["bar_count"] == 8
    assert row["import_setup_seconds"] >= 0
    assert row["elapsed_seconds"] >= 0
    assert row["memory"]["scope"] == "own_process_lifetime_high_water"
    assert row["environment"]["python_version"]
    assert row["implementation"]["source_sha256"]
    assert events == ["verify"]


# Main opts in after freezing all package source. These tests launch real children.
def test_transfer_receiver_acknowledges_decode_before_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pickle

    from distributed_backtester.market_data import load_dataset
    from distributed_backtester.workloads import write_synthetic_csv

    measurement = module()
    source = tmp_path / "data.csv"
    write_synthetic_csv(source, bars=8)
    data = load_dataset(source, symbol="BENCH", price_basis="raw")
    payload = pickle.dumps(data, protocol=5)
    events: list[str] = []
    replies: list[dict[str, Any]] = []
    original = measurement.verify_dataset

    class Connection:
        def recv_bytes(self) -> bytes:
            return payload

        def send(self, value: dict[str, Any]) -> None:
            events.append(value["status"])
            replies.append(value)

        def recv(self) -> dict[str, str]:
            events.append("verification_requested")
            return {"command": "verify"}

        def close(self) -> None:
            events.append("closed")

    def verify(dataset: Any) -> dict[str, object]:
        events.append("verify")
        return cast(dict[str, object], original(dataset))

    monkeypatch.setattr(measurement, "verify_dataset", verify)
    measurement._receive_transfer(Connection())
    assert events == [
        "decoded",
        "verification_requested",
        "verify",
        "verified",
        "closed",
    ]
    assert replies[1]["identity"] == original(data)
    assert replies[1]["payload_bytes"] == len(payload)
    from hashlib import sha256

    assert replies[1]["payload_sha256"] == sha256(payload).hexdigest()


def test_spawn_transfer_uses_spawn_and_reaps_on_bad_ack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    measurement = module()
    events: list[str] = []

    class Connection:
        def send_bytes(self, payload: bytes) -> None:
            events.append("sent")

        def poll(self, timeout: float) -> bool:
            return True

        def recv(self) -> dict[str, object]:
            return {"status": "error", "error": "decode failed"}

        def close(self) -> None:
            events.append("closed")

    class Process:
        exitcode = 1

        def start(self) -> None:
            events.append("started")

        def join(self, timeout: float | None = None) -> None:
            events.append("joined")

        def is_alive(self) -> bool:
            return False

    class Context:
        def Pipe(self) -> tuple[Connection, Connection]:
            return Connection(), Connection()

        def Process(self, **kwargs: Any) -> Process:
            return Process()

    def context(name: str) -> Context:
        assert name == "spawn"
        return Context()

    monkeypatch.setattr(measurement.multiprocessing, "get_context", context)
    with pytest.raises(RuntimeError, match="decode failed"):
        measurement.transfer_payload(b"payload")
    assert "joined" in events
    assert events.count("closed") == 2


def test_spawn_transfer_failed_exit_preserves_verified_child_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    measurement = module()
    ticks = iter(float(value) for value in range(1, 9))
    monkeypatch.setattr(measurement, "perf_counter", lambda: next(ticks))
    child_report = {"status": "verified", "payload_bytes": 5}
    replies = iter(cast(list[dict[str, Any]], [{"status": "decoded"}, child_report]))
    events: list[object] = []

    class Connection:
        def send_bytes(self, payload: bytes) -> None:
            pass

        def send(self, value: object) -> None:
            events.append(value)

        def poll(self, timeout: float) -> bool:
            return True

        def recv(self) -> dict[str, Any]:
            return next(replies)

        def close(self) -> None:
            pass

    class Process:
        exitcode = 7

        def start(self) -> None:
            pass

        def join(self, timeout: float | None = None) -> None:
            pass

        def is_alive(self) -> bool:
            return False

    class Context:
        def Pipe(self) -> tuple[Connection, Connection]:
            return Connection(), Connection()

        def Process(self, **kwargs: Any) -> Process:
            return Process()

    monkeypatch.setattr(
        measurement.multiprocessing, "get_context", lambda name: Context()
    )
    with pytest.raises(RuntimeError, match="child failed") as caught:
        measurement.transfer_payload(b"12345")
    assert events == [{"command": "verify"}]
    assert caught.value.observations["child"] == child_report  # type: ignore[attr-defined]
    assert caught.value.observations["returncode"] == 7  # type: ignore[attr-defined]
    assert caught.value.observations["transfer_seconds"] >= 0  # type: ignore[attr-defined]
    assert caught.value.observations["end_to_end_seconds"] == 7.0  # type: ignore[attr-defined]


def test_transfer_sender_start_failure_preserves_original_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    measurement = module()

    class Connection:
        def close(self) -> None:
            pass

    class Process:
        exitcode = -15
        alive = True

        def start(self) -> None:
            pass

        def is_alive(self) -> bool:
            return self.alive

        def terminate(self) -> None:
            self.alive = False

        def join(self, timeout: float | None = None) -> None:
            pass

    class Context:
        def Pipe(self) -> tuple[Connection, Connection]:
            return Connection(), Connection()

        def Process(self, **kwargs: Any) -> Process:
            return Process()

    class Thread:
        def __init__(self, **kwargs: Any) -> None:
            pass

        def start(self) -> None:
            raise RuntimeError("cannot start sender")

        def join(self, timeout: float) -> None:
            raise RuntimeError("cannot join unstarted sender")

    monkeypatch.setattr(
        measurement.multiprocessing, "get_context", lambda name: Context()
    )
    monkeypatch.setattr(measurement, "Thread", Thread)
    with pytest.raises(
        measurement.MeasurementFailure, match="cannot start sender"
    ) as caught:
        measurement.transfer_payload(b"payload")
    assert caught.value.observations["returncode"] == -15


def test_command_interruption_kills_and_reaps_owned_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    measurement = module()
    events: list[str] = []

    class Process:
        def wait(self, *, timeout: float | None = None) -> int:
            if timeout is not None:
                raise KeyboardInterrupt("stop")
            events.append("reaped")
            return -9

        def kill(self) -> None:
            events.append("killed")

    monkeypatch.setattr(
        measurement.subprocess, "Popen", lambda *args, **kwargs: Process()
    )
    with pytest.raises(KeyboardInterrupt, match="stop"):
        measurement.measure_command(
            ["child"], stdout=tmp_path / "out", stderr=tmp_path / "err"
        )
    assert events == ["killed", "reaped"]


def test_transfer_interruption_terminates_and_reaps_owned_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    measurement = module()
    events: list[str] = []

    class Connection:
        def send_bytes(self, payload: bytes) -> None:
            pass

        def poll(self, timeout: float) -> bool:
            raise KeyboardInterrupt("stop")

        def close(self) -> None:
            events.append("closed")

    class Process:
        exitcode = -15
        alive = True

        def start(self) -> None:
            pass

        def is_alive(self) -> bool:
            return self.alive

        def terminate(self) -> None:
            self.alive = False
            events.append("terminated")

        def join(self, timeout: float | None = None) -> None:
            events.append("reaped")

    class Context:
        def Pipe(self) -> tuple[Connection, Connection]:
            return Connection(), Connection()

        def Process(self, **kwargs: Any) -> Process:
            return Process()

    monkeypatch.setattr(
        measurement.multiprocessing, "get_context", lambda name: Context()
    )
    with pytest.raises(KeyboardInterrupt, match="stop"):
        measurement.transfer_payload(b"payload")
    assert "terminated" in events and "reaped" in events
    assert events.count("closed") == 2


@pytest.mark.skipif(
    os.environ.get("PHASE5_STORAGE_CHILD_TESTS") != "1",
    reason="requires source freeze and explicit child-test opt-in",
)
def test_real_first_read_child_and_spawn_transfer(tmp_path: Path) -> None:
    import pickle

    from distributed_backtester.market_data import load_dataset
    from distributed_backtester.workloads import write_synthetic_csv

    measurement = module()
    source = tmp_path / "data.csv"
    write_synthetic_csv(source, bars=16)
    data = load_dataset(source, symbol="BENCH", price_basis="raw")
    request = tmp_path / "request.json"
    request.write_text(
        json.dumps(
            {
                "path": str(source),
                "data_format": "csv",
                "expected_version": data.metadata.version_id,
            }
        )
    )
    result = tmp_path / "child.json"
    run = measurement.measure_command(
        [
            sys.executable,
            "-m",
            "distributed_backtester.storage_measurement",
            "_read",
            "--request",
            str(request),
            "--output",
            str(result),
        ],
        stdout=tmp_path / "stdout",
        stderr=tmp_path / "stderr",
    )
    assert run.returncode == 0, (tmp_path / "stderr").read_text()
    row = json.loads(result.read_text())
    assert row["identity"] == measurement.verify_dataset(data)
    assert run.elapsed_seconds >= row["elapsed_seconds"]
    transfer = measurement.transfer_payload(pickle.dumps(data, protocol=5))
    assert transfer.child["identity"] == measurement.verify_dataset(data)
    assert transfer.child["pid"] != os.getpid()
    assert transfer.elapsed_seconds >= transfer.transfer_seconds >= 0

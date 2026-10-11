"""Phase 5 local measurements, with optional storage imports inside operations.

RSS is resource.RUSAGE_SELF's lifetime high water, not a per-operation delta or
a cluster total. The read API's internal validation stays inside its timing;
the harness's independent digest checks stay outside. OS cache is uncontrolled.
"""

import argparse
import json
import math
import multiprocessing
import os
import pickle
import platform
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from hashlib import sha256
from multiprocessing.connection import Connection
from pathlib import Path
from threading import Thread
from time import perf_counter
from typing import Any, cast


@dataclass(frozen=True, slots=True)
class OperationMeasurement[T]:
    value: T
    elapsed_seconds: float
    memory: dict[str, object]


@dataclass(frozen=True, slots=True)
class CommandMeasurement:
    returncode: int
    elapsed_seconds: float


@dataclass(frozen=True, slots=True)
class TransferMeasurement:
    transfer_seconds: float
    elapsed_seconds: float
    child: dict[str, Any]


class MeasurementFailure(RuntimeError):
    """Preserve observed timings/status/replies when a local child fails."""

    def __init__(self, message: str, observations: dict[str, Any]) -> None:
        super().__init__(message)
        self.observations = observations


def peak_rss() -> dict[str, object]:
    """Normalize macOS bytes and Linux KiB, recording unknowns as unavailable."""
    system = platform.system()
    row: dict[str, object] = {
        "available": False,
        "peak_rss_bytes": None,
        "unit": "bytes",
        "raw_value": None,
        "raw_unit": {"Darwin": "bytes", "Linux": "KiB"}.get(system),
        "source": "resource.getrusage(RUSAGE_SELF).ru_maxrss",
        "scope": "own_process_lifetime_high_water",
        "pid": os.getpid(),
        "reason": None,
    }
    if system not in {"Darwin", "Linux"}:
        row["reason"] = f"unsupported ru_maxrss units on {system}"
        return row
    try:
        import resource

        raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if type(raw) not in (int, float) or not math.isfinite(raw) or raw < 0:
            raise ValueError("invalid ru_maxrss value")
        row.update(
            available=True,
            raw_value=raw,
            peak_rss_bytes=int(raw * (1024 if system == "Linux" else 1)),
        )
    except (ImportError, AttributeError, OSError, ValueError, OverflowError) as error:
        row["reason"] = f"{type(error).__name__}: {error}"
    return row


def measure_operation[T](operation: Callable[[], T]) -> OperationMeasurement[T]:
    started = perf_counter()
    value = operation()
    elapsed = perf_counter() - started
    return OperationMeasurement(value, elapsed, peak_rss())


def validate_timeout(timeout_seconds: float) -> None:
    if type(timeout_seconds) not in (int, float):
        raise ValueError("timeout_seconds must be finite and positive")
    try:
        valid = math.isfinite(timeout_seconds) and timeout_seconds > 0
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError("timeout_seconds must be finite and positive")


def measure_command(
    command: Sequence[str],
    *,
    stdout: Path,
    stderr: Path,
    timeout_seconds: float = 3600,
) -> CommandMeasurement:
    """Time before Popen through wait; only the owned child is killed on error."""
    validate_timeout(timeout_seconds)
    with stdout.open("xb") as output, stderr.open("xb") as errors:
        started = perf_counter()
        child = subprocess.Popen(command, stdout=output, stderr=errors, shell=False)
        try:
            status = child.wait(timeout=timeout_seconds)
            elapsed = perf_counter() - started
        except BaseException as error:
            child.kill()
            status = child.wait()
            observations = {
                "stage": "first_process_read",
                "returncode": status,
                "end_to_end_seconds": perf_counter() - started,
            }
            if isinstance(error, subprocess.TimeoutExpired):
                raise MeasurementFailure(
                    "first-read child timeout", observations
                ) from error
            if isinstance(error, Exception):
                raise MeasurementFailure(
                    f"first-read child failed: {error}", observations
                ) from error
            raise
    return CommandMeasurement(status, elapsed)


def verify_dataset(dataset: object) -> dict[str, object]:
    """Independently identify an actual immutable dataset outside read timing."""
    from distributed_backtester.dataset_identity import bars_digest
    from distributed_backtester.models import MarketDataset

    if not isinstance(dataset, MarketDataset):
        raise ValueError("expected a MarketDataset")
    return {
        "source_sha256": dataset.metadata.source_sha256,
        "version_id": dataset.metadata.version_id,
        "bars_sha256": bars_digest(dataset.bars),
        "bar_count": len(dataset.bars),
    }


def _context() -> dict[str, object]:
    from distributed_backtester.provenance import (
        capture_environment,
        current_implementation,
    )

    return {
        "environment": capture_environment(),
        "implementation": asdict(current_implementation()),
    }


def read_child(request: Mapping[str, Any], output: Path) -> None:
    """Read once in a new interpreter; report import/setup separately from load.

    import_setup_seconds covers market-data and optional Arrow module imports
    and argument setup. Interpreter/stdlib startup, verification, metadata,
    report writing and exit are included only by the parent's end-to-end timer.
    """
    setup_started = perf_counter()
    from distributed_backtester.market_data import load_dataset

    selected = request["data_format"]
    if selected not in {"csv", "parquet"}:
        raise ValueError("read child requires explicit csv or parquet format")
    if selected == "parquet":
        from distributed_backtester.parquet_data import _arrow

        _arrow()
    source = Path(request["path"])
    version = request["expected_version"]
    setup_seconds = perf_counter() - setup_started
    measured = measure_operation(
        lambda: load_dataset(
            source,
            symbol="BENCH",
            price_basis="raw",
            data_format=selected,
            expected_version=version,
        )
    )
    identity = verify_dataset(measured.value)
    row = {
        "schema_version": 1,
        "identity": identity,
        "import_setup_seconds": setup_seconds,
        "elapsed_seconds": measured.elapsed_seconds,
        "memory": measured.memory,
        **_context(),
    }
    from distributed_backtester.jobs import canonical_json
    from distributed_backtester.results import write_text

    write_text(canonical_json(row) + "\n", output)


def _receive_transfer(connection: Connection) -> None:
    """Trusted local protocol-5 bytes only. Acknowledge decode before checks."""
    try:
        started = perf_counter()
        payload = connection.recv_bytes()
        received = perf_counter()
        dataset = pickle.loads(payload)
        decoded = perf_counter()
        connection.send({"status": "decoded"})
        if connection.recv() != {"command": "verify"}:
            raise ValueError("invalid transfer verification handshake")
        identity = verify_dataset(dataset)
        connection.send(
            {
                "status": "verified",
                "identity": identity,
                "payload_bytes": len(payload),
                "payload_sha256": sha256(payload).hexdigest(),
                "receive_seconds": received - started,
                "deserialize_seconds": decoded - received,
                "pid": os.getpid(),
                "memory": peak_rss(),
                **_context(),
            }
        )
    except Exception as error:
        connection.send(
            {"status": "error", "error": f"{type(error).__name__}: {error}"}
        )
    finally:
        connection.close()


def transfer_payload(
    payload: bytes, *, timeout_seconds: float = 3600
) -> TransferMeasurement:
    """Spawn one same-host interpreter and send in-band protocol-5 bytes.

    transfer_seconds covers spawn, IPC, decode and acknowledgement, excluding
    independent child digest checks. elapsed_seconds also includes verification,
    environment capture and child exit. Serialization happens before this call.
    A bounded sender thread lets timeout cleanup unblock a full pipe safely.
    """
    validate_timeout(timeout_seconds)
    context = multiprocessing.get_context("spawn")
    parent, receiver = context.Pipe()
    child = context.Process(target=_receive_transfer, args=(receiver,))
    started = perf_counter()
    deadline = started + timeout_seconds
    launched = False
    sender: Thread | None = None
    sender_launched = False
    send_errors: list[BaseException] = []
    observations: dict[str, Any] = {
        "stage": "spawn_transfer",
        "child": None,
        "transfer_seconds": None,
        "end_to_end_seconds": None,
        "returncode": None,
        "replies": [],
    }

    def remaining() -> float:
        seconds = deadline - perf_counter()
        if seconds <= 0:
            raise RuntimeError("dataset transfer timeout")
        return seconds

    def send() -> None:
        try:
            parent.send_bytes(payload)
        except BaseException as error:
            send_errors.append(error)

    def reply(status: str) -> dict[str, Any]:
        if not parent.poll(remaining()):
            raise RuntimeError("dataset transfer timeout")
        value = parent.recv()
        observations["replies"].append(value)
        if not isinstance(value, dict) or value.get("status") != status:
            raise RuntimeError(f"dataset transfer failed: {value}")
        return cast(dict[str, Any], value)

    try:
        child.start()
        launched = True
        receiver.close()
        sender = Thread(target=send, daemon=True)
        sender.start()
        sender_launched = True
        sender.join(remaining())
        if sender.is_alive():
            raise RuntimeError("dataset transfer send timeout")
        if send_errors:
            raise RuntimeError(f"dataset transfer send failed: {send_errors[0]}")
        reply("decoded")
        transfer_seconds = perf_counter() - started
        observations["transfer_seconds"] = transfer_seconds
        parent.send({"command": "verify"})
        row = reply("verified")
        observations["child"] = row
        child.join(remaining())
        if child.is_alive():
            raise RuntimeError("dataset transfer exit timeout")
        if child.exitcode != 0:
            raise RuntimeError(f"dataset transfer child failed: {child.exitcode}")
        elapsed = perf_counter() - started
        return TransferMeasurement(transfer_seconds, elapsed, row)
    except Exception as error:
        observations["end_to_end_seconds"] = perf_counter() - started
        raise MeasurementFailure(str(error), observations) from error
    finally:
        if launched:
            if child.is_alive():
                child.terminate()
                child.join(1)
                if child.is_alive():
                    child.kill()
            child.join()
            observations["returncode"] = child.exitcode
        parent.close()
        if not launched:
            receiver.close()
        if sender is not None and sender_launched:
            sender.join(1)
        if observations["end_to_end_seconds"] is not None:
            observations["end_to_end_seconds"] = perf_counter() - started


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m distributed_backtester.storage_measurement"
    )
    parser.add_argument("command", choices=["_read"])
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    options = parser.parse_args(argv)
    try:
        request = json.loads(options.request.read_text(encoding="utf-8"))
        if not isinstance(request, dict):
            raise ValueError("read request must be a JSON object")
        read_child(request, options.output)
    except (ValueError, OSError, RuntimeError, KeyError, TypeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

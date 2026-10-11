"""Ray failure boundaries, bounded references, and isolated real worker faults.

The pure cases never import Ray. Real cases run in a separate process group;
coordinate a source freeze before running them alongside development work.
"""

import gc
import importlib
import json
import os
import signal
import subprocess
import sys
import weakref
from dataclasses import replace
from pathlib import Path
from time import monotonic, sleep
from types import ModuleType, SimpleNamespace
from typing import NoReturn, cast

import pytest

from distributed_backtester.backends import ray as ray_backend
from distributed_backtester.data import load_csv
from distributed_backtester.experiments import collect_experiment
from distributed_backtester.jobs import JobSpec, create_job
from distributed_backtester.models import BacktestConfig, MarketDataset
from distributed_backtester.provenance import (
    ImplementationIdentity,
    current_implementation,
)
from distributed_backtester.results import write_text
from distributed_backtester.worker import JobOutcome

_SCRIPT = Path(__file__).resolve()
_REPOSITORY = _SCRIPT.parents[1]


def _inputs(
    count: int = 12,
) -> tuple[MarketDataset, ImplementationIdentity, list[JobSpec]]:
    data = load_csv(_REPOSITORY / "examples/sma.csv", symbol="SYNTH", price_basis="raw")
    identity = current_implementation()
    jobs = [
        create_job(BacktestConfig(fee_bps=fee), data.metadata, identity)
        for fee in range(count)
    ]
    return data, identity, jobs


class _Reference:
    def __init__(self, value: object) -> None:
        self.value = value


class _Ready:
    def __init__(self, worker: ray_backend._SimulationWorker) -> None:
        self.worker = worker

    def remote(self) -> _Reference:
        return _Reference(self.worker.ready())


class _Execute:
    def __init__(self, actor: "_FakeActor", transport: "_FakeRay") -> None:
        self.actor = actor
        self.transport = transport

    def remote(self, job: JobSpec) -> _Reference:
        value: object = self.actor.worker.execute(job)
        if self.transport.fault == "actor-death":
            value = RuntimeError("injected actor death")
        elif self.transport.fault == "wrong-id":
            assert isinstance(value, JobOutcome)
            value = replace(value, job_id="f" * 64)
        elif self.transport.fault == "wrong-type":
            value = {"job_id": job.job_id}
        reference = _Reference(value)
        self.transport.submitted.append(job.job_id)
        self.transport.references.append(weakref.ref(reference))
        return reference


class _FakeActor:
    def __init__(
        self,
        worker: ray_backend._SimulationWorker,
        transport: "_FakeRay",
    ) -> None:
        self.worker = worker
        self.ready = _Ready(worker)
        self.execute = _Execute(self, transport)


class _FakeActorClass:
    def __init__(self, transport: "_FakeRay") -> None:
        self.transport = transport

    def options(self, **options: object) -> "_FakeActorClass":
        self.transport.actor_options = options
        return self

    def remote(
        self, dataset: MarketDataset, implementation: ImplementationIdentity
    ) -> _FakeActor:
        actor = _FakeActor(
            ray_backend._SimulationWorker(dataset, implementation), self.transport
        )
        self.transport.actors.append(actor)
        return actor


class _Cloudpickle(ModuleType):
    def dumps(self, value: object) -> bytes:
        return b"serialized dataset"


class _FakeRay(ModuleType):
    """Replace transport only; retain the actual worker/backend/collector code."""

    def __init__(self) -> None:
        super().__init__("ray")
        self.fault = ""
        self.initialized = False
        self.actors: list[_FakeActor] = []
        self.killed: list[_FakeActor] = []
        self.submitted: list[str] = []
        self.references: list[weakref.ReferenceType[_Reference]] = []
        self.actor_options: dict[str, object] = {}
        self.readiness_timeouts: list[float | None] = []
        self.shutdowns = 0
        self.dataset_puts = 0

    def is_initialized(self) -> bool:
        return self.initialized

    def init(self, **options: object) -> SimpleNamespace:
        self.initialized = True
        return SimpleNamespace(
            address_info={"session_dir": "/tmp/fake-ray", "gcs_address": "fake:1"}
        )

    def nodes(self) -> list[dict[str, object]]:
        return [
            {
                "Alive": True,
                "NodeID": "fake-node",
                "NodeManagerAddress": "127.0.0.1",
                "Resources": {"CPU": 2, "object_store_memory": 134_217_728},
            }
        ]

    def put(self, dataset: MarketDataset) -> MarketDataset:
        self.dataset_puts += 1
        return dataset

    def remote(self, worker_type: object) -> _FakeActorClass:
        assert worker_type is ray_backend._SimulationWorker
        return _FakeActorClass(self)

    def get_runtime_context(self) -> SimpleNamespace:
        return SimpleNamespace(get_node_id=lambda: "fake-node")

    def get(
        self, references: _Reference | list[_Reference], *, timeout: float | None = None
    ) -> object:
        if isinstance(references, list):
            self.readiness_timeouts.append(timeout)
            if self.fault == "readiness-timeout":
                raise TimeoutError("injected readiness timeout")
            return [reference.value for reference in references]
        if isinstance(references.value, BaseException):
            raise references.value
        return references.value

    def wait(
        self, references: list[_Reference], *, num_returns: int
    ) -> tuple[list[_Reference], list[_Reference]]:
        assert num_returns == 1
        # Complete in reverse submission order to exercise pending bookkeeping.
        return references[-1:], references[:-1]

    def kill(self, actor: _FakeActor, *, no_restart: bool) -> None:
        assert no_restart
        self.killed.append(actor)

    def shutdown(self) -> None:
        self.initialized = False
        self.shutdowns += 1


@pytest.fixture
def fake_ray(monkeypatch: pytest.MonkeyPatch) -> _FakeRay:
    transport = _FakeRay()
    private_worker = ModuleType("ray._private.worker")
    pickler = _Cloudpickle("ray.cloudpickle")
    original_import = importlib.import_module

    def import_module(name: str, package: str | None = None) -> ModuleType:
        if name == "ray._private.worker":
            return private_worker
        if name == "ray.cloudpickle":
            return pickler
        if name == "ray" or name.startswith("ray."):
            raise AssertionError(f"pure test attempted a real Ray import: {name}")
        return original_import(name, package)

    monkeypatch.setattr(ray_backend, "_require_ray", lambda: transport)
    monkeypatch.setattr(importlib, "import_module", import_module)
    return transport


def _assert_cleaned(transport: _FakeRay) -> None:
    assert len(transport.actors) == 2
    assert transport.killed == transport.actors
    assert transport.shutdowns == 1
    assert not transport.is_initialized()
    assert transport.actor_options["max_restarts"] == 0
    assert transport.actor_options["max_task_retries"] == 0


@pytest.mark.parametrize("fault", ["wrong-id", "wrong-type", "actor-death"])
def test_pure_fatal_transport_output_cannot_finalize(
    tmp_path: Path, fake_ray: _FakeRay, fault: str
) -> None:
    fake_ray.fault = fault
    data, identity, jobs = _inputs()
    output = tmp_path / fault
    expected = "actor death" if fault == "actor-death" else "outcome does not match"
    with pytest.raises(RuntimeError, match=expected):
        collect_experiment(
            jobs, data, identity, output, ray_backend.RayBackend(2, max_in_flight=3)
        )
    assert (output / "manifest.json").is_file()
    assert not (output / "index.json").exists()
    assert not (output / "execution.json").exists()
    assert not (output / "results").exists()
    assert len(fake_ray.submitted) == 3
    _assert_cleaned(fake_ray)


def test_pure_readiness_timeout_cleans_pool_without_submitting(
    tmp_path: Path, fake_ray: _FakeRay
) -> None:
    fake_ray.fault = "readiness-timeout"
    data, identity, jobs = _inputs()
    output = tmp_path / "timeout"
    with pytest.raises(RuntimeError, match="readiness timeout") as error:
        collect_experiment(
            jobs,
            data,
            identity,
            output,
            ray_backend.RayBackend(2, startup_timeout_seconds=0.125),
        )
    assert isinstance(error.value.__cause__, TimeoutError)
    assert fake_ray.readiness_timeouts == [0.125]
    assert fake_ray.submitted == []
    assert not (output / "index.json").exists()
    _assert_cleaned(fake_ray)


def test_pure_export_failure_closes_live_backend(
    tmp_path: Path, fake_ray: _FakeRay, monkeypatch: pytest.MonkeyPatch
) -> None:
    exported = 0

    def fail_export(text: str, path: Path, *, overwrite: bool = False) -> None:
        nonlocal exported
        if path.parent.name == "results":
            assert fake_ray.is_initialized()
            assert not fake_ray.killed
            if exported == 1:
                raise OSError("injected result export failure")
            exported += 1
        write_text(text, path, overwrite=overwrite)

    monkeypatch.setattr("distributed_backtester.experiments.write_text", fail_export)
    data, identity, jobs = _inputs()
    output = tmp_path / "export"
    with pytest.raises(OSError, match="result export failure"):
        collect_experiment(
            jobs, data, identity, output, ray_backend.RayBackend(2, max_in_flight=3)
        )
    assert len(list((output / "results").glob("*.json"))) == 1
    assert not (output / "index.json").exists()
    assert not (output / "execution.json").exists()
    _assert_cleaned(fake_ray)


def test_pure_early_close_releases_pending_references(fake_ray: _FakeRay) -> None:
    data, identity, jobs = _inputs()
    outcomes = ray_backend.RayBackend(2, max_in_flight=3).iter_outcomes(
        jobs, data, identity
    )
    assert next(outcomes).result is not None
    assert len(fake_ray.submitted) == 3
    outcomes.close()
    gc.collect()
    assert all(reference() is None for reference in fake_ray.references)
    _assert_cleaned(fake_ray)


def test_pure_submission_and_reference_retention_are_independently_bounded(
    fake_ray: _FakeRay,
) -> None:
    data, identity, jobs = _inputs()
    outcomes = ray_backend.RayBackend(2, max_in_flight=3).iter_outcomes(
        jobs, data, identity
    )
    received: list[str] = []
    try:
        for outcome in outcomes:
            received.append(outcome.job_id)
            assert len(fake_ray.submitted) == min(len(received) + 2, len(jobs))
            gc.collect()
            live_references = sum(
                reference() is not None for reference in fake_ray.references
            )
            assert live_references <= 3
            # Repeated observation while paused cannot trigger new submissions.
            submitted = len(fake_ray.submitted)
            gc.collect()
            assert len(fake_ray.submitted) == submitted
            del outcome
    finally:
        outcomes.close()
    gc.collect()
    assert set(received) == {job.job_id for job in jobs}
    assert len(received) == len(jobs) == len(fake_ray.submitted)
    assert all(reference() is None for reference in fake_ray.references)
    assert fake_ray.dataset_puts == 1
    _assert_cleaned(fake_ray)


class _DyingWorker(ray_backend._SimulationWorker):
    def execute(self, job: JobSpec) -> NoReturn:
        evidence = Path(os.environ["PHASE3_FAULT_DIRECTORY"])
        (evidence / f"death-{os.getpid()}.json").write_text(
            json.dumps({"pid": os.getpid(), "job_id": job.job_id, "exit_code": 23}),
            encoding="utf-8",
        )
        os._exit(23)


def _actor_pids(backend: ray_backend.RayBackend) -> list[int]:
    workers = backend.diagnostics["workers"]
    assert isinstance(workers, list)
    pids: list[int] = []
    for worker in workers:
        assert isinstance(worker, dict)
        pid = worker["pid"]
        assert isinstance(pid, int)
        pids.append(pid)
    assert len(pids) == len(set(pids)) == 2
    return pids


def _process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _real_fault(mode: str, directory: Path, source: str) -> None:
    """Child entry point: run actual actors and verify owned process cleanup."""
    ray = importlib.import_module("ray")
    assert ray.__version__ == ray_backend.RAY_VERSION
    assert not ray.is_initialized()
    data, identity, jobs = _inputs()
    assert identity.source_sha256 == source, "source changed before worker startup"
    backend = ray_backend.RayBackend(2, max_in_flight=3)
    os.environ["PHASE3_FAULT_DIRECTORY"] = str(directory)
    services: list[subprocess.Popen[bytes]] = []
    original_shutdown = ray.shutdown

    def record_shutdown() -> None:
        worker = importlib.import_module("ray._private.worker")
        node = getattr(worker, "_global_node", None)
        for infos in getattr(node, "all_processes", {}).values():
            services.extend(
                cast(subprocess.Popen[bytes], info.process) for info in infos
            )
        original_shutdown()

    output = directory / "incomplete"
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(ray, "shutdown", record_shutdown)
        try:
            if mode == "actor-death":
                monkeypatch.setattr(ray_backend, "_SimulationWorker", _DyingWorker)
                with pytest.raises(RuntimeError, match="Ray execution failed") as error:
                    collect_experiment(jobs, data, identity, output, backend)
                assert isinstance(error.value.__cause__, ray.exceptions.RayActorError)
                assert error.value.__cause__.actor_init_failed is False
                assert "workers" in backend.diagnostics
                evidence = [
                    json.loads(path.read_text()) for path in directory.glob("death-*.json")
                ]
                assert evidence, "worker never reached the intentional os._exit"
                assert all(record["exit_code"] == 23 for record in evidence)
                assert {record["pid"] for record in evidence} <= set(_actor_pids(backend))
            else:
                assert mode == "export-failure"
                exported = 0

                def fail_export(
                    text: str, path: Path, *, overwrite: bool = False
                ) -> None:
                    nonlocal exported
                    if path.parent.name == "results":
                        assert ray.is_initialized()
                        assert all(_process_exists(pid) for pid in _actor_pids(backend))
                        if exported == 1:
                            raise OSError("injected real result export failure")
                        exported += 1
                    write_text(text, path, overwrite=overwrite)

                monkeypatch.setattr(
                    "distributed_backtester.experiments.write_text", fail_export
                )
                with pytest.raises(OSError, match="real result export failure"):
                    collect_experiment(jobs, data, identity, output, backend)
                assert len(list((output / "results").glob("*.json"))) == 1
        finally:
            if ray.is_initialized():
                ray_backend._shutdown_ray(ray)
    assert not ray.is_initialized()
    assert (output / "manifest.json").is_file()
    assert not (output / "index.json").exists()
    assert not (output / "execution.json").exists()
    assert backend.diagnostics["cleanup_errors"] == []
    assert services and all(process.poll() is not None for process in services)
    pids = _actor_pids(backend)
    deadline = monotonic() + 10
    while any(_process_exists(pid) for pid in pids):
        assert monotonic() < deadline, f"experiment worker process leaked: {pids}"
        sleep(0.05)
    assert current_implementation() == identity, (
        "source changed during worker execution"
    )
    record = {
        "mode": mode,
        "source_sha256": source,
        "actor_pids": pids,
        "driver_disconnected": True,
        "index_exists": False,
        "owned_services": [
            {"pid": process.pid, "exit_code": process.poll()} for process in services
        ],
    }
    (directory / "fault-report.json").write_text(json.dumps(record), encoding="utf-8")
    gc.collect()


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX process-group fault harness")
@pytest.mark.parametrize("mode", ["actor-death", "export-failure"])
def test_real_fault_cleans_owned_workers_and_services(
    tmp_path: Path, mode: str
) -> None:
    pytest.importorskip("ray")
    log = tmp_path / "fault.log"
    with log.open("wb") as stream:
        child = subprocess.Popen(
            [
                sys.executable,
                "-W",
                "error",
                str(_SCRIPT),
                mode,
                str(tmp_path),
                current_implementation().source_sha256,
            ],
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            cwd=tmp_path,
        )
        try:
            assert child.wait(timeout=120) == 0, log.read_text()
        finally:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait(timeout=10)
    assert "ResourceWarning" not in log.read_text()
    assert "Exception ignored" not in log.read_text()
    report = json.loads((tmp_path / "fault-report.json").read_text())
    assert report["mode"] == mode
    assert report["driver_disconnected"] is True
    assert report["index_exists"] is False


if __name__ == "__main__":
    _real_fault(sys.argv[1], Path(sys.argv[2]), sys.argv[3])

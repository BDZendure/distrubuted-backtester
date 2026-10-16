"""Real local cluster tests, including drivers that do not own the cluster.

Ray 2.59.0's ``ray.cluster_utils.Cluster`` is a version-bound test helper.
Its Node/process attributes are private APIs used only for owned cleanup and
evidence. Revisit this harness when updating the optional Ray pin. These nodes
share one physical host; this is not validation on multiple machines.

The cluster owner runs outside pytest in its own process group. Backend drivers
run in separate subprocesses because RayBackend refuses initialized drivers.
Pytest, the owner, and backend drivers keep warnings as errors. Ray auxiliary
services retain their normal environment, matching the production backend.
Reaper stdin is retained until AFTER
cluster shutdown and process exit: closing a live pipe kills its fate group.
To run an owner directly (also outside pytest), after freezing package sources:
``.venv/bin/python -W error tests/test_ray_cluster.py owner 2 /tmp/ray-cluster``.
"""

import gc
import importlib
import json
import os
import subprocess
import sys
from collections.abc import Generator, Sequence
from dataclasses import asdict
from pathlib import Path
from time import monotonic, sleep
from types import ModuleType
from typing import Any, cast

import pytest

from distributed_backtester.backends.base import ExecutionBackend
from distributed_backtester.backends.process import ProcessBackend
from distributed_backtester.backends.ray import RAY_VERSION, RayBackend, _shutdown_ray
from distributed_backtester.backends.sequential import SequentialBackend
from distributed_backtester.benchmark import semantic_digest, verify_trial_identity
from distributed_backtester.data import load_csv
from distributed_backtester.experiments import collect_experiment
from distributed_backtester.jobs import JobSpec, create_job
from distributed_backtester.models import BacktestConfig, MarketDataset, StrategyConfig
from distributed_backtester.provenance import (
    ImplementationIdentity,
    current_implementation,
)
from distributed_backtester.worker import JobOutcome

_STORE_BYTES = 134_217_728
_WAIT_SECONDS = 60.0
_SCRIPT = Path(__file__).resolve()
_REPOSITORY = _SCRIPT.parents[1]


def _write_record(path: Path, record: dict[str, object]) -> None:
    temporary = path.with_suffix(".partial")
    temporary.write_text(json.dumps(record, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def _read_record(path: Path) -> dict[str, Any]:
    record = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(record, dict), f"invalid record: {path}"
    return cast(dict[str, Any], record)


def _stop_child(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def _wait_for_path(path: Path, process: subprocess.Popen[bytes] | None = None) -> None:
    deadline = monotonic() + _WAIT_SECONDS
    while not path.exists():
        if process is not None and process.poll() is not None:
            raise AssertionError(f"child exited {process.returncode} before {path}")
        if monotonic() >= deadline:
            raise TimeoutError(f"timed out waiting for {path}")
        sleep(0.05)


class _PausedBackend(RayBackend):
    """Pause at a real outcome so the owner can inspect live reservations."""

    def __init__(self, workers: int, address: str, directory: Path) -> None:
        super().__init__(
            workers,
            address=address,
            max_in_flight=2,
            resources={"backtest": 1},
            startup_timeout_seconds=_WAIT_SECONDS,
        )
        self.directory = directory

    def iter_outcomes(
        self,
        jobs: Sequence[JobSpec],
        dataset: MarketDataset,
        implementation: ImplementationIdentity,
    ) -> Generator[JobOutcome, None, None]:
        outcomes = super().iter_outcomes(jobs, dataset, implementation)
        try:
            for index, outcome in enumerate(outcomes):
                if index == 0:
                    _write_record(self.directory / "ready.json", self.diagnostics)
                    _wait_for_path(self.directory / "release.json")
                yield outcome
        finally:
            outcomes.close()


def _driver(
    address: str, workers: int, mode: str, directory: Path, source: str
) -> None:
    ray = importlib.import_module("ray")
    assert ray.__version__ == RAY_VERSION
    assert not ray.is_initialized()
    implementation = current_implementation()
    assert implementation.source_sha256 == source, (
        "sources changed before child startup"
    )
    data = load_csv(_REPOSITORY / "examples/sma.csv", symbol="SYNTH", price_basis="raw")
    jobs = [
        create_job(
            BacktestConfig(strategy=StrategyConfig(fast, slow), fee_bps=10),
            data.metadata,
            implementation,
        )
        for fast, slow in [(1, 3), (2, 3), (1, 4), (2, 4)]
    ]
    record: dict[str, object] = {"mode": mode, "source_sha256": source}
    if mode == "resource-error":
        # Aggregate resources are misleading with two nodes: each actor needs
        # two backtest units, but neither node can fit even one complete slot.
        backend = RayBackend(
            workers,
            address=address,
            resources={"backtest": 2},
            startup_timeout_seconds=_WAIT_SECONDS,
        )
        output = directory / "incomplete"
        with pytest.raises(
            RuntimeError, match=r"cannot fit.*workers/resources"
        ) as error:
            collect_experiment(jobs, data, implementation, output, backend)
        assert (output / "manifest.json").is_file()
        assert not (output / "index.json").exists()
        assert not (output / "results").exists()
        record.update(error=str(error.value), index_exists=False)
    else:
        backend = _PausedBackend(workers, address, directory)
        if mode == "success":
            collect_experiment(jobs, data, implementation, directory / "ray", backend)
            digests = {"ray": semantic_digest(directory / "ray")}
            references: list[tuple[str, ExecutionBackend]] = [
                ("sequential", SequentialBackend()),
                ("process", ProcessBackend(workers)),
            ]
            for name, reference in references:
                output = directory / name
                collect_experiment(jobs, data, implementation, output, reference)
                verify_trial_identity(output, asdict(implementation))
                digests[name] = semantic_digest(output)
            verify_trial_identity(directory / "ray", asdict(implementation))
            assert len(set(digests.values())) == 1, digests
            record["semantic_digests"] = digests
        else:
            assert mode == "early-close"
            outcomes = backend.iter_outcomes(jobs, data, implementation)
            try:
                assert next(outcomes).result is not None
            finally:
                outcomes.close()
            record["yielded_outcomes"] = 1
        assert backend.diagnostics["local_owned"] is False
        assert backend.diagnostics["cleanup_errors"] == []
        assert backend.max_observed_in_flight <= 2
        record["diagnostics"] = backend.diagnostics
    assert not ray.is_initialized(), "backend left the child connected"
    assert current_implementation() == implementation, (
        "sources changed during execution"
    )
    record["driver_disconnected"] = True
    _write_record(directory / "result.json", record)
    gc.collect()


def _probe_node_id() -> str:
    ray = importlib.import_module("ray")
    return cast(str, ray.get_runtime_context().get_node_id())


def _probe(ray: ModuleType, node_ids: set[str]) -> list[str]:
    affinity = importlib.import_module("ray.util.scheduling_strategies")
    task = ray.remote(num_cpus=0, max_retries=0)(_probe_node_id)
    refs = [
        task.options(
            scheduling_strategy=affinity.NodeAffinitySchedulingStrategy(
                node_id, soft=False
            )
        ).remote()
        for node_id in sorted(node_ids)
    ]
    actual = cast(list[str], ray.get(refs, timeout=_WAIT_SECONDS))
    assert set(actual) == node_ids, actual
    return actual


def _resources_restored(ray: ModuleType, slots: int) -> dict[str, float]:
    deadline = monotonic() + _WAIT_SECONDS
    while True:
        available = cast(dict[str, float], ray.available_resources())
        if available.get("CPU", 0) == slots and available.get("backtest", 0) == slots:
            return available
        if monotonic() >= deadline:
            raise AssertionError(f"actor reservations leaked: {available}")
        sleep(0.05)


def _owner(nodes: int, directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    ray = importlib.import_module("ray")
    assert ray.__version__ == RAY_VERSION
    assert not ray.is_initialized()
    cluster = importlib.import_module("ray.cluster_utils").Cluster()
    source = current_implementation().source_sha256
    report: dict[str, object] = {
        "ray_version": ray.__version__,
        "physical_machines": 1,
        "requested_nodes": nodes,
        "source_sha256": source,
    }
    processes: list[subprocess.Popen[bytes]] = []
    reapers: list[subprocess.Popen[bytes]] = []
    try:
        # Let Ray choose the node address: Linux drivers resolve loopback GCS
        # addresses to the host IP, so forcing loopback would prevent attachment.
        for _ in range(nodes):
            cluster.add_node(
                num_cpus=1,
                num_gpus=0,
                resources={"backtest": 1},
                object_store_memory=_STORE_BYTES,
                include_dashboard=False,
            )
        cluster.wait_for_nodes(timeout=_WAIT_SECONDS)
        assert cluster.address is not None
        ray.init(
            address=cluster.address,
            include_dashboard=False,
            log_to_driver=False,
            logging_level="ERROR",
        )
        live = [node for node in ray.nodes() if node["Alive"]]
        node_ids = {node["NodeID"] for node in live}
        assert len(node_ids) == nodes, live
        assert all(node["Resources"]["CPU"] == 1 for node in live)
        assert all(node["Resources"]["backtest"] == 1 for node in live)
        report["nodes"] = live
        report["initial_probe_node_ids"] = _probe(ray, node_ids)
        stages = []
        for mode in ("success", "early-close", "resource-error"):
            child_directory = directory / mode
            child_directory.mkdir()
            log = child_directory / "driver.log"
            with log.open("wb") as stream:
                child = subprocess.Popen(
                    [
                        sys.executable,
                        "-W",
                        "error",
                        str(_SCRIPT),
                        "driver",
                        cluster.address,
                        str(nodes),
                        mode,
                        str(child_directory),
                        source,
                    ],
                    stdout=stream,
                    stderr=subprocess.STDOUT,
                    cwd=_REPOSITORY,
                    env=os.environ.copy(),
                )
                try:
                    if mode != "resource-error":
                        _wait_for_path(child_directory / "ready.json", child)
                        ready = _read_record(child_directory / "ready.json")
                        workers = ready["workers"]
                        assert len(workers) == nodes, workers
                        assert {worker["node_id"] for worker in workers} == node_ids, (
                            workers
                        )
                        assert len({worker["pid"] for worker in workers}) == nodes
                        assert all(
                            worker["source_sha256"] == source for worker in workers
                        )
                        assert ready["cpus_per_worker"] == 1
                        assert ready["resources_per_worker"] == {"backtest": 1}
                        available = ray.available_resources()
                        assert available.get("CPU", 0) == 0, available
                        assert available.get("backtest", 0) == 0, available
                    _write_record(child_directory / "release.json", {})
                    assert child.wait(timeout=120) == 0, log.read_text()
                finally:
                    # Release even on an owner assertion failure, allowing the
                    # backend to perform normal actor cleanup before termination.
                    _write_record(child_directory / "release.json", {})
                    _stop_child(child)
            assert "ResourceWarning" not in log.read_text(), log.read_text()
            assert "Exception ignored" not in log.read_text(), log.read_text()
            assert ray.is_initialized(), "child disconnected the owner"
            assert cluster.remaining_processes_alive(), "child killed external services"
            assert {node["NodeID"] for node in ray.nodes() if node["Alive"]} == node_ids
            stage = _read_record(child_directory / "result.json")
            stage["restored_resources"] = _resources_restored(ray, nodes)
            stage["probe_node_ids_after_child"] = _probe(ray, node_ids)
            stage["external_services_alive"] = True
            stages.append(stage)
            report["stages"] = stages
            _write_record(directory / "report.json", report)
    finally:
        # Retain Popen handles BEFORE Cluster.shutdown drops Node.all_processes.
        # Never close reaper stdin while its process is alive.
        for node in cluster.list_all_nodes():
            for kind, infos in node.all_processes.items():
                handles = [info.process for info in infos]
                processes.extend(handles)
                if kind == "reaper":
                    reapers.extend(handles)
        try:
            _shutdown_ray(ray)
        finally:
            cluster.shutdown()
            for process in reapers:
                assert process.poll() is not None, "cluster left a live reaper"
                if process.stdin is not None:
                    process.stdin.close()
        report["owned_processes"] = [
            {"pid": process.pid, "exit_code": process.poll()} for process in processes
        ]
        assert all(process.poll() is not None for process in processes)
        report["owned_cleanup_complete"] = True
        _write_record(directory / "report.json", report)
        gc.collect()


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX process-group test harness")
@pytest.mark.parametrize(
    "nodes", [1, 2], ids=["external-one-node", "external-two-nodes"]
)
def test_real_external_cluster_ownership_and_placement(
    tmp_path: Path, nodes: int
) -> None:
    ray = pytest.importorskip("ray")
    assert ray.__version__ == RAY_VERSION, (
        "review private test APIs when changing Ray pin"
    )
    directory = tmp_path / "cluster"
    log = tmp_path / "owner.log"
    with log.open("wb") as stream:
        owner = subprocess.Popen(
            [
                sys.executable,
                "-W",
                "error",
                str(_SCRIPT),
                "owner",
                str(nodes),
                str(directory),
            ],
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            cwd=_REPOSITORY,
            env=os.environ.copy(),
        )
        try:
            assert owner.wait(timeout=300) == 0, log.read_text()
        finally:
            _stop_child(owner)
    assert "ResourceWarning" not in log.read_text(), log.read_text()
    assert "Exception ignored" not in log.read_text(), log.read_text()
    report = _read_record(directory / "report.json")
    assert report["owned_cleanup_complete"] is True
    assert len(report["initial_probe_node_ids"]) == nodes
    assert [stage["mode"] for stage in report["stages"]] == [
        "success",
        "early-close",
        "resource-error",
    ]
    assert all(stage["external_services_alive"] for stage in report["stages"])
    assert all(stage["driver_disconnected"] for stage in report["stages"])
    assert len(set(report["stages"][0]["semantic_digests"].values())) == 1


if __name__ == "__main__":
    if sys.argv[1] == "owner":
        _owner(int(sys.argv[2]), Path(sys.argv[3]))
    else:
        assert sys.argv[1] == "driver"
        _driver(
            sys.argv[2], int(sys.argv[3]), sys.argv[4], Path(sys.argv[5]), sys.argv[6]
        )

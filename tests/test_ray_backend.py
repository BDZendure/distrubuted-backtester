"""Validation and real remote-worker tests for the optional Ray backend."""

import importlib
import io
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from distributed_backtester.backends.ray import (
    RayBackend,
    _local_cpu_budget,
    _local_resource_budget,
    _shutdown_ray,
)
from distributed_backtester.backends.sequential import SequentialBackend
from distributed_backtester.experiments import collect_experiment
from distributed_backtester.jobs import create_job
from distributed_backtester.models import BacktestConfig, StrategyConfig
from distributed_backtester.provenance import current_implementation
from tests.test_simulation import dataset


def test_local_cpu_budget_is_an_integer_even_with_fractional_workers() -> None:
    assert type(_local_cpu_budget(2, 1.0)) is int
    assert _local_cpu_budget(3, 0.5) == 2


def test_local_custom_capacity_is_integral_for_fractional_requests() -> None:
    assert _local_resource_budget(1, 0.5) == 1
    assert _local_resource_budget(3, 0.5) == 2
    assert _local_resource_budget(3, 0.1) == 1


def test_shutdown_closes_owned_pipe_only_after_reaper_has_exited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipe = io.BytesIO()
    process = SimpleNamespace(stdin=pipe, poll=lambda: 0)
    node = SimpleNamespace(all_processes={"reaper": [SimpleNamespace(process=process)]})
    module = ModuleType("worker")
    module._global_node = node  # type: ignore[attr-defined]
    monkeypatch.setattr(importlib, "import_module", lambda _: module)
    ray = ModuleType("ray")

    def shutdown() -> None:
        assert not pipe.closed
        node.all_processes.clear()

    ray.shutdown = shutdown  # type: ignore[attr-defined]
    _shutdown_ray(ray)
    assert pipe.closed


def test_shutdown_does_not_close_a_live_reaper_pipe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipe = io.BytesIO()
    process = SimpleNamespace(stdin=pipe, poll=lambda: None)
    node = SimpleNamespace(all_processes={"reaper": [SimpleNamespace(process=process)]})
    module = ModuleType("worker")
    module._global_node = node  # type: ignore[attr-defined]
    monkeypatch.setattr(importlib, "import_module", lambda _: module)
    ray = ModuleType("ray")
    ray.shutdown = lambda: None  # type: ignore[attr-defined]
    _shutdown_ray(ray)
    assert not pipe.closed
    pipe.close()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"workers": 0},
        {"workers": True},
        {"workers": 1, "max_in_flight": 0},
        {"workers": 1, "cpus_per_worker": 0},
        {"workers": 1, "cpus_per_worker": float("nan")},
        {"workers": 1, "cpus_per_worker": True},
        {"workers": 1, "cpus_per_worker": 0.00001},
        {"workers": 1, "cpus_per_worker": 0.33333},
        {"workers": 1, "resources": {"CPU": 1}},
        {"workers": 1, "resources": {"x": -1}},
        {"workers": 1, "resources": {"x": float("inf")}},
        {"workers": 1, "resources": {"x": 0.00001}},
        {"workers": 1, "resources": {"": 1}},
        {"workers": 1, "address": "ray://host:10001"},
        {"workers": 1, "address": ""},
        {"workers": 1, "address": "auto", "object_store_memory": 100_000_000},
        {"workers": 1, "object_store_memory": 1},
        {"workers": 1, "startup_timeout_seconds": 0},
    ],
)
def test_rejects_invalid_configuration(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        RayBackend(**kwargs)  # type: ignore[arg-type]


def test_minimum_resource_quantity_is_supported() -> None:
    backend = RayBackend(1, cpus_per_worker=0.0001, resources={"custom": 0.0001})
    assert backend.cpus_per_worker == 0.0001
    assert backend.resources == {"custom": 0.0001}


def test_empty_jobs_do_not_import_or_initialize_ray(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = RayBackend(2)

    def fail(name: str) -> ModuleType:
        raise AssertionError(f"unexpected import {name}")

    monkeypatch.setattr(importlib, "import_module", fail)
    assert (
        list(backend.iter_outcomes([], dataset([10]), current_implementation())) == []
    )


def test_missing_optional_dependency_has_install_guidance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing(name: str) -> ModuleType:
        raise ImportError(name)

    monkeypatch.setattr(importlib, "import_module", missing)
    data = dataset([10, 11, 12, 9, 9])
    identity = current_implementation()
    job = create_job(BacktestConfig(), data.metadata, identity)
    with pytest.raises(RuntimeError, match=r"\[distributed\]"):
        list(RayBackend(1).iter_outcomes([job], data, identity))


def test_wrong_ray_version_is_rejected_before_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = ModuleType("ray")
    module.__version__ = "0.0.0"  # type: ignore[attr-defined]
    monkeypatch.setattr(importlib, "import_module", lambda _: module)
    data = dataset([10])
    identity = current_implementation()
    job = create_job(BacktestConfig(), data.metadata, identity)
    with pytest.raises(RuntimeError, match=r"2\.59\.0"):
        list(RayBackend(1).iter_outcomes([job], data, identity))


@pytest.fixture
def cluster() -> Iterator[tuple[ModuleType, str]]:
    ray = pytest.importorskip("ray")
    context = ray.init(
        address="local",
        num_cpus=4,
        resources={"backtest": 2},
        include_dashboard=False,
        object_store_memory=134_217_728,
        logging_level="ERROR",
    )
    try:
        yield ray, context.address_info["gcs_address"]
    finally:
        _shutdown_ray(ray)


def test_real_workers_equivalent_reused_and_bounded() -> None:
    ray = pytest.importorskip("ray")
    data = dataset([10, 11, 12, 9, 9])
    identity = current_implementation()
    jobs = [
        create_job(
            BacktestConfig(strategy=StrategyConfig(fast, slow)), data.metadata, identity
        )
        for fast, slow in [(1, 3), (2, 3), (1, 4), (2, 4)]
    ]
    backend = RayBackend(2, max_in_flight=1, resources={"backtest": 1})
    expected = list(SequentialBackend().iter_outcomes(jobs, data, identity))
    actual = list(backend.iter_outcomes(jobs, data, identity))
    assert sorted(actual, key=lambda item: item.job_id) == sorted(
        expected, key=lambda item: item.job_id
    )
    assert backend.max_observed_in_flight == 1
    assert backend.diagnostics["local_owned"] is True
    workers = backend.diagnostics["workers"]
    assert isinstance(workers, list)
    assert len({worker["pid"] for worker in workers}) == 2
    assert all(worker["initialized_once"] is True for worker in workers)
    assert sum(worker["jobs_completed"] for worker in workers) == 4
    assert not ray.is_initialized()


def test_real_mixed_failures_and_early_close() -> None:
    ray = pytest.importorskip("ray")
    data = dataset([10, 11, 12, 9, 9])
    identity = current_implementation()
    valid = create_job(BacktestConfig(), data.metadata, identity)
    invalid = create_job(
        BacktestConfig(), replace(data.metadata, symbol="OTHER"), identity
    )
    backend = RayBackend(1, max_in_flight=1)
    outcomes = list(backend.iter_outcomes([valid, invalid], data, identity))
    assert outcomes[0].result is not None
    assert outcomes[1].failure is not None
    assert outcomes[1].failure.error_type == "DatasetMismatch"
    iterator = backend.iter_outcomes([valid, invalid], data, identity)
    assert next(iterator).result is not None
    iterator.close()
    assert not ray.is_initialized()


@pytest.mark.parametrize("workers", [1, 3])
def test_real_fractional_cpu_and_custom_resources(workers: int) -> None:
    ray = pytest.importorskip("ray")
    data = dataset([10, 11, 12, 9, 9])
    identity = current_implementation()
    job = create_job(BacktestConfig(), data.metadata, identity)
    backend = RayBackend(workers, cpus_per_worker=0.5, resources={"custom": 0.5})
    assert list(backend.iter_outcomes([job], data, identity)) == list(
        SequentialBackend().iter_outcomes([job], data, identity)
    )
    assert not ray.is_initialized()


def test_real_initialization_mismatch_has_no_complete_index(tmp_path: Path) -> None:
    ray = pytest.importorskip("ray")
    data = dataset([10, 11, 12])
    identity = replace(current_implementation(), source_sha256="b" * 64)
    job = create_job(BacktestConfig(), data.metadata, identity)
    with pytest.raises(RuntimeError, match="implementation"):
        collect_experiment(
            [job], data, identity, tmp_path / "experiment", RayBackend(1)
        )
    assert not (tmp_path / "experiment" / "index.json").exists()
    assert not ray.is_initialized()


def test_refuses_to_take_over_initialized_driver(
    cluster: tuple[ModuleType, str],
) -> None:
    ray, _ = cluster
    if not ray.is_initialized():
        ray.init(
            address="local",
            num_cpus=1,
            include_dashboard=False,
            object_store_memory=134_217_728,
            logging_level="ERROR",
        )
    data = dataset([10])
    identity = current_implementation()
    job = create_job(BacktestConfig(), data.metadata, identity)
    with pytest.raises(RuntimeError, match="already initialized"):
        list(RayBackend(1).iter_outcomes([job], data, identity))
    assert ray.is_initialized()

"""Optional fixed Ray actor pool using the existing deterministic worker."""

import importlib
import math
import os
import sys
from collections.abc import Generator, Mapping, Sequence
from time import perf_counter
from types import ModuleType
from typing import Any, Protocol, cast

from distributed_backtester.jobs import JobSpec
from distributed_backtester.models import MarketDataset
from distributed_backtester.money import require_integer
from distributed_backtester.provenance import (
    ImplementationIdentity,
    current_implementation,
)
from distributed_backtester.worker import JobOutcome, execute_job

RAY_VERSION = "2.59.0"
DEFAULT_STORE_BYTES = 134_217_728
MAX_RESOURCE_QUANTITY = 100_000_000_000_000


class _ReadyMethod(Protocol):
    def remote(self) -> object: ...


class _ExecuteMethod(Protocol):
    def remote(self, job: JobSpec) -> object: ...


class _Actor(Protocol):
    @property
    def ready(self) -> _ReadyMethod: ...

    @property
    def execute(self) -> _ExecuteMethod: ...


class _ActorClass(Protocol):
    def remote(
        self, dataset: object, implementation: ImplementationIdentity
    ) -> _Actor: ...


def _positive(value: float, name: str) -> float:
    if type(value) not in (int, float):
        raise ValueError(f"{name} must be a finite positive number")
    try:
        number = float(value)
    except (OverflowError, ValueError) as error:
        raise ValueError(f"{name} must be a finite positive number") from error
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{name} must be a finite positive number")
    return number


def _resource_quantity(value: float, name: str) -> float:
    number = _positive(value, name)
    if not 0.0001 <= number <= MAX_RESOURCE_QUANTITY:
        raise ValueError(f"{name} is outside Ray's supported resource range")
    scaled = number * 10_000
    if not math.isclose(scaled, round(scaled), rel_tol=0, abs_tol=1e-7):
        raise ValueError(f"{name} must have at most four decimal places")
    return number


def _require_ray() -> ModuleType:
    try:
        ray = importlib.import_module("ray")
    except ImportError as error:
        raise RuntimeError(
            "Ray execution requires pip install '.[distributed]'"
        ) from error
    if ray.__version__ != RAY_VERSION:
        raise RuntimeError(f"Ray execution requires Ray {RAY_VERSION}")
    return ray


def _local_cpu_budget(workers: int, cpus_per_worker: float) -> int:
    # ray.init documents an integer CPU count. Ray 2.59.0 on this platform
    # stalls before raylet registration when passed 2.0 instead of 2.
    return _local_resource_budget(workers, cpus_per_worker)


def _local_resource_budget(workers: int, quantity: float) -> int:
    try:
        budget = workers * quantity
    except OverflowError as error:
        raise ValueError(
            "local Ray capacity exceeds the supported resource range"
        ) from error
    if not math.isfinite(budget) or budget > MAX_RESOURCE_QUANTITY:
        raise ValueError("local Ray capacity exceeds the supported resource range")
    return math.ceil(budget)


def _shutdown_ray(ray: ModuleType) -> None:
    """Close the pinned release's reaper pipe after Ray terminates the process.

    Ray 2.59.0 drops its Popen without closing stdin on macOS. Retain the
    handle through shutdown so finalization cannot leak it. This version-bound
    private API adapter must be reviewed when changing the optional Ray pin.
    Never close a live reaper pipe, which would trigger fate sharing.
    """
    worker = importlib.import_module("ray._private.worker")
    node = getattr(worker, "_global_node", None)
    handles = [
        info.process for info in getattr(node, "all_processes", {}).get("reaper", [])
    ]
    try:
        ray.shutdown()
    finally:
        for process in handles:
            if process.poll() is not None and process.stdin is not None:
                process.stdin.close()


class _SimulationWorker:
    """An actor holds one decoded dataset; strategy state is created per job."""

    def __init__(
        self, dataset: MarketDataset, implementation: ImplementationIdentity
    ) -> None:
        if current_implementation() != implementation:
            raise RuntimeError(
                "installed implementation differs in Ray worker initialization"
            )
        self.dataset = dataset
        self.implementation = implementation
        self.jobs_completed = 0

    def ready(self) -> dict[str, object]:
        return {
            "pid": os.getpid(),
            "node_id": _require_ray().get_runtime_context().get_node_id(),
            "source_sha256": self.implementation.source_sha256,
            "dataset_sha256": self.dataset.metadata.source_sha256,
            "initialized_once": True,
            "jobs_completed": self.jobs_completed,
        }

    def execute(self, job: JobSpec) -> JobOutcome:
        outcome = execute_job(job, self.dataset, self.implementation)
        self.jobs_completed += 1
        return outcome


def _capacity(nodes: Sequence[Mapping[str, Any]], requests: Mapping[str, float]) -> int:
    """Count complete actor slots per live node, not just summed resources."""
    slots = 0
    for node in nodes:
        if node.get("Alive"):
            resources = node.get("Resources", {})
            slots += min(
                math.floor(float(resources.get(name, 0)) / amount + 1e-9)
                for name, amount in requests.items()
            )
    return slots


class _RayPool:
    """Own a driver connection, shared dataset and only this pool's actors."""

    def __init__(
        self,
        backend: "RayBackend",
        dataset: MarketDataset,
        implementation: ImplementationIdentity,
        *,
        worker_type: type[_SimulationWorker] | None = None,
    ) -> None:
        self.backend = backend
        self.dataset = dataset
        self.implementation = implementation
        self.worker_type = worker_type
        self.ray: ModuleType
        self.actor_class: _ActorClass
        self.shared_dataset: object = None
        self.actors: list[_Actor] = []
        self.timings: dict[str, float] = {}
        self.cleanup_errors: list[str] = []
        self.diagnostics: dict[str, object] = {
            "timings": self.timings,
            "cleanup_errors": self.cleanup_errors,
        }
        self._owns_connection = False
        self._closed = False

    def start(self) -> None:
        backend = self.backend
        ray = self.ray = _require_ray()
        if ray.is_initialized():
            raise RuntimeError(
                "Ray driver is already initialized; use a separate driver"
            )
        local_owned = backend.address in (None, "local")
        start = perf_counter()
        options: dict[str, object] = {
            "address": "local" if local_owned else backend.address,
            "include_dashboard": False,
            "log_to_driver": False,
            "logging_level": "ERROR",
        }
        if local_owned:
            options.update(
                num_cpus=_local_cpu_budget(backend.workers, backend.cpus_per_worker),
                num_gpus=0,
                resources={
                    key: _local_resource_budget(backend.workers, amount)
                    for key, amount in backend.resources.items()
                },
                object_store_memory=backend.object_store_memory or DEFAULT_STORE_BYTES,
            )
        # Own partial initialization too; never take over an existing driver.
        self._owns_connection = True
        context = ray.init(**options)
        self.timings["startup_seconds"] = perf_counter() - start
        nodes = ray.nodes()
        requests = {"CPU": backend.cpus_per_worker, **backend.resources}
        if _capacity(nodes, requests) < backend.workers:
            raise RuntimeError(
                "live cluster nodes cannot fit the requested Ray workers/resources"
            )
        self.diagnostics.update(
            local_owned=local_owned,
            ray_version=RAY_VERSION,
            session_dir=context.address_info.get("session_dir"),
            address=context.address_info.get("gcs_address"),
            cpus_per_worker=backend.cpus_per_worker,
            resources_per_worker=backend.resources.copy(),
            max_restarts=0,
            max_task_retries=0,
            nodes=[
                {
                    "node_id": node["NodeID"],
                    "address": node["NodeManagerAddress"],
                    "resources": node["Resources"],
                }
                for node in nodes
                if node.get("Alive")
            ],
            object_store_capacity_bytes=sum(
                int(node["Resources"].get("object_store_memory", 0))
                for node in nodes
                if node.get("Alive")
            ),
        )
        putting = perf_counter()
        self.diagnostics["dataset_serialized_bytes"] = len(
            importlib.import_module("ray.cloudpickle").dumps(self.dataset)
        )
        self.shared_dataset = ray.put(self.dataset)
        self.timings["dataset_put_seconds"] = perf_counter() - putting
        creating = perf_counter()
        self.actor_class = cast(
            _ActorClass,
            ray.remote(self.worker_type or _SimulationWorker).options(
                num_cpus=backend.cpus_per_worker,
                resources=backend.resources,
                max_restarts=0,
                max_task_retries=0,
                scheduling_strategy="SPREAD",
            ),
        )
        for _ in range(backend.workers):
            self.create_actor()
        self.diagnostics["workers"] = ray.get(
            [actor.ready.remote() for actor in self.actors],
            timeout=backend.startup_timeout_seconds,
        )
        self.timings["actors_ready_seconds"] = perf_counter() - creating

    def create_actor(self) -> _Actor:
        actor = self.actor_class.remote(self.shared_dataset, self.implementation)
        self.actors.append(actor)
        return actor

    def retire(self, actor: _Actor) -> None:
        self.ray.kill(actor, no_restart=True)
        self.actors.remove(actor)

    def close(self, *, primary_error: BaseException | None = None) -> None:
        if self._closed:
            return
        self._closed = True
        if not self._owns_connection:
            return
        stopping = perf_counter()
        for actor in self.actors:
            try:
                self.ray.kill(actor, no_restart=True)
            except Exception as error:
                self.cleanup_errors.append(f"{type(error).__name__}: {error}")
        self.actors.clear()
        self.shared_dataset = None
        try:
            _shutdown_ray(self.ray)
        except Exception as error:
            self.cleanup_errors.append(f"{type(error).__name__}: {error}")
        finally:
            self._owns_connection = False
            self.timings["shutdown_seconds"] = perf_counter() - stopping
        if self.cleanup_errors:
            message = "Ray cleanup failed: " + "; ".join(self.cleanup_errors)
            if primary_error is not None:
                raise RuntimeError(
                    f"{type(primary_error).__name__}: {primary_error}; {message}"
                ) from primary_error
            raise RuntimeError(message)


class RayBackend:
    name = "ray"

    def __init__(
        self,
        workers: int,
        *,
        max_in_flight: int | None = None,
        address: str | None = None,
        cpus_per_worker: float = 1.0,
        resources: Mapping[str, float] | None = None,
        object_store_memory: int | None = None,
        startup_timeout_seconds: float = 60.0,
    ) -> None:
        require_integer(workers, "workers", minimum=1)
        self.workers = workers
        self.max_in_flight = 2 * workers if max_in_flight is None else max_in_flight
        require_integer(self.max_in_flight, "max_in_flight", minimum=1)
        self.cpus_per_worker = _resource_quantity(cpus_per_worker, "cpus_per_worker")
        self.resources: dict[str, float] = {}
        for key, value in (resources or {}).items():
            if (
                not isinstance(key, str)
                or not key.strip()
                or key != key.strip()
                or key in {"CPU", "GPU", "memory", "object_store_memory"}
                or key.startswith("node:")
            ):
                raise ValueError("worker resources need nonreserved, nonblank names")
            self.resources[key] = _resource_quantity(value, f"resource {key}")
        if address is not None and (
            not isinstance(address, str)
            or not address.strip()
            or address != address.strip()
            or address.startswith("ray://")
        ):
            raise ValueError(
                "ray address must be a GCS address or auto, not Ray Client"
            )
        self.address = address
        if address in (None, "local"):
            _local_cpu_budget(workers, self.cpus_per_worker)
            for amount in self.resources.values():
                _local_resource_budget(workers, amount)
        if object_store_memory is not None:
            require_integer(
                object_store_memory, "object_store_memory", minimum=80 * 1024**2
            )
            if address not in (None, "local"):
                raise ValueError(
                    "object_store_memory is only configurable for a local cluster"
                )
        self.object_store_memory = object_store_memory
        self.startup_timeout_seconds = _positive(
            startup_timeout_seconds, "startup_timeout_seconds"
        )
        self.max_observed_in_flight = 0
        self.diagnostics: dict[str, object] = {}

    def iter_outcomes(
        self,
        jobs: Sequence[JobSpec],
        dataset: MarketDataset,
        implementation: ImplementationIdentity,
    ) -> Generator[JobOutcome, None, None]:
        self.max_observed_in_flight = 0
        self.diagnostics = {}
        if not jobs:
            return
        pool = _RayPool(self, dataset, implementation)
        self.diagnostics = pool.diagnostics
        pending: dict[object, str] = {}
        try:
            pool.start()
            ray = pool.ray
            actors = pool.actors
            iterator = iter(jobs)
            exhausted = False
            next_actor = 0
            while pending or not exhausted:
                while len(pending) < self.max_in_flight and not exhausted:
                    job = next(iterator, None)
                    if job is None:
                        exhausted = True
                    else:
                        ref = actors[next_actor].execute.remote(job)
                        next_actor = (next_actor + 1) % self.workers
                        pending[ref] = job.job_id
                        self.max_observed_in_flight = max(
                            self.max_observed_in_flight, len(pending)
                        )
                if not pending:
                    break
                ready, _ = ray.wait(list(pending), num_returns=1)
                ref = ready[0]
                expected_id = pending.pop(ref)
                outcome = cast(JobOutcome, ray.get(ref))
                if not isinstance(outcome, JobOutcome) or outcome.job_id != expected_id:
                    raise RuntimeError("worker outcome does not match submitted job")
                yield outcome
                del outcome, ready, ref
            self.diagnostics["workers"] = ray.get(
                [actor.ready.remote() for actor in actors],
                timeout=self.startup_timeout_seconds,
            )
        except Exception as error:
            raise RuntimeError(f"Ray execution failed: {error}") from error
        finally:
            pending.clear()
            primary_error = sys.exception()
            if isinstance(primary_error, GeneratorExit):
                # The collector closes this generator while unwinding an
                # export failure. GeneratorExit carries that outer error in
                # its context; include it if pool cleanup also fails.
                primary_error = primary_error.__context__
            pool.close(primary_error=primary_error)

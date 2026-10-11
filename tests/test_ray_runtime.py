"""Pure transport tests for bounded Ray attempts; no real Ray import/startup."""

import gc
import importlib
import weakref
from collections.abc import Callable, Iterator
from dataclasses import replace
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING, Protocol, cast

import pytest

from distributed_backtester.backends import ray as ray_backend
from distributed_backtester.jobs import JobSpec, create_job
from distributed_backtester.models import BacktestConfig, MarketDataset
from distributed_backtester.provenance import (
    ImplementationIdentity,
    current_implementation,
)
from distributed_backtester.worker import JobFailure, execute_job
from tests.test_simulation import dataset

if TYPE_CHECKING:
    from distributed_backtester.scheduler.models import AttemptReply, AttemptSpec
    from distributed_backtester.scheduler.ray_runtime import RayAttemptRuntime


class _RayError(Exception):
    actor_init_failed = False


class _InitializationError(_RayError):
    actor_init_failed = True


class _RayTaskError(_RayError):
    def __init__(self, cause: Exception) -> None:
        self.cause = cause
        super().__init__(f"remote task failed: {type(cause).__name__}: {cause}")


class _WrappedValueError(_RayTaskError, ValueError):
    """Ray get raises an exception inheriting both task and cause types."""


class _ActorError(_RayError):
    pass


class _ResourceError(_RayError):
    pass


class _ObjectLostError(_RayError):
    pass


class _Reference:
    def __init__(self, value: object, *, complete: bool = True) -> None:
        self.value = value
        self.complete = complete


class _Ready:
    def __init__(self, actor: "_Actor") -> None:
        self.actor = actor

    def remote(self) -> _Reference:
        value: object = self.actor.worker.ready()
        if self.actor.transport.ready_error is not None:
            value = self.actor.transport.ready_error
        reference = self.actor.transport.reference(
            value, complete=self.actor.transport.complete_readiness
        )
        self.actor.transport.readiness.append(weakref.ref(reference))
        return reference


class _Execute:
    def __init__(self, actor: "_Actor") -> None:
        self.actor = actor

    def remote(self, attempt: "AttemptSpec") -> _Reference:
        if self.actor.transport.submit_error is not None:
            raise self.actor.transport.submit_error
        value = self.actor.worker.execute_attempt(attempt)
        reference = self.actor.transport.reference(
            value, complete=self.actor.transport.complete_tasks
        )
        self.actor.transport.submitted.append((self.actor, attempt))
        self.actor.transport.tasks.append(weakref.ref(reference))
        return reference


class _Actor:
    def __init__(self, worker: object, transport: "_Ray") -> None:
        self.worker = cast("_Worker", worker)
        self.transport = transport
        self.ready = _Ready(self)
        self.execute_attempt = _Execute(self)


class _Worker(Protocol):
    """Typing surface only; fake Ray instantiates the real worker class."""

    def ready(self) -> dict[str, object]:
        raise NotImplementedError

    def execute_attempt(self, attempt: "AttemptSpec") -> object:
        raise NotImplementedError


class _ActorClass:
    def __init__(self, transport: "_Ray", worker_type: type[object]) -> None:
        self.transport = transport
        self.worker_type = worker_type

    def options(self, **options: object) -> "_ActorClass":
        self.transport.actor_options = options
        return self

    def remote(
        self, data: MarketDataset, implementation: ImplementationIdentity
    ) -> _Actor:
        if self.transport.create_error is not None:
            raise self.transport.create_error
        factory = cast(
            Callable[[MarketDataset, ImplementationIdentity], _Worker], self.worker_type
        )
        actor = _Actor(factory(data, implementation), self.transport)
        self.transport.actors.append(actor)
        return actor


class _Ray(ModuleType):
    def __init__(self) -> None:
        super().__init__("ray")
        self.exceptions = SimpleNamespace(
            RayError=_RayError,
            RayTaskError=_RayTaskError,
            RayActorError=_ActorError,
            WorkerCrashedError=_ActorError,
            LocalRayletDiedError=_ActorError,
            NodeDiedError=_ActorError,
            ObjectStoreFullError=_ResourceError,
            OutOfDiskError=_ResourceError,
            OutOfMemoryError=_ResourceError,
            ObjectLostError=_ObjectLostError,
            GetTimeoutError=_ObjectLostError,
            RpcError=_ActorError,
        )
        self.initialized = False
        self.actors: list[_Actor] = []
        self.killed: list[_Actor] = []
        self.submitted: list[tuple[_Actor, object]] = []
        self.tasks: list[weakref.ReferenceType[_Reference]] = []
        self.readiness: list[weakref.ReferenceType[_Reference]] = []
        self.actor_options: dict[str, object] = {}
        self.init_options: dict[str, object] = {}
        self.get_timeouts: list[float | None] = []
        self.wait_timeouts: list[float] = []
        self.shutdowns = 0
        self.dataset_puts = 0
        self.complete_tasks = True
        self.complete_readiness = True
        self.ready_error: Exception | None = None
        self.submit_error: Exception | None = None
        self.create_error: Exception | None = None
        self.kill_error: Exception | None = None
        self.shutdown_error: Exception | None = None
        self.wait_error: Exception | None = None

    def reference(self, value: object, *, complete: bool = True) -> _Reference:
        return _Reference(value, complete=complete)

    def is_initialized(self) -> bool:
        return self.initialized

    def init(self, **options: object) -> SimpleNamespace:
        self.init_options = options
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
                "Resources": {"CPU": 20, "object_store_memory": 134_217_728},
            }
        ]

    def put(self, data: MarketDataset) -> MarketDataset:
        self.dataset_puts += 1
        return data

    def remote(self, worker_type: type[object]) -> _ActorClass:
        return _ActorClass(self, worker_type)

    def get_runtime_context(self) -> SimpleNamespace:
        return SimpleNamespace(get_node_id=lambda: "fake-node")

    def get(
        self, refs: _Reference | list[_Reference], *, timeout: float | None = None
    ) -> object:
        self.get_timeouts.append(timeout)
        if isinstance(refs, list):
            return [self.get(ref, timeout=timeout) for ref in refs]
        assert refs.complete, "attempted blocking get before readiness"
        if isinstance(refs.value, Exception):
            raise refs.value
        return refs.value

    def wait(
        self, refs: list[_Reference], *, num_returns: int, timeout: float
    ) -> tuple[list[_Reference], list[_Reference]]:
        self.wait_timeouts.append(timeout)
        assert num_returns == 1
        if self.wait_error is not None:
            raise self.wait_error
        ready = [ref for ref in refs if ref.complete][:num_returns]
        return ready, [ref for ref in refs if ref not in ready]

    def kill(self, actor: _Actor, *, no_restart: bool) -> None:
        assert no_restart
        if self.kill_error is not None:
            raise self.kill_error
        self.killed.append(actor)

    def shutdown(self) -> None:
        self.shutdowns += 1
        self.initialized = False
        if self.shutdown_error is not None:
            raise self.shutdown_error


@pytest.fixture
def transport(monkeypatch: pytest.MonkeyPatch) -> _Ray:
    ray = _Ray()
    original_import = importlib.import_module
    pickler = ModuleType("ray.cloudpickle")
    pickler.dumps = lambda _: b"serialized data"  # type: ignore[attr-defined]

    def import_module(name: str, package: str | None = None) -> ModuleType:
        if name == "ray.cloudpickle":
            return pickler
        if name == "ray._private.worker":
            return ModuleType(name)
        if name == "ray" or name.startswith("ray."):
            raise AssertionError(f"pure test imported real Ray: {name}")
        return original_import(name, package)

    monkeypatch.setattr(ray_backend, "_require_ray", lambda: ray)
    monkeypatch.setattr(importlib, "import_module", import_module)
    return ray


def _inputs() -> tuple[MarketDataset, ImplementationIdentity, list[JobSpec]]:
    data = dataset([10, 11, 12, 9, 9])
    identity = current_implementation()
    jobs = [
        create_job(BacktestConfig(fee_bps=fee), data.metadata, identity)
        for fee in range(4)
    ]
    return data, identity, jobs


def _attempt(job: JobSpec, *, number: int = 1, generation: int = 1) -> "AttemptSpec":
    from distributed_backtester.scheduler.models import AttemptSpec

    return AttemptSpec(job, number, generation, f"{job.job_id}:{number}", 100.0)


def _runtime(backend: ray_backend.RayBackend) -> "RayAttemptRuntime":
    from distributed_backtester.scheduler.ray_runtime import RayAttemptRuntime

    data, identity, _ = _inputs()
    return RayAttemptRuntime(backend, data, identity)


@pytest.fixture
def runtime(transport: _Ray) -> Iterator["RayAttemptRuntime"]:
    value = _runtime(ray_backend.RayBackend(2, max_in_flight=4))
    value.start()
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def single_worker_runtime(transport: _Ray) -> Iterator["RayAttemptRuntime"]:
    value = _runtime(ray_backend.RayBackend(1))
    value.start()
    try:
        yield value
    finally:
        value.close()


def _submit_unready_replacement(
    runtime: "RayAttemptRuntime", transport: _Ray
) -> "AttemptSpec":
    _, _, jobs = _inputs()
    runtime.submit(_attempt(jobs[0]))
    runtime.cancel(jobs[0].job_id + ":1")
    transport.complete_readiness = False
    attempt = _attempt(jobs[0], number=2)
    runtime.submit(attempt)
    return attempt


def test_lazy_single_use_start(transport: _Ray) -> None:
    runtime = _runtime(ray_backend.RayBackend(2, max_in_flight=1))
    assert transport.init_options == {}
    assert transport.dataset_puts == 0
    assert runtime.name == "ray"
    assert runtime.workers == 2
    assert runtime.capacity == 1
    runtime.start()
    assert len(transport.actors) == 2
    assert transport.dataset_puts == 1
    assert transport.actor_options["max_restarts"] == 0
    assert transport.actor_options["max_task_retries"] == 0
    with pytest.raises(RuntimeError, match=r"single.use|already|started"):
        runtime.start()
    runtime.close()
    runtime.close()
    assert transport.killed == transport.actors
    assert transport.shutdowns == 1
    with pytest.raises(RuntimeError, match=r"single.use|closed|started"):
        runtime.start()


def test_one_attempt_per_actor_and_idle_actor_reuse(
    runtime: "RayAttemptRuntime", transport: _Ray
) -> None:
    _, _, jobs = _inputs()
    for job in jobs[:2]:
        runtime.submit(_attempt(job))
    with pytest.raises(RuntimeError, match=r"capacity|idle|full"):
        runtime.submit(_attempt(jobs[2]))
    first = runtime.poll(0.25)
    assert len(first) == 1
    assert first[0].outcome == execute_job(jobs[0], *_inputs()[:2])
    runtime.submit(_attempt(jobs[2]))
    assigned = [actor for actor, _ in transport.submitted]
    assert assigned[0] is not assigned[1]
    assert assigned[2] is assigned[0]
    assert transport.wait_timeouts == [0.25]
    assert transport.get_timeouts[-1] == 0
    assert runtime.diagnostics["max_observed_in_flight"] == 2


def test_poll_returns_no_reply_without_blocking_get(
    runtime: "RayAttemptRuntime", transport: _Ray
) -> None:
    transport.complete_tasks = False
    _, _, jobs = _inputs()
    runtime.submit(_attempt(jobs[0]))
    gets = len(transport.get_timeouts)
    assert runtime.poll(0.125) == ()
    assert transport.wait_timeouts == [0.125]
    assert len(transport.get_timeouts) == gets


@pytest.mark.parametrize("timeout", [-1, float("nan"), float("inf"), True])
def test_poll_rejects_invalid_timeout(
    runtime: "RayAttemptRuntime", timeout: float
) -> None:
    with pytest.raises(ValueError, match="timeout"):
        runtime.poll(timeout)


def test_simulation_failure_is_an_outcome_and_reuses_actor(
    runtime: "RayAttemptRuntime", transport: _Ray
) -> None:
    data, identity, jobs = _inputs()
    job = create_job(BacktestConfig(), replace(data.metadata, symbol="OTHER"), identity)
    runtime.submit(_attempt(job))
    reply = runtime.poll(0)[0]
    assert reply.failure is None
    assert reply.outcome == execute_job(job, data, identity)
    assert reply.outcome is not None
    assert reply.outcome.failure is not None
    assert reply.outcome.failure.error_type == "DatasetMismatch"
    assert transport.killed == []
    runtime.submit(_attempt(jobs[0]))


@pytest.mark.parametrize("mode", ["get", "submit", "wait"])
def test_ray_failure_has_stable_failure_and_separate_diagnostic(
    runtime: "RayAttemptRuntime", transport: _Ray, mode: str
) -> None:
    _, _, jobs = _inputs()
    attempt = _attempt(jobs[0])
    failure = _RayError("pid=123 transport detail")
    if mode == "submit":
        transport.submit_error = failure
    runtime.submit(attempt)
    if mode == "get":
        ref = transport.tasks[-1]()
        assert ref is not None
        ref.value = failure
    if mode == "wait":
        transport.wait_error = failure
    reply = runtime.poll(0)[0]
    assert reply.attempt_id == attempt.attempt_id
    assert reply.generation == 1
    assert reply.outcome is None
    assert reply.failure == JobFailure(
        "WorkerFailure", "Ray worker failed before returning an outcome"
    )
    assert reply.diagnostic is not None
    assert "pid=123 transport detail" in reply.diagnostic
    assert transport.killed == [transport.actors[0]]
    assert runtime.diagnostics["retirements"] == 1


def test_cancel_drops_late_reply_and_replacement_is_queued_without_ready_get(
    runtime: "RayAttemptRuntime", transport: _Ray
) -> None:
    _, _, jobs = _inputs()
    attempt = _attempt(jobs[0])
    runtime.submit(attempt)
    cancelled = transport.tasks[-1]
    runtime.cancel(attempt.attempt_id)
    runtime.cancel(attempt.attempt_id)
    gc.collect()
    assert cancelled() is None
    assert transport.killed == [transport.actors[0]]
    runtime.submit(_attempt(jobs[1]))
    gets = len(transport.get_timeouts)
    replacement_attempt = _attempt(jobs[0], number=2, generation=2)
    runtime.submit(replacement_attempt)
    assert len(transport.actors) == 3
    assert len(transport.get_timeouts) == gets
    assert transport.dataset_puts == 1
    assert runtime.diagnostics["replacements"] == 1
    replies = runtime.poll(0) + runtime.poll(0) + runtime.poll(0)
    assert {reply.attempt_id for reply in replies} == {
        jobs[1].job_id + ":1",
        jobs[0].job_id + ":2",
    }
    assert all(reply.attempt_id != attempt.attempt_id for reply in replies)
    assert any(reply.generation == 2 for reply in replies)
    workers = runtime.diagnostics["workers"]
    assert isinstance(workers, list)
    assert len(workers) == 3
    assert all(worker["node_id"] == "fake-node" for worker in workers)


@pytest.mark.parametrize("corruption", ["type", "token", "generation", "job"])
def test_malformed_reply_aborts(
    runtime: "RayAttemptRuntime", transport: _Ray, corruption: str
) -> None:
    _, _, jobs = _inputs()
    runtime.submit(_attempt(jobs[0]))
    ref = transport.tasks[-1]()
    assert ref is not None
    if corruption == "type":
        ref.value = {"attempt_id": jobs[0].job_id + ":1"}
    elif corruption == "token":
        ref.value = replace(
            cast("AttemptReply", ref.value), attempt_id=jobs[0].job_id + ":2"
        )
    elif corruption == "generation":
        ref.value = replace(cast("AttemptReply", ref.value), generation=2)
    else:
        ref.value = replace(
            cast("AttemptReply", ref.value),
            outcome=execute_job(jobs[1], *_inputs()[:2]),
        )
    with pytest.raises(RuntimeError, match=r"reply|attempt|match"):
        runtime.poll(0)


def test_duplicate_live_attempt_is_rejected(runtime: "RayAttemptRuntime") -> None:
    _, _, jobs = _inputs()
    attempt = _attempt(jobs[0])
    runtime.submit(attempt)
    with pytest.raises(RuntimeError, match=r"already|duplicate|active"):
        runtime.submit(attempt)


def test_unexpected_programming_error_is_fatal(
    runtime: "RayAttemptRuntime", transport: _Ray
) -> None:
    _, _, jobs = _inputs()
    runtime.submit(_attempt(jobs[0]))
    ref = transport.tasks[-1]()
    assert ref is not None
    ref.value = ValueError("broken serializer")
    with pytest.raises(ValueError, match="broken serializer"):
        runtime.poll(0)


@pytest.mark.parametrize("mode", ["get", "submit", "wait", "ready"])
@pytest.mark.parametrize("cause_type", [ValueError, TypeError, RuntimeError])
def test_wrapped_programming_error_is_fatal(
    runtime: "RayAttemptRuntime",
    transport: _Ray,
    mode: str,
    cause_type: type[Exception],
) -> None:
    _, _, jobs = _inputs()
    cause = cause_type("broken attempt protocol")
    error = (
        _WrappedValueError(cause) if cause_type is ValueError else _RayTaskError(cause)
    )
    with pytest.raises(RuntimeError, match="broken attempt protocol"):
        if mode == "submit":
            transport.submit_error = error
            runtime.submit(_attempt(jobs[0]))
        elif mode == "ready":
            runtime.submit(_attempt(jobs[0]))
            runtime.submit(_attempt(jobs[1]))
            runtime.cancel(jobs[0].job_id + ":1")
            transport.ready_error = error
            runtime.submit(_attempt(jobs[0], number=2))
            runtime.poll(0)
        else:
            runtime.submit(_attempt(jobs[0]))
            if mode == "get":
                ref = transport.tasks[-1]()
                assert ref is not None
                ref.value = error
            else:
                transport.wait_error = error
            runtime.poll(0)
    assert runtime.diagnostics["retirements"] == (1 if mode == "ready" else 0)


@pytest.mark.parametrize("cause_type", [_ActorError, _ResourceError, _ObjectLostError])
def test_wrapped_infrastructure_error_remains_retryable(
    runtime: "RayAttemptRuntime", transport: _Ray, cause_type: type[Exception]
) -> None:
    _, _, jobs = _inputs()
    runtime.submit(_attempt(jobs[0]))
    ref = transport.tasks[-1]()
    assert ref is not None
    ref.value = _RayTaskError(_RayTaskError(cause_type("infrastructure unavailable")))
    reply = runtime.poll(0)[0]
    assert reply.failure == JobFailure(
        "WorkerFailure", "Ray worker failed before returning an outcome"
    )
    assert reply.diagnostic is not None
    assert "infrastructure unavailable" in reply.diagnostic
    assert transport.killed == [transport.actors[0]]


def test_wrapped_initialization_mismatch_is_fatal(
    runtime: "RayAttemptRuntime", transport: _Ray
) -> None:
    _, _, jobs = _inputs()
    runtime.submit(_attempt(jobs[0]))
    ref = transport.tasks[-1]()
    assert ref is not None
    ref.value = _RayTaskError(_InitializationError("installed implementation differs"))
    with pytest.raises(RuntimeError, match="installed implementation differs"):
        runtime.poll(0)


def test_start_readiness_failure_cleans_all_owned_actors(transport: _Ray) -> None:
    transport.ready_error = TimeoutError("startup readiness timeout")
    runtime = _runtime(ray_backend.RayBackend(2, startup_timeout_seconds=0.25))
    with pytest.raises(RuntimeError, match="readiness timeout"):
        runtime.start()
    assert transport.killed == transport.actors
    assert transport.shutdowns == 1
    assert transport.submitted == []
    assert all(timeout == 0.25 for timeout in transport.get_timeouts)


def test_already_initialized_connection_is_not_owned(transport: _Ray) -> None:
    transport.initialized = True
    runtime = _runtime(ray_backend.RayBackend(2))
    with pytest.raises(RuntimeError, match="already initialized"):
        runtime.start()
    runtime.close()
    assert transport.initialized
    assert transport.shutdowns == 0
    assert transport.killed == []


def test_external_cluster_disconnects_only_own_connection(transport: _Ray) -> None:
    runtime = _runtime(ray_backend.RayBackend(2, address="cluster:1234"))
    runtime.start()
    assert transport.init_options["address"] == "cluster:1234"
    assert "num_cpus" not in transport.init_options
    assert runtime.diagnostics["local_owned"] is False
    runtime.close()
    assert transport.killed == transport.actors
    assert transport.shutdowns == 1


@pytest.mark.parametrize("mode", ["kill", "shutdown"])
def test_cleanup_errors_are_reported(transport: _Ray, mode: str) -> None:
    runtime = _runtime(ray_backend.RayBackend(2))
    runtime.start()
    if mode == "kill":
        transport.kill_error = _RayError("cleanup kill failed")
    else:
        transport.shutdown_error = _RayError("cleanup disconnect failed")
    with pytest.raises(RuntimeError, match=r"cleanup|disconnect"):
        runtime.close()
    assert transport.shutdowns == 1
    assert runtime.diagnostics["cleanup_errors"]


@pytest.mark.parametrize("mode", ["kill", "shutdown"])
def test_start_failure_reports_source_mismatch_and_cleanup(
    transport: _Ray, mode: str
) -> None:
    primary = _InitializationError("installed implementation differs")
    transport.ready_error = primary
    cleanup = _RayError("injected cleanup failure")
    if mode == "kill":
        transport.kill_error = cleanup
    else:
        transport.shutdown_error = cleanup
    runtime = _runtime(ray_backend.RayBackend(2))
    with pytest.raises(RuntimeError) as raised:
        runtime.start()
    assert "installed implementation differs" in str(raised.value)
    assert "injected cleanup failure" in str(raised.value)
    assert raised.value.__cause__ is primary
    assert transport.shutdowns == 1
    assert runtime.diagnostics["cleanup_errors"]
    runtime.close()
    assert transport.shutdowns == 1


def test_close_releases_pending_attempt_references(transport: _Ray) -> None:
    runtime = _runtime(ray_backend.RayBackend(2))
    runtime.start()
    _, _, jobs = _inputs()
    for job in jobs[:2]:
        runtime.submit(_attempt(job))
    runtime.close()
    gc.collect()
    assert all(reference() is None for reference in transport.tasks)
    assert transport.killed == transport.actors


def test_cancel_after_observing_deadline_reply_retires_actor(
    runtime: "RayAttemptRuntime", transport: _Ray
) -> None:
    _, _, jobs = _inputs()
    attempt = _attempt(jobs[0])
    runtime.submit(attempt)
    assert runtime.poll(0)[0].attempt_id == attempt.attempt_id
    # The coordinator checks elapsed time after poll and can expire this reply.
    runtime.cancel(attempt.attempt_id)
    assert transport.killed == [transport.actors[0]]
    assert runtime.diagnostics["retirements"] == 1


def test_replacement_pid_and_node_are_observed_with_final_reply(
    runtime: "RayAttemptRuntime", transport: _Ray
) -> None:
    _, _, jobs = _inputs()
    for job in jobs[:2]:
        runtime.submit(_attempt(job))
    runtime.cancel(jobs[0].job_id + ":1")
    runtime.submit(_attempt(jobs[0], number=2))
    assert len(transport.actors) == 3
    received = runtime.poll(0) + runtime.poll(0)
    assert len(received) == 2
    workers = runtime.diagnostics["workers"]
    assert isinstance(workers, list)
    assert len(workers) == 3
    assert workers[-1]["node_id"] == "fake-node"
    assert workers[-1]["pid"] > 0


def test_replacement_result_waits_for_readiness_within_poll_timeout(
    single_worker_runtime: "RayAttemptRuntime", transport: _Ray
) -> None:
    runtime = single_worker_runtime
    attempt = _submit_unready_replacement(runtime, transport)
    gets = len(transport.get_timeouts)
    assert runtime.poll(0.125) == ()
    assert transport.wait_timeouts == [0.125]
    assert len(transport.get_timeouts) == gets
    with pytest.raises(RuntimeError, match="capacity"):
        runtime.submit(_attempt(_inputs()[2][1]))
    readiness = transport.readiness[-1]()
    assert readiness is not None
    readiness.complete = True
    reply = runtime.poll(0)[0]
    assert reply.attempt_id == attempt.attempt_id
    workers = cast(list[dict[str, object]], runtime.diagnostics["workers"])
    pid = workers[-1]["pid"]
    assert isinstance(pid, int) and pid > 0
    assert workers[-1]["node_id"] == "fake-node"


def test_delayed_readiness_failure_is_charged_to_original_lease(
    single_worker_runtime: "RayAttemptRuntime", transport: _Ray
) -> None:
    runtime = single_worker_runtime
    attempt = _submit_unready_replacement(runtime, transport)
    assert runtime.poll(0) == ()
    readiness = transport.readiness[-1]()
    assert readiness is not None
    readiness.value = _ObjectLostError("readiness object lost")
    readiness.complete = True
    reply = runtime.poll(0)[0]
    assert reply.attempt_id == attempt.attempt_id
    assert reply.failure == JobFailure(
        "WorkerFailure", "Ray worker failed before returning an outcome"
    )
    assert transport.killed[-1] is transport.actors[-1]
    transport.complete_readiness = True
    newer = _attempt(_inputs()[2][1])
    runtime.submit(newer)
    reply = runtime.poll(0)[0]
    assert reply.attempt_id == newer.attempt_id
    assert reply.failure is None
    assert reply.outcome is not None
    assert reply.outcome.result is not None


def test_delayed_readiness_mismatch_aborts_before_result_delivery(
    single_worker_runtime: "RayAttemptRuntime", transport: _Ray
) -> None:
    runtime = single_worker_runtime
    _submit_unready_replacement(runtime, transport)
    assert runtime.poll(0) == ()
    readiness = transport.readiness[-1]()
    assert readiness is not None
    readiness.value = _InitializationError("installed implementation differs")
    readiness.complete = True
    with pytest.raises(RuntimeError, match="installed implementation differs"):
        runtime.poll(0)


@pytest.mark.parametrize("mode", ["cancel", "close"])
def test_unready_replacement_releases_all_references(
    single_worker_runtime: "RayAttemptRuntime", transport: _Ray, mode: str
) -> None:
    runtime = single_worker_runtime
    attempt = _submit_unready_replacement(runtime, transport)
    assert runtime.poll(0) == ()
    if mode == "cancel":
        runtime.cancel(attempt.attempt_id)
        assert runtime.poll(0) == ()
    else:
        runtime.close()
    gc.collect()
    assert all(reference() is None for reference in transport.tasks)
    assert all(reference() is None for reference in transport.readiness)


def test_replacement_initialization_mismatch_is_fatal(
    runtime: "RayAttemptRuntime", transport: _Ray
) -> None:
    _, _, jobs = _inputs()
    for job in jobs[:2]:
        runtime.submit(_attempt(job))
    runtime.cancel(jobs[0].job_id + ":1")
    transport.ready_error = _InitializationError("installed implementation differs")
    runtime.submit(_attempt(jobs[0], number=2))
    with pytest.raises(RuntimeError, match="implementation"):
        runtime.poll(0)


def test_replacement_creation_failure_returns_infrastructure_reply(
    runtime: "RayAttemptRuntime", transport: _Ray
) -> None:
    _, _, jobs = _inputs()
    for job in jobs[:2]:
        runtime.submit(_attempt(job))
    runtime.cancel(jobs[0].job_id + ":1")
    transport.create_error = _RayError("cannot allocate replacement")
    runtime.submit(_attempt(jobs[0], number=2))
    reply = runtime.poll(0)[0]
    assert reply.attempt_id == jobs[0].job_id + ":2"
    assert reply.failure == JobFailure(
        "WorkerFailure", "Ray worker failed before returning an outcome"
    )
    assert runtime.diagnostics["replacements"] == 0


def test_cancelled_queued_submission_failure_is_not_delivered(
    runtime: "RayAttemptRuntime", transport: _Ray
) -> None:
    _, _, jobs = _inputs()
    transport.submit_error = _RayError("actor died before submit")
    runtime.submit(_attempt(jobs[0]))
    runtime.cancel(jobs[0].job_id + ":1")
    assert runtime.poll(0) == ()
    assert transport.killed == [transport.actors[0]]


def test_calls_before_start_and_after_close_are_rejected(transport: _Ray) -> None:
    runtime = _runtime(ray_backend.RayBackend(2))
    _, _, jobs = _inputs()
    for closed in (False, True):
        if closed:
            runtime.close()
        with pytest.raises(RuntimeError, match=r"started|closed"):
            runtime.submit(_attempt(jobs[0]))
        with pytest.raises(RuntimeError, match=r"started|closed"):
            runtime.poll(0)
        with pytest.raises(RuntimeError, match=r"started|closed"):
            runtime.cancel(jobs[0].job_id + ":1")
    assert transport.shutdowns == 0

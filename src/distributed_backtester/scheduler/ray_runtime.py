"""Bounded asynchronous Ray attempts with coordinator-owned retry decisions."""

import math
from collections import deque
from time import perf_counter
from typing import Protocol, cast

from distributed_backtester.backends import ray as ray_backend
from distributed_backtester.models import MarketDataset
from distributed_backtester.provenance import ImplementationIdentity
from distributed_backtester.scheduler.models import AttemptReply, AttemptSpec
from distributed_backtester.worker import JobFailure, JobOutcome


class _AttemptMethod(Protocol):
    def remote(self, attempt: AttemptSpec) -> object: ...


class _AttemptActor(ray_backend._Actor, Protocol):
    @property
    def execute_attempt(self) -> _AttemptMethod: ...


class _AttemptWorker(ray_backend._SimulationWorker):
    def execute_attempt(self, attempt: AttemptSpec) -> AttemptReply:
        return AttemptReply(
            attempt.attempt_id,
            attempt.generation,
            self.execute(attempt.job),
            None,
        )


class RayAttemptRuntime:
    """One active attempt per actor; replacement startup consumes its lease.

    The initial pool is ready when start returns. Replacement readiness and
    execution are queued together, and poll observes both without blocking on
    startup. Cancellation retires the actor and releases all its references.
    """

    name = "ray"

    def __init__(
        self,
        backend: ray_backend.RayBackend,
        dataset: MarketDataset,
        implementation: ImplementationIdentity,
    ) -> None:
        self.workers = backend.workers
        self.capacity = min(backend.workers, backend.max_in_flight)
        self._backend = backend
        self._pool = ray_backend._RayPool(
            backend, dataset, implementation, worker_type=_AttemptWorker
        )
        self.diagnostics = self._pool.diagnostics
        self.diagnostics.update(replacements=0, retirements=0, max_observed_in_flight=0)
        self._idle: deque[ray_backend._Actor] = deque()
        self._pending: dict[object, tuple[AttemptSpec, ray_backend._Actor]] = {}
        self._active: dict[str, object | None] = {}
        self._readiness: dict[object, tuple[AttemptSpec, ray_backend._Actor]] = {}
        self._delivered: dict[ray_backend._Actor, str] = {}
        self._replies: deque[AttemptReply] = deque()
        self._replacements = 0
        self._retirements = 0
        self._started = False
        self._closed = False

    def start(self) -> None:
        if self._started or self._closed:
            raise RuntimeError("Ray attempt runtime is single-use and already started")
        self._started = True
        self._backend.diagnostics = self.diagnostics
        self._backend.max_observed_in_flight = 0
        try:
            self._pool.start()
            self._idle.extend(self._pool.actors)
        except Exception as error:
            self._close(primary_error=error)
            raise RuntimeError(f"Ray execution failed: {error}") from error

    def _require_started(self) -> None:
        if not self._started or self._closed:
            raise RuntimeError("Ray attempt runtime is not started or is closed")

    def _check_fatal_error(self, error: Exception) -> None:
        exceptions = self._pool.ray.exceptions
        cause = error
        while isinstance(cause, exceptions.RayTaskError):
            cause = cause.cause
        if getattr(cause, "actor_init_failed", False):
            raise RuntimeError(f"Ray worker initialization failed: {error}") from error
        if isinstance(error, exceptions.RayTaskError) and not isinstance(
            cause,
            (
                exceptions.RayActorError,
                exceptions.WorkerCrashedError,
                exceptions.LocalRayletDiedError,
                exceptions.NodeDiedError,
                exceptions.ObjectStoreFullError,
                exceptions.OutOfDiskError,
                exceptions.OutOfMemoryError,
                exceptions.ObjectLostError,
                exceptions.GetTimeoutError,
                exceptions.RpcError,
            ),
        ):
            # Simulation exceptions already arrive as JobOutcome failures. A
            # task exception outside that boundary is fatal unless its cause
            # identifies an actor, transport, or resource failure.
            raise RuntimeError(f"Ray worker task failed: {error}") from error

    def _failure(self, attempt: AttemptSpec, error: Exception) -> AttemptReply:
        self._check_fatal_error(error)
        return AttemptReply(
            attempt.attempt_id,
            attempt.generation,
            None,
            JobFailure(
                "WorkerFailure", "Ray worker failed before returning an outcome"
            ),
            f"{type(error).__name__}: {error}",
        )

    def _retire(self, actor: ray_backend._Actor) -> None:
        self._delivered.pop(actor, None)
        if actor in self._idle:
            self._idle.remove(actor)
        for ref, (_, ready_actor) in tuple(self._readiness.items()):
            if ready_actor == actor:
                del self._readiness[ref]
        self._pool.retire(actor)
        self._retirements += 1
        self.diagnostics["retirements"] = self._retirements

    def submit(self, attempt: AttemptSpec) -> None:
        self._require_started()
        if not isinstance(attempt, AttemptSpec):
            raise ValueError("submission requires an AttemptSpec")
        if attempt.attempt_id in self._active:
            raise RuntimeError("attempt is already active")
        if len(self._active) >= self.capacity:
            raise RuntimeError("Ray attempt runtime capacity is full")
        self._active[attempt.attempt_id] = None
        observed = max(self._backend.max_observed_in_flight, len(self._active))
        self._backend.max_observed_in_flight = observed
        self.diagnostics["max_observed_in_flight"] = observed
        actor: ray_backend._Actor | None = None
        try:
            if self._idle:
                actor = self._idle.popleft()
                self._delivered.pop(actor, None)
            else:
                actor = self._pool.create_actor()
                self._replacements += 1
                self.diagnostics["replacements"] = self._replacements
                self._readiness[actor.ready.remote()] = attempt, actor
            ref = cast(_AttemptActor, actor).execute_attempt.remote(attempt)
            self._pending[ref] = attempt, actor
            self._active[attempt.attempt_id] = ref
        except self._pool.ray.exceptions.RayError as error:
            reply = self._failure(attempt, error)
            if actor is not None:
                self._retire(actor)
            self._replies.append(reply)

    @staticmethod
    def _validate_reply(value: object, attempt: AttemptSpec) -> AttemptReply:
        if (
            not isinstance(value, AttemptReply)
            or value.attempt_id != attempt.attempt_id
            or type(value.generation) is not int
            or value.generation != attempt.generation
            or (value.outcome is None) == (value.failure is None)
            or (
                value.outcome is not None
                and (
                    not isinstance(value.outcome, JobOutcome)
                    or value.outcome.job_id != attempt.job.job_id
                )
            )
            or (value.failure is not None and not isinstance(value.failure, JobFailure))
        ):
            raise RuntimeError("worker reply does not match submitted attempt/job")
        return value

    def _fail_pending(self, ref: object, error: Exception) -> AttemptReply:
        attempt, actor = self._pending[ref]
        reply = self._failure(attempt, error)
        del self._pending[ref]
        del self._active[attempt.attempt_id]
        self._retire(actor)
        return reply

    def poll(self, timeout: float) -> tuple[AttemptReply, ...]:
        self._require_started()
        if (
            type(timeout) not in (int, float)
            or not math.isfinite(timeout)
            or timeout < 0
        ):
            raise ValueError("poll timeout must be finite and nonnegative")
        if self._replies:
            replies = tuple(self._replies)
            self._replies.clear()
            for reply in replies:
                del self._active[reply.attempt_id]
            return replies
        ray = self._pool.ray
        deadline = perf_counter() + timeout
        remaining = timeout
        while self._pending or self._readiness:
            unready = {actor for _, actor in self._readiness.values()}
            # Keep the replacement's original lease active until readiness is
            # resolved. Its result can become available first, but the actor
            # must not be reused or deliver that result before its PID/node and
            # initialization errors have been observed.
            references = [
                *self._readiness,
                *(
                    ref
                    for ref, (_, actor) in self._pending.items()
                    if actor not in unready
                ),
            ]
            try:
                ready, _ = ray.wait(references, num_returns=1, timeout=remaining)
            except ray.exceptions.RayError as error:
                self._check_fatal_error(error)
                return tuple(
                    self._fail_pending(ref, error) for ref in tuple(self._pending)
                )
            if not ready:
                return ()
            ref = ready[0]
            if ref in self._readiness:
                attempt, actor = self._readiness.pop(ref)
                try:
                    information = ray.get(ref, timeout=0)
                except ray.exceptions.RayError as error:
                    self._check_fatal_error(error)
                    attempt_ref = self._active[attempt.attempt_id]
                    if attempt_ref is None:
                        raise RuntimeError(
                            "replacement readiness has no active task"
                        ) from error
                    return (self._fail_pending(attempt_ref, error),)
                workers = cast(list[object], self.diagnostics["workers"])
                workers.append(information)
            elif ref in self._pending:
                attempt, actor = self._pending[ref]
                try:
                    value = ray.get(ref, timeout=0)
                except ray.exceptions.RayError as error:
                    return (self._fail_pending(ref, error),)
                reply = self._validate_reply(value, attempt)
                del self._pending[ref]
                del self._active[attempt.attempt_id]
                self._idle.append(actor)
                # Deadline checks happen after poll returns. Retain only the
                # actor's latest delivery so that an expired reply can retire it.
                self._delivered[actor] = attempt.attempt_id
                return (reply,)
            else:
                raise RuntimeError("Ray wait returned an unknown attempt reference")
            remaining = max(0.0, deadline - perf_counter())
        return ()

    def cancel(self, attempt_id: str) -> None:
        self._require_started()
        if attempt_id not in self._active:
            actor = next(
                (
                    actor
                    for actor, delivered in self._delivered.items()
                    if delivered == attempt_id
                ),
                None,
            )
            if actor is not None:
                self._retire(actor)
            return
        ref = self._active.pop(attempt_id)
        if ref is None:
            self._replies = deque(
                reply for reply in self._replies if reply.attempt_id != attempt_id
            )
            return
        _, actor = self._pending.pop(ref)
        self._retire(actor)

    def close(self) -> None:
        self._close()

    def _close(self, *, primary_error: BaseException | None = None) -> None:
        if self._closed:
            return
        self._closed = True
        self._idle.clear()
        self._pending.clear()
        self._active.clear()
        self._readiness.clear()
        self._delivered.clear()
        self._replies.clear()
        self._pool.close(primary_error=primary_error)

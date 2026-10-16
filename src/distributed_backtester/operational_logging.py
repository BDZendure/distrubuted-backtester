"""Scoped JSON observations, separate from economic and durable state."""

import json
import logging
import math
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TextIO, cast

LEVELS = ("OFF", "ERROR", "WARNING", "INFO", "DEBUG")
_RESERVED = {"schema_version", "timestamp", "level", "event", "pid"}
_LOGGER = logging.getLogger("distributed_backtester.operations")
_LEVEL: ContextVar[str] = ContextVar("operational_level", default="OFF")
_CONTEXT: ContextVar[dict[str, object] | None] = ContextVar(
    "operational_context", default=None
)


def _fields(values: dict[str, object]) -> dict[str, object]:
    if _RESERVED.intersection(values):
        raise ValueError("operational context cannot contain reserved record fields")
    for key, value in values.items():
        if not isinstance(key, str) or not key:
            raise ValueError("operational field names must be nonempty text")
        if value is not None and type(value) not in (str, int, float, bool):
            raise ValueError("operational fields must contain JSON scalar values")
        if type(value) is float and not math.isfinite(value):
            raise ValueError("operational fields must contain finite numbers")
    return dict(values)


@dataclass(frozen=True, slots=True)
class LogState:
    """Serializable settings copied into a worker, without handler/stream objects."""

    level: str
    context: dict[str, object]

    def __post_init__(self) -> None:
        if self.level not in LEVELS:
            raise ValueError("invalid operational log level")
        object.__setattr__(self, "context", _fields(self.context))


class _JSONHandler(logging.Handler):
    def __init__(self, stream: TextIO) -> None:
        super().__init__()
        self.stream = stream

    def emit(self, record: logging.LogRecord) -> None:
        # Observational delivery is best effort; a closed/full log destination
        # must never change committed results or replace a workload exception.
        try:
            payload = {
                "schema_version": 1,
                "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(
                    timespec="milliseconds"
                ),
                "level": record.levelname,
                "event": record.getMessage(),
                "pid": os.getpid(),
                **cast(dict[str, object], record.__dict__["operational_fields"]),
            }
            self.stream.write(
                json.dumps(payload, sort_keys=True, allow_nan=False) + "\n"
            )
            self.stream.flush()
        except Exception:
            return


@contextmanager
def log_session(level: str, *, stream: TextIO | None = None) -> Iterator[None]:
    """Own one process-local CLI logging scope; restore the caller on every exit.

    Concurrent reconfiguration from different threads is unsupported. Ordinary
    event emission uses logging's handler lock; nested scopes are supported.
    """
    if level not in LEVELS:
        raise ValueError("invalid operational log level")
    previous = (_LOGGER.handlers[:], _LOGGER.level, _LOGGER.propagate, _LOGGER.disabled)
    handler = _JSONHandler(sys.stderr if stream is None else stream)
    token = _LEVEL.set(level)
    try:
        _LOGGER.handlers = [handler]
        _LOGGER.propagate = False
        _LOGGER.disabled = level == "OFF"
        _LOGGER.setLevel(logging.ERROR if level == "OFF" else level)
        yield
    finally:
        _LOGGER.handlers, old_level, _LOGGER.propagate, _LOGGER.disabled = previous
        _LOGGER.setLevel(old_level)
        _LEVEL.reset(token)
        handler.close()


@contextmanager
def event_context(**values: object) -> Iterator[None]:
    token = _CONTEXT.set({**(_CONTEXT.get() or {}), **_fields(values)})
    try:
        yield
    finally:
        _CONTEXT.reset(token)


def capture_logging() -> LogState:
    return LogState(_LEVEL.get(), _CONTEXT.get() or {})


@contextmanager
def worker_session(state: LogState | None) -> Iterator[None]:
    """Recreate a parent's operational scope in a worker without inherited IO."""
    if state is None or state.level == "OFF":
        yield
        return
    with log_session(state.level), event_context(**state.context):
        yield


def emit_event(event: str, *, level: str = "INFO", **values: object) -> None:
    if _LEVEL.get() == "OFF":
        return
    if level not in LEVELS or level == "OFF":
        raise ValueError("invalid event log level")
    if not isinstance(event, str) or not event:
        raise ValueError("operational event must be nonempty text")
    fields = {**(_CONTEXT.get() or {}), **_fields(values)}
    try:
        _LOGGER.log(
            logging.getLevelName(level),
            event,
            extra={"operational_fields": fields},
        )
    except Exception:
        # Caller filters and record factories run before the JSON handler.
        # These delivery failures are observational too; explicit API field
        # validation above still raises and interrupts still propagate.
        return

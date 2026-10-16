"""Translate one driver shutdown signal into existing exception cleanup paths."""

import signal
import sys
import threading
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from types import FrameType


class CommandInterrupted(KeyboardInterrupt):
    def __init__(self, signum: int) -> None:
        self.signum = signum
        self.exit_code = 128 + signum
        super().__init__(f"interrupted by {signal.Signals(signum).name}")

    def __str__(self) -> str:
        # Notes remain attached to the original control-flow exception and also
        # reach the CLI's structured interruption detail without another event.
        return "; ".join([super().__str__(), *getattr(self, "__notes__", [])])


@dataclass(slots=True)
class SignalState:
    # Native libraries can translate KeyboardInterrupt subclasses into the base
    # exception. Keep the received signal outside the exception for exit status.
    signum: int | None = None
    _defer_depth: int = 0
    _handler: Callable[[int, FrameType | None], None] | None = None


_ACTIVE: ContextVar[SignalState | None] = ContextVar("cli_signal_state", default=None)


def owned_signal_handler() -> Callable[[int, FrameType | None], None] | None:
    """Return the active CLI bridge only in the process's main thread."""
    if threading.current_thread() is not threading.main_thread():
        return None
    state = _ACTIVE.get()
    return state._handler if state is not None else None


def add_cleanup_note(primary: BaseException, detail: str) -> None:
    """Retain cleanup diagnostics without replacing interruption identity."""
    if detail not in getattr(primary, "__notes__", []):
        primary.add_note(detail)


@contextmanager
def defer_cleanup_signals() -> Iterator[None]:
    """Finish owned cleanup before delivering its first CLI shutdown signal.

    Outside an active main-thread CLI scope this is a no-op. Nested ownership
    scopes deliver once, at the outermost exit. Existing exceptions keep their
    identity unless a first shutdown signal arrives during cleanup; that signal
    takes priority and any cleanup error becomes its diagnostic note.
    """
    state = _ACTIVE.get() if owned_signal_handler() is not None else None
    if state is None:
        yield
        return
    before = state.signum
    cleanup_error: BaseException | None = None
    state._defer_depth += 1
    try:
        yield
    except BaseException as error:
        cleanup_error = error
        raise
    finally:
        state._defer_depth -= 1
        if before is None and state.signum is not None and state._defer_depth == 0:
            interruption = CommandInterrupted(state.signum)
            if cleanup_error is not None:
                add_cleanup_note(
                    interruption,
                    f"cleanup failed: {type(cleanup_error).__name__}: {cleanup_error}",
                )
            raise interruption from cleanup_error


def _restore_handlers(originals: Mapping[signal.Signals, object]) -> None:
    """Try every restoration even if the caller's restored handler interrupts."""
    primary = sys.exception()
    errors: list[BaseException] = []
    with defer_cleanup_signals():
        for sig, handler in originals.items():
            try:
                signal.signal(sig, handler)  # type: ignore[arg-type]
            except BaseException as error:
                errors.append(error)
        if errors:
            first = primary if isinstance(primary, KeyboardInterrupt) else errors[0]
            for secondary in errors:
                if secondary is not first:
                    add_cleanup_note(
                        first,
                        "signal restoration failed: "
                        f"{type(secondary).__name__}: {secondary}",
                    )
            raise first


@contextmanager
def preserve_signal_handlers() -> Iterator[None]:
    """Restore a caller's handlers after a third-party initialization boundary."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    originals = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        yield
    finally:
        _restore_handlers(originals)


@contextmanager
def cli_signals() -> Iterator[SignalState]:
    """The first signal unwinds; later shutdown signals allow cleanup to finish.

    No IO, scheduler work or Ray operation happens inside a signal handler.
    Cleanup has no strict time bound, and SIGKILL remains an external escape.
    In a library caller's non-main thread, leave process-wide handlers untouched.
    """
    if threading.current_thread() is not threading.main_thread():
        yield SignalState()
        return
    state = SignalState()

    def interrupt(signum: int, _frame: FrameType | None) -> None:
        if state.signum is None:
            state.signum = signum
            if state._defer_depth == 0:
                raise CommandInterrupted(signum)

    originals = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    installed: list[signal.Signals] = []
    state._handler = interrupt
    token = _ACTIVE.set(state)
    try:
        for sig in originals:
            # Obligation must precede installation: a newly installed callback
            # may run before signal.signal returns.
            installed.append(sig)
            signal.signal(sig, interrupt)
        yield state
    finally:
        try:
            _restore_handlers({sig: originals[sig] for sig in installed})
        finally:
            _ACTIVE.reset(token)

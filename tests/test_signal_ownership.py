"""Ownership adapter isolation and exception identity, with real POSIX signals."""

import signal
import threading
from collections.abc import Callable
from contextvars import copy_context
from types import FrameType, ModuleType, SimpleNamespace

import pytest

from distributed_backtester import cli_lifecycle as lifecycle
from distributed_backtester.backends import ray as backend


@pytest.mark.parametrize(
    "error", [ValueError("original"), KeyboardInterrupt("original")]
)
def test_cleanup_deferral_without_new_signal_preserves_exception(
    error: BaseException,
) -> None:
    with (
        lifecycle.cli_signals(),
        pytest.raises(type(error)) as caught,
        lifecycle.defer_cleanup_signals(),
    ):
        raise error
    assert caught.value is error
    assert not hasattr(error, "__notes__")


def test_nested_cleanup_delivers_first_signal_only_after_outer_completion() -> None:
    calls: list[str] = []
    with lifecycle.cli_signals() as state:
        with (
            pytest.raises(lifecycle.CommandInterrupted) as caught,
            lifecycle.defer_cleanup_signals(),
        ):
            with lifecycle.defer_cleanup_signals():
                signal.raise_signal(signal.SIGTERM)
                signal.raise_signal(signal.SIGINT)
                calls.append("inner-finished")
            calls.append("outer-finished")
        assert calls == ["inner-finished", "outer-finished"]
        assert caught.value.signum == state.signum == signal.SIGTERM
        signal.raise_signal(signal.SIGINT)
    assert lifecycle.owned_signal_handler() is None


def test_new_cleanup_signal_keeps_cleanup_error_as_note_and_cause() -> None:
    error = OSError("close diagnostic")
    with (
        lifecycle.cli_signals(),
        pytest.raises(lifecycle.CommandInterrupted) as caught,
        lifecycle.defer_cleanup_signals(),
    ):
        signal.raise_signal(signal.SIGTERM)
        raise error
    assert caught.value.__cause__ is error
    assert "close diagnostic" in str(caught.value)
    assert "close diagnostic" in " ".join(caught.value.__notes__)


def test_handler_restoration_defers_first_signal_until_both_handlers_restored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    originals = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    fired = False

    def install(sig: signal.Signals, handler: object) -> object:
        nonlocal fired
        if not fired and sig == signal.SIGINT and handler is originals[sig]:
            fired = True
            signal.raise_signal(signal.SIGTERM)
        return signal.signal(sig, handler)  # type: ignore[arg-type]

    monkeypatch.setattr(
        lifecycle,
        "signal",
        SimpleNamespace(
            SIGINT=signal.SIGINT,
            SIGTERM=signal.SIGTERM,
            Signals=signal.Signals,
            getsignal=signal.getsignal,
            signal=install,
        ),
    )
    try:
        with pytest.raises(lifecycle.CommandInterrupted), lifecycle.cli_signals():
            pass
        assert all(
            signal.getsignal(sig) == handler for sig, handler in originals.items()
        )
        assert lifecycle.owned_signal_handler() is None
    finally:
        for sig, handler in originals.items():
            signal.signal(sig, handler)


SignalHandler = Callable[[int, FrameType | None], None]


@pytest.mark.parametrize("scope", ["cli", "vendor"])
def test_first_signal_after_caller_sigint_restored_still_restores_sigterm(
    monkeypatch: pytest.MonkeyPatch, scope: str
) -> None:
    originals = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    fired = False

    def install(sig: signal.Signals, handler: object) -> object:
        nonlocal fired
        previous = signal.signal(sig, handler)  # type: ignore[arg-type]
        if not fired and sig == signal.SIGINT and handler is originals[sig]:
            fired = True
            signal.raise_signal(signal.SIGINT)
        return previous

    monkeypatch.setattr(
        lifecycle,
        "signal",
        SimpleNamespace(
            SIGINT=signal.SIGINT,
            SIGTERM=signal.SIGTERM,
            Signals=signal.Signals,
            getsignal=signal.getsignal,
            signal=install,
        ),
    )
    try:
        manager = (
            lifecycle.cli_signals
            if scope == "cli"
            else lifecycle.preserve_signal_handlers
        )
        with pytest.raises(KeyboardInterrupt), manager():
            if scope == "vendor":
                signal.signal(signal.SIGINT, signal.SIG_IGN)
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
        assert fired
        assert all(
            signal.getsignal(sig) == handler for sig, handler in originals.items()
        )
        assert lifecycle.owned_signal_handler() is None
    finally:
        for sig, handler in originals.items():
            signal.signal(sig, handler)


@pytest.fixture
def ray_hook(monkeypatch: pytest.MonkeyPatch) -> tuple[ModuleType, list[SignalHandler]]:
    utils = ModuleType("ray._private.utils")
    received: list[SignalHandler] = []

    def setter(handler: SignalHandler) -> None:
        received.append(handler)

    utils.__dict__["set_sigterm_handler"] = setter

    def import_module(name: str) -> ModuleType:
        assert name == utils.__name__
        return utils

    monkeypatch.setattr(
        backend, "importlib", SimpleNamespace(import_module=import_module)
    )
    return utils, received


@pytest.mark.parametrize(
    "error", [None, ValueError("init"), KeyboardInterrupt(), SystemExit(15)]
)
def test_ray_registration_uses_owned_bridge_and_restores_prior_hook(
    ray_hook: tuple[ModuleType, list[SignalHandler]], error: BaseException | None
) -> None:
    utils, received = ray_hook
    previous = utils.set_sigterm_handler
    public_setter = signal.signal

    def vendor(signum: int, frame: FrameType | None) -> None:
        raise SystemExit(signum)

    with lifecycle.cli_signals():
        bridge = lifecycle.owned_signal_handler()
        try:
            with backend._protect_startup_signal_registration():
                utils.set_sigterm_handler(vendor)
                assert received == [bridge]
                if error is not None:
                    raise error
        except BaseException as caught:
            assert caught is error
        assert utils.set_sigterm_handler is previous
        assert signal.getsignal(signal.SIGTERM) is bridge
    assert signal.signal is public_setter


def test_ray_adapter_leaves_library_calls_untouched(
    ray_hook: tuple[ModuleType, list[SignalHandler]],
) -> None:
    utils, received = ray_hook
    previous = utils.set_sigterm_handler
    with backend._protect_startup_signal_registration():
        assert utils.set_sigterm_handler is previous
    assert received == []


def test_ray_adapter_other_thread_passes_vendor_handler_even_with_copied_context(
    ray_hook: tuple[ModuleType, list[SignalHandler]],
) -> None:
    utils, received = ray_hook
    errors: list[BaseException] = []

    def vendor(signum: int, frame: FrameType | None) -> None:
        pass

    def run() -> None:
        try:
            assert lifecycle.owned_signal_handler() is None
            utils.set_sigterm_handler(vendor)
        except BaseException as error:
            errors.append(error)

    with lifecycle.cli_signals(), backend._protect_startup_signal_registration():
        context = copy_context()
        thread = threading.Thread(target=context.run, args=(run,))
        thread.start()
        thread.join(timeout=5)
        assert not thread.is_alive()
    assert errors == []
    assert received == [vendor]


def test_nested_ray_adapters_restore_exact_previous_hook(
    ray_hook: tuple[ModuleType, list[SignalHandler]],
) -> None:
    utils, _ = ray_hook
    previous = utils.set_sigterm_handler
    with lifecycle.cli_signals(), backend._protect_startup_signal_registration():
        outer = utils.set_sigterm_handler
        with backend._protect_startup_signal_registration():
            assert utils.set_sigterm_handler is not outer
        assert utils.set_sigterm_handler is outer
    assert utils.set_sigterm_handler is previous


def test_first_signal_during_ray_hook_restoration_finishes_restoration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def previous(handler: SignalHandler) -> None:
        pass

    class RestoreBoundary(ModuleType):
        def __setattr__(self, name: str, value: object) -> None:
            if (
                name == "set_sigterm_handler"
                and value is previous
                and self.__dict__.get("armed")
            ):
                signal.raise_signal(signal.SIGTERM)
                self.__dict__["finished"] = True
            super().__setattr__(name, value)

    utils = RestoreBoundary("ray._private.utils")
    utils.__dict__["set_sigterm_handler"] = previous
    monkeypatch.setattr(
        backend, "importlib", SimpleNamespace(import_module=lambda name: utils)
    )
    with (
        lifecycle.cli_signals(),
        pytest.raises(lifecycle.CommandInterrupted) as caught,
        backend._protect_startup_signal_registration(),
    ):
        utils.__dict__["armed"] = True
    assert caught.value.exit_code == 143
    assert utils.set_sigterm_handler is previous
    assert utils.__dict__["finished"] is True

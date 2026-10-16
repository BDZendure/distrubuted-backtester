"""Signals request normal cleanup rather than mutate scheduler state in handlers."""

import signal
import threading

import pytest


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_signal_scope_restores_handlers_and_unwinds(signum: signal.Signals) -> None:
    from distributed_backtester.cli_lifecycle import CommandInterrupted, cli_signals

    originals = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    cleaned = False
    with pytest.raises(CommandInterrupted) as interrupted, cli_signals():
        try:
            signal.raise_signal(signum)
        finally:
            cleaned = True
    assert cleaned
    assert interrupted.value.exit_code == 128 + signum
    assert all(signal.getsignal(sig) == handler for sig, handler in originals.items())


def test_second_shutdown_signal_does_not_interrupt_cleanup() -> None:
    from distributed_backtester.cli_lifecycle import CommandInterrupted, cli_signals

    with cli_signals():
        with pytest.raises(CommandInterrupted):
            signal.raise_signal(signal.SIGTERM)
        signal.raise_signal(signal.SIGTERM)


def test_non_main_thread_does_not_change_process_signal_handlers() -> None:
    from distributed_backtester.cli_lifecycle import cli_signals

    failures: list[BaseException] = []

    def run() -> None:
        try:
            with cli_signals():
                pass
        except BaseException as error:
            failures.append(error)

    originals = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    thread = threading.Thread(target=run)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert not failures
    assert all(signal.getsignal(sig) == handler for sig, handler in originals.items())


def test_interruption_is_keyboard_interrupt_compatible() -> None:
    from distributed_backtester.cli_lifecycle import CommandInterrupted

    assert issubclass(CommandInterrupted, KeyboardInterrupt)


def test_vendor_signal_changes_restore_driver_bridge_and_signal_identity() -> None:
    from distributed_backtester.cli_lifecycle import (
        CommandInterrupted,
        cli_signals,
        preserve_signal_handlers,
    )

    with cli_signals() as state:
        original = signal.getsignal(signal.SIGTERM)
        with preserve_signal_handlers():
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        assert signal.getsignal(signal.SIGTERM) == original
        with pytest.raises(CommandInterrupted):
            signal.raise_signal(signal.SIGTERM)
        assert state.signum == signal.SIGTERM

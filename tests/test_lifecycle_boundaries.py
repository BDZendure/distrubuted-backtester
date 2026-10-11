"""Real CLI and signal boundaries, with no native cluster or actor startup."""

import json
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from distributed_backtester.backends import ray as ray_backend
from distributed_backtester.cli_lifecycle import CommandInterrupted, cli_signals
from distributed_backtester.provenance import current_implementation
from distributed_backtester.scheduler.coordinator import Clock, run_durable
from distributed_backtester.scheduler.models import RetryPolicy
from distributed_backtester.scheduler.ray_runtime import RayAttemptRuntime
from tests.test_coordinator import FakeClock, FakeRuntime, make_journal
from tests.test_simulation import dataset

PROBE = r"""
import json
import signal
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from distributed_backtester import cli, cli_lifecycle
from distributed_backtester.backends import ray as backend
from distributed_backtester.scheduler import experiments
from distributed_backtester.scheduler.models import AttemptReply
from distributed_backtester.worker import execute_job

mode, signum, source, output = sys.argv[1:]
signum = int(signum)
output = Path(output)
evidence = []
originals = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
hook_restored = True

class Runtime:
    name = "boundary-probe"
    workers = capacity = 1
    def __init__(self, config, data, identity):
        self.data, self.identity = data, identity
        self.diagnostics = {}
        self.replies = []
        self.pool = backend._RayPool(config, data, identity)
    def start(self):
        if mode == "startup":
            self.pool.start()
    def submit(self, attempt):
        if mode == "cleanup-error":
            signal.raise_signal(signum)
        self.replies.append(AttemptReply(attempt.attempt_id, attempt.generation,
            execute_job(attempt.job, self.data, self.identity), None))
    def poll(self, timeout):
        replies, self.replies = tuple(self.replies), []
        return replies
    def cancel(self, attempt_id):
        pass
    def close(self):
        evidence.append("close-start")
        if mode in ("cleanup", "cleanup-new-error"):
            signal.raise_signal(signum)
        if mode in ("cleanup-error", "cleanup-new-error"):
            raise OSError("cleanup diagnostic")
        if mode == "startup":
            self.pool.close()
        evidence.append("close-finished")

experiments.RayAttemptRuntime = Runtime
if mode == "startup":
    # The installed, pinned Ray helper performs the real signal registration.
    # Stop immediately at that boundary, before Ray can create any processes.
    import ray._private.utils as utils
    original_setter = utils.set_sigterm_handler
    def probe_setter(handler):
        original_setter(handler)
        evidence.append("registered")
        signal.raise_signal(signum)
    utils.set_sigterm_handler = probe_setter
    fake_ray = ModuleType("ray")
    fake_ray.is_initialized = lambda: False
    def init(**options):
        def vendor_handler(sig, frame):
            sys.exit(sig)
        utils.set_sigterm_handler(vendor_handler)
        raise AssertionError("probe must interrupt before native startup")
    fake_ray.init = init
    backend._require_ray = lambda: fake_ray
    backend._shutdown_ray = lambda ray: evidence.append("shutdown")
elif mode == "installation":
    # Replace only this module's alias, never signal.signal globally.
    fired = False
    def install(sig, handler):
        global fired
        previous = signal.signal(sig, handler)
        if not fired and sig == signum:
            fired = True
            signal.raise_signal(signum)
        return previous
    cli_lifecycle.signal = SimpleNamespace(SIGINT=signal.SIGINT,
        SIGTERM=signal.SIGTERM, Signals=signal.Signals, getsignal=signal.getsignal,
        signal=install)

try:
    code = cli.main(["sweep", "--data", source, "--symbol", "SYNTH",
        "--price-basis", "raw", "--fast", "2", "--slow", "3",
        "--fee-bps", "10", "--backend", "ray", "--workers", "1",
        "--durable", "--output-dir", str(output), "--log-level", "INFO"])
except BaseException as error:
    code = error.code if isinstance(error, SystemExit) else 99
finally:
    restored = all(signal.getsignal(sig) == handler
        for sig, handler in originals.items())
    if mode == "startup":
        hook_restored = utils.set_sigterm_handler is probe_setter
        utils.set_sigterm_handler = original_setter
    for sig, handler in originals.items():
        signal.signal(sig, handler)
    output.with_suffix(".evidence.json").write_text(json.dumps({"calls": evidence,
        "handlers_restored": restored, "hook_restored": hook_restored}))
sys.exit(code)
"""


@pytest.mark.parametrize(
    "mode,signum",
    [
        ("cleanup", signal.SIGINT),
        ("cleanup", signal.SIGTERM),
        ("cleanup-error", signal.SIGINT),
        ("cleanup-error", signal.SIGTERM),
        ("cleanup-new-error", signal.SIGTERM),
        ("startup", signal.SIGTERM),
        ("installation", signal.SIGINT),
    ],
)
def test_cli_signal_boundary(tmp_path: Path, mode: str, signum: signal.Signals) -> None:
    if mode == "startup":
        pytest.importorskip("ray")
    output = tmp_path / "durable"
    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            PROBE,
            mode,
            str(int(signum)),
            str(Path("examples/sma.csv").resolve()),
            str(output),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=20,
    )
    (tmp_path / "stdout.txt").write_text(completed.stdout)
    (tmp_path / "stderr.jsonl").write_text(completed.stderr)
    evidence = json.loads(output.with_suffix(".evidence.json").read_text())
    assert completed.returncode == 128 + signum, completed.stderr
    assert completed.stdout == ""
    records = [json.loads(line) for line in completed.stderr.splitlines()]
    interruptions = [r for r in records if r["event"] == "command_interrupted"]
    assert len(interruptions) == 1
    assert interruptions[0]["exit_code"] == 128 + signum
    assert not any(r["event"] == "command_failed" for r in records)
    assert evidence["handlers_restored"]
    assert evidence["hook_restored"]
    if mode == "cleanup":
        assert evidence["calls"] == ["close-start", "close-finished"]
    if mode in {"cleanup-error", "cleanup-new-error"}:
        assert "cleanup diagnostic" in interruptions[0]["detail"]
        cleanup = next(r for r in records if r["event"] == "runtime_cleanup_failed")
        assert cleanup["level"] == "ERROR"
        assert cleanup["error_type"] == "OSError"
        assert cleanup["primary_type"] == (
            "CommandInterrupted" if mode == "cleanup-error" else None
        )
    if mode == "startup":
        assert evidence["calls"] == [
            "registered",
            "close-start",
            "shutdown",
            "close-finished",
        ]
    assert not (output / "index.json").exists()


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
@pytest.mark.parametrize("boundary", ["kill", "shutdown"])
def test_first_cleanup_signal_finishes_all_owned_ray_cleanup(
    monkeypatch: pytest.MonkeyPatch, signum: signal.Signals, boundary: str
) -> None:
    pool = ray_backend._RayPool(
        ray_backend.RayBackend(2), dataset([10, 11, 12]), current_implementation()
    )
    actors = [object(), object()]
    calls: list[str] = []
    fake_ray = type(sys)("ray")

    def kill(actor: object, *, no_restart: bool) -> None:
        calls.append("kill-start")
        if boundary == "kill" and actor is actors[0]:
            signal.raise_signal(signum)
        calls.append("kill-finished")

    def shutdown(ray: object) -> None:
        calls.append("shutdown-start")
        if boundary == "shutdown":
            signal.raise_signal(signum)
        calls.append("shutdown-finished")

    monkeypatch.setattr(fake_ray, "kill", kill, raising=False)
    monkeypatch.setattr(ray_backend, "_shutdown_ray", shutdown)
    pool.ray = fake_ray
    pool.actors = actors.copy()  # type: ignore[assignment]
    pool.shared_dataset = object()
    pool._owns_connection = True
    with cli_signals(), pytest.raises(CommandInterrupted) as raised:
        pool.close()
    assert raised.value.exit_code == 128 + signum
    assert calls == ["kill-start", "kill-finished"] * 2 + [
        "shutdown-start",
        "shutdown-finished",
    ]
    assert pool._closed and not pool._owns_connection
    assert pool.actors == [] and pool.shared_dataset is None
    pool.close()
    assert len(calls) == 6


@pytest.mark.parametrize("controlled", [False, True])
def test_pool_cleanup_failure_retains_primary_interruption(
    monkeypatch: pytest.MonkeyPatch, controlled: bool
) -> None:
    pool = ray_backend._RayPool(
        ray_backend.RayBackend(1), dataset([10, 11, 12]), current_implementation()
    )
    pool.ray = type(sys)("ray")
    pool._owns_connection = True
    primary = CommandInterrupted(signal.SIGTERM) if controlled else KeyboardInterrupt()

    def shutdown(ray: object) -> None:
        raise OSError("disconnect diagnostic")

    monkeypatch.setattr(ray_backend, "_shutdown_ray", shutdown)
    with pytest.raises(KeyboardInterrupt) as raised:
        pool.close(primary_error=primary)
    assert raised.value is primary
    assert "disconnect diagnostic" in " ".join(raised.value.__notes__)
    assert not pool._owns_connection


@pytest.mark.parametrize("controlled", [False, True])
def test_coordinator_cleanup_failure_retains_interruption_object(
    tmp_path: Path, controlled: bool
) -> None:
    primary = CommandInterrupted(signal.SIGTERM) if controlled else KeyboardInterrupt()
    clock = FakeClock()

    class InterruptedRuntime(FakeRuntime):
        def submit(self, attempt: object) -> None:
            raise primary

        def close(self) -> None:
            super().close()
            raise OSError("runtime disconnect diagnostic")

    runtime = InterruptedRuntime(clock)
    with (
        make_journal(tmp_path, RetryPolicy()) as journal,
        pytest.raises(KeyboardInterrupt) as caught,
    ):
        run_durable(
            journal, runtime, clock=Clock(clock.utc, clock.monotonic, clock.sleep)
        )
    assert caught.value is primary
    assert runtime.closed
    assert "runtime disconnect diagnostic" in " ".join(primary.__notes__)


def test_attempt_runtime_cleans_partial_startup_on_controlled_interrupt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = RayAttemptRuntime(
        ray_backend.RayBackend(1), dataset([10, 11, 12]), current_implementation()
    )
    calls: list[str] = []
    primary = CommandInterrupted(signal.SIGTERM)

    def start() -> None:
        runtime._pool._owns_connection = True
        runtime._pool.ray = type(sys)("ray")
        raise primary

    monkeypatch.setattr(runtime._pool, "start", start)
    monkeypatch.setattr(
        ray_backend, "_shutdown_ray", lambda ray: calls.append("shutdown")
    )
    with pytest.raises(CommandInterrupted) as caught:
        runtime.start()
    assert caught.value is primary
    assert calls == ["shutdown"]
    assert runtime._closed and not runtime._pool._owns_connection

"""Optional benchmark process measurements and separate host metadata."""

import math
import os
import platform
import signal
import subprocess
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from types import ModuleType
from typing import Any, cast


@dataclass(frozen=True, slots=True)
class MeasuredRun:
    returncode: int
    elapsed_seconds: float
    peak_tree_rss_bytes: int | None
    peak_coordinator_rss_bytes: int | None
    samples: tuple[dict[str, object], ...]
    vanished_observations: int
    denied_observations: int


def _require_psutil() -> ModuleType:
    try:
        import psutil  # type: ignore[import-untyped]
    except ImportError as error:
        raise RuntimeError(
            "Benchmark measurement requires psutil; "
            "install with pip install '.[benchmark]'"
        ) from error
    return cast(ModuleType, psutil)


def measure_command(
    command: Sequence[str],
    *,
    stdout: Path,
    stderr: Path,
    interval_seconds: float = 0.05,
) -> MeasuredRun:
    """Measure a command without a shell, writing both streams directly to files.

    RSS is an approximate simultaneous sum, sampled once per interval. It can
    miss short lived processes and counts shared resident pages more than once.
    ``processes`` counts successful RSS reads, and observation counts are local
    to each sample. Raw sums may be partial after a vanished process. A denied
    sample has no tree total and invalidates both peaks. An unobserved peak
    is None. On POSIX, failure cleanup kills the command's private process group.
    """
    if type(interval_seconds) not in (int, float):
        raise ValueError("interval_seconds must be a finite positive int or float")
    try:
        valid_interval = math.isfinite(interval_seconds) and interval_seconds > 0
    except OverflowError:
        valid_interval = False
    if not valid_interval:
        raise ValueError("interval_seconds must be a finite positive int or float")
    psutil = _require_psutil()
    samples: list[dict[str, object]] = []
    tree_peak: int | None = None
    coordinator_peak: int | None = None
    vanished_total = denied_total = 0

    with stdout.open("wb") as output, stderr.open("wb") as errors:
        started = perf_counter()
        child = subprocess.Popen(
            command,
            stdout=output,
            stderr=errors,
            shell=False,
            start_new_session=os.name == "posix",
        )
        observed: list[Any] = []
        try:
            coordinator = None
            while True:
                vanished = denied = processes = tree_rss = 0
                coordinator_rss: int | None = None
                observed = []
                try:
                    if coordinator is None:
                        coordinator = psutil.Process(child.pid)
                    observed = [coordinator]
                    try:
                        observed.extend(coordinator.children(recursive=True))
                    except psutil.NoSuchProcess:
                        vanished += 1
                    except (psutil.AccessDenied, PermissionError):
                        denied += 1
                except psutil.NoSuchProcess:
                    vanished += 1
                except (psutil.AccessDenied, PermissionError):
                    denied += 1

                for process in observed:
                    try:
                        if not process.is_running():
                            raise psutil.NoSuchProcess(process.pid)
                        rss = int(process.memory_info().rss)
                    except psutil.NoSuchProcess:
                        vanished += 1
                    except (psutil.AccessDenied, PermissionError):
                        denied += 1
                    else:
                        tree_rss += rss
                        processes += 1
                        if process is coordinator:
                            coordinator_rss = rss

                samples.append(
                    {
                        "elapsed_seconds": perf_counter() - started,
                        "tree_rss_bytes": None if denied else tree_rss,
                        "coordinator_rss_bytes": coordinator_rss,
                        "processes": processes,
                        "vanished_observations": vanished,
                        "denied_observations": denied,
                    }
                )
                vanished_total += vanished
                denied_total += denied
                if processes and not denied:
                    tree_peak = max(tree_peak or 0, tree_rss)
                if coordinator_rss is not None:
                    coordinator_peak = max(coordinator_peak or 0, coordinator_rss)
                try:
                    returncode = child.wait(timeout=interval_seconds)
                except subprocess.TimeoutExpired:
                    continue
                elapsed = perf_counter() - started
                break
        except BaseException:
            try:
                if os.name == "posix":
                    with suppress(ProcessLookupError):
                        os.killpg(child.pid, signal.SIGKILL)
                else:
                    # Best effort on Windows, where no POSIX process group exists.
                    with suppress(psutil.Error, OSError):
                        observed.extend(
                            psutil.Process(child.pid).children(recursive=True)
                        )
                    for process in reversed(observed):
                        with suppress(psutil.Error, OSError):
                            process.kill()
            finally:
                child.kill()
                child.wait()
            raise

    return MeasuredRun(
        returncode,
        elapsed,
        None if denied_total else tree_peak,
        None if denied_total else coordinator_peak,
        tuple(samples),
        vanished_total,
        denied_total,
    )


def hardware_environment() -> dict[str, object]:
    """Capture observed host fields, recording unavailable metadata explicitly."""
    psutil = _require_psutil()
    metadata_errors: dict[str, str] = {}

    def observe(field: str, lookup: Callable[[], object]) -> object:
        try:
            value = lookup()
        except (OSError, psutil.Error) as error:
            metadata_errors[field] = f"{type(error).__name__}: {error}"
            return None
        if value is None:
            metadata_errors[field] = "unavailable"
        return value

    environment: dict[str, object] = {
        "psutil_version": psutil.__version__,
        "physical_cores": observe(
            "physical_cores", lambda: psutil.cpu_count(logical=False)
        ),
        "logical_cores": observe(
            "logical_cores", lambda: psutil.cpu_count(logical=True)
        ),
        "ram_bytes": observe("ram_bytes", lambda: psutil.virtual_memory().total),
        "cpu_model": observe("cpu_model", lambda: platform.processor().strip() or None),
        "metadata_errors": metadata_errors,
    }
    if platform.system() == "Darwin" and environment["cpu_model"] in (
        None,
        "arm",
        "arm64",
        "aarch64",
        "i386",
        "x86_64",
    ):
        try:
            model = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout.strip()
            environment["cpu_model"] = model or None
            if model:
                metadata_errors.pop("cpu_model", None)
            else:
                metadata_errors["cpu_model"] = "sysctl returned an empty CPU model"
        except (OSError, subprocess.SubprocessError) as error:
            environment["cpu_model"] = None
            metadata_errors["cpu_model"] = f"{type(error).__name__}: {error}"
    return environment

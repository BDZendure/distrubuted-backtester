"""Owned POSIX trial inventory; private memory and logical store data are separate.

Only a trial's private process group, its exact published local Ray session, and
published (PID, creation time) identities establish ownership. Names classify
roles but never establish ownership. RSS is recorded per process for inspection;
it is never summed because Ray's shared mappings would be counted repeatedly.
psutil is optional and imported only when measurement is requested.
"""

import json
import math
import os
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from types import ModuleType
from typing import Any, cast


@dataclass(frozen=True, slots=True)
class MeasuredRun:
    returncode: int
    elapsed_seconds: float
    peak_private_uss_bytes: int | None
    samples: tuple[dict[str, object], ...]
    denied_observations: int
    vanished_observations: int
    cleanup: dict[str, object]


def _require_psutil() -> ModuleType:
    try:
        import psutil  # type: ignore[import-untyped]
    except ImportError as error:
        raise RuntimeError("Measurement requires pip install '.[benchmark]'") from error
    return cast(ModuleType, psutil)


def validate_interval(interval_seconds: float) -> None:
    if type(interval_seconds) not in (int, float):
        raise ValueError("interval_seconds must be finite and positive")
    try:
        valid = math.isfinite(interval_seconds) and interval_seconds > 0
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError("interval_seconds must be finite and positive")


def _session_argument(arguments: Sequence[str], session: str) -> bool:
    # Reject prefixes such as session_owned_other and generic /tmp/ray matches.
    for argument in arguments:
        value = argument.split("=", 1)[-1]
        if value == session or value.startswith(session + "/"):
            return True
    return False


def _role(name: str, arguments: Sequence[str], *, actor: bool) -> str:
    if actor:
        return "actor"
    # A raylet command embeds both agent launch commands. Its executable name
    # identifies the service before those argument strings are inspected.
    if name in ("raylet", "gcs_server"):
        return "raylet" if name == "raylet" else "gcs"
    text = " ".join((name, *arguments))
    for needle, role in (
        ("runtime_env/agent", "runtime_env_agent"),
        ("runtime_env_agent", "runtime_env_agent"),
        ("dashboard/agent", "dashboard_agent"),
        ("dashboard_agent", "dashboard_agent"),
        ("gcs_server", "gcs"),
        ("raylet", "raylet"),
        ("log_monitor.py", "log_monitor"),
        ("autoscaler", "monitor"),
        ("dashboard.py", "dashboard"),
        ("ray::", "ray_worker"),
    ):
        if needle in text:
            return role
    return "other_worker_or_service"


class _Inventory:
    def __init__(
        self,
        psutil: ModuleType,
        *,
        driver_pid: int,
        ownership: Path | None,
        ray: bool,
    ) -> None:
        self.psutil = psutil
        self.driver_pid = driver_pid
        self.ownership = ownership
        self.ray = ray
        self.known: dict[int, tuple[float, str, str]] = {}
        self.driver_created: float | None = None
        self.published: dict[str, object] = {}
        self.ownership_error: str | None = None
        try:
            self.driver_created = float(psutil.Process(driver_pid).create_time())
            self.known[driver_pid] = (self.driver_created, "driver", "private_group")
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
            pass

    def _read_ownership(self) -> None:
        if not self.ray:
            return
        self.ownership_error = "local ownership has not been published"
        if self.ownership is None or not self.ownership.exists():
            return
        try:
            record = json.loads(self.ownership.read_text(encoding="utf-8"))
            if not isinstance(record, dict):
                raise ValueError("ownership must be a record")
            if (
                record.get("local_owned") is not True
                or record.get("driver_pid") != self.driver_pid
                or record.get("driver_create_time") != self.driver_created
                or self.driver_created is None
            ):
                raise ValueError("ownership is not this driver's owned local cluster")
            session = record.get("session_dir")
            if (
                not isinstance(session, str)
                or not Path(session).is_absolute()
                or not Path(session).name.startswith("session_")
                or Path(session).name == "session_latest"
                or str(Path(session)) != session
            ):
                raise ValueError("ownership must name an exact absolute Ray session")
            processes = record.get("processes")
            if not isinstance(processes, list):
                raise ValueError("ownership must publish process identities")
            for process in processes:
                if (
                    not isinstance(process, dict)
                    or type(process.get("pid")) is not int
                    or process["pid"] <= 0
                    or type(process.get("create_time")) not in (float, int)
                    or not math.isfinite(process["create_time"])
                    or process["create_time"] < self.driver_created
                    or process.get("role") != "actor"
                ):
                    raise ValueError("invalid published process identity")
            self.published = cast(dict[str, object], record)
            self.ownership_error = None
        except (OSError, ValueError, TypeError) as error:
            self.ownership_error = f"{type(error).__name__}: {error}"

    def _group_anchored(self) -> bool:
        """A group number alone is not proof after its original members exit."""
        for pid, (created, _role_name, evidence) in self.known.items():
            if evidence != "private_group":
                continue
            try:
                process = self.psutil.Process(pid)
                if (
                    float(process.create_time()) == created
                    and process.is_running()
                    and os.getpgid(pid) == self.driver_pid
                ):
                    return True
            except (self.psutil.NoSuchProcess, self.psutil.AccessDenied, OSError):
                continue
        return False

    def sample(self) -> dict[str, object]:
        self._read_ownership()
        psutil = self.psutil
        rows: list[dict[str, object]] = []
        denied = vanished = 0
        discovery_errors: list[dict[str, object]] = []
        reused: list[int] = []
        session = self.published.get("session_dir")
        actors: dict[int, float] = {}
        for item in cast(list[dict[str, Any]], self.published.get("processes", [])):
            actors[item["pid"]] = float(item["create_time"])
        seen: set[int] = set()
        candidates: dict[int, Any] = {}
        try:
            for process in psutil.process_iter():
                candidates[int(process.pid)] = process
        except (
            psutil.AccessDenied,
            PermissionError,
            OSError,
            NotImplementedError,
        ) as error:
            denied += 1
            discovery_errors.append(
                {"pid": None, "error": f"{type(error).__name__}: {error}"}
            )
        # Direct identities may remain readable when a system-wide scan is denied.
        for pid in {self.driver_pid, *self.known, *actors} - candidates.keys():
            try:
                candidates[pid] = psutil.Process(pid)
            except psutil.NoSuchProcess:
                vanished += 1
            except (
                psutil.AccessDenied,
                PermissionError,
                OSError,
                NotImplementedError,
            ) as error:
                denied += 1
                discovery_errors.append({"pid": pid, "error": type(error).__name__})
        for process in candidates.values():
            pid = int(process.pid)
            known = self.known.get(pid)
            evidence: str | None = known[2] if known else None
            role: str | None = known[1] if known else None
            created: float | None = None
            expected = known[0] if known else actors.get(pid)
            try:
                created = float(process.create_time())
                if expected is not None and created != expected:
                    reused.append(pid)
                    continue
                if self.driver_created is not None and created < self.driver_created:
                    continue
                group = os.getpgid(pid)
                if (
                    evidence is None
                    and group == self.driver_pid
                    and self._group_anchored()
                ):
                    evidence = "private_group"
                if (
                    evidence is None
                    and pid not in actors
                    and not isinstance(session, str)
                ):
                    continue
                name = str(process.name())
                arguments = cast(list[str], process.cmdline())
                if evidence is None:
                    if isinstance(session, str) and _session_argument(
                        arguments, session
                    ):
                        evidence = "owned_session_argument"
                    elif pid in actors:
                        evidence = "published_pid_creation_time"
                if evidence is None:
                    continue
                if not process.is_running():
                    raise psutil.NoSuchProcess(pid)
                role = (
                    "driver"
                    if pid == self.driver_pid
                    else _role(name, arguments, actor=pid in actors)
                )
                self.known[pid] = (created, role, evidence)
                seen.add(pid)
            except (psutil.NoSuchProcess, ProcessLookupError):
                if known or pid in actors:
                    vanished += 1
                continue
            except (
                psutil.AccessDenied,
                PermissionError,
                OSError,
                NotImplementedError,
            ) as error:
                discovery_errors.append({"pid": pid, "error": type(error).__name__})
                denied += 1
                if known or pid in actors or evidence is not None:
                    rows.append(
                        {
                            "pid": pid,
                            "create_time": expected
                            if expected is not None
                            else created,
                            "role": role or "other_worker_or_service",
                            "ownership": evidence,
                            "rss_bytes": None,
                            "uss_bytes": None,
                            "memory_error": f"discovery denied: {type(error).__name__}",
                        }
                    )
                continue
            rss: int | None = None
            uss: int | None = None
            memory_error: str | None = None
            try:
                # Recheck PID identity after discovery, before any memory read.
                if float(process.create_time()) != created or not process.is_running():
                    reused.append(pid)
                    continue
                rss = int(process.memory_info().rss)
                value = getattr(process.memory_full_info(), "uss", None)
                if type(value) is int and value >= 0:
                    uss = value
                else:
                    memory_error = "USS unavailable on this platform"
            except psutil.NoSuchProcess:
                vanished += 1
                memory_error = "process vanished during memory read"
            except (psutil.AccessDenied, PermissionError, OSError) as error:
                denied += 1
                memory_error = f"{type(error).__name__}: {error}"
            except (AttributeError, NotImplementedError) as error:
                memory_error = f"USS unavailable: {type(error).__name__}"
            rows.append(
                {
                    "pid": pid,
                    "create_time": created,
                    "name": name,
                    "role": role,
                    "ownership": evidence,
                    "rss_bytes": rss,
                    "uss_bytes": uss,
                    "memory_error": memory_error,
                }
            )
        rows.sort(key=lambda row: cast(int, row["pid"]))
        roles = {row["role"] for row in rows}
        missing = [
            role
            for role in (
                (
                    "driver",
                    "gcs",
                    "raylet",
                    "dashboard_agent",
                    "runtime_env_agent",
                    "actor",
                )
                if self.ray
                else ("driver",)
            )
            if role not in roles
        ]
        worker_pids = {
            worker["pid"]
            for worker in cast(list[dict[str, Any]], self.published.get("workers", []))
            if type(worker.get("pid")) is int
        }
        missing_actors = sorted((set(actors) | worker_pids) - seen)
        complete = bool(rows) and not (
            denied
            or discovery_errors
            or reused
            or missing
            or missing_actors
            or (self.ray and self.ownership_error)
            or any(row["uss_bytes"] is None for row in rows)
        )
        private = sum(cast(int, row["uss_bytes"]) for row in rows) if complete else None
        return {
            "scope": "owned_local_ray_cluster" if self.ray else "private_trial_group",
            "inventory": rows,
            "group_anchor_available": self._group_anchored(),
            "private_uss_bytes": private,
            "coverage_complete": complete,
            "missing_roles": missing,
            "missing_actor_pids": missing_actors,
            "reused_pids": sorted(set(reused)),
            "discovery_errors": discovery_errors,
            "discovery_uncertain": bool(discovery_errors),
            "ownership_error": self.ownership_error,
            "denied_observations": denied,
            "vanished_observations": vanished,
            "dataset_serialized_bytes": self.published.get("dataset_serialized_bytes"),
            "object_store_capacity_bytes": self.published.get(
                "object_store_capacity_bytes"
            ),
            "object_store_used_bytes": None,
            "object_store_usage_note": (
                "Live plasma occupancy was not observed. Serialized Python dataset "
                "bytes and configured store capacity are logical quantities, not "
                "physical resident memory; neither is added to USS."
            ),
        }

    def cleanup(self) -> dict[str, object]:
        """Kill only individually revalidated identities, never a global Ray stop."""
        killed: list[int] = []
        errors: list[dict[str, object]] = []
        for pid, (created, _role_name, _evidence) in self.known.items():
            try:
                process = self.psutil.Process(pid)
                if float(process.create_time()) != created or not process.is_running():
                    continue
                process.kill()
                killed.append(pid)
            except self.psutil.NoSuchProcess:
                pass
            except (self.psutil.AccessDenied, OSError) as error:
                errors.append({"pid": pid, "error": f"{type(error).__name__}: {error}"})
        return {"killed_owned_pids": killed, "errors": errors}


def measure_command(
    command: Sequence[str],
    *,
    stdout: Path,
    stderr: Path,
    ownership: Path | None = None,
    ray: bool = False,
    interval_seconds: float = 0.05,
) -> MeasuredRun:
    """Time launch through child exit; retain inventory at every observation.

    This observer runs outside the timed child. Inventory scans and scheduling
    perturb timing; short-lived processes can escape sampling. Aggregate USS is
    null for incomplete samples; an unavailable/denied USS read invalidates the
    run peak. Cleanup after exit is reported separately and excluded from timing.
    Owned local POSIX clusters only; no remote node memory is measured.
    """
    validate_interval(interval_seconds)
    if os.name != "posix":
        raise RuntimeError("Owned local benchmark inventory requires POSIX")
    psutil = _require_psutil()
    samples: list[dict[str, object]] = []
    peak: int | None = None
    memory_gap = False
    denied = vanished = 0
    with stdout.open("wb") as output, stderr.open("wb") as errors:
        started = perf_counter()
        child = subprocess.Popen(
            command, stdout=output, stderr=errors, shell=False, start_new_session=True
        )
        sampler: _Inventory | None = None
        cleanup: dict[str, object] = {}
        try:
            sampler = _Inventory(
                psutil, driver_pid=child.pid, ownership=ownership, ray=ray
            )
            while True:
                sample = sampler.sample()
                sample["elapsed_seconds"] = perf_counter() - started
                samples.append(sample)
                denied += cast(int, sample["denied_observations"])
                vanished += cast(int, sample["vanished_observations"])
                rows = cast(list[dict[str, object]], sample["inventory"])
                memory_gap |= (
                    bool(sample["discovery_errors"])
                    or cast(int, sample["denied_observations"]) > 0
                    or any(row["uss_bytes"] is None for row in rows)
                )
                value = sample["private_uss_bytes"]
                if isinstance(value, int):
                    peak = max(peak or 0, value)
                try:
                    returncode = child.wait(timeout=interval_seconds)
                except subprocess.TimeoutExpired:
                    continue
                elapsed = perf_counter() - started
                break
        finally:
            try:
                if sampler is not None:
                    # Discover session-owned non-descendants once more before cleanup.
                    try:
                        sampler.sample()
                    finally:
                        cleanup = sampler.cleanup()
            finally:
                if child.poll() is None:
                    child.kill()
                child.wait()
    return MeasuredRun(
        returncode,
        elapsed,
        None if memory_gap else peak,
        tuple(samples),
        denied,
        vanished,
        cleanup,
    )

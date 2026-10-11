import importlib
import json
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest


def module() -> ModuleType:
    return importlib.import_module("distributed_backtester.ray_measurement")


class MissingProcess(Exception):
    pass


class DeniedProcess(Exception):
    pass


class FakeProcess:
    def __init__(
        self,
        pid: int,
        *,
        created: float = 100.0,
        name: str = "python",
        args: tuple[str, ...] = (),
        uss: int | None = 10,
        denied: bool = False,
    ) -> None:
        self.pid = pid
        self.created = created
        self.process_name = name
        self.args = args
        self.uss = uss
        self.denied = denied
        self.killed = False

    def create_time(self) -> float:
        return self.created

    def name(self) -> str:
        return self.process_name

    def cmdline(self) -> list[str]:
        return list(self.args)

    def is_running(self) -> bool:
        return not self.killed

    def memory_info(self) -> SimpleNamespace:
        return SimpleNamespace(rss=1000)

    def memory_full_info(self) -> SimpleNamespace:
        if self.denied:
            raise DeniedProcess(self.pid)
        return SimpleNamespace() if self.uss is None else SimpleNamespace(uss=self.uss)

    def kill(self) -> None:
        self.killed = True


def boundary(processes: list[FakeProcess]) -> ModuleType:
    result = ModuleType("process_boundary")

    def process(pid: int) -> FakeProcess:
        for item in processes:
            if item.pid == pid:
                return item
        raise MissingProcess(pid)

    result.__dict__.update(
        Process=process,
        process_iter=lambda: iter(processes),
        NoSuchProcess=MissingProcess,
        AccessDenied=DeniedProcess,
        ZombieProcess=MissingProcess,
    )
    return result


def inventory(
    monkeypatch: pytest.MonkeyPatch,
    processes: list[FakeProcess],
    *,
    ownership: Path | None = None,
    groups: dict[int, int] | None = None,
) -> Any:
    measurement = module()
    groups = groups or {1: 1, 2: 1, 3: 99, 4: 99}
    monkeypatch.setattr(measurement.os, "getpgid", lambda pid: groups[pid])
    return measurement._Inventory(
        boundary(processes),
        driver_pid=1,
        ownership=ownership,
        ray=ownership is not None,
    )


def publish(path: Path, *, local: bool = True, created: float = 100.0) -> None:
    path.write_text(
        json.dumps(
            {
                "driver_pid": 1,
                "driver_create_time": 100.0,
                "local_owned": local,
                "session_dir": "/tmp/ray/session_owned",
                "processes": [{"pid": 4, "create_time": created, "role": "actor"}],
                "workers": [{"pid": 4}],
                "dataset_serialized_bytes": 75,
                "object_store_capacity_bytes": 10000,
            }
        )
    )


def test_private_memory_never_sums_shared_rss(monkeypatch: pytest.MonkeyPatch) -> None:
    sampler = inventory(monkeypatch, [FakeProcess(1, uss=20), FakeProcess(2, uss=30)])
    sample = sampler.sample()
    assert sample["private_uss_bytes"] == 50
    assert [row["rss_bytes"] for row in sample["inventory"]] == [1000, 1000]
    assert "total_rss_bytes" not in sample


@pytest.mark.parametrize("denied,uss", [(True, 20), (False, None)])
def test_denied_or_unavailable_uss_invalidates_aggregate(
    monkeypatch: pytest.MonkeyPatch, denied: bool, uss: int | None
) -> None:
    sampler = inventory(
        monkeypatch, [FakeProcess(1), FakeProcess(2, denied=denied, uss=uss)]
    )
    sample = sampler.sample()
    assert sample["private_uss_bytes"] is None
    assert sample["inventory"][1]["uss_bytes"] is None
    assert sample["inventory"][1]["memory_error"]
    assert sample["coverage_complete"] is False


def test_session_and_published_pid_include_non_descendants(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ownership = tmp_path / "owned.json"
    publish(ownership)
    sampler = inventory(
        monkeypatch,
        [
            FakeProcess(1),
            FakeProcess(2, name="gcs_server"),
            FakeProcess(
                3, name="raylet", args=("--session-dir=/tmp/ray/session_owned",)
            ),
            FakeProcess(4, name="ray::Actor"),
            FakeProcess(5, args=("/tmp/ray/session_owned_other/logs",)),
        ],
        ownership=ownership,
        groups={1: 1, 2: 1, 3: 99, 4: 99, 5: 99},
    )
    sample = sampler.sample()
    assert [row["pid"] for row in sample["inventory"]] == [1, 2, 3, 4]
    assert sample["dataset_serialized_bytes"] == 75
    assert sample["object_store_capacity_bytes"] == 10000
    assert sample["object_store_used_bytes"] is None
    assert "dashboard_agent" in sample["missing_roles"]
    assert "runtime_env_agent" in sample["missing_roles"]
    assert sample["coverage_complete"] is False


def test_external_session_is_never_adopted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ownership = tmp_path / "owned.json"
    publish(ownership, local=False)
    sampler = inventory(
        monkeypatch,
        [FakeProcess(1), FakeProcess(3, args=("/tmp/ray/session_owned/logs",))],
        ownership=ownership,
    )
    sample = sampler.sample()
    assert [row["pid"] for row in sample["inventory"]] == [1]
    assert sample["ownership_error"]
    assert sample["private_uss_bytes"] is None


def test_reused_pid_is_never_counted_or_killed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver, worker = FakeProcess(1), FakeProcess(2)
    sampler = inventory(monkeypatch, [driver, worker])
    assert sampler.sample()["private_uss_bytes"] == 20
    worker.created = 101.0
    sample = sampler.sample()
    assert [row["pid"] for row in sample["inventory"]] == [1]
    assert sample["reused_pids"] == [2]
    sampler.cleanup()
    assert driver.killed
    assert not worker.killed


def test_published_pid_creation_time_must_match(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ownership = tmp_path / "owned.json"
    publish(ownership)
    actor = FakeProcess(4, created=101.0)
    sampler = inventory(monkeypatch, [FakeProcess(1), actor], ownership=ownership)
    sample = sampler.sample()
    assert [row["pid"] for row in sample["inventory"]] == [1]
    assert sample["reused_pids"] == [4]
    sampler.cleanup()
    assert not actor.killed


def test_cleanup_does_not_kill_unrelated_same_named_ray_processes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = FakeProcess(1)
    unrelated = FakeProcess(3, name="raylet")
    sampler = inventory(monkeypatch, [driver, unrelated])
    sampler.sample()
    sampler.cleanup()
    assert driver.killed
    assert not unrelated.killed


def test_denied_process_inventory_is_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    sampler = inventory(monkeypatch, [FakeProcess(1)])

    def denied() -> object:
        raise PermissionError("sysctl denied")

    monkeypatch.setattr(sampler.psutil, "process_iter", denied)
    sample = sampler.sample()
    assert sample["private_uss_bytes"] is None
    assert sample["coverage_complete"] is False
    assert sample["discovery_errors"]
    assert sample["denied_observations"] >= 1


def test_unclassified_creation_denial_invalidates_process_sample(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DeniedCreation(FakeProcess):
        def create_time(self) -> float:
            raise DeniedProcess(self.pid)

    sampler = inventory(monkeypatch, [FakeProcess(1, uss=20), DeniedCreation(2)])
    sample = sampler.sample()
    assert sample["discovery_errors"]
    assert sample["denied_observations"] == 1
    assert sample["coverage_complete"] is False
    assert sample["private_uss_bytes"] is None


def test_discovery_denial_then_complete_sample_invalidates_run_peak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    measurement = module()
    driver, worker = FakeProcess(1, uss=20), FakeProcess(2, uss=40)
    psutil = boundary([driver, worker])
    calls = 0

    def process_iter() -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise PermissionError("sysctl temporarily denied")
        return iter([driver, worker])

    monkeypatch.setattr(psutil, "process_iter", process_iter)
    monkeypatch.setattr(measurement, "_require_psutil", lambda: psutil)
    monkeypatch.setattr(measurement.os, "getpgid", lambda pid: 1)

    class Command:
        pid = 1
        waits = 0

        def wait(self, timeout: float | None = None) -> int:
            self.waits += 1
            if self.waits == 1:
                assert timeout is not None
                raise subprocess.TimeoutExpired("fake", timeout)
            return 0

        def poll(self) -> int:
            return 0

    monkeypatch.setattr(measurement.subprocess, "Popen", lambda *a, **k: Command())
    run = measurement.measure_command(
        ["fake"], stdout=tmp_path / "stdout", stderr=tmp_path / "stderr"
    )
    assert run.denied_observations == 1
    assert any(sample["private_uss_bytes"] == 60 for sample in run.samples)
    assert all(
        row["uss_bytes"] is not None
        for sample in run.samples
        for row in sample["inventory"]
    )
    assert run.peak_private_uss_bytes is None


def test_normal_ray_readiness_gap_does_not_invalidate_later_complete_sample_peak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    measurement = module()
    ownership = tmp_path / "ownership.json"
    live = [
        FakeProcess(1),
        FakeProcess(2, name="gcs_server"),
        FakeProcess(4, name="ray::Actor"),
        FakeProcess(5, name="raylet"),
        FakeProcess(6, args=("dashboard/agent.py",)),
        FakeProcess(7, args=("runtime_env/agent/main.py",)),
    ]
    monkeypatch.setattr(measurement, "_require_psutil", lambda: boundary(live))
    monkeypatch.setattr(measurement.os, "getpgid", lambda pid: 1)

    class Command:
        pid = 1
        waits = 0

        def poll(self) -> int:
            return 0

        def wait(self, timeout: float | None = None) -> int:
            self.waits += 1
            if self.waits == 1:
                publish(ownership)
                assert timeout is not None
                raise subprocess.TimeoutExpired("fake", timeout)
            return 0

    monkeypatch.setattr(measurement.subprocess, "Popen", lambda *a, **k: Command())
    run = measurement.measure_command(
        ["fake"],
        stdout=tmp_path / "stdout",
        stderr=tmp_path / "stderr",
        ownership=ownership,
        ray=True,
    )
    assert run.samples[0]["coverage_complete"] is False
    assert run.samples[0]["ownership_error"]
    assert not run.samples[0]["discovery_errors"]
    assert run.samples[1]["coverage_complete"] is True
    assert run.peak_private_uss_bytes == 60


def test_recycled_trial_pgid_never_admits_replacement_members(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    measurement = module()
    driver = FakeProcess(1)
    live = [driver]
    psutil = boundary(live)
    monkeypatch.setattr(measurement.os, "getpgid", lambda pid: 1)
    sampler = measurement._Inventory(psutil, driver_pid=1, ownership=None, ray=False)
    sampler.sample()
    driver.created = 101.0
    replacement = FakeProcess(2, created=102.0)
    live.append(replacement)
    sample = sampler.sample()
    assert sample["reused_pids"] == [1]
    assert not sample["inventory"]
    sampler.cleanup()
    assert not driver.killed
    assert not replacement.killed


def test_surviving_original_group_member_anchors_new_owned_members(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    measurement = module()
    driver, worker = FakeProcess(1), FakeProcess(2)
    live = [driver, worker]
    psutil = boundary(live)
    monkeypatch.setattr(measurement.os, "getpgid", lambda pid: 1)
    sampler = measurement._Inventory(psutil, driver_pid=1, ownership=None, ray=False)
    sampler.sample()
    live.remove(driver)
    new_worker = FakeProcess(3, created=101.0)
    live.append(new_worker)
    sample = sampler.sample()
    assert [row["pid"] for row in sample["inventory"]] == [2, 3]
    sampler.cleanup()
    assert worker.killed and new_worker.killed


def test_session_proof_remains_usable_after_original_group_anchor_exits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ownership = tmp_path / "ownership.json"
    publish(ownership)
    driver = FakeProcess(1)
    live = [driver]
    sampler = inventory(monkeypatch, live, ownership=ownership)
    sampler.sample()
    live.remove(driver)
    session_owned = FakeProcess(3, created=101.0, args=("/tmp/ray/session_owned/logs",))
    unrelated = FakeProcess(2, created=101.0)
    live.extend([session_owned, unrelated])
    sample = sampler.sample()
    assert [row["pid"] for row in sample["inventory"]] == [3]
    sampler.cleanup()
    assert session_owned.killed
    assert not unrelated.killed


def test_owned_process_discovery_denial_invalidates_memory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DeniedCommand(FakeProcess):
        def cmdline(self) -> list[str]:
            raise DeniedProcess(self.pid)

    sampler = inventory(monkeypatch, [FakeProcess(1), DeniedCommand(2)])
    sample = sampler.sample()
    assert sample["private_uss_bytes"] is None
    assert sample["denied_observations"] == 1
    assert [row["pid"] for row in sample["inventory"]] == [1, 2]


def test_pid_reused_between_discovery_and_memory_read_is_excluded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ChangingProcess(FakeProcess):
        calls = 0

        def create_time(self) -> float:
            self.calls += 1
            return 100.0 if self.calls == 1 else 101.0

    changing = ChangingProcess(2)
    sampler = inventory(monkeypatch, [FakeProcess(1), changing])
    sample = sampler.sample()
    assert sample["reused_pids"] == [2]
    assert [row["pid"] for row in sample["inventory"]] == [1]
    sampler.cleanup()
    assert not changing.killed


def test_missing_published_actor_identity_is_a_coverage_gap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ownership = tmp_path / "owned.json"
    publish(ownership)
    record = json.loads(ownership.read_text())
    record["workers"].append({"pid": 6})
    ownership.write_text(json.dumps(record))
    sampler = inventory(
        monkeypatch, [FakeProcess(1), FakeProcess(4)], ownership=ownership
    )
    sample = sampler.sample()
    assert sample["missing_actor_pids"] == [6]
    assert sample["private_uss_bytes"] is None


@pytest.mark.parametrize(
    "script,role",
    [
        ("dashboard/agent.py", "dashboard_agent"),
        ("runtime_env/agent/main.py", "runtime_env_agent"),
    ],
)
def test_agents_with_raylet_arguments_are_classified_as_agents(
    monkeypatch: pytest.MonkeyPatch, script: str, role: str
) -> None:
    sampler = inventory(
        monkeypatch,
        [
            FakeProcess(1),
            FakeProcess(2, args=(script, "--raylet-name=/tmp/ray/socket")),
        ],
    )
    assert sampler.sample()["inventory"][1]["role"] == role


def test_raylet_with_embedded_agent_commands_is_classified_as_raylet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sampler = inventory(
        monkeypatch,
        [
            FakeProcess(1),
            FakeProcess(
                2,
                name="raylet",
                args=(
                    "--dashboard_agent_command=python dashboard/agent.py",
                    "--runtime_env_agent_command=python runtime_env/agent/main.py",
                ),
            ),
        ],
    )
    assert sampler.sample()["inventory"][1]["role"] == "raylet"


def test_old_unrelated_process_is_excluded_before_denied_command_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class OldDeniedCommand(FakeProcess):
        def cmdline(self) -> list[str]:
            raise AssertionError("older unrelated process must never be inspected")

    ownership = tmp_path / "ownership.json"
    publish(ownership)
    sampler = inventory(
        monkeypatch,
        [FakeProcess(1), OldDeniedCommand(3, created=99.0)],
        ownership=ownership,
    )
    assert not sampler.sample()["discovery_errors"]


def test_denied_published_actor_creation_time_has_safe_inventory_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class DeniedCreation(FakeProcess):
        def create_time(self) -> float:
            raise DeniedProcess(self.pid)

    ownership = tmp_path / "ownership.json"
    publish(ownership)
    sampler = inventory(monkeypatch, [DeniedCreation(4)], ownership=ownership)
    # Driver creation time is known from the launch identity; discovery can fail.
    sampler.driver_created = 100.0
    sample = sampler.sample()
    assert sample["inventory"][0]["pid"] == 4
    assert sample["inventory"][0]["create_time"] == 100.0
    assert sample["inventory"][0]["memory_error"]
    assert sample["private_uss_bytes"] is None


def test_unexpected_sampler_error_still_cleans_verified_owned_identities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    measurement = module()
    root = FakeProcess(1)

    class BrokenMemory(FakeProcess):
        def memory_full_info(self) -> SimpleNamespace:
            raise RuntimeError("unexpected memory failure")

    worker = BrokenMemory(2)
    sampler_boundary = boundary([root, worker])
    monkeypatch.setattr(measurement, "_require_psutil", lambda: sampler_boundary)
    monkeypatch.setattr(measurement.os, "getpgid", lambda pid: 1)

    class FakeCommand:
        pid = 1

        def poll(self) -> int:
            return 0

        def wait(self, timeout: float | None = None) -> int:
            return 0

    monkeypatch.setattr(
        measurement.subprocess, "Popen", lambda *args, **kwargs: FakeCommand()
    )
    with pytest.raises(RuntimeError, match="unexpected memory failure"):
        measurement.measure_command(
            ["test-command"], stdout=tmp_path / "stdout", stderr=tmp_path / "stderr"
        )
    assert root.killed
    assert worker.killed


@pytest.mark.parametrize("interval", [0, -1, float("nan"), float("inf"), True])
def test_invalid_interval_creates_no_files(tmp_path: Path, interval: float) -> None:
    with pytest.raises(ValueError, match="interval_seconds"):
        module().measure_command(
            [sys.executable, "-c", "pass"],
            stdout=tmp_path / "stdout",
            stderr=tmp_path / "stderr",
            interval_seconds=interval,
        )
    assert not list(tmp_path.iterdir())


def test_optional_psutil_is_lazy_and_missing_fails_before_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    measurement = module()
    monkeypatch.setitem(sys.modules, "psutil", None)
    with pytest.raises(RuntimeError, match="benchmark"):
        measurement.measure_command(
            [sys.executable, "-c", "pass"],
            stdout=tmp_path / "stdout",
            stderr=tmp_path / "stderr",
        )
    assert not list(tmp_path.iterdir())


@pytest.mark.skipif(os.name != "posix", reason="POSIX ownership inventory")
def test_real_subprocess_timing_includes_exit_and_preserves_arguments(
    tmp_path: Path,
) -> None:
    pytest.importorskip("psutil")
    argument = "$(echo do-not-run); literal *"
    run = module().measure_command(
        [
            sys.executable,
            "-c",
            "import sys,time; print(sys.argv[1]); time.sleep(.1); sys.exit(7)",
            argument,
        ],
        stdout=tmp_path / "stdout",
        stderr=tmp_path / "stderr",
        interval_seconds=0.02,
    )
    assert run.returncode == 7
    assert run.elapsed_seconds >= 0.1
    assert (tmp_path / "stdout").read_text() == argument + "\n"
    assert run.samples
    assert all("inventory" in sample for sample in run.samples)
    assert all(
        sample["elapsed_seconds"] <= run.elapsed_seconds for sample in run.samples
    )

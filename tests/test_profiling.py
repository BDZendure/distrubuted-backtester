import cProfile
import importlib
import importlib.util
import json
import pstats
import sys
import tracemalloc
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest

from distributed_backtester import experiments, simulation
from distributed_backtester.backends import sequential
from distributed_backtester.data import load_csv
from distributed_backtester.jobs import canonical_json, config_record, create_job
from distributed_backtester.provenance import ImplementationIdentity
from distributed_backtester.results import result_to_dict
from distributed_backtester.strategy import MovingAverageCrossover
from distributed_backtester.workloads import job_configs


@pytest.fixture
def profiling(monkeypatch: pytest.MonkeyPatch) -> Any:
    if importlib.util.find_spec("distributed_backtester.profiling") is None:
        pytest.skip("API and CLI availability tests cover the missing module")
    module = importlib.import_module("distributed_backtester.profiling")
    identity = ImplementationIdentity(simulation.ENGINE_ID, "a" * 64)
    monkeypatch.setattr(module, "current_implementation", lambda: identity)
    monkeypatch.setattr(module, "source_fingerprint", lambda: identity.source_sha256)
    monkeypatch.setattr(sequential, "current_implementation", lambda: identity)
    environment = {"python_version": "test", "distributions": []}
    monkeypatch.setattr(module, "capture_environment", lambda: environment)
    monkeypatch.setattr(experiments, "capture_environment", lambda: environment)
    return module


@pytest.mark.parametrize("entry", ["profile_workload", "main"])
def test_profiler_api_and_cli_are_available(entry: str) -> None:
    assert importlib.util.find_spec("distributed_backtester.profiling") is not None, (
        "Phase 6 profiler module is missing"
    )
    module = importlib.import_module("distributed_backtester.profiling")
    assert callable(getattr(module, entry, None))


def read_report(output: Path) -> dict[str, Any]:
    return json.loads((output / "report.json").read_text())  # type: ignore[no-any-return]


def invoke(profiling: Any, output: Path, **options: Any) -> dict[str, Any]:
    return profiling.profile_workload(  # type: ignore[no-any-return]
        output, **{"bars": 8, "jobs": 2, "kind": "timing", **options}
    )


@pytest.mark.parametrize(
    "options",
    [
        {name: value}
        for name in ("bars", "jobs", "repetitions")
        for value in (True, False, 0, -1, 1.0, "1", None)
    ]
    + [{"seed": value} for value in (True, False, -1, 1.0, "1", None)]
    + [
        {"bars": 3_000_000},
        {"kind": "allocation"},
        {"kind": None},
        {"scope": "cluster"},
        {"scope": None},
        {"kind": "cpu", "repetitions": 2},
        {"kind": "memory", "repetitions": 2},
    ],
)
def test_invalid_arguments_create_no_output(
    profiling: Any, tmp_path: Path, options: dict[str, Any]
) -> None:
    output = tmp_path / "profile"
    with pytest.raises(ValueError):
        invoke(profiling, output, **options)
    assert not output.exists()


def test_fresh_and_protected_destination(profiling: Any, tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    package = root / "src/distributed_backtester"
    existing = tmp_path / "existing"
    existing.mkdir()
    marker = existing / "marker"
    marker.write_text("keep")
    alias = tmp_path / "alias"
    alias.symlink_to(package, target_is_directory=True)
    dangling = tmp_path / "dangling"
    dangling.symlink_to(tmp_path / "missing")
    for output in (
        existing,
        dangling,
        package / "profile-output",
        root / "profile-output",
        root / ".git/profile-output",
        alias / "profile-output",
    ):
        with pytest.raises(ValueError, match=r"fresh|protected"):
            invoke(profiling, output)
    assert marker.read_text() == "keep"
    with pytest.raises(ValueError, match="Path"):
        invoke(profiling, str(tmp_path / "invalid"))  # type: ignore[arg-type]


@pytest.mark.parametrize("scope", ["simulation", "experiment"])
def test_timing_repeats_complete_digest_and_metadata(
    profiling: Any, tmp_path: Path, scope: str
) -> None:
    output = tmp_path / "timing"
    report = invoke(profiling, output, scope=scope, repetitions=3, seed=7)
    assert report == read_report(output)
    assert report["schema_version"] == 1
    assert report["status"] == "succeeded"
    assert report["kind"] == "timing"
    assert report["scope"] == scope
    assert report["source_unchanged"] is True
    assert report["semantic_outputs_equal"] is True
    assert report["implementation"]["source_sha256"] == "a" * 64
    assert report["environment"]["python_version"] == "test"
    configs = sorted(job_configs(2), key=lambda c: canonical_json(config_record(c)))
    dataset = load_csv(output / "data.csv", symbol="BENCH", price_basis="raw")
    records = [result_to_dict(simulation.run_backtest(c, dataset)) for c in configs]
    expected = sha256(canonical_json(records).encode()).hexdigest()
    assert report["semantic_sha256"] == expected
    workload = report["workload"]
    assert workload["configs"] == [config_record(c) for c in configs]
    assert (
        workload["config_sha256"]
        == sha256(canonical_json(workload["configs"]).encode()).hexdigest()
    )
    assert (
        workload["source_sha256"]
        == sha256((output / "data.csv").read_bytes()).hexdigest()
    )
    assert workload["fee_bps"] == 10
    assert workload["symbol"] == "BENCH"
    assert workload["price_basis"] == "raw"
    assert workload["seed"] == 7
    assert report["method"]["instrumented"] is False
    assert (
        report["workload_sha256"]
        == sha256(canonical_json(workload).encode()).hexdigest()
    )
    assert "own" in report["method"]["cpu_scope"]
    assert "interpreter" in report["method"]["cpu_scope"]
    assert len(report["runs"]) == 3
    for row in report["runs"]:
        assert row["semantic_sha256"] == expected
        assert row["elapsed_seconds"] >= 0
        assert row["cpu_seconds"] >= 0
        assert row["cpu_scope"] == "own_process_this_interpreter"
        assert row["memory"]["scope"] == "own_process_lifetime_high_water"
        assert "cpu_profile" not in row and "allocations" not in row
    if scope == "experiment":
        for row in report["runs"]:
            assert (output / row["experiment"] / "index.json").is_file()


def test_simulation_repeats_use_fresh_strategy_and_hash_after_clock(
    profiling: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    strategies: list[Any] = []
    original_strategy = MovingAverageCrossover
    original_export = profiling.result_to_dict
    ticks: list[str] = []

    def strategy(config: Any) -> Any:
        instance = original_strategy(config)
        strategies.append(instance)
        return instance

    def wall() -> float:
        ticks.append("wall")
        return float(len(ticks))

    def cpu() -> float:
        ticks.append("cpu")
        return float(len(ticks))

    def export(result: Any) -> dict[str, object]:
        assert ticks[-4:] == ["wall", "cpu", "cpu", "wall"]
        assert sys.getprofile() is None
        return original_export(result)  # type: ignore[no-any-return]

    monkeypatch.setattr(simulation, "MovingAverageCrossover", strategy)
    monkeypatch.setattr(profiling, "perf_counter", wall)
    monkeypatch.setattr(profiling, "process_time", cpu)
    monkeypatch.setattr(profiling, "result_to_dict", export)
    report = invoke(profiling, tmp_path / "fresh", repetitions=3)
    assert len(strategies) == len({id(s) for s in strategies}) == 6
    assert all(row["cpu_seconds"] == 1 for row in report["runs"])
    assert all(row["elapsed_seconds"] == 3 for row in report["runs"])


def test_cpu_records_match_raw_stats(profiling: Any, tmp_path: Path) -> None:
    output = tmp_path / "cpu"
    report = invoke(profiling, output, kind="cpu")
    profile = report["runs"][0]["cpu_profile"]
    stats = pstats.Stats(str(output / profile["raw_stats"]))
    raw_stats: dict[tuple[str, int, str], Any] = cast(Any, stats).stats
    assert report["method"]["instrumented"] is True
    assert profile["functions"]
    records = {(r["filename"], r["line"], r["name"]): r for r in profile["functions"]}
    assert set(records) == set(raw_stats)
    for key, values in raw_stats.items():
        primitive, total, self_seconds, cumulative, _ = values
        record = records[key]
        assert record["primitive_calls"] == primitive
        assert record["total_calls"] == total
        assert record["self_seconds"] == self_seconds
        assert record["cumulative_seconds"] == cumulative
    assert any(key[2] == "run_backtest" for key in records)
    assert not any(key[2] == "result_to_dict" for key in records)
    assert sys.getprofile() is None


def test_memory_peak_before_snapshot_and_retained_results(
    profiling: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_run = profiling.run_backtest
    original_snapshot = tracemalloc.take_snapshot
    snapshots: list[bytearray] = []

    def run(*args: Any, **kwargs: Any) -> Any:
        transient = bytearray(500_000)
        assert transient[0] == 0
        return original_run(*args, **kwargs)

    def snapshot() -> tracemalloc.Snapshot:
        if snapshots:
            snapshots.append(bytearray(2_000_000))
        else:
            snapshots.append(bytearray(0))
        return original_snapshot()

    monkeypatch.setattr(profiling, "run_backtest", run)
    monkeypatch.setattr(tracemalloc, "take_snapshot", snapshot)
    report = invoke(profiling, tmp_path / "memory", kind="memory")
    allocation = report["runs"][0]["allocations"]
    assert 500_000 <= allocation["peak_traced_bytes"] < 2_000_000
    assert 0 < allocation["current_traced_bytes"] < allocation["peak_traced_bytes"]
    assert allocation["live_differences"]
    for record in allocation["live_differences"]:
        assert {
            "traceback",
            "size_diff_bytes",
            "count_diff",
            "size_bytes",
            "count",
        } <= record.keys()
    assert "retained" in report["method"]["memory_scope"]
    assert "live" in report["method"]["memory_scope"]
    assert "frequency" in report["method"]["memory_scope"]
    assert not tracemalloc.is_tracing()


def test_existing_tracer_rejected_without_output(
    profiling: Any, tmp_path: Path
) -> None:
    tracemalloc.start()
    try:
        output = tmp_path / "tracing"
        with pytest.raises(ValueError, match="tracing"):
            invoke(profiling, output, kind="memory")
        assert not output.exists()
        assert tracemalloc.is_tracing()
    finally:
        tracemalloc.stop()


@pytest.mark.parametrize(
    "field",
    [
        "fills",
        "order_outcomes",
        "equity",
        "final_portfolio",
        "metrics",
        "config",
        "dataset",
        "engine_id",
    ],
)
def test_repetitions_reject_any_full_result_difference(
    profiling: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    original = profiling.result_to_dict
    count = 0

    def altered(result: Any) -> dict[str, Any]:
        nonlocal count
        record = original(result)
        count += 1
        if count > 2:
            record[field] = {"changed": True}
        return record  # type: ignore[no-any-return]

    monkeypatch.setattr(profiling, "result_to_dict", altered)
    output = tmp_path / "different"
    with pytest.raises(RuntimeError, match="differ"):
        invoke(profiling, output, repetitions=2)
    report = read_report(output)
    assert report["status"] == "failed"
    assert report["semantic_outputs_equal"] is False
    assert "semantic_sha256" not in report


@pytest.mark.parametrize("when", ["generation", "simulation", "hashing"])
def test_source_change_fails_closed_with_terminal_report(
    profiling: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, when: str
) -> None:
    changed = False
    name = {
        "generation": "write_synthetic_csv",
        "simulation": "run_backtest",
        "hashing": "result_to_dict",
    }[when]
    original = getattr(profiling, name)

    def change(*args: Any, **kwargs: Any) -> Any:
        nonlocal changed
        value = original(*args, **kwargs)
        changed = True
        return value

    monkeypatch.setattr(profiling, name, change)
    monkeypatch.setattr(
        profiling, "source_fingerprint", lambda: ("b" if changed else "a") * 64
    )
    output = tmp_path / "changed"
    with pytest.raises(RuntimeError, match="source"):
        invoke(profiling, output)
    report = read_report(output)
    assert report["source_unchanged"] is False
    assert report["status"] == "failed"
    assert "semantic_sha256" not in report


@pytest.mark.parametrize("kind", ["cpu", "memory", "timing"])
@pytest.mark.parametrize("error_type", [RuntimeError, KeyboardInterrupt, SystemExit])
def test_failure_cleanup_and_persistence(
    profiling: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    error_type: type[BaseException],
) -> None:
    error = error_type("interrupted workload")

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise error

    monkeypatch.setattr(profiling, "run_backtest", fail)
    output = tmp_path / "failed"
    with pytest.raises(error_type) as caught:
        invoke(profiling, output, kind=kind)
    assert caught.value is error
    assert sys.getprofile() is None
    assert not tracemalloc.is_tracing()
    report = read_report(output)
    assert report["status"] == "failed"
    assert report["error"] == {"error_type": error_type.__name__, "message": str(error)}
    assert report["finished_utc"]
    assert report["semantic_outputs_equal"] is False
    assert "semantic_sha256" not in report


@pytest.mark.parametrize("tamper", ["artifact", "implementation", "config", "failed"])
def test_experiment_verifies_full_snapshot_after_measurement(
    profiling: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tamper: str
) -> None:
    original = profiling.collect_experiment

    def altered(*args: Any, **kwargs: Any) -> Any:
        arguments = list(args)
        if tamper == "config":
            arguments[0] = tuple(
                create_job(job_configs(3)[index + 1], job.dataset, job.implementation)
                for index, job in enumerate(arguments[0])
            )
        summary = original(*arguments, **kwargs)
        output = arguments[3]
        if tamper == "artifact":
            next((output / "results").glob("*.json")).write_text("{}\n")
        elif tamper == "implementation":
            manifest = json.loads((output / "manifest.json").read_text())
            manifest["implementation"]["source_sha256"] = "b" * 64
            (output / "manifest.json").write_text(canonical_json(manifest) + "\n")
        elif tamper == "failed":
            return replace(summary, failed=1)
        return summary

    monkeypatch.setattr(profiling, "collect_experiment", altered)
    output = tmp_path / "corrupt"
    with pytest.raises((ValueError, RuntimeError)):
        invoke(profiling, output, scope="experiment")
    assert read_report(output)["status"] == "failed"
    assert "semantic_sha256" not in read_report(output)


@pytest.mark.parametrize("scope", ["simulation", "experiment"])
@pytest.mark.parametrize("kind", ["cpu", "memory", "timing"])
def test_cli_runs_scoped_small_workload(
    profiling: Any,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    scope: str,
    kind: str,
) -> None:
    output = tmp_path / "cli"
    assert (
        profiling.main(
            [
                "run",
                "--bars",
                "8",
                "--jobs",
                "2",
                "--kind",
                kind,
                "--scope",
                scope,
                "--output-dir",
                str(output),
            ]
        )
        == 0
    )
    assert read_report(output)["status"] == "succeeded"
    assert "succeeded" in capsys.readouterr().out


def test_cli_invalid_api_input_returns_error(
    profiling: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    output = tmp_path / "cli"
    assert (
        profiling.main(
            [
                "run",
                "--bars",
                "0",
                "--jobs",
                "2",
                "--kind",
                "cpu",
                "--output-dir",
                str(output),
            ]
        )
        == 2
    )
    assert "error:" in capsys.readouterr().err
    assert not output.exists()


def test_timing_does_not_enable_instrumentation(
    profiling: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import cProfile

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("timing enabled instrumentation")

    monkeypatch.setattr(cProfile, "Profile", forbidden)
    monkeypatch.setattr(tracemalloc, "start", forbidden)
    invoke(profiling, tmp_path / "plain")


def test_experiment_constructs_fresh_jobs_inside_clock_and_verifies_outside(
    profiling: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_configs = profiling.job_configs
    original_digest = profiling._experiment_digest
    ticks = 0
    config_ticks: list[int] = []

    def clock() -> float:
        nonlocal ticks
        ticks += 1
        return float(ticks)

    def configs(*args: Any, **kwargs: Any) -> Any:
        config_ticks.append(ticks)
        return original_configs(*args, **kwargs)

    def digest(*args: Any, **kwargs: Any) -> Any:
        assert ticks in (4, 8)
        assert not tracemalloc.is_tracing()
        assert sys.getprofile() is None
        return original_digest(*args, **kwargs)

    monkeypatch.setattr(profiling, "perf_counter", clock)
    monkeypatch.setattr(profiling, "process_time", clock)
    monkeypatch.setattr(profiling, "job_configs", configs)
    monkeypatch.setattr(profiling, "_experiment_digest", digest)
    invoke(profiling, tmp_path / "jobs", scope="experiment", repetitions=2)
    assert config_ticks == [0, 2, 6]


def test_changed_generated_data_fails_identity_guard(
    profiling: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "changed-data"
    original = profiling.run_backtest

    def mutate(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        (output / "data.csv").write_text("changed dataset bytes\n")
        return result

    monkeypatch.setattr(profiling, "run_backtest", mutate)
    with pytest.raises(RuntimeError, match="data"):
        invoke(profiling, output)
    report = read_report(output)
    assert report["status"] == "failed"
    assert "semantic_sha256" not in report


@pytest.mark.parametrize("kind", ["cpu", "memory"])
def test_instrumentation_setup_failure_still_cleans_up(
    profiling: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    def stop(*args: Any, **kwargs: Any) -> Any:
        raise KeyboardInterrupt("setup interrupted")

    if kind == "memory":
        monkeypatch.setattr(tracemalloc, "take_snapshot", stop)
    else:
        monkeypatch.setattr(profiling, "perf_counter", stop)
    output = tmp_path / "setup"
    with pytest.raises(KeyboardInterrupt, match="setup interrupted"):
        invoke(profiling, output, kind=kind)
    assert not tracemalloc.is_tracing()
    assert sys.getprofile() is None
    assert read_report(output)["error"]["error_type"] == "KeyboardInterrupt"


@pytest.mark.parametrize("scope", ["simulation", "experiment"])
@pytest.mark.parametrize("hook", ["tracemalloc", "profile", "trace"])
def test_timing_rejects_caller_instrumentation_before_output(
    profiling: Any, tmp_path: Path, scope: str, hook: str
) -> None:
    output = tmp_path / "caller"

    def callback(*args: Any) -> Any:
        return callback

    previous_profile, previous_trace = sys.getprofile(), sys.gettrace()
    try:
        if hook == "tracemalloc":
            tracemalloc.start()
        elif hook == "profile":
            sys.setprofile(callback)
        else:
            sys.settrace(callback)
        with pytest.raises(ValueError, match=r"tracing|profil"):
            invoke(profiling, output, jobs=1, scope=scope)
        assert not output.exists()
        if hook == "tracemalloc":
            assert tracemalloc.is_tracing()
        elif hook == "profile":
            assert sys.getprofile() is callback
        else:
            assert sys.gettrace() is callback
    finally:
        if hook == "tracemalloc":
            tracemalloc.stop()
        sys.setprofile(previous_profile)
        sys.settrace(previous_trace)


def inject_secondary_failure(
    profiling: Any,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
    secondary: BaseException,
) -> str:
    def fail(*args: Any, **kwargs: Any) -> Any:
        raise secondary

    if stage in {"cpu_stats", "cpu_disable"}:
        original_profile = cProfile.Profile

        class FaultyProfiler:
            def __init__(self) -> None:
                self.inner = original_profile()

            def enable(self) -> None:
                self.inner.enable()

            def disable(self) -> None:
                self.inner.disable()
                if stage == "cpu_disable":
                    raise secondary

            def dump_stats(self, filename: str) -> None:
                if stage == "cpu_stats":
                    raise secondary
                self.inner.dump_stats(filename)

            def getstats(self) -> Any:
                return self.inner.getstats()

        monkeypatch.setattr(cProfile, "Profile", FaultyProfiler)
        return "cpu"
    if stage == "rss":
        monkeypatch.setattr(profiling, "peak_rss", fail)
        return "timing"
    if stage == "cpu_clock":
        calls = 0

        def clock() -> float:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise secondary
            return 0.0

        monkeypatch.setattr(profiling, "process_time", clock)
        return "timing"
    if stage == "memory_snapshot":
        original_snapshot = tracemalloc.take_snapshot
        calls = 0

        def snapshot() -> tracemalloc.Snapshot:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise secondary
            return original_snapshot()

        monkeypatch.setattr(tracemalloc, "take_snapshot", snapshot)
    elif stage == "memory_diff":
        monkeypatch.setattr(tracemalloc.Snapshot, "compare_to", fail)
    elif stage == "memory_stop":
        original_stop = tracemalloc.stop

        def stop() -> None:
            original_stop()
            raise secondary

        monkeypatch.setattr(tracemalloc, "stop", stop)
    else:
        raise AssertionError(f"unknown failure stage: {stage}")
    return "memory"


@pytest.mark.parametrize("error_type", [KeyboardInterrupt, SystemExit, RuntimeError])
@pytest.mark.parametrize(
    "stage",
    [
        "cpu_stats",
        "cpu_disable",
        "rss",
        "cpu_clock",
        "memory_snapshot",
        "memory_diff",
        "memory_stop",
    ],
)
def test_primary_workload_exception_survives_secondary_observation_failure(
    profiling: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[BaseException],
    stage: str,
) -> None:
    primary = error_type("primary workload interrupt")
    secondary = OSError(f"secondary {stage} failed")
    kind = inject_secondary_failure(profiling, monkeypatch, stage, secondary)

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise primary

    monkeypatch.setattr(profiling, "run_backtest", fail)
    output = tmp_path / "combined"
    with pytest.raises(error_type) as caught:
        invoke(profiling, output, jobs=1, kind=kind)
    assert caught.value is primary
    assert sys.getprofile() is None
    assert not tracemalloc.is_tracing()
    report = read_report(output)
    assert report["status"] == "failed"
    assert report["error"] == {
        "error_type": error_type.__name__,
        "message": str(primary),
    }
    assert report["semantic_outputs_equal"] is False
    assert "semantic_sha256" not in report
    diagnostics = report["runs"][0]["diagnostics"]
    assert any(d["message"] == str(secondary) for d in diagnostics)
    assert any(str(secondary) in note for note in primary.__notes__)


@pytest.mark.parametrize("error_type", [KeyboardInterrupt, SystemExit, RuntimeError])
def test_primary_exception_survives_failed_report_publication(
    profiling: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[BaseException],
) -> None:
    primary = error_type("primary workload interrupt")
    secondary = OSError("report disk full")

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise primary

    def publication(*args: Any, **kwargs: Any) -> Any:
        raise secondary

    monkeypatch.setattr(profiling, "run_backtest", fail)
    monkeypatch.setattr(profiling, "write_text", publication)
    output = tmp_path / "unwritable"
    with pytest.raises(error_type) as caught:
        invoke(profiling, output, jobs=1)
    assert caught.value is primary
    assert not (output / "report.json").exists()
    assert any(
        "report publication" in note and str(secondary) in note
        for note in primary.__notes__
    )


def test_successful_workload_publication_failure_propagates(
    profiling: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secondary = OSError("report disk full")

    def publication(*args: Any, **kwargs: Any) -> Any:
        raise secondary

    monkeypatch.setattr(profiling, "write_text", publication)
    output = tmp_path / "successful-unwritable"
    with pytest.raises(OSError) as caught:
        invoke(profiling, output, jobs=1)
    assert caught.value is secondary
    assert not (output / "report.json").exists()


def test_tracer_activation_interruption_records_ownership_before_start(
    profiling: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary = KeyboardInterrupt("interrupted activation")
    original_start = tracemalloc.start

    def start() -> None:
        original_start()
        raise primary

    monkeypatch.setattr(tracemalloc, "start", start)
    output = tmp_path / "activation"
    try:
        with pytest.raises(KeyboardInterrupt) as caught:
            invoke(profiling, output, jobs=1, kind="memory")
        assert caught.value is primary
        assert not tracemalloc.is_tracing()
        assert read_report(output)["error"]["error_type"] == "KeyboardInterrupt"
    finally:
        tracemalloc.stop()


def test_setup_exception_survives_secondary_rss_failure(
    profiling: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary = KeyboardInterrupt("primary setup interrupt")
    secondary = SystemExit("secondary RSS interrupt")

    def setup() -> float:
        raise primary

    def observation() -> dict[str, object]:
        raise secondary

    monkeypatch.setattr(profiling, "perf_counter", setup)
    monkeypatch.setattr(profiling, "peak_rss", observation)
    output = tmp_path / "setup-and-rss"
    with pytest.raises(KeyboardInterrupt) as caught:
        invoke(profiling, output, jobs=1, kind="cpu")
    assert caught.value is primary
    assert sys.getprofile() is None
    assert read_report(output)["error"]["error_type"] == "KeyboardInterrupt"
    assert any(str(secondary) in note for note in primary.__notes__)

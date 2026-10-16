import json
from pathlib import Path

import pytest

from distributed_backtester.backends.sequential import SequentialBackend
from distributed_backtester.benchmark import (
    run_benchmarks,
    semantic_digest,
    summarize,
    verify_trial_identity,
)
from distributed_backtester.experiments import run_experiment


def test_summary_uses_medians_and_observed_ranges() -> None:
    rows = [
        {
            "configuration": name,
            "workers": workers,
            "elapsed_seconds": seconds,
            "peak_tree_rss_bytes": 100,
            "peak_coordinator_rss_bytes": 50,
        }
        for name, workers, times in [
            ("sequential", 1, [9.0, 3.0, 6.0]),
            ("process-2", 2, [4.0, 2.0, 3.0]),
        ]
        for seconds in times
    ]
    summary = summarize(rows, jobs=12)
    assert summary[0]["median_seconds"] == 6.0
    assert summary[0]["min_seconds"] == 3.0
    assert summary[0]["max_seconds"] == 9.0
    assert summary[1]["jobs_per_second"] == 4.0
    assert summary[1]["speedup"] == 2.0
    assert summary[1]["parallel_efficiency"] == 1.0


def test_trial_cannot_change_report_implementation(tmp_path: Path) -> None:
    expected = {"engine_id": "engine", "source_sha256": "a" * 64}
    (tmp_path / "manifest.json").write_text(
        json.dumps({"implementation": {**expected, "source_sha256": "b" * 64}})
    )
    with pytest.raises(RuntimeError, match="implementation"):
        verify_trial_identity(tmp_path, expected)


def test_verified_digest_detects_changed_artifact(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[1] / "examples/sma.csv"
    output = tmp_path / "experiment"
    run_experiment(
        source,
        symbol="SYNTH",
        fast=[1],
        slow=[3],
        initial_cash=1_000_000_000,
        fee_bps=10,
        output=output,
        backend=SequentialBackend(),
    )
    first = semantic_digest(output)
    assert first == semantic_digest(output)
    index = json.loads((output / "index.json").read_text())
    artifact = output / index["outcomes"][0]["artifact"]
    artifact.write_text("corrupted\n")
    with pytest.raises(RuntimeError, match="digest"):
        semantic_digest(output)


@pytest.mark.parametrize(
    "bars,jobs,repetitions,workers",
    [
        (0, 1, 1, []),
        (1, 0, 1, []),
        (1, 1, 0, []),
        (1, 1, 1, [0]),
        (1, 1, 1, [2, 2]),
        (True, 1, 1, []),
    ],
)
def test_invalid_benchmark_creates_no_output(
    tmp_path: Path, bars: int, jobs: int, repetitions: int, workers: list[int]
) -> None:
    output = tmp_path / "benchmark"
    with pytest.raises(ValueError):
        run_benchmarks(
            output, bars=bars, jobs=jobs, repetitions=repetitions, workers=workers
        )
    assert not output.exists()


def test_real_benchmark_runs_fresh_sequential_and_spawn_trials(tmp_path: Path) -> None:
    report = run_benchmarks(
        tmp_path / "bench", bars=8, jobs=3, workers=[2], repetitions=1
    )
    runs, warmups, summary = report["runs"], report["warmups"], report["summary"]
    assert isinstance(runs, list) and len(runs) == 2
    assert isinstance(warmups, list) and len(warmups) == 2
    assert report["semantic_outputs_equal"] is True
    assert (tmp_path / "bench/report.json").exists()
    assert isinstance(summary, list) and len(summary) == 2

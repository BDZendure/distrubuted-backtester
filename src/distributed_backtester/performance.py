"""Compare complete retained Ray-harness reports across authentic revisions."""

import argparse
import json
import math
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path
from statistics import median
from typing import Any, cast

from distributed_backtester.benchmark import semantic_digest
from distributed_backtester.data import load_csv
from distributed_backtester.experiment_artifacts import manifest_record
from distributed_backtester.experiment_snapshot import read_experiment_snapshot
from distributed_backtester.jobs import canonical_json, create_job
from distributed_backtester.money import require_integer
from distributed_backtester.provenance import ImplementationIdentity
from distributed_backtester.results import write_text
from distributed_backtester.workloads import job_configs

_ENVIRONMENT_KEYS = (
    "python_version",
    "python_implementation",
    "platform",
    "machine",
    "logical_cores",
    "available_cores",
    "thread_environment",
)


def _safe(path: Path) -> Path:
    if not isinstance(path, Path) or ".." in path.parts:
        raise ValueError("comparison paths must be Paths without traversal")
    absolute = path.absolute()
    for component in (absolute, *absolute.parents):
        if component.is_symlink():
            raise ValueError("comparison paths must not contain symlinks")
    return absolute


def _record(path: Path) -> dict[str, Any]:
    def invalid(value: str) -> None:
        raise ValueError(f"nonfinite JSON value: {value}")

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    value = json.loads(
        path.read_text(encoding="utf-8"),
        parse_constant=invalid,
        object_pairs_hook=unique,
    )
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return cast(dict[str, Any], value)


def _digest(value: object) -> str:
    return sha256(canonical_json(value).encode()).hexdigest()


def _positive_time(value: object) -> float:
    if (
        type(value) not in (int, float)
        or not math.isfinite(cast(float, value))
        or cast(float, value) <= 0
    ):
        raise ValueError("elapsed time must be finite and positive")
    return float(cast(float, value))


def _integer(value: object, name: str, *, minimum: int = 0) -> int:
    require_integer(cast(int, value), name, minimum=minimum)
    return cast(int, value)


def _environment(report: Mapping[str, Any]) -> dict[str, Any]:
    env = report.get("environment")
    if not isinstance(env, dict) or any(key not in env for key in _ENVIRONMENT_KEYS):
        raise ValueError("benchmark environment is incomplete")
    for key in ("python_version", "python_implementation", "platform", "machine"):
        if not isinstance(env[key], str) or not env[key]:
            raise ValueError("invalid interpreter/host metadata")
    for key in ("logical_cores", "available_cores"):
        if env[key] is not None:
            _integer(env[key], key, minimum=1)
    if not isinstance(env["thread_environment"], dict):
        raise ValueError("invalid thread environment")
    for key in ("hardware", "dependencies"):
        if not isinstance(report.get(key), dict) or not report[key]:
            raise ValueError("host/observer metadata is missing")
    if (
        not isinstance(report.get("python_executable"), str)
        or not report["python_executable"]
    ):
        raise ValueError("interpreter path is missing")
    distributions = env.get("distributions")
    if not isinstance(distributions, list) or not distributions:
        raise ValueError("benchmark distributions are missing")
    versions: dict[str, str] = {}
    for item in distributions:
        if not isinstance(item, dict) or set(item) != {"name", "version"}:
            raise ValueError("invalid distribution record")
        name, version = item["name"], item["version"]
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(version, str)
            or not version
        ):
            raise ValueError("invalid distribution name/version")
        key = re.sub(r"[-_.]+", "-", name).lower()
        if key in versions:
            raise ValueError("duplicate distribution")
        versions[key] = version
    project = versions.pop("distributed-backtester", None)
    if project is None:
        raise ValueError("benchmark project distribution is missing")
    identity = report.get("implementation")
    if not isinstance(identity, dict) or set(identity) != {
        "engine_id",
        "source_sha256",
    }:
        raise ValueError("invalid implementation record")
    if identity["engine_id"] != f"distributed-backtester/{project}:daily-long-only-v1":
        raise ValueError("engine does not match declared release and timing contract")
    ImplementationIdentity(**identity)
    return {
        **{key: env[key] for key in _ENVIRONMENT_KEYS},
        "dependencies": versions,
        "hardware": report.get("hardware"),
        "observer_dependencies": report.get("dependencies"),
        "python_executable": report.get("python_executable"),
    }


def _configuration(row: Mapping[str, Any]) -> tuple[str, str, int]:
    name, workers = row.get("configuration"), row.get("workers")
    if not isinstance(name, str):
        raise ValueError("configuration must be text")
    workers = _integer(workers, "workers", minimum=1)
    if name == "sequential" and workers == 1:
        return name, "sequential", workers
    match = re.fullmatch(r"(process|ray)-([1-9][0-9]*)", name)
    if match is None or int(match[2]) != workers:
        raise ValueError("configuration and workers disagree")
    return name, match[1], workers


def _command(
    row: Mapping[str, Any],
    root: Path,
    label: str,
    jobs: int,
    backend: str,
    workers: int,
    python_executable: str,
) -> None:
    command = row.get("command")
    if not isinstance(command, list) or any(
        not isinstance(item, str) for item in command
    ):
        raise ValueError("trial command is missing")
    expected = [
        "-m",
        "distributed_backtester.ray_benchmark",
        "_trial",
        "--data",
        str(root / "data.csv"),
        "--jobs",
        str(jobs),
        "--backend",
        backend,
        "--workers",
        str(workers),
        "--output-dir",
        str(root / label),
        "--ownership",
        str(root / f"{label}.ownership.json"),
    ]
    if (
        len(command) != len(expected) + 1
        or command[0] != python_executable
        or command[1:] != expected
    ):
        raise ValueError("trial command differs from declared workload/resources")


def _resources(
    execution: dict[str, Any], row: dict[str, Any], backend: str, workers: int
) -> dict[str, Any]:
    bound = execution.get("max_in_flight")
    if bound != (None if backend == "sequential" else 2 * workers):
        raise ValueError("trial queue bound differs from harness policy")
    observed = execution.get("max_observed_in_flight")
    if observed is not None:
        _integer(observed, "max_observed_in_flight", minimum=1)
        if bound is None or observed > bound:
            raise ValueError("trial exceeded queue bound")
    diagnostics = execution.get("backend_diagnostics")
    if row.get("backend_diagnostics") != diagnostics:
        raise ValueError("reported backend diagnostics differ from execution")
    resources: dict[str, Any] = {
        "backend": backend,
        "workers": workers,
        "max_in_flight": bound,
    }
    if backend == "ray":
        if (
            not isinstance(diagnostics, dict)
            or diagnostics.get("local_owned") is not True
        ):
            raise ValueError("Ray trial must own its local cluster")
        for key in (
            "ray_version",
            "cpus_per_worker",
            "resources_per_worker",
            "max_restarts",
            "max_task_retries",
            "object_store_capacity_bytes",
        ):
            if key not in diagnostics:
                raise ValueError("Ray resource policy is missing")
            resources[key] = diagnostics[key]
        if (
            diagnostics["cpus_per_worker"] != 1
            or diagnostics["resources_per_worker"] != {}
            or diagnostics["max_restarts"] != 0
            or diagnostics["max_task_retries"] != 0
        ):
            raise ValueError("Ray resources differ from harness policy")
        _integer(
            diagnostics["object_store_capacity_bytes"],
            "object_store_capacity_bytes",
            minimum=1,
        )
    return resources


def _trial(
    root: Path,
    row: dict[str, Any],
    expected: dict[str, Any],
    jobs: int,
    bars: int,
    normalize_engine: str,
    python_executable: str,
    environment: dict[str, Any],
) -> tuple[str, dict[str, str], dict[str, Any]]:
    name, backend, workers = _configuration(row)
    label = row.get("label")
    if not isinstance(label, str) or not re.fullmatch(
        rf"(?:warmup|run-[1-9][0-9]*)-{re.escape(name)}", label
    ):
        raise ValueError("unsafe or inconsistent trial label")
    if (
        type(row.get("returncode")) is not int
        or row["returncode"] != 0
        or row.get("detailed_results_retained") is not True
    ):
        raise ValueError("benchmark trials must succeed with retained details")
    _positive_time(row.get("elapsed_seconds"))
    peak = row.get("peak_private_uss_bytes")
    if peak is not None:
        require_integer(peak, "private memory bytes")
    if row.get("discarded_artifacts") != [] or row.get("discarded_artifact_bytes") != 0:
        raise ValueError("discarded artifacts cannot be compared")
    _command(row, root, label, jobs, backend, workers, python_executable)
    trial = _safe(root / label)
    digest = semantic_digest(trial)
    if digest != row.get("semantic_sha256"):
        raise ValueError("trial semantic artifact digest differs from report")
    records: dict[str, str] = {}
    with read_experiment_snapshot(trial) as snapshot:
        if (
            snapshot.manifest != expected
            or snapshot.submitted != jobs
            or snapshot.succeeded != jobs
            or snapshot.failed
        ):
            raise ValueError("trial differs from declared complete workload")
        execution = snapshot.observation.get("execution")
        if (
            not isinstance(execution, dict)
            or execution.get("backend") != backend
            or execution.get("workers") != workers
        ):
            raise ValueError("trial execution resources disagree")
        if execution.get("source_path") != str(root / "data.csv"):
            raise ValueError("trial source path differs")
        child_environment = _environment(
            {
                "environment": execution.get("environment"),
                "implementation": expected["implementation"],
                "hardware": environment["hardware"],
                "dependencies": environment["observer_dependencies"],
                "python_executable": execution.get("python_executable"),
            }
        )
        if child_environment != environment:
            raise ValueError("child environment/interpreter differs from parent")
        resources = _resources(execution, row, backend, workers)
        if row.get("experiment_id") != expected["experiment_id"]:
            raise ValueError("reported experiment differs from snapshot")
        validation = row.get("input_validation")
        required_validation = {
            "expected_csv_sha256": expected["dataset"]["source_sha256"],
            "actual_csv_sha256": expected["dataset"]["source_sha256"],
            "expected_experiment_id": expected["experiment_id"],
            "actual_experiment_id": expected["experiment_id"],
            "changed_manifest_fields": [],
        }
        if validation != required_validation:
            raise ValueError("trial lacks captured input validation")
        for _, content, _ in snapshot.iter_artifacts():
            artifact = json.loads(content)
            result = artifact.get("result")
            if (
                not isinstance(result, dict)
                or result.get("engine_id") != expected["implementation"]["engine_id"]
            ):
                raise ValueError("result producer engine differs")
            if len(result["equity"]) != bars:
                raise ValueError("result bar count differs")
            key = canonical_json(result["config"])
            if key in records:
                raise ValueError("duplicate configuration")
            normalized = {**result, "engine_id": normalize_engine}
            records[key] = _digest(normalized)
    if len(records) != jobs:
        raise ValueError("result configuration membership differs")
    return _digest(sorted(records.items())), records, resources


def _load(root: Path, normalize_engine: str) -> dict[str, Any]:
    report = _record(root / "report.json")
    if (
        type(report.get("schema_version")) is not int
        or report["schema_version"] != 1
        or report.get("semantic_outputs_equal") is not True
        or report.get("detailed_results_retained") is not True
        or not isinstance(report.get("finished_utc"), str)
    ):
        raise ValueError("benchmark report is incomplete or discarded")
    environment = _environment(report)
    workload, method = report.get("workload"), report.get("method")
    if not isinstance(workload, dict) or not isinstance(method, dict):
        raise ValueError("missing workload/method")
    for key in ("bars", "jobs"):
        _integer(workload.get(key), key, minimum=1)
    _integer(workload.get("seed"), "seed")
    if (
        workload.get("symbol") != "BENCH"
        or workload.get("config_generator") != "bounded-sma-windows-cash-batches-v1"
        or type(workload.get("fee_bps")) is not int
        or workload["fee_bps"] != 10
        or workload.get("expected_manifest") != "expected_manifest.json"
    ):
        raise ValueError("unsupported workload contract")
    repetitions = _integer(method.get("repetitions"), "repetitions", minimum=1)
    if (
        method.get("timer") != "perf_counter before Popen through child exit"
        or type(method.get("warmups_per_configuration")) is not int
        or method["warmups_per_configuration"] != 1
    ):
        raise ValueError("unsupported benchmark timing protocol")
    _positive_time(method.get("interval_seconds"))
    dataset = load_csv(_safe(root / "data.csv"), symbol="BENCH", price_basis="raw")
    if len(dataset.bars) != workload[
        "bars"
    ] or dataset.metadata.source_sha256 != workload.get("source_sha256"):
        raise ValueError("dataset differs from workload")
    identity = ImplementationIdentity(**report["implementation"])
    specs = tuple(
        create_job(config, dataset.metadata, identity)
        for config in job_configs(workload["jobs"])
    )
    expected = cast(
        dict[str, Any],
        manifest_record(
            specs, dataset.metadata, identity, [job.job_id for job in specs]
        ),
    )
    if (
        _record(root / "expected_manifest.json") != expected
        or workload.get("expected_experiment_id") != expected["experiment_id"]
    ):
        raise ValueError("captured manifest differs from generated workload")
    runs, warmups = report.get("runs"), report.get("warmups")
    if (
        not isinstance(runs, list)
        or not runs
        or not isinstance(warmups, list)
        or not warmups
    ):
        raise ValueError("missing warmup/timed samples")
    groups: dict[str, list[dict[str, Any]]] = {}
    names: set[str] = set()
    economic: dict[str, str] | None = None
    economics_sha: str | None = None
    source_semantic: str | None = None
    warmup_names: set[str] = set()
    policies: dict[str, dict[str, Any]] = {}
    for row in [*warmups, *runs]:
        if not isinstance(row, dict):
            raise ValueError("invalid sample")
        label = row.get("label")
        if not isinstance(label, str) or label in names:
            raise ValueError("duplicate/missing trial label")
        names.add(label)
        digest, records, policy = _trial(
            root,
            row,
            expected,
            workload["jobs"],
            workload["bars"],
            normalize_engine,
            report["python_executable"],
            environment,
        )
        name = row["configuration"]
        if name in policies and policies[name] != policy:
            raise ValueError("resource policy differs within revision")
        policies[name] = policy
        if economic is None:
            economic, economics_sha = records, digest
            source_semantic = row["semantic_sha256"]
        elif economic != records or source_semantic != row["semantic_sha256"]:
            raise ValueError("economic outputs differ within revision")
        if row in warmups:
            if (
                label != f"warmup-{row['configuration']}"
                or row["configuration"] in warmup_names
            ):
                raise ValueError("invalid warmup membership")
            warmup_names.add(row["configuration"])
        else:
            groups.setdefault(row["configuration"], []).append(row)
    if "sequential" not in groups or set(groups) != warmup_names:
        raise ValueError("configuration membership differs")
    for name, rows in groups.items():
        if {row["label"] for row in rows} != {
            f"run-{i}-{name}" for i in range(1, repetitions + 1)
        } or len(rows) != repetitions:
            raise ValueError("incomplete timed repetition membership")
    if source_semantic != report.get("semantic_sha256"):
        raise ValueError("overall semantic artifact digest differs")
    sequential = median(
        _positive_time(row["elapsed_seconds"]) for row in groups["sequential"]
    )
    summary = []
    for name in sorted(groups):
        rows = groups[name]
        times = [_positive_time(row["elapsed_seconds"]) for row in rows]
        mid = median(times)
        workers = rows[0]["workers"]
        peaks = [row.get("peak_private_uss_bytes") for row in rows]
        summary.append(
            {
                "configuration": name,
                "workers": workers,
                "repetitions": repetitions,
                "median_seconds": mid,
                "min_seconds": min(times),
                "max_seconds": max(times),
                "jobs_per_second": workload["jobs"] / mid,
                "speedup": sequential / mid,
                "parallel_efficiency": sequential / mid / workers,
                "median_peak_private_uss_bytes": median(cast(list[int], peaks))
                if all(value is not None for value in peaks)
                else None,
            }
        )
    inputs = {
        key: value
        for key, value in workload.items()
        if key not in {"expected_experiment_id", "expected_manifest"}
    }
    return {
        "path": str(root),
        "implementation": asdict(identity),
        "environment": environment,
        "inputs": inputs,
        "method": method,
        "configurations": {name: groups[name][0]["workers"] for name in groups},
        "resource_policies": policies,
        "economic_sha256": economics_sha,
        "configuration_result_hashes": economic,
        "verified_trials": len(names),
        "summary": summary,
    }


def compare_benchmarks(baseline: Path, candidate: Path, output: Path) -> dict[str, Any]:
    """Verify all retained economics before publishing any before/after ratios."""
    first, second, destination = _safe(baseline), _safe(candidate), _safe(output)
    if destination.exists():
        raise FileExistsError(destination)
    if (
        destination.is_relative_to(first)
        or destination.is_relative_to(second)
        or destination.is_relative_to(Path(__file__).resolve().parent)
        or any(part in {".git", ".hg", ".svn"} for part in destination.parts)
    ):
        raise ValueError("comparison output must be separate from sources/package")
    header = _record(first / "report.json")
    _environment(header)
    normalize_engine = header["implementation"]["engine_id"]
    left, right = _load(first, normalize_engine), _load(second, normalize_engine)
    for key in (
        "environment",
        "inputs",
        "method",
        "configurations",
        "resource_policies",
    ):
        if left[key] != right[key]:
            raise ValueError(f"benchmark {key} differs")
    if left["configuration_result_hashes"] != right["configuration_result_hashes"]:
        raise ValueError("complete economic outputs differ across revisions")
    summary = []
    lookup = {row["configuration"]: row for row in right["summary"]}
    for before in left["summary"]:
        after = lookup[before["configuration"]]
        entry = {
            "configuration": before["configuration"],
            "workers": before["workers"],
            "optimization_speedup": before["median_seconds"] / after["median_seconds"],
            "runtime_reduction_percent": 100
            * (1 - after["median_seconds"] / before["median_seconds"]),
        }
        for key in (
            "median_seconds",
            "min_seconds",
            "max_seconds",
            "jobs_per_second",
            "speedup",
            "parallel_efficiency",
            "median_peak_private_uss_bytes",
        ):
            entry[f"baseline_{key}"] = before[key]
            entry[f"candidate_{key}"] = after[key]
        summary.append(entry)
    result = {
        "schema_version": 1,
        "economic_outputs_equal": True,
        "economic_projection": (
            "complete results in config order; only validated engine release normalized"
        ),
        "economic_sha256": left["economic_sha256"],
        "workload": left["inputs"],
        "method": left["method"],
        "baseline": left,
        "candidate": right,
        "summary": summary,
    }
    write_text(canonical_json(result) + "\n", destination)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m distributed_backtester.performance"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    compare = commands.add_parser("compare")
    compare.add_argument("--baseline", type=Path, required=True)
    compare.add_argument("--candidate", type=Path, required=True)
    compare.add_argument("--output", type=Path, required=True)
    options = parser.parse_args(argv)
    try:
        result = compare_benchmarks(options.baseline, options.candidate, options.output)
        print(json.dumps(result["summary"], indent=2))
    except (ValueError, RuntimeError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

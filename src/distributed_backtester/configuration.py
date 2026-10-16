"""Strict, command-specific TOML defaults for the run commands."""

import math
import tomllib
from pathlib import Path

_COMMON_FIELDS = {"data", "data_format", "dataset_version", "log_level"}
_SIMULATION_FIELDS = {
    "symbol",
    "price_basis",
    "fast",
    "slow",
    "initial_cash",
    "fee_bps",
}
_RAY_FIELDS = {
    "workers",
    "max_in_flight",
    "ray_address",
    "cpus_per_worker",
    "worker_resource",
    "object_store_memory",
}
_COMMAND_FIELDS = {
    "backtest": _COMMON_FIELDS | _SIMULATION_FIELDS | {"output", "overwrite"},
    "sweep": _COMMON_FIELDS
    | _SIMULATION_FIELDS
    | _RAY_FIELDS
    | {
        "backend",
        "durable",
        "max_attempts",
        "job_timeout",
        "retry_delay",
        "output_dir",
    },
    "resume": _COMMON_FIELDS | _RAY_FIELDS | {"output_dir"},
}
_INTEGER_FIELDS = {
    "fast",
    "slow",
    "fee_bps",
    "workers",
    "max_in_flight",
    "object_store_memory",
    "max_attempts",
}
_FLOAT_FIELDS = {"cpus_per_worker", "job_timeout", "retry_delay"}
_BOOLEAN_FIELDS = {"overwrite", "durable"}
_PATH_FIELDS = {"data", "output", "output_dir"}
_OUTPUT_FIELDS = {"output", "output_dir"}
_CHOICES = {
    "data_format": ("auto", "csv", "parquet"),
    "price_basis": ("raw",),
    "backend": ("sequential", "process", "ray"),
    "log_level": ("OFF", "ERROR", "WARNING", "INFO", "DEBUG"),
}


def _validate_field(name: str, value: object, command: str) -> object:
    if name in {"fast", "slow"} and command == "sweep":
        if (
            type(value) is not list
            or not value
            or any(type(item) is not int or item <= 0 for item in value)
        ):
            raise ValueError(f"{name} must be a nonempty array of positive integers")
        return value
    if name in _INTEGER_FIELDS:
        if type(value) is not int:
            raise ValueError(f"{name} must be an integer")
        return value
    if name in _FLOAT_FIELDS:
        if type(value) is not int and type(value) is not float:
            raise ValueError(f"{name} must be a finite number")
        try:
            number = float(value)
        except OverflowError as error:
            raise ValueError(f"{name} must be a finite number") from error
        if not math.isfinite(number):
            raise ValueError(f"{name} must be a finite number")
        return number
    if name in _BOOLEAN_FIELDS:
        if type(value) is not bool:
            raise ValueError(f"{name} must be a boolean")
        return value
    if name == "worker_resource":
        message = "worker_resource must be a nonempty array of NAME=QUANTITY strings"
        if type(value) is not list or not value:
            raise ValueError(message)
        for item in value:
            if type(item) is not str:
                raise ValueError(message)
            resource, separator, quantity = item.partition("=")
            if not separator or not resource.strip() or not quantity.strip():
                raise ValueError(message)
        return value
    if type(value) is not str:
        raise ValueError(f"{name} must be a string")
    if name in _CHOICES and value not in _CHOICES[name]:
        raise ValueError(f"{name} must be one of {', '.join(_CHOICES[name])}")
    return Path(value) if name in _PATH_FIELDS else value


def load_configuration(path: Path, command: str) -> dict[str, object]:
    """Load supplied defaults, preserving CLI ownership of domain validation.

    Only schema_version and command are required. Money and resource quantities
    remain text for the CLI's existing parsers and constructors. File paths are
    resolved against the configuration directory without creating outputs.
    """
    if command not in _COMMAND_FIELDS:
        raise ValueError("configuration command must be backtest, sweep, or resume")
    try:
        with path.open("rb") as stream:
            document: dict[str, object] = tomllib.load(stream)
    except (tomllib.TOMLDecodeError, UnicodeError) as error:
        raise ValueError(f"cannot parse configuration {path}: {error}") from error
    if (
        type(document.get("schema_version")) is not int
        or document["schema_version"] != 1
    ):
        raise ValueError("configuration schema_version must be the integer 1")
    if type(document.get("command")) is not str or document["command"] != command:
        raise ValueError(f"configuration command must match {command!r}")
    unknown = document.keys() - _COMMAND_FIELDS[command] - {"schema_version", "command"}
    if unknown:
        raise ValueError(
            f"unknown configuration keys for {command}: {', '.join(sorted(unknown))}"
        )
    directory = path.resolve().parent
    defaults: dict[str, object] = {}
    for name, value in document.items():
        if name in {"schema_version", "command"}:
            continue
        validated = _validate_field(name, value, command)
        if isinstance(validated, Path):
            validated = (directory / validated).resolve()
            if name in _OUTPUT_FIELDS and (
                validated == path.resolve()
                or (validated.exists() and validated.samefile(path))
            ):
                raise ValueError("output must not refer to the configuration file")
        defaults[name] = validated
    return defaults

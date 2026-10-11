"""Read run configuration before constructing the normal argparse interface."""

import argparse
import os
from collections.abc import Sequence
from pathlib import Path
from typing import cast

from distributed_backtester.configuration import load_configuration
from distributed_backtester.operational_logging import LEVELS


class ExplicitAppend(argparse.Action):
    """Replace configured defaults on first use; collect repeated CLI values."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str | Sequence[object] | None,
        option_string: str | None = None,
    ) -> None:
        current = getattr(namespace, self.dest, None)
        explicit = [] if current is self.default or current is None else list(current)
        explicit.append(cast(str, values))
        setattr(namespace, self.dest, explicit)


def protect_configuration(options: argparse.Namespace) -> None:
    """Reject aliases of any destination the selected command can mutate."""
    config = getattr(options, "config", None)
    if config is None:
        return
    destinations: list[Path] = []
    output = getattr(options, "output", None)
    if output is not None:
        destinations.append(output)
    directory = getattr(options, "output_dir", None)
    if directory is not None:
        destinations.extend(
            directory / name
            for name in (
                "manifest.json",
                "execution.json",
                "index.json",
                "journal.sqlite3",
                "journal.sqlite3-wal",
                "journal.sqlite3-shm",
                "coordinator.lock",
            )
        )
        destinations.extend((directory / "results").glob("*.json"))
    for destination in destinations:
        if config.resolve() == destination.resolve() or (
            destination.exists() and os.path.samefile(config, destination)
        ):
            raise ValueError("output must not refer to the configuration file")


def requested_log_level(argv: Sequence[str]) -> str:
    """Read an explicit level even when a configuration file cannot be loaded."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--log-level", choices=LEVELS, default="OFF")
    options, _ = parser.parse_known_args(argv)
    return str(options.log_level)


def configuration_defaults(
    argv: Sequence[str],
) -> tuple[str | None, dict[str, object]]:
    preparser = argparse.ArgumentParser(add_help=False)
    preparser.add_argument("command", nargs="?")
    preparser.add_argument("--config", type=Path)
    options, _ = preparser.parse_known_args(argv)
    if options.config is None:
        return None, {}
    defaults = load_configuration(options.config, options.command)
    return options.command, defaults

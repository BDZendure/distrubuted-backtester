"""Installed source identity and separate operational environment metadata."""

import os
import platform
import re
import subprocess
from dataclasses import dataclass
from hashlib import sha256
from importlib import metadata
from pathlib import Path

from distributed_backtester import simulation

_CACHE_DIRECTORIES = frozenset(
    {"__pycache__", ".mypy_cache", ".pytest_cache", ".ruff_cache"}
)
_THREAD_ENVIRONMENT_VARIABLES = (
    "BLIS_NUM_THREADS",
    "MKL_DYNAMIC",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "OMP_DYNAMIC",
    "OMP_NUM_THREADS",
    "OMP_PLACES",
    "OMP_PROC_BIND",
    "OMP_THREAD_LIMIT",
    "OPENBLAS_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
)


@dataclass(frozen=True, slots=True)
class ImplementationIdentity:
    """Release/engine semantics together with the actual installed source bytes."""

    engine_id: str
    source_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.engine_id, str) or not self.engine_id.strip():
            raise ValueError("engine_id must be a nonempty string")
        if not isinstance(self.source_sha256, str) or not re.fullmatch(
            r"[0-9a-f]{64}", self.source_sha256
        ):
            raise ValueError("source_sha256 must be a lowercase SHA-256 digest")


def _raise_walk_error(error: OSError) -> None:
    raise error


def source_fingerprint(package_root: Path | None = None) -> str:
    """Hash sorted package-relative Python source names and exact file bytes.

    Each UTF-8 POSIX filename and file body has an unsigned 8-byte big-endian
    length prefix. Absolute paths, non-Python files, and cache directories are
    excluded. Directory symlinks are rejected rather than silently skipped or
    followed into cycles. Missing source and traversal/read errors fail closed.
    The default is this installed module's directory, independent of the cwd.
    """
    if package_root is not None and not isinstance(package_root, Path):
        raise ValueError("package_root must be a Path or None")
    root = Path(__file__).parent if package_root is None else package_root
    if not root.is_dir():
        raise ValueError("package_root must be an existing directory")

    sources: list[tuple[str, Path]] = []
    for directory, subdirectories, filenames in os.walk(
        root, onerror=_raise_walk_error
    ):
        subdirectories[:] = [
            name for name in subdirectories if name not in _CACHE_DIRECTORIES
        ]
        for name in subdirectories:
            if (Path(directory) / name).is_symlink():
                raise ValueError("package source directories must not be symlinks")
        for name in filenames:
            if name.endswith(".py"):
                path = Path(directory) / name
                sources.append((path.relative_to(root).as_posix(), path))
    if not sources:
        raise ValueError("package_root contains no Python source files")

    digest = sha256()
    for relative, path in sorted(sources):
        filename = relative.encode("utf-8")
        content = path.read_bytes()
        digest.update(len(filename).to_bytes(8, "big"))
        digest.update(filename)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def current_implementation() -> ImplementationIdentity:
    """Read installed identity once per experiment or worker initialization."""
    return ImplementationIdentity(simulation.ENGINE_ID, source_fingerprint())


def _available_cores(logical_cores: int | None) -> int | None:
    process_cpu_count = getattr(os, "process_cpu_count", None)
    if process_cpu_count is not None:
        count = process_cpu_count()
        if isinstance(count, int) and count > 0:
            return count
    sched_getaffinity = getattr(os, "sched_getaffinity", None)
    if sched_getaffinity is not None:
        try:
            count = len(sched_getaffinity(0))
        except OSError:
            pass
        else:
            if count > 0:
                return count
    return logical_cores


def _source_repository() -> Path | None:
    """Recognize this package's src checkout, never an enclosing wheel's repo."""
    package_root = Path(__file__).resolve().parent
    if package_root.parent.name != "src":
        return None
    repository = package_root.parent.parent
    return repository if (repository / ".git").exists() else None


def _git_snapshot(
    repository: Path | None,
) -> tuple[str | None, str | None, bool | None]:
    if repository is None:
        return None, None, None
    command = [
        "git",
        "--no-optional-locks",
        "-c",
        "core.fsmonitor=false",
        "-C",
        str(repository),
    ]
    try:
        revision = subprocess.run(
            [*command, "rev-parse", "--verify", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
        status = subprocess.run(
            [*command, "status", "--porcelain=v1", "--untracked-files=all"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None, None, None
    if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", revision):
        return None, None, None
    return revision, status, bool(status)


def capture_environment(repository: Path | None = None) -> dict[str, object]:
    """Capture JSON-ready operational context, separate from semantic identity.

    ``distributions`` contains every unique installed (name, version) pair,
    sorted by name and version. ``git_status`` preserves porcelain v1 text;
    all three Git fields are null when revision/status cannot be obtained.
    Automatic Git capture recognizes this package's source checkout only.
    Wheel installations need an explicit repository to record a revision.
    ``thread_environment`` contains only known BLAS/OMP/thread controls.
    """
    if repository is not None and not isinstance(repository, Path):
        raise ValueError("repository must be a Path or None")
    installed: set[tuple[str, str]] = set()
    for distribution in metadata.distributions():
        name, version = distribution.metadata["Name"], distribution.version
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(version, str)
            or not version
        ):
            raise ValueError("installed distributions must provide name and version")
        installed.add((name, version))

    logical_cores = os.cpu_count()
    revision, status, dirty = _git_snapshot(
        _source_repository() if repository is None else repository
    )
    return {
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "logical_cores": logical_cores,
        "available_cores": _available_cores(logical_cores),
        "distributions": [
            {"name": name, "version": version} for name, version in sorted(installed)
        ],
        "git_revision": revision,
        "git_status": status,
        "git_dirty": dirty,
        "thread_environment": {
            name: os.environ[name]
            for name in _THREAD_ENVIRONMENT_VARIABLES
            if name in os.environ
        },
    }

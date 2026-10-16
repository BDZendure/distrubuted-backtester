"""Installed implementation identity and operational provenance contracts."""

import json
import os
import pickle
import platform
import re
import shutil
import subprocess
from dataclasses import FrozenInstanceError, replace
from importlib import metadata
from pathlib import Path
from typing import cast

import pytest

from distributed_backtester import provenance, simulation
from distributed_backtester.provenance import (
    ImplementationIdentity,
    capture_environment,
    source_fingerprint,
)


def test_implementation_identity_is_frozen_slotted_and_picklable() -> None:
    identity = ImplementationIdentity("daily-test-v1", "a" * 64)
    assert identity.engine_id == "daily-test-v1"
    assert identity.source_sha256 == "a" * 64
    assert not hasattr(identity, "__dict__")
    assert pickle.loads(pickle.dumps(identity)) == identity
    attribute = "engine_id"
    with pytest.raises(FrozenInstanceError):
        setattr(identity, attribute, "daily-test-v2")


def test_engine_version_and_source_digest_both_affect_identity() -> None:
    identity = ImplementationIdentity("daily-test-v1", "a" * 64)
    assert replace(identity, engine_id="daily-test-v2") != identity
    assert replace(identity, source_sha256="b" * 64) != identity


@pytest.mark.parametrize("bad", [None, 1, True, b"engine", "", " \t\n"])
def test_identity_rejects_invalid_engine_id(bad: object) -> None:
    with pytest.raises(ValueError, match="engine_id"):
        ImplementationIdentity(cast(str, bad), "a" * 64)


@pytest.mark.parametrize(
    "bad",
    [
        None,
        1,
        True,
        b"a" * 64,
        "",
        "a" * 63,
        "a" * 65,
        "A" * 64,
        "g" * 64,
        "a" * 64 + "\n",
        " " + "a" * 64,
    ],
)
def test_identity_rejects_invalid_source_digest(bad: object) -> None:
    with pytest.raises(ValueError, match="source_sha256"):
        ImplementationIdentity("daily-test-v1", cast(str, bad))


def write_sources(root: Path, sources: dict[str, bytes]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    for relative, content in sources.items():
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)


def test_source_fingerprint_is_stable_across_roots_and_creation_order(
    tmp_path: Path,
) -> None:
    sources = {"z.py": b"z = 1\r\n", "nested/a.py": b"a = 2\n", "cafe.py": b"\xff"}
    first, second = tmp_path / "first", tmp_path / "elsewhere" / "second"
    write_sources(first, sources)
    write_sources(second, dict(reversed(list(sources.items()))))
    fingerprint = source_fingerprint(first)
    assert re.fullmatch(r"[0-9a-f]{64}", fingerprint)
    assert source_fingerprint(first) == fingerprint
    assert source_fingerprint(second) == fingerprint


@pytest.mark.parametrize("mutation", ["content", "filename", "add", "remove"])
def test_source_fingerprint_changes_with_python_source_mutations(
    tmp_path: Path, mutation: str
) -> None:
    write_sources(tmp_path, {"__init__.py": b"", "nested/module.py": b"x = 1\n"})
    before = source_fingerprint(tmp_path)
    source = tmp_path / "nested" / "module.py"
    if mutation == "content":
        source.write_bytes(b"x = 2\n")
    elif mutation == "filename":
        source.rename(source.with_name("renamed.py"))
    elif mutation == "add":
        source.with_name("new.py").write_bytes(b"")
    else:
        source.unlink()
    assert source_fingerprint(tmp_path) != before


def test_source_fingerprint_ignores_non_sources_and_caches(tmp_path: Path) -> None:
    write_sources(tmp_path, {"module.py": b"x = 1\n"})
    before = source_fingerprint(tmp_path)
    write_sources(
        tmp_path,
        {
            "README.md": b"changed docs",
            "module.pyi": b"x: int",
            "data.csv": b"data",
            "__pycache__/module.pyc": b"compiled",
            "__pycache__/stray.py": b"cache",
            ".mypy_cache/stray.py": b"cache",
            ".pytest_cache/stray.py": b"cache",
            ".ruff_cache/stray.py": b"cache",
        },
    )
    assert source_fingerprint(tmp_path) == before


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ({"a.py": b"b.pyc"}, {"a.pyb.py": b"c"}),
        ({"a.py": b"xb.pyy", "b.py": b""}, {"a.py": b"x", "b.py": b"yb.py"}),
    ],
)
def test_source_fingerprint_frames_names_and_content_lengths(
    tmp_path: Path, left: dict[str, bytes], right: dict[str, bytes]
) -> None:
    # Both trees have identical naive concatenations of filenames and bytes.
    def naive(sources: dict[str, bytes]) -> bytes:
        return b"".join(name.encode() + sources[name] for name in sorted(sources))

    assert naive(left) == naive(right)
    first, second = tmp_path / "first", tmp_path / "second"
    write_sources(first, left)
    write_sources(second, right)
    assert source_fingerprint(first) != source_fingerprint(second)


@pytest.mark.parametrize("kind", ["missing", "file", "empty", "no-python"])
def test_source_fingerprint_rejects_missing_or_source_free_roots(
    tmp_path: Path, kind: str
) -> None:
    root = tmp_path / "package"
    if kind == "file":
        root.write_text("not a directory")
    elif kind != "missing":
        root.mkdir()
        if kind == "no-python":
            (root / "README.md").write_text("no Python sources")
    with pytest.raises(ValueError):
        source_fingerprint(root)


@pytest.mark.parametrize("bad", ["not-a-path", 1, True])
def test_source_fingerprint_rejects_invalid_root_types(bad: object) -> None:
    with pytest.raises(ValueError, match="package_root"):
        source_fingerprint(cast(Path, bad))


def test_source_fingerprint_propagates_unreadable_python_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_sources(tmp_path, {"good.py": b"x = 1", "nested/bad.py": b"x = 2"})
    original_read = Path.read_bytes

    def denied_read(path: Path) -> bytes:
        if path.name == "bad.py":
            raise PermissionError("injected source read denial")
        return original_read(path)

    monkeypatch.setattr(Path, "read_bytes", denied_read)
    with pytest.raises(PermissionError, match="source read denial"):
        source_fingerprint(tmp_path)


def test_source_fingerprint_propagates_directory_traversal_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_sources(tmp_path, {"good.py": b"x = 1", "nested/hidden.py": b"x = 2"})
    original_scandir = os.scandir

    def denied_scandir(path: object) -> object:
        if Path(cast(str, path)).name == "nested":
            raise PermissionError("injected directory traversal denial")
        return original_scandir(cast(str, path))

    monkeypatch.setattr(os, "scandir", denied_scandir)
    with pytest.raises(PermissionError, match="directory traversal denial"):
        source_fingerprint(tmp_path)


def test_source_fingerprint_rejects_untraversed_directory_symlinks(
    tmp_path: Path,
) -> None:
    root, target = tmp_path / "package", tmp_path / "target"
    write_sources(root, {"good.py": b"x = 1"})
    write_sources(target, {"linked.py": b"x = 2"})
    (root / "linked").symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        source_fingerprint(root)


def test_source_fingerprint_uses_module_location_independent_of_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = tmp_path / "site-packages" / "distributed_backtester"
    write_sources(package, {"provenance.py": b"# installed\n", "other.py": b"x = 1"})
    unrelated = tmp_path / "checkout"
    write_sources(unrelated, {"other.py": b"# unrelated\n", "docs/example.py": b"docs"})
    monkeypatch.setattr(provenance, "__file__", str(package / "provenance.py"))
    monkeypatch.chdir(unrelated)
    assert provenance.source_fingerprint() == provenance.source_fingerprint(package)
    (unrelated / "docs" / "example.py").write_text("changed docs")
    assert provenance.source_fingerprint() == provenance.source_fingerprint(package)


def test_current_implementation_uses_installed_sources_and_simulation_engine() -> None:
    identity = provenance.current_implementation()
    assert identity.engine_id == simulation.ENGINE_ID
    assert identity.source_sha256 == provenance.source_fingerprint()


def test_current_implementation_preserves_engine_version_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = provenance.current_implementation()
    monkeypatch.setattr(simulation, "ENGINE_ID", "changed-release:daily-long-only-v1")
    changed = provenance.current_implementation()
    assert changed.engine_id == "changed-release:daily-long-only-v1"
    assert changed.source_sha256 == original.source_sha256
    assert changed != original


def test_capture_environment_has_json_serializable_operational_contract(
    tmp_path: Path,
) -> None:
    snapshot = capture_environment(tmp_path)
    assert json.loads(json.dumps(snapshot, allow_nan=False)) == snapshot
    assert snapshot["python_version"] == platform.python_version()
    assert snapshot["python_implementation"] == platform.python_implementation()
    assert snapshot["platform"] == platform.platform()
    assert snapshot["machine"] == platform.machine()
    for key in ("available_cores", "logical_cores"):
        count = snapshot[key]
        assert count is None or (type(count) is int and count > 0)
    distributions = snapshot["distributions"]
    assert isinstance(distributions, list)
    assert distributions
    pairs: list[tuple[str, str]] = []
    for distribution in distributions:
        assert isinstance(distribution, dict)
        assert set(distribution) == {"name", "version"}
        assert isinstance(distribution["name"], str) and distribution["name"]
        assert isinstance(distribution["version"], str) and distribution["version"]
        pairs.append((distribution["name"], distribution["version"]))
    assert pairs == sorted(set(pairs))
    assert snapshot["git_revision"] is None
    assert snapshot["git_status"] is None
    assert snapshot["git_dirty"] is None
    assert isinstance(snapshot["thread_environment"], dict)


def test_capture_environment_snapshots_all_distributions_sorted_and_unique(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def distribution(name: str, version: str, suffix: str) -> metadata.PathDistribution:
        directory = tmp_path / f"{suffix}.dist-info"
        directory.mkdir()
        (directory / "METADATA").write_text(
            f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n"
        )
        return metadata.PathDistribution(directory)

    installed = [
        distribution("z-other", "9", "z"),
        distribution("a-other", "2", "a2"),
        distribution("a-other", "1", "a1"),
        distribution("a-other", "1", "duplicate"),
    ]
    # Inject only metadata enumeration; the distribution metadata is read from disk.
    monkeypatch.setattr(metadata, "distributions", lambda: iter(installed))
    assert capture_environment(tmp_path)["distributions"] == [
        {"name": "a-other", "version": "1"},
        {"name": "a-other", "version": "2"},
        {"name": "z-other", "version": "9"},
    ]


def test_capture_environment_limits_variables_to_known_thread_controls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OMP_NUM_THREADS", "2")
    monkeypatch.setenv("OPENBLAS_NUM_THREADS", "3")
    monkeypatch.setenv("MKL_NUM_THREADS", "4")
    monkeypatch.setenv("BLIS_NUM_THREADS", "5")
    monkeypatch.setenv("VECLIB_MAXIMUM_THREADS", "6")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "must-not-appear")
    monkeypatch.setenv("OMP_PRIVATE_SECRET", "must-not-appear")
    monkeypatch.setenv("ARBITRARY_ENVIRONMENT_VALUE", "must-not-appear")
    snapshot = capture_environment(tmp_path)
    controls = snapshot["thread_environment"]
    assert isinstance(controls, dict)
    assert controls["OMP_NUM_THREADS"] == "2"
    assert controls["OPENBLAS_NUM_THREADS"] == "3"
    assert controls["MKL_NUM_THREADS"] == "4"
    assert controls["BLIS_NUM_THREADS"] == "5"
    assert controls["VECLIB_MAXIMUM_THREADS"] == "6"
    assert "must-not-appear" not in json.dumps(snapshot)


def test_capture_environment_distinguishes_available_and_logical_cores(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(os, "cpu_count", lambda: 12)
    monkeypatch.setattr(os, "process_cpu_count", lambda: 3, raising=False)
    snapshot = capture_environment(tmp_path)
    assert snapshot["logical_cores"] == 12
    assert snapshot["available_cores"] == 3


def test_capture_environment_uses_cpu_affinity_on_older_python(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(os, "cpu_count", lambda: 12)
    monkeypatch.delattr(os, "process_cpu_count", raising=False)
    monkeypatch.setattr(os, "sched_getaffinity", lambda pid: {2, 4}, raising=False)
    snapshot = capture_environment(tmp_path)
    assert snapshot["logical_cores"] == 12
    assert snapshot["available_cores"] == 2


@pytest.mark.parametrize("logical", [None, 8])
def test_capture_environment_cpu_fallback_preserves_unknown_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, logical: int | None
) -> None:
    monkeypatch.setattr(os, "cpu_count", lambda: logical)
    monkeypatch.delattr(os, "process_cpu_count", raising=False)

    def unavailable_affinity(pid: int) -> set[int]:
        raise OSError("injected unsupported affinity query")

    monkeypatch.setattr(os, "sched_getaffinity", unavailable_affinity, raising=False)
    snapshot = capture_environment(tmp_path)
    assert snapshot["logical_cores"] == logical
    assert snapshot["available_cores"] == logical


def test_capture_environment_records_real_source_checkout_git_contract() -> None:
    if shutil.which("git") is None:
        pytest.skip("Git is unavailable")
    root = Path(provenance.__file__).resolve().parents[2]
    if not (root / ".git").exists():
        pytest.skip("installed package is outside a source checkout")
    snapshot = provenance.capture_environment(root)
    revision, status, dirty = (
        snapshot["git_revision"],
        snapshot["git_status"],
        snapshot["git_dirty"],
    )
    assert isinstance(revision, str) and re.fullmatch(r"[0-9a-f]{40,64}", revision)
    assert isinstance(status, str)
    assert type(dirty) is bool
    assert dirty == bool(status)
    default = provenance.capture_environment()
    assert isinstance(default["git_revision"], str)


@pytest.mark.parametrize("status", ["", " M tracked.py\n?? untracked.py\n"])
def test_capture_environment_preserves_porcelain_status_and_dirty_indicator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: str
) -> None:
    def git_output(
        args: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        output = status if "status" in args else "a" * 40 + "\n"
        return subprocess.CompletedProcess(args, 0, stdout=output, stderr="")

    # Inject Git output only, avoiding repository writes while exercising snapshots.
    monkeypatch.setattr(subprocess, "run", git_output)
    snapshot = capture_environment(tmp_path)
    assert snapshot["git_revision"] == "a" * 40
    assert snapshot["git_status"] == status
    assert snapshot["git_dirty"] == bool(status)


@pytest.mark.parametrize("failure", ["missing", "not-repo", "timeout", "status"])
def test_capture_environment_uses_explicit_null_git_fields_when_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    def failed_git(
        args: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        if failure == "missing":
            raise FileNotFoundError("injected missing Git")
        if failure == "timeout":
            raise subprocess.TimeoutExpired(args, 5)
        if failure == "status" and "status" not in args:
            return subprocess.CompletedProcess(args, 0, stdout="a" * 40 + "\n")
        raise subprocess.CalledProcessError(128, args)

    monkeypatch.setattr(subprocess, "run", failed_git)
    snapshot = capture_environment(tmp_path)
    assert snapshot["git_revision"] is None
    assert snapshot["git_status"] is None
    assert snapshot["git_dirty"] is None


def test_capture_environment_does_not_guess_wheel_revision_from_cwd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_checkout = Path(provenance.__file__).resolve().parents[2]
    # A wheel may be installed inside a checkout's venv or run from a checkout.
    wheel_module = (
        real_checkout
        / ".venv"
        / "lib"
        / "python3.12"
        / "site-packages"
        / "distributed_backtester"
        / "provenance.py"
    )
    monkeypatch.setattr(provenance, "__file__", str(wheel_module))
    monkeypatch.chdir(real_checkout)
    snapshot = provenance.capture_environment()
    assert snapshot["git_revision"] is None
    assert snapshot["git_status"] is None
    assert snapshot["git_dirty"] is None


@pytest.mark.parametrize("bad", ["not-a-path", 1, True])
def test_capture_environment_rejects_invalid_repository_types(bad: object) -> None:
    with pytest.raises(ValueError, match="repository"):
        capture_environment(cast(Path, bad))

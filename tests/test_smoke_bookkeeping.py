"""Status smoke permits SQLite bookkeeping and detects authoritative writes."""

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture
def smoke() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts/smoke.py"
    spec = importlib.util.spec_from_file_location("phase7_bookkeeping_smoke", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("name", ["journal.sqlite3-shm", "journal.sqlite3-wal"])
@pytest.mark.parametrize("remove", [False, True])
def test_terminal_journal_bookkeeping_creation_and_removal_is_allowed(
    smoke: ModuleType, tmp_path: Path, name: str, remove: bool
) -> None:
    (tmp_path / "journal.sqlite3").write_bytes(b"authoritative database")
    (tmp_path / "coordinator.lock").write_bytes(b"")
    sidecar = tmp_path / name
    content = b"shared memory bookkeeping" if name.endswith("-shm") else b""
    if remove:
        sidecar.write_bytes(content)
    before = smoke._tree(tmp_path)
    if remove:
        sidecar.unlink()
    else:
        sidecar.write_bytes(content)
    assert smoke._tree(tmp_path) == before


@pytest.mark.parametrize(
    "name,content,journal",
    [
        ("journal.sqlite3-wal", b"authoritative frames", True),
        ("journal.sqlite3", b"changed database", True),
        ("coordinator.lock", b"changed lock", True),
        ("index.json", b"changed export", True),
        ("journal.sqlite3-shm", b"unexpected file", False),
        ("journal.sqlite3-wal", b"", False),
        ("results/journal.sqlite3-shm", b"unexpected nested file", True),
        ("other-wal", b"unexpected file", True),
    ],
)
def test_authoritative_or_unrelated_changes_are_detected(
    smoke: ModuleType, tmp_path: Path, name: str, content: bytes, journal: bool
) -> None:
    if journal:
        (tmp_path / "journal.sqlite3").write_bytes(b"original database")
    before = smoke._tree(tmp_path)
    target = tmp_path / name
    target.parent.mkdir(exist_ok=True)
    target.write_bytes(content)
    assert smoke._tree(tmp_path) != before


def test_existing_nonempty_wal_mutation_is_detected(
    smoke: ModuleType, tmp_path: Path
) -> None:
    (tmp_path / "journal.sqlite3").write_bytes(b"authoritative database")
    wal = tmp_path / "journal.sqlite3-wal"
    wal.write_bytes(b"original committed frames")
    before = smoke._tree(tmp_path)
    wal.write_bytes(b"changed committed frames")
    assert smoke._tree(tmp_path) != before

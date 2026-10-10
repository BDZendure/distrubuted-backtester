# Phase 0 Implementation Plan

**Goal:** Establish a reviewable architecture and an installable Python foundation,
then stop for explicit approval before Phase 1.

**Architecture:** One Python package with logical component boundaries. A future
deterministic simulation function accepts validated in-memory data and immutable
configuration. Execution backends wrap that function in subsequent phases.

**Tech stack:** Python 3.12+, setuptools, pytest, Ruff, mypy, build, GitHub Actions.

## Global constraints

- Implement Phase 0 only; simulation contracts are documentation, not code.
- Preserve the existing MIT license and any user changes.
- Add no runtime dependencies or distributed infrastructure.
- Use a `src/` layout and a focused local feature branch.
- Record actual checks; do not invent benchmark or CI results.
- Include beginner and technical explanations requested by the user.
- Stop at the Phase 1 approval checkpoint; do not push to a remote.

## Task 1: Architecture and learning documents

**Files:** `docs/architecture.md`, `docs/roadmap.md`,
`docs/decisions/README.md`, `docs/decisions/0001-modular-package.md`,
`docs/decisions/0002-simulation-semantics.md`,
`docs/decisions/0003-reproducibility-and-results.md`,
`docs/testing.md`, `docs/benchmarking.md`, `docs/dependencies.md`,
`docs/clear explanation.md`, `docs/technical-explanation.md`, `README.md`.

- [x] Inspect repository, history, global instructions, and Python environment.
- [x] Critically review the handoff and choose simple component boundaries.
- [x] Document timing, accounting, data validation, contracts, and risks.
- [x] Describe all phases, dependencies, acceptance criteria, and validation.
- [x] Explain the same decisions at beginner and technical levels.
- [x] Independently review assumptions and fix ambiguities.

## Task 2: Packaging and continuous integration

**Files:** `pyproject.toml`, `MANIFEST.in`, `.gitignore`, `.github/workflows/ci.yml`,
`src/distributed_backtester/__init__.py`,
`src/distributed_backtester/py.typed`, `tests/test_packaging.py`.

**Interfaces:** An installed importable package called `distributed_backtester`;
no CLI, simulation functions, domain models, or execution backends yet.

- [x] Record and pin direct development/build dependency versions from PyPI.
- [x] Add a smoke test that imports the installed package in a subprocess
      outside the repository with isolated Python mode.
- [x] Observe the import test fail before adding the package.
- [x] Add the docstring-only package, metadata, strict tool configuration,
      and CI on Python 3.12 and 3.14.
- [x] Install development tools in the repository-local virtual environment.
- [x] Run pytest, lint, format, type checking, and dependency consistency checks.
- [x] Build a wheel and source distribution; install the wheel into a fresh
      environment and import it from outside the repository.

The packaging test is intentionally limited to a real installation boundary:

```python
from pathlib import Path
import subprocess
import sys


def test_installed_package_imports_outside_checkout(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-I", "-c", "import distributed_backtester"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
```

Verification commands after editable installation:

```bash
python -m pytest
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m pip check
python -m build
```

Expected: one packaging test passes; lint, format, type, and dependency checks
succeed; both distribution formats build. These are expectations until executed.

## Task 3: Review and checkpoint

**Files:** `docs/phase-0-report.md` and this plan.

- [x] Review docs against the handoff and the revised decisions.
- [x] Check internal Markdown links and Git whitespace diagnostics.
- [x] Record exact commands, versions, results, and limitations in the report.

After these checks, commit the reviewed Phase 0 foundation on the feature branch
and present the completion report with the explicit Phase 1 approval question.
The actual commit and presentation are recorded in Git history and the conversation.

The user's instruction authorizes completing Phase 0 in this session. The next
approval gate is Phase 1; successful checks do not authorize that phase.

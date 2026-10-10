"""Check the installation boundary without importing from the checkout."""

import subprocess
import sys
from pathlib import Path


def test_installed_package_imports_outside_checkout(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-I", "-c", "import distributed_backtester"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

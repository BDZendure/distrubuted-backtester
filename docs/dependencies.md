# Dependency plan

## Phase 0 environment

Runtime dependencies are empty. Development/build tools are declared and pinned
in `pyproject.toml`; installation uses a repository-local virtual environment.

| Tool | Direct version | Purpose |
| --- | --- | --- |
| setuptools | 84.0.0 | Build backend and package discovery |
| pytest | 9.1.1 | Automated checks |
| Ruff | 0.17.0 | Lint and formatting |
| mypy | 2.4.0 | Strict static type checks |
| build | 1.6.1 | Wheel and source distribution validation |

Versions were read from public PyPI metadata during Phase 0. Transitive dependencies
are resolved by pip, so this is not a complete environment lock. Capture `pip freeze`
and exact Python/OS versions for benchmark evidence; adopt a complete environment
lock before making reproducible performance comparisons in Phase 2.

The CI Python matrix is 3.12 and 3.14. Local Phase 0 validation uses the available
Python 3.14 interpreter; CI configuration is not proof of an observed Python 3.12 run.
Future optional libraries may impose narrower interpreter compatibility and must
be verified at adoption time.

## Planned additions

| Phase | Dependencies to evaluate | Rationale |
| --- | --- | --- |
| 1 | Standard-library `csv`, `decimal` at parsing boundaries, `argparse`, `json`, `hashlib`; NumPy only if a demonstrated numerical need warrants it | Exact small synthetic simulations do not need a dataframe framework |
| 2 | Standard-library `concurrent.futures`, `multiprocessing`; a measurement tool such as psutil if needed | Local CPU execution and whole-process memory measurement |
| 3 | Ray as an optional extra | Distributed scheduling and data placement |
| 4 | Standard-library SQLite candidate | Atomic lifecycle/result commits for a single coordinator |
| 5 | PyArrow and DuckDB; pandas for concrete tabular/reporting use if needed | Columnar storage and analytical queries |
| 6 | Profilers as justified; NumPy or compiled tools only for measured bottlenecks | Optimize based on evidence |
| 7 | Docker tooling and operational additions justified by the implementation | Reproducible developer/runtime environment |
| 8 | No assumed new runtime dependencies | Documentation and verified portfolio evidence |

This refines the handoff's preferred stack rather than rejecting it. NumPy and
pandas remain good tools for numerical arrays and reporting; daily event accounting
must retain its documented chronology and exact ledger regardless of those tools.

For each addition, document purpose, version compatibility, optionality, memory or
deployment consequences, and reproduction commands. Do not import Ray or storage
extras on a sequential execution path that does not use them.

## Tool references

- [PyPA pyproject guide](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/)
- [pytest installation and layout practices](https://docs.pytest.org/en/stable/explanation/goodpractices.html)
- [Ruff configuration](https://docs.astral.sh/ruff/configuration/)
- [mypy configuration](https://mypy.readthedocs.io/en/stable/config_file.html)
- [checkout action](https://github.com/actions/checkout)
- [setup-python action](https://github.com/actions/setup-python)

CI uses verified full commit revisions for the v6 checkout/setup-python actions,
read-only repository permissions, and no persisted checkout credentials. Updates
to pinned tool or action revisions should be reviewed and checked together.

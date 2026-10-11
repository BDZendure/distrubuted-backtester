# Dependency plan

## Current Phase 3 environment

Runtime dependencies are empty. Development/build tools are declared and pinned
in `pyproject.toml`; installation uses a repository-local virtual environment.

| Tool | Direct version | Purpose |
| --- | --- | --- |
| setuptools | 84.0.0 | Build backend and package discovery |
| pytest | 9.1.1 | Automated checks |
| Ruff | 0.17.0 | Lint and formatting |
| mypy | 2.4.0 | Strict static type checks |
| build | 1.6.1 | Wheel and source distribution validation |
| psutil (optional `benchmark` extra) | 7.2.2 | Sample coordinator/descendant RSS; hardware observations |
| Ray (optional `distributed` extra) | 2.59.0 | Remote actors, resource scheduling, and object-store data placement |

The core and process backend still require no third-party runtime dependency.
Phase 2 adds optional psutil; sequential/process sweeps work without it. Version
and macOS arm64 wheel support were checked against [PyPI metadata](https://pypi.org/project/psutil/).
Its [API documentation](https://psutil.readthedocs.io/stable/) defines RSS and process-tree
observations. The complete observed macOS arm64/Python 3.14.6 tool environment,
including transitive versions, is in [benchmark.lock](../requirements/benchmark.lock).
It is an exact version snapshot, without artifact hashes or a universal platform
solver. Reports also capture all installed versions and Python/OS context.
Install the lock before `--no-deps` project installation for equivalent tools;
new platforms may require a separately reviewed snapshot.

The CI Python matrix is 3.12 and 3.14. Local Phase 0 validation uses the available
Python 3.14 interpreter, as does Phase 1; CI configuration is not proof of an
observed Python 3.12 run.
Future optional libraries may impose narrower interpreter compatibility and must
be verified at adoption time.

Phase 3 pins the optional core Ray package to 2.59.0, excluding dashboard, cluster
launcher, ML, and storage extras. Published wheel resolution and a real local
cluster succeeded on CPython 3.14.6/macOS arm64. This establishes that environment,
not every claimed supported platform. The optional integration CI matrix is
configured for Linux/Python 3.12 and 3.14; remote CI outcomes remain unobserved.
The observed complete environment is captured separately in
[ray-benchmark.lock](../requirements/ray-benchmark.lock), preserving Phase 2's
historical lock. Ray's dependencies are installed only by the `distributed` extra;
the core wheel remains dependency free. All cluster nodes require compatible
Python/Ray versions and identical installed project source.

The private reaper-pipe cleanup adapter in `backends/ray.py` is tied to Ray 2.59.0.
Any pin update must revalidate strict warning checks, local/external lifecycle,
actor semantics, and optional import behavior. Ray publishes dynamic runtime API
types; the narrow boundary uses explicitly typed dataclasses and validates remote
outcomes before handing them to the collector.

## Planned additions

| Phase | Dependencies to evaluate | Rationale |
| --- | --- | --- |
| 1 (implemented) | Standard-library `csv`, exact integer text parsing, `fractions`, `argparse`, `json`, `hashlib`, filesystem publication | No runtime dependency added; exact accounting needs no dataframe framework |
| 2 (implemented) | Standard-library `concurrent.futures`, `multiprocessing`; optional psutil 7.2.2 | Local CPU execution and whole-process memory measurement |
| 3 (implemented) | Optional Ray 2.59.0 | Remote actor scheduling and data placement |
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

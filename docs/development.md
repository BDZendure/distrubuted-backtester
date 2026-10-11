# Development and installed checks

Python 3.12 and 3.14 remain the native CI matrix. The dependency-free core is
still installable without extras. Development uses the existing `dev` extra;
measurement, Ray and storage use `benchmark`, `distributed` and `storage`.
Use a frozen package source for native tests and subprocess smoke runs: an
experiment records installed source bytes, so editing the package while workers
start can change its identity.

## Native development

From a checkout:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --editable '.[dev,benchmark,distributed,storage]'
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
.venv/bin/python -m mypy src tests scripts/smoke.py
.venv/bin/python -m pytest
.venv/bin/python -m pip check
.venv/bin/python -m build
```

These native commands resolve platform-specific transitive requirements from
the pinned direct extras. Existing [observed locks](dependencies.md) describe
the original macOS environments. They are not Linux container locks.

## Installed wheel smoke

The helper runs fresh CLI parent processes with `python -I -m
distributed_backtester`, a finite deadline per child, strict stdout JSON checks,
JSON stderr checks when logging is enabled, and complete artifact verification.
It rejects editable installation. Outputs are new `smoke-*` children of the
chosen parent, retained after success, failure or timeout. `report.json` and
per-command `.command.json`, `.stdout`, `.stderr` preserve commands, exit codes,
signal observations and application output. Timeout cleanup targets only the
child's newly created process group. No wall-clock performance ratio is a gate.

After building, run against a core-only wheel from outside the checkout:

```bash
python3 -m venv .venv-wheel
.venv-wheel/bin/python -m pip install --no-deps dist/distributed_backtester-0.8.0-py3-none-any.whl
wheel_python="$PWD/.venv-wheel/bin/python"
smoke_script="$PWD/scripts/smoke.py"
sample_data="$PWD/examples/sma.csv"
smoke_parent="$(mktemp -d /tmp/backtester-smoke.XXXXXX)"
cd "$smoke_parent"
"$wheel_python" -I -m pip check
"$wheel_python" -I "$smoke_script" --data "$sample_data" \
  --output-parent "$smoke_parent" --timeout 120 --signals
```

The core check verifies reference equity `749.257000`, fees `1.743000`, two
detailed fills, configuration and explicit overrides, malformed configuration,
sequential/process complete output equality, spawned worker logs, and classic
read-only status including `--job-id`. Configured backtest and sweep paths are
file-relative. Logging OFF keeps stderr empty on successful core commands.

For actual optional execution, install the built wheel with
`[benchmark,distributed,storage]` in a separate environment and add
`--ray --storage --signals`. This starts real local Ray actors, compares complete
Ray/durable outputs, checks persisted attempts and committed log correlations,
resumes with a TOML configuration, and exercises Parquet conversion/backtest and
DuckDB import/query. `--signals` uses a generated 40,000-bar dataset, waits for a
driver `job_started` or persisted `attempt_claimed` event, then sends SIGTERM or
SIGINT. Expected driver exits are 143 and 130. With `--ray`, SIGTERM recovery
also checks generation advancement, unique successful attempts, and fully
validated final exports. Subprocess deadlines are correctness bounds, not speed
benchmarks. CLI-level interruption does not itself test `docker stop` delivery
or forced container termination; see [operations](operations.md).

## Container development

The Dockerfile's default runtime and explicit `dev` target install the
same project wheel into `/opt/venv`. The dev target adds existing development
tools, the pinned build backend and checkout files under `/workspace`; it never
uses an editable install. Both targets start in `/outputs` with UID/GID
`10001:10001`. Use `--target runtime` for the smaller CLI image.

After verifying daemon and storage availability:

```bash
docker version
docker info
df -h .
docker build --target runtime --build-arg VCS_REF="$(git rev-parse HEAD)" \
  -t distributed-backtester:0.8.0-runtime .
docker build --target dev --build-arg VCS_REF="$(git rev-parse HEAD)" \
  -t distributed-backtester:0.8.0-dev .
docker run --rm --init --entrypoint python distributed-backtester:0.8.0-dev \
  -I -m pip check
docker run --rm --init --entrypoint python distributed-backtester:0.8.0-dev \
  -I -m ruff check /workspace
docker run --rm --init --entrypoint python distributed-backtester:0.8.0-dev \
  -I -m ruff format --check /workspace
docker run --rm --init --workdir /workspace --entrypoint python \
  distributed-backtester:0.8.0-dev -I -m mypy src tests scripts/smoke.py
docker run --rm --init --workdir /workspace --cpus=2 --memory=4g \
  --memory-swap=4g --shm-size=256m --entrypoint python \
  distributed-backtester:0.8.0-dev -I -m pytest
docker run --rm --init --workdir /workspace --entrypoint python \
  distributed-backtester:0.8.0-dev -I -m build --no-isolation --outdir /outputs/dist
```

Static checks and the existing full test suite use `/workspace` for fixture
paths. `-I`, pytest's import mode, and the absence of `PYTHONPATH=src` keep
application imports on the installed wheel. Operational smoke executes from
`/outputs` without a checkout mount in both targets. Mount `/outputs` when build
or test outputs must survive removal. The dev image omits historical benchmark
payloads to limit build context size; use the complete source distribution for
historical evidence. Native CI retains the existing Ray fault/storage tests and
both Python minors. Container CI builds/runs native Linux amd64 runtime/dev,
inspects UID/resource/input mount settings, and retains smoke artifacts from a
volume after removing the smoke container. No job pushes an image.

## Linux dependency locks

The immutable base is the official multi-platform index:

```text
python:3.12.15-slim-bookworm@sha256:34386ef0cb081344d7ec1c103ba398e6e9f64e9ab3a1509accc92a4e24a07258
```

Its [official manifest metadata](https://raw.githubusercontent.com/docker-library/repo-info/master/repos/python/remote/3.12.15-slim-bookworm.md)
lists Linux amd64 and arm64. Matching builder/runtime stages use this same
digest. Base and wheel availability establish packaging support; successful
runtime claims require an actual build/run on each architecture.

The three reviewed locks are specific to CPython 3.12.15 and Debian bookworm's
glibc 2.36 on Linux amd64/arm64:

| Lock | Installation contract |
| --- | --- |
| `container-runtime.lock` | 21 exact packages, closure of all existing runtime extras |
| `container-dev.lock` | Includes runtime lock, plus 12 existing dev packages and transitives |
| `container-build.lock` | `setuptools==84.0.0`, the existing build backend |

Each lock permits binary wheels only, pins every transitive version, and includes
SHA-256 hashes only for compatible wheels. Comments give primary PyPI release
JSON URLs and filenames for each architecture. Versions were resolved separately
for both architectures using temporary `uv==0.13.0`, then compared. All selected
wheel dependency metadata was checked against Linux/Python 3.12 marker contexts;
PEP 658 metadata hashes were checked against the primary PyPI simple index.
The dev image installs both dev and build locks, for 34 distinct packages before
the project wheel. `pip` itself is supplied by the immutable Python base.
`uv` is resolution tooling, not a project dependency or image component.

To regenerate in a temporary tooling environment, run from the checkout:

```bash
python3 -m venv /tmp/backtester-lock-tool
/tmp/backtester-lock-tool/bin/python -m pip install uv==0.13.0
for platform in x86_64-manylinux_2_36 aarch64-manylinux_2_36; do
  /tmp/backtester-lock-tool/bin/uv pip compile pyproject.toml \
    --extra benchmark --extra distributed --extra storage \
    --python-version 3.12.15 --python-platform "$platform" \
    --no-python-downloads --only-binary :all: --generate-hashes \
    --no-header --no-annotate --default-index https://pypi.org/simple \
    --output-file "/tmp/backtester-runtime-$platform.resolved"
  /tmp/backtester-lock-tool/bin/uv pip compile pyproject.toml --all-extras \
    --python-version 3.12.15 --python-platform "$platform" \
    --no-python-downloads --only-binary :all: --generate-hashes \
    --no-header --no-annotate --default-index https://pypi.org/simple \
    --output-file "/tmp/backtester-dev-$platform.resolved"
done
```

Review both exact version closures before updating locks. Resolver hash output
can include other platforms and source archives; filter it using the published
wheel filenames/tags, `requires_python`, artifact SHA-256 and `Requires-Dist`
metadata from `https://pypi.org/pypi/NAME/VERSION/json` and the PEP 691 simple
index. Reject yanked files and ensure every dependency selected by Linux/Python
3.12 markers is pinned within the appropriate closure. Preserve runtime pins in
the dev closure, and retain only its additional packages in the dev lock.
Resolve the build-system requirement from `pyproject.toml` the same way. Add no
new direct project dependency for container resolution.

Before accepting regenerated locks, use an actual Linux 3.12 environment on
each architecture, install with `python -m pip install --require-hashes -r
requirements/container-dev.lock` and the build lock, run `pip check`, then run
installed smoke. A primary metadata/hash check is not a downloaded-wheel hash
verification or a Linux execution result. Docker's pip installs enforce wheel
hashes during the actual image build. Keep actual local, container and remote CI
results separate in the release verification record.

## Observed Phase 7 checks

The [completion report](phase-7-report.md) and
[verification record](phase-7-verification.json) distinguish the actual checks:
macOS ARM64/Python 3.14.6 full suite, fresh dependency-free wheel execution, and
Linux ARM64/Python 3.12.15 runtime/dev images. Both images imported the installed
wheel as UID 10001. Runtime smoke exercised actual Ray, journal resume, storage
and signals; dev static checks and installed core smoke passed. Docker stop,
SIGINT and forced SIGKILL recovery used a persistent local named volume.
No complete dev-image Linux test suite, Linux AMD64 run or remote Actions run
is claimed. These remain configured CI checks or reproduction procedures.

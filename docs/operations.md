# Operating the installed CLI

The runtime image includes the existing benchmark, distributed and storage
extras. Core wheels remain dependency-free. Build an installed image using the
[development instructions](development.md); it uses the official Python
3.12.15 slim bookworm digest recorded there. The runtime has no source checkout,
editable install, dashboard, job server or background Ray supervisor.

## Paths, mounts and ownership

Default UID/GID is `10001:10001`, home is `/home/backtester`, working directory
is `/outputs`, and examples/configs are at `/opt/backtester/examples`. Installed
code stays under root-owned `/opt/venv`. Mount datasets/configs read-only at
`/inputs` and the whole writable output parent at `/outputs`. TOML paths are
relative to the config file; explicit CLI paths are relative to the working
directory. Examples omit an output destination, so select a fresh child on the
CLI. A sweep creates its experiment child itself. Precreating that child makes
the command fail.

On Linux, a host bind mount hides the image's directory ownership. As a nonroot
host user, use the host UID/GID with a writable temporary home:

```bash
mkdir -p artifacts/operations
docker run --rm --init --stop-timeout=30 \
  --user "$(id -u):$(id -g)" --env HOME=/tmp \
  --cpus=2 --memory=4g --memory-swap=4g --shm-size=256m \
  --mount "type=bind,source=$PWD/examples,target=/inputs,readonly" \
  --mount "type=bind,source=$PWD/artifacts/operations,target=/outputs" \
  distributed-backtester:0.8.0-runtime backtest \
  --config /inputs/backtest.toml --fee-bps 10 \
  --output /outputs/reference.json --log-level INFO
```

For the image's default UID, provision the bind parent with ownership matching
10001 or use a local Docker-managed volume. A new local volume mounted at
`/outputs` inherits the image directory's initial ownership. For durable runs
on Docker Desktop, prefer a local named volume until the intended host bind
filesystem's flock, WAL, fsync, hard-link and atomic replacement behavior has
been validated. Mount the entire experiment directory; mounting only
`journal.sqlite3` separates its WAL/SHM, coordinator lock and atomic exports.
Persistence is not a power-loss guarantee. See [reliability](reliability.md).

## Output and operational logs

Successful command stdout remains one JSON document for backtests without
`--output`, sweep/resume/status, and storage commands. Backtest with `--output`
writes the result file instead of a stdout success document. Logs are opt-in
JSON lines on stderr, using `--log-level OFF|ERROR|WARNING|INFO|DEBUG` on the
command; the default is OFF. Keep stdout and stderr separate:

```bash
docker run --rm --init distributed-backtester:0.8.0-runtime backtest \
  --config /opt/backtester/examples/backtest.toml --fee-bps 10 --log-level INFO \
  > reference.json 2> operations.jsonl
```

Each operational record has schema version, UTC timestamp, level, event and
process PID. Experiment, job and durable attempt fields supply correlation when
available. Driver commit observations are emitted after journal commit calls
return. Delivery is best effort and does not establish a commit on its own.
Read the journal for authoritative durable state. Ray actor logs stay in their
local Ray session files; driver application JSON remains on stderr without Ray
worker forwarding. Native Ray's own failure diagnostics can appear in session
files and on driver stderr during interruption. Application records are JSON;
native diagnostics may be plain text. Sensitive external-cluster token contents
should not appear in configuration or logs.

## Tiny sweeps and persistent smoke

This configured sequential sweep uses a fresh child under the mounted parent:

```bash
docker run --rm --init --stop-timeout=30 \
  --user "$(id -u):$(id -g)" --env HOME=/tmp \
  --cpus=2 --memory=4g --memory-swap=4g --shm-size=256m \
  --mount "type=bind,source=$PWD/examples,target=/inputs,readonly" \
  --mount "type=bind,source=$PWD/artifacts/operations,target=/outputs" \
  distributed-backtester:0.8.0-runtime sweep --config /inputs/sweep.toml \
  --fee-bps 10 --output-dir /outputs/sequential --log-level INFO
```

Use `--backend process --workers 2 --max-in-flight 4` for process execution.
For local native Ray, add `--backend ray --workers 2 --max-in-flight 4
--cpus-per-worker 1 --object-store-memory 134217728`. Choose a different child
for each run. The two-CPU, 4-GiB memory, 256-MiB shared-memory profile is a
starting configuration for tiny runs, not a measured minimum or capacity
guarantee. Shared memory, object store, worker heaps and driver/Ray overhead
consume the memory limit. Equal `--memory` and `--memory-swap` disable container
swap. Ray's logical worker reservations do not enforce kernel CPU quotas, and
the application's observed CPU count is not a claim of quota-aware capacity.
Align requested worker capacity with the configured Docker quota.

Run all actual smoke paths and preserve outputs after container removal:

```bash
docker volume create backtester-smoke-outputs
docker run --rm --init --stop-timeout=30 \
  --cpus=2 --memory=4g --memory-swap=4g --shm-size=256m \
  --mount "type=bind,source=$PWD/examples,target=/inputs,readonly" \
  --mount type=volume,source=backtester-smoke-outputs,target=/outputs \
  --entrypoint python distributed-backtester:0.8.0-runtime \
  -I /opt/backtester/scripts/smoke.py --data /inputs/sma.csv \
  --output-parent /outputs --timeout 180 --ray --storage --signals
docker run --rm --init \
  --mount type=volume,source=backtester-smoke-outputs,target=/outputs,readonly \
  --entrypoint python distributed-backtester:0.8.0-runtime -I -c \
  'from pathlib import Path; [print(p.read_text()) for p in Path("/outputs").glob("smoke-*/report.json")]'
```

The helper retains complete results and per-command evidence under unique
`smoke-*` children. `--ray` starts real classic and durable Ray runs.
`--storage` exercises real PyArrow/DuckDB; no mocked success is reported.
`--signals` checks application SIGINT/SIGTERM exits and, with `--ray`, durable
SIGTERM recovery. The helper's deadlines also cover Ray startup and cleanup.
Use the image's pinned source and the same input for recovery.

## Durable stop, status and resume

Use a fresh local volume and a fresh experiment child. The following commands
are a manual lifecycle procedure; wait for a journaled running attempt through
status or the `attempt_claimed` JSON event before stopping. Increase the dataset
or grid if the tiny sample finishes before observation. A completed sample
cannot demonstrate interruption recovery.

```bash
docker volume create backtester-durable-outputs
docker run --detach --name backtester-durable --init --stop-timeout=30 \
  --cpus=2 --memory=4g --memory-swap=4g --shm-size=256m \
  --mount "type=bind,source=$PWD/examples,target=/inputs,readonly" \
  --mount type=volume,source=backtester-durable-outputs,target=/outputs \
  distributed-backtester:0.8.0-runtime sweep --config /inputs/sweep.toml \
  --backend ray --workers 2 --max-in-flight 4 --cpus-per-worker 1 \
  --object-store-memory 134217728 --durable --fee-bps 10 \
  --output-dir /outputs/experiment --log-level INFO
docker logs backtester-durable
docker run --rm --init \
  --mount type=volume,source=backtester-durable-outputs,target=/outputs \
  distributed-backtester:0.8.0-runtime status --output-dir /outputs/experiment
docker stop --timeout 30 backtester-durable
docker inspect --format '{{.State.ExitCode}}' backtester-durable
docker run --rm --init \
  --mount type=volume,source=backtester-durable-outputs,target=/outputs \
  distributed-backtester:0.8.0-runtime status --output-dir /outputs/experiment
docker run --rm --init --stop-timeout=30 \
  --cpus=2 --memory=4g --memory-swap=4g --shm-size=256m \
  --mount "type=bind,source=$PWD/examples,target=/inputs,readonly" \
  --mount type=volume,source=backtester-durable-outputs,target=/outputs \
  distributed-backtester:0.8.0-runtime resume --data /inputs/sma.csv \
  --output-dir /outputs/experiment --workers 2 --max-in-flight 4 \
  --object-store-memory 134217728 --log-level INFO
```

For a separate forced-termination drill on an observed active run, use
`docker kill --signal KILL backtester-durable`, then the same status/resume
procedure. This is not graceful shutdown. Preserve the volume and completed
commits, check generation fencing, and verify every final artifact after
resume. Do not start a second coordinator on the same directory. Interrupted
classic exports can be incomplete and have no resume contract.

Status accepts complete classic exports or a validated journal and never starts
a runtime, resumes, creates an experiment or acquires its coordinator lock.
Durable status is logically read-only, but needs directory write permission
when SQLite must create WAL/SHM bookkeeping. Use a writable output mount for
these status commands; a physical read-only mount can fail for a valid closed
journal with absent sidecars. Dataset mounts and JSON/export copying stay
read-only. Status does not modify committed jobs or results.
`--job-id LOWERCASE_SHA256` filters jobs/attempts while counts remain aggregate.
Last-observed worker metadata is not present liveness. A corrupt journal does
not fall back to exported files.

The CLI returns 143 for controlled SIGTERM and 130 for Ctrl-C/SIGINT. Docker uses
the exec-form CLI entrypoint and `STOPSIGNAL SIGTERM`; use `--init` to forward
signals and reap children. Cleanup has no guaranteed short bound: process pools
can wait for running jobs. After the stop timeout Docker can force SIGKILL.
Exit 137 can indicate SIGKILL or OOM; inspect `.State.OOMKilled` and Docker logs
instead of treating it as application completion. Docker launch errors have
their own codes (125, 126, 127). CLI validation errors return 2; sweep job failures
return 1. See the [Docker stop reference](https://docs.docker.com/reference/cli/docker/container/stop/).

## Exporting results and external Ray

Copy a completed output tree from a named volume without modifying it:

```bash
mkdir -p artifacts/exported
docker create --name backtester-export \
  --mount type=volume,source=backtester-durable-outputs,target=/outputs,readonly \
  distributed-backtester:0.8.0-runtime
docker cp backtester-export:/outputs/. artifacts/exported/
docker rm backtester-export
```

Names in this runbook belong to these commands; choose fresh names when other
runs exist. Retain output volumes until reviewed. Removing a stopped container
does not remove its named output volume. Never use broad prune commands as
application cleanup.

External Ray uses native GCS attachment with `--ray-address HOST:PORT` or
`auto`; `ray://` is unsupported. Nodes need matching Python minor, pinned Ray
version and installed source identity. The driver loads data into the object
store, so workers do not need the input mount. Provide trusted connectivity and
the separately mounted `RAY_AUTH_TOKEN_PATH` where required. Docker Desktop
external-cluster routing is not established by these commands. Owned local Ray
services stop on cleanup; disconnecting from an external cluster does not stop
that cluster. Do not run administrative `ray stop` as experiment cleanup. See
[native Ray clusters](ray-clusters.md).

## Evidence limits

The lock resolutions and primary artifact metadata support both Linux
architectures. Container CI is configured to exercise native Linux amd64.
The [Phase 7 report](phase-7-report.md) and
[verification record](phase-7-verification.json) record actual Linux ARM64
runtime/dev builds, installed CLI checks and STOP/INT/KILL plus resume. The
checks used a local named volume and read-only inputs with two CPUs, 4 GiB
memory and 256 MiB shared memory. These are observations for that environment,
not a minimum resource guarantee. Linux AMD64, remote CI, host bind-filesystem
durability, power-loss recovery and external-cluster routing remain unobserved.
Record the actual image ID/base digest, architecture, limits, CLI reports and
retained artifacts when running these procedures in another environment.

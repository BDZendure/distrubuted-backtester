# Running Ray experiments

## One machine, automatic cluster lifetime

Install the optional dependency in your activated environment:

```bash
python -m pip install --editable ".[distributed]"
distributed-backtester sweep --data examples/sma.csv --symbol SYNTH \
  --price-basis raw --fast 1 2 --slow 3 4 --fee-bps 10 \
  --backend ray --workers 2 --max-in-flight 4 \
  --output-dir artifacts/sweep-ray
python -m json.tool artifacts/sweep-ray/execution.json
```

Choose a new output directory for each run. The command starts an owned local
cluster, initializes two actors, executes four jobs, exports results, and stops
its actors and services. It uses real remote processes, with no Ray local mode.
The dashboard is disabled. The supplied CSV is synthetic and ready to use.

`--cpus-per-worker` sets each actor's lifetime CPU reservation, default 1.
Repeat `--worker-resource backtest=1` to request named custom resources. The local
cluster declares enough CPU/custom capacity for the requested pool. Fractional
CPU reservations are allowed; they do not speed up a single-thread simulation or
enforce operating-system CPU quotas. `--object-store-memory 268435456` changes
the local store capacity in bytes; the default is 128 MiB. It is capacity, not
observed usage. The option is rejected when attaching to an existing cluster.
CPU/custom requests must be at least 0.0001 and have at most four decimal places.
Local node capacity is rounded up to whole units while actor requests retain
their fractions. Invalid resource options are rejected before output creation.

The library's `startup_timeout_seconds` bounds actor readiness, default 60 seconds.
Ray controls cluster startup timeouts. Individual job deadlines are not implemented.
Do not edit installed source while drivers or workers are running; restart Python
after editing it. Worker initialization checks the exact installed source digest.

## An existing cluster

Run the driver on a cluster machine or another machine with direct access to its
GCS and worker network. This phase does not implement Ray Client (`ray://`), code
uploading, automatic dependency installation, or cloud provisioning.

Every node needs the same Python minor version, Ray 2.59.0, and project wheel.
Use the same built wheel on every node rather than independently edited checkouts:

```bash
python -m build
python -m pip install 'dist/distributed_backtester-0.4.0-py3-none-any.whl[distributed]'
python -c 'from distributed_backtester.provenance import current_implementation; print(current_implementation())'
```

The driver reads the CSV and places the immutable dataset in Ray's object store.
Workers do not need the CSV path. Each actor still decodes its own Python objects.
Only the coordinator writes result artifacts, so output need not be shared storage.

On a dedicated head machine, with its activated environment:

```bash
ray start --head --node-ip-address HEAD_PRIVATE_IP --port 6379 \
  --num-cpus 2 --include-dashboard false --object-store-memory 134217728 \
  --resources '{"backtest": 2}'
```

On each dedicated worker machine, after replacing the address:

```bash
ray start --address HEAD_PRIVATE_IP:6379 --num-cpus 2 \
  --object-store-memory 134217728 --resources '{"backtest": 2}'
```

Use the cluster owner's token configuration on nodes and driver. Ray 2.59 enables
local token authentication by default; an existing cluster connection requires
the cluster's token when authentication is enabled. Prefer a protected token file
configured through `RAY_AUTH_TOKEN_PATH`; never place it in manifests or the repo.
See [Ray token configuration](https://docs.ray.io/en/latest/ray-security/token-auth.html).
Use private, trusted connectivity rather than exposing Ray ports publicly.

On the driver, using an accessible CSV and a fresh result directory:

```bash
distributed-backtester sweep --data examples/sma.csv --symbol SYNTH \
  --price-basis raw --fast 1 2 --slow 3 4 --fee-bps 10 \
  --backend ray --workers 4 --ray-address HEAD_PRIVATE_IP:6379 \
  --worker-resource backtest=1 --output-dir artifacts/cluster-sweep
```

`--ray-address auto` discovers an already started local cluster and errors if
none exists. An explicit address avoids discovery ambiguity. Cluster capacity
must fit all actor reservations on live nodes. Busy clusters can delay readiness;
this backend does not autoscale or wait for additional nodes to appear.

The command kills its own actors and disconnects its driver. The external cluster
remains running. Only its owner should stop it. On dedicated machines running
only this cluster, the owner can use `ray stop` after all experiments finish.
This is a cluster-wide administrative action and is never library cleanup.

## Verification and measurement boundaries

`execution.json` records actor PIDs/node IDs, initialization/job counts, resource
requests, node capacity, and component times. Semantic files are the manifest,
per-job artifacts, and final index; backend metadata does not affect their bytes.

```bash
python -c 'from pathlib import Path; from distributed_backtester.benchmark import semantic_digest; print(semantic_digest(Path("artifacts/sweep-ray")))'
```

Compare that verified digest with sequential and process runs using identical
source, symbol, settings, and CSV. Infrastructure failures leave no final index.
No actor restart, task retry, experiment resume, or durable recovery is promised.

Local multiple-node tests share one physical machine and establish functional
placement/data transfer behavior only. Multiple-machine instructions describe
the deployment contract; actual multiple-machine scaling needs real machines.
The local benchmark cannot measure memory or network traffic on remote nodes.
On macOS, multi-node support is not generally validated by upstream Ray; prefer
Linux for a future multiple-machine deployment and validate it there explicitly.
See [Ray platform support](https://docs.ray.io/en/latest/ray-overview/installation.html).

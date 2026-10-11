# ADR 0005: Optional Ray actor execution

**Status:** Accepted for approved Phase 3.

## Context

Phase 2 already separates immutable jobs, deterministic simulation, and artifact
publication from execution. Phase 3 needs real remote worker processes and an
existing-cluster connection without introducing durable scheduling early.

## Decision

Pin the optional `distributed` extra to Ray 2.59.0. The dependency-free core remains
usable without importing Ray. Resolve the platform wheel before installation;
CPython 3.14.6 on macOS arm64 has a published matching wheel. Wheel availability
does not prove every platform or cluster topology has been validated.

Use a fixed actor pool with explicit lifetime CPU and optional custom resource
reservations. Each actor checks package version/source identity once, receives
the immutable dataset once, and uses `execute_job` for every simulation. A fresh
strategy is constructed per job. Independent tasks were considered but would
decode Python data repeatedly and make an exact worker limit less direct on an
existing cluster. A custom scheduler is deferred to Phase 4.

Put the dataset into the object store once. References allow Ray to transfer the
object to nodes that need it. Ordinary Python tuples/dataclasses still deserialize
into each actor's heap. Neither zero copy nor one physical copy across machines
is promised. Serialized payload bytes and placement time are operational
observations, not physical object-store occupancy measurements.

Bound unconsumed remote calls using `max_in_flight`, default twice worker count.
Distribute calls round robin to actors, collect ready results using `ray.wait`,
and stream them through the existing collector. SPREAD prefers placement across
nodes; it does not promise equal load. A queued call to a busy actor can wait
while another actor is idle. This simple scheduling policy is documented and
can be measured before replacing it.

Explicitly set `max_restarts=0` and `max_task_retries=0`. Ordinary engine exceptions
remain job failures. Actor initialization, death, protocol, and IO failures abort
the experiment without a final index. This phase makes no recovery guarantee.

Default to a new owned local cluster with address `local`, dashboard disabled,
and a 128 MiB object-store budget unless configured. An explicit GCS address or
`auto` attaches to a prestarted cluster. Refuse an already initialized driver;
do not commandeer its connection. Cleanup kills experiment actors and disconnects
this driver. Local ownership also shuts down its services. Never call global
`ray stop` from library cleanup.

Count feasible actor slots per live node using CPU and every custom resource.
Summed cluster resources alone can admit impossible placements. Wait at most the
configured readiness timeout after actor creation; busy clusters can still delay
jobs afterward. Local cluster startup itself follows Ray's startup timeouts.
Job deadlines belong to Phase 4.

The GCS driver and all nodes must have compatible Python/Ray versions and an
identical installed project. Source mismatches fail closed. Automatic package
deployment, Ray Client, autoscaling, and remote cluster administration are absent.
Resource reservations are scheduling quantities, not OS CPU isolation.

## Consequences

The simulation and semantic JSON contracts remain shared across three backends.
Operational metadata adds worker PIDs/node IDs, resource requests, node capacity,
initialization counts, and startup/placement/readiness/shutdown timings.

Cold benchmarks must include cluster and actor startup, export, and shutdown.
Driver process-tree RSS alone is insufficient for external clusters. Owned local
benchmarks inventory services and actors explicitly and distinguish private
memory from shared store capacity. Missing observations remain null. Physical
store pages must not be counted once per mapping. Multiple-machine scaling needs
actual machines and per-node measurement agents, which have not been provided.

## Primary references

- [Ray installation](https://docs.ray.io/en/latest/ray-overview/installation.html)
- [Ray init API](https://docs.ray.io/en/latest/ray-core/api/doc/ray.init.html)
- [Actor and retry options](https://docs.ray.io/en/latest/ray-core/api/doc/ray.remote.html)
- [Object references](https://docs.ray.io/en/latest/ray-core/objects.html)

References were consulted on October 10, 2026. Implementation and verification
use the pinned release; documentation at `latest` can change.

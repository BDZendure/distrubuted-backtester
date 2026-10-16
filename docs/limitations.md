# System limitations and guarantees

This is a production-inspired engineering portfolio, validated on synthetic
workloads. The application is version `0.8.0`. The tables distinguish the
implemented contract from boundaries that need additional design or evidence.

## Simulation

| Area | Implemented contract | Limit |
| --- | --- | --- |
| Timing | Observe closes through session T; fill at the next supplied session's open; mark at its close | No intraday order book, exchange calendar or stale-bar interpolation |
| Accounting | Whole shares, long-only USD, integer micro-dollars, half-even fees and nonnegative cash | No borrowing, shorting, fractional shares or multi-currency portfolio |
| Execution | Deterministic next-open fills with configured basis-point fees | No market impact, volume participation, liquidity, spread or stochastic fills |
| Data | Strict increasing daily labels, coherent OHLCV and declared raw policy | No automatic corporate-action adjustment; caller establishes no splits/dividends over a raw-price interval |
| Strategy | Fully described SMA configuration, causal history and fresh per-job state | Arbitrary Python extensions have caller-owned state/provenance responsibilities |
| Metrics | Total return, maximum drawdown, equity and fill/fee summaries | No annualized Sharpe convention, exchange-session calendar or profitability evidence |
| Universe | Synthetic single-asset examples and identity-bound inputs | Incomplete real universes do not establish absence of survivorship bias |

Invalid, missing-field, duplicated, malformed and unsorted rows are rejected.
Missing dates are not invented; "next session" means the next supplied row.
There is no final forced liquidation. Pending final orders are canceled and
remaining holdings are marked at the final close.

## Execution and durability

| Area | Implemented contract | Limit |
| --- | --- | --- |
| Backends | Same semantic engine through sequential, spawn processes and Ray actors | Physical multi-machine scaling/partitions were not measured |
| Scheduling | Bounded submissions, stable jobs, attempts/deadlines, generation fencing and bounded retries | Retry budgets can expire before work starts; deadlines do not promise bounded whole-runtime shutdown |
| Commits | At-least-once calculation with idempotent fenced journal commits and conflicting duplicate rejection | No exactly-once calculation, replicated journal or coordinator high availability |
| Recovery | Resume a local POSIX journal with one coordinator; repair derived exports | No disk-loss recovery, Windows writer or network-filesystem safety claim |
| Status | Validated journal or complete classic outputs, logical read-only operation | SQLite may need directory write access for WAL/SHM; worker observations are historical, not liveness |
| Cleanup | Owned workers/services released on normal unwind; signals preserve primary interruption | SIGKILL bypasses cleanup; process pools may wait; native diagnostics can appear on stderr |
| Observations | Scoped opt-in JSON application events and explicit worker settings | Best-effort delivery is not commit acknowledgment; concurrent reconfiguration unsupported |

Process-crash tests and Docker stop/INT/KILL recovery establish the tested local
failure modes. They do not establish power-loss, network-partition or storage
hardware durability. A source change, even in an operational module, changes
job identity. Old-phase journals have no silent migration contract.

## Storage and measurement

The journal is authoritative; JSON exports and DuckDB are derived representations.
Results are duplicated in the journal and detail files. Data uses per-asset
Parquet directories with no multi-asset catalogue or fine date partitioning.
The cache rehashes current input bytes; a cache hit is not a complete experiment.
Atomic file publication relies on appropriate local hard-link/fsync/replacement
behavior and does not make network filesystems safe automatically.

Measured throughput is workload/revision/machine-specific. Historical results
include cold startup and full detail export. Few repetitions, uncontrolled OS
caches/host activity and consecutive revision order limit causal/statistical
claims. Private USS peaks were unavailable in Ray and Phase 6 comparisons;
Phase 2 RSS sums can count shared pages twice. CPU utilization, live plasma
occupancy, physical network transfer and steady-state cluster throughput remain
unmeasured. Speedup/worker count is efficiency, not CPU utilization.

## Deployment and maintenance

Python 3.14.6/macOS ARM64 full tests and Python 3.12.15/Linux ARM64 installed
runtime/dev checks were observed. Remote GitHub Actions, Linux AMD64 runtime,
the complete Linux dev-image suite, Docker Desktop bind-filesystem durability
and external-cluster routing remain unobserved. Hash locks/base metadata support
reproducibility but cannot establish execution success on an untested platform.
Pinned private Ray startup/reaper adapters need review when upgrading Ray.

Proposed changes for larger scale are discussed in the
[deep dive](engineering-deep-dive.md). They are future design options. See
[current verification](phase-8-report.md), [historical operational evidence](phase-7-report.md)
and [benchmark scope](benchmark-results.md).

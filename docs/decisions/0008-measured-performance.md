# ADR 0008: Profile first and preserve complete simulation semantics

Status: accepted for the approved Phase 6 design; optimization acceptance depends
on recorded measurements. Date: 2026-10-10.

## Context

Phase 5 showed that compressed files could halve storage without improving complete
experiment runtime. CPU/allocation costs inside the current Python pipeline need
measurement before changing representation or adding native infrastructure.
The first read-only CPU profile of four 65,536-bar simulations identified SMA
history indexing and exact drawdown calculations as useful candidates. Cumulative
profile entries overlap; their durations are not unprofiled throughput.

## Decision

Use cProfile and tracemalloc with explicit scope and reproducible synthetic input.
Retain CPU call/self/cumulative data and Python retained/peak allocation observations.
Measure uninstrumented candidate runs separately. Keep the existing event loop,
strategy interface, integer prices/fees, worker representation and detailed exports.

Evaluate small Python changes first: bounded history access and integer comparison
of drawdown ratios. Compare plausible alternatives, retaining only improvements
supported by timings and unchanged full simulation outputs. Native acceleration,
arrays and batching remain conditional on profiles, with no assumed introduction.

Reuse the existing owned local Ray benchmark for full-process comparisons, startup,
IPC, export/shutdown and documented memory coverage. Archive a baseline wheel and
a candidate wheel; run both with the same interpreter/dependencies and verified
source identities. Their job IDs may change because implementation is part of
identity. Validate both producers' manifests/artifacts before comparing all economic
fields in canonical configuration order. Only a validated engine release identifier
may be normalized; dataset/config/timing/accounting/strategy/output remain identical.

## Alternatives

- Redesign the engine around arrays now: increases semantic and interface risk
  before costs are known. Revisit if profiles justify it.
- Add C++ now: introduces build/ABI/deployment complexity without evidence that
  crossing the language boundary improves complete workloads.
- Measure only a kernel: useful for a focused change but insufficient for claims
  about complete simulations, worker startup, IPC, export or parallel scaling.

## Consequences and bounds

Profilers perturb execution; they identify candidates rather than establish
speedups. Tracemalloc covers traced Python allocations, not native/private/cluster
memory, and snapshots describe live allocation differences rather than all churn.
RSS/USS and unavailable coverage retain their existing measurement boundaries.
Use raw repetitions, medians and ranges; no linear-scaling, cold-disk or network
claim follows from one-machine synthetic workloads. Preserve unsuccessful findings
and reproduction commands. Stop after Phase 6 until Phase 7 is explicitly approved.

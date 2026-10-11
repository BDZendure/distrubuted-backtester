# Phase 2 benchmark record

Workloads are fixed after pilots: 32 unique jobs / 64 bars, and 64 unique jobs /
4,096 bars. Both use seed 1729, raw USD synthetic data, $1,000 initial cash,
10 bps fees, and `bounded-sma-windows-cash-batches-v1` configurations.

The final comparison is pending five timed repetitions after one warm-up for
each of sequential, process-2, process-4, and process-8. Trials start fresh
processes and rotate order. Startup, loading, serialization, detailed export,
finalization, and shutdown are included. All semantic artifacts are verified.

Raw process-tree RSS is sampled at 50 ms with psutil 7.2.2. This is an approximate
RSS sum, not unique physical memory. Short peaks/processes may be missed; shared
pages may be counted repeatedly. The observer is excluded. OS cache state is
uncontrolled. See [methodology](../../benchmarking.md).

Final tables, raw observations, environment, reproduction commands, and limitations
will be added after measurement. Pilot samples will remain labeled separately.

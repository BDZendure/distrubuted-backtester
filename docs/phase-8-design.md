# Phase 8 portfolio documentation design

## Authorization and scope

The user approved Phase 8 after the verified Phase 7 checkpoint `215cd0a`.
Follow the approved roadmap's final documentation scope on
`feat/phase-8-portfolio`, using the established in-place feature-branch workflow.
The delivered application remains version `0.8.0`. Publication requires separate
explicit authorization under the project's Git workflow.

## Reader journey

Use a short README for a new reader: purpose, two runnable examples, execution
timing, a diagram, selected measured evidence, limits and links. Move detailed
command usage into `docs/cli-reference.md`. Keep the beginner explanation and
technical reference available, with a navigation note pointing to the new guides.

Alternatives considered: one comprehensive README makes the first read long;
a separate documentation site adds a second publishing surface. Linked Markdown
keeps the public repository readable and its evidence in the same review.

## Deliverables and boundaries

| Document | Responsibility |
| --- | --- |
| `README.md` | Project overview, installed example, process sweep, diagram and evidence summary |
| `docs/cli-reference.md` | Detailed command usage moved from the existing README |
| `docs/engineering-deep-dive.md` | Architecture, determinism, scheduling, failures, duplicates, profiling, optimization and larger-scale changes |
| `docs/benchmark-results.md` | Version-scoped benchmark/scaling synthesis linked to retained raw data |
| `docs/fault-tolerance-demo.md` | Reproducible real failure tests, expected invariants and observed recovery scope |
| `docs/reproducibility.md` | Fresh core/all-extras environments, complete checks, installed wheel smoke and historical measurements |
| `docs/limitations.md` | Simulation, execution, storage, measurement and deployment limits |
| `docs/portfolio-summary.md` | Interview summary, discussion questions and three evidence-linked resume candidates |
| `docs/phase-8-report.md` | Required completion report with actual commands and outcomes |
| `docs/phase-8-verification.json` | Compact claim provenance and current verification observations |

The existing architecture document remains the detailed technical design.
The README diagram describes calls/data flow in the installed package, not
independent services. Existing ADRs document decisions. Phase reports and raw
benchmark artifacts remain historical evidence and retain their recorded scope.

## Claim rules

- Trace economic/scheduling claims to implementation and tests.
- Trace numerical performance claims to retained raw observations and their
  measured revision, workload, machine, repetitions and timer boundary.
- Distinguish optimization speedup from backend speedup and parallel efficiency.
- State unavailable USS/CPU/network measurements explicitly. Never substitute
  RSS sums for unique memory or task reservation for measured utilization.
- Explain slower cold Ray, mixed parallel results and unhelpful optimizations.
- Keep Phase 6 measurements scoped to archived `0.7.0` wheels; documentation
  changes do not establish a new `0.8.0` performance measurement.
- Describe at-least-once calculation and idempotent fenced commits. Process
  termination is not proof of power-loss, network-partition or disk-loss safety.
- Resume bullets describe verified engineering, with workload-qualified numbers.
  Do not claim profitable trading, production readiness or measured multi-machine
  throughput.

## Validation

Freeze package/helper bytes while running native workers. Run the existing full
suite and static/dependency checks. Follow the documented examples from a fresh
installed wheel outside the checkout, using real process/Ray/storage execution
through the existing smoke helper. Reconcile selected benchmark claims with raw
JSON, check local Markdown links/anchors and shell syntax, build wheel/source
archive, inspect inclusion and obtain independent evidence/quality review.
Do not add tests or runtime dependencies for editorial work.

## Self-review

The design covers every approved Phase 8 deliverable, defines separate document
responsibilities, preserves current execution contracts and names the evidence
boundary. All required verification is concrete; unavailable remote CI and Linux
AMD64 execution remain disclosed. Proceed under the user's Phase 8 approval.

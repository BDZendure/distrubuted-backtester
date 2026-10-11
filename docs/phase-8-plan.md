# Phase 8 portfolio implementation plan

> For agentic workers: use subagent-driven-development for independent documents
> and review each deliverable. This is the execution ledger.

**Goal:** Make the verified backtester understandable, reproducible and defensible
in quantitative software engineering interviews.

**Architecture:** A concise README links focused Markdown guides, the existing
technical design and retained source evidence. Application architecture is the
implemented version `0.8.0` described in [the design](phase-8-design.md).

**Stack:** Markdown/Mermaid, JSON evidence, existing Python/pytest/Ruff/mypy/build,
installed core/optional wheel smoke and the existing Git feature workflow.

## Global constraints

- Phase 8 authorized after `215cd0a`; publication/push requires separate approval.
- Use `feat/phase-8-portfolio` and preserve every existing user change.
- No runtime, dependency, version, economic, scheduler or helper behavior changes.
- Freeze package/helper source during all worker execution.
- No new performance claims; historical numbers retain revision/workload scope.
- No unsupported exactly-once, production, profitability or multi-machine claims.
- Preserve historical phase reports/raw benchmark artifacts and measured identities.
- No em dash, attribution footer or unnecessary frontend/service.

## Task 1: Establish scope and baseline

- [x] Inspect the clean Phase 7 checkpoint, approved roadmap and evidence.
- [x] Select linked Markdown over a monolithic README or separate docs site.
- [x] Create the Phase 8 branch and self-review the approved-scope design.
- [x] Run baseline `PHASE5_STORAGE_CHILD_TESTS=1 .venv/bin/python -m pytest -q`;
  record observed results before making completion claims.
- [x] Commit this design/plan as a focused local planning checkpoint (`3f8cd97`).

Baseline: 1,928 passed in 226.08 seconds, exit 0. Package/helper bytes stayed
frozen; only documentation was being edited. This duration is a verification
observation, not a performance benchmark.

## Task 2: Technical narrative and failure demonstration

Create `docs/engineering-deep-dive.md` and `docs/fault-tolerance-demo.md`.
Read source contracts in `simulation.py`, `jobs.py`, `backends/`, `scheduler/`,
`provenance.py`, `performance.py` and the existing architecture/reliability guides.

- [x] Explain all eight requested deep-dive topics with concrete module/test links.
- [x] Include chronological signal/fill timing, exact accounting, bounded work,
  identity, generation fencing, result uniqueness and commit acknowledgment loss.
- [x] Provide commands for the existing real Ray/SQLite failure tests; identify
  injected behavior, supported POSIX runtime and invariants checked by each test.
- [x] Explain code-based larger-scale proposals separately from observed behavior.
- [x] Obtain independent spec/evidence/quality review; resolve actual findings.

Task 2 independent review accepted both documents without P1/P2 findings.
The review traced source semantics, existing test assertions/selectors, retained
measurements and the limits of the demonstrated fault contract.

## Task 3: Benchmarks and interview material

Create `docs/benchmark-results.md` and `docs/portfolio-summary.md`.
Read benchmark results/raw JSON for Phases 2/3/5/6, profiling reports, Phase 4
failure evidence and Phase 7 installed-container evidence.

- [x] Produce version-scoped tables; distinguish timer boundaries and workloads.
- [x] Explain startup/export costs, non-linear scaling, unsuccessful approaches,
  unavailable private-memory data and limitations of few samples.
- [x] Trace every highlighted number to raw observations; preserve regressions.
- [x] Write short/extended interview explanations and three resume candidates.
- [x] Link each candidate to implementation/tests and any numerical evidence.
- [x] Obtain independent numerical/scope/quality review and resolve findings.

Task 3 independent review accepted without P1/P2 findings: 44 table rows,
24 profiles, three resume candidates and all highlighted numbers match retained
raw observations and primary implementation/test support at displayed precision.

## Task 4: Public entry point and reproduction

Modify `README.md`; create `docs/cli-reference.md`, `docs/reproducibility.md`,
`docs/limitations.md`; synchronize current-stage navigation in architecture,
roadmap and beginner/technical guides. Modify `MANIFEST.in` to ship Phase 8 JSON.

- [x] Move detailed README commands into the CLI reference with corrected links.
- [x] Write a concise README, explicit daily timing, implemented architecture
  diagram, core example, four-job process sweep and evidence/limitations links.
- [x] Provide fresh core/full-check/wheel commands and named-output guidance.
- [x] Centralize limits and distinguish actual local verification from configured CI.
- [x] Follow those commands with fresh installed wheel environments and the
  existing `scripts/smoke.py --ray --storage --signals` helper.
- [x] Preserve source identity and existing historical evidence sections.

Public-entry review accepted without P1/P2 findings. Exact README commands also
passed from a clean unpacked source archive. Fresh build verification replaced
the nonisolated build instruction with default isolation after reproducing the
missing-backend failure. Final installed core and optional wheel checks passed.

## Task 5: Final review and delivery

- [x] Run `.venv/bin/python -m ruff check .`, `ruff format --check .`, `mypy`
  and `pip check`, plus the complete frozen-source test suite if needed after
  relevant changes. Record actual commands and results.
- [x] Check Markdown paths/anchors, Bash syntax and selected raw evidence claims.
- [x] Build with default isolated `python -m build`, inspect wheel/source
  archive bytes/inclusion and verify fresh installed execution.
- [x] Obtain a whole-phase independent consistency/claim review and resolve findings.
- [x] Publish `docs/phase-8-report.md` and `docs/phase-8-verification.json` locally.
- [x] Make focused commits, refresh the final source archive and confirm a clean branch.
- [x] Deliver the nine-section report and request separate authorization for any
  remote push/publication or additional work.

Whole-phase review accepted the observed evidence, preservation checks and
cross-document claims without P1/P2 findings. Final completion/navigation wording
and the refreshed 238-file source archive are main-owned followup checks.
Final wheel metadata and all 47 package files remain unchanged from the installed
checks. Phase 8 adds no new performance measurement. Publication remains pending
explicit user authorization.

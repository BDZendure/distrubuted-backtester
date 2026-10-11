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
- [ ] Run baseline `PHASE5_STORAGE_CHILD_TESTS=1 .venv/bin/python -m pytest -q`;
  record observed results before making completion claims.
- [ ] Commit this design/plan as a focused local planning checkpoint.

## Task 2: Technical narrative and failure demonstration

Create `docs/engineering-deep-dive.md` and `docs/fault-tolerance-demo.md`.
Read source contracts in `simulation.py`, `jobs.py`, `backends/`, `scheduler/`,
`provenance.py`, `performance.py` and the existing architecture/reliability guides.

- [ ] Explain all eight requested deep-dive topics with concrete module/test links.
- [ ] Include chronological signal/fill timing, exact accounting, bounded work,
  identity, generation fencing, result uniqueness and commit acknowledgment loss.
- [ ] Provide commands for the existing real Ray/SQLite failure tests; identify
  injected behavior, supported POSIX runtime and invariants checked by each test.
- [ ] Explain code-based larger-scale proposals separately from observed behavior.
- [ ] Obtain independent spec/evidence/quality review; resolve actual findings.

## Task 3: Benchmarks and interview material

Create `docs/benchmark-results.md` and `docs/portfolio-summary.md`.
Read benchmark results/raw JSON for Phases 2/3/5/6, profiling reports, Phase 4
failure evidence and Phase 7 installed-container evidence.

- [ ] Produce version-scoped tables; distinguish timer boundaries and workloads.
- [ ] Explain startup/export costs, non-linear scaling, unsuccessful approaches,
  unavailable private-memory data and limitations of few samples.
- [ ] Trace every highlighted number to raw observations; preserve regressions.
- [ ] Write short/extended interview explanations and three resume candidates.
- [ ] Link each candidate to implementation/tests and any numerical evidence.
- [ ] Obtain independent numerical/scope/quality review and resolve findings.

## Task 4: Public entry point and reproduction

Modify `README.md`; create `docs/cli-reference.md`, `docs/reproducibility.md`,
`docs/limitations.md`; synchronize current-stage navigation in architecture,
roadmap and beginner/technical guides. Modify `MANIFEST.in` to ship Phase 8 JSON.

- [ ] Move detailed README commands into the CLI reference with corrected links.
- [ ] Write a concise README, explicit daily timing, implemented architecture
  diagram, core example, four-job process sweep and evidence/limitations links.
- [ ] Provide fresh core/full-check/wheel commands and named-output guidance.
- [ ] Centralize limits and distinguish actual local verification from configured CI.
- [ ] Follow those commands with fresh installed wheel environments and the
  existing `scripts/smoke.py --ray --storage --signals` helper.
- [ ] Preserve source identity and existing historical evidence sections.

## Task 5: Final review and delivery

- [ ] Run `.venv/bin/python -m ruff check .`, `ruff format --check .`, `mypy`
  and `pip check`, plus the complete frozen-source test suite if needed after
  relevant changes. Record actual commands and results.
- [ ] Check Markdown paths/anchors, Bash syntax and selected raw evidence claims.
- [ ] Build with `.venv/bin/python -m build --no-isolation`, inspect wheel/source
  archive bytes/inclusion and verify fresh installed execution.
- [ ] Obtain a whole-phase independent consistency/claim review and resolve findings.
- [ ] Publish `docs/phase-8-report.md` and `docs/phase-8-verification.json` locally.
- [ ] Make focused commits, refresh the final source archive and confirm a clean branch.
- [ ] Deliver the nine-section report and request separate authorization for any
  remote push/publication or additional work.

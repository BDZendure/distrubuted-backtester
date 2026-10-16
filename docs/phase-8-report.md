# Phase 8 completion report

Phase 8 is complete on `feat/phase-8-portfolio`, application version `0.8.0`.
Package/helper behavior and source identity remain the verified Phase 7
implementation. Publication requires separate authorization.

## Implemented

A concise README, implemented architecture diagram, detailed CLI reference,
engineering deep dive, benchmark/scaling synthesis, fault demonstration,
reproducibility and limitations guides, interview summary and three resume
candidates. Beginner and technical guides retain their detailed explanations
and link the new reading routes.

Important files: [README](../README.md), [deep dive](engineering-deep-dive.md),
[benchmark analysis](benchmark-results.md), [fault demonstration](fault-tolerance-demo.md),
[reproduction](reproducibility.md), [limitations](limitations.md),
[CLI reference](cli-reference.md), and [interview/resume material](portfolio-summary.md).

## Architecture

The README introduces a modular package and links the existing detailed design
and ADRs. The deep dive explains causal simulation, exact accounting, immutable
jobs, bounded backends, durable attempts/fencing and unique commits. The journal
is authoritative; JSON/DuckDB are derived. Proposed larger-scale changes are
separated from implemented/observed behavior.

The application remains `distributed-backtester/0.8.0:daily-long-only-v1`, source
SHA `a3edda1731d8694fda5d6987e570e0cca365bbee22d18bbfc0dbd116003f618f`.
Publication requires the project's explicit Git authorization.

## Verification

The [verification record](phase-8-verification.json) records observed commands,
installed reports, numerical claim sources and packaging evidence.

| Command/check | Actual outcome |
| --- | --- |
| `PHASE5_STORAGE_CHILD_TESTS=1 .venv/bin/python -m pytest -q` | 1,928 passed in 226.08 seconds; exit 0 on frozen source. |
| `.venv/bin/python -m ruff check .` | All checks passed. |
| `.venv/bin/python -m ruff format --check .` | 157 files already formatted at the final static check. |
| `.venv/bin/python -m mypy` | No issues in 102 source files. |
| `.venv/bin/python -m pip check` | No broken requirements found. |
| Final core-wheel helper outside checkout, `--timeout 180 --signals` | Exit 0, `ok=true`, 46 recorded Python files, reference/process/status/log/signal checks. |
| Final optional-wheel helper outside checkout, `--timeout 180 --ray --storage --signals` | Exit 0, `ok=true`; real Ray, durable recovery, Parquet and DuckDB, same semantic digest. |
| Fresh development environment `python -I -m build` | Default isolated build succeeded using the pinned backend; wheel and source archive built. |
| Wheel and source archive inspection | All 47 package files match current source; wheel README metadata matches; all 238 required archive files match after refresh. Core has zero runtime dependencies. |
| README commands from a clean unpacked source archive | Editable installation, example, result export, two-worker sweep and status all exit 0. Two reference fills; four successful jobs, zero failures. |
| Markdown links/anchors and Bash syntax | 55 files, 589 local links, 16 anchor links and 102 shell blocks, zero errors after navigation refresh. |

Commands actually used absolute interpreter/script/data paths. They are retained
in the JSON record. Both smoke reports share semantic digest
`b3d189adfc79df1c9567cbf09f01b2b9ec96d236c4d83a65d0962fa471584006`.
Native full tests include the real failure cases described in the demonstration;
the current full-suite result establishes their execution, without presenting
the reproduced historical Phase 7 Docker drills as a new container run.

The final core environment was created fresh. The optional environment was
created fresh for the candidate wheel, then reinstalled offline with the final
wheel before repeating all optional checks. All 102 tracked Python files,
`pyproject.toml` and the typing marker remain byte-identical to `215cd0a`.
Phase 6 sections of the beginner and technical guides remain byte-identical.
The source archive was refreshed and all 238 required files compared with the
delivery tree. The README and all implementation bytes used by the clean-source
walkthrough stayed unchanged through the documentation/evidence refresh.

Independent technical, public-entry, numerical and whole-phase consistency
reviews accepted their scopes without P1/P2 findings. The numerical review
recomputed all 44 table rows and inspected 24 profiles and three resume candidates.
Reviewers inspected primary source, tests and retained observations; they did not
rerun native commands. Final archive refresh and completion wording are main-owned.

Fresh development verification exposed a documentation dependency: `build
--no-isolation` failed when the declared Setuptools build backend was absent
from the environment. The fresh README/reproduction commands now use `build`
with its default isolation and the existing pinned build-system requirement.
The corrected command succeeded in that same fresh environment. This changes
instructions, not runtime dependencies or package behavior.

The first clean-source temporary checker used `max_drawdown` instead of the
existing `maximum_drawdown` output field. Its reference CLI command succeeded;
the private assertion failed. Correcting that checker and repeating the whole
walkthrough in another fresh tree passed. No application change was needed.

## Performance

No new benchmark or optimization is claimed. The synthesis recomputes historical
numbers from retained observations and keeps release/workload/timer boundaries.
Archived Phase 6 sequential medians for 32 jobs x 16,384 bars were 3.821517 and
2.667269 seconds: 30.20% less runtime, 1.433x optimization speedup. Parallel
changes were mixed and cold Ray samples regressed. Test/smoke elapsed times are
verification observations. The three resume candidates retain these qualifiers.

## Known Limitations

See [limitations](limitations.md). The engine is daily, single-asset and long-only
without realistic liquidity/impact/corporate-action handling. Durable execution
uses one local POSIX journal and one writer. It does not establish exactly-once
calculation, high availability, power-loss or multi-machine performance.
USS/CPU/network measurement gaps remain disclosed. Remote CI and Linux AMD64
remain unobserved; Phase 7 Linux ARM64 image evidence is historical. No remote
push or public release has been performed.

## Reproduction Instructions

Follow [reproducibility](reproducibility.md) for clean core, development and
installed-wheel environments. The README's synthetic example expects two fills,
equity `749.257000`, fees `1.743000`; its process sweep expects four successes.
[Failure demonstration](fault-tolerance-demo.md) lists existing real injected
worker/journal cases. [Benchmark synthesis](benchmark-results.md) links recorded
identities and historical measurement procedures. Containers use the existing
[development](development.md) and [operations](operations.md) instructions.

## Engineering Lessons

1. A technical claim should name its invariant and the evidence that checks it.
2. Backend speedup, optimization speedup and timer scope answer different questions.
3. A repeated calculation can coexist with one idempotent committed result.
4. Fresh environments detect assumptions hidden by installed tooling and caches.
5. Missing measurements and unsuccessful approaches are part of the engineering record.

## Next Phase

Phase 8 is the final planned phase. Any additional engineering should follow a
new scoped objective with measured need. Publication is a separate action.

## Approval Required

The project's Git workflow prohibits remote pushes without explicit approval.
Do you want to push `feat/phase-8-portfolio` to the configured GitHub `origin`?
The local work is reviewable; this report does not authorize a push or merge.

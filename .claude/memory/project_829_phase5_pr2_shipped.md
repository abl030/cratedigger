---
name: project-829-phase5-pr2-shipped
description: "Issue #829 Phase 5: PR1-PR2d shipped and deployed 2026-07-29; PR3 next with a revised single-window design; 20 borrowed corpus rows seeded in prod"
metadata:
  type: project
---

2026-07-29. The codec-blind spectral defect is **fixed and live**. PRs #904 (PR1,
migration 065), #922 (PR2a module), #927 (PR2b decider seam — deployed and
cycle-verified), #931 (PR2c parity closure), #934 (PR2d Stage-2 counterfactual),
#929 (calibration record), #935 (plan realignment).

**Pick-up point for PR3:** `docs/plans/2026-07-27-001-feat-829-phase5-implementation-plan.md`
— read §1.5 first, it corrects five falsified claims and three change PR3's design.
Realignment comment: issue #829 comment 5116926250.

**Seeded ground-truth rows to remove later: request ids 8916-8935** (20 corpus
albums, `target_format=lossless` so FLAC is retained). Borrowed for the Apple
launder measurement, NOT curation. Full list + census query in plan §1.6. Remove
via `pipeline-cli library-delete` / `pipeline-delete`, never raw SQL, and only
after the measurement.

**Calibration instance is torn down** — DB dropped, encodes and corpus FLACs
deleted. Deleting the corpus was a mistake (my recommendation): the manifest
makes it reacquirable but not free, and it forecloses closing the Apple gap.
The measurements survive in `docs/research/calibration-data/` (2.4 MB gzipped,
60,102 rows across four arms) — that directory is now the ONLY evidence for
every per-codec constant in production.

**Key corrections worth not re-deriving:** un-backfillable proof cohort is ~40%
not 93% (lossless *lineage* ≠ vanished source — target=flac keeps the file);
production measures ONE spectral window so the multi-window union was never
implementable, and window 0 dominates anyway; `apple-256→FLAC` was never gate-
tested (exists only in `probe_pair.tsv.gz`); SBR gate is not a blocker (zero
HE-AAC on the box, and the laundered case has no object type left to read).

Related: [[project-829-spectral-calibration]], [[feedback-worktree-switch-kills-live-subagent]].

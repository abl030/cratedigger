---
name: project-812-spectral-tie-fix
description: "PR #812 fixed Stage-1 spectral tie-reject that pre-empted Stage 2; structural debt in issue #813"
metadata: 
  node_type: memory
  type: project
  originSessionId: 08a16c9b-6bf7-4550-8c7f-437bb8decbe9
  modified: 2026-07-21T10:18:32.224Z
---

2026-07-21 (v after PR #812, nixosconfig d4e2fba9): the importer's quality
decision runs in ordered stages, and **Stage 1 `spectral_import_decision`
(`lib/quality/decisions.py`) can DISAGREE with Stage 2 `compare_quality`
(`lib/quality/compare.py`) and short-circuits before Stage 2 runs.** Live bug:
Mark DeNardo "Lion, Tiger, Bear" (request 1308) — candidate MP3 192 CBR /
suspect / spectral 128 was rejected against on-disk MP3 128 CBR /
likely_transcode / spectral 128 because Stage 1 rejected on an EQUAL spectral
estimate (`new_q <= existing_q`) even though Stage 2 returns "better" (192 vs
128, metric_tiebreak). Fix: `<=` → `<` — a spectral tie defers to Stage 2's
raw-metric tiebreak; only strictly-worse spectral rejects at Stage 1.

Why Stage 1 can't just be deleted: it protects a Stage 2 blind spot —
`_transcode_candidate_real_rank_regresses` only guards a transcode candidate
over a NON-transcode existing album, so transcode-over-transcode strictly-worse
is caught only by Stage 1. This is the **2nd instance** of the
"narrow spectral check upstream of the single decider overrides it"
anti-pattern (1st was PR #257; U11 folded `preimport_decide` for the same
reason). See [[feedback-quality-decisions-one-place]] if written.

Also surfaced: the decision dict's `denylisted` bool diverges from the real
denylist write (driven by `dispatch_action(decision_string)`, not the bool) —
they disagree for `downgrade` (dict False, production True). The dict field is
read only by `pipeline-cli quality` display + tests, so the simulator lies
about denylist for downgrades.

Both structural findings tracked in **GitHub issue #813** (Finding 1:
close the `_transcode_candidate_real_rank_regresses` asymmetry + Stage-1/Stage-2
parity audit; Finding 2: single-source the `denylisted` field). Do NOT
re-investigate the tie bug itself — it's fixed and pinned (Mark DeNardo pin +
evidence twin + generated property `test_only_strictly_lower_spectral_rejects_at_stage1`).

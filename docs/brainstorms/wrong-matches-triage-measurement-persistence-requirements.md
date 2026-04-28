---
date: 2026-04-28
topic: wrong-matches-triage-measurement-persistence
---

# Wrong Matches Triage Measurement Persistence

## Problem Frame

The just-shipped `wrong-matches-spectral-evidence` feature (PR #181, plan
`docs/plans/2026-04-28-001-feat-wrong-matches-spectral-evidence-plan.md`)
extends the Wrong Matches tab to display per-candidate spectral grade/floor
and lossless-source V0 probe average. The display reads from the flat
`download_log.spectral_grade`, `spectral_bitrate`, `v0_probe_kind`,
`v0_probe_avg_bitrate` columns, which are populated only when an actual
import attempt logs its measurement.

Live data on doc2 shows the feature renders `— —` on every triaged candidate
row. The cause: `lib/wrong_match_triage.py::_triage_audit_payload`
discards the entire `ImportResult.new_measurement` and
`ImportResult.v0_probe` when persisting the triage outcome. The
auto-triage hook in `lib/download.py::_run_post_rejection_wrong_match_triage`
runs preview-with-spectral on every new wrong-match row (and the
`backfill_wrong_match_previews` CLI runs it on visible legacy rows), so
the measurement was *taken* — it's just thrown away before it reaches the
DB.

The operator confirmed this on a live row (#8047): `wrong_match_triage`
JSONB carries `verdict: would_import`, `decision: import`,
`gate: accept` — the preview ran, returned a populated `ImportResult`
including `new_measurement`, but only the verdict/stage strings survived.

## Requirements

**Triage persistence**

- R1. Auto-triage (`lib/download.py::_run_post_rejection_wrong_match_triage`)
  must persist enough measurement data from
  `ImportPreviewResult.import_result` for the Wrong Matches tab to display
  spectral grade, spectral bitrate floor, V0 probe kind, and V0 probe
  average per candidate without re-running analysis.
- R2. The `backfill_wrong_match_previews` CLI must persist the same
  measurement data on every row it processes, so a single
  operator-initiated run fills in the legacy population.
- R3. Persistence must remain a side effect of triage. No new endpoint, no
  new per-row "analyze" button, no new preview lifecycle in scope —
  consistent with R3 of the upstream `wrong-matches-spectral-evidence`
  brainstorm.

**Display continuity**

- R4. The four candidate-evidence keys already pinned by the Wrong Matches
  contract (`spectral_grade`, `spectral_bitrate`, `v0_probe_kind`,
  `v0_probe_avg_bitrate`) must populate for any wrong-match row whose
  triage produced an `ImportResult` with a measurement, after this work
  ships.
- R5. Rows whose preview legitimately produced no measurement
  (`nested_layout` and similar early-reject paths that exit before
  `run_import_one`) continue to render as a dash — missing data, not a
  trigger to run analysis.

**Backfill scope**

- R6. Backfill targets only currently-visible Wrong Matches rows whose
  on-disk folder still exists (i.e., the rows the operator can actually
  see in the tab). Stale-folder rows and out-of-scope scenarios
  (`audio_corrupt`, `spectral_reject`) are skipped, matching today's
  `backfill_wrong_match_previews` filter behaviour.
- R7. Backfill is operator-initiated via the existing CLI, not
  auto-triggered on deploy. CPU cost is bounded by the visible-with-folder
  population (live: ~21 releases / ~83 candidates / ~15-40 minutes of
  spectral work) so single-shot execution is fine; no batching/rate-limit
  required in scope.

## Acceptance Examples

- AE1. **Covers R1, R4.** Given a fresh wrong-match candidate is rejected
  for `high_distance` and auto-triage runs `preview_import_from_download_log`,
  when the resulting `ImportResult` carries a populated `new_measurement`,
  the wrong-match tab row for that candidate shows the persisted spectral
  grade and floor (and lossless-source V0 probe avg when present) on the
  next page load — no re-triage, no new analysis.
- AE2. **Covers R2, R6, R7.** Given the live system with ~21 visible
  Wrong Matches releases / ~83 candidates whose folders are still on disk,
  when the operator runs `backfill_wrong_match_previews` once, every
  candidate whose preview produces a measurement has its spectral and V0
  probe evidence persisted, and the tab populates after the run.
- AE3. **Covers R5.** Given a wrong-match candidate rejected by triage at
  the `nested_layout` preimport gate (no spectral run), when the row
  renders, the spectral and V0 cells stay as a dash — the row exposes no
  preview button or fallback "analyze now" action as part of this feature.
- AE4. **Covers R3.** Given the operator opens Wrong Matches between
  triage runs, no per-row "analyze" / "preview" / "re-triage" action is
  exposed; the only operator-driven trigger remains the existing CLI.

## Success Criteria

- The four candidate-evidence cells the prior feature added populate for
  the bulk of live Wrong Matches rows (where preview reached
  `run_import_one`) without changing the read-side contract or the
  operator workflow.
- The original brainstorm's R1+R2 ("Wrong Matches rows must surface
  stored spectral evidence ... when available") become operationally true
  rather than vacuous.
- Planning can proceed without inventing new product behaviour, audit
  semantics, or backfill ergonomics.

## Scope Boundaries

- Does not change beets distance thresholds, spectral thresholds, V0
  probe policy, or quality-gate decisions.
- Does not add a new web endpoint, new background worker, or new preview
  workflow.
- Does not add a per-row "analyze" / "preview" button to Wrong Matches —
  evidence is a side effect of triage that already runs.
- Does not change the `Delete Lossless Opus` safety gate (R4-R6 of the
  prior feature) — that reads on-disk `current_spectral_grade` from
  `album_requests` and is independent of per-candidate triage state.
- Does not extend persistence beyond Wrong Matches (e.g.,
  `audio_corrupt` / `spectral_reject` rows that have their own buckets
  and never appear in this tab).
- Does not change the existing `wrong_match_triage` JSONB shape's
  semantics for already-persisted `verdict` / `decision` / `stage_chain`
  fields — only adds measurement data alongside.

## Key Decisions

- **Persist the measurement**, not just the verdict. The existing JSONB
  carries `verdict: would_import` etc., which would have been the cheaper
  ship — but the original brainstorm explicitly requested numeric
  spectral grade/floor and V0 probe avg display, and the operator confirmed
  that's still the desired shape.
- **Triage is the persistence trigger.** Rather than adding a new
  measurement persistence path, extend the existing audit-write inside
  triage (and inside backfill, which already calls the same audit
  function). Both auto-triage and backfill benefit from a single change.
- **Backfill scope = visible-with-folder rows.** `backfill_wrong_match_previews`
  already iterates `get_wrong_matches()` and skips rows whose folder is
  missing — that's the right denominator. No need to rebuild a separate
  scan.
- **Operator-initiated backfill, not auto-on-deploy.** The visible
  population is small enough (~83 candidates) that a single CLI run is
  fine; baking it into deploy adds risk for no benefit.

## Dependencies / Assumptions

- `lib/wrong_match_triage.py::triage_wrong_match` continues to be the
  ownership boundary for "preview a wrong-match row and persist what
  happened" — both `_run_post_rejection_wrong_match_triage` and
  `backfill_wrong_match_previews` route through it.
- `ImportPreviewResult.import_result` is the carrier for measurement data
  on `would_import` and `confident_reject` (post-`run_import_one`)
  outcomes. Verified by reading
  `lib/import_preview.py::preview_import_from_path`.
- Early-reject preview paths (`nested_layout`, `path_missing`) leave
  `import_result=None`. These rows legitimately have nothing to persist
  and are covered by R5 (display as dash).
- Live measurement of spectral analysis cost on doc2: ~10-30s per
  candidate folder. Backfill scope of ~83 candidates implies ~15-40 min
  total CPU — acceptable for single-shot CLI execution. Assumption to
  verify during planning if it changes materially.
- The four flat `download_log` columns (`spectral_grade`,
  `spectral_bitrate`, `v0_probe_kind`, `v0_probe_avg_bitrate`) and the
  Wrong Matches `get_wrong_matches` SELECT that surfaces them already
  exist (shipped in PR #181).

## Outstanding Questions

### Resolve Before Planning

- None.

### Deferred to Planning

- [Affects R1, R2][Technical] Decide whether the persistence write goes
  to the existing flat `download_log` columns (matching the SELECT
  shipped in PR #181, requires `record_wrong_match_triage` or a sibling
  function to UPDATE flat columns from the `ImportResult`) or to JSONB
  inside the `wrong_match_triage` blob (requires changing the SELECT to
  read from JSONB). Either achieves R4 — the trade-off is read-side
  simplicity vs. write-side simplicity.
- [Affects R1][Technical] Decide whether early-reject preview paths
  (`spectral_reject` / `audio_corrupt` already excluded; `nested_layout`
  excluded by triage early-return) need any specific persistence
  treatment, or whether R5 ("dash for no measurement") fully covers them.
  Live data should confirm whether the population of these rows in
  visible Wrong Matches is non-zero.
- [Affects R2][Technical] Decide whether the existing
  `backfill_wrong_match_previews` CLI surface needs to grow new flags
  (e.g., `--measurement-only` to skip non-measurement audit updates), or
  whether the existing `--cleanup`/`--dry-run`/no-flag modes are
  sufficient when the audit payload itself is extended.
- [Affects R4][Technical] Decide whether the `FakePipelineDB` test fake
  needs to mirror the new persistence path, and what test scenarios pin
  the round-trip from `ImportResult.new_measurement` → audit write →
  `get_wrong_matches` row → contract test ENTRY field.

## Next Steps

-> /ce-plan for structured implementation planning.

# Rejection routing — the events table

Every scenario production can write to `download_log.validation_result->>
'scenario'` (or, via the force-import manifest guard, directly compose the
audit row), what happens to the peer, what happens to the folder on disk,
and whether the operator ever sees it. This is the settled routing after
issue #1077 (D1–D10, decisions comment:
<https://github.com/abl030/cratedigger/issues/1077#issuecomment-5264029316>).
Current-state source material for the original sweep: the third correction
comment on the same issue.

## The two questions, and the two predicates that answer them

`lib/wrong_match_policy.py` keeps two separate predicates rather than one:

- **Worklist visibility** — `rejection_scenario_is_wrong_match_candidate`.
  Does a quarantined folder belong in the operator's Wrong Matches review
  surface? A small exclusion set: everything is visible except the five
  folder/audio-integrity facts and the quality-only spectral reject — none
  of which quarantine with a reviewable folder any more (D3), so this
  predicate mostly protects the historical cohort quarantined before that
  fix.
- **Cleanup-lane admission** — `rejection_scenario_is_delete_eligible`. May a
  kept, banned, visible folder be evaluated for deletion by
  `lib.wrong_match_cleanup_service.cleanup_wrong_match` (the reducer, D2/D9,
  untouched by this issue)? An explicit four-item allowlist, not a fail-open
  exclusion set (D6). Everything else — world failures with a reviewable
  folder, every unknown/novel scenario string, and `None` — is never
  delete-eligible; the reducer is never even consulted for them.

Deletion for a **kept, banned, visible** folder therefore only ever happens
through two routes, neither of which is "entering the lane": the cleanup
reducer's own redundancy proof (`confident_reject + cleanup_eligible`, or the
verified-lossless-parent short circuit — D2/D9), or force-import success
consuming its own source (D7). `audio_corrupt` is a third, distinct route —
ban + delete at reject time, never quarantined at all, so it never becomes
"kept" in the first place (D3).

`tests/test_wrong_match_scenario_producer_audit.py` derives the classified
scenario set by introspection from the producer files below and fails closed
on anything spelled but unclassified.

## Events table

| scenario | producer | denylist | artifact destination | worklist visible | cleanup-lane admission | requeue |
|---|---|---|---|---|---|---|
| `extra_tracks` | `lib/beets.py:175` (`choose_match` handler) | yes | `wrong_matches/` (curated move — untracked audio outside the download manifest stays out) | yes | **yes** (allowlist) | yes → `wanted` |
| `high_distance` | `lib/beets.py:183` | yes | `wrong_matches/` (curated) | yes | **yes** (allowlist) | yes → `wanted` |
| `mbid_not_found` | `lib/beets.py:325` | yes | `wrong_matches/` (curated) | yes | **yes** (allowlist) | yes → `wanted` |
| `no_choose_match` | `lib/beets.py:124` (`NO_CHOOSE_MATCH_SCENARIO`) | yes | `wrong_matches/` (curated) | yes | **yes** (allowlist) | yes → `wanted` |
| `strong_match` | `lib/beets.py:179` | n/a | n/a | n/a | n/a | n/a — only fires on a VALID match; structurally cannot reach either rejection admission surface. This is D8's exact hazard: a post-match reject persists `scenario='strong_match'` (the pre-dispatch envelope, never overwritten) with the real reason only in `import_result->>'decision'`. |
| `validation_error` | `lib/beets.py:53` (`VALIDATION_ERROR_SCENARIO`) | yes | `wrong_matches/` (curated) | yes | no — not in the allowlist | yes → `wanted` |
| `audio_corrupt` | `lib/download_processing.py:149` (media-readiness, pre-beets) **and** `lib/quality/pipeline.py::evidence_decision_name` (evidence-gate reject, reached by both auto and force dispatch) | **yes**, always | **none — deleted outright** (D3) | **no** — never gets a `failed_path` | n/a — never reaches the reducer; the ban+delete happens directly at reject time, not through the lane | auto: yes → `wanted`; force: no, preserves operator status |
| `bad_audio_hash` | `lib/quality/pipeline.py::evidence_decision_name` (evidence-gate) | yes | none — `_cleanup_staged_dir` immediately, never quarantined | no | n/a | per caller's `requeue_on_failure` |
| `nested_layout` | `lib/quality/pipeline.py::evidence_decision_name` | **no** | none — immediate cleanup | no | n/a | per caller |
| `empty_fileset` | `lib/quality/pipeline.py::evidence_decision_name` | **no** | none — immediate cleanup | no | n/a | per caller |
| `mixed_source` | `lib/quality/pipeline.py::evidence_decision_name` | yes | none — immediate cleanup | no | n/a | per caller |
| `spectral_reject` | `lib/quality/pipeline.py` (stage1/stage2 quality decision) | yes | none — immediate cleanup | no | n/a | per caller |
| `untracked_audio` (automation) | `lib/download_validation.py:932` and `:940` (`_check_staged_audio_manifest`, pre-beets manifest guard) | **yes** (D4 — was missing before this issue) | `wrong_matches/` — **whole-folder move** including untracked extras (D4; was curated-only, silently dropping the very files that caused the reject) | yes | no — not in the allowlist | yes → `wanted` |
| `untracked_audio` (force manifest guard) | `lib/dispatch/manifest_guard.py:181` (`_guard_force_import_audio_manifest`'s `extra()` default) | **no** — the guard's own docstring: "a manifest mismatch reflects the operator's folder choice, not the peer's quality" | none moved — the existing Wrong Matches folder is preserved exactly where it was; only the audit row is written | yes (the row carries `failed_path`) | n/a — force jobs never reach `_cleanup_committed_wrong_match_rejection` at all | no — preserves the request's current status |
| `incomplete_fileset` | `lib/dispatch/manifest_guard.py:177` (`incomplete()`) | no | none moved — folder preserved | yes | n/a — force only | no |
| `unverifiable_source` | `lib/dispatch/manifest_guard.py:225` | no | none moved — folder preserved | yes | n/a — force only | no |
| `request_missing_mbid` | `lib/download_validation.py:1084` (`_handle_valid_result`, Lane B) | **yes** (D4) | `wrong_matches/` — whole-folder move (D4) | yes | no — not in the allowlist | yes → `wanted` |
| `request_missing_request_id` | `lib/download_validation.py:1071` | **yes** (D4) | `wrong_matches/` — whole-folder move (D4) | yes | no — not in the allowlist | yes → `wanted` |
| `timeout` | `lib/dispatch/core.py:1465` (`import_one.py` subprocess timeout) | no | none — no folder move at all | no | n/a | per caller |
| `exception` | `lib/dispatch/core.py:1494` (unhandled exception in dispatch) | no | none | no | n/a | per caller |
| `quality_downgrade` / `transcode_downgrade` / `suspect_lossless_downgrade` / `suspect_lossless_probe_missing` / `lossless_source_locked` / `duplicate_remove_guard_failed` / `verified_lossless_locked` | `lib/dispatch/core.py::_describe_rejection` (post-beets-launch quality decisions) | decision-dependent (`decision_denylists`) | none — `_cleanup_staged_dir` (auto) or preserved (force, `mark_done=False`); never quarantined into Wrong Matches | no — the persisted `validation_result.scenario` stays `strong_match` (D8), and none of these ever get a `failed_path` regardless | n/a | per caller |
| `abandoned_auto_import` | **HISTORICAL — no current writer.** Pre-#898 interrupted-request auto-import cleanup rows. Only reader: `lib/pipeline_db/misc.py:262` (a `WHERE … <> 'abandoned_auto_import'` exclusion, not a producer). Registered historical in `tests/test_wrong_match_scenario_producer_audit.py`. | — | — | — | — | — |

Every folder/audio-integrity fact's denylist policy above and the general
quality-decision denylist policy both come from the single production
lookup, `lib.quality.dispatch_actions.dispatch_action` /
`decision_denylists` — CLAUDE.md § "Quality decisions live in ONE place".
This document never restates that policy; it only records which of those
decisions can ever reach a Wrong-Matches-relevant admission surface (a
`failed_path`) at all — most cannot, because they are cleaned up immediately
and never quarantined.

## The four sites that actually write `failed_path`

Only these ever set `validation_result->>'failed_path'`, which is the sole
gate `PipelineDB.get_wrong_matches()` filters on:

1. **Lane A** — `lib.download_rejection._handle_rejected_result`, the shared
   beets-invalid-match / media-readiness reject handler. `move_failed_import_
   curated` for every scenario except `audio_corrupt`, which instead calls
   `_delete_rejected_source_cancellable` (no move, no `failed_path`) — the
   only scenario `_handle_rejected_result` special-cases (D3).
2. **Lane B** — `lib.download_rejection._reject_request_auto_import`, the
   pre-dispatch "cannot safely auto-import" guard
   (`untracked_audio` / `request_missing_mbid` / `request_missing_request_id`).
   Always `move_failed_import_whole` (D4) plus the denylist write this lane
   was missing before this issue.
3. **The force-import manifest guard** —
   `lib.dispatch.manifest_guard._guard_reject`, which writes `failed_path`
   directly into the `ValidationResult` it persists, without moving anything
   (`incomplete_fileset` / `untracked_audio` / `unverifiable_source`). No
   denylist, by design (the guard's own docstring: a manifest mismatch
   reflects the operator's folder choice, not the peer's quality).
4. **The evidence-gate `audio_corrupt` reject, before D3** — historical only.
   Current code deletes instead (see the events table above); the read-side
   exclusion for the historical cohort remains in `wrong_match_row_is_visible`
   (`lib/wrong_matches.py`) and `PipelineDB.get_retained_failure_paths` (the
   `post_commit_quarantine` audit key protects those rows from the disk
   reaper for as long as the audit row exists — no current writer produces
   the key any more).

## Force-import outcomes (D7)

- **Success** consumes its source: `scripts/importer.py::
  _dismiss_successful_force_import` calls
  `lib.wrong_matches.cleanup_wrong_match_source` (delete + clear pointers) —
  completing the operator's own explicit action, not an autonomous quality
  result. This reverses the mid-July "dismiss but preserve" regression that
  had stranded 64 of 90 wrong-match-sourced force imports as invisible disk
  folders (verified live 2026-08-12).
- **Failure on `audio_corrupt`** also bans + deletes:
  `scripts/importer.py::_cleanup_failed_force_import` calls the same
  `cleanup_wrong_match_source` helper on the ORIGINAL Wrong Matches source
  (not the disposable private force action copy dispatch itself touches),
  keyed on `outcome.post_commit_wrong_match_scenario == "audio_corrupt"`.
- **Failure on every other scenario** preserves the source exactly as-is:
  `"outcome": "preserved_operator_force_source"` — the original
  force/quarantine directory is operator authority and audit evidence;
  cleanup of the raw source requires a distinct operator action, never a
  quality result.

## What is deliberately unchanged

- **The cleanup reducer** (`lib/wrong_match_cleanup_service.py`) — keep/
  delete/skip logic and the verified-lossless-parent short circuit. D2/D9.
- **Quality rejects** (the downgrade family, `spectral_reject`) — ban +
  delete-the-source stays exactly as it was: "library holds better" is the
  redundancy proof by definition, and neither of these ever reaches a
  Wrong-Matches-visible state anyway (they never get a `failed_path`).

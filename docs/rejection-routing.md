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
| `extra_tracks` | `lib/beets.py:175` (`choose_match` handler) | yes | `wrong_matches/` (curated move — see "The curated move's actual residue behavior (B1)" below) | yes | **yes** (allowlist) | yes → `wanted` |
| `unmapped_audio` | `lib/beets.py::apply_candidate_scenario` (selected candidate does not cover every admitted local audio path exactly once, after the one Discogs flat-subtrack retry when applicable) | yes | `wrong_matches/` (curated) | yes | no — a manifest-integrity failure is not a pressing-match cleanup authorization | yes → `wanted` |
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
   `_checkpoint_then_delete_rejected_source` (no move, no `failed_path`) — the
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

## The curated move's actual residue behavior (B1)

`move_failed_import_curated` (`lib/import_manifest.py`) does NOT split
untracked content out of the folder it quarantines. Earlier drafts of this
document said untracked audio "stays out" of `wrong_matches/` — that was
never quite true, and B1 (a round-2 review blocker) makes the actual
behavior load-bearing rather than incidental:

- **Empty directory skeletons are pruned silently.** The move loop
  relocates files via `os.walk` but never removes the directories it
  walked through, so a benign non-audio subdirectory (e.g. a `Scans/`
  folder whose contents all moved) is left behind as an empty shell.
  `_prune_empty_dirs` removes these bottom-up before the leftover check
  runs, so a benign sidecar-only world produces no anomaly and no note in
  the persisted detail.
- **Genuine residue is swept into the SAME destination, never split off
  or silently dropped.** If real content survives the move plus pruning —
  including an out-of-manifest audio file the main loop deliberately
  skipped — "kept implies visible" (D1) outranks manifest purity:
  `_sweep_residue_into_destination` moves it into the same
  `wrong_matches/<name>/` folder the curated files landed in. The caller
  (`lib.download_rejection._handle_rejected_result`) folds the resulting
  note into the persisted `ValidationResult.detail`, so it surfaces in
  Recents forensics (`download_log.beets_detail`) — never as a stack
  trace, and never silently.
- **This is deliberate, not a fallback.** Before B1, a genuinely
  unexpected leftover raised an exception AFTER the move had already
  happened — outside the function's own rollback block — which stranded
  the album in `wrong_matches/` with zero `download_log` rows, zero
  denylist writes, and no requeue: the exact invisible-quarantine
  pathology this issue exists to kill. Sweeping instead of raising is the
  fix; the anomaly note is how the operator still finds out.
- **The composed note is one of three distinct, truthful facts (R4-2,
  round-4 review) — never a single "confident" wording stretched over an
  uncertain observation.** `_observe_leftovers` walks the whole subtree
  for actual FILES (not just directory nodes) and reports a tri-state
  result — `"empty"`, `"present"`, or `"unverified"` when the walk itself
  could not fully complete (EACCES/EIO on a sub-directory; virtiofs makes
  these real):
  1. **Confirmed content, confirmed swept clean** — the observation found
     real files, the sweep ran, and a re-check confirms nothing real
     remains: `"curated move left untracked content behind … swept into
     the wrong_matches quarantine destination"`.
  2. **Confirmed content, sweep left some behind** — same as above but the
     re-check still finds real content (the sweep itself failed, or hit
     its own per-file error): the same note with an appended `"
     (incompletely — some residue could not be moved)"`.
  3. **Unverified — the observation, not the content, is what's in
     doubt.** Either the FIRST check couldn't complete (a transient read
     failure that may or may not resolve on retry) or the prune step
     itself failed while leaving behind only an empty, content-free
     directory skeleton (an unprunable but genuinely empty `Scans/`, for
     example — the R3-1 pin's exact world). Both compose the SAME hedged
     note: `"could not verify the curated move source was fully consumed
     despite an exact allowed_audio match; any residue was swept into the
     wrong_matches quarantine destination if present"`. This case never
     claims untracked content definitely existed — a transient read
     failure is not evidence of a leftover, and a benign unprunable
     skeleton holds no content to begin with.
- **The F7 consequence:** a folder that received swept-in, out-of-manifest
  audio will subsequently REFUSE force-import. The force-import manifest
  guard (`lib.dispatch.manifest_guard._guard_force_import_audio_manifest`)
  compares the folder's actual audio against the request's validated
  manifest and rejects with `untracked_audio` when they don't match
  exactly. The swept folder stays exactly where the curated move put it —
  visible in the worklist, kept, and deletable by the operator — but it
  will not import via the normal force-import path until the extra audio
  is removed by hand. This is accepted design, not a bug: the guard's own
  job is refusing to hand beets a folder whose contents don't match what
  the operator is confirming, and a swept anomaly folder is exactly such
  a folder.

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
- **A crash between the terminal commit and this receipt is durably
  retried, not lost** (issue #1122): `scripts/importer.py::
  recover_abandoned_running_jobs` replays `_dismiss_successful_force_import`
  / `_cleanup_failed_force_import` — the SAME helper the live path calls,
  driven from the `result` JSONB alone, never a live `DispatchOutcome` —
  for every row `lib/pipeline_db/import_jobs.py::
  list_terminal_force_wrong_match_cleanup_jobs` names, on every importer
  startup.
- **That predicate is a POSITIVE selection rule, not an exclusion
  enumeration** (issue #1122 review round corrected an unsound draft that
  shipped as an exclusion list): a row qualifies only if (1) its `result`
  carries `post_commit_wrong_match_scenario` — an era-AND-lane marker only
  `scripts/importer.py::_job_result` ever writes, so every historical
  (pre-#1122) or otherwise non-adjudicating shape is excluded by
  construction, not by naming each one — measured live on doc2: 619
  pre-feature rows would have matched a presence-only predicate at first
  startup (477 completed, 142 failed), none of them actual crash-window
  rows, and they stay receiptless forever by design; and (2) its existing
  receipt, if any, is not PROVEN successful
  (`result #>> '{wrong_match_dismissal,success}'` /
  `result #>> '{cleanup,success}'` `IS DISTINCT FROM 'true'`) — a receipt
  can be PRESENT with `success: false` (entry not found, an unsafe path,
  an `rmtree` failure, an EACCES-shaped `path_unavailable` — the #1063
  shape), and a presence-only check would treat that failed receipt as
  "done," parking the row forever; keying on proven success instead means
  a persistently failing cleanup is retried every startup, never parked.
  Two failure-arm rows carry the marker but are still excluded, because
  the LIVE path never reaches a cleanup decision for them either:
  `code = 'requeue_failed'` (the requeue UPDATE itself failed, never even
  calls the cleanup helper) and `deferred = true` (e.g. release-lock
  contention — the cleanup helper IS called live but its own first line
  skips the decision). Replay must reach that identical no-op, not invent
  a verdict the live path never made.

## Local-import outcomes (issue #1176 PR3)

`local_import` is a **dispatch-attempt scenario** (`dispatch_import_core`'s
`scenario` parameter / `FORCE_IMPORT_SCENARIOS` namespace), not a
rejection-scenario literal in the events table above — the same namespace
`force_import` and `auto_import` live in, neither of which is a table row
either. It never appears as `validation_result->>'scenario'` and never
reaches `outcome.post_commit_wrong_match_scenario`; when a producer file
above spells a rejection scenario for a local-import attempt, it is
*exactly* one of the strings already in the events table (see below) — no
new rejection-scenario vocabulary exists (`tests/test_wrong_match_scenario_
producer_audit.py`'s registered producer files —
`lib/beets.py`/`lib/download_validation.py`/`lib/download_processing.py`/
`lib/dispatch/manifest_guard.py` — spell no new literal for this lane).

- **Deliberately excluded from `FORCE_IMPORT_SCENARIOS`** (unlike
  `force_import`), so `lib.dispatch.helpers._should_cleanup_path` always
  returns `True` for it: for any outcome that reaches `dispatch_import_core`
  (accept, or a reject in the evidence pipeline downstream of the
  strict-validation guard), the private action copy is cleaned up on
  every one of THOSE — exactly like an auto-import's disposable
  processing-owner source, never only on success the way force's is. This
  is a correctness requirement, not a convenience: the action copy is
  ordinarily Cratedigger's own disposable scratch, not the operator's
  quarantine folder, so there is no reason to preserve it for review. The
  strict-validation guard's OWN reject (below) never reaches
  `dispatch_import_core` at all, so `_should_cleanup_path` is never
  consulted for it. But it is still a terminal failure bundle, so
  `scripts/importer.py::_cleanup_terminal_force_action` DOES run for it —
  it just finds nothing, because relocation into `wrong_matches/` (below)
  always runs FIRST, synchronously, before the terminal bundle is ever
  persisted. That ordering is the safety, not an incidental detail: it is
  what stops cleanup from ever finding the relocated folder still at its
  original private-copy path and deleting it there. Its `force_action_
  cleanup.removed: true` receipt is therefore misleading in isolation
  (nothing was removed by that specific call) but harmless, since the
  crash-recovery replay sweep it feeds correctly reads `true` as "nothing
  left to reap" either way.
- **A strict-validation reject reuses the manifest guard's own writer**
  (`lib.dispatch.manifest_guard._guard_reject`, extended with an optional
  `distance` parameter for this caller) rather than a parallel one. The
  local-import entry point (`lib/dispatch/entry_points.py`) runs
  `validate_release_with_merge_redirect` at the request's ORDINARY
  `beets_distance_threshold` — never `FORCE_IMPORT_DISTANCE_THRESHOLD` — and
  when the verdict is invalid, rejects with `scenario=validation.result
  .scenario` (beets_validate's own vocabulary: `extra_tracks` /
  `high_distance` / `mbid_not_found` / `no_choose_match` / …) and the real
  measured `distance`. That reject lands in the SAME table rows above, with
  the SAME worklist-visibility and cleanup-lane-admission routing automation
  gets for the identical scenario. Unlike force, local-import does NOT
  ignore an invalid verdict — "import despite the verdict" is what force is;
  strict pressing identity is what local-import is (CLAUDE.md decision 3 for
  #1176). Force-importing the resulting Wrong Matches row remains the
  already-built escape hatch.
- **`audit_source_path` is always `None`** (`source_reference_path` is never
  set for this lane — CLAUDE.md decision 2 for #1176), so `_guard_reject`'s
  `failed_path = audit_source_path or failed_path` always falls back to the
  disposable private action copy. No Wrong Matches row this lane writes ever
  carries the operator's real folder as `failed_path`.
  Unlike force — whose `failed_path` fallback is always an EXISTING
  `wrong_matches/`-rooted folder, since force always acts on a row already
  there — local-import has no pre-existing quarantine source at all, so an
  unmoved action copy would name a folder with no `wrong_matches` path
  component, making the row invisible to `open_configured_quarantine_
  directory` (the gate `enqueue_force_import`, `wrong-match-delete[-group]`,
  `-converge`, and the autonomous reducer all open the row's path through)
  and worklist-visible with literally no action able to touch it (issue
  #1176 PR3 review round, F4). The strict-validation guard therefore
  relocates the action copy into `<processing_dir>/albums/wrong_matches/`
  (`lib.import_manifest.move_failed_import_whole` — whole, not curated,
  since no validated audio manifest exists yet at this pre-evidence guard)
  BEFORE calling `_guard_reject`, so `failed_path` names that new location.
  A relocation failure never blocks the rejection itself; it just leaves
  the audit naming the unmoved path, exactly as it did before this fix.
- **Never consumes a Wrong Matches source on success or failure** — unlike
  force's D7 routing above. `scripts/importer.py::
  _force_job_wrong_match_payload` returns `None` for any job whose kind
  adapter does not set `owns_wrong_match_source` — since issue #1278 that
  registry flag, not a `job_type != 'force_import'` comparison, is the
  mechanism, and `force_import` is the only kind that sets it. So
  `_dismiss_successful_force_import` / `_cleanup_failed_force_import` are
  no-ops for every local-import job regardless of outcome — there is no
  "original quarantine folder" for this lane to consume; a local import's
  REAL source is the operator's folder, which this lane never deletes,
  moves, or otherwise mutates. `lib/pipeline_db/import_jobs.py::
  list_terminal_force_wrong_match_cleanup_jobs` (the D7 crash-recovery
  replay sweep) stays `job_type = 'force_import'`-only for the same reason —
  widening it would cost nothing functionally (the same no-op guard applies)
  but would misstate what the query is for.
- **The job-scoped private action copy IS reaped on crash**, mirroring D7's
  OTHER receipt: `lib/pipeline_db/import_jobs.py::
  list_terminal_force_action_cleanup_jobs` is widened to `job_type IN
  ('force_import', 'local_import')`, since both retain an identically-shaped
  private copy under `processing/albums/` (`force-action-<job_id>` /
  `local-import-action-<job_id>`) that needs the same crash-safe convergence.
  `scripts/importer.py::_cleanup_terminal_force_action` (the live cleanup
  call this sweep replays) resolves each job's own prefix through that job
  kind's action-copy lane, which reads
  `lib.preview_snapshot.ACTION_COPY_PREFIX_BY_JOB_TYPE` — the single source
  for the mapping — before calling
  `cleanup_force_action_copy_for_job` — a PR3 review round found this call
  hardcoding force's own prefix, so every local-import cleanup compared its
  path against the WRONG job type's deterministic name, raised before ever
  touching the filesystem, and re-raised identically on every subsequent
  replay of this same sweep (issue #1176 PR3 review round, F5).

## The `wrong_match_triage` audit block is reducer-only

`download_log.validation_result.wrong_match_triage` is written by exactly one
producer: `lib.wrong_match_cleanup_service.cleanup_wrong_match` (the reducer),
via `db.record_wrong_match_triage`. Nothing else writes it — not Lane A, not
Lane B, not the force-import manifest guard, not force-import success/failure
consumption. Because the reducer is only ever reached for the
**cleanup-lane-admission allowlist** (`rejection_scenario_is_delete_eligible`
— `extra_tracks` / `high_distance` / `mbid_not_found` / `no_choose_match`,
D6), every OTHER worklist-visible scenario in the events table above —
`unmapped_audio`, `untracked_audio`, `validation_error`, `request_missing_mbid`,
`request_missing_request_id`, `incomplete_fileset`, `unverifiable_source` —
never gets a `wrong_match_triage` block, past or present. This was always
true; issue #1077 widens which of those rows are worklist-visible in the
first place (D1: kept implies visible), so the "visible with no triage chip"
population is now larger than before, not new.

`web/js/recents.js` and `web/js/history.js` both render the
`wrong_match_triage_*` fields conditionally (`if (h.wrong_match_triage_summary)
{ … }` in `history.js`; the equivalent guard in `recents.js`) — a row with no
triage evidence simply shows no chip/detail, which is the CORRECT and
expected rendering for the whole non-delete-eligible cohort, not a missing-data
bug. Only a row the reducer actually evaluated (delete-eligible, evaluated at
least once) ever carries this block.

## What is deliberately unchanged

- **The cleanup reducer** (`lib/wrong_match_cleanup_service.py`) — keep/
  delete/skip logic and the verified-lossless-parent short circuit. D2/D9.
  One later refinement (issue #1241): when the request carries the
  operator's incomplete mark AND the row's persisted scenario proves the
  candidate whole (`scenario_covers_declared_program`), the shared decider
  disregards the installed side, the decision is import-class, and the
  reducer KEEPS the folder (`kept_would_import`, still kept + banned +
  visible per #1077). Of the delete-eligible four, only `high_distance`
  is covered — `extra_tracks` proves the opposite, and
  `mbid_not_found`/`no_choose_match` never produced a checked candidate
  (`unmapped_audio` is also covered but is not delete-eligible, so it
  never reaches this reducer's deletion lane).
  The verified-lossless-parent short circuit yields for exactly those
  rows — its "guaranteed to lose" premise no longer holds — so the full
  decider runs instead of deleting. Everything below is otherwise as
  written.
- **Quality rejects** (the downgrade family, `spectral_reject`) — ban +
  delete-the-source stays exactly as it was: "library holds better" is the
  redundancy proof by definition, and neither of these ever reaches a
  Wrong-Matches-visible state anyway (they never get a `failed_path`).

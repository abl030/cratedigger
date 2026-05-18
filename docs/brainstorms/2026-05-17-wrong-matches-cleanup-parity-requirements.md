# Wrong Matches Cleanup ↔ Force-Import Parity — Requirements

**Date:** 2026-05-17
**Tracking issue:** #268
**Status:** Brainstorm complete, ready for planning

## Problem

Wrong Matches bulk cleanup and force-import call the same decision reducer (`full_pipeline_decision_from_evidence`) but feed it different inputs. Specifically, cleanup loads "current" (in-Beets) evidence only when the request's `imported_path` is set or `status='imported'`. For a `wanted` row whose MBID is already present in Beets, cleanup passes `current=None` and the reducer can return `would_import` — while force-import's action-time evidence gate looks the album up in Beets by MBID, loads/backfills the current evidence, and the same reducer correctly returns `downgrade`.

Concrete incident: Parts & Labor — Escapers Two (request 2762, download_log 16092, force-import job 40881). Cleanup left the row visible as `kept_would_import`; the operator then clicked Force Import, which rejected the same candidate as `downgrade` (candidate avg 197 kbps MP3 spectral_likely_transcode 160 vs current avg 198 kbps MP3).

A prod query identified 722 currently-visible Wrong Matches rows in the same risk bucket — `status != 'imported'`, empty `imported_path`, but a non-NULL `current_evidence_id`. Not all are misclassified, but cleanup is systematically making weaker-evidence decisions for the bucket.

A second, smaller UI bug compounds the confusion: the Wrong Matches expanded header reports "No successful import on disk" based on `download_log` success history, not on Beets/current evidence. For the Parts & Labor row the header said no import existed, even though Beets had the album and force-import rejected against it.

## Goal

For any Wrong Matches row, the decision cleanup reaches must be identical to the decision force-import would reach for the same candidate / request / MBID / config. No second reducer; no weaker evidence loader; no divergence.

## Scope

### In scope

1. **Factor the action-time current-evidence loader into a shared helper.**
   The current-evidence loading half of `lib/import_dispatch.py::_load_evidence_import_gate` — Beets-by-MBID lookup → `ensure_current_evidence_for_action` → fail-closed semantics — moves into a neutral helper (likely a new function in `lib/import_evidence.py`, or `_load_evidence_import_gate` itself becomes the shared call site). Force-import and cleanup both call it. Cleanup's own `_load_current_evidence` is deleted.

2. **Cleanup fails closed when Beets has the album but current evidence cannot be obtained.**
   Same semantics as force-import: if Beets returns `album_info` for the MBID but `ensure_current_evidence_for_action` fails / returns stale / returns incomplete, cleanup must not pass `current=None` to the reducer. It records the failure outcome and leaves the row visible.

3. **Verified-lossless early exit in cleanup.**
   When the shared helper returns a `current` whose `verified_lossless_proof` is set, cleanup skips the full pipeline reducer entirely and deletes the source. Verified-lossless is the top rank — any candidate sitting in Wrong Matches against that MBID is guaranteed to lose the upgrade gate. New outcome label (e.g. `OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT`) for audit clarity; no candidate-evidence comparison, no bitrate math.

4. **UI: Wrong Matches header distinguishes "no cratedigger import history" from "current album exists in Beets."**
   The expanded group header in the Wrong Matches view stops saying "No successful import on disk" when current evidence exists / Beets has the album. New copy should make it obvious that the MBID is already in the library (and that any force-import attempt will gate against it).

### Out of scope

- **No retroactive sweep of the 722 risk-bucket rows.** Fix forward — the next manual Clean All click re-evaluates them correctly through the shared helper.
- **No second / parallel quality decision function.** Explicit non-goal from the issue. Everything routes through `full_pipeline_decision_from_evidence` (or, for the verified-lossless case, an explicit short-circuit before that call).
- **No change to per-row cleanup deletion / advisory-lock / active-job policy.** Cleanup still owns those after the shared decision (or the verified-lossless short-circuit) returns a terminal reject.
- **No change to candidate evidence loading.** Cleanup's candidate-evidence loader already pulls from the explicit download_log row with snapshot freshness — that path is fine.

### Deferred to ce-plan

- Exact factoring shape. Three plausible options: (a) expose `_load_evidence_import_gate` directly to cleanup, (b) lift just the current-evidence half into a smaller helper in `lib/import_evidence.py` and have both call sites compose candidate+current themselves, (c) introduce a thin orchestrator that returns `(candidate, current, gate_outcome)` for any action-time caller. ce-plan picks one based on the call-site shapes.
- Exact UI copy for the corrected header.

## Tests owed

All exist in `tests/test_wrong_match_cleanup_service.py` or `tests/test_integration_slices.py` (whichever fits — slices are required when the test exercises the shared evidence helper end-to-end).

1. **Cleanup uses current evidence even for `wanted` rows.**
   Request `wanted`, no `imported_path`, MBID present, candidate evidence MP3 avg 197 / spectral likely_transcode 160, current evidence (Beets-resolved) MP3 avg 198 → cleanup classifies as `confident_reject` / `downgrade`, must NOT return `OUTCOME_KEPT_WOULD_IMPORT`.

2. **Cleanup ↔ force-import decision parity.**
   Same candidate evidence and current evidence fed into both the cleanup path and `dispatch_import_from_db`'s evidence gate (Beets mutation mocked). Both must produce the same decision name (`downgrade`); neither may diverge into `would_import` vs `quality_pipeline_rejected`.

3. **Beets-absent still allows `would_import`.**
   Beets-by-MBID returns no album → cleanup passes `current=None` → a genuinely-first-import candidate may still classify as `would_import`. Guards against overcorrection.

4. **Beets-present-but-current-evidence-missing fails closed.**
   Beets returns `album_info` but `ensure_current_evidence_for_action` fails / stale / incomplete → cleanup records a current-evidence skip outcome, must NOT classify as `would_import`.

5. **Verified-lossless short-circuit.**
   Current has `verified_lossless_proof` set; candidate is any plausible non-verified-lossless file (even a high-bitrate-looking MP3). Cleanup deletes with the new `OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT` outcome and does NOT invoke `full_pipeline_decision_from_evidence`. (Mock or assert-not-called the reducer to lock this in.)

6. **UI header test.**
   Wrong Matches group state: no successful `download_log` import history, but `current_evidence_id` set / Beets has album. Frontend renders a header that distinguishes the two cases — does not say "No successful import on disk." Existing `tests/test_js_util.mjs` or a contract test in `tests/test_web_server.py` (whichever owns this header's data path) gets the assertion.

## Invariant being restored

From `CLAUDE.md`: *"Quality decisions live in ONE place. `full_pipeline_decision_from_evidence` is the single source of truth for every importer decision. Never re-create import decisions elsewhere."*

Calling the right reducer with materially weaker inputs is a decision divergence even when the function call is shared. After this PR, every action-time caller of the reducer (import dispatch, force-import, manual import, Wrong Matches cleanup) acquires its current evidence through the same Beets-by-MBID → `ensure_current_evidence_for_action` path with the same fail-closed semantics. The verified-lossless short-circuit is an explicit, named exception that lives in one place (cleanup), guards a single decision class (delete-doomed-clutter), and never reaches the reducer with partial evidence.

## Success criteria

- A candidate that force-import rejects as `downgrade` is never classified by cleanup as `would_import`.
- Cleanup never passes `current=None` to the reducer when Beets has the MBID and current evidence is loadable.
- Wrong Matches rows whose parent album is verified-lossless in Beets are auto-deleted on the next Clean All without manual force-import roundtrip.
- The Wrong Matches expanded header no longer falsely claims the album is absent from disk when it is in Beets.
- All six tests above pass; `pyright` clean; full suite green.

## Open questions

None. All product decisions resolved during brainstorm.

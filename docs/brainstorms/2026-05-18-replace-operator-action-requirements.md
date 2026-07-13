---
date: 2026-05-18
topic: replace-operator-action
---

# Replace Operator Action

## Summary

A first-class "Replace" action lets the operator abandon an existing `album_requests` row in favor of a fresh row pointing at a different MusicBrainz release ID within the same release group. The old row is preserved as a frozen audit record in a new terminal status `replaced` — its MBID, download history, denylist, wrong-matches log, and search history all stay attached, untouched. A new row is created with the chosen target MBID, `status='wanted'`, and a lineage link (`replaces_request_id`) back to the old row. The action surfaces as a button on every release line in the Browse, Pipeline, and Wrong Matches tabs, with a single shared picker showing same-release-group pressings. The work also enshrines two new CLAUDE.md critical invariants ("the pipeline self-heals" and "don't duplicate convergence") and drops obsolete upstream-bridge columns in a sibling migration.

---

## Problem Frame

Three distinct operator workflows currently dead-end with no clean recovery:

**(a) MBID merged upstream.** A user adds a release via the web UI. Months later, MusicBrainz editors merge that release into another canonical pressing. Beets queries MB during validation and only ever returns the canonical MBID as a candidate; the harness's exact-equality check on `mb_release_id` never matches. The request stalls forever with `scenario=mbid_not_found`. Pet Grief (request 4194, MBID `72988560-e8fc-4429-9c69-7045bb63e248` merged into `18056805-33f5-3e99-aa4b-5f5919c4f8af`) has accumulated 12 rejected downloads across five days. Today, the only recourse is hand-rolled SQL.

**(b) Operator picked the wrong pressing.** The web UI surfaces many pressings of an album; the operator clicks the wrong one. Today there is no way to switch an existing request to a different pressing — the operator has to delete the request (losing its history) and re-add at the new MBID.

**(c) Soulseek-only pressing mismatch.** Soulseek peers serve a specific pressing (e.g. a Japanese edition with one track swapped). Validation correctly rejects every attempt against the original MBID, files pile up in the wrong-matches surface, and the operator notices the actual available pressing is a sibling. Today they have to manually clean up wrong-matches, delete the request, and re-add at the sibling MBID — fighting both the audit trail and the wrong-matches state machine.

All three are the same shape: the request's MBID has stopped pointing at what the operator actually wants in the library, and there is no first-class way to switch targets while keeping the decision history intact.

Beyond the immediate operator workflow, the abandonment itself carries meaning. A `replaced` row is the system's record that "this MBID was deliberately discarded for some reason." The pipeline must never auto-re-attempt the abandoned MBID, and the operator should be reminded if they consider adding the same MBID again later.

---

## Key Flows

- F1. **Replace a request from the Pipeline tab.**
  - **Trigger:** Operator sees a stalled request and clicks the Replace button on the row.
  - **Steps:**
    1. Picker opens showing pressings in the same release group, fetched from the local MB mirror.
    2. Operator clicks the target pressing.
    3. Confirmation dialog summarises the destructive scope and the new identity.
    4. Operator confirms; service runs.
    5. Response surfaces a summary; the row is replaced in the active list by the new request, and the old row is hidden by the default `replaced`-filter.
  - **Outcome:** Old request has `status='replaced'`. New request points to the target MBID with `status='wanted'` and `replaces_request_id` linking back. Next 5-minute pipeline cycle re-sources from the new request.

- F2. **Replace a request from the Wrong Matches tab.**
  - **Trigger:** Operator reviewing wrong-matches realises the rejected files match a different pressing than the requested one.
  - **Steps:** As F1; the row context is a rejected wrong-match record (still attached to the soon-to-be-replaced request).
  - **Outcome:** As F1. The on-disk wrong-matches folders for the old request are purged. The wrong-match log rows themselves stay attached to the old (now `replaced`) request as historical evidence.

- F3. **Replace an imported album from the Browse tab (library mode).**
  - **Trigger:** Operator browsing the library notices an album is the wrong pressing.
  - **Steps:** As F1. The service additionally invokes the Palo-Santo-safe beets-removal primitive against the old request's library entry and triggers Plex / Jellyfin rescans so the ghost is dropped.
  - **Outcome:** As F1. The next import lands the new pressing in the (possibly different) canonical folder.

- F4. **Replace via the Browse tab in search mode (inverted click model).**
  - **Trigger:** Operator browsing MB search finds the pressing they actually want; an existing non-replaced request is on a different pressing in the same release group.
  - **Steps:**
    1. The Replace button on each search row is enabled only when an existing non-replaced request in the same release group exists.
    2. Click opens the shared picker pre-selected on the search-row MBID. If two or more non-replaced requests in the release group exist, the picker first asks which to replace; otherwise it proceeds straight to confirm.
    3. From there, identical to F1.
  - **Outcome:** As F1.

- F5. **Mid-import contention (failure path).**
  - **Trigger:** Operator clicks Replace while the importer worker is actively running for that request_id.
  - **Steps:** Service attempts `pg_try_advisory_lock(IMPORT, request_id)`, fails, returns `wrong_state`. UI surfaces "request is being imported; try again in a few seconds."
  - **Outcome:** No state changes. Operator retries when the import completes.

---

## Requirements

**Action surface**
- R1. Replace is invokable as a service method (`MbidReplaceService.replace_request_mbid`) returning a typed `@dataclass(frozen=True) ReplaceResult`, exposed identically by `pipeline-cli replace <request_id> --to <mbid>` and `POST /api/pipeline/<request_id>/replace` (path-param style, matching the existing `/api/pipeline/<id>/search-plan/...` convention).
- R2. Outcome → exit/status mapping follows the existing CLI ⇄ API convention: `replaced` 200/0, `not_found` 404/2, `wrong_state` 409/4, `target_invalid` 422/3, `target_release_group_mismatch` 422/3, `target_same_as_current` 422/3, `target_collision_request` 409/4, `transient` 503/5.
- R3. Three new routes are added to the route contract audit (`tests/test_web_server.py::TestRouteContractAudit.CLASSIFIED_ROUTES`): `POST /api/pipeline/<request_id>/replace` (the action — path-param style, matching the existing `/api/pipeline/<id>/search-plan/...` convention), `GET /api/pipeline/requests-by-rg/<rg_id>` (the auxiliary listing used by the search-tab inverted-click picker), and `GET /api/pipeline/active-rgs` (returns the set of `mb_release_group_id` values currently held by any non-replaced request, used by the Browse-search button to compute the enable state per R7).

**UI presence**
- R4. A Replace button appears on every release line in the Browse, Pipeline, and Wrong Matches tabs. Browse is a single tab hosting both library and search modes — three button host tabs, not four.
- R5. All button surfaces share a single picker UI component (new `web/js/replace_picker.js`) that lists releases in the same release group as the source, fetched from the local MB mirror via the existing `web/mb.py::get_release_group_releases` helper.
- R6. Browse-library mode / Pipeline / Wrong Matches use the standard click model: the row IS the (non-replaced) request being replaced.
- R7. Browse-search mode uses the inverted click model: the row IS the new MBID; the button is enabled only when an existing non-replaced request in the same release group exists, and clicking opens a two-stage picker (which request to replace → confirm).
- R8. Before any destructive action, the UI shows a confirmation dialog summarising what will happen (old request will be marked replaced; files, beets entry, wrong-matches, staging for the old request will be deleted; new request will be created) and the new identity. The dialog uses a generic summary, not a service-computed dry-run preview.

**Guardrails**
- R9. The service refuses any target MBID whose release group differs from the source request's `mb_release_group_id` (outcome `target_release_group_mismatch`). When the source row's `mb_release_group_id` is NULL (legacy row), the service lazily backfills it from MB before applying the guardrail; if the source MBID itself does not resolve, outcome is `target_invalid`.
- R10. The service refuses any target MBID that fails to resolve on the local MB mirror (outcome `target_invalid`). MB lookup uses `fresh=True` to bypass the 24h cache. If MB's response is HTTP 301 to a canonical MBID different from the operator's target, the canonical is used; an explicit `target_collision_request` outcome fires when the canonical equals the source's current MBID.
- R11. The service refuses when the target MBID equals the source request's current `mb_release_id` (outcome `target_same_as_current`).
- R12. The service refuses when the target MBID is already used by ANY existing `album_requests` row regardless of status (outcome `target_collision_request`). The response body includes the existing row's status so the UI can render a clear message when the existing row is `replaced` ("MBID was previously abandoned") versus active. The UNIQUE constraint on `album_requests.mb_release_id` enforces this structurally; the service catches `psycopg2.errors.UniqueViolation` defensively and maps to the same outcome.
- R13. When the importer worker holds the per-request advisory lock (`ADVISORY_LOCK_NAMESPACE_IMPORT`, `request_id`), the service returns `wrong_state` without making any changes — no pre-emption, no waiting. Replace acquires this same namespace+key for its own work, providing mutual exclusion with the importer.

**Identity transition (supersede semantics)**
- R14. On success, the OLD `album_requests` row transitions to a new terminal status `replaced` and its `updated_at` is touched. **One column carve-out: `imported_path` is cleared to NULL** because Phase 4 deletes the files at that path and a stale pointer would lie about on-disk reality. **No other column on the old row is rewritten.** `mb_release_id`, `mb_release_group_id`, `mb_artist_id`, `artist_name`, `album_title`, `year`, `country`, `min_bitrate`, `verified_lossless`, `current_spectral_*`, `current_lossless_source_v0_probe_*`, `search_filetype_override`, `target_format`, `active_plan_id`, etc. all stay exactly as they were — these are **characteristic** fields (they describe the audio that existed), not **pointer** fields. The old row's child rows (`album_tracks`, `download_log`, `search_log`, `source_denylist`, references in `album_quality_evidence`) stay attached to the old request_id, frozen as historical record.
- R15. On success, a NEW `album_requests` row is INSERTed with: the target MBID, the new release-group ID and metadata (fetched fresh from MB), `status='wanted'`, `replaces_request_id` set to the old row's id, and all derived columns at their default values (NULL / false / 0). `target_format` and any operator-set quality intent do NOT carry over — the new request starts clean and the operator can re-set intent after the next surfacing.
- R16. The new request's `album_tracks` are populated from the target MBID's MB response via the existing `set_tracks` helper.
- R17. The new request's `source_denylist` is empty by default. Old denylist rows stay attached to the old request_id — never read for the new request because the FK is keyed on `request_id`.
- R18. A fresh search plan is generated inline for the new request via `SearchPlanService.generate_for_request` after the DB commit, so the next 5-minute cycle includes the new row in `get_wanted_searchable`. Without this step, the new row would sit out of the search loop until the next cratedigger process restart triggers startup reconciliation.

**Filesystem and external state**
- R19. The OLD request's beets library entry (if it was `imported`) is deleted via the Palo-Santo-safe primitive in `lib/release_cleanup.py`. Replace passes `clear_pipeline_state=False` (a new optional parameter) so the primitive does NOT call `clear_on_disk_quality_fields`. The characteristic fields (`verified_lossless`, `current_spectral_*`, `current_lossless_source_v0_probe_*`) stay set on the frozen row as historical truth. The pointer field `imported_path` is cleared by the supersede transaction itself (per R14's carve-out), separately from this primitive, because Phase 4 deletes the files and the path would be a dangling pointer. Ban-source keeps the existing default `clear_pipeline_state=True` so its behavior is unchanged. Do NOT hand-roll `beet remove -d`.
- R20. The OLD request's wrong-matches folders (if any) are deleted by composing `lib/wrong_match_delete_service.py::delete_wrong_match_group(db, old_request_id)`. This is the operator-authority cleanup path; never `cleanup_wrong_match` (classifier-gated — would skip rows the operator just deliberately abandoned).
- R21. The OLD request's staged folders (if any) under `<staging_dir>/auto-import/<old_artist>/<old_title> [request-<old_id>]/` and `<staging_dir>/post-validation/<old_artist>/<old_title> [request-<old_id>]/` are deleted. Paths are computed via `lib/processing_paths.py::stage_to_ai_path` using values captured from the old row (still readable post-supersede because the old row is unmutated). Skip when the old request was `downloading` at lock-acquire time (staging may be mid-write); log a non-fatal warning instead.
- R22. Plex / Jellyfin rescans are triggered (synchronously, best-effort, ~10s each timeout) using the existing helpers in `lib/util.py`. Plex partial-scan path uses the old request's captured `imported_path` if set.
- R23. In-flight slskd transfers and the OLD request's `active_download_state` are NOT touched by Replace. The transfer continues to completion against slskd; its file lands in `slskd_download_dir` and is no longer polled (because the old row is now `status='replaced'`, excluded from `poll_active_downloads`). The orphaned transfer and landed file are deferred to general slskd-orphan convergence (issue #278). Building partial cancellation here would duplicate that work; under self-heal + don't-duplicate-convergence, accept the orphan inventory until #278 ships.

**Ordering and failure semantics**
- R24. Target validation (MB mirror lookup, release-group check, collision check) runs read-only before any destructive action.
- R25. DB mutations are wrapped in a single transaction with `SELECT ... FOR UPDATE` on the old request row: UPDATE old row's status to `replaced` + INSERT new row + INSERT new `album_tracks` via `set_tracks`. The transaction commits before filesystem cleanup runs.
- R26. Filesystem cleanup failures after the DB commit are logged and surfaced as non-fatal warnings in the API response. They do NOT roll back the DB change — the new request exists with correct identity; orphaned files are tolerable under self-heal.

**Schema cleanup**
- R27. A sibling migration drops two obsolete upstream-bridge columns from `album_requests`. The columns have no readers or writers in the codebase and are vestigial from the soularr-fork era.

**Schema additions**
- R28. A migration adds the new terminal status value `replaced` to the request status taxonomy and a nullable `replaces_request_id INTEGER REFERENCES album_requests(id)` column. The column is set only when the row was created by Replace; otherwise NULL.
- R29. *(withdrawn — was Model-A "download history MBID surfacing"; under the supersede model the historical MBID is naturally attached to the frozen old row, so no view-layer join is needed)*

**Status-aware filters**
- R30. The Pipeline tab's default view filters out `status='replaced'` rows. An opt-in toggle ("show replaced") surfaces them as read-only audit entries with a forward link to the row that replaced them (via `replaces_request_id` reverse lookup).
- R31. The Wrong Matches tab's default view filters out rows attached to `status='replaced'` requests.
- R32. The search-tab inverted-click query (`GET /api/pipeline/requests-by-rg/<rg_id>`) excludes `status='replaced'` rows from the candidate list of "existing requests in the same release group."
- R33. The web-UI add flow (`POST /api/pipeline/add`) continues to refuse adding an MBID that already exists in `album_requests` (UNIQUE backstop). When the existing row is `status='replaced'`, the response surfaces that explicitly so the UI can render a "previously abandoned" warning rather than a generic "already in pipeline" message.

**CLAUDE.md invariants**
- R34. Two new entries are added to the Critical invariants section of CLAUDE.md:
  - **The pipeline self-heals — the request is the source of truth, everything else is derived.** Files, beets entries, wrong-matches folders, search plans, denylist, overrides, evidence — all derived state. Operator actions that touch identity supersede the row rather than mutate it, and let the pipeline rebuild from the new row. Audit trail (the frozen old row and its content-addressed child rows) is preserved by virtue of the old row never being mutated or deleted.
  - **Don't duplicate convergence — reuse the cleanup paths that already exist.** When an operator action could leave behind orphans (in-flight slskd transfers, stale staging, dangling rows), prefer letting existing convergence pick them up over adding bespoke teardown to the action. Where convergence does not yet exist, file an issue (e.g. #278) and ship the closest direct cleanup in the action itself.
- R35. The Critical rule "NEVER use `beet remove -d`" gains an explicit exception for the Replace action, alongside the existing ban-source exception.

---

## Acceptance Examples

- AE1. **Covers R9.** Given a request with `mb_release_group_id=A`, when the operator targets a MBID whose release group is `B`, the service returns `target_release_group_mismatch` and no rows are created or modified.
- AE2. **Covers R12.** Given two existing requests, r1 with MBID X and r2 with MBID Y, when the operator replaces r1 with target=Y, the service returns `target_collision_request` with `current_status='wanted'` (or whatever r2's status is) and no rows are created or modified.
- AE3. **Covers R13.** Given a request with the importer worker actively holding the per-request advisory lock, when the operator invokes Replace, the service returns `wrong_state` immediately and no DB or filesystem mutation occurs.
- AE4. **Covers R14–R23, R26.** Given Pet Grief request 4194 with MBID `72988560-...`, `status='wanted'`, no in-flight downloads, when the operator replaces it with target=`18056805-...`, then: request 4194 has `status='replaced'`; `imported_path` on 4194 is NULL (R14 carve-out, only applies when the prior status was `imported`); all OTHER columns on 4194 are unchanged (including characteristic fields `verified_lossless`, `current_spectral_*`, `current_lossless_source_v0_probe_*`); a new request (e.g. 4995) exists with `mb_release_id='18056805-...'`, `status='wanted'`, `replaces_request_id=4194`; fresh `album_tracks` populated from the new MBID for request 4995; no `source_denylist` rows for 4995; the 12 historical `download_log` rows for request 4194 are still present and still reference 4194; no wrong-matches folder remains on disk for 4194; no staging folders for 4194 remain on disk; 4995 has an active search plan; Plex / Jellyfin rescans were triggered.
- AE5. **Covers R23.** Given a request in `downloading` status with active slskd transfers tracked in its `active_download_state`, when the operator replaces it, the slskd transfers are NOT cancelled and the old row's `active_download_state` is preserved unchanged; the new request is born with empty `active_download_state`; the orphaned transfers' eventual landed files wait for the convergence at issue #278.
- AE6. **Covers R7, R32.** Given an MB search result for MBID Z in release-group R, and no non-replaced existing request in R, when the operator views the search result the Replace button is disabled. When the operator separately adds a request for MBID W in release-group R and revisits the search result, the Replace button on Z becomes enabled.
- AE7. **Covers R33.** Given a previously-replaced request (`mb_release_id='72988560-...'`, `status='replaced'`), when the operator attempts to add the same MBID via the web UI, the response includes `current_status='replaced'` and the UI shows a "previously abandoned" warning rather than the generic "already in pipeline" message.
- AE8. **Covers double-click / already-replaced source.** Given a request that has already been replaced (`status='replaced'`, with a descendant row pointing back via `replaces_request_id`), when the operator invokes Replace on the already-replaced row's id, the service returns `RESULT_WRONG_STATE` (HTTP 409) with `descendant_request_id` set to the descendant's id; no DB mutation occurs. The UI surfaces "This request was already replaced. The new request is at /pipeline/{descendant_request_id}." with a deep-link.

---

## Success Criteria

- Pet Grief request 4194 self-heals end to end via the new action: operator clicks Replace, picks `18056805-...`, a new request (e.g. 4995) is created with the canonical MBID and `status='wanted'`, the next 5-minute pipeline cycle picks it up, search succeeds, validation succeeds (`scenario=strong_match`), the album imports cleanly, and Plex shows the new entry. Request 4194 stays in the DB with `status='replaced'` (its row otherwise unchanged), and 4995's `replaces_request_id` points back to 4194.
- The cratedigger logs show a complete trace through: identity transition (old → `replaced`, new born) → fs cleanup → search-plan regenerate for new request → search → download → validation success → import → rescan, with no manual SQL or filesystem intervention.
- A downstream `ce-plan` working from this requirements document can write the implementation plan without inventing scope: every behavioral question is settled, every outcome and exit code is named, every filesystem effect is specified.
- No obsolete upstream-bridge columns remain in the schema after deploy. The schema includes the new `replaced` status and the `replaces_request_id` column.
- CLAUDE.md contains both new invariants in Critical invariants, and the rule "NEVER use `beet remove -d`" exception is documented.

---

## Scope Boundaries

- **Auto-detection of merged-upstream MBIDs is out of scope.** Replace handles every cause (merged, wrong-pressing, Soulseek-only-sibling) uniformly via manual operator click.
- **Bulk migration of existing stuck requests is out of scope.** Pet Grief 4194 is the dogfood case; operators triage one at a time as they appear.
- **General slskd-orphan convergence is out of scope** — tracked at issue #278. Replace does NOT cancel in-flight transfers; they orphan until #278 ships its convergence path. Deliberate trade-off: building partial cancellation now would duplicate #278's work.
- **UPDATE-in-place on `album_requests` is rejected** as the implementation strategy. No row's `mb_release_id` is ever rewritten after INSERT. Replace supersedes via a new row plus `replaces_request_id` lineage link. This is the load-bearing simplification — no first-of-its-kind mutation, content-addressing stays clean, identity is immutable per row.
- **Retag-in-place of beets entries is rejected.** Replace always supersedes and rebuilds; the old beets entry is removed via the existing Palo-Santo-safe primitive.
- **Wrong-matches-tab Replace operates on the request, not on the on-disk folder identity.** The legacy interpretation (retag a wrong-matches folder to a new MBID) is not implemented.
- **Browse-library-mode Replace requires an attached request row.** The library tab does not mutate beets without a request — every Replace surface flows through `album_requests`.
- **Re-replacing a row that already has `status='replaced'` is not supported.** Replace operates only on non-replaced rows. To switch direction further, operate on the active descendant in the lineage chain.
- **Un-replacing (restoring an abandoned MBID) is not supported.** A `replaced` row is the operator's record of a deliberate abandonment; the system never auto-re-attempts that MBID. If an operator genuinely wants to retry the abandoned MBID, the only path is to delete the lineage chain manually via SQL (descendants first, the `replaces_request_id` FK is `ON DELETE RESTRICT`) and re-add — accepting the loss of audit history.

---

## Key Decisions

- **Supersede via new row, not UPDATE in place.** Avoids cratedigger's first-ever mutation of `mb_release_id`; preserves the operator's abandonment decision as a frozen audit row; aligns with content-addressing intuitions; the `replaced` status is a deliberate signal that this MBID was discarded.
- **Single picker UI component shared across all three button host tabs.** Forces code reuse, gives the operator one mental model, prevents accidental cross-album silliness.
- **Refuse on importer-held lock; no pre-emption.** Imports complete in seconds-to-low-minutes; "click again in a few seconds" is cheaper than building an abort path into the importer worker.
- **slskd transfers are NOT cancelled by Replace; orphans wait for #278.** Earlier consideration was inline cancellation; reversed once research showed the transfer ID isn't persisted in `active_download_state`. Cleanest path is to ship Replace as a pure DB+fs operation, leave in-flight transfers alone, and let the general convergence at #278 handle the inventory. Replace is operator-rare so the orphan count stays small in the meantime.
- **Lineage column direction: `replaces_request_id` on the NEW row.** Newer references older — standard supersede direction.
- **Lineage FK is `ON DELETE RESTRICT`, not `SET NULL` or `CASCADE`.** RESTRICT enforces the "replaced rows are never deleted" invariant at the schema level. SET NULL would silently sever lineage on out-of-band deletes (DBA cleanup, future prune scripts) — the very class of bug the audit trail exists to prevent. CASCADE would compound damage by deleting active descendants. RESTRICT makes "delete a row in the lineage chain" an explicit operator action requiring descendants-first traversal, matching the brainstorm's "un-replacing is not supported" scope boundary.
- **No identity-change audit table needed.** The frozen old row + the `replaces_request_id` link IS the audit, naturally.
- **Obsolete upstream-bridge columns dropped in the same PR.** Vestigial since the soularr-fork era; the "never defer work" feedback calls them out as the canonical example.

---

## Dependencies / Assumptions

- The local MB mirror returns HTTP 301 to the canonical release for merged MBIDs. Verified live during the originating `ce-debug` session: `curl http://192.168.1.35:5200/ws/2/release/72988560-...` returned `HTTP/1.1 301 Moved Permanently\nLocation: .../18056805-...`. `urllib.request.urlopen` (the `web/mb.py::_get` transport) follows 301 transparently and parses the canonical body — so `data["id"]` is the canonical MBID and `data["release-group"]["id"]` is the canonical RG. No code change needed in `web/mb.py`.
- `lib/wrong_match_delete_service.py::delete_wrong_match_group` is the right operator-authority cleanup for stale wrong-matches under the old request_id.
- `lib/release_cleanup.py::remove_and_reset_release` is the Palo-Santo-safe beets-removal primitive used by ban-source. Replace reuses it against the OLD request_id + OLD release_id, with a new `clear_pipeline_state=False` parameter (added in this PR, default `True` keeps ban-source behavior) so the OLD row's quality fields stay frozen.
- `lib/processing_paths.py::stage_to_ai_path` constructs staging paths keyed by request_id + artist/title. Replace captures these from the now-frozen old row.
- The per-request `ADVISORY_LOCK_NAMESPACE_IMPORT` advisory lock used by the importer worker is available for Replace's contention check (same namespace+key for mutual exclusion).
- `SearchPlanService.generate_for_request` can be called inline to seed a fresh plan for the new request.
- The UNIQUE constraint on `album_requests.mb_release_id` enforces target-collision structurally; the service catches `psycopg2.errors.UniqueViolation` and maps to `target_collision_request`.
- Operators are the only actors; no multi-user permissions model is needed.

---

## Outstanding Questions

### Deferred to Planning

- [Affects R5, R8][Technical] Exact UI placement / styling of the Replace button on each tab (icon, label, position in the row), and the confirmation dialog's content shape. Research recommendation: extend the existing `web/js/release_actions.js::renderActionToolbar` and use a generic confirm-dialog (the action is fully recoverable under self-heal, so a dry-run preview adds round-trip cost without operator benefit).
- [Affects R21][Technical] Detailed handling of the "skip staging cleanup when the old request was downloading" branch — confirm the predicate against `lib/download.py` semantics.
- [Affects R30, R31][Technical] UI affordance for the "show replaced" toggle on Pipeline and Wrong Matches tabs (default off, persisted as a localStorage preference, or always shown collapsed). Frontend concern best resolved with the actual UI in front of the operator.
- [Affects R9][Needs research] When MB upstream moves a release between release groups, the guardrail refuses correctly. Is an operator escape path needed for this rare case, or is "delete and re-add" the expected workaround? Planning can call this based on observed frequency in the live DB.

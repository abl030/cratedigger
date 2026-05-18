---
date: 2026-05-18
topic: replace-operator-action
status: active
source: docs/brainstorms/2026-05-18-replace-operator-action-requirements.md
---

# feat: Replace operator action

## Summary

Add a first-class "Replace" operator action — a service method exposed identically by `pipeline-cli replace` and `POST /api/pipeline/<request_id>/replace`, plus a shared web UI picker on the Browse, Pipeline, and Wrong Matches tabs. Replace supersedes an existing `album_requests` row (status → `replaced`, otherwise frozen) with a new row carrying the operator-selected target MBID from the same release group. The work also drops the long-dead lidarr columns, adds the schema for the supersede pattern, and enshrines two new CLAUDE.md critical invariants ("pipeline self-heals", "don't duplicate convergence"). Pet Grief request 4194 is the dogfood case for success.

---

## Problem Frame

See origin: `docs/brainstorms/2026-05-18-replace-operator-action-requirements.md` § Problem Frame. Three operator workflows currently dead-end with no clean recovery: (a) MBID merged upstream on MusicBrainz, (b) operator picked the wrong pressing originally, (c) Soulseek-only pressing mismatch piling up in wrong-matches. All three need a first-class way to switch the request's target MBID while preserving the audit trail of the deliberate abandonment. The supersede model (vs UPDATE-in-place) was settled in brainstorm via the user's "track the replace and leave it there" invariant: a `replaced` row IS the operator's record that this MBID was deliberately discarded.

---

## High-Level Technical Design

The supersede transition for a single Replace invocation:

```
                Replace(request_id=4194, target=18056805-...)
                              │
        ┌─────────────────────┴─────────────────────┐
        │  Phase 0: validate (read-only)            │
        │  - get source row                         │
        │  - target == current?  (same_as_current)  │
        │  - lazy-backfill source RG if NULL        │
        │  - pre-check target collision             │
        │  - fresh MB lookup of target              │
        │  - release-group match check              │
        └─────────────────────┬─────────────────────┘
                              │
        ┌─────────────────────┴─────────────────────┐
        │  Phase 1: acquire IMPORT lock             │
        │  pg_try_advisory_lock(IMPORT, 4194)       │
        │  contention → wrong_state                  │
        └─────────────────────┬─────────────────────┘
                              │
        ┌─────────────────────┴─────────────────────┐
        │  Phase 2: capture pre-supersede state     │
        │  - old artist/title (for staging path)    │
        │  - old imported_path (for Plex partial)   │
        │  - old release_id (for beet remove)       │
        │  (active_download_state is NOT touched —  │
        │   orphans deferred to issue #278)         │
        └─────────────────────┬─────────────────────┘
                              │
        ┌─────────────────────┴─────────────────────┐
        │  Phase 3: DB txn (autocommit=False)       │
        │  - SELECT ... FOR UPDATE on old row       │
        │  - UPDATE old SET status='replaced'       │
        │  - INSERT new row → new_id                │
        │  - set_tracks(new_id, new_tracks)         │
        │  - COMMIT                                  │
        │  UniqueViolation → target_collision_*     │
        └─────────────────────┬─────────────────────┘
                              │
        ┌─────────────────────┴─────────────────────┐
        │  Phase 4: filesystem cleanup              │
        │  (non-fatal warnings, R26)                │
        │  - if old was imported:                   │
        │      remove_and_reset_release(            │
        │        clear_pipeline_state=False)        │
        │  - delete_wrong_match_group(old_id)       │
        │  - if old was NOT downloading:            │
        │      rmtree old staging paths             │
        └─────────────────────┬─────────────────────┘
                              │
        ┌─────────────────────┴─────────────────────┐
        │  Phase 5: post-cleanup                    │
        │  - SearchPlanService.generate(new_id)     │
        │  - trigger_meelo / plex / jellyfin scan   │
        │  - release advisory lock                  │
        └─────────────────────┬─────────────────────┘
                              │
                ReplaceResult(outcome='replaced',
                              new_request_id=4995,
                              warnings=(...))
```

This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementer should treat it as context, not code to reproduce.

---

## Output Structure

New files this plan creates:

```
lib/
  mbid_replace_service.py             [U4]
migrations/
  022_drop_lidarr_columns.sql         [U1]
  023_add_replace_supersede_schema.sql [U2]
tests/
  test_mbid_replace_service.py        [U4]
web/js/
  replace_picker.js                   [U8]
```

Modified files (illustrative; per-unit `Files:` lists are authoritative):

```
CLAUDE.md, docs/advisory-locks.md     [U12]
lib/pipeline_db.py                    [U3]
lib/release_cleanup.py                [U4]
scripts/pipeline_cli.py               [U5]
tests/fakes.py, helpers.py            [U1, U3]
tests/test_fakes.py                   [U3]
tests/test_integration_slices.py      [U11]
tests/test_pipeline_cli.py            [U5]
tests/test_release_cleanup.py         [U4]
tests/test_web_server.py              [U6, U10]
web/index.html                        [U8, U9, U10]
web/js/{browse,pipeline,wrong-matches,main,release_actions}.js  [U9, U10]
web/routes/pipeline.py                [U6, U10]
```

---

## Key Technical Decisions

- **Service Result type: `@dataclass(frozen=True)`, not `msgspec.Struct`.** Follows the `SearchPlanService` precedent (`lib/search_plan_service.py:230-290`). Inputs are already typed; the route handler hand-shapes the JSON response. `msgspec.Struct` would buy nothing.

- **Advisory lock namespace: existing `ADVISORY_LOCK_NAMESPACE_IMPORT`, keyed by `request_id`.** Same namespace and key as the importer worker (`lib/import_dispatch.py:2159`). A new namespace would lose mutual exclusion — Replace must refuse exactly when the importer holds this lock.

- **DB transaction: single `autocommit=False` block** modeled on `abandon_auto_import_request` (`lib/pipeline_db.py:2671`). Wraps `SELECT ... FOR UPDATE` on the old row, UPDATE-old-status, INSERT-new-row, and INSERT-new-tracks atomically (per F7 from doc review — R25's atomicity invariant means no half-built new row is possible even if track-insert fails mid-flight). `UniqueViolation` caught and mapped to `target_collision_request`.

- **Beets cleanup: compose `lib/release_cleanup.py::remove_and_reset_release` with `clear_pipeline_state=False`.** Same Palo-Santo-safe primitive ban-source already uses, but with a new optional parameter (default `True` preserves ban-source behavior) so Replace can run the beets removal without clearing the OLD request's **characteristic** fields (`verified_lossless`, `current_spectral_*`, `current_lossless_source_v0_probe_*`) — these describe the audio that existed and stay frozen as historical truth. The **pointer** field `imported_path` is a separate concern: it is cleared by the supersede transaction itself (U3), not by this primitive, because Phase 4 deletes the files and a dangling path would lie. Do NOT hand-roll `beet remove -d`. This refactor is part of U4's work.

- **Wrong-matches cleanup: compose `lib/wrong_match_delete_service.py::delete_wrong_match_group(db, old_request_id)`.** Operator-authority path (not the classifier-gated `cleanup_wrong_match` — per the `project_converge_operator_authority` memory).

- **Staging path computation via `lib/processing_paths.py::stage_to_ai_path`** using OLD artist/title captured from the now-frozen old row. Staging is keyed by request_id + artist/title, not MBID — the brainstorm's original R21 wording was wrong; research corrected it.

- **Search plan inline regeneration.** `SearchPlanService.generate_for_request(new_id)` runs after the DB commit. Required: `get_wanted_searchable` excludes rows with NULL `active_plan_id`. Without inline regeneration, Pet Grief 4194's success criterion ("next 5-min cycle re-searches") fails.

- **Rescans synchronous and best-effort.** The three rescan helpers (`lib/util.py:459-591`) each timeout in ~10s and never raise. Total worst case ~40s. Replace blocks on them — no thread pool, no async machinery (cuts in favor of "don't duplicate convergence").

- **Lineage column: `replaces_request_id` on the NEW row.** Newer references older. Indexed for reverse lookup.

- **slskd transfers are NOT cancelled by Replace; orphans wait for #278.** Replace does not touch `active_download_state` or call slskd at all. Earlier draft had inline cancellation, but `transfer_id` is not persisted in the JSONB shape — partial workarounds would duplicate the general convergence at #278. Trade-off accepted: small orphan inventory between now and #278's ship. Operator-rare invocation keeps it small.

- **UI: extend existing `web/js/release_actions.js::renderActionToolbar`** with `renderReplaceButton`. The picker is a NEW shared module `web/js/replace_picker.js` — no existing reusable modal/picker component exists; this is the first.

- **Only 3 button host tabs.** Brainstorm wording said "library, pipeline, wrong-matches, search" but only 5 web tabs exist; "library" and "search" both live in the Browse tab in different modes. Replace surfaces are Browse + Pipeline + Wrong Matches.

---

## Alternatives Considered

- **UPDATE-in-place on `album_requests.mb_release_id`** (Model A). Rejected during brainstorm. Would have been cratedigger's first-ever rewrite of an identity column; would have created confused "row claims new MBID but download_log rows are about the old MBID" history; would have required complex derived-state clearing on every Replace. The supersede model (Model B) avoids all three. See origin doc `## Key Decisions` §1.

- **Separate `request_identity` table** (a third architecture not in the brainstorm's binary). Shape: `album_requests` keeps a stable `request_id` and never mutates it; a separate `request_identity (id, request_id, mb_release_id, valid_from, valid_to, replaced_reason)` table holds the identity history with time bounds. Every consumer that needs current identity joins through this table. Pros: every download_log / search_log / evidence row stays bound to one request_id forever (no lineage chain to traverse); cleaner audit (the identity-table rows ARE the audit trail by construction); no new FK semantics to reason about. Cons: every consumer of `album_requests.mb_release_id` needs a join — `lib/`, `web/`, `harness/` would all need touch-ups; the indirection layer is non-trivial to introduce after a decade of direct-MBID-column reads; identity changes are still mutations (just to a different table). Rejected because the touch surface for retrofitting the indirection is large (estimated 30-50 files), the supersede model is simpler operationally (one row per identity period, no time-range queries needed), and the lineage column + content-addressed evidence already preserves the audit story this approach was meant to formalize. Worth naming so that the second identity-mutating operator action (e.g., artist-rename, label-correction, if either is ever needed) can revisit the trade-off with the supersede experience in hand.

---

## Implementation Units

### U1. Migration 022 — drop lidarr columns

**Goal:** Remove the long-dead `lidarr_album_id` and `lidarr_artist_id` columns from `album_requests` and clean up test infrastructure references.

**Requirements:** R27.

**Dependencies:** None.

**Files:**
- `migrations/022_drop_lidarr_columns.sql` (new)
- `tests/helpers.py` (modify — strip `lidarr_album_id` / `lidarr_artist_id` from `make_request_row` defaults at lines 72-73)
- `tests/fakes.py` (modify — strip `lidarr_album_id` / `lidarr_artist_id` at lines 2013-2014; verified present during review)
- `scripts/migrate_to_postgres.py` (leave references in place — historical one-shot)

**Approach:** Plain `ALTER TABLE album_requests DROP COLUMN lidarr_album_id, DROP COLUMN lidarr_artist_id;` per existing migration style. No defensive `IF EXISTS`. No FK considerations (verified: only `001_initial.sql` declares them).

**Patterns to follow:** `migrations/021_evidence_canonical_rekey.sql` for the header comment style and direct-DDL style.

**Test scenarios:**
- `tests/test_migrator.py::TestMigrator` applies the migration; assert `SELECT column_name FROM information_schema.columns WHERE table_name='album_requests' AND column_name IN ('lidarr_album_id', 'lidarr_artist_id')` returns 0 rows after migration 022.
- Grep `tests/` for any remaining `lidarr_` reference before commit — should be zero.

**Verification:** Migration applies cleanly via `nix-shell --run "python3 -m unittest tests.test_migrator -v"`; no test file references the dropped columns.

---

### U2. Migration 023 — add `replaced` status and `replaces_request_id` column

**Goal:** Add the schema affordances for the supersede pattern: nullable self-referencing `replaces_request_id` column with `ON DELETE RESTRICT` (schema-enforced lineage preservation), and drop+recreate the `album_requests_status_check` CHECK constraint to include the new `'replaced'` value.

**Requirements:** R28.

**Dependencies:** U1 (sequential migration order — U1 is 022, this is 023).

**Files:**
- `migrations/023_add_replace_supersede_schema.sql` (new)

**Approach:**
- `ALTER TABLE album_requests ADD COLUMN replaces_request_id INTEGER REFERENCES album_requests(id) ON DELETE RESTRICT;` — RESTRICT enforces the brainstorm invariant ("replaced rows are never deleted") at the schema level. Deleting a row that still has a descendant pointing at it raises `ForeignKeyViolation` rather than silently severing lineage. To intentionally prune a chain, the operator deletes descendants first (new → old).
- `CREATE INDEX idx_album_requests_replaces_request_id ON album_requests(replaces_request_id) WHERE replaces_request_id IS NOT NULL;`
- Drop and recreate the status CHECK constraint to include `'replaced'`. Use the `DO $$ ... END $$` pattern that `migrations/001_initial.sql:259-263` establishes for this same constraint (keeps the DROP+ADD pair as one atomic block, matches existing code style): `DO $$ BEGIN ALTER TABLE album_requests DROP CONSTRAINT album_requests_status_check; ALTER TABLE album_requests ADD CONSTRAINT album_requests_status_check CHECK (status IN ('wanted', 'downloading', 'imported', 'manual', 'replaced')); END $$;`. The constraint is explicitly named `album_requests_status_check` at `migrations/001_initial.sql:261`; no later migration touches it, so the name is deterministic.

**Patterns to follow:** `migrations/021_evidence_canonical_rekey.sql` for `ALTER TABLE ADD COLUMN` + `CREATE INDEX` style; no `IF EXISTS` per `.claude/rules/pipeline-db.md`.

**Test scenarios:**
- Migration applies; `INSERT INTO album_requests (..., status) VALUES (..., 'replaced')` succeeds.
- `INSERT INTO album_requests (..., replaces_request_id) VALUES (..., 99999999)` where 99999999 doesn't exist → FK violation.
- After inserting a row with `replaces_request_id=N`, deleting row N raises `ForeignKeyViolation` (ON DELETE RESTRICT) — the chain must be deleted descendants-first.
- Deleting a row with NO descendants (a `replaces_request_id` value points at it, but no row points BACK at IT) — deleting succeeds, but if such a deletion happens it has no FK consequences since nothing references it back.
- Bottom-up chain delete works: given r1 → r2 → r3 lineage (`r3.replaces_request_id=r2.id`, `r2.replaces_request_id=r1.id`), `DELETE r3; DELETE r2; DELETE r1;` succeeds in order.

**Verification:** `nix-shell --run "python3 -m unittest tests.test_migrator -v"` passes; `pipeline-cli query "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 3"` shows 022 and 023 after local apply.

---

### U3. PipelineDB primitives and FakePipelineDB mirrors

**Goal:** Add the DB methods Replace needs: atomic supersede transaction, release-group request listing, delete-denylist-for-request, status surfaced in collision-check response. Mirror on FakePipelineDB with self-tests.

**Requirements:** R12, R14, R15, R16, R17, R32, R33.

**Dependencies:** U2.

**Files:**
- `lib/pipeline_db.py` (modify — add `supersede_request_mbid`, `list_requests_in_release_group`; verify `get_request_by_mb_release_id` surfaces `status`)
- `tests/fakes.py` (modify — mirror new methods on FakePipelineDB)
- `tests/test_fakes.py` (modify — self-tests for new fakes)

**Approach:**

`supersede_request_mbid(old_request_id, *, new_mb_release_id, new_mb_release_group_id, new_mb_artist_id, new_artist_name, new_album_title, new_year, new_country, new_tracks: list[dict]) -> int`:
- `with conn: autocommit=False:` (existing pattern at `lib/pipeline_db.py:2671`)
- `SELECT ... FOR UPDATE` on old row
- `UPDATE album_requests SET status='replaced', imported_path=NULL, updated_at=NOW() WHERE id=%s AND status != 'replaced'` — if rowcount=0, raise `SupersedeRaceError`. The `imported_path=NULL` clear is the R14 carve-out: Phase 4 will delete the files at that path, so leaving the column set would create a dangling pointer. Characteristic fields (spectral, V0, verified_lossless) are NOT cleared here — those describe the audio that existed and stay frozen as historical truth.
- `INSERT INTO album_requests (mb_release_id, mb_release_group_id, mb_artist_id, artist_name, album_title, year, country, status, source, replaces_request_id) VALUES (..., 'wanted', <inherit old.source>, %s) RETURNING id`. Capture `new_id`.
- `INSERT INTO album_tracks (request_id, disc_number, track_number, title, length_seconds) VALUES (...)` for each entry in `new_tracks`. Bundled into the same transaction so R25's atomicity invariant holds (UPDATE-old + INSERT-new-row + INSERT-new-tracks succeed or fail together; no half-built new row possible).
- Commit. Return `new_id`.
- Catch `psycopg2.errors.UniqueViolation` (on the INSERT album_requests if target MBID collides) → raise `MbidCollisionError`. Catch any other failure → automatic rollback via context manager; raise to caller.

`list_requests_in_release_group(rg_id, *, exclude_replaced=True, exclude_request_id=None) -> list[dict]`:
- Parameterized SELECT against `album_requests` filtering by `mb_release_group_id`, optionally excluding `status='replaced'` and a specific request_id.
- ORDER BY id DESC.

`get_request_by_replaces_request_id(replaced_id) -> Optional[dict]`:
- `SELECT * FROM album_requests WHERE replaces_request_id = %s LIMIT 1`.
- Used by the Replace service's early-exit branch (U4 Phase 0) when the source row is already `status='replaced'`, to surface the descendant's id in the response so the UI can deep-link to it. The index added by U2 (`idx_album_requests_replaces_request_id`) backs this lookup.
- Returns None when no descendant exists (operator manually broke the chain via SQL despite the RESTRICT FK — should be impossible, but defend defensively).

`list_active_release_group_ids() -> set[str]`:
- `SELECT DISTINCT mb_release_group_id FROM album_requests WHERE status != 'replaced' AND mb_release_group_id IS NOT NULL`.
- Returns a set of UUID strings. Used by the Browse-search Replace button to decide enable state per R7 — frontend builds a Set once on tab mount, then `set.has(row.release_group_id)` per render.
- Result set is small (one row per release-group with active requests; expected hundreds to low thousands at the pipeline DB's scale).

Verify `get_request_by_mb_release_id` returns the row including `status` (it should — it returns the full row via `dict(cursor.fetchone())`).

Mirror on `FakePipelineDB`:
- `supersede_request_mbid` against in-memory request dict; raises `MbidCollisionError` if target MBID exists in any row.
- `list_requests_in_release_group` against the in-memory rows.
- `list_active_release_group_ids` against the in-memory rows (filter `status != 'replaced'`, project to set of RG ids).
- `get_request_by_replaces_request_id` against the in-memory rows (filter by `replaces_request_id` field).
- Self-tests in `tests/test_fakes.py`.

**Patterns to follow:**
- Autocommit=False transaction template: `lib/pipeline_db.py:2671-2727` (`abandon_auto_import_request`).
- FakePipelineDB pattern: `tests/fakes.py:1951` (`update_request_fields`) and surrounding methods.

**Test scenarios (in `tests/test_fakes.py`):**
- `supersede_request_mbid` happy path: old row's status flips to `replaced`, new row has the target MBID, `replaces_request_id` set to old id.
- After supersede, the old row's `imported_path` is NULL (R14 carve-out — Phase 4 deletes the files at that path). Capture the pre-supersede value first and assert NULL after.
- After supersede, all OTHER columns are identical to pre-supersede — assert each: `mb_release_id`, `mb_release_group_id`, `mb_artist_id`, `artist_name`, `album_title`, `year`, `country`, `min_bitrate`, `verified_lossless`, `current_spectral_grade`, `current_spectral_bitrate`, `current_lossless_source_v0_probe_*`, `search_filetype_override`, `target_format`.
- Supersede with target MBID equal to ANOTHER row's MBID raises `MbidCollisionError`.
- Supersede on a row already in `status='replaced'` raises `SupersedeRaceError`.
- `list_requests_in_release_group(rg, exclude_replaced=True)` does not return `replaced` rows.
- `list_requests_in_release_group(rg, exclude_replaced=False)` returns ALL rows in the RG.
- `list_requests_in_release_group(rg, exclude_request_id=N)` excludes row N from results.
- `list_active_release_group_ids()` returns the distinct set of RGs with `status != 'replaced'` rows; excludes `replaced` rows; returns empty set when no active requests exist.
- `get_request_by_replaces_request_id(N)` returns the descendant row when one exists; returns None otherwise.
- New row's `source_denylist` is empty (no rows in fake's denylist with new id).
- Old row's `source_denylist` rows are unchanged (still attached to old id).

**Verification:** Self-tests pass; FakePipelineDB stays interchangeable with PipelineDB.

---

### U4. MbidReplaceService — the core service

**Goal:** The service entrypoint that orchestrates Replace. Composes existing primitives; status-dispatches the fs/beets steps; status-maps every outcome.

**Requirements:** R1, R2, R9–R26.

**Dependencies:** U3.

**Execution note:** Implement test-first. Each outcome branch gets a RED test before its GREEN code. Use `subTest()` tables for the outcome matrix per `.claude/rules/code-quality.md`.

**Files:**
- `lib/mbid_replace_service.py` (new)
- `lib/release_cleanup.py` (modify — add `clear_pipeline_state: bool = True` parameter to `remove_and_reset_release`; default preserves existing ban-source behavior. When `False`, skip the call to `clear_on_disk_quality_fields` so the caller's request row stays frozen)
- `tests/test_mbid_replace_service.py` (new)
- `tests/test_release_cleanup.py` (modify if exists, or new — assert `clear_pipeline_state=True` still clears quality fields; assert `clear_pipeline_state=False` leaves them untouched)

**Approach:**

Module-level outcome constants matching `lib/search_plan_service.py:74-86` style:
- `RESULT_REPLACED = "replaced"`
- `RESULT_NOT_FOUND = "not_found"`
- `RESULT_WRONG_STATE = "wrong_state"`
- `RESULT_TARGET_INVALID = "target_invalid"`
- `RESULT_TARGET_RELEASE_GROUP_MISMATCH = "target_release_group_mismatch"`
- `RESULT_TARGET_SAME_AS_CURRENT = "target_same_as_current"`
- `RESULT_TARGET_COLLISION_REQUEST = "target_collision_request"`
- `RESULT_TRANSIENT = "transient"`

`@dataclass(frozen=True) ReplaceResult`:
- `outcome: str`
- `request_id: int`
- `new_request_id: Optional[int] = None`
- `current_status: Optional[str] = None` — surfaced on `RESULT_TARGET_COLLISION_REQUEST` so the UI can render "MBID held by a {status} request" or the "previously abandoned" warning.
- `descendant_request_id: Optional[int] = None` — surfaced on `RESULT_WRONG_STATE` when the source row is already `status='replaced'`, so the UI can deep-link to the descendant: "This request was already replaced. The new request is at /pipeline/{descendant_request_id}."
- `warnings: tuple[str, ...] = ()`

`class MbidReplaceService` with constructor `(db, config, slskd, beets_db_factory, mb_lookup=None, search_plan_service=None)`. Defaults: `mb_lookup = web.mb.get_release`, `search_plan_service = SearchPlanService(db, config)`.

`replace_request_mbid(request_id, *, target_mb_release_id) -> ReplaceResult`:
1. Load source via `db.get_request(request_id)`. None → `RESULT_NOT_FOUND`.
1a. If `source['status'] == 'replaced'`: the row was already replaced (double-click race, or operator picked the wrong tab and clicked Replace on a frozen audit row). Look up the descendant via `db.get_request_by_replaces_request_id(request_id)` and return `RESULT_WRONG_STATE` with `descendant_request_id` set to the descendant's id (or None if the chain was manually broken). This early-exit makes the second click's UX explicit: "This request was already replaced. New request is at /pipeline/{descendant_request_id}." instead of leaking through as a generic collision or transient.
2. `target == source['mb_release_id']` → `RESULT_TARGET_SAME_AS_CURRENT`.
3. If `source['mb_release_group_id']` is None: lazy-backfill via `mb_lookup(source['mb_release_id'], fresh=True)`. Errors → `RESULT_TARGET_INVALID` (sub-reason source-resolve failure).
4. Pre-check collision: `existing = db.get_request_by_mb_release_id(target_mb_release_id)`. If found → `RESULT_TARGET_COLLISION_REQUEST` with `current_status=existing['status']`.
5. `mb_lookup(target_mb_release_id, fresh=True)`. URLError → `RESULT_TRANSIENT`. Missing required fields (id, release_group_id) → `RESULT_TARGET_INVALID`. RG mismatch with source's RG → `RESULT_TARGET_RELEASE_GROUP_MISMATCH`. If MB's response has `id != target_mb_release_id` (301 redirect to canonical), use the canonical and re-check collision against it.
6. Capture pre-supersede state: `old_artist`, `old_title`, `old_imported_path`, `old_release_id` (= source mb_release_id for MB releases), `old_status`. `active_download_state` is NOT captured or touched — in-flight slskd transfers are intentionally left to orphan per R23 / issue #278.
7. `with db.advisory_lock(ADVISORY_LOCK_NAMESPACE_IMPORT, request_id) as acquired:` — if not acquired, `RESULT_WRONG_STATE`.
8. **Phase 3 — DB transaction**: call `db.supersede_request_mbid(..., new_tracks=target_tracks)`. The method runs UPDATE-old + INSERT-new + INSERT-tracks atomically in one autocommit=False block (per F7 / R25). Catch `MbidCollisionError` → `RESULT_TARGET_COLLISION_REQUEST` (defensive). Catch `SupersedeRaceError` → `RESULT_TRANSIENT`. On success, capture `new_request_id`.
9. **Phase 4 — filesystem cleanup**, collecting warnings:
    - If `old_status == 'imported'`: `release_cleanup.remove_and_reset_release(beets_db, db, old_release_id, request_id, clear_pipeline_state=False)`. The `clear_pipeline_state=False` flag (newly added by this PR — see U4 Files below) skips the call to `clear_on_disk_quality_fields` so the OLD row's **characteristic** fields (`verified_lossless`, `current_spectral_*`, `current_lossless_source_v0_probe_*`) stay frozen as historical truth. The **pointer** field `imported_path` has already been cleared by Phase 3's supersede transaction (the R14 carve-out) since this Phase 4 step deletes the files. Pass `request_id` (the OLD request_id; row now has `status='replaced'`). Selector failures from the returned `ReleaseCleanupResult` → warning strings.
    - `wrong_match_delete_service.delete_wrong_match_group(db, request_id)`. Skipped/errors → warning strings.
    - If `old_status != 'downloading'`: compute `stage_to_ai_path(artist=old_artist, title=old_title, staging_dir=cfg.staging_directory, request_id=request_id, auto_import=True)` and `auto_import=False`. `shutil.rmtree` each that exists. `FileNotFoundError` → silent. Other → warning.
10. **Phase 5 — post-cleanup**:
    - `search_plan_service.generate_for_request(new_request_id, regenerate=False)`. Errors → warning.
    - `trigger_meelo_scan(cfg)`, `trigger_plex_scan(cfg, imported_path=old_imported_path)`, `trigger_jellyfin_scan(cfg)`. All already best-effort internally.
11. Return `ReplaceResult(outcome=RESULT_REPLACED, request_id=request_id, new_request_id=new_request_id, warnings=tuple(warnings))`.

**Patterns to follow:**
- `lib/search_plan_service.py:382` (`advance_for_request`) for service method structure and advisory-lock idiom.
- `lib/release_cleanup.py:183-210` for the beets-cleanup composition.
- `lib/wrong_match_delete_service.py:103-159` for the wrong-matches group-delete composition.
- `tests/helpers.py::patch_dispatch_externals` style for patching external edges during tests.

**Test scenarios (in `tests/test_mbid_replace_service.py`):**

Outcome-matrix `subTest` table covering every outcome string with the minimum scenario that triggers it:
- `RESULT_REPLACED` happy path (status=imported source, valid target in same RG, no in-flight).
- `RESULT_NOT_FOUND`: nonexistent request_id.
- `RESULT_TARGET_SAME_AS_CURRENT`: target equals source's MBID.
- `RESULT_TARGET_RELEASE_GROUP_MISMATCH`: target's RG differs from source's stored RG.
- `RESULT_TARGET_RELEASE_GROUP_MISMATCH` after lazy-backfill: source's stored RG is NULL; MB lookup populates it; mismatch detected against backfilled value.
- `RESULT_TARGET_INVALID`: MB lookup returns payload missing `release_group_id`.
- `RESULT_TARGET_INVALID` source-resolve failure: source's stored RG is NULL AND source MBID lookup fails.
- **Source MBID merged upstream (the Pet Grief 4194 case)**: source row has stored `mb_release_group_id` (no lazy-backfill needed); target MBID is the canonical, and `mb_lookup(target, fresh=True)` returns the canonical body. Live mirror behavior verified during prior `ce-debug` session — the local MB mirror returns HTTP 301 to canonical for merged MBIDs (e.g. `72988560-...` → `18056805-...`), and `urllib.request.urlopen` follows the redirect transparently, so `data["id"]` is the canonical ID. Assert outcome `RESULT_REPLACED`.
- **Source MBID merged upstream WITH NULL stored RG (legacy edge)**: source row's `mb_release_group_id` is NULL; lazy-backfill via `mb_lookup(source['mb_release_id'], fresh=True)` follows the 301 redirect and gets the canonical release's RG. Assert outcome `RESULT_REPLACED`.
- `RESULT_TRANSIENT`: MB lookup raises URLError.
- `RESULT_TARGET_COLLISION_REQUEST` pre-check: another active request holds the target.
- `RESULT_TARGET_COLLISION_REQUEST` with `current_status='replaced'`: target was previously abandoned.
- `RESULT_TARGET_COLLISION_REQUEST` defensive: `supersede_request_mbid` raises `MbidCollisionError`.
- `RESULT_WRONG_STATE` (lock contention): arrange `FakePipelineDB.set_advisory_lock_result` to refuse the IMPORT-namespace+request_id key (see `tests/fakes.py:719`; the API takes either a bool or a callable, not a per-namespace 3-arg form). Assert `descendant_request_id` is None in this case.
- `RESULT_WRONG_STATE` (source already replaced — F9 double-click): seed `FakePipelineDB` with a source row at `status='replaced'` and a descendant row pointing back via `replaces_request_id`. Assert outcome is `RESULT_WRONG_STATE`, `descendant_request_id` equals the descendant's id, no DB mutation, no filesystem call.
- `RESULT_WRONG_STATE` (source already replaced, descendant missing — defensive): seed a `replaced` source row with no descendant (chain manually broken via SQL despite RESTRICT). Assert outcome is `RESULT_WRONG_STATE`, `descendant_request_id` is None, no DB mutation.
- `RESULT_TRANSIENT` race: `supersede_request_mbid` raises `SupersedeRaceError` (row already replaced).

Status-dispatch coverage (per-`old_status` scenario):
- `old_status='wanted'`: no beets removal, no slskd cancel; staging cleanup runs.
- `old_status='downloading'`: slskd is NOT called; old row's `active_download_state` preserved untouched; staging cleanup skipped (mid-write risk); warning logged about the orphaned transfer for visibility.
- `old_status='imported'`: beets removal called, then `clear_on_disk_quality_fields` already part of the primitive; staging cleanup runs.
- `old_status='manual'`: like `wanted`; old `manual_reason` stays on frozen row.

Ordering invariants:
- `supersede_request_mbid` called BEFORE any filesystem helper. Covers R25.
- `remove_and_reset_release` (when called) is always called with `clear_pipeline_state=False` — assert via mock call arguments. Regression guard so a future maintainer doesn't accidentally clear the OLD row's quality fields.
- Old row's **characteristic** fields (`verified_lossless`, `current_spectral_*`, `current_lossless_source_v0_probe_*`) are identical pre- vs post-Replace — assert via DB read in the `old_status='imported'` scenario.
- Old row's `imported_path` is NULL post-Replace (R14 carve-out — was set pre-Replace, cleared by the supersede transaction because Phase 4 deletes the files).
- slskd module is never called during Replace (R23 — orphan deferred to #278). Assert mock slskd's call list is empty across every scenario.
- All three rescan triggers called AFTER fs cleanup.
- `search_plan_service.generate_for_request(new_request_id)` called after DB commit.

Warning surface:
- Mock `delete_wrong_match_group` returns summary with errors → `warnings` tuple includes the error string; outcome still `RESULT_REPLACED`.
- Mock `release_cleanup.remove_and_reset_release` returns `selector_failures=[...]` → warning surfaced; outcome still `RESULT_REPLACED`.
- Mock `shutil.rmtree` raises `PermissionError` → warning surfaced; outcome still `RESULT_REPLACED`.

AE coverage:
- `Covers AE1.` RG mismatch refuses (already in matrix).
- `Covers AE2.` Collision returns `current_status` (already in matrix).
- `Covers AE3.` Importer-held lock returns `wrong_state` (already in matrix).
- `Covers AE5.` In-flight slskd transfer is NOT cancelled; old row's `active_download_state` preserved unchanged; new row has empty `active_download_state`.

**Verification:** All outcome branches green; coverage report shows every public method exercised; `FakePipelineDB` exercised throughout; no real subprocess / network calls.

---

### U5. `pipeline-cli replace` subcommand

**Goal:** CLI wrapper around `MbidReplaceService.replace_request_mbid`.

**Requirements:** R1, R2.

**Dependencies:** U4.

**Files:**
- `scripts/pipeline_cli.py` (modify — add `cmd_replace`; argparse declaration; exit-code mapping)
- `tests/test_pipeline_cli.py` (modify — exit-code mapping tests)

**Approach:** Add argparse subcommand `replace request_id --to <mbid>` with both args required. Construct `MbidReplaceService` from the CLI's existing factories (config, db, slskd, beets_db). Call `replace_request_mbid`. Map outcome → exit code per R2 (matches `cmd_search_plan_advance` at `scripts/pipeline_cli.py:1778-1788`):
- `RESULT_REPLACED` → 0
- `RESULT_NOT_FOUND` → 2
- `RESULT_TARGET_INVALID`, `RESULT_TARGET_RELEASE_GROUP_MISMATCH`, `RESULT_TARGET_SAME_AS_CURRENT` → 3
- `RESULT_WRONG_STATE`, `RESULT_TARGET_COLLISION_REQUEST` → 4
- `RESULT_TRANSIENT` → 5

Argparse declaration alongside the existing subparsers near `scripts/pipeline_cli.py:1948+`. Use `type=lambda s: s` for `--to` (UUID validation deferred to the service; CLI rejects only via argparse's required-arg check).

**Patterns to follow:** `scripts/pipeline_cli.py:1716-1788` (`cmd_search_plan_advance`).

**Test scenarios:**
- Exit code 0 on `RESULT_REPLACED`.
- Exit code 2 on `RESULT_NOT_FOUND`.
- Exit code 3 on each of the three 422-mapped outcomes via subTest table.
- Exit code 4 on each of the two 409-mapped outcomes.
- Exit code 5 on `RESULT_TRANSIENT`.
- argparse rejects missing `--to`.
- `--json` flag (if existing CLI pattern uses it) prints `ReplaceResult` as JSON.

**Verification:** Each outcome maps to the right exit code via unit tests against a mocked service.

---

### U6. API routes — `POST /api/pipeline/<id>/replace`, `GET /api/pipeline/requests-by-rg/<rg_id>`, `GET /api/pipeline/active-rgs`

**Goal:** Web API exposure of the service, plus the two auxiliary endpoints the Browse-search inverted-click button + picker depend on (per-RG listing for the picker dialog, bulk active-RGs set for the per-row button enable state per R7).

**Requirements:** R1, R2, R3, R7, R32, R33.

**Dependencies:** U4.

**Files:**
- `web/routes/pipeline.py` (modify — add `post_pipeline_replace`, `get_pipeline_requests_by_rg`. Note: `post_pipeline_add` already returns `current_status` on the `exists` branch at lines 880, 928 — verified during review — so no backend change is required for R33; only the R33 contract test in `tests/test_web_server.py` is new work)
- `tests/test_web_server.py` (modify — contract tests + `CLASSIFIED_ROUTES` entries)

**Approach:**

`post_pipeline_replace(h, body, req_id_str)`:
- Validate body has `target_mb_release_id` as a non-empty string. 400 if missing.
- Call `MbidReplaceService.replace_request_mbid(int(req_id_str), target_mb_release_id=body['target_mb_release_id'])`.
- Map outcome to HTTP status:
  - `RESULT_REPLACED` → 200 with body `{outcome, request_id, new_request_id, warnings}`
  - `RESULT_NOT_FOUND` → 404
  - `RESULT_WRONG_STATE`, `RESULT_TARGET_COLLISION_REQUEST` → 409 (response body includes `current_status` for collision, `descendant_request_id` for source-already-replaced)
  - `RESULT_TARGET_INVALID`, `RESULT_TARGET_RELEASE_GROUP_MISMATCH`, `RESULT_TARGET_SAME_AS_CURRENT` → 422
  - `RESULT_TRANSIENT` → 503
  - Unknown → 500 (defensive)

`get_pipeline_requests_by_rg(h, params, rg_id)`:
- Call `db.list_requests_in_release_group(rg_id, exclude_replaced=True)`.
- Return `{requests: [{id, mb_release_id, mb_release_group_id, status, artist_name, album_title}, ...]}`.

`get_pipeline_active_rgs(h, params)`:
- Call `db.list_active_release_group_ids()`.
- Return `{release_group_ids: [<rg-uuid>, ...]}`. The frontend builds a Set from this list and uses `.has(row.release_group_id)` per Browse-search row to decide if the Replace button is enabled. Backs R7's "button is enabled only when an existing non-replaced request in the same release group exists" guarantee.

`post_pipeline_add` already returns `current_status` on the `exists` branch (`web/routes/pipeline.py:880, 928` — verified during review). R33 needs only a contract test, no backend change.

Register routes via the module-level pattern tables in `web/routes/pipeline.py` (`GET_PATTERNS` near line 1552, `POST_PATTERNS` near line 1572 — verified during review; the `_FUNC_*` names live on `Handler` in `web/server.py` and are not what the route module exports):
- `r"^/api/pipeline/(\d+)/replace$"` → `post_pipeline_replace`
- `r"^/api/pipeline/requests-by-rg/([a-f0-9-]{36})$"` → `get_pipeline_requests_by_rg`
- `r"^/api/pipeline/active-rgs$"` → `get_pipeline_active_rgs`

Add all three patterns to `CLASSIFIED_ROUTES` at `tests/test_web_server.py:963`.

**Patterns to follow:** `web/routes/pipeline.py:705-800` (`post_pipeline_search_plan_advance`).

**Test scenarios (in `tests/test_web_server.py`):**

Per-outcome status mapping for POST `/api/pipeline/<id>/replace`:
- 200 on `RESULT_REPLACED` with all `REQUIRED_FIELDS` (outcome, request_id, new_request_id, warnings).
- 404 on `RESULT_NOT_FOUND`.
- 409 on `RESULT_WRONG_STATE` (lock contention) — response body does NOT include `descendant_request_id`.
- 409 on `RESULT_WRONG_STATE` (source already replaced) — response body includes `descendant_request_id` pointing at the descendant; covers F9 double-click case.
- 409 on `RESULT_TARGET_COLLISION_REQUEST` with `current_status` field in body.
- 422 on `RESULT_TARGET_INVALID`.
- 422 on `RESULT_TARGET_RELEASE_GROUP_MISMATCH`.
- 422 on `RESULT_TARGET_SAME_AS_CURRENT`.
- 503 on `RESULT_TRANSIENT`.
- 400 on missing/empty `target_mb_release_id` in body (input validation runs before service).

GET `/api/pipeline/requests-by-rg/<rg_id>`:
- 200 with list of non-replaced requests in the RG.
- Empty list when no matching requests exist.
- Response shape includes `REQUIRED_FIELDS = {'id', 'mb_release_id', 'status', 'artist_name', 'album_title'}` for the picker frontend.
- Does NOT include `replaced` rows.

GET `/api/pipeline/active-rgs`:
- 200 with `{release_group_ids: [<uuid>, ...]}`.
- Returns the distinct set of `mb_release_group_id` values across non-replaced requests.
- Empty list when no active requests exist.
- Excludes `replaced` rows.
- Excludes rows with NULL `mb_release_group_id`.

POST `/api/pipeline/add` with `current_status`:
- When the MBID exists with `status='wanted'`, response includes `current_status='wanted'`.
- When the MBID exists with `status='replaced'`, response includes `current_status='replaced'`. Covers R33.

`TestRouteContractAudit`: all three new routes appear in `CLASSIFIED_ROUTES`; audit passes.

**Mock shape discipline:** per `docs/solutions/testing/contract-test-mocks-must-mirror-production-shape.md`, the mock `album_requests` rows include real `datetime.datetime` for timestamps, real UUID strings, real JSONB values for `active_download_state`. At least one scenario uses production-shaped rows.

**Verification:** Audit passes; all status mappings exercised; production-shaped mocks present.

---

### U8. Web UI — replace picker module

**Goal:** New shared modal component handling both standard and inverted click models. Fetches release-group siblings, lets the operator pick a target (or which existing request to rebind, for inverted mode), shows the confirmation dialog.

**Requirements:** R5, R6, R7, R8.

**Dependencies:** U6.

**Files:**
- `web/js/replace_picker.js` (new)
- `web/index.html` (modify — add modal container div near existing modals/overlays)

**Approach:** ES6 module with `// @ts-check` exporting `openReplacePicker({sourceRequestId?, targetMbid?, releaseGroupId, source})` returning `Promise<{outcome: 'confirmed', sourceRequestId, targetMbid} | {outcome: 'cancelled'}>`.

Two modes dispatched on input shape:

**Standard mode** (`sourceRequestId` given, `targetMbid` not):
1. Fetch RG siblings via the local MB mirror through an existing route. (If a route doesn't exist for "list releases in a release group," add a slim helper in `web/routes/browse.py` or similar — but verify first; `web/mb.py::get_release_group_releases` exists and is the right helper, just needs a route wrapper if not yet exposed.)
2. Render a list of pressings (id, title, year, country, label, track count).
3. Operator clicks one → confirm dialog → resolve with `{outcome: 'confirmed', sourceRequestId, targetMbid: clickedId}`.

**Inverted mode** (`targetMbid` given, `sourceRequestId` not):
1. Fetch `GET /api/pipeline/requests-by-rg/<releaseGroupId>`.
2. If 0 results, render an error ("no existing request in this release group"). The button should not have been enabled in this case — handled in U9 — but defend defensively.
3. If 1 result, skip the picker stage; go straight to confirm with that request preselected.
4. If 2+, show "which request to replace?" list → operator picks → confirm.

Picker mode signaling — each mode renders an explicit header so the operator knows which inverse flow they're in:
- **Standard mode** (source row IS the request being replaced): header *"Switch {old_artist} — {old_album} to a different pressing"*, subtitle *"Pick the pressing you want instead. The current request will be marked `replaced` and a new request will be created."*
- **Inverted mode** (Browse-search, row IS the new MBID): header *"Use this pressing to replace an existing request"*, subtitle *"Pick which request in this release group should be replaced with {target_artist} — {target_album} ({year}, {country})."*

Confirmation dialog content (R8): summarises destructive scope and the new identity. The dialog wording must match actual Replace semantics — specifically: in-flight Soulseek transfers are NOT cancelled (per R23 they orphan; cleanup deferred to issue #278). Suggested copy: *"Replace request #{id}? The current request will be marked `replaced` (frozen for audit). The library entry (if imported), wrong-matches folders, and staging folders for this request will be deleted. A new request will be created targeting {target MBID} ({artist} — {album}, {year}, {country}). Any in-flight Soulseek transfers for the old request are left running; their landed files become orphans cleaned up by future convergence work (issue #278)."* Two buttons: "Replace" (destructive styling) and "Cancel" (default). Generic text, no service-computed dry-run.

Module exports `window.openReplacePicker` for cross-module onclick handlers.

**Patterns to follow:**
- ES6 module + `// @ts-check` + JSDoc per `.claude/rules/web.md`.
- The Browse tab's add-pressing flow as the stylistic reference for the pressings list rendering.

**Test scenarios:**
- Pure unit tests (in `tests/test_js_util.mjs` or sibling): pressings-list HTML output given a sample input; confirmation-dialog content given a sample new-identity object; outcome promise resolution shapes.
- Manual Playwright smoke against `music.ablz.au` (or local dev server): picker opens, lists pressings for Pet Grief's RG, selecting `18056805-...` triggers confirmation.

**Verification:** Picker opens and renders correctly; chosen MBID flows through the Promise.

---

### U9. Web UI — Replace button integration across tabs

**Goal:** Wire the Replace button into the three tab surfaces (Browse, Pipeline, Wrong Matches), composing `renderReplaceButton` in the existing release-actions toolbar.

**Requirements:** R4, R6, R7.

**Dependencies:** U8.

**Files:**
- `web/js/release_actions.js` (modify — add `renderReplaceButton(row, opts)` returning button HTML; export)
- `web/js/main.js` (modify — bind `window.openReplacePicker`)
- `web/js/browse.js` (modify — call `renderReplaceButton` on library-mode rows and search-mode rows; inverted-click enable/disable logic for search mode)
- `web/js/pipeline.js` (modify — call `renderReplaceButton` on every non-replaced row)
- `web/js/wrong-matches.js` (modify — call `renderReplaceButton` on each rejection row)

**Approach:**

`renderReplaceButton(row, opts={mode, enabled})`:
- Standard mode: `onclick="window.openReplacePicker({sourceRequestId: <id>, releaseGroupId: <rg>, source: '<tab-name>'})"`.
- Inverted mode (Browse-search): `onclick="window.openReplacePicker({targetMbid: <mbid>, releaseGroupId: <rg>, source: 'browse-search'})"`. Button disabled (`<button disabled>`) when `opts.enabled === false`.

Browse-search inverted-click enable logic — proper enable/disable per R7 and AE6. On Browse tab mount (or first search), fire `GET /api/pipeline/active-rgs` once and build a `Set<string>` of release-group IDs. Per rendered search-result row, `renderReplaceButton(row, {mode: 'inverted', enabled: activeRgSet.has(row.release_group_id)})`. The set is cached for the lifetime of the Browse session (refetched on add/replace mutations that change the active set). Disabled buttons render with `<button disabled>` styling so the affordance communicates "nothing to replace here" without requiring a click.

**Patterns to follow:**
- `web/js/release_actions.js` for the toolbar render pattern.
- ES6 module + `window.*` binding per `.claude/rules/web.md`.

**Test scenarios:**
- Pure unit tests in `tests/test_js_util.mjs` for `renderReplaceButton`: standard mode HTML, inverted mode enabled HTML, inverted mode disabled HTML (asserts `disabled` attribute is present when `enabled: false`).
- Pure unit test for the active-RG Set computation: given a `release_group_ids` array, `Set.has(rg)` returns true/false correctly.
- `Covers AE6.` Integration assertion (Playwright): given an MB search result for an RG with no matching active request, the Replace button is rendered with `disabled` attribute. When a request is added for that RG via the standard add flow, the button becomes enabled on re-render.
- Manual Playwright smoke: button appears on every release row in each of the 3 tabs.
- Manual Playwright smoke: clicking Replace in Pipeline tab on Pet Grief 4194 opens the picker showing Pet Grief release-group siblings; selecting `18056805-...` triggers confirm.

**Success-state UX after 200 response.** When the Replace POST returns 200, the source tab fires a refetch (Pipeline / Wrong Matches / Browse list-fetch endpoints) so the old row disappears from the default-filtered list and the new row surfaces. A toast renders: *"Replaced — new request #{new_request_id} for {artist} — {album}."* with a click-through to `/pipeline/{new_request_id}`. Warnings from the response (non-fatal fs cleanup failures, R26) render in the toast or an expandable detail row. After 409 with `descendant_request_id` set (per F9 / AE8), the toast renders: *"This request was already replaced. New request is at /pipeline/{descendant_request_id}."* with the deep-link.

**Verification:** Button visible and correctly enabled/disabled across tabs per R7/AE6; click integration works end-to-end with U8; success toast surfaces new_request_id; AE8 deep-link surfaces on already-replaced-source clicks.

---

### U10. Web UI — status filters for `replaced` rows

**Goal:** Pipeline and Wrong Matches tabs hide `replaced` rows by default with an opt-in toggle. Add-flow surfaces the "previously abandoned" warning for replaced-MBID re-adds.

**Requirements:** R30, R31, R33.

**Dependencies:** U6.

**Files:**
- `web/routes/pipeline.py` (modify — pipeline-list and wrong-matches-list endpoints accept `include_replaced` query param; extend `post_pipeline_add` `exists` response with `descendant_request_id` + `descendant_status` when the existing row is `status='replaced'`)
- `web/js/pipeline.js` (modify — default filter, show-replaced toggle, fetch parameter)
- `web/js/wrong-matches.js` (modify — filter rows by parent request status; show-replaced toggle)
- `web/js/browse.js` (modify — when add-flow response has `current_status='replaced'`, render the "previously abandoned" warning with lineage forward-link to the descendant)

**Approach:** Each list-fetch endpoint takes a new `include_replaced` query param defaulting to `false`. Frontend wires a checkbox/toggle that triggers a refetch with the param flipped. The toggle's state is persisted in localStorage so the operator's preference survives navigations.

For the add-flow warning: when the response has `current_status='replaced'`, fetch the descendant via `GET /api/pipeline/by-replaces/<replaced_id>` (or include `descendant_request_id` in the `exists` response — see below) and render the warning with the lineage forward-link surfaced. Suggested copy: *"This MBID was previously abandoned via Replace. The current active request for this release group is /pipeline/{descendant_request_id} ({descendant_status})."* Plus a secondary link: *"View the abandoned row's audit"* → `/pipeline/{replaced_id}?include_replaced=true`. The warning is **actionable** (operator can navigate to the active descendant or audit the abandonment), not a dead-end stop sign. To avoid an extra round-trip, extend the `post_pipeline_add` `exists` response with `descendant_request_id` + `descendant_status` when the existing row is `status='replaced'` — the data is cheap (reverse-lookup via the indexed `replaces_request_id` column).

**Patterns to follow:** Existing filter conventions in `web/js/pipeline.js`; localStorage preference patterns if existing UI already uses them.

**Test scenarios:**
- Backend: pipeline list endpoint with `?include_replaced=false` (default) excludes `replaced` rows.
- Backend: with `?include_replaced=true`, includes them.
- Backend: wrong-matches list endpoint filters rows whose parent request is `replaced`.
- Backend: `post_pipeline_add` with an existing `status='replaced'` row returns `current_status='replaced'`, `descendant_request_id={new_id}`, `descendant_status={status}` so the frontend can render the lineage forward-link.
- Frontend: pure HTML rendering test for the "previously abandoned" warning component — assert it surfaces both the descendant forward-link AND the audit-row link.
- Frontend: localStorage persistence test for the toggle (`tests/test_js_util.mjs`).

**Verification:** Default views hide replaced rows; toggle works; add-flow surfaces the warning.

---

### U11. Integration slice — full Pet Grief dogfood path

**Goal:** End-to-end test exercising the Replace flow with real path helpers, real `delete_wrong_match_group`, real `set_tracks`, against a `FakePipelineDB`, mocked MB mirror, mocked beets removal, and temp filesystem.

**Requirements:** R14–R23, AE4.

**Dependencies:** U4.

**Files:**
- `tests/test_integration_slices.py` (modify — add `TestReplaceFullPath`)

**Approach:** Model on `TestForceImportSlice` at `tests/test_integration_slices.py:1259`. For each scenario:
- Seed a `FakePipelineDB` with Pet-Grief-4194-like data (status varies per scenario, MBID=72988560-..., RG=<known rg>, denylist rows, wrong-matches rows in `download_log`).
- Construct a `tmp_path` filesystem with mock `/Incoming` staging folders matching `stage_to_ai_path` output for the seeded artist/title.
- Construct an `MbidReplaceService` with the FakePipelineDB, a `FakeSlskdAPI`, a mocked `beets_db_factory` (returns object whose `locate` returns `kind='exact'` for the old MBID), and a mocked MB lookup function returning the target release in the same release group.
- Patch `subprocess.run` for the `beet remove` calls (used by `remove_album_by_selectors`).
- Patch the three rescan triggers to assert they're called.
- Run `replace_request_mbid(4194, target_mb_release_id='18056805-...')` and assert the full post-state.

Assertions for AE4:
- `outcome == RESULT_REPLACED`.
- Old request 4194 has `status='replaced'`; all OTHER columns unchanged.
- New request exists with `mb_release_id='18056805-...'`, `status='wanted'`, `replaces_request_id=4194`.
- New request has tracks populated; empty denylist.
- Old request's `download_log` rows still present; still reference 4194.
- Old request's staging folders no longer exist on tmp_path.
- Beets removal subprocess invoked for the old MBID.
- All three rescan triggers called.
- SearchPlanService called for the new request id.

**Patterns to follow:**
- `tests/test_integration_slices.py::TestForceImportSlice` (file:1259) for slice structure.
- `tests/helpers.py::patch_dispatch_externals` for the patches pattern.

**Test scenarios:**
- Full happy path (AE4) for `old_status='imported'`.
- `old_status='wanted'` variant: no beets removal, slskd not called.
- `old_status='downloading'` variant: slskd NOT called; old row's `active_download_state` preserved; staging cleanup skipped; warning logged about the orphaned transfer.
- `old_status='manual'` variant.
- `Covers AE4.`

**Verification:** Slice exercises ~80% of Replace's real code paths through one test class.

---

### U12. CLAUDE.md invariants and advisory-locks doc update

**Goal:** Land the two new critical invariants, update the `beet remove -d` rule exception, update `docs/advisory-locks.md` to note Replace as a new caller of `ADVISORY_LOCK_NAMESPACE_IMPORT`.

**Requirements:** R34, R35.

**Dependencies:** None (recommended last — invariants should describe shipped behavior).

**Files:**
- `CLAUDE.md` (modify — add invariants 6 and 7 in Critical invariants section near line 19; modify Critical rule #1 near line 441)
- `docs/advisory-locks.md` (modify — add Replace to the IMPORT-namespace call-site table per the doc's "Extending" §5 requirement)

**Approach:**

In `CLAUDE.md` Critical invariants, after the existing 5 items, add:

> 6. **The pipeline self-heals — the request is the source of truth, everything else is derived.** Files, beets entries, wrong-matches folders, search plans, denylist, overrides, evidence — all derived state. Operator actions that touch identity supersede the row rather than mutate it, and let the pipeline rebuild from the new row. Audit trail (the frozen old row and its content-addressed child rows) is preserved by virtue of the old row never being mutated or deleted.
>
> 7. **Don't duplicate convergence — reuse the cleanup paths that already exist.** When an operator action could leave behind orphans (in-flight slskd transfers, stale staging, dangling rows), prefer letting existing convergence pick them up over adding bespoke teardown to the action. Where convergence does not yet exist, file an issue and ship the closest direct cleanup in the action itself.

Update Critical rule #1 from:

> 1. **NEVER use `beet remove -d`** — deletes files permanently (exception: ban-source endpoint, explicit user action).

To:

> 1. **NEVER use `beet remove -d`** — deletes files permanently (exceptions: ban-source endpoint and Replace action, both explicit user actions composed via `lib/release_cleanup.py::remove_and_reset_release`).

In `docs/advisory-locks.md`, append a row to the IMPORT-namespace call-site table identifying `lib/mbid_replace_service.py::MbidReplaceService.replace_request_mbid` as a caller acquiring `(IMPORT, request_id)` for mutual exclusion with the importer worker.

**Test expectation: none — documentation changes.**

**Verification:** Grep `CLAUDE.md` for the new invariant text; grep `docs/advisory-locks.md` for the new call-site row.

---

## Risks and Mitigations

- **First-of-its-kind supersede pattern in cratedigger.** Mitigation: Model B avoids mutating `mb_release_id` (the most precedent-setting risk); integration slice (U11) and the per-outcome service test matrix (U4) cover behavior end-to-end. Pet Grief dogfood is the live verification.
- **MB-mirror 301 redirect on merged MBIDs is the central use case.** Mitigation: `web/mb.py::get_release` is the existing helper and is already used by the add flow with `fresh=True`; Replace reuses the same pattern. The R10 outcome explicitly handles 301-to-different-MBID via re-check against canonical.
- **`beet remove -d` is the Palo Santo data-loss surface.** Mitigation: Replace composes `lib/release_cleanup.py::remove_and_reset_release` rather than hand-rolling. Same primitive ban-source uses.
- **slskd transfers are intentionally orphaned by Replace.** Replace does not cancel in-flight transfers or modify `active_download_state`; the orphan inventory waits for general convergence at issue #278. Trade-off: small inventory of orphan transfers + landed files between now and #278's ship. Operator-rare invocation keeps inventory small.
- **Migration unit blocks app startup on failure.** Mitigation: pg_dump backup before deploy; run `nix-shell --run "python3 -m unittest tests.test_migrator -v"` locally first; lidarr drop has no FKs; 023 only adds a column (low-risk).
- **Race window: Replace fires between Phase 2 search submission and `set_downloading_if_plan_current`.** Mitigation: verified during review — `set_downloading_if_plan_current` already includes `status = 'wanted'` in its WHERE clause (`lib/pipeline_db.py:2347`), so a row that Replace has flipped to `status='replaced'` cannot be transitioned to `downloading` by a stale cycle. No code change needed.
- **Picker→POST TOCTOU window** (operator opens picker, source row's status drifts between fetch and POST). Accepted trade-off: Replace acts on `request_id`, not on a snapshot of status at picker time. For single-operator homelab use this is operator-rare and self-correcting (Phase 0 step 1a's already-replaced early-exit + lock contention + collision branches together cover the realistic drift paths). No idempotency token needed; advisory lock + plan-aware downstream serialization handle the concurrency that matters.
- **WMCL contention during wrong-matches cleanup.** Accepted trade-off: if a concurrent classifier-gated `cleanup_wrong_match` is mid-flight on a source the operator-authority `delete_wrong_match_group` wants, the latter returns `OUTCOME_SKIPPED_LOCKED` per source. Replace's Phase 4 surfaces this as a warning. After Replace completes, R31's filter hides the old request's wrong-matches rows from the default Wrong Matches tab view; operator can flip the "show replaced" toggle to find leftover folders if needed. The orphan folder is harmless under self-heal but worth knowing about.

---

## Deployment Notes

1. Backup: `ssh doc2 'pg_dump -h 192.168.100.11 -U cratedigger cratedigger' > /tmp/cratedigger_backup_$(date +%Y%m%d_%H%M%S).sql`.
2. Push the feature branch.
3. On doc1: `cd ~/nixosconfig && nix flake update cratedigger-src && git add flake.lock && git commit -m "cratedigger: replace operator action" && git push`.
4. On doc2: `sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh`.
5. Verify migrations applied: `ssh doc2 'pipeline-cli query "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 5"'` — expect 022 and 023 present.
6. Verify cratedigger-web restarted: `ssh doc2 'sudo systemctl is-active cratedigger-web'`.
7. Verify config rendering: `ssh doc2 'grep -c lidarr /var/lib/cratedigger/config.ini || true'` — should be 0 references.
8. **Dogfood Pet Grief**: open `music.ablz.au` → Pipeline tab → find request 4194 → click Replace → pick `18056805-...` → confirm.
9. Follow logs: `ssh doc2 'sudo journalctl -u cratedigger -u cratedigger-web -u cratedigger-importer -f'`.
10. Within 5 minutes: verify new request appears (e.g., 4995) with `status='downloading'`, then `imported`. Verify Meelo and Plex show the album.
11. Confirm: `ssh doc2 'pipeline-cli query "SELECT id, status, mb_release_id, replaces_request_id FROM album_requests WHERE id IN (4194, <new_id>)"'` — request 4194 has `status='replaced'`, otherwise unchanged; new row has correct identity and lineage.

---

## Success Criteria

(See origin: `docs/brainstorms/2026-05-18-replace-operator-action-requirements.md` § Success Criteria.)

- Pet Grief 4194 self-heals end-to-end via the new action (operator click → supersede → search → download → import → Meelo/Plex visible). Request 4194 stays `status='replaced'` with row otherwise unchanged.
- Logs show the complete trace: identity transition → fs cleanup → search-plan regenerate → search → download → validation success → import → rescan. No manual SQL or filesystem intervention.
- All service-test outcome branches green (U4). The integration slice (U11) covers AE4.
- No `album_requests.lidarr_*` columns in the schema after deploy.
- CLAUDE.md contains both new invariants + rule #1 exception (U12).
- `tests/test_web_server.py::TestRouteContractAudit` includes both new routes (U6).

---

## Scope Boundaries

(See origin: `docs/brainstorms/2026-05-18-replace-operator-action-requirements.md` § Scope Boundaries.)

### Deferred to Follow-Up Work

- General slskd-orphan convergence (`find_slskd_orphans`) — tracked at issue #278. Replace ships WITHOUT in-flight transfer handling; orphaned transfers wait for #278 to clean them up.
- Bulk migration of existing stuck requests — Pet Grief 4194 is dogfood; operators triage one at a time.
- Operator escape for cross-release-group MB upstream moves — rare; "delete and re-add" is the documented workaround.

---

## Dependencies / Assumptions

(See origin: `docs/brainstorms/2026-05-18-replace-operator-action-requirements.md` § Dependencies / Assumptions.)

Additionally:
- The status column on `album_requests` DOES carry a CHECK constraint enumerating allowed values (`migrations/001_initial.sql:52-53` declares `CHECK(status IN ('wanted', 'downloading', 'imported', 'manual'))`, re-asserted at line 260). U2 MUST drop and recreate this constraint to include `'replaced'`; without that, every `status='replaced'` write fails with `CheckViolation` and the supersede transaction rolls back.
- `set_downloading_if_plan_current` includes a status predicate that prevents a replaced row from being transitioned to `downloading` by a stale in-flight cycle. To be verified during U4 implementation; if not present, U4 adds a defensive check.
- `web/mb.py::get_release` follows HTTP 301 redirects transparently and returns the canonical release ID in `data['id']`. To be verified at U4 implementation; if not, U4 grows a thin redirect-aware wrapper.

---

## Outstanding Questions

### Deferred to Planning — Resolved

These items from the origin doc's "Deferred to Planning" list have positions taken in this plan:

- **UI placement/styling per tab** → Resolved: extend `web/js/release_actions.js::renderActionToolbar` with `renderReplaceButton`; exact icon/label/position is a UI iteration concern best handled with a screenshot loop during `ce-work`, but the structural decision is locked.
- **Confirmation dialog: dry-run preview or generic summary** → Resolved: generic summary. Reasoning: R26 makes filesystem cleanup non-fatal; a dry-run preview adds a round-trip per click for marginal operator benefit.
- **Filesystem path resolution** → Resolved: use `lib/processing_paths.py::stage_to_ai_path` with old artist/title captured from the frozen row; wrong-matches paths come from JSONB via `delete_wrong_match_group`.
- **Cross-RG MB upstream move escape** → Resolved: no escape; documented as "delete and re-add" in Scope Boundaries.

### Deferred to Implementation

- ~~Affects U4: transfer-id extraction from `active_download_state.files`~~ — moot; Replace no longer touches active_download_state or slskd transfers (R23 / issue #278).
- ~~Affects U9: bulk vs always-enabled enable-state trade-off~~ — resolved: ship `GET /api/pipeline/active-rgs` in U6 so R7/AE6 are met as written. Browse-search button enable state is computed from the cached Set.
- [Affects U6][Technical] Whether to expose a thin route wrapper for `web/mb.py::get_release_group_releases` if one doesn't already exist; alternatively, can the picker call the MB mirror directly? Verify the existing `web/routes/browse.py` surface during implementation.

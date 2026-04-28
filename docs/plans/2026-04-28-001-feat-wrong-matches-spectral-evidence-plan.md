---
title: Wrong Matches Spectral Evidence + Bulk Cleanup Safety
type: feat
status: active
date: 2026-04-28
deepened: 2026-04-28
origin: docs/brainstorms/wrong-matches-spectral-evidence-requirements.md
---

# Wrong Matches Spectral Evidence + Bulk Cleanup Safety

## Overview

Surface stored per-candidate spectral and V0-probe evidence on the Wrong Matches tab so the operator can eyeball candidates by audio quality before destructive actions, and add a backend safety check to `Delete Lossless Opus` that skips groups whose on-disk Opus copy is itself spectrally suspect (`suspect` / `likely_transcode`).

Two halves, weakly coupled:
1. **Evidence display (R1–R3)** — extend the existing wrong-matches read path (DB query → row payload → frontend renderer) to expose `download_log.spectral_grade`, `download_log.spectral_bitrate`, and `download_log.v0_probe_*` per candidate. No new preview workflow, no new analysis trigger.
2. **Bulk-cleanup safety (R4–R6)** — gate `post_wrong_match_delete_lossless_opus` on `current_spectral_grade NOT IN ('suspect', 'likely_transcode')`, enforced server-side, with the existing `skipped[]` payload extended to carry the new reason.

---

## Problem Frame

Wrong Matches groups can hold several candidate folders for one release. Today the operator sees beets distance + scenario + candidate metadata per row, plus on-disk format/verified-lossless/spectral state on the group header. They cannot see whether the *candidates themselves* are spectrally suspect or whether their lossless-source V0 probe evidence is comparable — that data exists in `download_log` (denormalized columns from migrations 001 and 007) but never reaches the UI. The result is that high-distance but better-quality candidates can be hidden behind low-distance transcodes.

Separately, `Delete Lossless Opus` will currently delete wrong-match candidates whenever an Opus copy is on disk and `verified_lossless = TRUE`, even if the on-disk Opus copy itself is `likely_transcode`. The operator wants that bulk action to refuse groups whose on-disk evidence is not trustworthy.

(see origin: `docs/brainstorms/wrong-matches-spectral-evidence-requirements.md`)

---

## Requirements Trace

- R1. Wrong Matches rows surface stored spectral evidence per candidate (grade + floor/estimated bitrate)
- R2. Wrong Matches rows surface stored V0 probe evidence per lossless-source candidate (at least probe average)
- R3. No new per-row preview action or async preview flow; missing evidence is "missing data", not a trigger
- R4. `Delete Lossless Opus` must not delete groups whose on-disk Opus copy is `suspect` / `likely_transcode`
- R5. `NULL` / `genuine` / `marginal` are safe; `suspect` / `likely_transcode` are not
- R6. Safety enforced by the backend endpoint, not only by frontend visibility

**Origin acceptance examples:** AE1 (covers R1, R2), AE2 (covers R3), AE3 (covers R4, R5, R6), AE4 (covers R5)

---

## Scope Boundaries

- No change to beets distance thresholds or green-row Converge logic
- No automatic best-candidate selection by spectral quality
- No per-row preview action, background preview job, or new preview lifecycle
- No change to spectral analysis thresholds or V0 probe policy
- No change to manual force-import semantics
- No new schema migration — all required columns exist (`migrations/001_initial.sql:165-166` for `spectral_grade`/`spectral_bitrate`; `migrations/007_v0_probe_evidence.sql:8-27` for `v0_probe_*` columns on `download_log`)
- No change to the group-header payload — `current_spectral_grade`, `current_spectral_bitrate`, `verified_lossless`, `format` are already exposed (see `tests/test_web_server.py:3375-3389`)

---

## Context & Research

### Relevant Code and Patterns

- `web/routes/imports.py:418` — `_build_wrong_match_groups()` is the single seam where group + entry payloads are assembled. Entry construction at lines 502-514 is where new candidate-evidence keys land.
- `web/routes/imports.py:536-538` — `_is_lossless_opus_group()` is the existing two-condition predicate (`verified_lossless` + `format == "opus"`). Extending it is the natural fit for R4–R6, but the spectral-safety decision itself belongs in `lib/quality.py` per the decision-purity rule.
- `web/routes/imports.py:615-651` — `post_wrong_match_delete_transparent_non_flac()` is the existing precedent for "filter eligible groups, iterate, append `{download_log_id, reason}` to `skipped[]`". Mirror this shape exactly so the frontend already understands it.
- `web/routes/imports.py:654-707` — `post_wrong_match_delete_lossless_opus()` already has the `eligible_groups` / `skipped[]` response shape required by the contract test at `tests/test_web_server.py:3915-3949`.
- `lib/pipeline_db.py:1314-1356` — `get_wrong_matches()` SQL. Currently selects `ar.current_spectral_grade`, `ar.current_spectral_bitrate`. Extending the SELECT with `dl.spectral_grade`, `dl.spectral_bitrate`, `dl.v0_probe_kind`, `dl.v0_probe_avg_bitrate` is a six-column addition with no new join.
- `web/js/wrong-matches.js:492` (`renderWrongMatches`), `:719` (entry rendering), `:266-271` (`groupIsLosslessOpusCleanupTarget`), `:1102-1146` (`deleteLosslessOpusWrongMatches` result handling) — all the frontend touch points.
- `tests/test_web_server.py:3375-3389` — `GROUP_REQUIRED_FIELDS` and `ENTRY_REQUIRED_FIELDS` for the wrong-matches contract test. New entry fields go here in RED before implementation.
- `tests/test_web_server.py:639` and `:3915-3949` — route-audit classification + the existing `Delete Lossless Opus` contract test to extend.

### Institutional Learnings

- `docs/quality-verification.md` — canonical definitions of `genuine` / `marginal` / `suspect` / `likely_transcode` and the V0-probe lossless-source policy. The safety predicate must align with these definitions, not invent a parallel grade taxonomy.
- `docs/quality-ranks.md` — `within_rank_tolerance_kbps` for V0 probe comparison. Not directly used here (we only display the average, not compare it), but worth knowing if the operator later asks for "candidate vs on-disk Δ" UI.
- `docs/webui-primer.md` — Wrong Matches API payload shape (`{groups: [...]}`), convergence + delete workflows, frontend module layout.

### Patterns to Follow

- **Pure decision in `lib/quality.py` + subTest table** — same shape as `TestIsVerifiedLossless` (`tests/test_quality_decisions.py`).
- **Wire-boundary types are already structs** — `AudioQualityMeasurement` (`lib/quality.py:606`), `V0ProbeEvidence` (`lib/quality.py:654`). We're reading from denormalized flat columns, not JSONB blobs, so no new struct boundary is added by this work.
- **Contract test extension** — add new field names to `ENTRY_REQUIRED_FIELDS` (RED), then add the keys to the entry dict in `_build_wrong_match_groups()` (GREEN). The route audit at `TestRouteContractAudit.CLASSIFIED_ROUTES` does not need updating — both routes are already classified.

---

## Key Technical Decisions

- **Read from flat `download_log` columns, not JSONB.** The flat columns (`spectral_grade`, `spectral_bitrate`, `v0_probe_kind`, `v0_probe_avg_bitrate`) are already denormalized for query performance and are the same source the gate logic uses. No new JSONB digging at the route layer.
- **Display only `lossless_source_v0` probe averages on candidate rows.** The `v0_probe_kind` enum has three values (`lossless_source_v0`, `native_lossy_research_v0`, `on_disk_research_v0`); R2 explicitly scopes the surfaced evidence to lossless-source candidates. The other kinds remain in the row payload for forward use but the frontend treats non-lossless-source kinds as absent for display purposes.
- **Absent evidence renders as a dash, not a placeholder badge.** Older rows that pre-date the V0 grind-up feature have `NULL` v0 columns; rejected-pre-import rows have `NULL` spectral columns. The brainstorm explicitly says missing evidence is missing data, not a trigger to run analysis. UI shows "—" or omits the cell.
- **Pure spectral-safety decision in `lib/quality.py`.** A standalone `is_opus_copy_safe_for_lossless_delete(grade: str | None) -> bool` (with a paired skip-reason constant) keeps the rule out of the route handler and gives us a subTest matrix per `docs/quality-verification.md`. Default for unknown strings: unsafe.
- **`current_spectral_grade` is the right source for the safety gate.** Verified at `lib/pipeline_db.py:122` (`RequestSpectralStateUpdate.as_update_fields`) — `current_spectral_grade` is set from `self.current.grade`, where `current` is the on-disk / in-library copy's `SpectralMeasurement`. Per the dataclass docstring (`lib/pipeline_db.py:111` "latest-download and on-disk spectral state"), `current_*` columns track the in-library copy, not the most recent attempt. The gate therefore reads "the spectral grade of the verified-lossless Opus copy on disk" exactly as the origin's AE3 demands.
- **Add `groups_skipped_spectral_suspect: int` as a non-breaking sibling of `groups_deleted`.** The frontend should not have to count `skipped[].reason === 'spectral_suspect'` and group-by `request_id` to derive a blocked-group count. Adding a top-level integer is non-breaking (the existing contract test asserts presence of specific keys, not absence of others) and gives the result toast a one-shot value to display. The existing `eligible_groups` semantics stay pinned to "passed `_is_lossless_opus_group`" so the contract is unambiguous.
- **`skipped[]` `request_id` is scoped to `spectral_suspect` rows only.** Adding `request_id` to existing `delete_failed` rows would silently diverge from `post_wrong_match_delete_transparent_non_flac` (`web/routes/imports.py:615-651`), which builds the same skip shape for the same frontend consumer. Keep `delete_failed` rows untouched. New `spectral_suspect` rows carry `request_id` because the operator's blocked-group count is per-request, not per-row, and the frontend needs it to deduplicate.
- **Frontend does not hide the `Delete Lossless Opus` button when a group is unsafe.** The deferred outstanding question (per origin) was whether to additionally hide buttons; the chosen answer is: no — the button stays visible because the operator already knows from the group header why it's unsafe (`current_spectral_grade` is exposed today). Surface the skipped-suspect count in the post-action result toast so the operator can see how many groups the safety rule blocked. Worst-case all-blocked behaviour ("0 deleted, N skipped (spectral suspect)") is covered explicitly in U5.
- **No schema migration.** All columns exist already. This is a pure read + filter feature.

---

## Open Questions

### Resolved During Planning

- *Which stored fields cover candidate evidence?* `download_log.spectral_grade`, `download_log.spectral_bitrate` for R1; `download_log.v0_probe_kind` + `download_log.v0_probe_avg_bitrate` (filtered to `kind = 'lossless_source_v0'`) for R2.
- *How to handle older rows lacking evidence?* Absent = display as missing. No backfill, no new preview job. (Resolves the first deferred-to-planning question in origin.)
- *Should the frontend hide skipped groups for `Delete Lossless Opus`?* No — backend filters and reports skip counts via the existing `skipped[]` array; frontend surfaces the count in the result toast. Buttons stay visible. (Resolves the second deferred-to-planning question.)

### Deferred to Implementation

- Exact display string for the candidate-evidence cells ("V0≈265" vs "V0 avg 265 kbps" vs "265 kbps V0"). Decide once the row layout is in front of you — the contract test pins the data shape, not the visual.
- Whether to add a "candidate vs on-disk" delta column. Not required by R1/R2 — defer until the operator asks.
- Whether to expose `existing_v0_probe_avg_bitrate` (the on-disk copy's research V0 average) on the group header. Not required by the brainstorm, group header is currently complete per origin Dependencies. Defer.

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification.*

```
                          ┌────────────────────────────────────────┐
                          │ lib/quality.py                          │
                          │ is_opus_copy_safe_for_lossless_delete()│  ◄── U1 (pure)
                          │ + OPUS_DELETE_SKIP_REASON_SPECTRAL      │
                          └────────────────────┬───────────────────┘
                                               │ used by
                                               ▼
GET /api/wrong-matches                  POST /api/wrong-matches/delete-lossless-opus
       │                                                │
       ▼                                                ▼
_build_wrong_match_groups()             post_wrong_match_delete_lossless_opus()
   ├─► pdb.get_wrong_matches()  ◄── U2     ├─► _is_lossless_opus_group()         ◄── unchanged
   │     SELECT … +                        ├─► is_opus_copy_safe_for_lossless… ◄── U4 gate
   │       dl.spectral_grade,              │     for groups passing _is_lossless_opus_group
   │       dl.spectral_bitrate,            │     unsafe → append to skipped[] with new reason
   │       dl.v0_probe_kind,               └─► iterate rows for safe groups, _delete_wrong_match_row()
   │       dl.v0_probe_avg_bitrate
   ├─► entry payload includes new keys ◄── U3
   └─► response shape unchanged at top level
                  │
                  ▼
       web/js/wrong-matches.js
       renderEntries(entry) prints evidence cells   ◄── U5
       deleteLosslessOpusWrongMatches() result UI
       surfaces skipped-by-spectral count           ◄── U5
```

The new safety predicate is the only new decision; every other change is plumbing existing data through to existing render sites.

---

## Implementation Units

- U1. **Pure spectral-safety decision in `lib/quality.py`**

**Goal:** Centralise the `Delete Lossless Opus` spectral safety rule as a pure function with a paired skip-reason constant, so both the route handler and tests share one definition.

**Requirements:** R5, R6

**Dependencies:** None

**Files:**
- Modify: `lib/quality.py`
- Test: `tests/test_quality_decisions.py`

**Approach:**
- Add `is_opus_copy_safe_for_lossless_delete(grade: str | None) -> bool`. Returns `True` for `None`, `"genuine"`, `"marginal"`. Returns `False` for `"suspect"`, `"likely_transcode"`. Returns `False` for any unrecognised string (defensive default — keeps unknown future grades from accidentally being treated as safe).
- Add a module-level constant `OPUS_DELETE_SKIP_REASON_SPECTRAL = "spectral_suspect"` to give U4 a single source of truth for the skip-reason string written into the API response.
- Cite `docs/quality-verification.md` in a one-line docstring; do not duplicate the grade definitions.

**Execution note:** Implement test-first with a subTest table.

**Patterns to follow:**
- `lib/quality.py::is_verified_lossless` (decision shape)
- `tests/test_quality_decisions.py::TestIsVerifiedLossless` (subTest table layout)

**Test scenarios:**
- Happy path: `("genuine", True)`, `("marginal", True)`, `(None, True)` — *supports AE4 trigger value; AE4 itself is proven end-to-end in U4*
- Error path: `("suspect", False)`, `("likely_transcode", False)` — *supports AE3 trigger values; AE3 itself is proven end-to-end in U4*
- Edge case: `("", False)`, `("nonsense", False)`, `("GENUINE", False)` — defensive default; case-sensitive match against the documented enum (DB stores lowercase per `lib/spectral_check.py`)
- Edge case: constant `OPUS_DELETE_SKIP_REASON_SPECTRAL` resolves to a non-empty string distinct from `"delete_failed"` (asserted in U4 too — keeps the two skip reasons disjoint)

**Note on AE coverage:** This unit only proves the boolean mapping. AE3 and AE4 are end-to-end behaviours of `post_wrong_match_delete_lossless_opus` ("rows are skipped", "release remains eligible for cleanup") — they require the U4 endpoint test to prove. U1 supplies the trigger values U4 verifies against.

**Verification:**
- New tests pass under `nix-shell --run "python3 -m unittest tests.test_quality_decisions -v"`
- Pyright clean on `lib/quality.py`

---

- U2. **Extend `PipelineDB.get_wrong_matches()` to project per-attempt spectral and V0 probe columns**

**Goal:** Make per-candidate spectral and V0-probe evidence available to the route layer by adding the four flat `download_log` columns to the existing wrong-matches SELECT.

**Requirements:** R1, R2

**Dependencies:** None (parallel with U1)

**Files:**
- Modify: `lib/pipeline_db.py`
- Test: `tests/test_pipeline_db.py` (or `tests/test_pipeline_db_wrong_matches.py` if a dedicated module exists — locate before adding)

**Approach:**
- Add `dl.spectral_grade`, `dl.spectral_bitrate`, `dl.v0_probe_kind`, `dl.v0_probe_avg_bitrate` to the SELECT list at `lib/pipeline_db.py:1328-1350`. Keep the same `DISTINCT ON (request_id, failed_path)` collapsing — these columns are read from the same surviving row, no extra join.
- Do not rename existing columns; do not add JSONB extraction. The denormalized flat columns are the contract.
- Document in the docstring that the row dict now includes per-attempt evidence alongside the request-level snapshot, and clarify that `v0_probe_kind` may be any of the three enum values — consumers filter for `'lossless_source_v0'` if they only want lossless-source evidence (R2).

**Patterns to follow:**
- The existing `request_*` aliasing convention in the SELECT — keep new columns un-aliased so consumers reference them by the `download_log` column names directly (`spectral_grade`, not `attempt_spectral_grade`).
- Existing test pattern for `get_wrong_matches` (find via `grep` for `get_wrong_matches` in `tests/`).

**Test scenarios:**
- Happy path: insert a `download_log` row with all four new columns populated, call `get_wrong_matches()`, assert the returned dict contains the four keys with the expected values.
- Edge case: insert a row with `NULL` for all four columns (legacy/pre-migration-007 case), assert the keys are present with `None` values — never missing keys.
- Edge case: two rejected attempts for the same `(request_id, failed_path)`, newer row has the evidence, older row does not. Assert the newer row's evidence is the one returned (verifies `DISTINCT ON ... ORDER BY id DESC` still wins after the SELECT change).
- Integration: confirm the full row dict shape is unchanged for keys other than the four additions (regression guard against accidental field rename).

**Verification:**
- New tests pass under `nix-shell --run "python3 -m unittest tests.test_pipeline_db -v"` (or the located test module)
- The existing wrong-matches contract test in `tests/test_web_server.py` still passes — only the entry payload changes there, in U3
- Pyright clean on `lib/pipeline_db.py`. The returned `dict[str, object]` is unchanged in declared type; new keys are reads from `cur.fetchall()` so no new annotations are required at this seam

---

- U3. **Add candidate-evidence keys to the wrong-matches entry payload + extend contract test**

**Goal:** Wire U2's new DB columns into the entry dict produced by `_build_wrong_match_groups()`, and pin the new keys in the contract test so the frontend can rely on them.

**Requirements:** R1, R2, R3

**Dependencies:** U2

**Files:**
- Modify: `web/routes/imports.py`
- Modify: `tests/test_web_server.py`

**Approach:**
- In `web/routes/imports.py:502-514`, add four keys to the entry dict: `spectral_grade`, `spectral_bitrate`, `v0_probe_kind`, `v0_probe_avg_bitrate`. Pull them from the `row` dict returned by `pdb.get_wrong_matches()` (now extended in U2). Pass through `None` when absent.
- Do **not** add `current_lossless_source_v0_probe_avg_bitrate` to the group header — origin Dependencies say the header is already complete, and adding it would expand scope.
- Do **not** add a preview button, preview action, or async hook anywhere in the entry payload. R3.
- Extend `ENTRY_REQUIRED_FIELDS` in `tests/test_web_server.py:3386` (RED first) with the four new keys.
- The route is already classified in `TestRouteContractAudit.CLASSIFIED_ROUTES` — no audit change.

**Execution note:** Add the test fields first (RED), confirm the contract test fails with a missing-keys assertion, then add the entry keys (GREEN).

**Patterns to follow:**
- The existing entry-dict construction at `web/routes/imports.py:502-514`
- `_assert_required_fields` usage in `TestWrongMatchesContract`
- Field-addition pattern in `tests/test_web_server.py` for prior wrong-matches contract changes (use `git log -p tests/test_web_server.py` to find a precedent if needed)

**Test scenarios:**
- Covers AE1. Contract test: row with `spectral_grade='genuine'`, `spectral_bitrate=950`, `v0_probe_kind='lossless_source_v0'`, `v0_probe_avg_bitrate=265` returns all four keys with those values.
- Covers AE2. Row with all four columns `NULL` returns all four keys with `None`. The payload exposes no preview action — assert no `"preview_*"` keys are in the entry dict (regression guard against accidental scope creep).
- Edge case: row with `v0_probe_kind='native_lossy_research_v0'` (non-lossless-source kind) still returns the kind so the frontend can decide whether to display. Backend does not filter.
- Edge case: row with spectral evidence but no V0 probe (rejected before conversion) — `spectral_grade` populated, `v0_probe_*` `None`. Both presence patterns must be tolerated.

**Verification:**
- `nix-shell --run "python3 -m unittest tests.test_web_server.TestWrongMatchesContract -v"` passes
- Pyright clean on `web/routes/imports.py`. The entry dict is currently `dict[str, object]`; new keys are typed as the same. If pyright trips on the `row.get(...)` return shape leaking `Any`, add an explicit `cast(...)` rather than introducing a new TypedDict — keeps scope tight
- Existing tests on the route still pass

---

- U4. **Apply spectral-safety filter to `Delete Lossless Opus` and report skipped reason**

**Goal:** Make `post_wrong_match_delete_lossless_opus` refuse groups whose on-disk Opus copy is `suspect` / `likely_transcode`, surface the skip in the response payload, and pin the behaviour with an extended contract test.

**Requirements:** R4, R5, R6

**Dependencies:** U1 (for the pure decision + reason constant). **Logically independent of U2/U3** — the safety check reads `group["current_spectral_grade"]`, which is already on the group header today (`web/routes/imports.py:306` `_quality_summary`, exposed in `tests/test_web_server.py:3375` `GROUP_REQUIRED_FIELDS`). U2/U3 are file-conflict sequencing only (both this unit and U3 edit the same two files); ship U4 after U3 to avoid merge churn, but it could land first if the work is split across PRs.

**Files:**
- Modify: `web/routes/imports.py`
- Modify: `tests/test_web_server.py`

**Approach:**
- In `post_wrong_match_delete_lossless_opus` (`web/routes/imports.py:654-707`), partition the `_build_wrong_match_groups()` output into two lists during the eligibility scan:
  1. **Safe groups** — `_is_lossless_opus_group(group)` AND `is_opus_copy_safe_for_lossless_delete(group["current_spectral_grade"])`. These proceed to deletion as today.
  2. **Unsafe groups** — `_is_lossless_opus_group(group)` AND NOT safe. For each unsafe group, append one `{"download_log_id": <entry_log_id>, "reason": OPUS_DELETE_SKIP_REASON_SPECTRAL, "request_id": <rid>}` per row to the response `skipped[]` array. Track unsafe `request_id`s for the new top-level count.
- Add a new top-level response field: `groups_skipped_spectral_suspect: int` = count of distinct unsafe `request_id`s. Non-breaking sibling of `groups_deleted`; gives the frontend a single integer instead of forcing it to count and group-by.
- Keep `eligible_groups` defined as "groups that passed `_is_lossless_opus_group`" (i.e., safe + unsafe). Preserves the existing semantics so existing test assertions and any external consumers stay intact. The frontend computes "blocked count" from the new `groups_skipped_spectral_suspect` directly.
- Do **not** change the existing top-level keys (`status`, `groups_deleted`, `deleted`, `deleted_request_ids`, `eligible_groups`, `skipped`). Only add the new `groups_skipped_spectral_suspect` and add `spectral_suspect`-reason entries (with `request_id`) to `skipped[]`. Do **not** add `request_id` to existing `delete_failed` skip rows — that would diverge from `post_wrong_match_delete_transparent_non_flac` (`web/routes/imports.py:615-651`), which builds the same skip shape for the same frontend consumer. Keep `delete_failed` shape stable across both endpoints.
- Confirm `_is_lossless_opus_group` is unchanged (still checks `verified_lossless` + `format == 'opus'`) — the spectral check is a *separate* gate, not a modification to the group predicate, so the two conditions stay independently testable.

**Execution note:** Extend the existing `test_delete_lossless_opus_removes_verified_opus_groups_only` test family with new scenarios (RED), then add the safety check (GREEN).

**Patterns to follow:**
- `web/routes/imports.py:615-651` — `post_wrong_match_delete_transparent_non_flac` already shows the "build eligible list, iterate, append to `skipped[]`" pattern.
- `tests/test_web_server.py:3915-3949` — existing test layout for this endpoint (beets-detail mock + `current_spectral_grade` setup + assertion shape).

**Test scenarios:**
- Covers AE3. Group with `verified_lossless=True`, `format='opus'`, `current_spectral_grade='likely_transcode'` and one wrong-match row → response has `deleted=0`, `groups_deleted=0`, `eligible_groups=1`, `groups_skipped_spectral_suspect=1`, `skipped[]` contains an entry with `reason='spectral_suspect'`, `download_log_id`, and `request_id` for that row, files on disk untouched (assert via `os.path.exists` on the staged path).
- Covers AE3 (alt grade). Same group with `current_spectral_grade='suspect'` → identical behaviour (skipped, not deleted, `groups_skipped_spectral_suspect=1`).
- Covers AE4. Group with `current_spectral_grade IS NULL`, `verified_lossless=True`, `format='opus'` → row deleted as today, `groups_skipped_spectral_suspect=0`, no spectral-suspect skip entries.
- Covers R5. Same group with `current_spectral_grade='genuine'` → deleted, `groups_skipped_spectral_suspect=0`. With `'marginal'` → deleted.
- Covers R4 + R6. Mixed batch: two opus-lossless groups, one with `'genuine'` (one row), one with `'likely_transcode'` (two rows). Response has `deleted=1`, `groups_deleted=1`, `deleted_request_ids=[genuine_rid]`, `eligible_groups=2`, `groups_skipped_spectral_suspect=1`, `skipped[]` length=2 (both rows from unsafe group), every entry's `reason='spectral_suspect'`, every entry's `request_id == unsafe_rid`. **Pin all three count fields explicitly in the assertion.**
- Edge case: opus group with `verified_lossless=True`, `format='opus'`, `current_spectral_grade='likely_transcode'` AND zero wrong-match rows → `eligible_groups=1`, `groups_skipped_spectral_suspect=1` (group is unsafe even with no rows), `skipped[]` is empty (skip-reason rows are *per row*; an empty group cannot generate row-level skip entries, but the group still counts as skipped at the group level).
- Regression A: `verified_lossless=False`, `format='opus'`, `current_spectral_grade='likely_transcode'` → `eligible_groups=0`, `groups_skipped_spectral_suspect=0`, no skip rows. `_is_lossless_opus_group` filters first; the spectral gate never sees the group. **Pins that the safety check does not leak past the existing predicate.**
- Regression B: non-Opus or non-verified-lossless groups still excluded by `_is_lossless_opus_group` regardless of spectral grade — the safety gate does not relax the existing predicate.
- Regression C: `delete_failed` skip rows (induced by mocking `_delete_wrong_match_row` to return `False` for one row) still have shape `{download_log_id, reason: 'delete_failed'}` — **no `request_id` added**. Pins backwards compat with the frontend and with `post_wrong_match_delete_transparent_non_flac`.
- Integration: route classification audit (`TestRouteContractAudit`) still passes — route already classified.

**Verification:**
- `nix-shell --run "python3 -m unittest tests.test_web_server -k delete_lossless_opus -v"` all green
- Existing transparent-non-flac contract test still passes (verifies we didn't accidentally share state)
- Pyright clean on `web/routes/imports.py`
- **Cache-invalidation check:** confirm `delete-lossless-opus` does not rely on a stale `web:*` cache entry of `current_spectral_grade`. Audit during implementation: the wrong-matches data path bypasses the per-route Redis cache (no `cache.get_or_set` wrapper around `_build_wrong_match_groups`), and `_quality_summary` reads `current_spectral_grade` directly from `album_requests` each call. If that audit reveals a cache wrapper, add an explicit `cache.invalidate_pattern("web:*")` (mirroring `web/server.py:386` / `:337`) before the response is sent. Document the audit result in the PR description so the gate is auditable. Origin AE3 hinges on the gate seeing fresh on-disk grade — a stale cache would silently break it.

---

- U5. **Frontend: render candidate evidence + surface spectral-skip count in delete result**

**Goal:** Display per-candidate spectral grade/floor and lossless-source V0 probe average on each Wrong Matches entry row, and update the `Delete Lossless Opus` result toast to mention spectral-suspect skips when present.

**Requirements:** R1, R2, R3

**Dependencies:** U3 (entry payload), U4 (response shape unchanged at top level, but `skipped[]` may carry the new reason)

**Files:**
- Modify: `web/js/wrong-matches.js`
- Possibly modify: `web/index.html` (only if a new CSS class is needed)
- Test: `tests/test_js_wrong_matches.mjs`

**Approach:**
- In the entry-row renderer at `web/js/wrong-matches.js:719`, add two presentation cells next to the existing distance/scenario cells:
  - **Spectral cell** — `entry.spectral_grade` ("genuine" / "marginal" / "suspect" / "likely_transcode") + estimated bitrate from `entry.spectral_bitrate` when present. Render as "—" when both are `null`. Reference example string: `"genuine · 950 kbps"` (grade · estimated bitrate). Ambiguity over exact label/separator is fine to resolve mid-implementation, but the data shape is pinned by the contract test in U3.
  - **V0 probe cell** — `entry.v0_probe_avg_bitrate` only when `entry.v0_probe_kind === 'lossless_source_v0'`. Render as "—" otherwise (covers absent + non-lossless-source kinds — R2 scopes this to lossless-source). Reference example: `"V0 ≈ 265 kbps"`.
- Do **not** add a preview button, click handler, or async fetch in the entry row. R3.
- In `deleteLosslessOpusWrongMatches` at `web/js/wrong-matches.js:1102-1146`, prefer the new top-level `data.groups_skipped_spectral_suspect` (added in U4) when present, falling back to counting `skipped[].reason === 'spectral_suspect'` for forwards-compat. Append to the result message: e.g., `"1 group deleted, 1 group skipped (spectral suspect)"`. Keep the existing `delete_failed` reporting unchanged. Note: today's renderer at `web/js/wrong-matches.js:169-172` only reports `skipped` length without breaking out by reason — extend that branch.
- No change to `groupIsLosslessOpusCleanupTarget` — the button stays visible regardless of group safety. Per Key Decisions, the operator already sees `current_spectral_grade` on the header and the backend is the source of truth.

**Patterns to follow:**
- Existing entry-cell rendering immediately above the insertion point at `web/js/wrong-matches.js:719`
- `tests/test_js_wrong_matches.mjs` data-shape testing pattern (Node, no DOM)

**Test scenarios:**
- Covers AE1. JS unit: entry with `spectral_grade='suspect'`, `spectral_bitrate=320`, `v0_probe_kind='lossless_source_v0'`, `v0_probe_avg_bitrate=270` → renderer produces a row containing both pieces of evidence in the visible output.
- Covers AE2. JS unit: entry with all four fields `null` → renderer produces the row with "—" placeholders, no `<button>` element with a preview-action class, no `data-action="preview"` attribute (assert by string-search on the rendered HTML — no preview hooks).
- Edge case: `v0_probe_kind='native_lossy_research_v0'` with a non-null `v0_probe_avg_bitrate` → V0 cell renders as "—" because R2 restricts surfaced V0 evidence to lossless-source.
- Edge case: spectral present, V0 absent (rejected pre-conversion) — spectral cell shows the grade, V0 cell shows "—".
- Covers R6 surfacing. Result-toast formatting: response with `groups_skipped_spectral_suspect=1` and `skipped=[{reason:'spectral_suspect'}, {reason:'spectral_suspect'}, {reason:'delete_failed'}]` produces a message that breaks out the spectral-suspect group count separately from the delete-failed row count.
- All-blocked worst case: response with `groups_deleted=0`, `eligible_groups=3`, `groups_skipped_spectral_suspect=3`, `skipped[]` length=4 (all `spectral_suspect`) → toast says something like `"0 deleted, 3 groups skipped (spectral suspect)"`, never blank or misleading. Asserts that the renderer handles the empty-result-but-non-empty-block case explicitly.
- Forwards-compat fallback: response with the legacy shape (no `groups_skipped_spectral_suspect` field, only `skipped[]` carrying `spectral_suspect` rows) — the renderer still produces the right count by deduplicating `skipped[].request_id`. Guards against a server/client deploy ordering bug.
- Regression: when `skipped[]` is empty and `groups_skipped_spectral_suspect=0`, the result message is unchanged from today's behaviour.
- Static check: `node --check web/js/wrong-matches.js` passes (pre-commit hook enforces, but list it explicitly as a verification gate alongside the JS unit test).

**Verification:**
- `node --test tests/test_js_wrong_matches.mjs` (or whichever runner the repo uses — see existing `package.json` / `scripts/run_tests.sh`) passes
- `node --check web/js/wrong-matches.js` returns 0 (also enforced by pre-commit, but listed as an explicit gate)
- Manual smoke (post-deploy on doc2): open `music.ablz.au` Wrong Matches tab; expand a release with multiple candidates; confirm spectral + V0 cells appear and "—" renders for absent data; click `Delete Lossless Opus` against a release whose on-disk copy was set to `likely_transcode` and confirm the toast reports the skip count

---

## System-Wide Impact

- **Interaction graph:** The Wrong Matches UI flow (`/api/wrong-matches` → `_build_wrong_match_groups` → `pdb.get_wrong_matches`) and the bulk-delete flow (`/api/wrong-matches/delete-lossless-opus` → `_build_wrong_match_groups` → `_delete_wrong_match_row`) share `_build_wrong_match_groups`. Adding entry keys in U3 affects both paths but the bulk-delete path doesn't read the new keys, so there's no behaviour change there.
- **Error propagation:** `is_opus_copy_safe_for_lossless_delete` returns `False` for unrecognised strings — defensive default. A future grade introduced without updating the function will be treated as unsafe, causing groups to be skipped rather than wrongly deleted. This is the safe failure mode.
- **State lifecycle risks:** None. No DB writes added; no new migration. Read-only feature plus a server-side filter.
- **API surface parity:** Other bulk-delete endpoints (`post_wrong_match_delete_transparent_non_flac`, `post_wrong_match_delete_group`, `post_wrong_match_delete`) are untouched. They have their own eligibility predicates and do not need the spectral check applied — `delete_transparent_non_flac` already requires the on-disk copy to be transparent (a stricter condition than "spectral grade is safe"), and the per-row / per-request endpoints are explicit operator actions where the safety question is the operator's, not the system's. Verify in code review that the spectral safety rule is not silently extended to those paths.
- **Integration coverage:** The U4 mixed-batch test scenario (one safe + one unsafe group) is the cross-path proof — it asserts `_is_lossless_opus_group` and `is_opus_copy_safe_for_lossless_delete` compose correctly without one masking the other.
- **Unchanged invariants:**
  - `_is_lossless_opus_group` semantics (`verified_lossless` + `format == 'opus'`) — unchanged. The new check is a separate gate.
  - Group-header payload — unchanged. No new keys on the group dict.
  - `eligible_groups` semantics — pinned to "passed `_is_lossless_opus_group`" (safe + unsafe). Adding `groups_skipped_spectral_suspect` as a sibling does not redefine `eligible_groups`.
  - Existing `skipped[]` shape for `delete_failed` rows — unchanged. New `request_id` field is on `spectral_suspect` rows only, ensuring `post_wrong_match_delete_transparent_non_flac` and `post_wrong_match_delete_lossless_opus` continue to share an identical `delete_failed` shape.
  - Top-level response shape of `delete-lossless-opus` — additive only. Adds one new key (`groups_skipped_spectral_suspect: int`); no existing keys renamed or removed.
  - DB schema — unchanged. All required columns exist via migrations 001 and 007.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Frontend renders raw enum values ("likely_transcode") that read poorly to the operator. | U5 maps grades to short labels in the renderer; assert in the JS test that the rendered output contains the human label, not the raw enum. |
| Operator sees the `Delete Lossless Opus` button on an unsafe group, clicks it, gets nothing deleted, and is confused. | The result toast (per U5) breaks out the spectral-suspect skip count via `groups_skipped_spectral_suspect`. Group header already shows `current_spectral_grade`, providing pre-action visibility. U5's all-blocked test scenario explicitly pins toast wording for the "0 deleted" case. |
| Future spectral grade introduced without updating `is_opus_copy_safe_for_lossless_delete`. | Defensive default returns `False` (unsafe) — fails closed. New grade would need an explicit allowlist edit. Documented in the U1 docstring. |
| `eligible_groups` semantics drift over time as the safety gate gets reused. | Key Decision pins `eligible_groups` = "passed `_is_lossless_opus_group`" (pre-spectral). Documented in the response-shape note in U4 and asserted in the mixed-batch test. |
| Stale `current_spectral_grade` cached in Redis lets an unsafe group slip past the gate. | U4 verification step audits the wrong-matches data path for cache wrappers; if absent, no risk. If present, U4 adds an explicit `cache.invalidate_pattern("web:*")` after deletion mirroring `web/server.py:386`. |
| Adding `request_id` to `spectral_suspect` skip rows makes the two bulk-delete endpoints diverge in `skipped[]` shape. | Confine `request_id` to `spectral_suspect` rows only; leave `delete_failed` rows untouched. `post_wrong_match_delete_transparent_non_flac` continues to emit identical `delete_failed` shape. Pinned by U4 Regression C test. |
| Per-row `download_log` evidence is sparse for legacy rows, leaving the UI mostly "—" until older rows age out. | Acceptable per origin Key Decisions. The brainstorm explicitly states absent evidence should reveal a storage/display gap, not start a new workflow. No backfill in scope. |

---

## Documentation / Operational Notes

- After deploying U5, restart `cratedigger-web` per `.claude/rules/web.md`: `ssh doc2 'sudo systemctl restart cratedigger-web'`. The 5-min cratedigger timer doesn't apply to the web service.
- No DB migration runs on this deploy — confirm `cratedigger-db-migrate` reports "no new migrations" in the deploy log, since the `requires` chain still triggers the unit.
- After deploy, run a single backend probe: `ssh doc2 'pipeline-cli query "SELECT id, current_spectral_grade FROM album_requests WHERE verified_lossless = true AND current_spectral_grade IN (\\'suspect\\', \\'likely_transcode\\') LIMIT 5"'`. Any rows returned represent groups that the new safety rule will start blocking. If the count is unexpectedly large, surface it before flipping the bulk-action.
- Update `docs/webui-primer.md` only if the row payload section there enumerates entry keys explicitly. Skip if the doc is general.

---

## Sources & References

- **Origin document:** [docs/brainstorms/wrong-matches-spectral-evidence-requirements.md](../brainstorms/wrong-matches-spectral-evidence-requirements.md)
- Wrong Matches route: `web/routes/imports.py:418-707`
- Wrong Matches DB query: `lib/pipeline_db.py:1314-1356`
- Spectral grade definitions: `docs/quality-verification.md`, `lib/spectral_check.py:46-177`
- V0 probe schema: `migrations/007_v0_probe_evidence.sql`
- Existing contract test: `tests/test_web_server.py:3312-3389` (wrong-matches), `tests/test_web_server.py:3915-3949` (delete-lossless-opus)
- Pattern reference for skip[]: `web/routes/imports.py:615-651` (transparent-non-flac bulk delete)
- Frontend touch points: `web/js/wrong-matches.js:266-271, 492, 510, 719, 1102-1146`

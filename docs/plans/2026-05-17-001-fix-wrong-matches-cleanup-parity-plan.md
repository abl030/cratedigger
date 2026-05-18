---
title: "fix: Wrong Matches cleanup ↔ force-import parity"
type: fix
status: active
created: 2026-05-17
origin: docs/brainstorms/2026-05-17-wrong-matches-cleanup-parity-requirements.md
tracking_issue: 268
depth: standard
---

# fix: Wrong Matches cleanup ↔ force-import parity

## Summary

Wrong Matches bulk cleanup and force-import call the same decision reducer (`full_pipeline_decision_from_evidence`) but feed it different current-evidence inputs. Cleanup gates current-evidence loading on `imported_path` being set or `status='imported'`, which means `wanted` rows whose MBID is already in Beets get `current=None` and can be misclassified as `would_import` while force-import correctly rejects them as `downgrade`. We're factoring the action-time Beets-by-MBID lookup + `ensure_current_evidence_for_action` block into a shared helper so both call sites acquire current evidence identically, adding a verified-lossless short-circuit that deletes Wrong Matches rows guaranteed to lose the upgrade gate, and fixing the misleading "No successful import on disk" group header.

## Problem Frame

Carried from origin (`docs/brainstorms/2026-05-17-wrong-matches-cleanup-parity-requirements.md`):

- **Decision divergence is the bug.** Cleanup at `lib/wrong_match_cleanup_service.py:429` (`_load_current_evidence`) returns `current=None` whenever `imported_path` is empty AND `status != 'imported'`, so the reducer is run with weaker inputs than force-import's `_load_evidence_import_gate` at `lib/import_dispatch.py:565` would supply for the same row. The contradiction was observed live on request 2762 (Parts & Labor — Escapers Two) and is structurally present for 722 currently-visible Wrong Matches rows.
- **Verified-lossless clutter never imports.** Any candidate in Wrong Matches against an MBID whose current is verified-lossless is guaranteed to lose the upgrade gate — these rows are permanent clutter that should auto-clean.
- **Header lies.** `web/js/wrong-matches.js:799` renders "No successful import on disk" purely from `download_log` success history; the group payload already carries `in_library` (set from Beets exact-ID match at `web/routes/imports.py:386`) and that signal is being ignored.

## Goal

After this PR: for any Wrong Matches row, cleanup's decision is identical to force-import's decision for the same candidate / request / MBID / config. Verified-lossless parents auto-clean without operator force-import roundtrip. The group header tells the operator when the album is already in the library.

## Scope

### In scope

- Extract a `load_current_evidence_for_action` helper in `lib/import_evidence.py` that does Beets-by-MBID lookup + `ensure_current_evidence_for_action` + provenance translation.
- `lib/import_dispatch.py::_load_evidence_import_gate` calls the helper instead of inlining the Beets+ensure block (lines ~598–624).
- `lib/wrong_match_cleanup_service.py` deletes `_load_current_evidence` and routes through the new helper; fails closed when Beets has the album but evidence is unavailable.
- New `OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT` outcome label + cleanup early-exit when `current.verified_lossless_proof is not None`, skipping the full reducer.
- `web/js/wrong-matches.js::renderLatestImport` uses the group's `in_library` field to choose copy that distinguishes "no cratedigger import history" from "album already in library (any candidate must beat it)".
- Tests per origin's enumerated list (six tests, see Verification & Tests).

### Out of scope

- Retroactive sweep of the 722 risk-bucket rows — fix forward, next manual Clean All re-evaluates them.
- Any second/parallel quality decision function (explicit non-goal from origin).
- Changes to candidate-evidence loading (cleanup's existing path is fine).
- Changes to cleanup's per-row deletion / advisory-lock / active-job policy after the shared decision returns.

### Deferred to Follow-Up Work

None. Origin lists no deferred items; all four scope items land in this PR.

---

## Key Technical Decisions

1. **Factoring shape: lift the current-evidence half into `lib/import_evidence.py`, keep `EvidenceImportGate` in dispatch.** Option (b) from origin §4 "Deferred to ce-plan". Rationale: cleanup already has its own candidate-evidence loader (`_load_candidate_evidence`, which calls `load_candidate_evidence_for_source`) — it doesn't need the candidate half of `_load_evidence_import_gate`. Lifting only the current-evidence half (Beets-by-MBID + `ensure_current_evidence_for_action`) into `lib/import_evidence.py` lets both call sites compose what they need without forcing cleanup through the `EvidenceImportGate` shape it doesn't use. The candidate side of `_load_evidence_import_gate` stays put in dispatch because that's where the dispatch-specific `EvidenceImportGate` provenance assembly lives.

2. **Helper returns `CurrentEvidenceActionResult | None`.** `None` means "Beets has no album for this MBID" (legitimate first-import case → pass `current=None` to reducer). A non-`None` result with `provenance.fail_closed=True` means "Beets has the album but evidence unavailable" → caller fails closed. A non-`None` result with `available=True` means current evidence is usable. This mirrors the existing `ensure_current_evidence_for_action` shape; no new dataclasses.

3. **Verified-lossless short-circuit lives in cleanup, not in the helper or the reducer.** It's a cleanup-specific policy ("rows that can never import are clutter, delete them") — not a quality decision. The check is `current.verified_lossless_proof is not None`, fires after current evidence loads and before the reducer is called, emits the new `OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT` outcome. The reducer is never called in this path; the test asserts the reducer is not invoked.

4. **UI uses the existing `in_library` field; no new API field.** `web/routes/imports.py::_quality_summary` already sets `in_library` from a Beets exact-ID match (`web/routes/imports.py:386`). The frontend gap is purely render-side. No backend change, no contract field addition.

5. **Header copy:** *"Album already in library — any candidate must beat current quality"* when `in_library=true` and no `latest_import`; keep existing render path when `latest_import` exists; keep neutral "No previous import" when `in_library=false`. Final wording is implementer's call but must distinguish the three states.

---

## Implementation Units

### U1. Extract `load_current_evidence_for_action` helper

**Goal:** Land the shared current-evidence loader in `lib/import_evidence.py` with no production call-site changes yet (helper is added but not consumed).

**Requirements:** Origin §3.1 (factor action-time current-evidence loader into shared helper).

**Dependencies:** none.

**Files:**
- `lib/import_evidence.py` — add `load_current_evidence_for_action`, add to `__all__`
- `tests/test_import_evidence.py` — add unit tests for the new helper

**Approach:**
- New function `load_current_evidence_for_action(db, *, request_id: int, mb_release_id: str, quality_ranks: QualityRankConfig | None = None, beets_library_root: str = "") -> CurrentEvidenceActionResult | None`.
- Does `BeetsDB(library_root=beets_library_root).get_album_info(mb_release_id, cfg)` inside a `with` block.
- If `album_info is None`, return `None` (signal: Beets has no album).
- Otherwise call `ensure_current_evidence_for_action(db, request_id=..., mb_release_id=..., quality_ranks=..., current_album_path=album_info.album_path, album_info=album_info, beets_library_root=beets_library_root)` and return the result.
- Mirror the exception-handling shape currently in `_load_evidence_import_gate` (lines 625–640) — catch broad `Exception`, log, return a `CurrentEvidenceActionResult` with `provenance.current_status='failed'`, `provenance.fail_closed=True`, `provenance.fallback_reason=f"{type(exc).__name__}: {exc}"`. This keeps fail-closed semantics intact for callers.

**Patterns to follow:**
- `lib/import_evidence.py::ensure_current_evidence_for_action` — same style of wrapper around evidence acquisition with provenance translation
- `lib/import_dispatch.py:598–640` — the existing inline block that this function replaces; mirror its exception handling exactly

**Test scenarios** (in `tests/test_import_evidence.py`):
- Happy path: Beets returns `album_info`, `ensure_current_evidence_for_action` returns available evidence → helper returns the `CurrentEvidenceActionResult` unchanged.
- Beets absent: `BeetsDB.get_album_info` returns `None` → helper returns `None`.
- Beets present, ensure raises: `BeetsDB.get_album_info` returns `album_info`, `ensure_current_evidence_for_action` raises `RuntimeError("backfill failed")` → helper returns a `CurrentEvidenceActionResult` with `provenance.fail_closed=True`, `provenance.current_status='failed'`, and the exception class+message in `fallback_reason`.
- Beets present, ensure returns unavailable+fail_closed: helper passes through unchanged.
- Default `quality_ranks=None` resolves to `QualityRankConfig.defaults()` before passing to BeetsDB (assert on the value passed to a mocked `get_album_info`).

**Verification:** New unit tests pass; pyright clean on both files; `__all__` includes the new symbol.

---

### U2. `_load_evidence_import_gate` delegates to the new helper

**Goal:** Replace the inline Beets+ensure block in dispatch with a call to `load_current_evidence_for_action`. No behavior change; pure factoring.

**Requirements:** Origin §3.1 (single shared evidence-loading path), CLAUDE.md "Quality decisions live in ONE place" extended to "evidence acquisition lives in ONE place".

**Dependencies:** U1.

**Files:**
- `lib/import_dispatch.py` — replace lines ~598–640 inside `_load_evidence_import_gate` with a single helper call + provenance plumbing
- `tests/test_dispatch_from_db.py` — verify existing dispatch coverage still passes; add one targeted test if needed

**Approach:**
- Inside `_load_evidence_import_gate`, after candidate evidence is loaded, replace the existing Beets-lookup → `ensure_current_evidence_for_action` → exception-handling block with a single call to `load_current_evidence_for_action(db, request_id=request_id, mb_release_id=mb_release_id, quality_ranks=quality_ranks, beets_library_root=beets_library_root)`.
- Branch on the return:
  - `None` → return `EvidenceImportGate(current=None, candidate=..., current_status=CURRENT_STATUS_MISSING, current_reason="album not in beets", current_fail_closed=False, ...)` (preserves existing behavior at lines 605–615).
  - non-`None` → assemble `EvidenceImportGate` from `current_result.evidence` / `current_result.provenance` exactly as today (lines 642–651).
- No public signature change to `_load_evidence_import_gate`; no public behavior change.

**Patterns to follow:**
- Existing assembly of `EvidenceImportGate` at `lib/import_dispatch.py:642–651`
- Module-local import style (helper imported at top of file alongside the other `lib.import_evidence` imports)

**Test scenarios:**
- Existing `tests/test_dispatch_from_db.py` cases for `_load_evidence_import_gate` still pass with the new internal implementation (rely on mocking `BeetsDB.get_album_info` and `ensure_current_evidence_for_action` at the same boundary they currently do — verify mocks still bite after the factoring).
- Add one new test if the boundary moved: patch `load_current_evidence_for_action` directly and assert `_load_evidence_import_gate` translates `None` → `current_status='missing'` / `current_fail_closed=False`, and a `fail_closed=True` result → `current_status='failed'` / `current_fail_closed=True`.

**Verification:** Existing dispatch tests in `tests/test_dispatch_from_db.py` and `tests/test_dispatch_core.py` pass unchanged; `tests/test_integration_slices.py` dispatch+evidence slices pass unchanged; pyright clean.

---

### U3. Cleanup uses the shared helper; delete `_load_current_evidence`

**Goal:** Cleanup acquires current evidence the same way force-import does — Beets-by-MBID, fail-closed on backfill failure, no gate on `imported_path`/`status`. This is the bug fix.

**Requirements:** Origin §3.2 (cleanup fails closed when Beets has album but evidence unavailable), origin §3 main bug fix.

**Dependencies:** U1.

**Files:**
- `lib/wrong_match_cleanup_service.py` — delete `_load_current_evidence` (lines 429–466); update call site at line 257 to use the shared helper; add a new outcome constant `OUTCOME_SKIPPED_CURRENT_EVIDENCE_FAILED` to distinguish "fail closed" from "stale" / "missing" (or reuse `OUTCOME_SKIPPED_CURRENT_EVIDENCE_MISSING` if the existing label fits — implementer's call, but the distinction matters for audit).
- `tests/test_wrong_match_cleanup_service.py` — add tests per the scenarios below

**Approach:**
- In `_cleanup_wrong_match` at line 257, after candidate evidence is loaded and the verified-lossless short-circuit (U4) has had its chance, call `load_current_evidence_for_action(db, request_id=request_id, mb_release_id=mb_release_id, quality_ranks=..., beets_library_root=...)` where `mb_release_id` comes from the `request` dict and `beets_library_root` comes from `cfg`.
- Translate the result:
  - `None` (Beets has no album) → pass `current=None` to reducer; legitimate first-import case.
  - non-`None` with `provenance.fail_closed=True` → return `_result(..., OUTCOME_SKIPPED_CURRENT_EVIDENCE_*, reason=provenance.fallback_reason)`. Do NOT call the reducer.
  - non-`None` with `available=True` → pass `result.evidence` to reducer.
  - non-`None` with `available=False` and `fail_closed=False` → also skip with appropriate outcome (mirrors existing stale/missing handling).
- Delete `_load_current_evidence` and the unused `_LoadedEvidence`-with-outcome path it consumed (keep `_LoadedEvidence` if still used by `_load_candidate_evidence`).
- Cleanup config plumbing: `beets_library_root` must reach `_cleanup_wrong_match` from the caller. Check `cleanup_wrong_matches` and any orchestration entry points and thread the value through if not already present.

**Patterns to follow:**
- `lib/import_dispatch.py::_load_evidence_import_gate` — same branching shape on the helper's three return states
- Existing outcome-tagging at `lib/wrong_match_cleanup_service.py:285–308` — return early with `_result(...)` and don't call the reducer

**Test scenarios** (in `tests/test_wrong_match_cleanup_service.py` or `tests/test_integration_slices.py`):

- **Covers origin test 1 — cleanup uses current evidence for `wanted` rows.** Request `wanted`, `imported_path=None`, MBID set, mock `load_current_evidence_for_action` to return a `CurrentEvidenceActionResult(available=True, evidence=<MP3 avg 198>)`. Candidate evidence is MP3 avg 197 / spectral likely_transcode 160 (the Parts & Labor scenario). Expected: outcome is `OUTCOME_DELETED` (cleanup deletes the source), NOT `OUTCOME_KEPT_WOULD_IMPORT`. Decision passed to the reducer has `current` populated; the resulting decision is a `downgrade`-family reject.

- **Covers origin test 3 — Beets-absent still allows `would_import`.** Same row state, but mock the helper to return `None`. Expected: reducer is called with `current=None`; if the candidate is genuinely better than nothing the row stays as `kept_would_import`. Guards against overcorrection.

- **Covers origin test 4 — Beets-present-but-evidence-failed fails closed.** Mock the helper to return `CurrentEvidenceActionResult(available=False, provenance=<fail_closed=True>)`. Expected: outcome is a current-evidence-failed skip label; reducer is NOT called; row remains visible.

- **Beets-present-but-stale** (existing semantic, preserved): helper returns `available=False, fail_closed=False` for stale snapshot. Cleanup skips with `OUTCOME_SKIPPED_CURRENT_EVIDENCE_STALE` (or equivalent — keep behavior parity with today's stale handling).

**Verification:** All four scenarios pass; `tests/test_wrong_match_cleanup_service.py` covers no regressions in existing cleanup paths (candidate-missing, active-job, advisory-lock); pyright clean.

---

### U4. Verified-lossless short-circuit in cleanup

**Goal:** When current evidence shows `verified_lossless_proof is not None`, cleanup deletes the source without calling the reducer and tags the outcome distinctly. Verified-lossless parents are unbeatable by anything currently in Wrong Matches; the row is guaranteed clutter.

**Requirements:** Origin §3.3 (verified-lossless early exit), origin Tests-Owed §5.

**Dependencies:** U3 (cleanup must already be loading current evidence via the shared helper).

**Files:**
- `lib/wrong_match_cleanup_service.py` — add `OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT` constant; add early-exit branch after current evidence loads, before the reducer call
- `tests/test_wrong_match_cleanup_service.py` — add the short-circuit test

**Approach:**
- After `load_current_evidence_for_action` returns and U3's translation completes, but BEFORE assembling reducer inputs, check: if `current_result is not None and current_result.evidence is not None and current_result.evidence.verified_lossless_proof is not None`, then run the cleanup deletion path with the new `OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT` outcome. Acquire the same advisory lock the normal `confident_reject` path acquires (`ADVISORY_LOCK_NAMESPACE_WRONG_MATCH_CLEANUP`, `wrong_match_cleanup_lock_key(...)`) so deletion ordering and active-job protection are preserved.
- The reducer is NEVER called in this branch. The test asserts this with a `patch` on `full_pipeline_decision_from_evidence` that records call count.
- New outcome label sits alongside `OUTCOME_DELETED` in the module-level constants (line ~30).

**Patterns to follow:**
- `lib/wrong_match_cleanup_service.py:310–...` — existing deletion flow with advisory lock; reuse the same lock acquisition + deletion call pattern
- `lib/quality.py:846` — `verified_lossless_proof` attribute on `AlbumQualityEvidence`

**Test scenarios** (in `tests/test_wrong_match_cleanup_service.py`):

- **Covers origin test 5 — verified-lossless short-circuit.** Request `wanted`, MBID set, mock the helper to return `available=True, evidence=<current with verified_lossless_proof=AudioFingerprintMatch(...)>`. Candidate evidence is any plausible non-verified-lossless file (e.g. MP3 320 CBR). Patch `full_pipeline_decision_from_evidence` to track calls. Expected: outcome is `OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT`; reducer was NOT called (`mock.call_count == 0`); deletion was invoked.
- **Negative: candidate-evidence-missing still bails before verified-lossless check.** Existing precedence — candidate-evidence-missing skip outcome must still fire BEFORE the verified-lossless branch (so a missing candidate doesn't get auto-deleted just because the parent is verified-lossless).
- **Negative: current without verified_lossless_proof reaches the reducer.** Mock helper to return current with `verified_lossless_proof=None`; assert the reducer IS called and outcome flows through the normal path.

**Verification:** Three scenarios pass; new constant exported from the module; pyright clean.

---

### U5. Wrong Matches header copy uses `in_library`

**Goal:** Group header tells the operator when the album is already in Beets. Stops saying "No successful import on disk" when `in_library=true`.

**Requirements:** Origin §3.4 (UI header distinguishes "no cratedigger history" from "current album exists in Beets"), origin Tests-Owed §6.

**Dependencies:** none (independent of U1–U4).

**Files:**
- `web/js/wrong-matches.js` — update `renderLatestImport(d, group)` to take the group context (or restructure: have the caller pass `in_library` / `verified_lossless` flags) and branch on `in_library`
- `tests/test_js_wrong_matches.mjs` — add tests for the three header states (no-history+absent, no-history+in-library, no-history+in-library+verified-lossless)

**Approach:**
- Current signature is `renderLatestImport(d)` where `d` is the latest_import payload or null. Change the call site (find with grep) to pass the group's `in_library` and `verified_lossless` flags.
- Render logic:
  - `latest_import` truthy → existing path (unchanged).
  - `latest_import` null, `in_library` true, `verified_lossless` true → "Verified-lossless copy in library — Wrong Matches against this album auto-clean on next sweep" (or implementer-chosen wording with the same semantic).
  - `latest_import` null, `in_library` true → "Album already in library — candidate must beat current quality to import".
  - `latest_import` null, `in_library` false → existing "No previous import" / "No successful import on disk" copy (keep the original since it's truthful here, or soften to "No previous import on disk").
- Implementer picks final wording; the test asserts the three branches render distinct strings.

**Patterns to follow:**
- `web/js/wrong-matches.js:799` — existing `renderLatestImport` render shape
- `tests/test_js_wrong_matches.mjs::wrongMatchesData` (lines 77–95) — mock-data builder pattern; extend with `in_library` / `verified_lossless` fields

**Test scenarios** (in `tests/test_js_wrong_matches.mjs`):

- **Covers origin test 6 — header distinguishes states.**
  - `latest_import=null, in_library=false` → header renders "no previous import"-family copy (matches existing behavior in spirit).
  - `latest_import=null, in_library=true, verified_lossless=false` → header renders the "album already in library" copy; does NOT render "No successful import on disk".
  - `latest_import=null, in_library=true, verified_lossless=true` → header renders the verified-lossless copy.
  - `latest_import={...}, in_library=true` → header renders the existing `latest_import` summary (unchanged).

**Verification:** `node --check web/js/*.js` passes; new JS tests pass via `node tests/test_js_wrong_matches.mjs`; no contract test changes needed (no new API fields).

---

## Verification & Tests

Per-unit scenarios above. Cross-cutting verification:

- Full suite via `nix-shell --run "bash scripts/run_tests.sh"` — green.
- `pyright lib/import_evidence.py lib/import_dispatch.py lib/wrong_match_cleanup_service.py` — 0 errors.
- Manual smoke on doc1: query the Parts & Labor row via `pipeline-cli show 2762`, run a cleanup pass against a tiny subset (or unit-test the exact scenario), confirm classification matches force-import.

---

## System-Wide Impact

- **Cleanup decision contract** changes for `wanted` rows whose MBID is in Beets. Those rows that previously kept-as-`would_import` will now either delete (verified-lossless or confident_reject), skip (fail-closed), or remain visible with a more accurate `kept_*` outcome. No live data is destroyed beyond what cleanup already deletes — the deletion policy is unchanged; only the classification leading to it is corrected.
- **Force-import** behavior unchanged. `_load_evidence_import_gate`'s observable signature and return shape are preserved; only its internals are refactored.
- **UI** copy change for one header in the Wrong Matches group view. No API contract change.
- **Importer / preview workers** untouched.
- **No DB migration** — no schema changes; all evidence already persisted.

---

## Risk Analysis & Mitigation

| Risk | Likelihood | Mitigation |
|---|---|---|
| `_load_evidence_import_gate` refactor accidentally changes force-import behavior (e.g. exception handling drift) | Low | U2's test plan locks the three branches (None / fail_closed / available); existing dispatch tests catch broader regressions |
| Verified-lossless short-circuit deletes a row that the operator wanted to keep visible | Low | The brainstorm validated this with the user as "very very simple" — verified-lossless current means no candidate can beat it; row is permanent clutter. Distinct outcome label makes it auditable; can be reverted on a single row by re-creating the Wrong Match if ever needed |
| `beets_library_root` not threaded through to cleanup, causing helper to misbehave | Medium | U3 explicitly calls out the config plumbing check; the helper accepts `beets_library_root=""` as a default and `BeetsDB` uses the configured root in that case — verify against cleanup's caller chain before merging |
| Cleanup config plumbing requires touching more files than expected | Low | If `cfg` is already plumbed (it is at `lib/wrong_match_cleanup_service.py:267`), `cfg.beets_library_root` is one attribute lookup |

---

## Sequencing

Strict: U1 → (U2, U3) → U4 → U5. U2 and U3 are independent after U1 lands (could parallelize) but the verified-lossless short-circuit (U4) builds on U3's helper call. U5 is fully independent — could land first if convenient.

Recommended commit shape (one logical change per commit):
1. U1 — add helper + tests
2. U2 — dispatch delegates to helper
3. U3 — cleanup uses helper + delete `_load_current_evidence`
4. U4 — verified-lossless short-circuit
5. U5 — UI header copy

---

## Deferred / Open

None. Origin `Open Questions` was empty; brainstorm resolved scope, factoring direction, retroactive-sweep policy, and copy intent. Final UI wording is the implementer's call within U5's stated semantic.

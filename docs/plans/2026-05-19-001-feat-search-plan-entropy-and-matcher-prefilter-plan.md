---
title: feat: Search plan entropy and matcher pre-filter
type: feat
status: completed
date: 2026-05-19
origin: docs/brainstorms/2026-05-19-search-plan-entropy-requirements.md
---

# feat: Search plan entropy and matcher pre-filter

## Summary

Flip the matcher pre-filter at `lib/matching.py::check_for_match` from
bidirectional to asymmetric (one-line change), persist `release_group_year`
on `album_requests` with a deploy-time backfill, restructure
`lib/search.py::generate_search_plan` to kill default-slot repetition and
the short-token drop while adding literal-title / format-hint /
track-with-artist / release-group-year / self-titled strategy slots, and
ship a dry-run simulator CLI + saturation telemetry plumbing. Bump
`SEARCH_PLAN_GENERATOR_ID` to invalidate all existing plans on next cycle.

---

## Problem Frame

See origin: `docs/brainstorms/2026-05-19-search-plan-entropy-requirements.md`.
A small cohort of curated requests (Radiohead Kid A, Bon Iver 22 a Million,
Willow self-titled, Mountains, Darren Hanlon) are continuously saturating
slskd or returning zero matches despite the albums being demonstrably
findable on the network. Root causes are the matcher pre-filter silently
skipping track-fallback results and the generator emitting low-entropy
queries. Plan order matters: the matcher fix lands before or with the
generator changes — shipping new variants while the pre-filter still
discards them would waste a deploy.

---

## Requirements

Carried from origin (`docs/brainstorms/2026-05-19-search-plan-entropy-requirements.md`):

- R1. Asymmetric pre-filter (`search_count > 2 * track_num`), codec guard preserved.
- R2. Dissolved (per-cycle cache; no leak).
- R3. Forensic encoding: `pre_filter_skip_count INTEGER` column + ≤5 flagged sample rows in candidates JSONB.
- R4. Replace 5-slot default repetition with distinct strategies.
- R5. Remove short-token drop entirely.
- R6. Add literal-full-title strategy slot.
- R7. Format-hint slots universal (no conditional gating).
- R8. Track-fallback queries prepend artist.
- R9. Persist `release_group_year`, backfill on deploy, conditional extra slot when years differ.
- R10. Detect self-titled via `normalize(artist) == normalize(title)` OR token-subset.
- R11. Self-titled strategy mix includes artist + first-track-title and artist + year.
- R12. Ship pre_filter_skip_count column + saturation aggregator + `pipeline-cli search-plan saturation`; defer dashboard UI to `docs/brainstorms/2026-05-09-search-plan-per-request-dashboard-requirements.md`.
- R13. `pipeline-cli search-plan dry-run <id>` runs new generator without persisting.

**Origin acceptance examples:** AE1 (R1), AE2 (R4/R5/R6/R8), AE3 (R7), AE4 (R9), AE5 (R10/R11), AE6 (R13).

---

## Scope Boundaries

- Not rebuilding `album_match` filename-similarity scoring. R1 fixes the pre-filter only.
- Not introducing a new query DSL or generator framework. Changes layer on `lib/search.py::generate_search_plan` in place.
- Not building the cross-pressing matcher. R1 (asymmetric `2n`) and R9 (release_group_year) are investments that enable it; design and implementation belong in their own brainstorm.
- Not implementing the dashboard UI for saturation/skip visualisation. R12 ships data + CLI; UI lives in `docs/brainstorms/2026-05-09-search-plan-per-request-dashboard-requirements.md`.
- Not changing slskd's `responseLimit` / `fileLimit` config. Saturation is the symptom; raising limits would not help focus.
- Not addressing bonus-track / multi-disc handling. The matcher's per-folder strict-count gate still rejects those cases. Separate known limitations.
- Not adding pipeline-level retry/cooldown policy on starving requests. R12 surfaces the data; what to do with chronically-starving requests is a separate decision.

---

## Context & Research

### Relevant Code and Patterns

- `lib/matching.py::check_for_match` (lines 308–517) — pre-filter at lines 350–356; negative-cache write at line 355; second strict-count gate at line 451.
- `lib/matching.py::album_track_num` — only counts audio files matching configured specs; junk (jpg/nfo/m3u/cue/log) is correctly ignored at both gates.
- `lib/context.py:34-35` — `search_dir_audio_count` and `negative_matches` (per-cycle; reset each context).
- `cratedigger.py:203 _build_search_cache` and `cratedigger.py:738` — both paths populate `dir_audio_counts`. Only files passing `filetype_matches` increment the count.
- `lib/search.py::generate_search_plan` (lines 749–1002) — current generator. `SEARCH_PLAN_GENERATOR_ID` at line 35.
- `lib/search.py::build_query`, `cap_tokens`, `strip_short_tokens`, `wildcard_artist_tokens` — composition helpers.
- `lib/search.py:802` — `_has_dropped_low_entropy` populates plan provenance for dropped tokens.
- `lib/search_plan_service.SearchPlanService` — plan lifecycle, regeneration entry points.
- `lib/pipeline_db.py::log_search` (line 3210) — search-log row insert; `candidates` is msgspec-encoded JSON.
- `lib/quality.CandidateScore` — wire-boundary `msgspec.Struct` for forensics; flag field would be added here.
- `lib/enqueue.py:400-416 _planned_grab_entry` — extracts `year=release_date[:4]` from release object; this is where release_group_year fetch is added.
- `lib/pipeline_db.py` request CRUD — new column accessor methods.
- `web/mb.py` (or equivalent MB-mirror client) — used by enqueue for release fetch; release-group fetch path may need a sibling method.
- `scripts/pipeline_cli.py` — CLI subcommand registration; existing `search-plan show/regenerate` patterns to follow.
- `web/routes/pipeline.py` — `get_pipeline_log` (line 94) and per-request endpoints for adding saturation aggregator.
- `tests/test_matching.py` — existing pre-filter / album_match tests (no current track-fallback coverage).
- `tests/test_search.py` — query-helper tests (`strip_short_tokens`, `wildcard_artist_tokens`).
- `tests/test_search_plan_service.py` — plan-generation tests.
- `tests/fakes.py::FakePipelineDB` — `log_search` (line 2945), `search_logs` list, `SearchLogRow` mock; needs accessor for new column.
- `tests/test_web_server.py::TestRouteContractAudit.CLASSIFIED_ROUTES` — every new route must be added here or the audit fails.

### Institutional Learnings

- `docs/brainstorms/2026-05-08-persisted-search-plans-requirements.md` — `SEARCH_PLAN_GENERATOR_ID` bump protocol, forward-only invalidation, `test_generator_id_constant_is_pinned` guard. The U5 generator changes constitute a generator-output change; bump is required.
- `docs/persisted-search-plans-rollout.md` — operational shape of plan invalidation, 580+ plans regenerated cleanly. This change will invalidate all current plans on next cycle; the cycle's compute spike is bounded by the existing wave caps.
- `docs/plans/2026-05-05-001-feat-peer-cache-redis-migration-plan.md` — 7-day Redis browse cache is the substrate that makes the `2n` pre-filter shape affordable. Without it, the change would have meaningful first-cycle browse load impact.
- `docs/brainstorms/2026-05-09-search-plan-per-request-dashboard-requirements.md` — R12 ships data + CLI; that dashboard brainstorm consumes the data when it's next iterated.
- `CLAUDE.md` § "CLI ⇄ API surface symmetry" — R12 and R13 must expose both CLI and API surfaces wrapping the same service-layer method. Audit via `TestRouteContractAudit.CLASSIFIED_ROUTES`.
- `.claude/rules/code-quality.md` § "Pipeline Decision Debugging — Simulator-First TDD" — R13 (`dry-run` CLI) is the search-plan analog of `pipeline-cli quality`. Use it during development to validate generator output against starving albums before committing to the GENERATOR_ID bump.
- `.claude/rules/code-quality.md` § "Wire-boundary types" — `CandidateScore` is the existing `msgspec.Struct` at the forensics boundary; the new `pre_filter_skip: bool` field lands there.
- `.claude/rules/code-quality.md` § "API Contract Tests" — every new route gets a `REQUIRED_FIELDS` set and an entry in `CLASSIFIED_ROUTES`. Mock rows must use production-shaped types (datetime, UUID, typed structs).

### External References

No external research warranted. This is internal pipeline infrastructure built on well-trodden patterns; no third-party libraries, security/payments boundaries, or unfamiliar protocols involved.

---

## Key Technical Decisions

- **Pre-filter is asymmetric, not variant-aware.** A single-line change (`search_count > 2 * track_num`) is preferred over plumbing variant context through `find_download → check_for_match`. The matcher stays unaware of search strategy; the threshold doubles the album's track count rather than fitting it tightly. Bonus: track-fallback bug fixed without special-casing.
- **`2n` over `n+2`.** The pre-filter is a junk-dir guard, not a tight-fit filter. `2n` captures more peer-folder data into the 7-day Redis browse cache, which is the substrate for the future cross-pressing matcher. First-cycle browse cost is bounded by existing wave caps; subsequent cycles are cache hits.
- **R3 encoding: scalar count + sampled JSONB.** `pre_filter_skip_count INTEGER` is queryable and aggregable; ~5 flagged sample rows ride alongside scored candidates inside the existing candidates JSONB blob for inspection. Per-row forensic size remains bounded.
- **`release_group_year` is a real column, backfilled on deploy.** The data is cheap (~4000 requests × local MB mirror call). Never defer cheap backfills. Conditional extra slot only when `year != release_group_year` (avoid the same wasteful repetition pattern killed in R4).
- **Format hints universal, not conditional.** 100% of pipeline requests want lossless (DB query confirmed; zero exclude FLAC). No state, no log lookback, generator stays pure of runtime data.
- **Wildcarding stays.** Empirically necessary for catalog-style searches (Beatles → `*eatles`); we are removing wildcarding-combined-with-token-drop, not wildcarding itself.
- **Single GENERATOR_ID bump for U5.** All generator-output changes ride one ID bump. Bumping mid-implementation would generate intermediate plans that get invalidated on the next bump.
- **Simulator (U6) lands before generator output is committed.** The dry-run CLI is the validation surface used during U5 development; landing the CLI shell first gives U5 development a feedback loop.

---

## Open Questions

### Resolved During Planning

- **Pre-filter shape (`2n` vs `n+2` vs rip vs variant-thread)** — resolved: asymmetric `2n` with codec guard preserved.
- **R3 encoding (new outcome enum vs JSONB flag vs separate column)** — resolved: count column + flagged JSONB samples.
- **R7 format-hint conditionality (log-lookback vs regen-flag vs unconditional)** — resolved: unconditional (100% of requests want lossless).
- **R9 release-group year availability** — resolved: not currently fetched; add column + backfill + generator usage.
- **R10 self-titled detection rule** — resolved: `normalize(artist) == normalize(title)` OR token-subset.
- **R12 dashboard scope** — resolved: ship data + CLI only; defer UI to the 2026-05-09 dashboard brainstorm.

### Deferred to Implementation

- **MB mirror release-group fetch shape** — verify during U3 whether the mirror exposes first-release year directly on the release-group fetch path, or whether it requires `min(release_year)` over the release-group's release list. Either path is bounded; backfill is local I/O. Fall back to derived `min(year)` if no direct field exists.
- **Candidates JSONB top-N split** — current cap is top-20 scored. With U2 adding ≤5 skip samples, the right split is likely 15 scored + 5 skipped, but the exact partition can be tuned during U2 implementation based on observed shapes.
- **Self-titled token-subset edge cases** — Mountains / Mountains Mountains Mountains is the model case. Need to confirm during U5 implementation that "Self / Self" doesn't accidentally match unrelated albums via subset (it shouldn't — token-subset requires the smaller token set to be a proper subset of the larger).

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

**Generator output shape, post-restructure.** The new plan slot mix for a typical request (year known, release_group_year populated and different):

| Slot | Strategy | Example query (Kid A, year=2008, rg_year=2000) |
|------|----------|------------------------------------------------|
| 1 | `default` (wildcarded, no short-token drop) | `*adiohead Kid A` |
| 2 | `literal` (no wildcard, no drop, no year) | `Radiohead Kid A` |
| 3 | `literal_flac` | `Radiohead Kid A FLAC` |
| 4 | `literal_lossless` | `Radiohead Kid A lossless` |
| 5 | `unwild_year` (release year) | `Radiohead Kid A 2008` |
| 6 | `unwild_rg_year` (conditional: differs from year) | `Radiohead Kid A 2000` |
| 7 | `track_0_artist` (artist + first track) | `Radiohead Everything In Its Right Place` |
| 8 | `track_1_artist` | `Radiohead Kid A` (if a track title matches the album title, semi-redundant — fine) |
| 9 | `track_2_artist` | `Radiohead The National Anthem` |

For matched-year requests, slot 6 is omitted (8 slots). For self-titled requests (R10/R11), the slot mix substitutes:

| Slot | Strategy | Example (Willow, year=2007, rg_year=2007) |
|------|----------|--------------------------------------------|
| 1 | `selftitled_artist_track_0` | `Willow And Finally I Can Breathe` |
| 2 | `selftitled_artist_track_0_flac` | `Willow And Finally I Can Breathe FLAC` |
| 3 | `selftitled_artist_year` | `Willow 2007` |
| 4-6 | `track_N_artist` | `Willow When the Sea Called Our Names`, etc. |

The exact slot count is variable but bounded — 8 to 10 slots for normal requests, 6 to 8 for self-titled. The persisted-plan storage already supports variable size (`plan_items` table with ordinal; no fixed schema constraint).

**Asymmetric pre-filter shape.** The change in `check_for_match`:

```text
# before
if len(cached_codecs) <= 1 and abs(search_count - track_num) > 2:

# after
if len(cached_codecs) <= 1 and search_count > 2 * track_num:
```

That is the entire matcher behavior change. The codec guard's purpose is unchanged (avoid skipping multi-codec dirs). The post-browse strict-count gate at line 451 (`tracks_info["count"] != track_num`) continues to enforce exact-fit before scoring.

---

## Implementation Units

### U1. Asymmetric pre-filter flip

**Goal:** Replace the bidirectional pre-filter in `check_for_match` with `search_count > 2 * track_num`. This is the matcher bug fix and is also the bonus dividend that makes track-fallback queries reachable (no variant plumbing needed).

**Requirements:** R1.

**Dependencies:** None.

**Files:**
- Modify: `lib/matching.py` (single condition at line ~350)
- Test: `tests/test_matching.py`

**Approach:**
- The change is one-line: `abs(search_count - track_num) > 2` → `search_count > 2 * track_num`.
- The `cached_codecs <= 1` guard stays in place.
- The `ctx.negative_matches.add(neg_key)` write stays as-is — its scope (per-cycle, in-memory) is already correct.

**Execution note:** Write the failing test scenarios first (track-fallback case with `search_count=1, track_num=4` currently rejected; with the new asymmetric rule it must reach `album_track_num`). Add the bug-repro scenario before flipping the condition.

**Patterns to follow:** `tests/test_matching.py` existing per-scenario test structure.

**Test scenarios:**
- Happy path: `search_count=4, track_num=4` (default-query whole-album case) — passes pre-filter, browse + score proceeds.
- Edge case (the bug fix): `search_count=1, track_num=4` (track-fallback case) — under old rule skipped; under new rule passes pre-filter, dir is browsed and scored.
- Happy path: `search_count=8, track_num=4` (whole album with bonus disc maybe) — passes (1 ≤ 8 ≤ 8, exactly at boundary, just barely permitted: `8 > 8` is false).
- Edge case: `search_count=9, track_num=4` (just over `2n`) — skipped pre-filter, negative-cache entry written.
- Junk-dir case: `search_count=50, track_num=4` (large dump folder) — skipped pre-filter, negative-cache entry written. (This is the case the pre-filter must continue to catch.)
- Codec-guard case: `search_count=50, track_num=4, cached_codecs=3` (multi-codec dir) — NOT skipped (codec guard bypasses the pre-filter).
- Covers AE1.

**Verification:**
- Pyright clean on `lib/matching.py`.
- All `tests/test_matching.py` tests pass; new track-fallback test passes.
- Spot-check via `pipeline-cli show 2125` after deploy: at least one Willow track-fallback row has non-empty `candidates`.

---

### U2. Pre-filter skip telemetry

**Goal:** Add `pre_filter_skip_count INTEGER` column on `search_log` for aggregable counts, and extend `CandidateScore` with a `pre_filter_skip: bool` flag so up to ~5 sample skipped rows can ride inside the existing candidates JSONB blob.

**Requirements:** R3.

**Dependencies:** None (independent of U1, though they land together for operator value).

**Files:**
- Create: `migrations/025_search_log_pre_filter_skip_count.sql`
- Modify: `lib/quality.py` (add `pre_filter_skip: bool = False` field to `CandidateScore`)
- Modify: `lib/matching.py::check_for_match` — when the pre-filter rejects a dir, append a flagged `CandidateScore` (with `pre_filter_skip=True`, `matched_tracks=0`) up to a per-search cap (~5) AND increment a local skip counter
- Modify: `lib/pipeline_db.py::log_search` — accept and persist `pre_filter_skip_count`
- Modify: `lib/search.py::SearchResult` — carry `pre_filter_skip_count: int` field
- Modify: `cratedigger.py::_log_search_result` — pass the count through
- Modify: `tests/fakes.py::FakePipelineDB::log_search` — record the new field
- Modify: `tests/fakes.py::FakePipelineDB` `SearchLogRow` (or equivalent) — expose new field
- Test: `tests/test_matching.py`, `tests/test_pipeline_db.py`

**Approach:**
- Migration: `ALTER TABLE search_log ADD COLUMN pre_filter_skip_count INTEGER NOT NULL DEFAULT 0`.
- `CandidateScore` extension: new optional `pre_filter_skip: bool = False` field. Defaults preserve existing wire shape; new field only set when the matcher records a skip.
- In `check_for_match`: track skips with a local counter; emit at most ~5 flagged `CandidateScore` rows into `candidates` (skip rows count against the overall top-N cap; prefer keeping scored rows when capacity is contested). Cap can be tuned during implementation.
- `SearchResult` and `log_search` carry the count through.
- Adjust the existing candidates top-N cap to `15 scored + 5 skipped` (or similar split) — exact partition decided during implementation based on observed shapes.

**Execution note:** Write a contract test asserting that a known pre-filter-skip case produces both the count and at least one flagged candidate row before changing the matcher.

**Technical design:** *Directional guidance only.* The `pre_filter_skip` flag lives on `CandidateScore`; rendering layers (`pipeline-cli show`) can show flagged rows with a distinct prefix.

**Patterns to follow:**
- Migration pattern: `migrations/024_backfill_v0_metric_from_measurement.sql` for shape.
- Field-on-struct extension: see `ImportResult`/`ValidationResult` extensions in prior migrations.
- `log_search` signature evolution: existing `final_state` precedent.
- `tests/test_pipeline_db.py` test patterns for column reads/writes.

**Test scenarios:**
- Happy path: a search where all dirs pass pre-filter — `pre_filter_skip_count = 0`, no flagged candidate rows.
- Happy path: a search where 17 dirs are skipped via pre-filter — `pre_filter_skip_count = 17`, candidates JSONB contains up to 5 flagged skip rows.
- Edge case: skip count higher than the sample cap — count is accurate; sample rows are capped at ~5; no overflow.
- Round-trip: msgspec encode → decode of `CandidateScore` with `pre_filter_skip=True` preserves the field.
- Migration test in `tests/test_migrator.py`: after migration 025, `search_log.pre_filter_skip_count` exists with `DEFAULT 0` and pre-existing rows have value 0.
- `FakePipelineDB.log_search` records the field for downstream tests.

**Verification:**
- Pyright clean.
- All tests pass.
- Post-deploy, `pipeline-cli query "SELECT MAX(pre_filter_skip_count) FROM search_log WHERE created_at > NOW() - INTERVAL '1 hour'"` returns a non-zero value (real pre-filter skips are happening).

---

### U3. `release_group_year` column + deploy-time backfill

**Goal:** Add `release_group_year INTEGER NULL` to `album_requests` and ship a backfill that populates it from the local MB mirror for every request with `mb_release_group_id IS NOT NULL`. Backfill runs as part of the deploy (via the existing `cratedigger-db-migrate` oneshot pattern, or a separate one-shot script invoked on deploy — implementation choice).

**Requirements:** R9 (data layer).

**Dependencies:** None.

**Files:**
- Create: `migrations/026_album_requests_release_group_year.sql`
- Create: `scripts/backfill_release_group_year.py` (or similar — one-shot script invoked on deploy)
- Modify: `lib/pipeline_db.py` — `request_by_id`, `update_request_*` methods need new column awareness
- Modify: `lib/album_source.py::AlbumRecord` — add `release_group_year: int | None` field
- Modify: `web/mb.py` (or wherever MB-mirror client lives) — verify or add a release-group-year fetch helper
- Modify: `tests/fakes.py::FakePipelineDB` — expose `release_group_year` on request rows
- Test: `tests/test_migrator.py`, `tests/test_pipeline_db.py`, new `tests/test_backfill_release_group_year.py`

**Approach:**
- Migration adds the column with NULL default.
- Backfill script iterates `album_requests WHERE mb_release_group_id IS NOT NULL AND release_group_year IS NULL`, fetches each release-group's first-release year from the local MB mirror, updates the row. Batch in transactions of ~500 for safety.
- During U3 implementation, **verify the MB mirror's release-group fetch path** — if it exposes the year directly, use that; if not, fall back to `min(year)` over the release-group's release list.
- The backfill is idempotent — re-runnable if it fails partway.

**Execution note:** Write the backfill against a known reissue (e.g. request id 1868 Kid A) and assert the expected `release_group_year=2000` outcome before running it broadly.

**Patterns to follow:**
- Migration patterns: `migrations/024_backfill_v0_metric_from_measurement.sql` (which combines schema change with backfill SQL).
- Deploy-time backfill: `cratedigger-db-migrate.service` runs migrations before app services start (per `CLAUDE.md` § Database migrations).
- MB mirror client: existing patterns in `web/mb.py` for release lookups.

**Test scenarios:**
- Migration test: after migration 026 applies, `release_group_year` column exists, NULL default, pre-existing rows have NULL.
- Backfill happy path: a request with `mb_release_group_id='21501'` (Kid A's release group) gets `release_group_year=2000` populated.
- Backfill idempotency: running twice produces same result (the WHERE clause filters out already-populated rows).
- Backfill resilience: if MB mirror returns 404 for a release-group, the row is left NULL (not deleted, not failed-hard) and logged.
- Backfill batching: a batch of 500 requests commits atomically; failures in one batch don't leak into the next.
- `FakePipelineDB` returns `release_group_year` field correctly for downstream test consumers.

**Verification:**
- Pyright clean.
- All tests pass.
- Post-deploy: `pipeline-cli query "SELECT COUNT(*) FROM album_requests WHERE mb_release_group_id IS NOT NULL AND release_group_year IS NULL"` returns 0 (or close to it — accounting for MB mirror 404s).
- Spot-check on Kid A request 1868: `release_group_year = 2000`.

---

### U4. Enqueue path fetches `release_group_year`

**Goal:** When a new request is added (web add, CLI add), populate `release_group_year` from the MB mirror at enqueue time so new requests land with the column already filled.

**Requirements:** R9 (ongoing data flow).

**Dependencies:** U3 (column must exist).

**Files:**
- Modify: `lib/enqueue.py::_planned_grab_entry` (line 400+) — add release-group fetch alongside the existing release fetch
- Modify: web request-add route (`web/routes/`) and `scripts/pipeline_cli.py` add subcommand — pass `release_group_year` through to enqueue
- Test: `tests/test_enqueue.py`, `tests/test_web_server.py`, `tests/test_pipeline_cli.py`

**Approach:**
- Reuse the MB-mirror release-group fetch helper from U3.
- Extract release-group year at enqueue time and persist on the new request row.
- If the MB mirror lookup fails or no release-group is found, leave `release_group_year` NULL and continue — the generator handles NULL gracefully (no extra slot).

**Execution note:** None — straightforward extension of existing enqueue logic.

**Patterns to follow:**
- `lib/enqueue.py::_planned_grab_entry` existing structure for fetching MB metadata at enqueue.
- Error-tolerant MB lookup pattern (don't fail-hard on a single bad lookup).

**Test scenarios:**
- Happy path: new request with reissue MB release → `release_group_year` populated and differs from `year`.
- Happy path: new request with original-release MB release → `release_group_year == year`.
- Edge case: MB mirror returns 404 for the release-group → request created with `release_group_year=NULL`, no error.
- Integration: a full add-from-web flow creates the row with the column populated.

**Verification:**
- All enqueue tests pass.
- Web add of a known reissue produces a row with the expected `release_group_year`.

---

### U5. Generator restructure

**Goal:** Rewrite `lib/search.py::generate_search_plan` to (a) remove the 5-slot default repetition, (b) remove the short-token drop, (c) add literal-full-title slot, (d) add unconditional format-hint slots (FLAC + lossless), (e) prepend artist to track-fallback queries, (f) emit conditional release-group-year slot, (g) detect self-titled and route through dedicated mix. Bump `SEARCH_PLAN_GENERATOR_ID`.

**Requirements:** R4, R5, R6, R7, R8, R9 (generator side), R10, R11.

**Dependencies:** U3 (uses `release_group_year` column).

**Files:**
- Modify: `lib/search.py::generate_search_plan` and supporting helpers
- Modify: `lib/search.py::SEARCH_PLAN_GENERATOR_ID` constant (bump)
- Modify: `lib/search.py::ReleaseSnapshot` — add `release_group_year: int | None` field
- Modify: `lib/search.py` — remove `strip_short_tokens` from the default-slot pipeline (the function may stay for other callers if any; verify during implementation)
- Modify: `lib/search.py` — add `wildcard_artist_tokens` invocation to track-fallback queries (R8)
- Modify: `lib/search_plan_service.py` — if `ReleaseSnapshot` construction lives here, thread `release_group_year` through
- Modify: snapshot signature: extending `ReleaseSnapshot` with `release_group_year` changes the snapshot fingerprint — this triggers automatic plan regeneration via the existing fingerprint-change pathway. No extra invalidation work needed beyond the GENERATOR_ID bump
- Test: `tests/test_search.py`, `tests/test_search_plan_service.py`

**Approach:**
- The generator's input snapshot grows by one field (`release_group_year`). Output structure stays the same (`SearchPlan` with `plan_items` list).
- Slot construction becomes a series of helper calls:
  1. `_default_slot` (wildcarded artist+title, capped token count, NO short-token drop)
  2. `_literal_slot` (no wildcard, no drop, no year)
  3. `_literal_flac_slot`
  4. `_literal_lossless_slot`
  5. `_unwild_year_slot` (existing, preserved)
  6. `_unwild_rg_year_slot` (conditional: `release_group_year is not None and release_group_year != year`)
  7. Track-fallback slots: `_track_with_artist_slot(track_idx, artist)` for up to 3 tracks
- Self-titled detection: a helper `_is_self_titled(artist, title)` returns True when `normalize(artist) == normalize(title)` OR `normalize_tokens(artist) ⊂ normalize_tokens(title)` (or reverse). When True, the generator emits the dedicated self-titled mix instead of the default mix.
- `SEARCH_PLAN_GENERATOR_ID` bumps to a new value (e.g. `"search-plan/2026-05-19-1"`).
- Plan-provenance: `dropped_low_entropy_tokens` is no longer populated (the drop no longer happens); provenance carries `selftitled: True` when detected and `release_group_year: <int>` when used.

**Execution note:** Write tests first. The new generator's output is a precise function of inputs — extensive table-driven tests for each strategy slot, edge cases (numeric tokens, comma-bearing titles, self-titled, missing year, missing release_group_year, single-track albums), and full snapshot-to-plan integration. The simulator from U6 is the primary validation surface during development.

**Technical design:** *Directional only — see High-Level Technical Design above for the slot mix table.*

**Patterns to follow:**
- Existing `generate_search_plan` structure (per-strategy candidate construction → dedupe → ordinal assignment).
- `test_generator_id_constant_is_pinned` test pattern (the GENERATOR_ID bump must be deliberate; the pinning test guards against accidental changes).
- `_per_track_candidates` existing helper.

**Test scenarios:**
- Happy path Kid A-shape: artist `Radiohead`, title `Kid A`, year `2008`, release_group_year `2000`, 14 tracks → plan contains literal title with "A" preserved, FLAC/lossless slots, year + release_group_year slots, artist-prepended track slots, NO 5x default repetition.
- Happy path Willow-shape (self-titled): artist `Willow`, title `Willow`, year `2007`, release_group_year `2007`, 4 tracks → self-titled detected, plan contains artist+first-track and artist+year slots, NO `*illow`-collapsed default.
- Happy path Mountains-shape: artist `Mountains`, title `Mountains Mountains Mountains` → token-subset self-titled detected, dedicated mix emitted.
- Happy path Hanlon-shape (niche, normal): artist `Darren Hanlon`, title `I Will Love You at All`, year `2010`, release_group_year `2010` → standard mix, no rg_year slot (years match), format-hint slots present (will return zero in production for this album, but generator emits them unconditionally).
- Happy path Bon Iver-shape (numeric title): title `22, a Million` → "22" preserved in slots, "a" preserved (no short-token drop).
- Edge case: missing year → no `unwild_year` slot, no `unwild_rg_year` slot.
- Edge case: missing release_group_year → no `unwild_rg_year` slot.
- Edge case: year == release_group_year → no `unwild_rg_year` slot (avoid duplicate).
- Edge case: fewer than 3 tracks → fewer track-fallback slots.
- Edge case: GENERATOR_ID test — pinned to the new value; bumping is deliberate.
- Plan deduplication: if two strategies produce the same canonical query key, dedupe keeps the higher-priority slot.
- Self-titled detection negative case: `Willow / Willow Tree` should NOT detect as self-titled (token-subset only fires when one set is a strict subset of the other with non-trivial overlap).
- Provenance: `selftitled: True` and `release_group_year: <int>` flags populate plan provenance when applicable.
- Covers AE2, AE4 (year-side), AE5.

**Verification:**
- Pyright clean.
- All search tests pass.
- `tests/test_search_plan_service.py::test_generator_id_constant_is_pinned` passes against the new pinned value.
- `pipeline-cli search-plan dry-run` (from U6) on requests 1868, 455, 2125, 3018, 1640 produces plans materially different from current production.

---

### U6. Simulator dry-run CLI

**Goal:** Add `pipeline-cli search-plan dry-run <request_id>` that loads the request's snapshot, runs `generate_search_plan` against the current code, and prints the resulting plan items without persisting or affecting `active_plan_id`.

**Requirements:** R13.

**Dependencies:** U5 (the simulator wraps the new generator — but the CLI shell can land first as a thin wrapper around the current generator and gain its real value when U5 lands).

**Files:**
- Modify: `scripts/pipeline_cli.py` — add `search-plan dry-run` subcommand
- Modify: `lib/search_plan_service.py` — add `dry_run_for_request(request_id) -> SearchPlan` method (read-only, no DB writes)
- Modify: `web/routes/pipeline.py` — add `GET /api/search-plan/<id>/dry-run` endpoint for CLI⇄API symmetry
- Modify: `tests/test_web_server.py::TestRouteContractAudit.CLASSIFIED_ROUTES` — add the new route
- Test: `tests/test_pipeline_cli.py`, `tests/test_search_plan_service.py`, `tests/test_web_server.py`

**Approach:**
- The service-layer method reads the request row + tracks, constructs the `ReleaseSnapshot`, calls `generate_search_plan`, and returns the `SearchPlan` (in-memory only, never persisted).
- CLI subcommand prints plan items in the same format as `search-plan show`.
- API endpoint returns the same shape as `search-plan show` for the active plan.
- No state mutation. No `active_plan_id` change. No `search_plans` or `plan_items` row inserts.

**Execution note:** Write the contract test first — `_assert_required_fields(self, payload, REQUIRED_FIELDS, "dry-run")` ensures the response shape is stable.

**Patterns to follow:**
- `scripts/pipeline_cli.py` existing `search-plan show` subcommand for CLI shape.
- `lib/search_plan_service.SearchPlanService` existing read-only methods.
- `web/routes/pipeline.py` existing GET endpoints + `_assert_required_fields` test pattern.
- `tests/test_web_server.py::TestRouteContractAudit.CLASSIFIED_ROUTES` — every new route must be added or the audit fails.
- `tests/test_pipeline_cli.py` exit-code mapping for the CLI subcommand.

**Test scenarios:**
- Happy path: `dry-run <request_id>` for a request with all metadata present → CLI prints the generator's slot list; no DB writes occur.
- Happy path: API endpoint returns JSON shape matching `REQUIRED_FIELDS` (items list, generator_id, provenance).
- Edge case: request id not found → CLI exits 2 (not_found); API returns 404.
- Edge case: request exists but has no tracks → CLI prints plan with no track-fallback slots; no error.
- Integration: dry-run for request 1868 (Kid A) shows the new generator's plan side-by-side with the active plan's `search-plan show` output.
- `TestRouteContractAudit` passes with the new route classified.
- Covers AE6.

**Verification:**
- Pyright clean.
- All tests pass; `TestRouteContractAudit` doesn't fail.
- Manual: `pipeline-cli search-plan dry-run 1868` produces a plan that contains literal-title slot, format-hint slots, artist-prepended tracks, and (post-U3 backfill) a release_group_year slot (2000 ≠ 2008).

---

### U7. Saturation telemetry CLI/API

**Goal:** Add an aggregator method on `PipelineDB` that returns per-request saturation rate (rows where `final_state LIKE '%LimitReached%'` / total rows in a window) and total `pre_filter_skip_count` (from U2). Expose via `pipeline-cli search-plan saturation <id>` and a parallel API endpoint.

**Requirements:** R12.

**Dependencies:** U2 (uses `pre_filter_skip_count` column).

**Files:**
- Modify: `lib/pipeline_db.py` — add `get_saturation_summary(request_id, window_days=14) -> SaturationSummary` method
- Modify: `lib/search_plan_service.py` (or new service file) — service-layer wrapper around the DB method
- Modify: `scripts/pipeline_cli.py` — add `search-plan saturation` subcommand
- Modify: `web/routes/pipeline.py` — add `GET /api/search-plan/<id>/saturation` endpoint
- Modify: `tests/test_web_server.py::TestRouteContractAudit.CLASSIFIED_ROUTES` — add the new route
- Modify: `tests/fakes.py::FakePipelineDB` — add `get_saturation_summary` stub
- Test: `tests/test_pipeline_db.py`, `tests/test_pipeline_cli.py`, `tests/test_search_plan_service.py`, `tests/test_web_server.py`

**Approach:**
- `SaturationSummary` is a typed dataclass: `total_searches: int`, `saturated_searches: int`, `saturation_rate: float`, `total_pre_filter_skips: int`, `window_days: int`.
- Single SQL query aggregates over `search_log` for the request in the window.
- CLI subcommand prints a human-readable summary; API returns JSON.
- This is the data layer for the future dashboard work; the dashboard brainstorm at `docs/brainstorms/2026-05-09-search-plan-per-request-dashboard-requirements.md` consumes these endpoints when next iterated.

**Execution note:** Write the contract test with `REQUIRED_FIELDS = {"total_searches", "saturated_searches", "saturation_rate", "total_pre_filter_skips", "window_days"}` first.

**Patterns to follow:**
- `lib/pipeline_db.py` existing aggregator method patterns.
- CLI⇄API symmetry per `CLAUDE.md`: thin CLI wrapper + thin API handler around a single service-layer method.
- Production-shaped mock rows in contract tests per `.claude/rules/code-quality.md` (timestamps as `datetime.datetime`, UUIDs as `uuid.UUID`).
- `TestRouteContractAudit.CLASSIFIED_ROUTES` registration.

**Test scenarios:**
- Happy path: request with 30 searches in window, 10 saturated, 50 pre-filter skips → returns `total_searches=30, saturated_searches=10, saturation_rate=0.333, total_pre_filter_skips=50, window_days=14`.
- Edge case: request with no searches in window → returns zeros, saturation_rate=0.0 (not NaN).
- Edge case: request id not found → CLI exits 2; API returns 404.
- Window parameter respected: `window_days=7` filters correctly.
- Production-shaped mock: contract test populates rows with `datetime.datetime` timestamps and `uuid.UUID` IDs, not synthetic str/int.
- `TestRouteContractAudit` passes with new route classified.
- CLI exit codes match the convention (0 success, 2 not_found).

**Verification:**
- Pyright clean.
- All tests pass.
- Manual: `pipeline-cli search-plan saturation 1868` returns Kid A's saturation rate (currently very high; expected to drop post-deploy).
- Manual: `pipeline-cli search-plan saturation 1640` returns Darren Hanlon's saturation rate (lower; this is the niche case).

---

## System-Wide Impact

- **Interaction graph:**
  - U1 changes `check_for_match` behavior for every search that produces results. Affects `cratedigger.py::_collect_search_results`, `find_download`, and downstream forensics.
  - U5 invalidates every active search plan on the next cycle (via GENERATOR_ID bump). The `cratedigger.service` 5-min timer regenerates them on first encounter.
  - U7's new endpoint is consumed by the future dashboard work but otherwise has no production caller.
- **Error propagation:**
  - U3 backfill: errors per-row (e.g. MB mirror 404) log and continue; rows left NULL. The backfill never aborts the deploy.
  - U4 enqueue: MB mirror lookup failure leaves `release_group_year=NULL` on the new request; generator handles NULL gracefully (no extra slot).
  - U5 generator: pure function over snapshot; failures bubble up to `SearchPlanService` which handles plan-generation failures via existing patterns (transient errors retried; deterministic errors marked sticky).
- **State lifecycle risks:**
  - **Plan invalidation spike on deploy.** All active plans get regenerated on first encounter post-`SEARCH_PLAN_GENERATOR_ID` bump. The 5-min cycle's existing wave caps bound the spike. Worth monitoring journalctl for the first 1-2 cycles post-deploy.
  - **Browse load spike on first cycle for currently-saturated requests.** With the asymmetric `2n` pre-filter, more peers per request get browsed on first cycle. Subsequent cycles hit the 7-day Redis cache and are cheap. Bounded by `browse_top_k` wave cap.
  - **Backfill duration on deploy.** ~4000 requests × MB mirror call. All local I/O. Estimated 5-10 min wall clock. Runs as part of `cratedigger-db-migrate.service` (or invoked immediately after).
- **API surface parity:**
  - U6 and U7 ship CLI subcommands AND API endpoints per `CLAUDE.md` § CLI ⇄ API surface symmetry.
  - Both new routes registered in `TestRouteContractAudit.CLASSIFIED_ROUTES`.
- **Integration coverage:**
  - Slice test in `tests/test_integration_slices.py`: end-to-end "request enters pipeline → generator emits new plan → matcher applies asymmetric pre-filter → forensics record skips" to validate the full chain.
- **Unchanged invariants:**
  - The post-browse strict-count gate (`tracks_info["count"] != track_num`) at `lib/matching.py:451` is unchanged. The asymmetric pre-filter relaxation does NOT relax exact-fit at scoring time.
  - `negative_matches` cache scope (per-cycle) is unchanged.
  - `album_match` filename-similarity scoring is unchanged.
  - `search_log` outcome enum is unchanged (no new outcomes — skips are recorded via the count column + flagged candidates, not a new outcome value).
  - Quality-decision pipeline (`full_pipeline_decision_from_evidence`) is unchanged.
  - The persisted search-plan model (snapshot signature, cursor, cycle wrap) is unchanged; new snapshot field (`release_group_year`) is additive.

---

## Risks & Dependencies

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Browse load spike on first post-deploy cycle for currently-saturated requests | Med | Med | Existing `browse_top_k` wave cap bounds per-cycle browse work; 7-day Redis cache makes subsequent cycles cheap. Monitor journalctl for first 1-2 cycles post-deploy. |
| MB mirror does not expose release-group year directly | Med | Low | Fall back to `min(year)` over the release-group's release list; both paths are bounded local I/O. Implementation choice in U3. |
| Backfill takes longer than expected and delays cratedigger service startup | Low | Med | Make backfill a separate one-shot unit that runs alongside (not before) `cratedigger.service`. Service starts even if backfill is in-flight; generator handles NULL `release_group_year` gracefully. |
| Generator regenerates 4000+ plans on next cycle producing transient compute spike | High (expected) | Low | This is the designed behavior of GENERATOR_ID bumps. Past bumps (2026-05-08) handled 580+ plans cleanly. Wave caps bound per-cycle work. |
| Self-titled token-subset detection produces false positives | Low | Low | Token-subset detection requires the smaller token set to be a strict subset of the larger AND non-trivially short. Edge cases caught by test scenarios. Operators can override via existing `search_filetype_override` mechanism if needed. |
| New `pre_filter_skip_count` column grows search_log row size meaningfully | Very Low | Very Low | INTEGER column is 4 bytes. ~1M rows/year = 4 MB. Negligible. |
| Candidates JSONB size increases beyond ~20 rows post-flagging | Low | Low | Cap at ~15 scored + ~5 skipped = 20 total, matches current behavior. Exact partition tunable during U2. |
| Plan size variability (new conditional rg_year slot) breaks downstream consumers expecting fixed-size plans | Low | Low | Plan size is already variable today (requests without year skip `unwild_year`; requests with fewer tracks emit fewer track slots). No fixed-size consumer exists. |

---

## Documentation / Operational Notes

- Bump `SEARCH_PLAN_GENERATOR_ID` to `"search-plan/2026-05-19-1"` (or similar dated value).
- Update `docs/persisted-search-plans-rollout.md` post-deploy with notes on the generator-ID bump and observed plan regeneration cycle.
- Add a brief note to `docs/musicbrainz-mirror.md` documenting the release-group year fetch path used by U3 + U4.
- Post-deploy: run `pipeline-cli search-plan dry-run` on the starving cohort (1868, 455, 2125, 3018, 1640) and capture the output as evidence the generator changes reached them.
- Monitor `journalctl -u cratedigger -f` for the first 1-2 post-deploy cycles to confirm the browse load spike is bounded.
- Two weeks post-deploy: re-run the starving-cohort SQL query from the brainstorm and verify the cohort has shrunk by ≥50%.

---

## Sources & References

- **Origin document:** `docs/brainstorms/2026-05-19-search-plan-entropy-requirements.md`
- Related brainstorms: `docs/brainstorms/2026-05-08-persisted-search-plans-requirements.md`, `docs/brainstorms/2026-05-09-search-plan-per-request-dashboard-requirements.md`
- Related plans: `docs/plans/2026-05-05-001-feat-peer-cache-redis-migration-plan.md` (the 7-day Redis browse cache underpinning the `2n` decision)
- Related code: `lib/matching.py::check_for_match`, `lib/search.py::generate_search_plan`, `lib/search_plan_service.SearchPlanService`, `lib/enqueue.py::_planned_grab_entry`
- Related rules: `.claude/rules/code-quality.md` (CLI ⇄ API symmetry, Simulator-First TDD, Wire-boundary types, API Contract Tests), `CLAUDE.md` (DB migrations, deploy flow)

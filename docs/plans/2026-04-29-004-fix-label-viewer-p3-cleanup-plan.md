---
title: "fix: label viewer P3 cleanup consolidation"
type: fix
status: implemented
date: 2026-04-29
origin: docs/plans/2026-04-29-001-feat-label-viewer-phase-a-plan.md
---

# fix: label viewer P3 cleanup consolidation

**Target repos:** `cratedigger` (this repo) and `discogs-api` (`~/discogs-api/`). Most items are cratedigger-side; the `via_label_id` rename touches both repos.

## Overview

Consolidation PR for the P3 cluster from the Phase A code review (`docs/plans/2026-04-29-001-feat-label-viewer-phase-a-plan.md` lines 530-541) plus the two pre-existing items called out at lines 539-541. Each item is small; landing them as a single consolidation PR keeps the review surface coherent and avoids the per-item ceremony tax.

**Implementation note:** after final review, this PR also folded in the P1/P2
fixes and the remaining P3 review items. The `via_label_id` rename shipped as
an additive compatibility window (`label_id` and `via_label_id` both emitted and
accepted), release search cache keys are bounded like artist/label search keys,
label `per_page` is aligned to the mirror's 100-row cap, and clearing the
browse search box cancels stale in-flight search responses.

Eight items, grouped into two natural commits:

**Commit A — cratedigger Python + tests:**
1. Rename `overlay_release_rows` → `overlay_release_rows_in_place` to make the mutation contract explicit.
2. Add direct unit test for `overlay_release_rows_in_place`.
3. Pass `sub_labels` through from the discogs-api response to the cratedigger label-detail payload (currently dropped).
4. Adapter `label_id` defense-in-depth: assert `str(label_id).isdigit()` before URL interpolation.
5. Cap user query length on `search_labels` and `search_artists` cache keys (200 chars).

**Commit B — cratedigger JS + the cross-repo rename:**
6. Year-input debounce in `web/js/labels.js`.
7. In-flight token guard in `openLabelDetail` (and apply the same fix to `searchArtists` — pre-existing race noted in the review).
8. Rename `via_label_id` → `label_id` in discogs-api response shape, with coordinated cratedigger Python + JS updates.

If item 8 turns out to have wider blast radius than expected during implementation (e.g. it ships in a public-ish doc), drop it from this PR and float as a separate plan; the other seven are independent and ship cleanly.

---

## Problem Frame

These are the items the post-merge review flagged as worth fixing but not blocking. They share three traits:

- Low individual value (none of them are user-visible behavior changes).
- Some of them are mild code smells (implicit mutation contract, unnamed test gap, redundant fields), some are mild robustness gaps (no debounce, no in-flight token, unbounded cache keys, raw URL interpolation), and one is a naming consistency issue.
- Bundled, they make a tidy cleanup PR; spread across separate PRs they create more reviewer overhead than they save.

Origin: `docs/plans/2026-04-29-001-feat-label-viewer-phase-a-plan.md` lines 530-541.

The review flagged the `overlay_release_rows` mutation as "implicit" because the function name doesn't signal in-place behavior, even though the docstring explicitly says "in place". Renaming forces every caller to acknowledge the contract.

The `searchArtists` race is older than Phase A (the review marked it "pre-existing"), but the same fix shape — an in-flight request token — applies to both `searchArtists` and `openLabelDetail`. Bundling them in one commit means a single token-tracking pattern lands consistently in both call sites.

---

## Requirements Trace

- R1. `overlay_release_rows` is renamed to make in-place mutation explicit. → U1
- R2. `overlay_release_rows_in_place` has a direct unit test (per `.claude/rules/code-quality.md` "every pure function must have direct unit tests"). → U2
- R3. The discogs-api `LabelDetail.sub_labels` field reaches the cratedigger label-detail JSON payload. → U3
- R4. The cratedigger Discogs adapter validates `label_id` shape before URL interpolation. → U4
- R5. Cache keys for `search_labels` and `search_artists` cap user-supplied query length. → U5
- R6. Year-filter inputs debounce before triggering re-render. → U6
- R7. `openLabelDetail` and `searchArtists` discard responses to stale requests via an in-flight request token. → U7
- R8. The discogs-api `via_label_id` field is renamed to `label_id` for naming consistency, with coordinated consumer updates. → U8 (optional — drop from this PR if blast radius widens during implementation)

---

## Scope Boundaries

- The P0 (connection pool) and P1+P2 (pagination, CTE cycle, nullable types, 503 fallback) clusters live in plans `2026-04-29-002` and `2026-04-29-003`. This plan does NOT touch them.
- BIG_LABEL_THRESHOLD is duplicated between `web/routes/labels.py:37` and `web/js/labels.js:28`. The review didn't flag it; if a single source of truth feels natural during implementation of one of the units, fold it in. Otherwise leave it.
- No new endpoints. No schema changes. No new tests beyond U2 and any contract additions for U3.
- The `via_label_id` rename (U8) is the single highest-impact item in this plan because it crosses repos and the JSON wire format. Treat as breaking change. Drop from the PR if implementation reveals consumers we missed.

### Deferred to Follow-Up Work

- BIG_LABEL_THRESHOLD constant deduplication (low priority; fold in if convenient).
- Any deeper rework of the search-results UX (out of scope — only the stale-result race is in scope).

---

## Context & Research

### Relevant Code and Patterns

**[U1 + U2] `overlay_release_rows`** — `web/routes/_overlay.py:21-66`:
```python
def overlay_release_rows(rows: list[dict], release_ids: Iterable[str]) -> None:
    """Annotate each release row with library + pipeline state in place.
    ...
    """
    from web import server as srv
    ids_list = list(release_ids)
    in_library = srv.check_beets_library(ids_list) if ids_list else set()
    ...
    for r in rows:
        rid = r["id"]
        r["in_library"] = rid in in_library
        r["beets_album_id"] = beets_ids.get(rid)
        ...
```

Callers (verified during research):
- `web/routes/labels.py:131` — `overlay_release_rows(releases, [r["id"] for r in releases])`
- The function is also referenced from artist-view release endpoints in `web/routes/browse.py` (verify all call sites in U1).

No direct test today. Indirect coverage at `tests/test_web_server.py:2878-2935::test_label_detail_overlay_integration` patches `check_beets_library`/`check_pipeline`/`_beets_db` and exercises the route end-to-end.

**[U3] `sub_labels` passthrough** — `web/routes/labels.py:140`:
```python
    sub_labels: list[dict] = []
```

The discogs-api response from `/api/labels/{id}` already populates `LabelDetail.sub_labels: Vec<SubLabel>` (`discogs-api/src/types.rs:280`). The cratedigger adapter's `_DiscogsLabelDetail` Struct in `web/discogs.py` decodes this. The route handler currently elides it.

The frontend (`web/js/labels.js`) does not render sub-labels today, but the contract test for `LABEL_DETAIL_RESPONSE_REQUIRED_FIELDS` already includes `"sub_labels"` (`tests/test_web_server.py:2785`), which means the field IS in the response — just always empty. This unit makes it actually populated.

**[U4] Adapter `label_id` validation** — `web/discogs.py:502, 528`:
```python
    raw = _get(f"{DISCOGS_API_BASE}/api/labels/{label_id}")
    ...
    f"{DISCOGS_API_BASE}/api/labels/{label_id}/releases"
```

Route regex `^/api/discogs/label/(\d+)$` (`web/routes/labels.py:159`) already enforces digits at the entry point. U4 is defense-in-depth at the adapter layer to protect any future caller (CLI tool, internal script, future MB code path) that imports `web.discogs.get_label` directly without the route gate.

**[U5] Cache key length cap** — `web/discogs.py:158, 489`:
```python
# search_artists (line 158):
return _cache.memoize_meta(f"discogs:search:artists:{query}", _fetch)

# search_labels (line 489):
cache_key = f"discogs:search:labels:{query}:p={page}:pp={per_page}"
```

Both interpolate the raw query string. A pathological 10KB query produces a 10KB Redis key. Cap at 200 chars; truncated key still works because the upstream still receives the full query (the cache key is only an in-memory + Redis lookup).

**[U6] Year-input debounce** — `web/js/labels.js:444-450`:
```html
<input type="number" id="label-year-min" ... oninput="window.onLabelFilterChange()">
<span style="color:#666;">–</span>
<input type="number" id="label-year-max" ... oninput="window.onLabelFilterChange()">
```

`onLabelFilterChange` (lines 532-557) runs a synchronous filter + DOM re-render. Light at 100 rows; will matter once U1 of the P1 plan lands and pagination grows the list.

**[U7] In-flight token race** — two call sites:

`web/js/labels.js:308-347::openLabelDetail`:
```javascript
export async function openLabelDetail(labelId, labelName) {
  state.browseLabel = { id: labelId, name: labelName };  // sync state mutation
  state.browseSubView = 'label';
  ...
  let payload = await loadLabelReleases(labelId, { include_sublabels: true });
  ...
  renderLabelDetail(body, payload);
```

`web/js/browse.js:431-488::searchArtists` — same pattern, no token, no AbortController.

The fix: a module-scoped counter token, stamped on each fetch, checked before render. Stale responses are silently discarded.

**[U8] `via_label_id` rename** — five call sites in discogs-api:
- `discogs-api/src/db.rs:1055, 1062, 1082, 1122` (SQL alias + struct hydration)
- `discogs-api/src/types.rs:310` (struct field)

Plus consumers in cratedigger Python:
- `web/discogs.py:386` (msgspec.Struct field)
- `web/discogs.py:549` (adapter passthrough)

The frontend (`web/js/labels.js:394, 492`) reads only `sub_label_name` (the derived field), not `via_label_id` directly. So the JS can stay untouched.

The rename is mechanically simple: 5 + 2 = 7 call sites. The risk is JSON contract drift — any third party hitting `/api/labels/{id}/releases` and depending on the field name breaks. The mirror is single-tenant (cratedigger), so the blast radius is contained.

### Institutional Learnings

- The `code-quality.md` rule mandates direct unit tests for pure functions. `overlay_release_rows` slipped through Phase A because it has integration coverage; U2 closes the gap and serves as a reminder for future audit sweeps.
- The "stale request token" pattern doesn't exist anywhere in `web/js/` today. Once U7 lands, future fetch-on-input UI surfaces should follow the same pattern. Worth a sentence in `.claude/rules/web.md` once shipped.

### External References

None used — all changes are internal hardening.

---

## Key Technical Decisions

- **Rename over docstring tightening** for `overlay_release_rows`. The existing docstring already says "in place." A reader scanning a call site sees the function name, not the docstring. The Python idiom is `_in_place` suffix or returning a new value; we pick the suffix because every caller already discards the return value.
- **`sub_labels` passes through unchanged.** No transformation, no field renaming. The Phase A contract test already requires the field; this just stops returning an empty list.
- **`assert str(label_id).isdigit()` over a typed parser.** The route regex enforces digits already. This is defense-in-depth for any future direct caller. A bare `assert` (rather than raising a typed exception) is fine — the function isn't reached on a malformed ID in practice.
- **Cap cache-key query length at 200 chars.** Long enough for any real label/artist name. Short enough that no single key blows up Redis. Truncate with a deterministic suffix (`f"{query[:200]}:#{len(query)}"`) so two distinct long queries don't collide on the same key.
- **300ms debounce on year inputs.** Standard UX number for type-and-pause. Fast enough not to feel sluggish, slow enough to coalesce keystroke bursts.
- **Module-scoped integer token for in-flight tracking.** Increment on each fetch, capture in closure, check before render. Discards stale responses without aborting them — simpler than `AbortController`, no fetch-spec quirks.
- **`via_label_id` rename uses serde rename, not column rename.** `#[serde(rename = "label_id")]` on the struct field, leave the SQL alias unchanged. Reduces diff surface inside the recursive CTE and keeps the SQL self-documenting (the column IS the via path through the tree). The wire contract changes; the Rust internals don't.

---

## Open Questions

### Resolved During Planning

- Naming pattern for the renamed mutation function → `_in_place` suffix.
- Cache-key cap → 200 chars with length-suffix dedup.
- Debounce duration → 300ms.
- In-flight pattern → module-scoped integer token (not AbortController).
- `via_label_id` rename strategy → serde rename, leave SQL.

### Deferred to Implementation

- Whether to also bundle BIG_LABEL_THRESHOLD deduplication. Inspect during U6/U7 — if the JS file is already being touched and a clean shared-constants spot exists, fold it in. Otherwise defer.
- Whether to extract a shared `withInFlightToken(fn)` helper or inline the pattern in both `openLabelDetail` and `searchArtists`. Inline first; extract if a third call site shows up during implementation.
- U8 cross-repo coordination — bundle into one PR pair (discogs-api + cratedigger), or split? Decide during implementation. The serde rename means the struct can keep the old field name internally and serialize the new name; cratedigger adapter updates the input field name. The two PRs land in lockstep but can be reviewed separately.

---

## Implementation Units

- U1. **[cratedigger] Rename `overlay_release_rows` → `overlay_release_rows_in_place`**

**Goal:** Make the in-place mutation explicit at the call site.

**Requirements:** R1.

**Dependencies:** None.

**Files** *(target repo: cratedigger):*
- Modify: `web/routes/_overlay.py` (rename function; keep docstring)
- Modify: every caller (verify with `rg "overlay_release_rows" web/`):
  - `web/routes/labels.py:131`
  - any artist-view callers in `web/routes/browse.py` (sweep)
- Modify: any test references in `tests/` (sweep)

**Approach:**
- Single rename. No behavior change. No new wrapper.
- The function still returns `None`; the docstring still says "in place."
- Sweep callers with `rg`; the existing test integration in `test_web_server.py:2878-2935` will fail loudly if a caller is missed (the patched `check_beets_library` won't be invoked).

**Patterns to follow:**
- Same shape as other `_in_place` Python helpers (e.g. `random.shuffle`, `list.sort` mental model).

**Test scenarios:**
- Existing `test_label_detail_overlay_integration` continues to pass after the rename — proves no caller was missed.
- `pyright` clean across `web/routes/`.

**Verification:**
- `nix-shell --run "bash scripts/run_tests.sh"` passes.
- `nix-shell --run "pyright web/routes/_overlay.py web/routes/labels.py web/routes/browse.py"` clean.

---

- U2. **[cratedigger] Direct unit test for `overlay_release_rows_in_place`**

**Goal:** Close the test gap flagged in P3 — `.claude/rules/code-quality.md` mandates direct unit tests for pure functions, and overlay_release_rows currently has only integration coverage.

**Requirements:** R2.

**Dependencies:** U1 (so the test name uses the renamed function).

**Files** *(target repo: cratedigger):*
- Create: `tests/test_overlay.py` (new file, mirrors the layout of `tests/test_web_server.py` for similar scope)
- Or modify: `tests/test_web_server.py` (add a `TestOverlayReleaseRowsInPlace` class). Pick whichever colocates better with the existing overlay integration test.

**Approach:**
- Patch `web.server.check_beets_library`, `web.server.check_pipeline`, `web.server._beets_db`, `web.server.compute_library_rank` — same pattern as the integration test.
- Test scenarios cover: in-library row, in-pipeline row, both, neither, missing `id` key (raises KeyError per the docstring), empty rows list, empty release_ids.
- This is pure-function-with-mocked-edges territory — fits the "pure function tests" category in `.claude/rules/code-quality.md`.

**Patterns to follow:**
- `tests/test_web_server.py:2878-2935::test_label_detail_overlay_integration` for the patch shape.
- `tests/test_quality_decisions.py` for `subTest()`-based decision-matrix testing if the scenarios benefit from a table.

**Test scenarios:**
- Happy path: a row with id in `in_library` set; assert `r["in_library"] == True`, `r["library_format"]` populated.
- Happy path: a row with id in `in_pipeline` map; assert `r["pipeline_status"]` populated.
- Both: row in both library and pipeline; assert both fields populated.
- Neither: row in neither; assert `in_library == False`, `pipeline_status is None`, `pipeline_id is None`.
- Edge case: empty `rows` list and empty `release_ids` — function returns without error.
- Edge case: row with `id` not in any backend response; non-overlay fields preserved unchanged.
- Edge case: `quality.get(rid)` returns dict with non-int `beets_bitrate` (e.g. None) — assert defensive coercion sets `library_min_bitrate = 0`.

**Verification:**
- New test file passes via `nix-shell --run "python3 -m unittest tests.test_overlay -v"`.
- Coverage for `overlay_release_rows_in_place` reaches all branches.

---

- U3. **[cratedigger] Pass `sub_labels` through from discogs-api response**

**Goal:** The label-detail JSON payload exposes the populated `sub_labels` list instead of an empty placeholder.

**Requirements:** R3.

**Dependencies:** None.

**Files** *(target repo: cratedigger):*
- Modify: `web/routes/labels.py:140` (replace `sub_labels: list[dict] = []` with the actual list from the adapter response)
- Modify: `web/discogs.py::get_label` (verify the adapter already passes `sub_labels` from the discogs-api response — it should, since `LabelDetail.sub_labels` is already in the `_DiscogsLabelDetail` Struct; if not, plumb through)
- Modify: `tests/test_web_server.py::TestLabelRouteContracts` (add test asserting `sub_labels` is populated for a label that has children — patch the adapter to return non-empty `sub_labels` and verify the route forwards them)

**Approach:**
- Each `sub_labels` entry is `{id: int, name: str, release_count: int}` per `discogs-api/src/types.rs::SubLabel`. The cratedigger contract test asserts the field exists; this unit asserts it can be non-empty.
- The frontend doesn't render `sub_labels` today. Phase B may. Either way, the field is populated and the contract is honest.
- No new UI rendering — that's outside this plan's scope.

**Patterns to follow:**
- Existing adapter passthrough patterns in `web/discogs.py` (e.g. `parent_label_name` is denormalized through unchanged).

**Test scenarios:**
- Happy path: label with sub-labels in the mirror returns a populated `sub_labels` list in the JSON payload.
- Happy path: label without sub-labels returns `sub_labels: []` (empty list, not absent key).
- Contract: `LABEL_DETAIL_RESPONSE_REQUIRED_FIELDS` still asserts the field exists.

**Verification:**
- Cratedigger contract tests pass.
- `curl https://music.ablz.au/api/discogs/label/{warp_id}` shows `sub_labels` populated with Warp Singles, Arcola, etc.

---

- U4. **[cratedigger] Defense-in-depth `label_id` validation in adapter**

**Goal:** `web/discogs.py::get_label` and `get_label_releases` reject non-numeric `label_id` before URL interpolation.

**Requirements:** R4.

**Dependencies:** None.

**Files** *(target repo: cratedigger):*
- Modify: `web/discogs.py::get_label` (add `assert str(label_id).isdigit()` at top)
- Modify: `web/discogs.py::get_label_releases` (same)
- Modify: `tests/test_discogs_api.py` (add tests asserting `AssertionError` for non-numeric inputs)

**Approach:**
- Single-line guard, no behavior change for valid inputs.
- The route regex enforces this at the entry point; the assert is defense for any future direct caller.
- Use `assert` rather than `raise ValueError` — calling code never hits this in production, so the assert is fine. If you prefer `ValueError` for explicitness, that's also acceptable; pick one and document.

**Patterns to follow:**
- Other input-validation patterns in `web/discogs.py` (verify during implementation).

**Test scenarios:**
- Happy path: `get_label(123)` and `get_label("123")` both work.
- Edge case: `get_label("../etc/passwd")` raises `AssertionError`.
- Edge case: `get_label("123 OR 1=1")` raises `AssertionError`.

**Verification:**
- New unit tests pass.
- Existing label-flow tests still pass.

---

- U5. **[cratedigger] Cache-key length cap on label/artist search**

**Goal:** A 10KB user query cannot create a 10KB cache key.

**Requirements:** R5.

**Dependencies:** None.

**Files** *(target repo: cratedigger):*
- Modify: `web/discogs.py::search_labels:489` (cap query in cache key at 200 chars; suffix with `:#{len(query)}` to dedup distinct long queries)
- Modify: `web/discogs.py::search_artists:158` (same)
- Modify: `tests/test_discogs_api.py` (assert that two queries differing past the 200-char boundary produce different cache keys)

**Approach:**
- The cache key is a string formatted from `query`. Replace `{query}` with `{query[:200]}:#{len(query)}` so:
  - Short queries: `discogs:search:labels:hymen:#5:p=1:pp=25` (the `:#5` is harmless).
  - Long queries: `discogs:search:labels:[200 chars]:#10000:p=1:pp=25` — distinct from another query that shares the first 200 chars but has a different total length.
- The `_get` URL still receives the full `query` — only the cache lookup key is truncated.
- Optionally normalize whitespace / lowercase before truncation if the existing keys do. Match the existing pattern.

**Patterns to follow:**
- The existing `f"discogs:search:..."` cache key shapes — same prefix, just truncated middle.

**Test scenarios:**
- Happy path: short query produces the same cache key as today, with a `:#N` suffix appended.
- Edge case: two 250-char queries differing only in the last 50 chars produce DIFFERENT keys (because the length matches but the truncated prefix differs at char ≤ 200, OR they differ in length, in which case the suffix differs).
- Edge case: two queries that share the first 200 chars and have identical lengths past 200 collide on the cache key. This is acceptable (effectively impossible in practice; cache miss is the worst case).

**Verification:**
- New unit tests pass.
- No regression in existing label/artist search tests.

---

- U6. **[cratedigger] 300ms debounce on year-filter inputs**

**Goal:** Typing in the year filter doesn't thrash the DOM on every keystroke.

**Requirements:** R6.

**Dependencies:** None.

**Files** *(target repo: cratedigger):*
- Modify: `web/js/labels.js` (add a small `debounce(fn, ms)` helper at module top; wrap `onLabelFilterChange` invocations from year inputs)
- Optional modify: `web/js/util.js` (if a shared debounce helper feels right; otherwise inline in `labels.js`)

**Approach:**
- Standard 300ms trailing-edge debounce. Coalesces rapid keystrokes into one filter operation.
- Format / hide-held inputs stay synchronous — they're click-driven, not type-driven.
- Pure JS; tested via `tests/test_js_util.mjs` if the helper is extracted.

**Patterns to follow:**
- If `web/js/util.js` already has a debounce or throttle helper, reuse. Otherwise, write the standard 5-line setTimeout pattern inline.

**Test scenarios:**
- Pure JS test (if helper extracted): rapid 5-keystroke burst within 300ms results in a single function invocation.
- Manual: rapidly type and delete in the year input — the release list re-renders only after a 300ms pause.

**Verification:**
- `node --check web/js/labels.js` clean (per `.claude/rules/web.md`).
- Manual smoke after deploy.

---

- U7. **[cratedigger] In-flight token guard for `openLabelDetail` and `searchArtists`**

**Goal:** A late response from a stale request cannot stomp a fresher render.

**Requirements:** R7.

**Dependencies:** None.

**Files** *(target repo: cratedigger):*
- Modify: `web/js/labels.js::openLabelDetail` (module-scoped counter token; stamp before fetch; check before render)
- Modify: `web/js/browse.js::searchArtists` (same pattern)
- Modify: `tests/test_js_util.mjs` if the pattern extracts cleanly into a helper

**Approach:**
- Module-scoped `let openLabelToken = 0;` (and similar for artist search). At fetch start, `const myToken = ++openLabelToken; const result = await fetch(...); if (myToken !== openLabelToken) return;`.
- Each new fetch invalidates earlier ones. Late responses are discarded silently.
- No `AbortController` — simpler, fewer browser-quirk edges.
- Apply the same pattern to `searchArtists` since it has the same race (called out as "pre-existing" in the original review).

**Patterns to follow:**
- Whatever `web/js/discography.js::loadArtistDiscography` does for race-handling, if anything (verify during implementation; if it has the same race, fix in this unit too).

**Test scenarios:**
- Pure JS test (if helper extracted): a token-guarded async function discards results when a newer call is made before the original resolves.
- Playwright integration: rapidly click two different labels in the search results in quick succession; the page that lands matches the second click, regardless of which response arrives first.
- Playwright integration: rapidly type two artist queries in succession; the rendered results match the latest query.

**Verification:**
- Playwright walkthrough on `https://music.ablz.au` after deploy.
- Manual: open browser devtools network panel, throttle to "Slow 3G," click two labels in <100ms, verify the second label's content is what renders.

---

- U8. **[discogs-api + cratedigger] Rename `via_label_id` → `label_id` (optional — drop if blast radius widens)**

**Goal:** Wire-format consistency — the field that identifies which label in the tree a release is attached to is called `label_id` everywhere, matching the rest of the schema.

**Requirements:** R8.

**Dependencies:** None on other units in this plan, but coordinated cross-repo.

**Files:**

*Target repo: discogs-api:*
- Modify: `src/types.rs::LabelReleaseEntry::via_label_id` — add `#[serde(rename = "label_id")]` (or rename the Rust field too — pick one approach during implementation)

*Target repo: cratedigger:*
- Modify: `web/discogs.py:386` (rename `via_label_id` to `label_id` in the msgspec.Struct)
- Modify: `web/discogs.py:549` (rename in adapter passthrough)
- Verify: `web/js/labels.js:394, 492` only reads `sub_label_name` (already verified — derived field, no change needed)

**Approach:**
- Coordinated PR pair. discogs-api ships first with the serde rename (the wire format changes from `"via_label_id"` to `"label_id"`); cratedigger ships second with the consumer-side rename.
- The discogs-api side can keep the Rust struct field name (`via_label_id`) and use `#[serde(rename = "label_id")]` to change only the JSON; reduces diff surface inside the SQL CTE.
- If implementation reveals additional consumers (third-party tools, internal scripts), drop this unit from the PR and float as its own plan.

**Approach checkpoint:** Before committing, run `rg "via_label_id" ~/discogs-api/ ~/cratedigger/` and confirm only the named files reference it. If anything unexpected turns up, drop U8.

**Patterns to follow:**
- Other `#[serde(rename = "...")]` annotations in `discogs-api/src/types.rs` (verify presence during implementation).
- Existing msgspec.Struct field renames in cratedigger Python (verify during implementation).

**Test scenarios:**
- Smoke (post-deploy of discogs-api): `curl https://discogs.ablz.au/api/labels/{warp_id}/releases` shows `"label_id": <int>` per row, no `via_label_id` key.
- Smoke (post-deploy of cratedigger): `curl https://music.ablz.au/api/discogs/label/{warp_id}` returns the same payload as before — sub-label rendering unchanged.
- Cratedigger contract tests still pass.
- The strict-boundary regression test (`msgspec.ValidationError` on unknown fields) catches any consumer that didn't get updated.

**Verification:**
- Both deploys land in lockstep.
- `journalctl -u cratedigger` shows no `msgspec.ValidationError` on label routes after deploy.
- UI walkthrough: load Warp Records, sub-label badges still render.

---

## System-Wide Impact

- **Interaction graph:** U1, U2, U6, U7 are local refactor / hardening. U3 fills an existing contract field. U4 + U5 are defense-in-depth. U8 changes the JSON wire format on a single endpoint with a single declared consumer.
- **Error propagation:** U7 changes failure semantics for stale requests — late responses now silently fail instead of stomping the UI. No new error surfaces; users see the latest fetch's result.
- **State lifecycle risks:** U7's module-scoped token is the only stateful change. It's intentionally simple (an integer counter); no leak risks because it lives forever in the module closure and resets on page reload.
- **API surface parity:** U3 surfaces a populated `sub_labels` list; the contract field already existed (was just empty). U8 is the only breaking-format change in the plan; mitigated by single-tenant mirror + coordinated deploys.
- **Integration coverage:** U2 fills the test gap directly. U7's playwright walkthroughs cover the race condition. The existing label-detail integration test continues to gate the route layer.
- **Unchanged invariants:** Beets DB queries, pipeline DB schema, slskd integration, harness, quality model, all artist/release rendering paths.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| U1 rename misses a caller, causing an `AttributeError` at runtime | `rg` sweep + the existing integration test fail loudly if missed. Pyright catches it at type-check time. |
| U2 unit test patches the wrong server symbol and over-asserts implementation | Mirror the existing integration test's patch shape. Assert observable mutations on the row dicts, not internal helper call counts. |
| U3 sub_labels populates with stale data because cratedigger caches the `get_label` response | The existing cache TTL applies; sub_labels are part of the same response. No new caching introduced. |
| U4 assert fires on a legitimate edge case (e.g. a future MB UUID-style label_id) | The function is Discogs-only; MB labels go through a different adapter (Phase B). If this assumption changes, the assert moves out and a typed parser replaces it. |
| U5 cache-key cap collides on long shared-prefix queries | Acceptable — extremely rare, worst case is cache miss. Length-suffix dedup minimizes the window. |
| U6 debounce makes rapid filter updates feel sluggish | 300ms is the standard responsive UX threshold; lower if testers feel it. |
| U7 token pattern doesn't compose if a future call site forgets to check | Acceptable — each call site is small and self-contained. Extract a helper if a third call site appears. |
| U8 wire-format rename breaks an undeclared consumer | Pre-rename `rg` sweep. If anything unexpected turns up, drop U8 from the PR and float as a separate plan. |

---

## Documentation / Operational Notes

- Standard deploy flow per `.claude/rules/deploy.md`.
- U8 requires coordinated discogs-api + cratedigger deploys. Land discogs-api first; the cratedigger Python adapter remains backward-compatible because msgspec strict-mode would only complain on missing fields (the new field IS present, the old field is gone — adapter must be updated in lockstep). If this risks more thrash than it's worth, drop U8.
- Once shipped, add a one-paragraph note to `.claude/rules/web.md` about the in-flight token pattern for fetch-on-input UI (U7), so future code follows the same convention.

---

## Sources & References

- **Origin plan:** `docs/plans/2026-04-29-001-feat-label-viewer-phase-a-plan.md` (P3 + pre-existing entries, lines 530-541)
- **Companion plans:**
  - `docs/plans/2026-04-29-002-fix-discogs-api-connection-pool-plan.md` (P0)
  - `docs/plans/2026-04-29-003-fix-label-viewer-p1-p2-plan.md` (P1 + P2)
- **Related code:**
  - `cratedigger/web/routes/_overlay.py:21-66` (overlay function — U1, U2)
  - `cratedigger/web/routes/labels.py:131, 140` (overlay caller, hardcoded sub_labels — U1, U3)
  - `cratedigger/web/discogs.py:158, 386, 489, 502, 528, 549` (cache keys, label_id interpolation, via_label_id field — U4, U5, U8)
  - `cratedigger/web/js/labels.js:308-347, 394, 444-450, 492` (openLabelDetail, year inputs, sub_label_name reader — U6, U7)
  - `cratedigger/web/js/browse.js:431-488` (searchArtists race — U7)
  - `discogs-api/src/db.rs:1055, 1062, 1082, 1122` (via_label_id call sites — U8)
  - `discogs-api/src/types.rs:310` (via_label_id struct field — U8)
  - `cratedigger/tests/test_web_server.py:2762-3067, 2878-2935` (label contract tests, overlay integration test — U2, U3)

---
title: "fix: label viewer P1 + P2 ship-fast cluster"
type: fix
status: implemented
date: 2026-04-29
origin: docs/plans/2026-04-29-001-feat-label-viewer-phase-a-plan.md
---

# fix: label viewer P1 + P2 ship-fast cluster

**Target repos:** `cratedigger` (this repo) and `discogs-api` (`~/discogs-api/`). Items per repo are tagged `[cratedigger]` / `[discogs-api]` in unit headers.

## Overview

Land the small, high-value follow-ups identified in Phase A's post-merge code review (P1 cluster + the cratedigger-side 503 handling from P2). These fixes ship independently of the P0 connection-pool plan (`docs/plans/2026-04-29-002-fix-discogs-api-connection-pool-plan.md`) — none of them require the pool, and most are local hardening rather than architectural change.

Five units, each small enough to land as one commit:
1. Forward `page`/`per_page` from the cratedigger label-detail route + add UI page controls.
2. Switch the recursive sub-label CTE from `UNION ALL` to `UNION` (cycle guard).
3. Promote nullable Rust label fields (`profile`, `contactinfo`, `data_quality`) from `String` to `Option<String>`.
4. Cratedigger-side 503 graceful retry on `get_label_releases` (P2 #6 from the original review). Stays useful even before the P0 pool plan lands — covers the post-pool 503 path and acts as a safety net for any current upstream timeout.
5. Eliminate the awkward "no results → retry without sublabels" branch in `web/js/labels.js::openLabelDetail` once the upstream contract is healthier.

The P0 connection-pool plan and this plan are independent — either can ship first. If the P0 plan ships first, U4 here lines up directly with the new 503 behavior; if this plan ships first, U4 is dormant for the actual 503 path but still defensive against urllib timeouts.

---

## Problem Frame

The Phase A review (`/tmp/compound-engineering/ce-code-review/20260429-ded45010/`) flagged five P1/P2 items that did not block merge but warrant follow-up. Each is small, but together they close measurable gaps:

- **R5 was advertised as "paginated for hundreds/thousands"** but `web/routes/labels.py` doesn't forward `page`/`per_page` to the adapter (which already supports them — see `web/discogs.py:510-579`). The UI shows "Showing first 100 of N" with no way to reach the rest.
- **The recursive sub-label CTE uses `UNION ALL`** (`discogs-api/src/db.rs:1041`). Discogs labels have no observed cycles in the dump, but a self-referencing `parent_label_id` would infinite-loop the query until either the CTE memory caps or — post-P0-plan — the 5s timeout fires. Switching to `UNION` deduplicates exact tuples and short-circuits any cycle for free.
- **`LabelDetail` and `LabelHit` type `profile`, `contactinfo`, `data_quality` as `String`** (`discogs-api/src/types.rs:261-282`). The schema (`src/schema.rs:32-39`) has `NOT NULL DEFAULT ''`, so `row.get::<_, String>(...)` works today, but a future schema change or an import-time data anomaly that leaves the cell NULL panics the handler. Cheap to make honest.
- **A 503 from the upstream Discogs mirror surfaces as a 500 to the cratedigger UI.** `web/routes/labels.py:111-128` catches only 404. The cratedigger UI shows a generic "Failed to load label" instead of degrading gracefully.
- **`web/js/labels.js::openLabelDetail` has a dead-feeling fallback** (`web/js/labels.js:332-336`): when the first call returns OK with empty results AND the label has `release_count > BIG_LABEL_THRESHOLD`, it retries with `include_sublabels=false`. The path doesn't fire today (the route always returns the auto-flipped result), but the branch lives in the JS and confuses readers. Fold it into the cleaner adapter-level fallback once U4 lands.

Origin: `docs/plans/2026-04-29-001-feat-label-viewer-phase-a-plan.md` (P1 + P2 entries, lines 517-527).

---

## Requirements Trace

- R1. Cratedigger label-detail route forwards `page`/`per_page` query params to the adapter; UI exposes page navigation when pages > 1. (Origin R5 — "paginated for hundreds/thousands".) → U1
- R2. The discogs-api recursive sub-label CTE cannot infinite-loop on a `parent_label_id` cycle. → U2
- R3. discogs-api Rust types reflect the actual nullability of `profile`, `contactinfo`, `data_quality`. → U3
- R4. A 503 from the upstream mirror does not surface as a 500 to the cratedigger UI; one-shot retry without sub-labels degrades the page gracefully and surfaces a banner. → U4
- R5. The "empty result on big label → retry" path in `web/js/labels.js::openLabelDetail` is removed in favor of the cleaner adapter-level fallback. → U5

---

## Scope Boundaries

- The P0 connection-pool work lives in plan `2026-04-29-002`. This plan does NOT change `tokio_postgres::Client` usage or add a pool.
- The P3 cleanup cluster (overlay rename, sub_labels passthrough, year-input debounce, label_id validation, via_label_id rename) lives in plan `2026-04-29-004`. This plan does not touch them.
- No new endpoints. No schema changes. No new SQL indexes.
- Pagination scope: server already supports it; this plan wires the existing parameter through the route + UI. Lazy-load / infinite-scroll patterns are explicitly out of scope — explicit page controls only.
- The `web/js/browse.js::searchArtists` stale-result race (called out as "pre-existing" in the review) is **not** in scope here. It belongs to plan `2026-04-29-004` (P3 cleanup) where it can be fixed alongside the in-flight token work for `openLabelDetail`.

### Deferred to Follow-Up Work

- Pre-existing artist-search stale-result race → P3 plan (`2026-04-29-004`).
- Cache-key length cap on `search_labels` / `search_artists` → P3 plan.
- BIG_LABEL_THRESHOLD duplication between `web/routes/labels.py:37` and `web/js/labels.js:28` → P3 plan as a small de-duplication.

---

## Context & Research

### Relevant Code and Patterns

**[cratedigger] Pagination wiring (U1):**

`web/discogs.py:510-579` already accepts `page` and `per_page`:
```python
def get_label_releases(label_id: int | str, *, include_sublabels: bool = True,
                       page: int = 1, per_page: int = 100) -> dict:
    def _fetch() -> dict:
        sub_flag = "true" if include_sublabels else "false"
        raw = _get(
            f"{DISCOGS_API_BASE}/api/labels/{label_id}/releases"
            f"?include_sublabels={sub_flag}&page={page}&per_page={per_page}"
        )
        ...
```

The route at `web/routes/labels.py:122-123` calls without forwarding:
```python
    releases_resp = discogs_api.get_label_releases(
        label_id, include_sublabels=include_sublabels)
```

The frontend message at `web/js/labels.js:413-415` advertises pagination as "coming":
```javascript
  const renderedNote = (allReleases.length < totalCount)
    ? `<div class="loading" ...>Showing first ${allReleases.length} of ${totalCount} — pagination coming.</div>`
    : '';
```

The artist-view paginator is the closest existing reference — verify in `web/js/discography.js` during implementation if it has explicit page controls. If not, this unit defines the pattern; if it does, mirror it.

**[discogs-api] Recursive CTE (U2):**

`discogs-api/src/db.rs:1036-1072`:
```sql
WITH RECURSIVE label_tree AS (
    SELECT id FROM label WHERE id = $1
    UNION ALL
    SELECT l.id FROM label l
    JOIN label_tree lt ON l.parent_label_id = lt.id
),
matched AS (
    SELECT DISTINCT ON (r.id)
           r.id, r.title, r.country, r.released, r.master_id,
           rl.label_id AS via_label_id
    FROM release r
    JOIN release_label rl ON rl.release_id = r.id
    WHERE rl.label_id IN (SELECT id FROM label_tree)
    ORDER BY r.id, (rl.label_id = $1) DESC, rl.label_id
)
...
```

Two CTE call sites use `UNION ALL`: the matched-rows query above, and the count query (verify exact location in `query_label_releases` during U2). Both need to switch.

**[discogs-api] Type fixes (U3):**

`discogs-api/src/types.rs:261-282`:
```rust
pub struct LabelHit {
    pub id: i32,
    pub name: String,
    pub profile: String,      // -> Option<String>
    pub parent_label_id: Option<i32>,
    ...
}

pub struct LabelDetail {
    pub id: i32,
    pub name: String,
    pub profile: String,           // -> Option<String>
    pub contactinfo: String,       // -> Option<String>
    pub data_quality: String,      // -> Option<String>
    ...
}
```

Schema (`discogs-api/src/schema.rs:32-39`):
```sql
CREATE TABLE label (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    contactinfo TEXT NOT NULL DEFAULT '',
    profile TEXT NOT NULL DEFAULT '',
    parent_label_id INT,
    data_quality TEXT NOT NULL DEFAULT ''
);
```

Row extraction in `discogs-api/src/db.rs::query_label` uses `row.get("profile")` etc. with type inference from the struct fields. Switching to `Option<String>` flips the inference automatically; downstream JSON serializes `null` for empty.

**[cratedigger] 503 fallback consumer (U4):**

`web/discogs.py::get_label_releases` raises `urllib.error.HTTPError` on non-200. `web/routes/labels.py:111-128` catches only 404:
```python
    try:
        entity = discogs_api.get_label(label_id)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            h._error("Label not found", 404)
            return
        raise
```

The 503 retry can live at the adapter (cleaner — caches the successful fallback) or the route (simpler error mapping). U4's Approach picks the adapter location for symmetry with the P0 plan's U3.

**[cratedigger] JS dead-branch removal (U5):**

`web/js/labels.js:308-347::openLabelDetail`:
```javascript
    let payload = await loadLabelReleases(labelId, { include_sublabels: true });
    const totalCount = (payload && payload.label && payload.label.release_count) || 0;
    if (totalCount > BIG_LABEL_THRESHOLD && payload.releases && payload.releases.length === 0) {
      payload = await loadLabelReleases(labelId, { include_sublabels: false });
    } else if (totalCount > BIG_LABEL_THRESHOLD) {
      state.labelFilters = state.labelFilters || {};
      state.labelFilters.bigLabel = true;
    }
```

The first branch (empty-results retry) doesn't fire because the route auto-flips `include_sublabels=false` for big labels server-side (`bf2d929`). With U4 in place, any genuine "missing sub-labels" surfaces via `payload.sub_labels_dropped`. Remove the empty-results retry; keep the `bigLabel` flag setter.

### Institutional Learnings

- The repo has no `docs/solutions/` yet. The architectural lessons from this plan + the P0 plan should land in a new `docs/solutions/` entry once the cluster ships.

### External References

- PG14+ adds a native `CYCLE` clause for recursive CTEs. The minimum supported PG version on the discogs-api host should be checked during U2; if PG14+, prefer `CYCLE label_id SET is_cycle USING path` over `UNION` (more explicit). Otherwise `UNION` is correct and works on PG12+.

---

## Key Technical Decisions

- **`UNION` over PG14+ `CYCLE`** — `UNION` works regardless of PG version and the dedup cost is negligible at single-digit hierarchy depth. `CYCLE` is more explicit but requires a version gate. Take `UNION` unless the host is confirmed PG14+ and the team prefers explicitness; either way, leave a code comment naming the cycle-guard intent.
- **`Option<String>` over a documented invariant comment.** The schema can change; the type system can't lie if we make it honest. Compare-and-go with serde defaults so the JSON serializes `null` (not `""`) when empty.
- **Adapter-layer 503 retry** (U4) over route-layer retry. Same rationale as in the P0 plan's U3: lets the cache memoize the successful fallback under its own key, keeps the route handler simple. The two plans are coordinated so the adapter retry semantics are identical regardless of which plan ships first.
- **Explicit pagination controls** over lazy-load / infinite-scroll. Discoverable, accessible, deep-linkable via URL state, mirrors the existing artist-view convention if any. Lazy-load is an entirely separate UX decision and out of scope.
- **`sub_labels_dropped` is the contract field name for the 503 fallback.** Same name proposed by the P0 plan (U3) — kept identical here. If both plans land, the contract is a single source of truth; if this one ships first, the P0 plan inherits the field unchanged.

---

## Open Questions

### Resolved During Planning

- Adapter vs route layer for 503 retry → adapter (see Key Decisions).
- Lazy-load vs explicit pagination → explicit (see Key Decisions).
- Where to surface `sub_labels_dropped` → JSON payload field + UI banner (matches P0 plan).

### Deferred to Implementation

- Default `per_page` for the UI's first page request — keep `100` (server default) unless artist-view convention differs.
- Whether the page control is buttons (`< Prev | Next >`) or a paginator (`< 1 2 3 ... 12 >`). Settle by looking at artist view during U1.
- `release_count` on the JSON payload header should match the rolled-up CTE count, not the direct count, to fix the "X releases" mismatch flagged in P2 #2 of the review. Confirm during U1 — if `payload.pagination.items` is the right value, surface it as the header count.

---

## Implementation Units

- U1. **[cratedigger] Forward pagination params and add UI page controls**

**Goal:** Make the label-detail page actually paginated. The route accepts `page`/`per_page`, forwards to the adapter; the UI renders `< Prev | Page N of M | Next >` controls when more than one page exists.

**Requirements:** R1.

**Dependencies:** None.

**Files** *(target repo: cratedigger):*
- Modify: `web/routes/labels.py::get_discogs_label_detail` (read `page` / `per_page` from query string, validate (`page >= 1`, `per_page` clamped 1-200), forward to `get_label_releases`)
- Modify: `web/js/labels.js::loadLabelReleases` (accept `{page, per_page}`, append to URL)
- Modify: `web/js/labels.js::renderLabelDetail` (render `< Prev | Page N of M | Next >` when `payload.pagination.pages > 1`; click handler reloads at the new page; replace the "Showing first X of N — pagination coming" placeholder)
- Modify: `web/js/state.js` (add `state.labelPage` to track current page across filter operations)
- Modify: `tests/test_web_server.py::TestLabelRouteContracts` (add a contract test that hitting `?page=2&per_page=50` forwards to the adapter — assert the adapter call shape via patched `get_label_releases`)

**Approach:**
- Route validation: `page = max(1, int(qs.get("page", "1")))`; `per_page = max(1, min(200, int(qs.get("per_page", "100"))))`. Return 400 on non-integer values.
- Cache invalidation considerations: `get_label_releases` already caches per `(label_id, include_sublabels, page, per_page)` (cache key at `web/discogs.py:576-577`). No changes needed.
- The page-control UI lives at the bottom of the release list. When the user changes pages, scroll to top and re-render.
- The `release_count` header (currently rendered at `web/js/labels.js:437-438`) should reflect the rolled-up total (`payload.pagination.items`) when `include_sublabels=true`, not the label entity's `release_count` field — fixes the P2 #2 mismatch flagged in the review.

**Patterns to follow:**
- `web/js/discography.js::renderArtistDiscography` for whatever pagination convention the artist view uses (verify during implementation; mirror it).
- `web/routes/labels.py::get_discogs_label_search` for the existing query-param parsing shape — extend with the new int parsers.

**Test scenarios:**
- Happy path: `GET /api/discogs/label/{warp_id}?page=2&per_page=50` returns rows 51-100.
- Edge case: `GET /api/discogs/label/{small_id}?page=99` returns empty `releases` array, `pagination.pages == 1` so the UI shows no page controls (verify the route doesn't 404 on out-of-range page).
- Edge case: `?per_page=500` clamps to 200; `?page=0` returns 400.
- Contract: `_assert_required_fields` covers `pagination.{page, per_page, pages, items}`.
- Pure JS test: page-control rendering predicate — given `pagination={pages: 1}` no controls render; given `{pages: 5, page: 1}` only "Next" is enabled; given `{pages: 5, page: 5}` only "Prev" is enabled.

**Verification:**
- All contract tests pass.
- After deploy, open Hymen Records (small label) — no page controls. Open Warp Records (mid-size) — page controls appear; `Next` advances to page 2; URL state survives a refresh (or, if URL hash isn't wired today, at minimum the in-memory state survives a filter change).

---

- U2. **[discogs-api] Switch recursive sub-label CTE to `UNION` (cycle guard)**

**Goal:** A self-referencing `parent_label_id` cannot infinite-loop the query.

**Requirements:** R2.

**Dependencies:** None.

**Files** *(target repo: discogs-api):*
- Modify: `src/db.rs::query_label_releases` (replace `UNION ALL` with `UNION` in both the matched-rows recursive CTE at lines 1036-1072 and the count CTE at the same call site)
- Test: live integration check post-deploy — `EXPLAIN ANALYZE` to confirm plan still uses index scans, smoke test on Warp + UMG to confirm the dedup cost is invisible.

**Approach:**
- One-line change per CTE: `UNION ALL` → `UNION`. Add a SQL comment naming the intent: `-- UNION (not UNION ALL): defends against parent_label_id cycles in the label tree.`
- The dedup cost in PostgreSQL is a hash distinct over `(id)` tuples — single-digit milliseconds for the trees observed in the dump.
- If the host is PG14+ and the team prefers explicit cycle guards, alternative: `WITH RECURSIVE label_tree AS (...) UNION ALL (...) CYCLE id SET is_cycle USING path` — this is a stricter, named guard. Check the running PG version with `psql -c "SHOW server_version_num"` during U2 and pick the cleaner option.

**Execution note:** Run `EXPLAIN ANALYZE` on the post-change query for Warp Records (multi-depth tree, observable size) before declaring done. The plan must still show `Bitmap Index Scan on idx_release_label_label_id` or similar — the change shouldn't disturb the executor's choice.

**Patterns to follow:**
- The existing CTE in `src/db.rs:1036-1072` is the only model needed.

**Test scenarios:**
- Smoke: `curl https://discogs.ablz.au/api/labels/{warp_id}/releases?include_sublabels=true` returns the expected catalogue size after the change (no rows lost or doubled).
- Smoke: same call against Hymen returns identical results pre- and post-change (no sub-label tree, dedup is a no-op).
- Synthetic cycle test (optional but recommended): in a local discogs-api instance against a test DB, insert two labels with `parent_label_id` pointing at each other; the query must return finite results in <100ms instead of running until OOM.

**Verification:**
- `EXPLAIN ANALYZE` plan post-change uses the same indexes as pre-change.
- Warp / Hymen catalogue sizes match pre-change.

---

- U3. **[discogs-api] Promote nullable label fields to `Option<String>`**

**Goal:** Make `LabelDetail` and `LabelHit` truthful about which fields can be NULL.

**Requirements:** R3.

**Dependencies:** None.

**Files** *(target repo: discogs-api):*
- Modify: `src/types.rs::LabelHit` (`profile` → `Option<String>`)
- Modify: `src/types.rs::LabelDetail` (`profile`, `contactinfo`, `data_quality` → `Option<String>`)
- Modify: `src/db.rs::query_label` (the `row.get(...)` calls now infer `Option<String>` automatically)
- Modify: `src/db.rs::query_label_search` (same — the search variant)
- Test: live curl check post-deploy. JSON serializes `null` for empty/NULL values; cratedigger Python adapter handles `None` cleanly (verify by re-running the cratedigger label-detail contract test against a label whose profile is empty).

**Approach:**
- The schema currently has `NOT NULL DEFAULT ''`, so today the value is always `Some("")` not `None`. The type change is forward-defensive — if a future migration drops the NOT NULL or an upstream import leaves the column NULL, the type catches it without panic.
- The cratedigger Python adapter already treats `profile` as `nullable string` in the source-agnostic `LabelEntity` shape (Phase A plan U3), so no Python-side changes are needed.
- Add `#[serde(skip_serializing_if = "Option::is_none")]` to the new `Option<String>` fields IF the contract should drop the key entirely when null. Otherwise, the JSON serializes `"profile": null` — fine for the cratedigger consumer, which already treats both as empty.

**Patterns to follow:**
- Existing `Option<i32>` and `Option<String>` fields in the same structs (`parent_label_id: Option<i32>`, `parent_label_name: Option<String>`).

**Test scenarios:**
- Smoke: `curl https://discogs.ablz.au/api/labels/{some_id}` for a label with non-empty profile — JSON shows `"profile": "..."`.
- Smoke: `curl https://discogs.ablz.au/api/labels/{some_id}` for a label with `profile = ''` — JSON shows `"profile": null` (or no key, depending on the serde flag chosen).
- Cratedigger contract test for label-detail still passes — `web/discogs.py` adapter normalizes `None` → empty string in `LabelEntity.profile` if necessary; verify during implementation.

**Verification:**
- `cargo build --release` succeeds.
- All cratedigger contract tests pass against the deployed mirror.
- A manually crafted label with NULL profile (in a test DB or via direct UPDATE on a non-prod schema) does not panic the handler.

---

- U4. **[cratedigger] Graceful 503 retry in `get_label_releases` adapter**

**Goal:** When the upstream returns 503 (timeout — relevant once the P0 plan ships, defensive otherwise), retry once without sub-labels and surface a banner.

**Requirements:** R4.

**Dependencies:** None on U1/U2/U3. Coordinated with `2026-04-29-002` plan U3 — same field name (`sub_labels_dropped`) and same fallback semantics.

**Files** *(target repo: cratedigger):*
- Modify: `web/discogs.py::get_label_releases` (catch `urllib.error.HTTPError` with code 503 and `include_sublabels=True`; retry once with `include_sublabels=False`; set `sub_labels_dropped=True` on the returned payload)
- Modify: `web/routes/labels.py::get_discogs_label_detail` (forward `sub_labels_dropped` from the adapter response into the JSON payload)
- Modify: `tests/test_web_server.py::TestLabelRouteContracts` (add `sub_labels_dropped` to `LABEL_DETAIL_RESPONSE_REQUIRED_FIELDS`; add a unit test for the 503 retry path)
- Modify: `web/js/labels.js::renderLabelDetail` (when `payload.sub_labels_dropped === true`, render a banner: "Sub-labels unavailable for this label — showing direct releases only")

**Approach:**
- Adapter retry is one-shot. Both calls 503 → re-raise the original `HTTPError` so the route returns 5xx (no infinite retry).
- The retry preserves `page` / `per_page` from the original call.
- `sub_labels_dropped` defaults to `False` on every label-detail response; only the 503-fallback path sets it `True`. Contract test asserts the field is always present.
- Banner is plain text + the existing `.loading` class. No new CSS.
- This unit duplicates the contract field with the P0 plan's U3. If both ship, they collide on the field name and converge cleanly. If only this plan ships, the field is dormant for the actual 503 path until P0 lands; it still triggers if urllib timeouts produce a 503-like surface (rare today) or for any other 503 source.

**Execution note:** Add the contract test (RED) for the 503 → fallback path first using a patched `_get`, then implement the adapter retry (GREEN). Per `.claude/rules/code-quality.md` API Contract Tests.

**Patterns to follow:**
- `web/discogs.py:466-491::search_labels` for the cache-then-`_fetch` shape — the retry inside `_fetch` happens before memoization, so the successful fallback caches under its own `include_sublabels=false` key.
- `tests/test_web_server.py::TestLabelRouteContracts` (lines 2762-3067) for the contract-test harness.
- `web/routes/labels.py:111-128` (existing 404 handling) for the error-mapping pattern.

**Test scenarios:**
- Happy path: normal `get_label_releases(small_label, include_sublabels=True)` returns payload with `sub_labels_dropped=False`.
- 503 fallback: patch `_get` to return 503 then 200; assert returned payload `sub_labels_dropped=True`.
- 503 → 503: both calls 503; `urllib.error.HTTPError` re-raises and route returns 500.
- 404 unchanged: 404 surfaces as 404 (existing path).
- Contract: every label-detail response includes `sub_labels_dropped` (default `False`).

**Verification:**
- All contract tests pass.
- After deploy, simulating a 503 on `https://discogs.ablz.au/api/labels/{id}/releases?include_sublabels=true` (or once the P0 plan lands and UMG triggers a real 503) — the cratedigger response is 200 with `sub_labels_dropped=true` and the UI shows the banner.

---

- U5. **[cratedigger] Remove the empty-results retry branch in `openLabelDetail`**

**Goal:** Eliminate the awkward client-side fallback that doesn't fire today and is now redundant with U4's adapter-level handling.

**Requirements:** R5.

**Dependencies:** U4 (the cleaner replacement must exist).

**Files** *(target repo: cratedigger):*
- Modify: `web/js/labels.js::openLabelDetail` (remove the `payload.releases.length === 0` retry branch at lines 332-336; keep the `bigLabel` flag setter for the > BIG_LABEL_THRESHOLD case)
- Modify: any pure JS tests in `tests/test_js_util.mjs` if the helper is exported and tested; otherwise rely on playwright walkthrough.

**Approach:**
- After U4 lands, any genuine "the upstream couldn't roll up sub-labels" surfaces via `payload.sub_labels_dropped`. The client no longer needs to second-guess an empty result.
- The `bigLabel` flag (`state.labelFilters.bigLabel = true`) stays — it still drives any UI affordance that wants to know "this is a big label."

**Patterns to follow:**
- The existing `openLabelDetail` shape — keep state transitions identical, remove only the retry branch.

**Test scenarios:**
- Pure JS test (if the function is exported): given a payload with `sub_labels_dropped=true`, the renderer surfaces the banner; given a payload with `sub_labels_dropped=false` and zero releases, no retry fires.
- Playwright: load Hymen → normal page; load Warp → page renders within SLA; load UMG-class (post-P0 deploy) → page renders with banner, no double-fetch in the network panel.

**Verification:**
- After deploy, the network panel shows exactly one `/api/discogs/label/{id}` call per label load (not two).
- The banner renders for genuinely degraded labels and never for healthy ones.

---

## System-Wide Impact

- **Interaction graph:** U1 adds a new query-string contract on the cratedigger label-detail route; the rest of the route surface is unchanged. U2 + U3 are internal to discogs-api; the wire format of `LabelDetail`/`LabelHit` shifts only in nullable-field semantics (string-or-null instead of always-string). U4 + U5 add one optional response field plus a UI banner.
- **Error propagation:** U4 is the load-bearing change here — a 503 from the mirror now degrades gracefully instead of surfacing as a 500. Other upstream errors (404, urllib timeouts, ECONNRESET) keep current behavior.
- **State lifecycle risks:** U1's pagination state is in-memory only (no URL hash wiring in scope here). A user changing filters mid-page may want the filter to reset to page 1 — verify the artist-view convention for this in U1 implementation.
- **API surface parity:** The new `sub_labels_dropped` field is shared with the P0 plan. The pagination query params on `/api/discogs/label/{id}` are new on the cratedigger side; the mirror's `/api/labels/{id}/releases` already accepts them.
- **Integration coverage:** U1's contract test is the primary cross-layer guard for pagination forwarding. U4's contract test guards the 503 fallback. U2 and U3 are verified by smoke + cratedigger integration tests (the existing `test_label_detail_overlay_integration` exercises a normal label end-to-end and would break if `Option<String>` serialization broke the adapter).
- **Unchanged invariants:** Beets DB queries, pipeline DB schema, slskd integration, harness, quality model. The label-detail JSON adds two optional concerns (`sub_labels_dropped`, page-control fields under `pagination`) but never removes a field.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| `UNION` dedup cost in U2 visibly slows down a deep label tree | `EXPLAIN ANALYZE` smoke test on Warp during U2. The hash distinct cost is microseconds at observed depth; if it's measurable, switch to PG14+ `CYCLE` clause instead. |
| `Option<String>` serialization changes the JSON shape for consumers | The cratedigger Python adapter already treats these fields as nullable. No third-party consumers exist. Manual smoke test post-deploy on a label with empty `profile` confirms the change. |
| Pagination breaks when filters are active (year, format) | U1 explicitly resets to page 1 on filter change. Verify in playwright. |
| 503 retry doubles upstream load on legitimate slow labels | Acceptable — the retry is one-shot and only fires on 503. The fallback is `include_sublabels=false`, which is much cheaper than the recursive CTE. |
| `sub_labels_dropped` field collision with P0 plan U3 | Explicitly identical contract by design. If both plans ship, no conflict. If only this plan ships, the field is harmlessly dormant for the actual 503 path. |
| U5 races U4 — removing the JS retry before the adapter retry exists strands the UI | U5 lists U4 as a hard dependency. Land in order. |
| `release_count` mismatch fix in U1 (P2 #2 of original review) hides a real upstream count drift | U1 surfaces `pagination.items` from the discogs-api response, which is the rolled-up CTE count, so the displayed number reflects the rolled-up reality. If the discogs-api count and the label-entity `release_count` ever disagree by more than a small delta, it's a separate data-quality issue, not a UI bug. |

---

## Documentation / Operational Notes

- No new sops secrets, systemd units, or migrations.
- After U2 ships, update `docs/discogs-mirror.md` to mention the cycle guard (single sentence in the `/api/labels/{id}/releases` section).
- After U3 ships, update `docs/discogs-mirror.md` to note that `profile`, `contactinfo`, `data_quality` are nullable in the JSON.
- After U4 ships, update `docs/webui-primer.md` to mention the `sub_labels_dropped` banner.
- Standard deploy flow per `.claude/rules/deploy.md`. discogs-api units (U2, U3) deploy first; cratedigger units (U1, U4, U5) follow.

---

## Sources & References

- **Origin plan:** `docs/plans/2026-04-29-001-feat-label-viewer-phase-a-plan.md` (P1 + P2 entries, lines 517-527)
- **Coordinated plan:** `docs/plans/2026-04-29-002-fix-discogs-api-connection-pool-plan.md` (shares `sub_labels_dropped` contract with U4 here)
- **Cleanup plan:** `docs/plans/2026-04-29-004-fix-label-viewer-p3-cleanup-plan.md` (handles overlay rename, sub_labels passthrough, debounce, validation, via_label_id rename, pre-existing artist-search race)
- **Related code:**
  - `cratedigger/web/discogs.py:510-579` (already supports pagination — see U1)
  - `cratedigger/web/routes/labels.py:111-128, 122-123, 140` (404-only handler, no page forward, hardcoded sub_labels)
  - `cratedigger/web/js/labels.js:308-347, 367-373, 413-415, 437-438` (openLabelDetail, loadLabelReleases, page placeholder, count header)
  - `discogs-api/src/db.rs:1036-1072` (recursive CTE — U2)
  - `discogs-api/src/db.rs:967-1011` (query_label — U3)
  - `discogs-api/src/types.rs:261-282` (LabelHit, LabelDetail — U3)
  - `discogs-api/src/schema.rs:32-39` (label table schema — confirms NOT NULL DEFAULT '')
  - `cratedigger/tests/test_web_server.py:2762-3067` (TestLabelRouteContracts — gets U1 + U4 contract additions)

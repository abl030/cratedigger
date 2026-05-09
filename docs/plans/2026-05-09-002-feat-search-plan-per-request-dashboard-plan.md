---
title: "feat: Search-plan per-request dashboard"
type: feat
status: active
date: 2026-05-09
origin: docs/brainstorms/2026-05-09-search-plan-per-request-dashboard-requirements.md
---

# feat: Search-plan per-request dashboard

## Summary

Land v1 of the per-request search-plan inspector across five units: one backend (paginated `search_log`-per-request endpoint plus paired CLI subcommand, since the existing `GET /search-plan` does not surface per-attempt rows) and four frontend (state scaffold, summary panel + per-row button injection across three tabs, detail subview rendered under the Pipeline tab with back-button restore, action buttons with confirmation UX). All UI mirrors existing idioms: `.p-detail.open` inline-expand, `setPipelineView`-style subview, template-literal `innerHTML` with `esc()`. No new patterns are introduced.

---

## Problem Frame

Origin doc captures the operator pain: per-request triage today requires `ssh doc2`, multiple `pipeline-cli search-plan show ...` commands, and ad-hoc SQL when investigating degenerate plans like the David Bowie 1967 incident behind PR #239. The data, CLI, and API exist; the UI is the remaining gap. v1 is largely frontend on top of three already-shipped endpoints, plus one new diagnostic GET to surface per-attempt telemetry that the existing inspection payload omits.

(See origin: `docs/brainstorms/2026-05-09-search-plan-per-request-dashboard-requirements.md` § Problem Frame for the full motivation.)

---

## Requirements

- R1. A per-album search-plan button exposes the inspector on Browse, Pipeline, and Recents — and only those tabs — through a single shared component. (Origin R1, R2, R3, AE1)
- R2. Clicking the button opens a compact in-place summary that does not change tabs; an "Open detail" affordance navigates to a per-request detail page rendered under the Pipeline tab; the detail page's back button returns the operator to the originating tab and prior scroll position. (Origin R3, R4, AE2, AE3)
- R3. The summary surfaces plan status, generator id, cursor `X/N`, cycle count, the most recent attempts, and visible generator-id drift. (Origin R5, R13, AE2, AE4)
- R4. The detail page surfaces the full ordered slot list, per-attempt history with full telemetry (paginated by row count), per-slot stats, expandable per-attempt candidate forensics, and plan health (failure class, sanitised error, provenance). (Origin R6-R10, R12, R13, AE4, AE5)
- R5. Cache statistics on either view are labeled cycle-level; legacy `plan_id IS NULL` `search_log` rows are visible but segregated from plan-aware rows; generator-id drift is visibly surfaced when present. (Origin R11, R12, R13, AE6, AE7)
- R6. Both views expose `regenerate` and `advance` action buttons; regenerate surfaces the cursor/cycle reset before commit and requires explicit confirmation; advance accepts an ordinal target or strategy prefix and enforces forward-only at the API. (Origin R14, R15, R16, AE8, AE9)
- R7. The inspector refreshes on click or page load only; v1 introduces no realtime push or polling. (Origin R17, AE10)
- R8. Any new web route (specifically the per-attempt history endpoint introduced in U1) registers in `Handler._FUNC_GET_ROUTES`/`_FUNC_POST_ROUTES`, classifies in `TestRouteContractAudit.CLASSIFIED_ROUTES`, has a contract test with a `REQUIRED_FIELDS` set, and ships with a paired `pipeline-cli` subcommand per CLI ⇄ API symmetry. Frontend follows `.claude/rules/web.md`: vanilla JS, ES6 modules under `web/js/`, no build step, `// @ts-check` + JSDoc on exports, state in `web/js/state.js`, no inline `<script>` blocks. (Origin R18)

**Origin actors:** A1 (Operator), A2 (Search executor — observed, not user-facing), A3 (Future cross-request dashboard — data dimensions must remain compatible).

**Origin flows:** F1 (Diagnose-and-repair from any tab), F2 (Operator-driven refresh), F3 (Mutation with consequence visibility).

**Origin acceptance examples:** AE1-AE10.

---

## Scope Boundaries

Carried verbatim from origin:

- Cross-request analytics (which canonical queries pull weight across the queue) — v2.
- Fleet health (readiness counters, generator drift across queue, wrap-rate trends, deterministic-failed sticky lists) — v2.
- The Decisions and Wrong Matches tabs do not get the search-plan button in v1.
- Time-window filters (24h / 7d / since-deploy) for the per-request history — deferred; v1 paginates by row count.
- Realtime push, websockets, and timer-based polling of `search_log` — out of scope.
- Before/after comparison across superseded plans (pre-regenerate vs. post-regenerate stats on the same request) — deferred.
- Deep-linkable URLs to a request's detail view — deferred; v1 reaches detail only via the summary's "Open detail" affordance.
- New operator actions beyond `show`, `regenerate`, and `advance` — out of scope.
- Generator-output fixes (the self-titled-dedup follow-up flagged in PR #239's "Out of scope") — tracked separately.

### Deferred to Follow-Up Work

- Extraction of pure renderers into `web/js/search_plan_render.js` mirroring the `release_actions.js` / `release_action_state.js` split. v1 ships everything in a single `search_plan.js`; refactor opportunity once the surface is real.
- Direct-link URLs (e.g., copyable links into a request's detail view) — separate iteration once routing patterns are needed elsewhere.
- Playwright E2E coverage for the inspector. v1's verification posture is dev iteration via `web_dev_server.py`; an automated E2E unit can land later.

---

## Context & Research

### Relevant Code and Patterns

**Backend (existing surfaces to extend):**
- `web/routes/pipeline.py`: `get_pipeline_search_plan` (lines 471-492), `post_pipeline_search_plan_regenerate` (495-578), `post_pipeline_search_plan_advance` (581-676), route registry (1430-1450).
- `lib/search_plan_inspection.py`: `build_inspection_payload` is the shared CLI/API payload builder; `LEGACY_LOG_HEAD_LIMIT=5` (line 31) is the hardcoded legacy cap. `_legacy_log_row_to_dict` (115-131) is the per-row shape pattern to mirror for the new history endpoint.
- `lib/search_plan_service.py`: `SearchPlanService` is the service-layer home; service methods are thin and return typed `Result` dataclasses (e.g. `AdvanceResult`).
- `lib/pipeline_db.py`: `get_search_history` (line 2448) returns all `search_log` rows newest-first via `SELECT *`; `get_search_plan_stats_history` (2457) is a projection of the same set without `candidates`. The new history endpoint's DB method will mirror this shape but with `LIMIT` + `WHERE id < before_id` cursor.
- `scripts/pipeline_cli.py`: `cmd_search_plan_show` (1531-1564) and `cmd_search_plan_advance` (PR #239) are the freshest CLI subcommand patterns to mirror for `cmd_search_plan_history`.
- `migrations/014_persisted_search_plans.sql:200-244`: plan-aware `search_log` columns, all nullable for legacy rows.

**Frontend (existing idioms to mirror):**
- `web/js/pipeline.js:686-810` `toggleDetail(elId, requestId)`: canonical inline-expand pattern. `.p-detail.open` CSS toggle.
- `web/js/pipeline.js:31` `setPipelineView('queue'|'dashboard')` with `state.pipelineView` field: existing intra-Pipeline subview switch. The detail page becomes a third value, `'search-plan-detail'`.
- `web/js/state.js:11` `state` singleton with sub-view fields (`browseSubView`, `pipelineView`, `recentsSub`); `state.js:45-47` `searchTargetId`/`searchTargetExpandId`/`searchTargetSource` is the precedent for stashing target context across renders. The new `state.searchPlanDetailContext` follows that pattern.
- `web/js/discography.js:41` `renderRgRow`, `:237` `renderRelease` (Browse); `web/js/pipeline.js:636` `renderPipelineItem` (Pipeline); `web/js/recents.js:39` `renderRecentsItems` (Recents): three separate row renderers — button injection happens in three places.
- `web/js/release_action_state.js:63` `buildReleaseActionState`: existing helper that consults `pipelineStore` to produce `pipelineId`. Browse-tab button conditional uses the same lookup (`pipelineId !== null`).
- `web/js/release_actions.js`: pure renderer pattern (template literals returning HTML strings, decorated with `// @ts-check` + JSDoc); the eventual extraction model when v1 surface grows.
- `web/js/main.js:24-35` `showTab`, `:62-125` `window.*` registrations: the canonical "expose an onclick handler" path.
- `web/js/util.js`: `esc()` for safe HTML interpolation; `qualityLabel`, `awstDateTime` for formatting helpers.

**Tests:**
- `tests/test_web_server.py:88-95` `_assert_required_fields`; `:98-131` `_WebServerCase`; `:912-984` `TestRouteContractAudit.CLASSIFIED_ROUTES`; `:2213-2369` `TestPipelineSearchPlanAdvanceContract` — the freshest reference (PR #239).
- `tests/fakes.py` `FakePipelineDB`: any new DB method gets a fake counterpart; `tests/test_fakes.py` `TestFakePipelineDBSearchPlanContract` enforces signature parity.
- `tests/test_js_util.mjs`: Node-runner pattern for pure JS unit tests (no npm, no DOM).

**Dev iteration:**
- `scripts/web_dev_server.py`: `--data prod-api` mode proxies GETs to a deployed backend (mutations 405). Frontend U2-U5 iterate against deployed v1 backend after U1 ships.

### Institutional Learnings

- `docs/solutions/testing/mocked-contract-tests-miss-helper-mirror-integration-bugs.md`: contract tests catch wire format but not helper integration. Plan-mode mocking + `_assert_required_fields` covers shape; the audit registry catches missing classifications. Slice tests in `tests/test_integration_slices.py` cover real code paths if needed; for the new history endpoint the mocked contract test plus the `FakePipelineDB` parity check cover the wire boundary; integration slice may not be needed for a thin GET wrapper.
- CLI ⇄ API symmetry (`.claude/rules/code-quality.md` § "CLI ⇄ API Surface Symmetry"): every operator action exists on both surfaces wrapping the same service method. The new history endpoint is a diagnostic GET, not strictly an "operator action," but the existing `GET /search-plan` is paired with `pipeline-cli search-plan show`. Continuing the convention with `pipeline-cli search-plan history` keeps parity.
- Wire-boundary types use `msgspec.Struct`, not `@dataclass`, when crossing JSON (`.claude/rules/code-quality.md` § "Wire-boundary types"). The new history endpoint's response uses dicts decoded directly from `search_log` rows; if a struct is needed for the response payload (e.g. for re-decoding in tests), it follows that rule.

### External References

- None used. The work is entirely codebase-local and follows established cratedigger patterns.

---

## Key Technical Decisions

- **A new diagnostic GET endpoint is required for per-attempt telemetry.** Existing `GET /search-plan` payload is verified to surface only an aggregated stats block (per-slot, per-query-group rollups) plus a hardcoded 5-row `legacy_logs.head` (`plan_id IS NULL` only). Per-attempt active-plan history is computed internally for stats but never returned. Adding `GET /api/pipeline/<id>/search-plan/history?limit=N&before_id=ID` is the cleanest split: `/search-plan` owns plan structure + aggregate stats, `/search-plan/history` owns the row-level log.
- **Cursor-style pagination on `search_log.id` (descending), not offset/limit.** Origin R7 says "paginate by row count rather than time window in v1"; cursor pagination over a monotonic int PK is durable across writes and trivially indexable.
- **Paired `pipeline-cli search-plan history` subcommand.** Per CLI ⇄ API symmetry. Wraps the same service method as the route. The CLI does not need pagination ergonomics for v1 (`pipeline-cli query "SELECT * FROM search_log ..."` is the deeper-dive escape hatch); the subcommand can default `limit=50` and accept `--limit` + `--before-id` flags mirroring the API.
- **Routing/state mechanism: stash + subview, no History API.** A new `state.searchPlanDetailContext = {requestId, originTab, originScrollY, originSubView}` field on the `state` singleton; a third Pipeline subview value `'search-plan-detail'` mirroring `state.pipelineView ∈ {'queue', 'dashboard', 'search-plan-detail'}`. Custom in-UI back button reads the stash and calls `showTab(originTab)` then `window.scrollTo(0, originScrollY)`. Browser back-button does not work in-app — consistent with all other tabs in this codebase.
- **Single module `web/js/search_plan.js`.** Covers summary render, detail render, and action handlers in one file. Pure renderers may extract later if surface grows (Deferred to Follow-Up Work). v1 keeps everything together for the dev-iteration loop.
- **Confirmation UX: native `confirm()` for regenerate, inline form for advance.** Mirrors `confirmDeleteBeets` (`web/js/release_actions.js`) for destructive actions. Advance needs richer input (strategy-prefix dropdown OR ordinal text input), so an inline form within the panel is the right shape; native `prompt()` is too crude for two-mode entry.
- **Legacy log segregation: two sections in detail.** Plan-aware history table (full telemetry per row) + a collapsed "Pre-rollout history (N rows)" block with the existing legacy slim shape. Not a toggle. Origin R12 explicitly forbids hiding.
- **Browse-tab button is conditional on having an active pipeline request.** Browse rows render for releases that may or may not have an `album_requests` entry. The button is rendered only when `buildReleaseActionState(item).pipelineId` is non-null. Pipeline and Recents always show the button (every row has a request id by construction).
- **Summary loads via two parallel GETs.** `GET /search-plan` for plan structure + cursor + drift; `GET /search-plan/history?limit=3` for the last few attempts. `Promise.all` for low latency. The detail page makes the same two calls but with the full pagination cursor on the history side.
- **Render style: template-literal `innerHTML` with `esc()`.** Mirrors every other module. No lit-html, no virtual DOM, no innovation.
- **Deploy ordering.** U1 (backend) is committed and deployed first to `cratedigger-web`. Frontend U2-U5 iterate locally with `web_dev_server.py --data prod-api` against the deployed v1 backend. Single canonical deploy after U5 ships everything.
- **No Playwright unit in v1.** Origin D7 explicitly defers cosmetic and verification details to dev iteration via the test webui. A Playwright smoke is a verification check in dev, not a planned implementation unit.

---

## Open Questions

### Resolved During Planning

- **Does the existing `GET /search-plan` payload satisfy the detail page's history needs?** No — verified by reading `lib/search_plan_inspection.py` and `web/routes/pipeline.py`. Add a new endpoint (U1).
- **What routing mechanism for the back button?** State.js field + Pipeline subview. No History API. (See Key Technical Decisions.)
- **Single module vs. extracted pure renderers?** Single module for v1. Extraction is Deferred to Follow-Up Work.
- **Confirmation UX shape?** Native `confirm()` for regen; inline form for advance. (See Key Technical Decisions.)
- **Legacy log segregation?** Two sections, not a toggle. (See Key Technical Decisions.)
- **Browse-tab button conditional?** Yes, conditional on `pipelineId !== null` via `buildReleaseActionState`. (See Key Technical Decisions.)
- **CLI parity for the new history endpoint?** Yes, add `pipeline-cli search-plan history`.

### Deferred to Implementation

- **Exact CSS class names** for the new summary/detail blocks (e.g., `.sp-summary`, `.sp-detail`, `.sp-history-row`). Pick during U2-U4 dev iteration; cosmetic only.
- **Default `limit` value on the history endpoint.** 50 is a sensible default; tune during dev if the detail page wants more or less.
- **Whether to invalidate the inspector's cache after a successful regenerate or advance** (i.e., refetch inline) or rely on the operator clicking the button again. Decide during U5 dev iteration; refetch inline is the better UX, but if it complicates the action handler it can ship as a follow-up.
- **Whether to surface the active plan's `provenance` block (omitted candidates, deduped losers, dropped low-entropy tokens) collapsed-by-default or always-visible.** Origin R10 requires it on the detail page; presentation tunes during U4 dev iteration.

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

**Data flow (summary panel):**

```
Click "🔍" button on any row  (Browse if pipelineId, Pipeline always, Recents always)
    │
    ▼
window.toggleSearchPlanSummary(requestId, rowEl)
    │
    ├──▶ Promise.all([
    │       fetch GET /api/pipeline/<id>/search-plan,
    │       fetch GET /api/pipeline/<id>/search-plan/history?limit=3
    │    ])
    │
    ▼
renderSummaryPanel({inspection, history}) → HTML string
    │
    ▼
sibling div .sp-summary.open  ← inserted/toggled below the row, mirrors .p-detail.open
```

**Data flow (detail page navigation):**

```
Click "Open detail →" inside .sp-summary
    │
    ▼
state.searchPlanDetailContext = {
    requestId,
    originTab,           // 'browse' | 'pipeline' | 'recents'
    originScrollY,       // window.scrollY at click time
    originSubView,       // 'queue' | 'dashboard' for pipeline; 'history'|'downloading'|'queue' for recents
}
state.pipelineView = 'search-plan-detail'
showTab('pipeline')
    │
    ▼
renderPipeline() dispatches on state.pipelineView:
    'queue'                  → renderQueue()
    'dashboard'              → renderPipelineDashboard()
    'search-plan-detail'     → renderSearchPlanDetail(state.searchPlanDetailContext.requestId)
    │
    ▼
Promise.all([
    fetch GET /api/pipeline/<id>/search-plan,
    fetch GET /api/pipeline/<id>/search-plan/history?limit=50,
])  → render full page into #pipeline-content
```

**Back button:**

```
Click "← Back" in .sp-detail page header
    │
    ▼
const ctx = state.searchPlanDetailContext
state.pipelineView = ctx.originSubView  (if originTab === 'pipeline')
showTab(ctx.originTab)
window.scrollTo(0, ctx.originScrollY)   (synchronously, after render)
state.searchPlanDetailContext = null
```

**New history endpoint contract:**

```
GET /api/pipeline/<id>/search-plan/history?limit=50&before_id=12345

200 OK
{
  "request_id": 2566,
  "rows": [
    {
      "id": 12340,
      "created_at": "2026-05-09T10:23:45Z",
      "request_id": 2566,
      "plan_id": 583, "plan_item_id": 5821, "plan_ordinal": 4,
      "plan_strategy": "track_0", "plan_canonical_query_key": "...",
      "plan_repeat_group": "track_0", "plan_generator_id": "12",
      "execution_stage": "accepted", "attempt_consumed": true,
      "cursor_update_status": "advanced", "stale_reason": null,
      "plan_cycle_snapshot": 3,
      "outcome": "no_match", "variant": "track_0", "query": "...",
      "result_count": 12, "elapsed_s": 4.23, "final_state": "Completed",
      "candidates": [...],
      "browse_time_s": 1.2, "match_time_s": 3.0,
      "peers_browsed": 5, "peers_browsed_lazy": 2, "fanout_waves": 2
    }
  ],
  "next_before_id": 12300         // null when exhausted
}

404 → request_not_found
400 → input validation (non-int limit/before_id, limit out of range)
```

---

## Implementation Units

### U1. Backend: paginated `search_log` history endpoint + paired CLI

**Goal:** Surface per-attempt `search_log` telemetry via a new diagnostic GET endpoint with cursor-style pagination, plus a paired `pipeline-cli search-plan history` subcommand. The new endpoint is the data source for both the summary's last-3 attempts and the detail page's full paginated history.

**Requirements:** R4, R5, R7, R8 (Origin R7, R8, R9, R10, R11, R12, R13, R18)

**Dependencies:** None.

**Files:**
- Modify: `lib/pipeline_db.py` (new `get_search_history_page(request_id, *, limit, before_id) -> SearchLogHistoryPage` method)
- Modify: `lib/search_plan_service.py` (new thin service method `history_for_request(...)` returning a typed result; or extend an existing service if simpler)
- Modify: `web/routes/pipeline.py` (new `get_pipeline_search_plan_history` handler; register in `_FUNC_GET_PATTERNS`)
- Modify: `scripts/pipeline_cli.py` (new `cmd_search_plan_history` subcommand under `pipeline-cli search-plan history <id>`)
- Modify: `tests/fakes.py` (new `FakePipelineDB.get_search_history_page` mirroring real method's signature + paging semantics)
- Modify: `tests/test_pipeline_db.py` (DB method coverage)
- Modify: `tests/test_search_plan_service.py` (service method coverage)
- Modify: `tests/test_pipeline_cli.py` (CLI subcommand coverage, exit-code mapping)
- Modify: `tests/test_web_server.py` (new contract test class `TestPipelineSearchPlanHistoryContract` + add `r"^/api/pipeline/(\d+)/search-plan/history$"` to `TestRouteContractAudit.CLASSIFIED_ROUTES`)
- Modify: `tests/test_fakes.py` (extend `TestFakePipelineDBSearchPlanContract` to assert parity for the new method)

**Approach:**
- DB method: `SELECT * FROM search_log WHERE request_id = %s AND (%s IS NULL OR id < %s) ORDER BY id DESC LIMIT %s + 1`. Take first `limit` rows; if `limit + 1` rows came back, the extra row's id seeds `next_before_id`. Cap `limit` to `[1, 200]` server-side; reject otherwise as 400 / exit 3.
- Service method returns a typed result dataclass with `rows: list[dict]` and `next_before_id: int | None`. Reuses dict shape from `search_log` row directly — no new struct needed unless `msgspec` validation at the boundary is wanted (defer; existing `GET /search-plan` returns a dict tree, mirror that style).
- Route handler validates `limit` and `before_id` query params (must be ints; `limit ∈ [1, 200]`; `before_id` ≥ 1 when present), calls the service method, returns `{request_id, rows, next_before_id}`. Status codes: 200 success, 400 input-validation, 404 request-not-found.
- CLI subcommand: `pipeline-cli search-plan history <id> [--limit N] [--before-id ID] [--json]`. Default human format renders one line per row (`[created_at] outcome plan_strategy ord=N query=...`) with the `next_before_id` printed as a "next page" hint. JSON mode returns the same payload as the API.
- Routing convention: composite FK on `search_log.plan_id` + `search_log.request_id` is preserved by reading rows for a single `request_id`; legacy `plan_id IS NULL` rows are returned in the same payload (the detail page segregates them client-side).

**Patterns to follow:**
- `web/routes/pipeline.py::post_pipeline_search_plan_advance` (PR #239) for the request-validation + service-call + status-mapping shape.
- `lib/pipeline_db.py::get_search_plan_inspection` for the DB method signature shape and `psycopg2.extras` row dict handling.
- `scripts/pipeline_cli.py::cmd_search_plan_advance` for argparse + JSON-mode toggle + exit-code mapping.
- `tests/test_web_server.py::TestPipelineSearchPlanAdvanceContract` for the contract test layout (`_patch_service` helper + `_assert_required_fields` + per-status-code cases).

**Test scenarios:**
- *Happy path — first page.* Given a request with 75 search_log rows, when called with `limit=50`, then return 50 rows newest-first and `next_before_id` equal to the 51st row's id.
- *Happy path — exhausted.* Given a request with 30 rows, when called with `limit=50`, then return all 30 rows and `next_before_id = null`.
- *Cursor pagination.* Given a sequence of calls: first `limit=50`, second `limit=50&before_id=<first.next_before_id>`, then assert no row appears in both pages and pages are id-monotonic descending.
- *Edge case — empty.* Given a request with zero `search_log` rows, when called, then return `{rows: [], next_before_id: null}` with status 200.
- *Edge case — legacy-only rows.* Given a request whose only `search_log` rows are pre-014 (`plan_id IS NULL`), when called, then return them with the plan-aware columns reading null. The frontend handles segregation.
- *Edge case — limit clamp.* Given `?limit=500`, then 400 with input-validation error message naming the bound `[1, 200]`.
- *Edge case — non-int limit.* Given `?limit=abc`, then 400.
- *Edge case — non-int before_id.* Given `?before_id=abc`, then 400.
- *Error path — request not found.* Given an unknown `request_id`, then 404.
- *Error path — limit zero or negative.* Given `?limit=0` or `?limit=-1`, then 400.
- *FakePipelineDB parity.* `tests/test_fakes.py::TestFakePipelineDBSearchPlanContract` includes the new method in its signature-parity assertion.
- *CLI exit codes.* `cmd_search_plan_history` returns 0 success, 2 not-found, 3 input-validation; mirror the established mapping.
- *Route audit.* Adding the route without classifying it in `CLASSIFIED_ROUTES` fails `test_all_web_routes_are_classified_for_contract_coverage` — the audit stays green only after registry update.

**Verification:**
- `nix-shell --run "bash scripts/run_tests.sh"` passes; `pyright lib/pipeline_db.py lib/search_plan_service.py web/routes/pipeline.py scripts/pipeline_cli.py` reports 0 errors.
- After deploy: `ssh doc2 'curl -s http://localhost:5300/api/pipeline/2566/search-plan/history?limit=3 | jq .'` returns 3 rows + a `next_before_id` value; `ssh doc2 'pipeline-cli search-plan history 2566 --limit 5'` prints rows with the correct `next_before_id` hint.

---

### U2. Frontend: state scaffold + module + main wiring

**Goal:** Set up the foundation for the inspector — the new `web/js/search_plan.js` module, the `state.js` field for back-button context, and `main.js` window bindings — without yet rendering any user-visible UI. This is the seam that U3-U5 plug into.

**Requirements:** R1, R2 (Origin R2, R4, R18)

**Dependencies:** U1 (the new history endpoint must be reachable for fetch helpers; U2 itself does not block U1, but iterating U2 in `prod-api` mode against deployed prod requires U1 in prod first).

**Files:**
- Create: `web/js/search_plan.js` (module with `// @ts-check`, module-level state, fetch helpers, exports)
- Modify: `web/js/state.js` (add `searchPlanDetailContext` field with JSDoc typedef on `state`)
- Modify: `web/js/main.js` (import the module; register `window.toggleSearchPlanSummary`, `window.openSearchPlanDetail`, `window.closeSearchPlanDetail`, `window.searchPlanRegenerate`, `window.searchPlanAdvance`)
- Create: `tests/test_js_search_plan.mjs` (Node-runner unit tests for pure helpers)

**Approach:**
- `state.js`: extend the JSDoc typedef of `state` with `searchPlanDetailContext: SearchPlanDetailContext | null`. Initialize to `null`. Export a typedef for `SearchPlanDetailContext = {requestId: number, originTab: string, originScrollY: number, originSubView: string | null}`.
- `search_plan.js`: scaffold with fetch helpers `fetchInspection(requestId)`, `fetchHistoryPage(requestId, {limit, beforeId})`. Pure helpers `buildHistoryUrl({requestId, limit, beforeId})` (testable without DOM). Module-level `searchPlanCache: Map<requestId, {inspection, historyHead, fetchedAt}>` for lightweight memoization (cleared on regenerate/advance success).
- Action handlers (regen/advance/back/open) get stub implementations that throw "TODO" or no-op; U3-U5 fill them in. Wiring `main.js → window.*` is the only exposure surface.
- No HTML rendering yet. No DOM mutation. No CSS additions.

**Patterns to follow:**
- `web/js/recents.js` for module shape + `// @ts-check` + JSDoc + exports.
- `web/js/state.js:45-47` `searchTargetId`/`searchTargetExpandId`/`searchTargetSource` for the stash-context precedent.
- `web/js/main.js:62-125` `window.*` registration block — add new bindings here, not in `search_plan.js` itself.
- `tests/test_js_util.mjs` for the Node-runner pattern — `import` the module, run assertions with `assert`, no DOM.

**Test scenarios:**
- *Pure helper — buildHistoryUrl.* Given `{requestId: 2566, limit: 50, beforeId: null}`, then `'/api/pipeline/2566/search-plan/history?limit=50'`.
- *Pure helper — buildHistoryUrl with cursor.* Given `{requestId: 2566, limit: 50, beforeId: 12345}`, then `'/api/pipeline/2566/search-plan/history?limit=50&before_id=12345'`.
- *Pure helper — buildHistoryUrl validates.* Given `{requestId: 0}` or non-positive, throw a typed error.
- *State stash/pop.* Pure helper that captures `{tab, scrollY, subView}` from a synthetic input and rehydrates into `state.searchPlanDetailContext`; assert round-trip equality.
- *Cache invalidation.* Given a populated `searchPlanCache`, when the (pure) invalidator is called with a request id, then that entry is removed and others remain.

**Verification:**
- `node --check web/js/search_plan.js web/js/state.js web/js/main.js` passes (CI runs this).
- `node tests/test_js_search_plan.mjs` passes.
- `pyright` is N/A (JS only); browser opens `web/index.html` with no console errors via `web_dev_server.py --data prod-api`.

---

### U3. Frontend: summary panel + per-row button injection

**Goal:** Render the in-place summary panel below an album row when the inspector button is clicked, with the button injected into all three album-row renderers (Browse conditional on `pipelineId`, Pipeline always, Recents always). After this unit, the summary view is fully usable; detail navigation and action buttons land in U4 and U5.

**Requirements:** R1, R2, R3, R5 (Origin R1, R2, R3, R5, R8, R11, R13, AE1, AE2, AE4)

**Dependencies:** U2 (uses `state.js` field, fetch helpers, `window.toggleSearchPlanSummary`); U1 (uses `/search-plan/history?limit=3` for last-attempts).

**Files:**
- Modify: `web/js/search_plan.js` (real `toggleSearchPlanSummary`, pure renderer `renderSummaryPanel({inspection, history})`)
- Modify: `web/js/discography.js` (inject button into `renderRgRow` and/or `renderRelease`; conditional on `buildReleaseActionState(...).pipelineId !== null`)
- Modify: `web/js/pipeline.js` (inject button into `renderPipelineItem` — always render)
- Modify: `web/js/recents.js` (inject button into `renderRecentsItems`, `renderImportQueueItems`, `renderDownloadingItems` as appropriate — always render)
- Modify: `web/index.html` (add CSS for `.sp-summary`, `.sp-summary.open`, `.sp-button`, drift indicator)
- Modify: `tests/test_js_search_plan.mjs` (add scenarios for `renderSummaryPanel`)

**Approach:**
- `renderSummaryPanel` is a pure function returning an HTML string. Inputs are the inspection payload and the history slice (last 3 rows). Outputs include: plan status badge, generator id with drift indicator (when `inspection.currentness.generator_id_mismatch === true`), cursor as `X/N`, cycle count, and a list of last 3 attempts (outcome + query + relative time). Cache stats on the summary are minimal; if shown, label cycle-level explicitly per R5/origin R11.
- `toggleSearchPlanSummary(requestId, rowEl)` mirrors `toggleDetail` semantics: locate or create a sibling `<div class="sp-summary" id="sp-summary-${requestId}">`, fetch via `Promise.all([fetchInspection, fetchHistoryPage(limit=3)])`, set `innerHTML`, toggle `.open`. Buttons inside the rendered HTML (regen, advance, open-detail, close) are stubs at this stage; U4-U5 wire them.
- Button injection: each row renderer adds a small `<button class="sp-button" onclick="toggleSearchPlanSummary(${id}, this.closest('.row-class'))" aria-label="Inspect search plan">🔍</button>` (icon and exact class names finalize during dev). Browse rows wrap in `state.acquireKind !== 'disabled' && state.pipelineId` test before rendering. The button placement adjacent to the existing acquire-action toolbar on Browse, adjacent to existing actions on Pipeline rows, and at the row trailing edge on Recents.
- CSS for `.sp-summary`/`.sp-summary.open` mirrors `.p-detail`/`.p-detail.open` exactly: `display: none` / `display: block`, padding, background tint to differentiate from `.p-detail` so a row that has BOTH expanded (rare but possible) is visually distinguishable.

**Patterns to follow:**
- `web/js/pipeline.js:686-810` `toggleDetail` for the fetch-then-render-then-toggle pattern.
- `web/js/release_actions.js::renderActionToolbar` for inserting a button next to existing actions on Browse.
- `web/js/util.js::esc()` for safe interpolation; `awstDateTime` for relative time formatting.
- `web/index.html:104-108` `.p-detail` CSS rules as the model for `.sp-summary`.

**Test scenarios:**
- **Covers AE2.** Given an inspection payload with `active_plan.next_ordinal=2`, `cycle_count=1`, `currentness.generator_id_mismatch=false`, when `renderSummaryPanel` runs, then the output HTML includes the cursor as `2/N`, cycle count `1`, plan status, and no drift indicator.
- **Covers AE4.** Given `currentness.generator_id_mismatch=true`, then the output includes a visibly-marked drift indicator with both the request's plan generator id and the current `SEARCH_PLAN_GENERATOR_ID`.
- *Happy path — last 3 attempts.* Given a history payload with 3 rows, then the output includes 3 attempt entries with outcome label + query + relative time.
- *Happy path — fewer than 3 attempts.* Given a history payload with 1 row, then the output includes 1 entry and a "no earlier attempts" hint or empty trailing space.
- *Edge case — no active plan.* Given `active_plan=null` (deterministic-failed state), then the output renders the failure class + sanitised error, not a slot list, and does not crash.
- *Edge case — escape interpolation.* Given an attempt query `"<script>alert(1)</script>"`, then the rendered HTML escapes it (no raw `<script>` substring).
- *Browse-row conditional.* Given a Browse row's `buildReleaseActionState(item).pipelineId === null`, then no button is injected; given non-null, the button is present.
- *Pipeline-row unconditional.* Given any Pipeline row, the button is present.
- *Recents-row unconditional.* Given any Recents row, the button is present.

**Verification:**
- `node --check web/js/*.js` passes.
- `node tests/test_js_util.mjs tests/test_js_search_plan.mjs` passes.
- Manual via `web_dev_server.py --data prod-api`: clicking the button on a Browse row opens the summary inline; clicking again toggles it closed; the Browse row without an `album_request` does not show the button. Same on Pipeline and Recents.

---

### U4. Frontend: detail subview + back button + scroll restore

**Goal:** Render the full detail page under the Pipeline tab when the operator clicks "Open detail" from the summary, with a back button that returns to the originating tab and prior scroll position.

**Requirements:** R2, R4, R5 (Origin R4, R6, R7, R8, R9, R10, R12, R13, AE3, AE5, AE7)

**Dependencies:** U2 (state.js, module scaffold), U1 (history endpoint with full pagination), U3 (the "Open detail" button click originates from the summary).

**Files:**
- Modify: `web/js/search_plan.js` (real `openSearchPlanDetail`, `closeSearchPlanDetail`, pure renderer `renderDetailPage({inspection, history, nextBeforeId})`, history pagination handler)
- Modify: `web/js/pipeline.js` (extend `setPipelineView` and the Pipeline render dispatcher to handle `state.pipelineView === 'search-plan-detail'`)
- Modify: `web/js/state.js` (no shape change — already added in U2; this unit hydrates and clears `searchPlanDetailContext`)
- Modify: `web/index.html` (CSS for `.sp-detail`, `.sp-history-row`, `.sp-history-row.legacy`, `.sp-back-button`, candidate-forensics expandable)
- Modify: `tests/test_js_search_plan.mjs` (add scenarios for `renderDetailPage` + back button state transitions)

**Approach:**
- `openSearchPlanDetail(requestId, rowContext)`: capture `state.searchPlanDetailContext = {requestId, originTab: getActiveTab(), originScrollY: window.scrollY, originSubView: getActiveSubView()}`, set `state.pipelineView = 'search-plan-detail'`, call `showTab('pipeline')`. The Pipeline render dispatcher reads `state.pipelineView` and routes to `renderSearchPlanDetail(requestId)` instead of the queue or dashboard renderer.
- `renderSearchPlanDetail(requestId)`: fetches `Promise.all([fetchInspection, fetchHistoryPage({limit: 50})])`, builds the detail HTML, sets `#pipeline-content.innerHTML`. Includes a "Load older" button that fetches the next page using `next_before_id` and appends rows to the history table. The back button at top-left calls `closeSearchPlanDetail()`.
- `renderDetailPage({inspection, history, nextBeforeId})` is pure (DOM-free, returns HTML string). Sections in order: header (request title, plan status, generator id + drift indicator, cursor, cycle); plan slot list (with cursor highlight); plan-aware history table (rows with full telemetry, candidate JSONB collapsed per row with expand-on-click); per-slot stats table (`get_search_plan_stats` aggregates with cache attribution label "cycle-level"); plan health (failure class, sanitised error, provenance: omitted candidates, deduped losers, dropped low-entropy tokens); collapsed "Pre-rollout history" section (the `legacy_logs.head` + count). All cache values labeled "cycle-level" per R5/origin R11.
- `closeSearchPlanDetail()`: `const ctx = state.searchPlanDetailContext`; if ctx is null, no-op; else: when `ctx.originTab === 'pipeline'`, restore `state.pipelineView = ctx.originSubView`; call `showTab(ctx.originTab)`; `requestAnimationFrame(() => window.scrollTo(0, ctx.originScrollY))` (after the new tab renders); clear `state.searchPlanDetailContext = null`.
- Candidate forensics expand: each history row's `candidates` JSONB is rendered into a `<details>` element (native browser collapsible) with the JSON pretty-printed. No JS event handlers needed.
- Legacy log rows: rendered in their own collapsed section using existing `legacy_logs.head` from the inspection payload (5-row cap is fine — origin only requires they're visible). Include the count text "Pre-rollout history (N rows; showing first 5)".

**Patterns to follow:**
- `web/js/pipeline.js:31` `setPipelineView` and `:151-163` `renderPipelineNav` for the subview-switch idiom.
- `web/js/decisions.js` for a Pipeline-style "full content render area" example (`#pipeline-content` analog).
- HTML5 `<details>`/`<summary>` for the candidate-forensics expandable, mirroring how the existing `forensics` block is rendered in `util.js::renderForensicBlock`.
- `web/js/wrong-matches.js:805-820` for the in-place DOM mutation pattern (preserves scroll); applies analogously for paginated "Load older" appending.

**Test scenarios:**
- **Covers AE3.** Given `originTab='browse'`, `originScrollY=420`, when `closeSearchPlanDetail` runs, then `showTab('browse')` is called and `window.scrollTo(0, 420)` is scheduled. (Use a fake `window`/`document` shim for the test.)
- **Covers AE5.** Given a complete inspection + history payload, when `renderDetailPage` runs, then the output HTML includes the slot list, the history table with telemetry columns (outcome, elapsed, result_count, final_state, cursor_update_status, stale_reason, plan_strategy, plan_ordinal, attempt_consumed, plan_cycle_snapshot), per-slot stats, plan-health block, and collapsed pre-rollout section.
- **Covers AE6.** Given the per-slot stats block in the input, when rendered, then any cache stat label includes the literal substring "cycle-level" (or equivalent visible signal).
- **Covers AE7.** Given a request with both plan-aware (`history.rows` non-empty) and legacy (`inspection.legacy_logs.head` non-empty) rows, when rendered, then plan-aware rows are in their own table and legacy rows are in a separate, visibly-distinct collapsed section.
- **Covers AE10.** Given a click on "Refresh" on the detail page, when the handler runs, then it re-fetches `/search-plan` and `/search-plan/history?limit=50` (asserted via fetch-mock spy) and re-renders.
- *Happy path — pagination.* Given an initial render with 50 rows + `next_before_id=12300`, when "Load older" is clicked, then a fetch hits `/search-plan/history?limit=50&before_id=12300` (spy assertion) and the new rows append below the existing ones (DOM assertion via JSDOM-free string match).
- *Edge case — exhausted pagination.* Given `next_before_id=null` after an initial render, then no "Load older" button is rendered.
- *Edge case — no plan-aware history.* Given `history.rows=[]` but `legacy_logs.head` non-empty, then the detail page shows "No plan-aware attempts yet" and the legacy section is the only history section visible.
- *Edge case — no history at all.* Given both empty, then both sections render empty-state messages and no errors.
- *Edge case — back when origin context is missing.* Given `state.searchPlanDetailContext === null` (e.g. user navigated directly via URL refresh — though v1 has no deep-link support), when back is clicked, then a fallback to `showTab('pipeline')` with `state.pipelineView='queue'` runs without throwing.

**Verification:**
- `node --check web/js/*.js` passes.
- `node tests/test_js_search_plan.mjs` passes.
- Manual via `web_dev_server.py --data prod-api`: open summary on a Browse row → click "Open detail" → confirm Pipeline tab activates with the detail page rendered → click back → confirm Browse tab is active and scroll position is restored. Repeat from Pipeline tab and Recents tab.

---

### U5. Frontend: action buttons (regenerate, advance) with confirmation UX

**Goal:** Wire the `regenerate` and `advance` operator actions on both summary and detail views, with native `confirm()` for regenerate and an inline form for advance. After this unit, the inspector is fully usable end-to-end and the brainstorm's R6/R7/F3 are satisfied.

**Requirements:** R6, R7 (Origin R14, R15, R16, R17, AE8, AE9, AE10)

**Dependencies:** U3, U4 (the buttons live in both rendered surfaces), U2 (`window.searchPlanRegenerate`, `window.searchPlanAdvance` bindings).

**Files:**
- Modify: `web/js/search_plan.js` (real `searchPlanRegenerate(requestId, opts?)`, `searchPlanAdvance(requestId, target)`, inline-form helpers `renderAdvanceForm(activePlan)`, `parseAdvanceTarget(formData)`)
- Modify: `web/js/main.js` (no new bindings if U2 already registered them; otherwise add)
- Modify: `web/index.html` (CSS for `.sp-action-button`, `.sp-action-button.destructive`, `.sp-advance-form`)
- Modify: `tests/test_js_search_plan.mjs` (add scenarios for action helpers + parse logic)

**Approach:**
- `searchPlanRegenerate(requestId)`: call `window.confirm("Regenerate this request's search plan? This will reset the cursor and cycle count.")`; if confirmed, `POST /api/pipeline/<id>/search-plan/regenerate` with `{prepend_artist: false}` (the existing default; advanced toggle deferred). On 200/`success` or `success_noop`, refresh the inspector cache and re-render. On 422 (`failed_deterministic`) or 503 (`failed_transient`), show a toast with the sanitised error message via `state.toast()`. On 404, toast and refresh inspector (the request is gone).
- `searchPlanAdvance(requestId, target)`: target is `{toOrdinal: int}` or `{toStrategy: str}` derived from the inline form. POST the appropriate body to `/api/pipeline/<id>/search-plan/advance`. On 200/`advanced`, refresh and re-render. On 422 (`invalid_target` — forward-only violated, out-of-range, ambiguous strategy), show the API's sanitised error in a toast. On 409 (`no_active_plan`), toast "Regenerate first." On 400 (body validation), this is an internal bug — log to console and toast "Internal error (advance request malformed)."
- `renderAdvanceForm(activePlan)` is a pure renderer returning HTML for an inline form: a `<select>` of unique `strategy` values from `activePlan.items[]` with a "—" leading option, an `<input type="number" min="0" max="N-1">` for ordinal, and a "Confirm" button. Mutually-exclusive client-side validation: pick one of the two; the API enforces the contract regardless.
- `parseAdvanceTarget(formData)` is pure: returns `{toOrdinal: int}` or `{toStrategy: str}` or throws a typed validation error. Tests cover both branches and the error path.
- Buttons render inside both the summary panel (U3) and the detail page (U4); both surfaces invoke the same handlers.
- Refresh-after-success: handler clears `searchPlanCache` for the request and re-invokes the appropriate render (`toggleSearchPlanSummary` for the summary view; `renderSearchPlanDetail` for the detail view). The Pipeline-tab pipeline store is also invalidated via `updatePipelineStatus(requestId, ...)` if the regenerate result changes `request_status`.

**Patterns to follow:**
- `web/js/release_actions.js` for native `confirm()` destructive-action precedent (`confirmDeleteBeets`).
- `web/js/state.js::toast()` for user-visible error notifications.
- `web/js/state.js::updatePipelineStatus` for cross-module cache invalidation when a mutation changes request status.
- `web/routes/pipeline.py::post_pipeline_search_plan_regenerate` and `:advance` for the response shape that handlers consume.

**Test scenarios:**
- **Covers AE8.** Given the operator clicks regenerate, when `window.confirm` returns true, then a `POST /search-plan/regenerate` is dispatched (fetch spy); given `confirm` returns false, then no fetch is dispatched.
- **Covers AE8.** The confirm message includes both "cursor" and "cycle" (string-match assertion).
- **Covers AE9.** Given `parseAdvanceTarget({strategy: 'track'})`, then `{toStrategy: 'track'}`; given `{ordinal: '7'}`, then `{toOrdinal: 7}`; given both, then a typed error; given neither, then a typed error; given `ordinal='abc'`, then a typed error.
- **Covers AE9.** Given an API response of `{outcome: 'invalid_target', error_message: 'Forward-only ...'}` with status 422, when the advance handler processes it, then `state.toast()` is called with the sanitised error and the inspector does NOT show a successful advance.
- *Happy path — regenerate success.* Given `POST .../regenerate` returns 200 with `outcome=success`, then the inspector cache for that request is cleared and a re-fetch + re-render is invoked.
- *Happy path — regenerate noop.* Given the same with `outcome=success_noop`, then the same refresh logic runs.
- *Happy path — advance success.* Given `POST .../advance` with `{to_strategy: 'track'}` returns 200 with `outcome=advanced`, then refresh runs and the active plan's `next_ordinal` reflects the new value.
- *Error path — advance forward-only violated.* Given the API returns 422 with `outcome=invalid_target`, then the toast surfaces the message and no refresh runs.
- *Error path — advance no active plan.* Given the API returns 409 with `outcome=no_active_plan`, then the toast says "Regenerate first" (or surfaces the API's message) and no refresh runs.
- *Error path — regenerate transient.* Given the API returns 503 with `outcome=failed_transient`, then the toast surfaces the retry-soon message and no cache mutation.
- *Pure helper — `parseAdvanceTarget` strategy mode rejects empty string.* Given `{strategy: ''}`, then a typed error.
- *Pure helper — `parseAdvanceTarget` ordinal mode rejects negative.* Given `{ordinal: '-1'}`, then a typed error.

**Verification:**
- `node --check web/js/*.js` passes.
- `node tests/test_js_search_plan.mjs` passes.
- Manual via `web_dev_server.py --data prod-api` against deployed v1 backend: the inspector renders and reads correctly. Mutations dispatched against the deployed `cratedigger-web` service (not the dev server, which 405s POSTs): trigger regenerate on a known request → confirm cursor reset on `pipeline-cli search-plan show <id>`. Trigger advance with `--to-strategy track` → confirm `pipeline-cli search-plan show <id>` agrees with the new cursor.
- Final smoke: open summary on Browse → Open detail → trigger advance → verify post-action UI reflects the new state without a manual reload.

---

## System-Wide Impact

- **Interaction graph:** New surface `web/js/search_plan.js` is invoked from three row renderers (`discography.js`, `pipeline.js`, `recents.js`) plus from `pipeline.js`'s subview dispatcher. Window-bound handlers (`window.toggleSearchPlanSummary`, `window.openSearchPlanDetail`, `window.closeSearchPlanDetail`, `window.searchPlanRegenerate`, `window.searchPlanAdvance`) are the cross-module entry points. The existing `pipelineStore` is the cross-module cache the action handlers invalidate; `state.searchPlanDetailContext` is new state owned by `state.js`.
- **Error propagation:** Backend errors (404, 422, 409, 503) from existing mutation endpoints surface to the user via `state.toast()`. The new history endpoint's errors propagate the same way. `console.error` logs internal bugs (400 on advance is an internal-bug case since the form should validate client-side first).
- **State lifecycle risks:** `state.searchPlanDetailContext` must be cleared on back-button restore to prevent stale-context bugs (e.g., navigating away mid-detail and returning). The `searchPlanCache` must invalidate on regenerate/advance success or the inspector shows stale data after a mutation. `pipelineStore` updates piggyback on `updatePipelineStatus` after a regenerate that changes `request_status`.
- **API surface parity:** The new `GET /api/pipeline/<id>/search-plan/history` route gets a paired `pipeline-cli search-plan history` subcommand per CLI ⇄ API symmetry. Existing `regenerate` and `advance` endpoints are reused unchanged.
- **Integration coverage:** `tests/test_web_server.py` contract tests cover the new GET endpoint's wire format. `tests/test_pipeline_db.py` covers the DB cursor-pagination semantics. `tests/test_search_plan_service.py` covers the service-method outcome mapping. `tests/test_pipeline_cli.py` covers the CLI exit codes. `tests/fakes.py` `FakePipelineDB` parity is enforced by `tests/test_fakes.py::TestFakePipelineDBSearchPlanContract`. Frontend `tests/test_js_search_plan.mjs` covers pure helpers; manual dev iteration via `web_dev_server.py` covers DOM integration.
- **Unchanged invariants:** Existing endpoints (`GET /search-plan`, `POST .../regenerate`, `POST .../advance`) are not modified. Their response shapes and status code mappings stay frozen. `lib/search_plan_inspection.py`'s `LEGACY_LOG_HEAD_LIMIT=5` stays at 5 (the new history endpoint is the deeper-dive). `SEARCH_PLAN_GENERATOR_ID` is read from the existing inspection payload's `current_generator_id`; no new generator-id sourcing.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Backend deploy ordering: U1 must reach `cratedigger-web` before frontend U2-U5 can iterate via `--data prod-api` mode. | Land U1 in its own commit, deploy via `/deploy`, then start frontend work. The web dev server's `--data live-db` mode is a fallback for backend-host iteration if needed. |
| Back-button scroll restore racing with the destination tab's data fetch (e.g. `loadPipeline()` triggers an async re-render that resets scroll). | Use `requestAnimationFrame(() => window.scrollTo(0, originScrollY))` after the tab render call; if the tab triggers a follow-up fetch+render, set `originScrollY` again on first paint. Verify during U4 dev iteration. |
| Browse-row button rendered for every release at all times causes visual noise (583 rows with active requests, more without). | Conditional: button only renders when `buildReleaseActionState(item).pipelineId !== null`. Releases without a pipeline entry don't get the button. |
| Inline-expand summary collides visually with the existing per-row `.p-detail` panel when both are open on the same row. | Use a distinct CSS class (`.sp-summary` vs `.p-detail`) with a different background tint. Both sibling divs can coexist; their `.open` states are independent. Decide during U3 dev iteration whether to make them mutually exclusive. |
| New history endpoint returns 50 rows × full `candidates` JSONB → potentially 100KB+ per page on long-lived requests. | Cap `limit` to `[1, 200]` server-side; default to 50 for v1 and tune. JSONB rows are already in the DB; the only cost is wire size. Acceptable for a single-request operator-driven fetch. |
| Stale `searchPlanCache` after a mutation surfaces wrong cursor/cycle to the operator. | Invalidate the cache on regen/advance success in U5; refresh-after-mutation re-fetches both `/search-plan` and `/search-plan/history?limit=3`. |
| `state.searchPlanDetailContext` orphaned by browser refresh — operator reloads while on the detail page and cannot navigate "back." | Document the v1 limitation: refresh = lose origin context. Detect `state.searchPlanDetailContext === null` on detail-page load and fall back to `state.pipelineView='queue'`. Deep-linkable URLs are deferred (see Scope Boundaries). |
| Native `confirm()` dialog is jarring vs. inline confirm in a polished UI. | Origin D7 explicitly defers cosmetic UX to dev iteration. Native `confirm` matches existing destructive-action precedent (`confirmDeleteBeets`); inline-confirm refactor is a follow-up if dev iteration finds it warranted. |

---

## Documentation / Operational Notes

- After U1 deploys, update `docs/persisted-search-plans-rollout.md` with one short paragraph noting the new history endpoint and `pipeline-cli search-plan history` subcommand. The rollout doc is the operator's reference for `pipeline-cli search-plan` actions.
- After U5 ships, update `CLAUDE.md` § "Persisted search plans" paragraph: add a one-line "Web inspector: per-album button on Browse / Pipeline / Recents opens summary panel + detail page under Pipeline."
- No NixOS module change needed — the new endpoint is served by the existing `cratedigger-web` service.
- No migration needed — the data is in `search_log` from migration 014.
- Standard deploy: `git push` → `nix flake update cratedigger-src` on doc1 → `nixos-rebuild switch` doc2. The `cratedigger-web` service restarts on switch (`restartIfChanged` defaults to true). Same for the importer/cratedigger services if any imports happen, though this work doesn't touch them.

---

## Sources & References

- Origin document: [docs/brainstorms/2026-05-09-search-plan-per-request-dashboard-requirements.md](docs/brainstorms/2026-05-09-search-plan-per-request-dashboard-requirements.md)
- Persisted-plan brainstorm origin: [docs/brainstorms/2026-05-08-persisted-search-plans-requirements.md](docs/brainstorms/2026-05-08-persisted-search-plans-requirements.md) (R72 deferred this UI)
- Persisted-plan plan: [docs/plans/2026-05-08-001-feat-persisted-search-plans-plan.md](docs/plans/2026-05-08-001-feat-persisted-search-plans-plan.md)
- Pipeline DB schema reference: [docs/pipeline-db-schema.md](docs/pipeline-db-schema.md)
- Rollout / verification queries: [docs/persisted-search-plans-rollout.md](docs/persisted-search-plans-rollout.md)
- Migration: [migrations/014_persisted_search_plans.sql](migrations/014_persisted_search_plans.sql)
- Web rules: [.claude/rules/web.md](.claude/rules/web.md)
- Code-quality rules: [.claude/rules/code-quality.md](.claude/rules/code-quality.md) (especially § CLI ⇄ API Surface Symmetry, § Test Taxonomy, § API Contract Tests)
- PR #225 (persisted plans data model + initial CLI/API): [commit references in `git log --grep=225`]
- PR #239 (operator-driven cursor advance): [commit references in `git log --grep=239`]

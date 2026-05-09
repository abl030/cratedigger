---
date: 2026-05-09
topic: search-plan-per-request-dashboard
---

# Search-Plan Per-Request Dashboard

## Summary

A per-album button on every album-listing tab (Browse, Pipeline, Recents) opens a compact summary panel of a request's active search plan in place, with an "Open detail" affordance that routes to a full per-request inspector under the Pipeline tab. Both views surface the three operator actions (`show`, `regenerate`, `advance`) and visibly honor the persisted-plan honesty constraints from PR #225. Cross-request analytics and fleet health are deferred to v2.

---

## Problem Frame

PR #225 (deployed 2026-05-08) and PR #239 (deployed 2026-05-09) put the search-plan model and its three operator actions into production. As of deploy: 583 wanted requests with active plans, 5,388 plan items, 50,870+ historical `search_log` rows, growing every cycle. The data and the CLI exist. `GET /api/pipeline/<id>/search-plan` returns the inspection payload; `POST .../regenerate` and `POST .../advance` mutate. What does not exist is a UI: when an operator suspects a request is misbehaving, the only path is to open a terminal, `ssh doc2`, run `pipeline-cli search-plan show 2566`, read it, then run `pipeline-cli search-plan advance 2566 --to-strategy track` or `regenerate 2566` if the plan is degenerate.

The David Bowie 1967 incident behind PR #239 is the canonical case: artist+album dedup collapsed five plan slots into the same `*avid *owie` query, stranding the track-search slots behind them, and the operator typed raw SQL before the `advance` CLI existed. The pattern recurs on every per-request investigation; the cost shape is friction (terminal context-switch, many keystrokes, occasional ad-hoc SQL when the CLI doesn't expose what's needed) every time the operator wants to see what's happening on a single album. R72 in `docs/brainstorms/2026-05-08-persisted-search-plans-requirements.md` explicitly deferred the dashboard UI but committed the data model and stable aggregate dimensions to support it. v1 is mostly a frontend exercise sitting on top of three already-shipped endpoints.

---

## Actors

- A1. Operator: The single human running cratedigger. Triages stuck requests, audits search behavior, and executes repair actions from the web UI. Currently bounces to `pipeline-cli` over SSH whenever the UI cannot answer their question.
- A2. Search executor: Background process that consumes plan slots and writes `search_log`. Not a UI actor — its behavior is what the inspector surfaces.
- A3. Future cross-request dashboard: Out of scope for v1. The per-request inspector's per-slot stats and per-attempt structure should not contradict the data dimensions a future cross-request view will need (the v1 inspector is a leaf of that future tree, not a parallel implementation).

---

## Key Flows

- F1. Diagnose-and-repair from any tab
  - **Trigger:** Operator notices a stuck or misbehaving request anywhere an album appears (Browse, Pipeline, Recents).
  - **Actors:** A1
  - **Steps:** Click the search-plan button on the album row. The summary panel opens in place with plan status, generator-id alignment, cursor position, cycle count, and the last 1-3 attempt outcomes. If that's enough, click `regenerate` or `advance` directly. If deeper inspection is needed, click "Open detail" to navigate to the per-request inspector under the Pipeline tab — full plan, full attempt history, per-slot stats, candidate forensics, plan health, same action buttons. Click back to return to the originating tab.
  - **Outcome:** The operator either takes a repair action or confirms the request is healthy without leaving the web UI.
  - **Covered by:** R1, R2, R3, R4, R5, R6, R14

- F2. Operator-driven refresh
  - **Trigger:** Operator wants the latest state after triggering an action or waiting for the next cycle.
  - **Actors:** A1
  - **Steps:** Click the button or reload the detail page. The inspector queries the existing API and re-renders. No realtime push, no background polling.
  - **Outcome:** Current state on demand; no background load.
  - **Covered by:** R17

- F3. Mutation with consequence visibility
  - **Trigger:** Operator clicks `regenerate` or `advance` from either view.
  - **Actors:** A1
  - **Steps:** A confirmation step shows the consequence (regenerate: cursor and cycle reset; advance: list of slots being skipped). The operator confirms. The inspector calls the existing mutation endpoint, surfaces the result, and refreshes.
  - **Outcome:** The operator does not accidentally reset cursor/cycle without seeing the consequence first.
  - **Covered by:** R14, R15, R16

---

## Requirements

**Workflow and entry points**

- R1. The web UI must expose a per-album button on the Browse, Pipeline, and Recents tabs that opens the search-plan inspector. It must not appear on the Decisions or Wrong Matches tabs.
- R2. The button and inspector must be implemented as a single shared component used identically across the three tabs.
- R3. Clicking the button must open a compact summary in place on the current tab without changing tabs.
- R4. The summary must include an "Open detail" affordance that navigates to a per-request detail page rendered under the Pipeline tab. The detail page must include a back button that returns the operator to the originating tab and their prior scroll position.

**Summary content**

- R5. The summary must display: plan status, generator id, cursor position as `X/N`, cycle count, and the last 1-3 search-attempt outcomes (outcome label, query, when).

**Detail content**

- R6. The detail page must display the active plan as an ordered slot list (ordinal, strategy, query, canonical query key, repeat group) with the cursor's current position visible.
- R7. The detail page must display the request's recent attempt history with full per-row telemetry from `search_log` (outcome, elapsed, result count, final state, cursor update status, stale reason, query, peers, fanout). History must paginate by row count rather than by time window in v1.
- R8. The detail page must display per-slot stats: attempts, found rate, no-match rate, mean elapsed.
- R9. The detail page must display per-attempt candidate forensics (top match scores by user / directory / filetype) collapsed by default and expandable on demand.
- R10. The detail page must display plan health: status, failure class, sanitised error message, and provenance (omitted candidates, deduped losers, dropped low-entropy tokens).

**Honesty and integrity**

- R11. Wherever cache statistics appear on either view, they must be labeled cycle-level. The UI must not present cache numbers in a way that implies per-search attribution.
- R12. The detail page must show pre-rollout `search_log` rows whose `plan_id IS NULL`, but segregate them visibly from plan-aware rows. Hiding them is not acceptable.
- R13. Generator-id drift — the request's plan generator id differs from the current shared generator id — must be visibly surfaced on both summary and detail views.

**Operator actions**

- R14. The summary and detail views must both expose `regenerate` and `advance` action buttons.
- R15. The `regenerate` button must surface the consequence (resets cursor and cycle count) before commit and require explicit confirmation.
- R16. The `advance` button must accept either an ordinal target or a strategy prefix and enforce the forward-only constraint already present in the API.

**Refresh**

- R17. The inspector must refresh on click or page load. v1 must not implement realtime push, websockets, or background polling.

**Wiring discipline**

- R18. If v1 introduces any new web route (e.g. a paginated per-request `search_log` endpoint), it must follow the route-classification and contract-test discipline established in `.claude/rules/code-quality.md` and `.claude/rules/web.md`: vanilla JS only, no build step, registered in the route handler's GET/POST tables, classified in `TestRouteContractAudit.CLASSIFIED_ROUTES`, and covered by a contract test with a `REQUIRED_FIELDS` set. Per CLI ⇄ API symmetry, any new operator-mutating route owes a paired `pipeline-cli` subcommand; v1 does not introduce such a route.

---

## Acceptance Examples

- AE1. **Covers R1, R2.** Given the operator is on the Browse tab viewing a MusicBrainz release that has an active album request, when they look at the album row, then they see the search-plan button rendered by the same shared component as on the Pipeline and Recents tabs; given they navigate to Decisions or Wrong Matches, no such button appears.

- AE2. **Covers R3, R5.** Given the operator clicks the search-plan button on an album row in the Recents tab, when the summary panel opens, then it appears in place without changing tabs and shows plan status, generator id, cursor X/N, cycle count, and the most recent attempt outcomes with their queries and timing.

- AE3. **Covers R4.** Given the operator opens the summary from the Browse tab and clicks "Open detail", when the detail page renders, then they navigate to a Pipeline-tab-rendered detail view; clicking the detail page's back button returns them to the Browse tab at their prior scroll position.

- AE4. **Covers R13.** Given a request's active plan was generated under a generator id that no longer matches the current shared generator id, when the operator opens either the summary or the detail view, then drift is visibly surfaced rather than silently rendered.

- AE5. **Covers R6, R7, R8, R9, R10.** Given the operator opens the detail page for a long-running request, when the page renders, then they see the full ordered slot list with cursor position, the recent paginated attempt history with per-row telemetry, per-slot attempt / found / no-match counts and mean elapsed, the candidate-forensics blob collapsed by default per attempt, and plan health including status, failure class, sanitised error, and provenance.

- AE6. **Covers R11.** Given the detail page presents cache statistics derived from `cycle_metrics`, when the operator reads the rendered values, then the labels make explicit that those numbers are cycle-level rather than per-search.

- AE7. **Covers R12.** Given the operator opens the detail page for a request that has both pre-rollout (`plan_id IS NULL`) and plan-aware `search_log` rows, when the history renders, then both buckets are visible and legacy rows are segregated from plan-aware rows.

- AE8. **Covers R15.** Given the operator clicks `regenerate` on the summary, when the confirmation step appears, then it states that cursor and cycle count will reset; only after explicit confirmation does the request hit the regenerate endpoint.

- AE9. **Covers R16.** Given the operator clicks `advance` and chooses a strategy prefix that resolves to an ordinal earlier than the current cursor, when the action is dispatched, then the API returns the same forward-only violation outcome the CLI surfaces today and the inspector renders it as an error rather than as a successful advance.

- AE10. **Covers R17.** Given the detail page is open and the operator wants the latest state, when they click the refresh affordance or reload the page, then the inspector re-queries the API and re-renders; v1 does not auto-refresh on a timer or push channel.

---

## Success Criteria

- An operator can open the inspector for any album from any of the three album-listing tabs without leaving the web UI.
- An operator can answer "what is this request's plan, where is its cursor, and what just happened on its last few searches" from the summary panel without navigating away from the current tab.
- An operator can answer "why is this request not finding the album" from the detail page using only data the page renders, without dropping to `ssh doc2 'pipeline-cli ...'` or ad-hoc SQL.
- An operator can run `regenerate` or `advance` from the inspector and see the result without leaving the inspector.
- The inspector visibly preserves the persisted-plan honesty constraints (cache attribution level, generator id alignment, legacy-log visibility) so the operator can trust what they're reading.
- A future cross-request analytics view can reuse the per-slot stats and per-attempt structure rendered here without contradicting the data dimensions.
- A downstream planner can implement v1 without inventing new operator capabilities, new mutation surfaces, or new API contracts beyond what is already deferred to planning.

---

## Scope Boundaries

- Cross-request analytics (which canonical queries pull weight across the queue) — v2.
- Fleet health (readiness counters, generator drift across the queue, wrap-rate trends, deterministic-failed sticky lists) — v2.
- The Decisions and Wrong Matches tabs do not get the search-plan button in v1.
- Time-window filters (24h / 7d / since-deploy) for the per-request history — deferred; v1 paginates by row count.
- Realtime push, websockets, and timer-based polling of `search_log` — out of scope.
- Before/after comparison across superseded plans (pre-regenerate vs. post-regenerate stats on the same request) — deferred.
- Deep-linkable URLs to a request's detail view — deferred; v1 reaches detail only via the summary's "Open detail" affordance.
- New operator actions beyond `show`, `regenerate`, and `advance` — out of scope.
- New CLI subcommands beyond what `pipeline-cli search-plan` already exposes — out of scope.
- Generator-output fixes — including the self-titled-dedup follow-up flagged in PR #239's "Out of scope" — tracked separately.

---

## Key Decisions

- D1. Two-view shape (in-place summary + detail under Pipeline tab) over a single dedicated tab. Rationale: the operator wants to inspect from any album-listing context without losing where they were; a single dedicated tab forces a context-switch every time.
- D2. The detail page lives under the Pipeline tab rather than as its own top-level tab. Rationale: Pipeline is the canonical home for per-request views; routing detail inside it keeps the tab count from creeping and gives the back button a clear origin/destination model.
- D3. v1 surfaces exactly the three existing operator actions. No new actions land in v1. Rationale: PR #239 just established CLI ⇄ API symmetry on these three; v1 stays inside that contract.
- D4. The inspector reuses existing API endpoints by default. New endpoints are introduced only if the existing inspection payload's history slice proves too narrow, and that decision is deferred to planning. Rationale: every new endpoint costs a contract test, a `CLASSIFIED_ROUTES` entry, and a CLI counterpart per the symmetry rule.
- D5. The history view paginates by row count rather than by time window in v1. Rationale: time-window filtering is a v2 cross-request analytics feature; per-request investigation almost always wants "the most recent N attempts" rather than "everything in the last 24h."
- D6. Refresh is operator-driven (click / load). No realtime, no polling. Rationale: search cycles run every 5 minutes; sub-cycle latency does not change what the operator sees, and adding push infrastructure is disproportionate.
- D7. Cosmetic UI specifics (panel size, button placement, label wording, exact layout) are deferred to dev iteration via the test webui. Rationale: the test webui exists for exactly this kind of "see it, adjust it" loop; pinning cosmetic details at brainstorm time is wasted ceremony.
- D8. Cache-attribution honesty, legacy-log visibility, and generator-id drift surfacing are first-class requirements, not nice-to-haves. Rationale: the persisted-plan rollout doc names these as non-negotiable; rendering otherwise misleads the operator.

---

## Dependencies / Assumptions

- The existing `GET /api/pipeline/<id>/search-plan` route returns the inspection payload the summary and detail need. Verified — the route is implemented in `web/routes/pipeline.py` and consumed today by `pipeline-cli search-plan show`.
- The existing `POST /api/pipeline/<id>/search-plan/regenerate` and `POST /api/pipeline/<id>/search-plan/advance` routes are sufficient for v1 mutations. Verified — both shipped in PR #225 and PR #239 with the full status-code contract.
- The album-row component used by Browse, Pipeline, and Recents is structured such that a single button addition can apply to all three. Unverified at brainstorm time; planning should confirm and adapt the shared-component approach if structure differs.
- The Pipeline tab's existing layout has space for a routed per-request detail view, and `web/js/state.js` (or equivalent) can carry the back-button origin tab and prior scroll position. Unverified at brainstorm time; planning should pick the routing/state mechanism.

---

## Outstanding Questions

### Resolve Before Planning

*(none — scope decisions resolved in dialogue.)*

### Deferred to Planning

- [Affects R6-R10, R18][Technical] Decide whether the existing inspection payload's history slice is wide enough for the detail page's pagination, or whether a paginated per-request `search_log` endpoint is needed. If introduced, it must follow the wiring discipline in R18.
- [Affects R4][Technical] Decide the routing/state mechanism for the back button — hash-based URL state, sessionStorage, history API, or a simple state-module field. Must preserve origin tab and prior scroll position.
- [Affects R2][Technical] Decide where the unified inspector component lives in `web/js/` and what its module API looks like — single module, sub-modules per view, or a small package.
- [Affects R15, R16][UX] Decide the exact confirmation UX for the regenerate and advance buttons (modal, inline confirm, two-step click) during dev iteration with the test webui.
- [Affects R5, R6-R10][UX] Decide the panel/page layout, label wording, and visual hierarchy during dev iteration with the test webui.
- [Affects R12][Technical] Decide how the detail page's history view segregates legacy `plan_id IS NULL` rows — separate section, toggle, or visual delineation.

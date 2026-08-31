---
name: 1278-items-2-3-shipped
description: "2026-08-31: #1278 items 2+3 live — lib/surface_outcomes.py convention + registry audit, set-intent service, search-plan/convergence outcome tables; residue recorded on the issue"
metadata:
  type: project
---

Shipped and live-verified 2026-08-31 (PRs #1284/#1285/#1286, cratedigger
`1ee9840d`, nixosconfig `9cf5e353`): `lib/surface_outcomes.py` owns the
CLI⇄API status/exit convention — services declare ONE outcome → HTTP map and
derive `*_EXIT_CODES` via `exit_codes_from_http`; every service-owned map is
registered in `tests/test_surface_outcomes.py`'s discovery-swept audit. New
`lib/set_intent_service.py` (model: incomplete_mark) put set-intent behind
both surfaces; five per-action `SEARCH_PLAN_*` tables + the convergence pair
replaced the surface ladders. Behavior changes: set-intent downloading →
409/4 (was 400/1), intent aliases deleted, conflict exits derive from
`transitions.transition_conflict_http_status` (vanished row → 2), regenerate
transient → exit 5 (was 4).

Residue is recorded on #1278 (comment 2026-08-31), notably: search-plan
actions have no per-action outcome Literals so the audit's Literal clause is
inert for them, and the saturation/history tables are byte-identical so a
cross-lookup between them is undetectable by construction. Route-local maps
(beets-distance 410/500 etc.) stay outside the audit's bounded sweep by
design. Related: [[worktree-isolated-git-boundaries]].

---
title: "fix: Surface wrong-match triage audit in Recents"
type: fix
status: completed
date: 2026-04-26
origin: direct-user-request
---

# fix: Surface wrong-match triage audit in Recents

## Overview

Recents already shows the original download rejection, for example
`Wrong match (dist 0.190) · moundsofass`, but it does not show the follow-up
wrong-match triage result that can delete the source after preview. The fix is
to surface the persisted `download_log.validation_result.wrong_match_triage`
audit on Recents History rows and expanded download-history rows, so an
operator can see both parts of the story: the original beets mismatch and the
later preview cleanup reason, such as a spectral-stage confident reject.

---

## Problem Frame

The concrete operator symptom is a Recents entry for Test Icicles' *For
Screening Purposes Only* that reads like a plain wrong match at distance
`0.190`, while the underlying source was also deleted after preview because it
did not pass the spectral/import preview path. That audit already exists in
`validation_result.wrong_match_triage`; hiding it forces DB/JSONB inspection to
understand why the source disappeared.

This plan treats Recents History as the primary surface. The Queue subview is
not the right place for this specific evidence because terminal queue rows are
not rendered as live queue work, and the sample row is a historical
`download_log` card.

---

## Requirements Trace

- R1. Recents History collapsed cards must show when a rejected wrong-match row
  has a persisted triage action, especially `deleted_reject`.
- R2. The UI must preserve the original rejection cause and source username
  while adding the follow-up triage/preview cause.
- R3. The expanded download history must expose enough triage detail to explain
  whether the follow-up failed at preimport, spectral, quality comparison, or
  another preview stage.
- R4. API responses must be robust for legacy rows where `validation_result` is
  missing, malformed, string-encoded, or lacks `wrong_match_triage`.
- R5. The change must not alter wrong-match cleanup policy, import policy,
  spectral thresholds, beets distance thresholds, queue eligibility, or
  database schema.
- R6. Tests must pin the backend classification, API contract, collapsed
  Recents rendering, and expanded history rendering.

---

## Scope Boundaries

- Do not change `lib.wrong_match_triage.py` cleanup decisions.
- Do not re-run preview work from the Recents API or frontend.
- Do not add a migration; the audit already lives in JSONB.
- Do not expose raw source paths on collapsed cards. If path context is useful,
  keep it to the expanded detail view or a short basename.
- Do not modify the Recents Queue subview except where shared helpers make it
  unavoidable.
- Do not turn historical terminal import jobs back into live queue rows.

---

## Context & Research

### Relevant Code and Patterns

- `lib/wrong_match_triage.py` persists `action`, `reason`,
  `preview_verdict`, `preview_decision`, `stage_chain`, `cleanup`, and related
  flags under `download_log.validation_result.wrong_match_triage`.
- `lib/pipeline_db.py::get_log` already returns `download_log.validation_result`
  through the joined log query used by Recents History.
- `web/classify.py` is the pure presentation layer for Recents badges,
  verdicts, summaries, and existing warning chips such as postflight
  disambiguation failures.
- `web/download_history_view.py` builds the typed expanded-history payload from
  the same classification path.
- `web/routes/pipeline.py::get_pipeline_log` serializes classified log rows for
  `/api/pipeline/log`.
- `web/js/recents.js` renders collapsed Recents History cards and already has a
  warning-chip pattern for postflight problems.
- `web/js/history.js` renders the expanded per-download history inside the
  detail panel.
- `tests/test_web_recents.py`, `tests/test_web_server.py`, and
  `tests/test_js_recents.mjs` already cover this route and rendering style.

### Institutional Learnings

- `docs/pipeline-db-schema.md` documents that wrong-match triage audit remains
  in `download_log.validation_result.wrong_match_triage` after actionable
  `failed_path` pointers are cleared.
- `docs/webui-primer.md` documents Wrong Matches triage as an operator-visible
  workflow whose action and reason are stored for audit.
- `CLAUDE.md` requires test-first work and `nix-shell` for Python test runs.

### External References

- None. This is a repo-local JSONB-to-UI presentation fix with established
  local patterns.

---

## Key Technical Decisions

- Extract triage audit in `web/classify.py`, not in JavaScript. The backend
  already owns classification and can safely decode dict or string JSONB once.
- Add structured triage fields plus a compact display summary. The frontend
  should not parse `stage_chain` to infer the headline.
- Let the backend summary use `stage_chain` as a fallback signal when
  `reason`/`preview_decision` are generic. This matters for the target case:
  "failed spectral" may be visible as a stage token even when the durable
  preview decision is a broader cleanup/requeue outcome.
- Keep the collapsed card concise: preserve the existing summary and add a
  warning chip such as `triage: deleted` or `preview: spectral reject` with a
  hover/title carrying stage-chain detail.
- Show the fuller audit in the expanded download history: action, preview
  verdict/decision, reason, and stage chain.
- Treat `deleted_reject` as the important happy path for this bug, but keep the
  extraction generic enough to render `kept_would_import`, `kept_uncertain`,
  `stale_path_cleared`, and `preview_backfilled` without another UI contract.
- Fail closed for malformed JSONB: no triage chip, no route error.

---

## Open Questions

### Resolved During Planning

- Should this use the Queue subview? No. The reported row is a historical
  `download_log` card from Recents History.
- Is a DB migration needed? No. The persisted audit already exists in
  `validation_result`.
- Should Recents re-run preview to get fresher detail? No. Recents should only
  display durable audit already written by the pipeline.

### Deferred to Implementation

- Exact display text for each action/decision pair: choose short labels during
  implementation, but keep the Test Icicles shape explicit in tests.
- Whether to include a shortened source basename in expanded history: include
  only if it helps explain deleted files without cluttering the collapsed card.

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for
> review, not implementation specification. The implementing agent should treat
> it as context, not code to reproduce.*

```mermaid
flowchart LR
    DL[download_log row] --> VR[validation_result JSONB]
    VR --> WT[wrong_match_triage audit]
    WT --> CL[web/classify.py classified fields]
    CL --> API1[/api/pipeline/log]
    CL --> API2[/api/pipeline/{id} history]
    API1 --> RC[Recents collapsed card chip]
    API2 --> HD[Expanded download history rows]
```

---

## Implementation Units

- U1. **Extract triage audit in shared classification**

**Goal:** Convert `validation_result.wrong_match_triage` into safe,
structured presentation fields that both Recents and expanded history can use.

**Requirements:** R1, R2, R3, R4

**Dependencies:** None

**Files:**
- Modify: `web/classify.py`
- Test: `tests/test_web_recents.py`

**Approach:**
- Add optional triage fields to the classified result, including action, reason,
  preview verdict/decision, stage chain, and a concise summary string.
- Parse `entry.validation_result` when it is either a dict or JSON string.
- Ignore non-dict audit payloads and malformed JSON without raising.
- Build the summary from persisted audit fields, not from raw stage parsing
  alone. Use `stage_chain` as a fallback classifier when the decision/reason is
  generic but a stage token clearly identifies preimport, spectral, quality, or
  post-import gate failure. For the target case, a row with
  `beets_scenario="high_distance"` and
  `wrong_match_triage.action="deleted_reject"` plus a spectral stage should
  still classify as the original wrong match while exposing the follow-up
  deletion/spectral summary separately.

**Execution note:** Add the failing classifier tests before changing the
extraction code.

**Patterns to follow:**
- `_extract_disambiguation_failure` and `_extract_bad_extensions` in
  `web/classify.py`.
- Existing malformed `import_result` handling that degrades gracefully.

**Test scenarios:**
- Happy path: rejected high-distance row with triage `deleted_reject`,
  `preview_verdict="confident_reject"`, `preview_decision` or `reason`
  indicating spectral rejection, and a stage chain returns the original
  `Wrong match (dist 0.190)` verdict plus non-null triage summary fields.
- Happy path: rejected high-distance row with generic `preview_decision` and
  `reason`, but a spectral reject token in `stage_chain`, still returns a
  deletion/spectral triage summary.
- Happy path: triage `kept_would_import` returns a concise kept/importable
  summary without changing the badge from `Rejected`.
- Edge case: dict-shaped `validation_result` without `wrong_match_triage`
  returns null/empty triage fields.
- Edge case: string-shaped JSONB with the same audit produces the same fields
  as a dict.
- Error path: malformed JSON string or non-object `wrong_match_triage` does not
  raise and produces no triage fields.

**Verification:**
- Classification remains pure, and existing Recents badge/verdict tests still
  pass with the new fields defaulted for legacy rows.

- U2. **Serialize triage fields through Recents and history APIs**

**Goal:** Make the classified triage audit available in both `/api/pipeline/log`
and the expanded request history payload.

**Requirements:** R1, R3, R4, R6

**Dependencies:** U1

**Files:**
- Modify: `web/download_history_view.py`
- Modify: `web/routes/pipeline.py`
- Modify: `tests/test_web_server.py`
- Modify: `tests/test_library_album_detail_service.py`

**Approach:**
- Extend `DownloadHistoryViewRow` with the new optional triage fields.
- Add the same fields to the `/api/pipeline/log` row serialization beside the
  existing `disambiguation_failure` and `bad_extensions` warning fields.
- Update route contract required-field sets so clean rows still return the keys
  with null/empty values.
- Preserve existing payload keys and meanings; this is an additive API change.

**Patterns to follow:**
- Existing disambiguation/bad-extension propagation from `ClassifiedEntry` to
  route JSON in `web/download_history_view.py` and `web/routes/pipeline.py`.
- Contract-field assertions in `tests/test_web_server.py`.

**Test scenarios:**
- Happy path: `/api/pipeline/log` returns triage fields for a rejected row whose
  mocked DB row carries `validation_result.wrong_match_triage`.
- Happy path: `/api/pipeline/{id}` history returns the same triage fields for
  the same raw download-log shape.
- Edge case: clean rows and legacy rejected rows include the new keys with
  null/empty values and do not break existing contract tests.
- Error path: malformed `validation_result` in a DB row does not 500 either
  endpoint.

**Verification:**
- API changes are additive, and the route contracts document the new frontend
  fields.

- U3. **Render triage audit in Recents collapsed and expanded views**

**Goal:** Let the operator see from Recents that a wrong-match row was later
previewed and deleted/reclassified, without opening JSONB manually.

**Requirements:** R1, R2, R3, R6

**Dependencies:** U2

**Files:**
- Modify: `web/js/recents.js`
- Modify: `web/js/history.js`
- Modify: `tests/test_js_recents.mjs`
- Create: `tests/test_js_history.mjs`

**Approach:**
- Add a compact warning chip to collapsed Recents rows when the new triage
  summary/action field is present.
- Keep the existing summary line intact so the username and original wrong
  match reason remain visible.
- Add expanded history rows for triage action, preview decision/reason, and
  stage chain when available.
- Escape all rendered values and keep hover/title text short enough for the
  current dense UI.

**Patterns to follow:**
- `disambigChip` and `badExtChip` in `web/js/recents.js`.
- Row rendering in `web/js/history.js`.
- Existing browserless JS tests in `tests/test_js_recents.mjs`.

**Test scenarios:**
- Happy path: collapsed Recents item for *For Screening Purposes Only* renders
  the existing `Wrong match (dist 0.190) · moundsofass` summary and an
  additional triage/deleted spectral chip.
- Happy path: expanded history item renders triage action, preview decision or
  reason, and a readable stage chain.
- Edge case: item without triage fields renders exactly as before, with no
  empty chip or placeholder text.
- Error path: triage fields containing HTML-like text are escaped in collapsed
  and expanded rendering.

**Verification:**
- The Recents History card explains both the original mismatch and the later
  deletion/preview reason in the UI.

- U4. **Document the Recents audit surface**

**Goal:** Update operator-facing docs so future debugging starts from Recents
instead of ad hoc JSONB queries.

**Requirements:** R3, R5

**Dependencies:** U1, U2, U3

**Files:**
- Modify: `docs/pipeline-db-schema.md`
- Modify: `docs/webui-primer.md`

**Approach:**
- Add a short note that `wrong_match_triage` audit is rendered in Recents
  History and in expanded download history.
- Keep the docs clear that this is display-only and does not change cleanup or
  import policy.

**Patterns to follow:**
- Existing Wrong Matches and Recents documentation in both docs.

**Test scenarios:**
- Test expectation: none -- documentation-only change.

**Verification:**
- Docs point operators to Recents for this audit trail and still describe JSONB
  as the source of truth.

---

## System-Wide Impact

- **Interaction graph:** `download_log.validation_result` flows through
  `web/classify.py`, `web/download_history_view.py`, `web/routes/pipeline.py`,
  `web/js/recents.js`, and `web/js/history.js`.
- **Error propagation:** malformed or missing audit fields should disappear
  from presentation, not fail API responses.
- **State lifecycle risks:** triage may clear `failed_path`, so Recents must
  read the persisted audit payload and not infer deletion from path presence.
- **API surface parity:** both collapsed Recents log rows and expanded history
  rows need the same audit fields so the card and details do not disagree.
- **Integration coverage:** route contract tests plus JS rendering tests should
  prove backend-to-frontend shape, while classifier tests prove JSONB decoding.
- **Unchanged invariants:** no queue claim rules, cleanup rules, spectral
  thresholds, beets distance thresholds, or persisted schema change.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| UI becomes noisy for every rejected row | Render only when persisted triage audit exists, and keep collapsed text to one chip. |
| Legacy malformed JSONB breaks Recents | Decode defensively and pin malformed cases in classifier/API tests. |
| Frontend infers semantics incorrectly from stage-chain strings | Backend provides a compact summary; stage chain is detail only. |
| Operator mistakes deleted source for current live queue failure | Keep this under History and label it as triage/preview audit, not live queue state. |

---

## Documentation / Operational Notes

- No deployment migration is required.
- Verification should include the existing Recents route and JS tests through
  the repo's `nix-shell` test path, per project guidance.
- After deploy, the live Test Icicles row should show the original wrong-match
  rejection plus the triage deletion/spectral preview audit without direct DB
  inspection.

---

## Sources & References

- Direct user report: Recents shows `For Screening Purposes Only` as
  `Wrong match (dist 0.190) · moundsofass` but not the later deleted/spectral
  triage outcome.
- Related requirements: `docs/brainstorms/import-preview-requirements.md`
- Related queue visibility requirements:
  `docs/brainstorms/importer-queue-requirements.md`
- Related plan: `docs/plans/2026-04-25-004-unified-import-preview-plan.md`
- Related plan:
  `docs/plans/2026-04-25-005-feat-async-preview-import-queue-plan.md`
- Related code: `lib/wrong_match_triage.py`
- Related code: `web/classify.py`
- Related code: `web/js/recents.js`

---
title: "feat: Wrong Matches converge workflow"
type: feature
status: completed
date: 2026-04-25
origin: direct-user-request
---

# feat: Wrong Matches converge workflow

## Overview

Wrong Matches needs a triage workflow for releases with many near-miss
candidates. Instead of force-importing one row at a time or deleting whole
groups blindly, each release group should expose a per-release validation-loosen
threshold. Candidates whose beets distance is within that threshold turn green.
The operator can then press **Converge** to enqueue every green candidate for
force import and optionally delete every non-green candidate for that release.

The first implementation should keep this as a Wrong Matches review workflow,
not a change to beets validation itself. The default threshold is `180`, treated
as milli-distance (`0.180`) because the UI displays candidate distances such as
`0.167`. The pipeline's existing force-import/importer queue remains the owner
of actual beets mutation and quality convergence.

---

## Problem Frame

Some releases accumulate many wrong-match candidates whose distances are just
above the normal validation threshold. The current UI makes the operator inspect
and act on each row individually. For examples like `Scott Walker - Scott 3`
with 15 candidates, the real workflow is:

1. Pick an acceptable loosened distance threshold for the release.
2. See which candidates become acceptable under that threshold.
3. Queue all acceptable candidates so the import queue can converge on the best
   resulting library state.
4. Optionally delete all unacceptable candidates so the release disappears from
   Wrong Matches.

The important boundary is that this should make manual triage faster without
turning the global matching policy into `0.180`. The default control value can
be `180` everywhere, but the threshold is a local operator review input for the
Wrong Matches group.

---

## Requirements Trace

- R1. Each Wrong Matches release group has a validation-loosen control that
  defaults to `180` milli-distance.
- R2. The control is per release group; editing one release's threshold changes
  only that group's green/not-green candidate state.
- R3. A candidate is green when its `distance` is known and
  `distance <= threshold_milli / 1000`.
- R4. The individual Force Import and Delete buttons remain available for each
  candidate.
- R5. The existing Delete All group action remains available.
- R6. A group-level Converge action queues force-import jobs for every green
  candidate in that release group.
- R7. Converge must not enqueue candidates that are no longer actionable:
  wrong request, missing files, missing `failed_path`, or already-active
  duplicate job for the same `download_log_id`.
- R8. After Converge queues green candidates, those selected rows should leave
  the actionable Wrong Matches list immediately while their source directories
  remain on disk for the importer job.
- R9. A checkbox labeled `remove all wrong matches when converging` controls
  whether non-green candidates for the same release are deleted during Converge.
- R10. When the checkbox is enabled, Converge deletes only the non-green
  candidates for that release; it must not delete green queued source folders.
- R11. The release group should disappear from Wrong Matches after Converge
  when all current candidates were either queued-and-dismissed or deleted.
- R12. Converge uses the existing `import_jobs` queue and `cratedigger-importer`
  execution path; it does not introduce another import worker.
- R13. Failure audit remains intact in `download_log` and `import_jobs`. Cleanup
  removes only actionable `failed_path` pointers and, for deleted non-green
  candidates, the source folders.

---

## Scope Boundaries

- This does not change global beets distance thresholds.
- This does not change `beets_validate()` or the validation result structure.
- This does not add pre-measured spectral caching or modify pre-import gates.
- This does not bypass audio integrity, spectral, or quality-gate checks during
  force import.
- This does not delete selected green source folders before import execution.
- This does not remove the existing single-row Force Import, single-row Delete,
  or Delete All controls.
- This does not add a new import job type; Converge creates ordinary
  `force_import` jobs.

---

## Context & Research

### Relevant Local Patterns

- `web/js/wrong-matches.js` renders grouped Wrong Matches cards, per-candidate
  Force Import/Delete actions, Delete All, active job badges, and refreshes the
  tab after terminal import jobs.
- `web/routes/imports.py::get_wrong_matches` builds the grouped payload from
  `PipelineDB.get_wrong_matches()`, resolves `failed_path`, filters missing
  folders, and enriches groups with active import jobs and last successful
  import summaries.
- `web/routes/imports.py::_delete_wrong_match_row` deletes a wrong-match source
  folder and clears its actionable `failed_path`.
- `web/routes/pipeline.py::post_pipeline_force_import` validates a single
  wrong-match row and enqueues an ordinary `force_import` job.
- `lib/import_queue.py::force_import_payload` and
  `force_import_dedupe_key(download_log_id)` are the existing queue contract.
- `scripts/importer.py` already cleans failed queued force-import sources and
  clears duplicate wrong-match rows after terminal, non-deferred rejection.
- `lib/wrong_matches.py::cleanup_wrong_match_source` already centralizes
  delete-plus-clear behavior for failed queued force imports. The new workflow
  should add a non-deleting companion for queued green candidates.
- `tests/test_web_server.py::TestWrongMatchesContract` is the existing route
  contract suite for grouped Wrong Matches.
- `tests/test_js_wrong_matches.mjs` is the existing browserless JS test harness
  for Wrong Matches polling behavior.

### Related Prior Requirements

- `docs/brainstorms/importer-queue-requirements.md` established that web
  force-import work should enqueue quickly, expose queued/running/failed state,
  and run through one importer lane.
- `docs/plans/2026-04-25-001-refactor-importer-queue-architecture-plan.md`
  implemented the queue-backed importer direction.
- `docs/plans/2026-04-25-002-fix-force-import-reject-cleanup-plan.md`
  established that failed queued force imports can clean source folders and
  clear actionable wrong-match pointers while preserving audit rows.

### External Research

None. This is a repo-local UI/API/queue workflow with established local
patterns.

---

## Key Technical Decisions

- Treat `180` as a milli-distance UI value. The underlying comparison uses
  `distance <= 0.180`.
- Keep the threshold client-side and per group. Persist it in `localStorage`
  keyed by `request_id` so refreshes do not throw away active triage work, but
  do not add a DB column.
- Add one backend endpoint for Converge instead of making the browser fire many
  single-row force-import and delete requests. The endpoint can validate the
  current DB state, compute the same green/non-green partition, enqueue jobs,
  and handle cleanup consistently.
- Compute green candidates on the backend from `request_id` and
  `threshold_milli`, rather than trusting a client-supplied list of green IDs.
  The UI still computes the same state for rendering.
- After queueing a green candidate, clear its actionable `failed_path` pointer
  without deleting its folder. The job payload already contains the resolved
  folder path that the importer needs. This is what lets the release disappear
  from Wrong Matches immediately.
- Delete only non-green candidates when the checkbox is enabled. These folders
  are not needed by any import job.
- Use existing `force_import` jobs with the existing per-download-log dedupe key
  so active duplicate submissions remain safe.
- Let the importer's existing quality gates converge the library state. Converge
  does not try to rank or predict which green candidate is best.

---

## Open Questions

### Resolved During Planning

- Should this be a global validation threshold change? No. The threshold is a
  Wrong Matches triage control only.
- Should Converge delete green candidate folders immediately? No. Those folders
  are the source data for queued import jobs.
- Should this require spectral premeasurement? No. That path is explicitly out
  of scope for this workflow.
- Should individual row actions remain? Yes. The user wants Converge in
  addition to the existing single-row and Delete All controls.

### Deferred to Implementation

- Exact visual styling for green candidate rows: use the current dark UI
  palette and existing badge/button classes; avoid a large redesign.
- Initial checkbox state: safest default is unchecked, but persist the last
  operator choice in `localStorage` so the workflow can become one-click after
  the user opts in.
- Exact response shape names for skipped entries: choose concise names while
  keeping enough detail for tests and operator toasts.

---

## High-Level Technical Design

```mermaid
sequenceDiagram
    participant UI as Wrong Matches UI
    participant Web as web/routes/imports.py
    participant DB as pipeline DB
    participant Worker as cratedigger-importer

    UI->>UI: Operator edits loosen threshold, green rows update
    UI->>Web: POST /api/wrong-matches/converge {request_id, threshold_milli, delete_unmatched}
    Web->>DB: Read current actionable wrong matches for request
    Web->>Web: Partition rows into green and non-green
    Web->>DB: Enqueue force_import jobs for green rows
    Web->>DB: Clear green failed_path pointers without deleting files
    alt delete_unmatched
        Web->>DB: Delete non-green source folders and clear failed_path
    end
    Web-->>UI: queued/deleted/skipped summary
    UI->>UI: Refresh Wrong Matches; release disappears when no entries remain
    Worker->>DB: Drain force_import jobs serially
    Worker->>DB: Record import success/failure audit and cleanup failed sources
```

This diagram is directional guidance only. Implementation should follow the
existing route, DB, and test conventions in the referenced files.

---

## Implementation Units

### U1. Add Non-Deleting Wrong-Match Dismissal Primitive

**Goal:** Provide a shared way to remove a queued green candidate from the
actionable Wrong Matches list without deleting the source folder needed by the
import job.

**Requirements:** R8, R11, R13

**Files:**
- Modify: `lib/wrong_matches.py`
- Modify: `tests/test_wrong_matches_cleanup.py`

**Approach:**
- Add a small helper next to `cleanup_wrong_match_source`, e.g.
  `dismiss_wrong_match_source(db, download_log_id, failed_path_hint=None)`.
- Reuse the same path-candidate logic as cleanup: raw `validation_result`
  path, payload/resolved path hint, and resolved absolute path when available.
- Clear matching rejected rows for the request/path using
  `clear_wrong_match_paths()` so older duplicate rows for the same folder do
  not reappear.
- Do not call `shutil.rmtree()` and do not require the path to exist for the DB
  pointer to be cleared.
- Return a small structured result with `entry_found`, `request_id`,
  `cleared_rows`, `raw_failed_path`, `failed_path_hint`, and `resolved_path`.

**Test Scenarios:**
- Given a wrong-match row with an existing folder, dismissal clears the
  actionable pointer and leaves the folder on disk.
- Given duplicate rejected rows for the same request/path, dismissal clears all
  matching rows so `get_wrong_matches()` does not reveal an older duplicate.
- Given a relative raw path and an absolute hint, dismissal clears both path
  representations.
- Given a missing download log row, dismissal reports `entry_found=False`.
- Given a row with missing path on disk, dismissal still clears stale DB
  pointers.

---

### U2. Add Wrong Matches Converge Endpoint

**Goal:** Queue green candidates for a release and optionally delete non-green
candidates in one validated backend operation.

**Requirements:** R3, R6, R7, R8, R9, R10, R11, R12, R13

**Files:**
- Modify: `web/routes/imports.py`
- Modify: `tests/test_web_server.py`
- Modify if needed: `tests/fakes.py`
- Modify if needed: `tests/test_fakes.py`

**Approach:**
- Add `POST /api/wrong-matches/converge`.
- Request body:
  - `request_id`: integer release/request id.
  - `threshold_milli`: integer, default/fallback `180`, clamped to a sane
    positive range such as `0..999`.
  - `delete_unmatched`: boolean.
- Read the current actionable wrong-match rows via `pdb.get_wrong_matches()`,
  filter to the target `request_id`, parse `validation_result`, and resolve
  each `failed_path`.
- Treat a row as green only when distance is numeric and
  `distance <= threshold_milli / 1000`.
- For each green row:
  - Resolve and validate files still exist.
  - Enqueue `IMPORT_JOB_FORCE` with the existing
    `force_import_dedupe_key(download_log_id)` and `force_import_payload()`.
  - Include source username from the row.
  - Dismiss the wrong-match pointer without deleting files using U1.
- For each non-green row:
  - If `delete_unmatched` is true, delete through the existing delete semantics
    and count it.
  - If false, leave it untouched.
- Return a summary with request id, threshold, queued jobs, deduped count,
  dismissed count, deleted count, skipped rows, and current group-empty hint.
- Keep the endpoint idempotent enough for retry: active deduped jobs should not
  create duplicate work; dismissed rows should not crash a repeated request.

**Test Scenarios:**
- Happy path: request has three rows at distances `0.167`, `0.180`, `0.226`;
  threshold `180` queues the first two, dismisses them, and leaves the third
  when `delete_unmatched=false`.
- Cleanup path: same rows with `delete_unmatched=true` queues/dismisses the
  first two and deletes the third.
- Boundary: distance equal to threshold is green.
- Edge case: missing or nonnumeric distance is non-green and only deleted when
  `delete_unmatched=true`.
- Edge case: wrong request rows are ignored.
- Edge case: missing `failed_path` or missing files skip queueing and return a
  skipped reason instead of enqueueing a broken job.
- Dedupe: if a green row already has an active force-import job, response marks
  it deduped and still dismisses the actionable wrong-match pointer.
- Safety: green source folders are not deleted by Converge.
- Contract: response shape includes stable fields the JS can render/toast.

---

### U3. Add Per-Release Threshold and Converge Controls to the UI

**Goal:** Let the operator adjust loosen distance, see green candidates, and
queue/cleanup a whole release group from the Wrong Matches card.

**Requirements:** R1, R2, R3, R4, R5, R6, R9, R11

**Files:**
- Modify: `web/js/wrong-matches.js`
- Modify: `web/js/main.js`
- Modify: `tests/test_js_wrong_matches.mjs`

**Approach:**
- Add group-level state helpers:
  - `thresholdForGroup(requestId)` defaults to `180`.
  - Store overrides in `localStorage` keyed by request id.
  - Store `delete_unmatched` preference in `localStorage`.
- In each expanded group, render:
  - Numeric threshold input/stepper for loosen milli-distance.
  - Count of green candidates, e.g. a compact badge in the control row.
  - Converge button, disabled when zero green candidates or active import jobs
    for the group make queue state ambiguous.
  - Checkbox labeled `remove all wrong matches when converging`.
- Apply green styling to entries whose distance passes the current threshold.
  Keep non-green entries readable and leave individual buttons intact.
- On threshold input change, update local state and re-render the group without
  refetching from the server.
- On Converge:
  - Confirm when `delete_unmatched` is true and there are non-green candidates.
  - POST to `/api/wrong-matches/converge`.
  - Toast queued/deleted counts.
  - Invalidate and refresh Wrong Matches.
- Export test seams in `__test__` for threshold normalization, green partition,
  and converge request building.

**Test Scenarios:**
- Default threshold is `180` and marks `0.167` green while `0.226` stays
  non-green.
- Editing one group's threshold does not affect another group.
- Boundary distance `0.180` is green at threshold `180`.
- Unknown distance is never green.
- Converge POST body includes `request_id`, `threshold_milli`, and
  `delete_unmatched`.
- When delete checkbox is enabled, Converge asks for confirmation before POST.
- Existing individual Force Import/Delete controls are still rendered.
- Successful Converge refreshes Wrong Matches.
- Failed Converge shows an error toast and leaves the UI state intact.

---

### U4. Update Documentation and Operator Notes

**Goal:** Document the new triage workflow and backend semantics so future work
does not confuse Converge with global validation policy.

**Requirements:** R12, R13

**Files:**
- Modify: `docs/webui-primer.md`
- Modify: `docs/pipeline-db-schema.md`

**Approach:**
- In `docs/webui-primer.md`, describe Wrong Matches Converge as a grouped
  triage workflow: set loosen threshold, green candidates, Converge queues
  force-import jobs, optional cleanup deletes non-green candidates.
- In `docs/pipeline-db-schema.md`, note that Converge still uses ordinary
  `import_jobs(job_type='force_import')` and that queued selected candidates
  may have their original `failed_path` pointer cleared while the job payload
  retains the resolved folder path.
- Explicitly state that Converge does not change global beets validation
  thresholds.

**Test Scenarios:**
- Documentation-only unit; verify during review that docs match endpoint and UI
  names.

---

## System-Wide Impact

- **UI:** Wrong Matches changes from row-by-row action only to group triage
  plus existing row actions.
- **Backend API:** Adds one endpoint under `web/routes/imports.py`; no schema
  migration required.
- **Queue:** Reuses `import_jobs` and existing force-import payload/dedupe
  semantics.
- **Files on disk:** Green selected folders remain until the importer consumes
  them. Non-green folders are deleted only when the cleanup checkbox is enabled.
- **Audit:** `download_log` rows remain; Converge clears actionable
  `failed_path` pointers to remove rows from the review queue.
- **Failure behavior:** Import job failures still surface through
  `import_jobs`, and the already-deployed failed-force cleanup handles rejected
  queued sources.

---

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Accidental destructive cleanup | Checkbox-gated deletion, confirmation when enabled, and tests proving green folders are not deleted. |
| UI/backend green partition drift | Backend recomputes green rows from `threshold_milli`; JS tests lock the same threshold math. |
| Older duplicate rows reappear after selected rows are dismissed | Use request/path clearing through the shared wrong-match helper, not single-row-only clearing. |
| Queued green sources disappear from Wrong Matches but import job later fails | Job remains visible in Import Jobs; failed force-import cleanup already records failure and deletes source on terminal rejection. |
| Active duplicate jobs are created | Reuse `force_import_dedupe_key(download_log_id)` and treat deduped jobs as successful queueing. |
| Converge deletes candidates for the wrong release | Endpoint filters by `request_id` from current `get_wrong_matches()` rows and ignores unrelated rows. |
| Threshold feels like a global policy change | Keep it UI-local, per group, and document it as triage-only. |

---

## Verification Plan

- Run focused backend contract tests:
  - `nix-shell --run "python3 -m unittest tests.test_web_server.TestWrongMatchesContract -v"`
- Run wrong-match helper tests:
  - `nix-shell --run "python3 -m unittest tests.test_wrong_matches_cleanup -v"`
- Run JS Wrong Matches tests:
  - `node tests/test_js_wrong_matches.mjs`
- Run fake contract tests if any fake DB method changes:
  - `nix-shell --run "python3 -m unittest tests.test_fakes -v"`
- Run broader web/import queue regression before shipping:
  - `nix-shell --run "python3 -m unittest tests.test_web_server tests.test_import_queue tests.test_wrong_matches_cleanup -v"`
  - `for f in tests/test_js_*.mjs; do node "$f" || exit 1; done`
- Run full suite before deploy:
  - `nix-shell --run "python3 -m unittest discover tests -v"`

---

## Implementation Sequence

1. Implement and test the non-deleting dismissal helper.
2. Add the Converge endpoint with backend-only tests.
3. Add the UI threshold/green/converge controls and JS tests.
4. Update docs.
5. Run focused tests, then full regression.

---

## Sources & References

- User request in current session: Wrong Matches per-release validation loosen,
  green candidates, Converge, optional deletion of non-green candidates.
- `docs/brainstorms/importer-queue-requirements.md`
- `docs/plans/2026-04-25-001-refactor-importer-queue-architecture-plan.md`
- `docs/plans/2026-04-25-002-fix-force-import-reject-cleanup-plan.md`
- `web/js/wrong-matches.js`
- `web/js/main.js`
- `web/routes/imports.py`
- `web/routes/pipeline.py`
- `lib/import_queue.py`
- `lib/wrong_matches.py`
- `lib/pipeline_db.py`
- `scripts/importer.py`
- `tests/test_web_server.py`
- `tests/test_js_wrong_matches.mjs`
- `tests/test_wrong_matches_cleanup.py`

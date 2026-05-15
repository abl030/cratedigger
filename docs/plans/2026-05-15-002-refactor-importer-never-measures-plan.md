---
title: "refactor: Importer never measures — preview owns candidate evidence"
type: refactor
status: active
date: 2026-05-15
origin: docs/brainstorms/2026-05-15-importer-never-measures-requirements.md
extends: docs/plans/2026-05-15-001-refactor-evidence-backed-import-mutation-plan.md
---

# refactor: Importer never measures — preview owns candidate evidence

## Summary

Make the preview worker the only producer of candidate evidence. Front-gate
preview's expensive measurement on the same cheap snapshot check the importer
uses (`load_candidate_evidence_for_source`). Replace the importer's hard-fail
on missing/stale evidence with a requeue back to `queued`/`waiting` so the
preview worker recovers the row on its next claim. Delete the
`services.cratedigger.importer.preview.enable` toggle and its
backward-compatible bypass branches across Python and Nix. Delete the
now-unreachable legacy dispatch branch that ran `inspect_local_files` /
`run_preimport_gates` directly.

---

## Problem Frame

The 2026-05-14 evidence-boundary refactor and the 2026-05-15-001 follow-up
established two of the three pieces of the intended model: durable
candidate evidence in a relational store, and evidence-authorized harness
mutation. The third piece — *how* candidate evidence reaches the mutation
boundary — is still wrong on this branch in two places:

The Nix option `services.cratedigger.importer.preview.enable` defaults to
off and, when off, marks newly enqueued jobs importable immediately. Every
`enqueue_import_job` (`lib/pipeline_db.py:922-939`), every fake
(`tests/fakes.py:752-810`), and the importer entry path
(`scripts/importer.py:61-87`, `186-209`, `_job_uses_preview_disabled_legacy_path`)
branches on a preview-disabled flag. A second migration surface — the
`exclude_preview_disabled_automation` parameter on `claim_next_import_job`
(`lib/pipeline_db.py:1097, 1126`) plus the
`requeue_disabled_automation_preview_jobs` method
(`lib/pipeline_db.py:1263`) called from `scripts/import_preview_worker.py:340`
and `scripts/importer.py:344` — exists to migrate legacy disabled-mode
rows and dies together with the toggle. The dual-mode surface adds permanent
carrying cost; production only ever wants preview on.

When the importer drains a job and `ensure_candidate_evidence_for_action`
returns unavailable — because the row is missing, the cheap snapshot
guard failed, or the evidence is incomplete — `lib/import_dispatch.py:1857-1870`
hard-fails with `DISPATCH_CODE_CANDIDATE_EVIDENCE_UNAVAILABLE`. The
2026-05-14 brainstorm's R6 specified that the active path "must recompute
or backfill evidence." The current code skips that step entirely. The
practical consequence: roughly 700 legacy Wrong Matches rows whose
originating `download_log` predates migration 017 cannot be force-imported
through the evidence-aware path. The recompute belongs in the preview
worker — the only producer of candidate evidence — not as a second
measurement codepath inside the importer.

Once preview is mandatory and the importer always supplies `import_job_id`
to dispatch (already true at `scripts/importer.py:187` modulo the
disabled-legacy branch), the legacy `_dispatch_import_from_db_locked`
fallback at `lib/import_dispatch.py:1899+` — which still runs
`inspect_local_files` and `run_preimport_gates` directly when no
`import_job_id`/`download_log_id` is supplied — becomes unreachable from
production and should be deleted with its test branches.

---

## Requirements

Cited from `docs/brainstorms/2026-05-15-importer-never-measures-requirements.md`:

**Preview is mandatory**
- R1. Delete the `services.cratedigger.importer.preview.enable` option and
  its `CRATEDIGGER_IMPORT_PREVIEW_ENABLE` env-var plumbing.
- R2. `enqueue_import_job` (and its fakes) must not branch on a
  preview-enabled flag; one enqueue path, one state.

**Importer never measures**
- R3. On invalid evidence at importer claim time, the importer requeues the
  job (flips status back to `queued`/`waiting`) instead of failing. It
  invokes no measurement helper.
- R4. A single shared function is the source of truth for evidence
  validity; both sides call it.

**Preview is idempotent on valid evidence**
- R5. Preview's front-gate is the same shared validity function. Valid
  evidence → mark ready, skip measurement. `None` → measure, persist, mark
  ready.

**Pipeline shape**
- R6. The dispatch branch that runs preimport gates directly (the
  `run_preimport_gates` path that fires when no `import_job_id`/`download_log_id`
  is supplied) is deleted.
- R7. The 2026-05-14 R6 importer-side fail-closed clause is superseded for
  the candidate-evidence path: requeue replaces fail-closed.

Acceptance examples AE1–AE6 from the origin are preserved as test targets;
each implementation unit cites the AE it covers.

---

## High-Level Technical Design

*Directional guidance for review, not implementation specification.*

The import-job state machine after this refactor is two axes — `status` and
`preview_status` — claimed by exactly one worker at each combination:

| Logical phase            | `status`  | `preview_status`              | Claimable by   |
|--------------------------|-----------|-------------------------------|----------------|
| Enqueued                 | `queued`  | `waiting`                     | preview worker |
| Preview measuring        | `queued`  | `running`                     | (held)         |
| Evidence ready           | `queued`  | `evidence_ready` (or legacy `would_import`) | importer       |
| Importer mutating        | `running` | `evidence_ready`              | (held)         |
| Importer requeue → preview | `queued`  | `waiting`                     | preview worker |
| Terminal                 | `completed` / `failed` | (any)            | —              |

The "Importer requeue → preview" row is the new edge. The existing
`requeue_running_import_jobs` helper (`lib/pipeline_db.py:1234-1261`)
demonstrates the exact column set: `status='queued'`, clear `worker_id`,
clear `started_at`, clear `heartbeat_at`, set `message`, set `updated_at`.
The new requeue method adds `preview_status='waiting'` on top, so preview's
claim WHERE clause (`lib/pipeline_db.py:1307-1336`) picks it up on its
next sweep.

Both sides — importer and preview — consult one shared validity gate:
`lib.quality_evidence.load_candidate_evidence_for_source`. It already
returns `EvidenceBuildResult(evidence=None, status='missing'|'stale'|'incomplete'|...)`
consistently. The importer uses it inside `ensure_candidate_evidence_for_action`
today; preview will use it as a front-gate inside `process_claimed_preview_job`
before invoking `execute_preview_job`.

---

## Implementation Units

### U1. Preview-worker cheap snapshot front-gate

**Goal:** Preview worker short-circuits measurement when stored candidate
evidence already passes the snapshot guard. Repeated claims of an unchanged
job become free.

**Requirements:** R4, R5. Covers AE4 (re-claim of valid evidence skips
measurement) and supports AE6 (legacy row flows through preview unchanged).

**Dependencies:** none.

**Files:**
- Modify: `scripts/import_preview_worker.py` (front-gate + extract
  `derive_canonical_import_folder` helper from
  `_materialize_automation_preview_path`)
- Modify: `tests/test_import_queue.py` (preview-worker behavior tests)
- Modify: `tests/test_integration_slices.py` (add a slice asserting no
  measurement on valid re-claim, for both force/manual and automation
  job types)

**Approach:**

In `process_claimed_preview_job` (`scripts/import_preview_worker.py:231`),
before invoking `execute_preview_job`, derive the candidate source path
cheaply and call
`load_candidate_evidence_for_source(db, source_path=..., import_job_id=job.id, download_log_id=_download_log_id_from_job(job))`.

For force/manual jobs the path is `payload.failed_path`. For automation
jobs, extract a new helper — `derive_canonical_import_folder(db, job, row, state)` —
from `_materialize_automation_preview_path` that runs ONLY the path
computation (`reconstruct_grab_list_entry` →
`_canonical_import_folder_path`), not the actual materialization. The
existing `_materialize_processing_dir` step stays inside the measuring
path and only runs when the front-gate misses. This keeps AE4 honest for
automation jobs: a re-claim of valid evidence skips both materialization
and measurement.

If `result.status == 'ready'` and `result.evidence is not None`, reuse
the `mark_import_job_preview_importable(job.id, preview_result=..., message=...)`
call shape currently at lines 254-259, but invoked before
`execute_preview_job` runs, with a `preview_payload` that records
`candidate_status='reused'` provenance.

If the result is anything else, fall through to the existing
`execute_preview_job` path. The post-measurement validity check on lines
249-253 remains as belt-and-braces in case measurement produces unusable
output.

**Execution note:** test-first. Write the "valid evidence + matching
snapshot → no measurement" test first and confirm it fails on `main`-of-branch
before adding the front-gate.

**Patterns to follow:**
- Provenance recording pattern from
  `lib/import_evidence.py::ensure_candidate_evidence_for_action`
- Existing `mark_import_job_preview_importable` call shape at
  `scripts/import_preview_worker.py:254-259`
- `tests/test_integration_slices.py::TestSpectralPropagationSlice` for the
  slice shape

**Test scenarios:**
- **Covers AE4.** Valid `import_job_candidate` evidence + matching
  snapshot → worker marks job ready without calling
  `preview_import_from_path` / `run_preimport_gates` / spectral analysis /
  V0 probing. Assert with a sentinel patch on those helpers.
- Missing evidence row → worker runs full measurement (existing behavior).
- Evidence row exists but snapshot mismatch → worker runs full
  measurement; new evidence row replaces stale one.
- Force/manual job with valid `download_log_candidate` evidence + matching
  snapshot → same skip-measurement behavior.
- Provenance: skipped path records `candidate_status='reused'` on the
  preview-result payload; measured path records `'recomputed'` (or
  whatever the existing builder writes).

**Verification:** The integration slice asserts that for a job whose
candidate evidence row exists and whose source files are unchanged, no
preview measurement helper is invoked between claim and `mark_import_job_preview_importable`.

---

### U2. Importer requeues invalid evidence to preview

**Goal:** Replace the importer's `DISPATCH_CODE_CANDIDATE_EVIDENCE_UNAVAILABLE`
hard-fail with a requeue that flips the job back to a preview-claimable
state. Preview's next sweep recovers it.

**Requirements:** R3, R4, R7. Covers AE2 (missing evidence → requeue → preview
measures), AE3 (stale snapshot → same), AE6 (legacy row flows through
unchanged).

**Dependencies:** U1 (so the preview worker doesn't redundantly re-measure
the next time around when evidence happens to still be valid). U1 and U2
land together in the same PR.

**Files:**
- Modify: `lib/pipeline_db.py` — add `requeue_import_job_for_preview` (or
  similar) that performs the column set documented above. Model on the
  existing `requeue_running_import_jobs` (`lib/pipeline_db.py:1234-1261`).
- Modify: `lib/import_dispatch.py` — at `_dispatch_import_from_db_locked`
  lines 1857-1870 (force/manual evidence-required branch) and at
  `lib/download.py:1172-1185` (automation evidence-required branch),
  replace the hard-fail return with a call to the new requeue method plus a
  `DispatchOutcome` outcome code that signals "requeued, do not record
  failure."
- Modify: `scripts/importer.py` — interpret the new dispatch outcome:
  do not increment failure attempts, do not write a `failed` terminal
  status; emit a log line and yield the worker tick.
- Modify: `tests/fakes.py` — add the requeue method to `FakePipelineDB`
  with self-tests in `tests/test_fakes.py`.
- Modify: `tests/test_pipeline_db.py` — direct test for the new requeue
  method against the real DB.
- Modify: `tests/test_dispatch_from_db.py` — replace the existing
  "evidence unavailable → fail" tests with "evidence unavailable →
  requeue."
- Modify: `tests/test_import_queue.py` — importer-loop test asserts requeue
  does not advance failure counters and flips status correctly.
- Modify: `tests/test_integration_slices.py` — add a slice that drains a
  force-import job with no candidate evidence and confirms it ends up back
  in `queued/waiting`.

**Approach:**

Pick a single dispatch outcome code (suggested: `DISPATCH_CODE_REQUEUED_FOR_PREVIEW`)
that the importer interprets as "requeue and yield, do not retry-count."
Dispatch performs the DB-side requeue itself (inside the locked region)
and returns the outcome; the importer recognizes the code and does not
write any terminal status on top.

Doing the requeue inside dispatch (rather than asking the importer to
requeue based on the outcome) keeps the lock semantics simple: the
advisory lock held during `_dispatch_import_from_db_locked` covers both
the evidence check and the state flip in one atomic transition.

The status flip must clear writer-side state that the preview worker's
claim WHERE clause would otherwise treat as stale: `worker_id=NULL`,
`started_at=NULL`, `heartbeat_at=NULL`, `status='queued'`,
`preview_status='waiting'`. Also clear `preview_message` and
`preview_error` so the preview worker's next claim doesn't carry forward
the importer's diagnostic text. Set a top-level `message` recording the
requeue reason ("candidate evidence missing"|"stale snapshot"|"incomplete")
on the import_job row itself. Leave `preview_attempts`/`attempts` alone
so existing counters remain historically accurate.

**Error path for the requeue UPDATE itself.** If the requeue SQL raises
inside the locked region (DB transient, connection drop), dispatch must
catch the exception and return
`DispatchOutcome(success=False, code=DISPATCH_CODE_REQUEUE_FAILED, message=...)`
rather than letting it bubble. The importer interprets that code as
"recoverable — leave job in `running`, do not write terminal failure";
the importer's startup-recovery path (`requeue_running_import_jobs`)
will pick up the stuck row on the next worker boot. The same outcome
code is used; only the message text changes.

The same requeue path covers both force/manual (originating in
`scripts/importer.py:187`) and automation (originating in
`lib/download.py::_process_beets_validation` for paths that the
2026-05-14 work already routed through `ensure_candidate_evidence_for_action`).

**Execution note:** test-first. The "missing evidence → requeue, no
measurement called, status flipped" test runs first; today's branch
hard-fails, so the test is RED until the requeue method exists.

**Patterns to follow:**
- `requeue_running_import_jobs` at `lib/pipeline_db.py:1234-1261` for the
  UPDATE shape.
- Existing `DispatchOutcome` `code` field and `DISPATCH_CODE_*` constants
  in `lib/import_dispatch.py` for outcome plumbing.
- `FakePipelineDB` builder pattern in `tests/fakes.py` — record requeue
  call args plus mutate the in-memory row.

**Test scenarios:**
- **Covers AE2.** Force-import claim with missing `import_job_candidate`
  evidence → dispatch returns the requeue outcome, no measurement helper
  called, job row is now `status='queued'`, `preview_status='waiting'`,
  `worker_id IS NULL`.
- **Covers AE3.** Force-import claim with evidence row whose snapshot
  mismatches the candidate folder → same requeue behavior.
- Automation path with missing evidence (preview job already enqueued in
  current flow) → same requeue behavior, no `_process_beets_validation`
  preimport gates fire.
- Importer interprets requeue outcome correctly: does not write `failed`
  to the job, does not increment retry counters, log line includes the
  reason from the `DispatchOutcome.message`.
- Slice: enqueue → importer claims → no evidence → requeued → preview
  claims (now that U1 front-gate exists) → measures → marks ready →
  importer claims and proceeds. Assert the full state arc.
- Negative: with valid evidence present, importer does NOT requeue — it
  proceeds to mutation. Guards against accidental over-eager requeue.

**Verification:** the slice above runs end-to-end against the real
PipelineDB; the in-memory FakePipelineDB self-tests catch parity drift
between the two implementations.

---

### U3. Delete the `preview.enable` toggle

**Goal:** Make preview mandatory. Remove the option, the env var, the
conditional branches in `enqueue_import_job`, the legacy-path helper in
the importer, and the test branches that exercise the disabled mode.

**Requirements:** R1, R2. Covers AE1 (grep finds no remaining toggle
references).

**Dependencies:** U2 (the requeue path must exist before the disabled
mode goes away, otherwise newly-enqueued jobs without evidence — which
this unit makes the only possible enqueue shape for legacy rows — would
hit the hard-fail). Land U2 first.

**Files:**
- Modify: `nix/module.nix`:
  - delete the `preview.enable` option definition (lines 345-353)
  - delete the `importPreviewEnableEnv` let-binding (line 37)
  - delete all five `CRATEDIGGER_IMPORT_PREVIEW_ENABLE` exports in the
    service script wrappers (lines 43, 60, 75, 83, 92)
  - make `systemd.services.cratedigger-import-preview-worker` (line 935)
    conditional only on `cfg.importer.enable`
  - **add `restartIfChanged = false;` to `systemd.services.cratedigger-importer`
    (around line 921)** so deploys don't kill in-flight beets mutations
    mid-import. Existing `requeue_running_import_jobs` recovers on
    worker boot, but avoiding the kill in the first place is cheaper
    and matches the always-running long-lived-worker pattern
- Modify: `lib/import_queue.py` — remove `IMPORT_JOB_PREVIEW_ENABLED_ENV`,
  `import_preview_enabled_from_env`, `IMPORT_JOB_PREVIEW_DISABLED_MESSAGE`.
- Modify: `lib/pipeline_db.py`:
  - remove the `preview_enabled` parameter from `enqueue_import_job`
    (around line 922) and inline the preview-enabled write shape
  - remove references to `IMPORT_JOB_PREVIEW_DISABLED_MESSAGE` at lines
    1128 and 1301
  - remove `exclude_preview_disabled_automation` parameter from
    `claim_next_import_job` (lines 1097, 1126) and inline the
    preview-enabled WHERE clause shape
  - delete the `requeue_disabled_automation_preview_jobs` method
    (line 1263) entirely — its only caller goes away in this unit
- Modify: `scripts/import_preview_worker.py`:
  - remove the `import_preview_enabled_from_env` import
  - delete the disabled-automation requeue block calling
    `db.requeue_disabled_automation_preview_jobs(...)` (line 340) and
    the surrounding env-check (line 339)
- Modify: `scripts/importer.py`:
  - delete `_job_uses_preview_disabled_legacy_path` (lines 61-87)
  - inline `import_job_id=job.id` / `download_log_id=int(download_log_id)`
    at lines 203-208 without the legacy-path guard
  - remove the similar conditional at line 270
  - remove the `exclude_preview_disabled_automation=import_preview_enabled_from_env()`
    argument at line 344
  - remove the `IMPORT_JOB_PREVIEW_DISABLED_MESSAGE` import at line 26
- Modify: `tests/fakes.py`:
  - remove the `preview_enabled` parameter from the fake's
    `enqueue_import_job` (lines 752-810) and inline the preview-enabled
    shape
  - remove `exclude_preview_disabled_automation` parameter from the
    fake's `claim_next_import_job` (line 911) and similar branches
    around line 920
  - delete the fake's `requeue_disabled_automation_preview_jobs`
    method (line 1056) and its corresponding self-test in
    `tests/test_fakes.py`
- Modify: `tests/test_dispatch_from_db.py:290` and `tests/test_pipeline_db.py:518-543`
  and elsewhere — remove `preview_enabled=True` / `preview_enabled=False`
  test parameters and any tests asserting the disabled message; ones
  that exercised `preview_enabled=False` get deleted (if they only
  validated the disabled path) or rewritten to use the unified flow.
- Audit step: `grep -rn IMPORT_JOB_PREVIEW_DISABLED_MESSAGE` and
  `grep -rn _job_uses_preview_disabled_legacy_path` and
  `grep -rn exclude_preview_disabled_automation` and
  `grep -rn requeue_disabled_automation_preview_jobs` and
  `grep -rn import_preview_enabled_from_env` across the repo — each
  must return zero matches before the unit ships.
- Run the VM check after `nix/module.nix` changes:
  `nix build .#checks.x86_64-linux.moduleVm`.

**Approach:**

Order the deletions so the test suite is green at each step:
1. Tests that exercise `preview_enabled=False` — either delete (when the
   only coverage they provide is the dead branch) or replace with the
   `True`-equivalent (when they covered a behavior shared by both modes).
2. Test fakes lose the parameter.
3. `lib/pipeline_db.py::enqueue_import_job` drops the parameter.
4. Importer helper and call sites lose the branch.
5. `lib/import_queue.py` constants go.
6. Nix module loses option, env exports, and `mkIf` guard.
7. VM check confirms the module still evaluates.

**Patterns to follow:**
- The existing always-on systemd services in `nix/module.nix` (e.g.,
  `cratedigger-importer`) show the right `mkIf` shape post-delete.

**Test scenarios:**
- **Covers AE1.** After this unit lands, `grep -r CRATEDIGGER_IMPORT_PREVIEW_ENABLE`
  `import_preview_enabled_from_env` `services.cratedigger.importer.preview.enable`
  `IMPORT_JOB_PREVIEW_DISABLED_MESSAGE` `preview_enabled=` across `nix/`,
  `lib/`, `scripts/`, `tests/` returns no matches.
- `nix build .#checks.x86_64-linux.moduleVm` passes against the modified
  module.
- Full Python suite passes after the cascading test deletions.
- Pyright clean on all touched files.

**Verification:** the grep + VM check + full suite together prove the
toggle is gone and the codebase compiles/tests without it. No new
positive tests are needed — the value here is subtraction.

---

### U4. Delete the unreachable legacy dispatch branch

**Goal:** Remove the `run_preimport_gates` direct-measurement branch in
`_dispatch_import_from_db_locked` that fires when neither `import_job_id`
nor `download_log_id` is supplied. After U3, no production caller hits it.

**Requirements:** R6. Covers AE5 (no caller of dispatch lands in the legacy
branch; the branch is deleted).

**Dependencies:** U3 (must land first so the importer always supplies
`import_job_id`).

**Files:**
- Modify: `lib/import_dispatch.py`:
  - delete the branch from `_dispatch_import_from_db_locked` after the
    existing `import_job_id is not None or download_log_id is not None`
    guard (around line 1899 through end of `inspect_local_files` /
    `run_preimport_gates` / downstream branching). Convert the guard
    into a precondition that returns
    `DispatchOutcome(success=False, message="import_job_id or download_log_id required", code=DISPATCH_CODE_BAD_REQUEST)`.
  - **Audit and delete the second hard-fail site at
    `lib/import_dispatch.py:1249`** inside `dispatch_import_core`
    (the `evidence_gate.candidate is None` branch that returns
    `DISPATCH_CODE_CANDIDATE_EVIDENCE_UNAVAILABLE`). After U2's requeue
    lands in the outer call sites (`_dispatch_import_from_db_locked` at
    line 1869 and `lib/download.py:1184`), the inner site is unreachable
    from production (it triggers only if a caller bypassed the outer
    requeue gate). Replace it with the same requeue call introduced in
    U2, or convert it to a defensive `assert` if the implementer
    confirms no remaining caller can hit it. Do not leave a dormant
    hard-fail that contradicts U2's invariant.
- Modify: `tests/test_dispatch_from_db.py` — delete or rewrite tests
  that previously exercised the legacy branch. Add one test that
  asserts the new precondition error path returns the right outcome
  shape; add one test that asserts the inner site (line 1249 today)
  is unreachable from production callers.
- Audit (no modification expected): all production callers of
  `dispatch_import_from_db`. Per the Q3 audit, the only caller is
  `scripts/importer.py:187`, and after U3 that caller always supplies
  `import_job_id`. Re-run `grep -rn "dispatch_import_from_db\|dispatch_import_core"`
  at U4 implementation time (not just at planning time) to catch any
  new caller a parallel branch may have added.

**Approach:**

Straight deletion of the branch body, plus a small precondition error
return at the top of `_dispatch_import_from_db_locked`. Keep the function
signature stable (both parameters remain optional in Python) so callers
that legitimately omit `download_log_id` for automation still work.

**Patterns to follow:**
- Existing precondition checks at the top of
  `_dispatch_import_from_db_locked` (e.g., `if not os.path.isdir(failed_path)`)
  show the right shape for an early `DispatchOutcome` return.

**Test scenarios:**
- **Covers AE5.** Test that calls `dispatch_import_from_db` with neither
  `import_job_id` nor `download_log_id` (a developer-error case) gets back
  a `DispatchOutcome` with the precondition error code; no measurement
  helper is invoked.
- All other existing dispatch tests continue to pass — none of them are
  expected to depend on the deleted branch.

**Verification:** pyright clean, full Python suite passes, grep for
`inspect_local_files` and `run_preimport_gates` inside `lib/import_dispatch.py`
returns no matches (those helpers may still be used elsewhere — that's
fine; they should just not be called from dispatch).

---

## Scope Boundaries

### Deferred to Follow-Up Work

- Surfacing preview-worker provenance in the web UI (the new
  `candidate_status='reused'` marker is persisted but not displayed).
- A proactive backfill pass over the ~700 wrong-match rows. They will
  flow through U1+U2 on demand when an operator force-imports them; if
  the on-demand cycle proves too slow in practice, batch backfill is
  a follow-up.

### Outside this plan's scope

- Wrong Matches cleanup / triage. Already routes through
  `decide_wrong_match_cleanup`, which has its own evidence acquisition.
- Content-hash or audio-fingerprint upgrades to the snapshot guard.
- Cross-request or cross-release candidate-evidence reuse.
- Changes to the evidence-authorized harness mutation mode
  (`--quality-evidence-action-file`). This plan governs how candidate
  evidence reaches the mutation boundary, not what happens inside the
  mutation.
- New operator capabilities. CLI ⇄ API symmetry does not apply here
  because no new operator action is being added — the toggle deletion
  removes an operator option, but force-import / manual-import surfaces
  remain unchanged.
- **Automatic cap on requeue oscillation.** If an operator keeps modifying
  candidate files between preview measurement and importer claim, the
  cycle preview-measure → importer-requeue → preview-remeasure can
  repeat indefinitely. This is deliberately accepted: the cycle is
  operator-caused, operator-visible via `pipeline-cli query` against
  `preview_attempts`, and self-resolving once the operator stops touching
  the files. No retry counter, no automated cap. The user explicitly
  chose "no new state" for the requeue mechanic during planning.

---

## Key Technical Decisions

- **Dispatch performs the requeue itself, not the importer.** The
  advisory lock held during `_dispatch_import_from_db_locked` already
  covers the evidence check; performing the status flip inside the same
  locked region keeps the transition atomic. The importer interprets a
  new outcome code (`DISPATCH_CODE_REQUEUED_FOR_PREVIEW`) but does not
  call `requeue_*` directly. Alternative considered (importer interprets
  outcome and calls `db.requeue` itself) was rejected because it splits
  the lock boundary across two methods.
- **Requeue clears writer-side state, preserves attempt counters.**
  `worker_id`, `started_at`, `heartbeat_at` go to NULL (otherwise the
  preview claim WHERE clause would treat the row as held).
  `preview_attempts` and `attempts` are preserved (counters reflect
  history). `message` is set to the reason; preview's own claim clears
  it.
- **Front-gate at `process_claimed_preview_job`, not inside
  `execute_preview_job`.** The point of the gate is to skip
  `execute_preview_job` entirely when evidence is valid. Putting the gate
  inside `execute_preview_job` would still incur its setup cost.
- **Toggle deletion is a hard break, not deprecated-no-op.** The user
  confirmed this in dialogue. The downstream Nix wrapper update is a
  coordinated operational step (see Operational / Rollout Notes), not
  an option-kept-for-back-compat workaround in the module.
- **The legacy dispatch branch becomes a precondition error, not a
  silent fallback.** After deletion, calling `dispatch_import_from_db`
  with neither ID is a programming error; surfacing it as a
  `DispatchOutcome` error code (rather than letting Python raise) is
  consistent with the existing error-return idiom in dispatch.

---

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Downstream Nix wrapper (`~/nixosconfig/modules/nixos/services/cratedigger.nix` on doc1) still sets `services.cratedigger.importer.preview.enable = true;` after U3 lands → `nixos-rebuild switch` on doc2 fails with "option does not exist" | High (depends on wrapper state today) | Operational sequence: update doc1's wrapper to remove the assignment BEFORE flake-updating cratedigger and rebuilding doc2. See Operational / Rollout Notes. |
| The automation preview-input path is harder to resolve cheaply than force/manual (path comes from `_materialize_automation_preview_path` which itself may do work) | Medium | If a cheap path-derivation is not available, defer the front-gate optimization for automation jobs (force/manual gate still lands in U1); the requeue path in U2 remains correct for both. Capture the decision under Deferred to Implementation. |
| Deleting `preview_enabled=False` test branches loses coverage of a previously-validated mode | Low | The disabled mode is being deleted; coverage of it is not valuable. Re-verify that no test under the `=False` branch was actually validating shared behavior — if it was, port it to the `=True`-equivalent shape before deleting. |
| In-flight jobs across the deploy window (importer running a beets mutation when the new code lands) | Medium | Verified: `cratedigger-importer` at `nix/module.nix:915` does NOT set `restartIfChanged`, so it currently restarts on every `nixos-rebuild switch`. U3 adds `restartIfChanged = false;` to that service so the importer keeps running across deploys and picks up new code on the next worker boot. Belt-and-braces: `requeue_running_import_jobs` recovers any job that did get killed mid-mutation by resetting `running` rows back to `queued` on importer startup. (`cratedigger.service` already sets `restartIfChanged = false` per CLAUDE.md; this aligns the importer to the same pattern.) |
| Test-fakes drift from real PipelineDB on the new requeue method | Medium | Direct test of the real DB method in `test_pipeline_db.py` plus a `FakePipelineDB` self-test in `test_fakes.py` for the same method, mirroring the existing pattern. |

---

## Operational / Rollout Notes

The Nix module here exposes `services.cratedigger.importer.preview.enable`,
but the option is **set** in the downstream wrapper at
`~/nixosconfig/modules/nixos/services/cratedigger.nix` on doc1. Deleting
the option in U3 without first removing its assignment in the wrapper
will break `nixos-rebuild switch` on doc2 with "the option does not exist."

**Deploy sequence (matches CLAUDE.md `.claude/rules/deploy.md`):**

1. SSH to doc1 and edit `~/nixosconfig/modules/nixos/services/cratedigger.nix`:
   remove the line `services.cratedigger.importer.preview.enable = true;`
   (and any other reference to that option). Commit and push from doc1.
2. Run the Nix module VM check locally on this repo:
   `nix build .#checks.x86_64-linux.moduleVm` — must pass before the
   cratedigger PR merges.
3. Merge the cratedigger PR.
4. From doc1: `cd ~/nixosconfig && nix flake update cratedigger-src && git add flake.lock && git commit -m "cratedigger: ..." && git push`
5. From doc1: `ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'`
6. Verify: `ssh doc2 'systemctl status cratedigger-import-preview-worker'` —
   service should be `active` (always-on after this refactor); previously
   would have shown "inactive (dead)" when `preview.enable = false`.
7. Watch the importer log: `ssh doc2 'sudo journalctl -u cratedigger-importer -f'` —
   look for "requeued for preview" messages on legacy rows being
   force-imported.

**Migration considerations:** No schema migration is needed. Existing
`status='queued'` / `preview_status='evidence_ready'` rows continue to be
claimed by the importer. Rows that were enqueued in the disabled-preview
mode (which marked them `preview_status='evidence_ready'` immediately
without measurement) will be claimed by the importer post-deploy; the new
requeue path catches the absence of evidence and routes them through
preview. This is the expected behavior, not a migration step.

---

## Verification Strategy

After each unit, run focused tests in `nix-shell`:

- After U1: `nix-shell --run "python3 -m unittest tests.test_import_queue tests.test_integration_slices -v"`
- After U2: `nix-shell --run "python3 -m unittest tests.test_pipeline_db tests.test_fakes tests.test_dispatch_from_db tests.test_import_queue tests.test_integration_slices -v"`
- After U3: `nix-shell --run "bash scripts/run_tests.sh"` (full suite) +
  `nix build .#checks.x86_64-linux.moduleVm`
- After U4: full suite + manual grep that `inspect_local_files` and
  `run_preimport_gates` are no longer referenced from `lib/import_dispatch.py`

Pre-PR-merge: pyright on every touched file, full Python suite from
`/tmp/cratedigger-test-output.txt` shows OK, VM check passes.

Post-deploy verification on doc2 (live):

- `ssh doc2 'pipeline-cli query --json "SELECT id, status, preview_status, message FROM import_jobs WHERE status = '\''queued'\'' AND preview_status = '\''waiting'\'' AND message LIKE '\''%requeued%'\'' ORDER BY updated_at DESC LIMIT 5"'` — should
  show rows requeued by the importer when their evidence is missing.
- Force-import a known pre-migration-017 Wrong Matches row via the web UI;
  observe importer log showing requeue, preview log showing measurement,
  importer log showing successful claim and decision.

---

## Deferred to Implementation

- **Shape of `preview_result` payload on the reused-evidence branch.**
  The existing `mark_import_job_preview_importable` call (line 254-259)
  receives a dict built from a measured `ImportPreviewResult` (verdict,
  reason, candidate measurements). When U1's front-gate skips
  measurement, the payload must be synthesized — either build a
  minimal `ImportPreviewResult` from the loaded
  `AlbumQualityEvidence` (preserving the shape consumers downstream
  expect), or write a minimal dict literal carrying just
  `candidate_status='reused'` provenance plus the fields the web UI's
  recents tab and the decision-tree code consume. The implementer
  decides; whichever shape lands must be covered by the AE4 test so
  the contract is testable.
- **Exact name of the requeue outcome code.** Suggested
  `DISPATCH_CODE_REQUEUED_FOR_PREVIEW`, but the existing `DISPATCH_CODE_*`
  naming may suggest a tighter form (e.g., `DISPATCH_CODE_REQUEUE`).
  Implementer's choice; keep it consistent with the existing constants.
- **Whether U3 should retain `IMPORT_JOB_PREVIEW_DISABLED_MESSAGE` rows
  in the importer's claim WHERE clause as a one-time read-only audit
  shape.** The audit found references at `lib/pipeline_db.py:1128, 1301`.
  Probably safe to delete outright, but if any production rows still
  carry that message text, leaving the read path tolerant for one cycle
  may be cheaper than backfilling messages. Check the DB:
  `pipeline-cli query --json "SELECT count(*) FROM import_jobs WHERE preview_message = 'Preview gate disabled'"`
  before deciding.
- **Final wording of the importer's "requeued for preview" log line.**
  Should include the dispatch reason (missing/stale/incomplete) and the
  job/request IDs. Match existing importer log shape.
- **Canonical entry point for the shared validity function.** Plan
  refers to `lib/quality_evidence.load_candidate_evidence_for_source`
  throughout. Brainstorm noted "planning to re-verify and decide
  whether it stays the canonical entry point or gets a thin wrapper."
  The function already exists with the right return shape, so the
  default is "use directly." Only extract a wrapper if call-site
  ergonomics force it during implementation.

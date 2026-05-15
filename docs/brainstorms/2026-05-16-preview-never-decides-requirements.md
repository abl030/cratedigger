---
date: 2026-05-16
topic: preview-never-decides
---

# Preview Never Decides — Symmetric Closure of the Evidence/Decision Boundary

## Summary

Make the preview worker emit only `evidence_ready` or `measurement_failed` —
never a verdict. Split the fused measure-and-decide function
`run_preimport_gates` into a pure measurement helper and a pure decision
function that lives next to the existing decision helpers in `lib/quality.py`,
and reclassify every remaining preview-side `verdict=confident_reject` /
`verdict=uncertain` exit as either a stored evidence fact or a measurement
failure. Both terminal states self-heal: the importer (or the worker on
measurement failure) finalizes the request to `wanted` so search resumes;
`preview_status='uncertain'` ceases to be a terminal state.

---

## Problem Frame

The 2026-05-14 evidence/decision boundary made preview the sole producer of
candidate evidence and the importer the sole authority on decisions. The
2026-05-15 importer-never-measures refactor enforced the importer half of
that contract — explicit R3 ("the importer does not invoke any candidate
measurement helper — spectral analysis, V0 probing, candidate bitrate
probing, `run_preimport_gates`, or equivalents") with helpers named.

The preview half was specified only implicitly. R5 said "measure, persist
evidence, mark the job ready" — terse, no enumeration of decision-emitting
helpers preview must stop calling, no ban on `verdict=confident_reject`.
The implementation honored the literal text. `lib/import_preview.py:366-393`
still calls `run_preimport_gates`, which is itself a fused measure-and-decide
function (sets `valid=False` + `scenario` for `audio_corrupt`,
`bad_audio_hash`, `spectral_reject`). When the gates fail, preview translates
the decision into `verdict="confident_reject"` and leaves the job at
`preview_status='uncertain'`, which the importer ignores. Beyond
`run_preimport_gates` the preview path has roughly a dozen other
`verdict="uncertain"` and `verdict="confident_reject"` exit branches
(`nested_layout`, `evidence_empty_fileset`, `evidence_invalid_snapshot`,
materialization errors) — each one is preview making a keep/reject/punt
decision under a different name.

The observable failure today: 315 import jobs sit in
`preview_status='uncertain'`, every parent `album_request` is locked in
`status='downloading'`, and the poll loop refuses to resume search because
an "active" import job exists for the request. Issue #250 is the stuck-
forever symptom; issue #251 is a false-positive policy bug inside
`validate_audio` that wouldn't be a deadlock if the architecture were
symmetric. Issue #252 names the cause: the preview worker is shadow-deciding
under the rules the 5/15 refactor was supposed to ban.

---

## Actors

- A1. Operator: Triggers force-import on Wrong Matches rows and adds
  album requests via the web UI; expects the pipeline to drive every
  request to a terminal state (`imported`, `manual`, or back to `wanted`
  for re-search) without manual triage.
- A2. Preview worker (`cratedigger-import-preview-worker`): Claims
  `queued` jobs, runs the measurement helpers, persists evidence, marks
  the job either `evidence_ready` (importer takes over) or
  `measurement_failed` (self-healing terminal state).
- A3. Importer worker (`cratedigger-importer`): Claims `evidence_ready`
  jobs, reads evidence and configuration, calls the pure decision
  functions in `lib/quality.py`, and is the sole emitter of accept and
  reject verdicts on candidate content.

---

## Key Flows

- F1. Healthy candidate flows through preview without a verdict
  - **Trigger:** Automation or force-import enqueues an `import_job`;
    the preview worker claims it.
  - **Actors:** A2, A3
  - **Steps:** Preview consults the shared validity function; on miss it
    runs the measurement helpers (audio decode probe, bad-audio-hash
    lookup, spectral analysis, V0 probe, folder inspection), persists
    the facts to the candidate evidence row, and marks the job
    `evidence_ready`. Importer claims, reads evidence, calls the pure
    decision functions, mutates beets.
  - **Outcome:** Preview emits no verdict. The importer alone decides
    accept or reject.
  - **Covered by:** R1, R2, R3, R5, R6

- F2. Suspect candidate is rejected by the importer, not by preview
  - **Trigger:** Same as F1, but the measurement facts (audio_corrupt,
    spectral grade likely-transcode without improvement, etc.) indicate
    the candidate fails policy.
  - **Actors:** A2, A3
  - **Steps:** Preview persists the facts and marks the job
    `evidence_ready` exactly as in F1 — no verdict, no `confident_reject`.
    Importer reads evidence, calls the decision function, decides reject,
    denylists the source user (where the existing rule applies), writes
    a `download_log` row, marks the job `failed`, and finalizes the
    request to `wanted`. The poll loop sees the request back in `wanted`
    and resumes search.
  - **Outcome:** A bad candidate from one user does not lock the request
    — search re-runs against non-denylisted sources. #250 closes
    architecturally.
  - **Covered by:** R1, R3, R6, R8

- F3. Measurement itself fails — preview cannot produce evidence
  - **Trigger:** Preview claims a job; an unrecoverable measurement
    error occurs (files vanished mid-claim, ffmpeg crashed, snapshot
    folder moved, audio inventory is empty so no measurement is
    possible).
  - **Actors:** A2
  - **Steps:** Preview does not emit a verdict. It marks the job
    `measurement_failed` with a typed reason, writes a `download_log`
    row scoped to the originating source path, denylists the source
    user where the existing per-user 5-strikes rule applies, and
    finalizes the request to `wanted` so search resumes.
  - **Outcome:** The same self-healing shape as F2. Search resumes.
    Operator does not need to triage the row.
  - **Covered by:** R7, R8

- F4. The 315 currently-stuck rows are recovered on deploy
  - **Trigger:** Deploy of this refactor.
  - **Actors:** A2
  - **Steps:** A one-time recovery sweep clears every job currently in
    `preview_status='uncertain'` and routes it through the new path
    (either by flipping it to a state the preview worker re-claims, or
    by directly finalizing the originating request to `wanted` when
    the row's existing evidence is already sufficient for the importer
    to decide). No operator-level intervention is required.
  - **Outcome:** After deploy, no `preview_status='uncertain'` rows
    remain anywhere in the system, and the parent requests of those
    315 rows are either `imported`, back in `wanted`, or otherwise in
    a state the poll loop can advance.
  - **Covered by:** R9

---

## Requirements

**Preview-never-decides contract**

- R1. The preview worker's terminal outputs on a claimed `import_job`
  are exactly two: `evidence_ready` (measurement complete, candidate
  evidence persisted) and `measurement_failed` (measurement could not
  be completed). No third outcome — in particular no
  `verdict='confident_reject'`, no `verdict='uncertain'`, no
  `preview_status='uncertain'` left dangling.
- R2. `lib/preimport.py::run_preimport_gates` is split. A pure
  measurement helper produces a typed result containing only facts
  (audio-decode outcome per file, bad-audio-hash match if any,
  candidate spectral measurement, V0 probe, folder inspection
  including layout shape and audio file count). A pure decision
  function — placed alongside the other pure decision functions in
  `lib/quality.py` — consumes that measurement plus configuration
  plus the existing-album evidence and returns the keep/reject
  decision. Neither half writes a verdict, denylists a user, mutates a
  job, or invokes the other.
- R3. The importer worker is the sole caller of the new pure decision
  function. The preview worker imports and calls only the measurement
  helper. Pyright and grep cannot find a call from the preview-worker
  process to the decision function after the refactor.

**Reclassification of the surviving preview verdict branches**

- R4. Folder-shape facts previously emitted as preview verdicts become
  evidence facts on the candidate row: the layout shape (e.g.,
  nested-audio vs flat), the audio file count (zero is a fact, not a
  verdict), the audio inventory's filetype band. The pure decision
  function decides what to do with each.
- R5. Preview branches that today emit `verdict='uncertain'` because
  the candidate source is unmeasurable — snapshot mismatch after
  retry, materialization failure, files-vanished — are reclassified
  as `measurement_failed`. None of them remain as a `verdict`.

**Self-healing terminal states**

- R6. When the importer rejects a candidate on the basis of evidence,
  it writes the same shape of `download_log` row the legacy preview-side
  reject path used to write (scoped to the source path, with the
  decision scenario and detail), denylists the source user where the
  existing rule applies, marks the import job `status='failed'`, and
  finalizes the parent `album_request` to `status='wanted'` so the
  poll loop resumes search on the next tick.
- R7. When preview emits `measurement_failed`, it finalizes the same
  way: `download_log` row, denylist where applicable under the existing
  per-user rule, `import_job.status='failed'`, request back to
  `wanted`. Preview must not leave a request locked in `downloading`
  after a `measurement_failed` outcome.
- R8. The `preview_status='uncertain'` terminal state is deleted as a
  state any code path can write. Migrations may keep the column for
  historical rows but no production write produces a new value of
  `uncertain` after the refactor.

**Recovery of in-flight rows**

- R9. The deploy that lands this refactor must recover the rows that
  are stuck under the old contract. After deploy, zero
  `import_job` rows remain in `preview_status='uncertain'`. The
  parent requests of those rows are either driven through the new
  path (preview re-measures, importer decides) or finalized to
  `wanted` if the importer can decide on existing evidence alone.
  Operator-level manual triage is not required.

---

## Acceptance Examples

- AE1. **Covers R1, R2, R3.** Given the cratedigger codebase after the
  refactor, when grepping the preview-worker process's imports and call
  graph (`scripts/import_preview_worker.py`, `lib/import_preview.py`)
  for the new pure decision function and for the old
  `run_preimport_gates` symbol, no matches remain. When grepping the
  same files for `verdict=` and `preview_status='uncertain'`, no
  production-write matches remain.
- AE2. **Covers R1, R3, R6.** Given an `import_job` whose candidate
  folder contains MP3 files that ffmpeg refuses to decode (real
  `rc!=0`, not metadata stderr), when the preview worker claims it,
  the worker persists the audio-decode facts on the candidate evidence
  row and marks the job `evidence_ready`. The importer claims the
  row, calls the pure decision function, decides reject on
  audio-decode failure, writes a `download_log` row scoped to the
  source, denylists the source user under the existing rule, marks the
  job `failed`, and finalizes the parent request to `wanted`. The
  next poll tick re-runs search.
- AE3. **Covers R1, R4.** Given an `import_job` whose candidate folder
  has audio nested in a subdirectory, when the preview worker claims
  it, the worker persists `folder_layout='nested'` (or equivalent
  fact) on the evidence row and marks the job `evidence_ready`. The
  importer claims, reads the layout fact, decides reject, and the
  same self-healing finalization as AE2 fires. Preview emits no
  verdict.
- AE4. **Covers R1, R4.** Given an `import_job` whose candidate folder
  contains zero audio files, when the preview worker claims it, the
  worker persists `audio_file_count=0` on the evidence row and marks
  the job `evidence_ready`. The importer claims, decides reject on
  empty-inventory, and the same finalization fires. Preview emits no
  verdict and no `uncertain`.
- AE5. **Covers R1, R5, R7.** Given an `import_job` whose candidate
  folder no longer matches the snapshot recorded at claim time
  (folder moved, files renamed under it), when the preview worker
  detects the mismatch after its existing retry, the worker marks the
  job `measurement_failed`, writes a `download_log` row scoped to the
  originating source path, denylists the source user where the
  existing rule applies, marks the job `failed`, and finalizes the
  parent request to `wanted`. Preview emits no `verdict='uncertain'`.
- AE6. **Covers R7.** Given an `import_job` whose source folder
  vanishes mid-measurement (ffmpeg returns ENOENT), when the preview
  worker catches the error, the same `measurement_failed`
  finalization as AE5 fires. The parent request reaches `wanted` on
  the same tick.
- AE7. **Covers R8.** Given the deployed cratedigger system after
  this refactor, when querying `SELECT COUNT(*) FROM import_jobs
  WHERE preview_status='uncertain' AND created_at > <deploy_time>`,
  the result is zero indefinitely.
- AE8. **Covers R9.** Given the deploy that lands this refactor, when
  the recovery sweep completes, the live count of
  `preview_status='uncertain'` rows is zero, and the parent requests
  of the previously-stuck 315 rows are each in a state the poll loop
  can advance (`imported`, `wanted`, `manual`) — none remain locked
  in `downloading` with an inactive import job.

---

## Success Criteria

- Issues #250, #251, and #252 close on the same merge. #250 is closed
  by construction (no preview-side verdict can lock a request). #251
  is closed via its own one-line fix to `validate_audio`, coordinated
  in the same change so the recovery sweep does not re-stick the 90
  audio_corrupt rows. #252 is closed by the architectural symmetry.
- The phrase "preview never decides" is enforceable by grep against
  the preview-worker process: no call to the pure decision function,
  no `verdict=` write, no `preview_status='uncertain'` write.
- The pipeline self-heals end-to-end. From the operator's perspective,
  every wanted album either ends up `imported`, is permanently held in
  `manual` by an explicit decision tree branch, or returns to
  `wanted` for re-search. No album can become silently undriven.
- A downstream agent picking up `ce-plan` can describe the data flow
  as: enqueue → preview (measurement only, two outcomes) → importer
  (decision only, three outcomes: accept / reject / requeue-for-
  preview), with one shared validity function and a small number of
  pure decision functions in `lib/quality.py` — without inventing
  preview-side verdicts, alternate uncertain states, or operator-
  triage paths.

---

## Scope Boundaries

- The importer-side dispatch path, the `load_candidate_evidence_for_source`
  snapshot guard, the requeue-to-preview mechanism, and the
  Wrong Matches cleanup path are settled in the 5/14 and 5/15 docs
  and inherited unchanged. This brainstorm does not retouch them.
- Concrete persistence design for the new evidence facts (column
  names, exact migration number and DDL, whether the facts live on
  `AlbumQualityEvidence` directly or on an auxiliary table) is
  planning territory. The requirements name which facts must be
  durable; the schema shape is for `ce-plan`.
- The exact function names and module placement of the split halves
  of `run_preimport_gates` are planning territory. The contract is:
  one pure measurement helper, one pure decision function in
  `lib/quality.py`, neither one writing to the DB.
- Issue #251's `validate_audio` stderr-policy fix is coordinated as
  a dependency, not duplicated here. The fix itself (drop the
  `or stderr` clause and trust ffmpeg's exit code, or expand the
  ignore list) is decided in #251's own PR — this doc only requires
  that the fix lands before or with this refactor's recovery sweep
  so the 90 stuck audio_corrupt rows do not reappear after re-measurement.
- Content-hash or audio-fingerprint upgrades to the snapshot guard,
  cross-request candidate-evidence reuse, and changes to the per-user
  5-strikes cooldown rule are all out of scope. The existing safety
  rails are inherited.
- Behavior of the manual-import path (operator-driven, not via
  `cratedigger-importer`) is out of scope where it does not enqueue
  an `import_job`. Where it does enqueue (force-import via web UI or
  CLI), the same contract applies uniformly.

---

## Key Decisions

- **Wide over surgical-to-`run_preimport_gates`.** The architectural
  smell has the same shape in `nested_layout`, `evidence_empty_fileset`,
  `evidence_invalid_snapshot`, and the materialization-error branches.
  A symmetric `preview-never-decides` contract closes the bug class.
  Narrow would unstick the 315 live rows but leave the same shape of
  bug in the other branches, where future incidents can hide.
- **`measurement_failed` self-heals.** Finalizing to `wanted` on
  measurement failure relies on the existing 5-strikes per-user
  cooldown to bound denylist loops. Operator-gated would reintroduce
  the exact failure mode #250 was about. The cooldown rule has
  shipped and been operational long enough to trust as the safety
  rail.
- **Decision function lives in `lib/quality.py` next to the other
  pure decision helpers.** Co-locating with `spectral_import_decision`,
  `import_quality_decision`, `transcode_detection`, `quality_gate_decision`,
  `dispatch_action`, etc. preserves the project's existing pattern:
  pure decisions are testable as subTest tables, no DB or filesystem
  side effects, no logging.
- **Recovery sweep is part of the deploy, not a follow-up.** Shipping
  the architecture without recovering the 315 stuck rows leaves
  operator-visible damage on the system. Recovery on deploy keeps the
  PR's promise observable.

---

## Dependencies / Assumptions

- The 5/14 candidate evidence row (`AlbumQualityEvidence`) and its
  snapshot guard (`load_candidate_evidence_for_source`) are the
  persistence vehicle for the new facts. Planning is expected to
  verify whether the existing columns cover audio-decode results,
  per-file corruption lists, bad-audio-hash matches, folder layout,
  and audio file count — and to specify any new columns or auxiliary
  rows where they do not.
- The 5/15 requeue-on-missing-evidence mechanism
  (`DISPATCH_CODE_REQUEUED_FOR_PREVIEW`) is the recovery path for
  rows whose evidence is incomplete after this refactor lands; no
  new requeue mechanism is required.
- The existing per-user 5-strikes denylist cooldown
  (`lib/cooldowns.py`) is the safety rail against denylist loops on
  `measurement_failed`. No new cooldown shape is introduced.
- Issue #251 lands coordinated with this work — same PR or a
  dependent PR merged in the same deploy. Without it, the 90
  `audio_corrupt`-stuck rows will be re-measured under the buggy
  helper and re-rejected on the recovery sweep.
- The poll loop (`lib/download.py::poll_active_downloads`) already
  resumes search for requests in `status='wanted'` whose
  `active_download_state` reflects a closed download. No new poll
  behavior is required.

---

## Outstanding Questions

### Deferred to Planning

- [Affects R2][Technical] Names and module placement for the split
  halves of `run_preimport_gates`. Candidates: `measure_preimport_state`
  + `preimport_decide`; or fold the decision into the existing
  `spectral_import_decision` / `import_quality_decision` family if the
  decision surface composes naturally.
- [Affects R4][Technical] Persistence shape for `audio_corrupt` /
  `corrupt_files` / `bad_audio_hash_id` / `folder_layout` /
  `audio_file_count`. Whether these extend `AlbumQualityEvidence` or
  live on an auxiliary table. Planning to verify what already exists
  on the row before specifying a migration.
- [Affects R5, R7][Technical] Exact typed shape of the
  `measurement_failed` reason: free-text, an enum, or a typed
  msgspec.Struct mirroring the existing `download_log` outcome
  taxonomy. The contract requires it to be machine-readable in the
  recents UI; the precise shape is planning territory.
- [Affects R9][Technical] Recovery sweep shape: a one-time SQL
  migration that re-routes stuck rows by flipping
  `preview_status='uncertain'` → a state the preview worker re-claims,
  versus a one-time Python script invoked by
  `cratedigger-db-migrate.service`. Planning to choose whichever is
  cleaner to reason about under the existing migrator.
- [Affects R6, R7][Technical] Audit every code path that finalizes an
  `album_request` to `wanted` after a download_log write
  (existing import-reject paths, the new importer-reject path, the
  new `measurement_failed` path) and confirm they share a single
  helper. The contract says "self-healing"; a single helper makes
  that enforceable.

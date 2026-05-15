---
date: 2026-05-14
topic: quality-evidence-decision-boundary
---

# Quality Evidence / Decision Boundary Refactor

## Summary

Refactor Cratedigger's quality pipeline around one neutral album-quality
evidence model and ephemeral action decisions. Expensive candidate
measurements are durable work product: once async preview has measured a
candidate and its fileset snapshot still matches, mutating import must reuse
that candidate evidence and must not rerun candidate spectral/V0/bitrate
measurement. Import, triage cleanup, simulator, and action-facing UI decisions
must always be freshly derived from current evidence, candidate evidence, and
policy.

---

## Problem Frame

The quality policy has accumulated enough edge-case work that the test suite is
the main reason operators can trust it: provisional lossless-source grind-up,
transcode-vs-genuine protection, codec-aware ranks, verified-lossless target
handling, and quality-gate behavior all encode real incident learnings.

The failure on British Sea Power's *Do You Like Rock Music?* exposed a boundary
bug rather than a policy bug. A preview-time `ImportResult` mixed reusable
candidate measurements with an import decision. That decision was later reused
by the mutating importer after the current Beets/request evidence had changed,
allowing a stale "would import" verdict to replace stronger current material.

The same modeling problem appears in older evidence names: V0 probe "kinds"
such as `lossless_source_v0`, `native_lossy_research_v0`, and
`on_disk_research_v0` encode policy interpretation into what should be a
neutral metric. The album has quality evidence; policy decides which evidence
matters in a given comparison.

---

## Actors

- A1. Operator: Reviews Wrong Matches, import history, and simulator output,
  and needs decisions to be explainable without risking the library.
- A2. Async preview worker: Computes expensive candidate evidence ahead of the
  serial beets mutation lane so import can reuse those measurements when the
  candidate fileset is unchanged.
- A3. Import worker: Owns beets mutation and must compute authoritative import
  decisions from fresh current evidence plus snapshot-valid candidate
  evidence, without remeasuring unchanged candidates.
- A4. Wrong Matches triage: Uses candidate/current evidence to decide whether
  a rejected folder can be safely cleared.
- A5. Quality simulator: Protects quality policy behavior and explains how a
  current album would react to common candidate scenarios.
- A6. On-demand backfill/repair paths: Fill missing current evidence for
  historical Beets albums when an active import/cleanup/evaluation path needs
  it, without inventing proof the files cannot support.

---

## Key Flows

- F1. Candidate evidence preview
  - **Trigger:** A download/import job reaches async preview or Wrong Matches
    triage asks for evidence on a candidate folder.
  - **Actors:** A2, A4
  - **Steps:** Measure the candidate album fileset, store neutral reusable
    album-quality evidence for that candidate owner, optionally store a
    preview/triage verdict for UI/audit, and record provenance.
  - **Outcome:** Expensive candidate measurements are available for later
    decision recomputation without carrying decision authority. If the cheap
    candidate fileset snapshot still matches, later action paths reuse these
    measurements rather than re-running candidate spectral/V0/bitrate work.
  - **Covered by:** R1, R2, R3, R4, R11, R12, R14, R22, R24

- F2. Mutating import decision
  - **Trigger:** The serial import worker is ready to import a candidate.
  - **Actors:** A3, A6
  - **Steps:** Load candidate evidence for the candidate owner; verify its cheap
    fileset snapshot; reuse it when the snapshot matches; recompute candidate
    evidence only when it is missing, stale, malformed, or incomplete; load
    fresh current request/Beets evidence; backfill missing current evidence
    when possible; run the quality decision pipeline; mutate beets only if the
    freshly computed decision allows it; after successful import, measure final
    Beets files for current evidence and carry forward only source-proof
    classifications that remain valid after conversion.
  - **Outcome:** Import authority comes only from current evidence, candidate
    evidence, current policy, and import context at mutation time.
  - **Covered by:** R5, R6, R7, R8, R9, R10, R15, R16, R17, R18, R20, R21,
    R24

- F3. Wrong Matches cleanup
  - **Trigger:** Wrong Matches cleanup or triage considers deleting/clearing a
    rejected candidate.
  - **Actors:** A1, A4, A6
  - **Steps:** Recompute the cleanup decision from stored candidate evidence,
    fresh current evidence, and policy; backfill current evidence if needed;
    refuse cleanup as uncertain when required evidence cannot be obtained.
  - **Outcome:** Cleanup never reuses an old preview/triage verdict as
    authority.
  - **Covered by:** R6, R10, R11, R12, R14, R22, R23, R24

- F4. Simulator parity guard
  - **Trigger:** Planning or implementation changes any active evidence or
    quality decision path.
  - **Actors:** A5
  - **Steps:** Preserve existing simulator scenario behavior, add
    incident-shaped regressions before refactor work, and verify simulator,
    preview, import, and UI explanations route through the same evidence and
    decision model.
  - **Outcome:** The refactor preserves the quality policy that existing tests
    already protect.
  - **Covered by:** R25, R26, R27

---

## Requirements

**Evidence Model**

- R1. Cratedigger must introduce one active, neutral `AlbumQualityEvidence`
  concept for both downloaded candidates and current Beets/request albums. It
  must replace, extend, or wrap the existing `AudioQualityMeasurement` shape
  rather than creating a second active measurement abstraction.
- R2. Album-quality evidence must describe measured fileset facts: codec or
  container facts, bitrate min/avg/median, spectral summary, V0 probe
  min/avg/median, storage/target facts, verified-lossless classification, and
  measurement/source provenance.
- R3. Album-quality evidence must not contain reusable import authority:
  `decision`, `would_import`, stage chains, quality-gate outcomes, and cleanup
  verdicts are audit/UI outputs only.
- R4. Candidate evidence must be scoped to the original candidate owner that
  spans preview, import, and cleanup for that request/download row. It must not
  be reused across requests/releases, even if two owners point at the same
  folder.
- R5. Current evidence must describe the current Beets/request fileset. If an
  import converts or transcodes files, post-import current evidence must be
  measured from the final Beets files rather than copied wholesale from the
  source candidate. Candidate evidence validity and current evidence validity
  are independent; each evidence record is valid only for the fileset it
  describes.
- R6. Cached evidence is an optimization, but snapshot-valid candidate evidence
  is also the expected import-time input. If candidate or current evidence is
  missing, stale, malformed, or incomplete, the active path must recompute or
  backfill evidence rather than reusing an old decision. If required evidence
  cannot be recomputed or backfilled for a mutating/destructive action, the
  action must fail closed and record the fallback reason. The cheap snapshot
  guard must at least cover sorted relative paths, file count, sizes, mtimes at
  available precision, extensions/container facts, and a measurement timestamp
  taken after the fileset is quiescent.

**Relational Persistence**

- R7. New active reusable evidence needed for candidate/current comparisons and
  decision provenance must be stored in a normalized relational evidence store
  with typed columns, not as JSONB blobs.
- R8. The evidence store must support owner type/id ownership so one evidence
  model can attach to candidate owners and current Beets/request owners used by
  active import, cleanup, and backfill flows.
- R9. Active decision paths must stop writing and reading legacy active
  evidence columns such as `download_log.v0_probe_*`,
  `download_log.spectral_*`, and current lossless-source V0 probe fields as
  policy inputs. Historical scalar columns and blobs remain read-only audit/UI.
- R10. The active evidence store must keep album-level summary evidence
  relationally. Per-track spectral detail may remain audit/debug-only and does
  not need a child relational table in this pass.
- R11. Existing `ImportResult`, `preview_result`, `validation_result`, and
  download-history JSONB blobs remain decodable/renderable for historical audit,
  but new active import/quality code must not consume them as reusable evidence
  or authority. Active paths may read legacy payloads only for identity/location
  fields needed to find files; decision and quality fields such as
  `would_import`, cleanup eligibility, stage chains, or stored verdicts are not
  valid active inputs.

**V0 Probe Cleanup**

- R12. V0 probe evidence must be collapsed to one neutral V0 metric:
  min/avg/median for an album fileset encoded or inspected as V0.
- R13. Active code must remove policy-shaped V0 probe kinds such as
  `lossless_source_v0`, `native_lossy_research_v0`, and
  `on_disk_research_v0`.
- R14. Whether a V0 probe is comparable, audit-only, eligible for provisional
  lossless-source policy, or eligible for verified-lossless corroboration must
  be decided by policy from album/file facts and source-lineage provenance, not
  by a stored probe kind. Required provenance includes probe input/container
  facts, probe stage, whether the source was a supported lossless container, and
  whether the probe describes current storage or a pre-conversion source proof.

**Verified Lossless**

- R15. `verified_lossless` is a stored boolean classification on album-quality
  evidence, not an override flag and not a tri-state. `false` means no
  verified-lossless proof is attached; it does not mean lossy files disproved
  losslessness.
- R16. `verified_lossless` may be newly computed or changed only while acting on
  lossless-container files. Lossy files cannot compute or change it; lossy
  backfill must preserve any existing true proof and otherwise leave false as
  absence of proof.
- R17. A successful import from a verified lossless-container source may carry
  `verified_lossless=true` into current evidence even if final storage is lossy,
  because the proof was observed before conversion. This carry-forward applies
  only to the source-proof classification and its provenance, not to
  source-file codec, bitrate, spectral, or V0 facts.
- R18. Later lossy backfill/recompute must not change `verified_lossless` in
  either direction. Replacement or backfill from actual lossless-container
  files may compute it again. Verified-lossless provenance must record proof
  origin, source fileset identity, and classifier version/config sufficient to
  explain why the boolean is trusted.
- R19. Active code must not expose `verified_lossless_override` or equivalent
  bypass semantics. The classification can influence policy only through the
  normal quality decision pipeline.

**Decision Authority**

- R20. Mutating import must always compute its authoritative decision at import
  time from fresh current evidence, valid candidate evidence, policy/config, and
  import context. If an existing current album is present and required current
  or candidate evidence cannot be loaded, validated, recomputed, or backfilled,
  mutating import must not proceed.
- R21. Force import is not a separate quality path. It must only bypass
  Beets distance/match gating and must not bypass spectral checks, V0 evidence,
  verified-lossless policy, downgrade prevention, quality gate, evidence
  validation, or decision recomputation.
- R22. Preview and triage verdicts may remain persisted for UI/audit and
  non-mutating prioritization only. Stored verdicts must never authorize Beets
  mutation, force-import execution, file deletion, or cleanup outcomes,
  regardless of whether the action happens in the same run or later.
- R23. Wrong Matches cleanup must recompute its cleanup decision from evidence
  at cleanup time. If required evidence cannot be obtained, cleanup remains
  uncertain and leaves files visible. Every Wrong Matches delete/clear path,
  bulk cleanup path, converge unmatched cleanup, and failed force-import cleanup
  must route through this recomputed cleanup decision service.
- R24. Mutating import, preview/triage audit, cleanup, and backfill outputs must
  persist applicable decision provenance: reused vs recomputed candidate
  evidence, existing vs backfilled current evidence, snapshot guard result, and
  fallback reason. Fields that do not apply to a flow must be explicitly absent
  or marked not-applicable. Simulator output must return/render equivalent
  provenance for parity, but does not need durable provenance storage unless an
  existing simulator audit path requires it.

**Simulator and Policy Preservation**

- R25. Existing quality policy must not be retuned by this refactor. Codec rank,
  provisional lossless, transcode-vs-genuine, verified-lossless target, quality
  gate, and cleanup semantics must preserve current behavior unless a new
  explicit product decision changes them.
- R26. Simulator parity and end-to-end stale-authority regressions are both
  ship-blocking acceptance gates. Existing simulator scenario tests must remain
  green, new incident-shaped simulator tests must be added before refactoring
  active paths, and importer/cleanup path regressions must prove stored
  preview/triage verdicts cannot authorize mutation or deletion.
- R27. The simulator must construct and compare current/candidate
  `AlbumQualityEvidence` pairs rather than passing policy-shaped probe kinds or
  overloaded import-result blobs.

**Measurement Reuse and Mutation Boundary**

- R28. Candidate evidence reuse is mandatory when safe. If import or cleanup
  has snapshot-valid candidate evidence, it must reuse the stored candidate
  measurements and must not rerun candidate spectral analysis, V0 probe
  generation, bitrate measurement, verified-lossless source probing, or other
  expensive candidate measurement work. Only the action decision is recomputed.
- R29. "Recompute the decision" must never be used as shorthand for
  "remeasure the candidate." Candidate evidence recomputation is allowed only
  when evidence is missing, stale, malformed, incomplete, or explicitly
  invalidated by the fileset snapshot guard.
- R30. Async preview must produce the same typed candidate evidence artifact
  that mutating import consumes. The handoff may include audit verdicts for UI,
  but the import path must not rebuild active candidate evidence by decoding an
  `ImportResult` decision blob or by rerunning the preview harness against the
  unchanged source.
- R31. The beets mutation boundary must support a mutation-only path once
  candidate evidence is snapshot-valid. Any harness operation that mutates
  Beets must be separable from candidate measurement so unchanged candidates do
  not pay the preview CPU cost a second time.
- R32. Import and cleanup provenance must record whether candidate evidence was
  reused, recomputed, or unavailable. The test suite must include regressions
  that fail if snapshot-valid preview evidence causes import to invoke
  candidate spectral/V0/bitrate measurement again.

---

## Acceptance Examples

- AE1. **Covers R3, R6, R20, R21, R24, R26, R28, R29, R32.** Given preview measured a
  candidate and stored an audit verdict of would-import before current evidence
  existed, and a different import later creates stronger current V0/spectral
  evidence, when force import runs for the previewed candidate, it may reuse the
  candidate measurements, must not remeasure the unchanged candidate, must load
  fresh current evidence, must recompute the decision, and must reject if
  current evidence is better.
- AE2. **Covers R20, R21, R25.** Given a high-distance wrong match with poor
  quality evidence, when force import is used, the Beets distance gate is
  bypassed but the candidate still rejects through the normal quality pipeline.
- AE3. **Covers R15, R16, R17, R18, R19.** Given a lossless-container candidate
  proves verified lossless and is imported to a lossy target, when later lossy
  backfill runs on the current Beets files, current evidence must keep the
  final lossy files' codec/bitrate/spectral/V0 facts, backfill must not flip
  `verified_lossless` false, and only acting on a new lossless-container
  fileset can recompute the classification.
- AE4. **Covers R12, R13, R14, R25, R27.** Given an MP3 candidate and a FLAC
  candidate both produce V0 probe min/avg/median, when evidence is stored, both
  records use the same neutral V0 metric plus source-lineage provenance rather
  than policy-shaped probe kinds; policy decides which record's V0 evidence is
  actionable in the comparison.
- AE5. **Covers R7, R8, R9, R10, R11.** Given a new candidate is previewed
  after this refactor, when reusable evidence is stored, the active evidence
  lives in relational typed columns attached to the candidate owner; legacy
  scalar columns and JSONB result blobs may be stored/rendered for audit but are
  not consumed by import as quality evidence or decision authority.
- AE6. **Covers R22, R23, R24.** Given Wrong Matches has an old triage verdict
  saying a row was a confident reject, when cleanup is requested later, cleanup
  recomputes from candidate/current evidence and records provenance rather than
  deleting based on the stored verdict.
- AE7. **Covers R5, R6, R24.** Given candidate evidence exists but its cheap
  fileset snapshot does not match the candidate folder at import time, when
  import runs, cached candidate evidence is ignored, candidate evidence is
  recomputed if possible, import fails closed if required evidence cannot be
  obtained, and provenance records the mismatch fallback.
- AE8. **Covers R11, R20, R21, R22.** Given legacy JSONB contains a poisoned
  `would_import=true` verdict and old scalar quality fields disagree with the
  new evidence store, when force import runs, active code may use legacy
  identity/location fields to find files but must ignore legacy decision/quality
  fields and recompute through the normal quality pipeline.
- AE9. **Covers R22, R23, R26.** Given any Wrong Matches direct delete, bulk
  delete, converge cleanup, or failed force-import cleanup entry point sees a
  stored confident-cleanup verdict, when cleanup is requested, that entry point
  must call the recomputed cleanup decision service and leave files visible if
  evidence cannot be obtained.
- AE10. **Covers R28, R29, R30, R31, R32.** Given async preview has persisted
  complete candidate evidence and the candidate fileset snapshot still matches,
  when automation, manual import, or force import reaches the mutating lane, the
  import path must run the fresh evidence-pair decision but must not call the
  candidate measurement functions (`run_preimport_gates`, spectral analysis,
  V0 probing, candidate bitrate probing, or verified-lossless source probing)
  again before Beets mutation.

---

## Success Criteria

- A stale preview or triage decision cannot cause Beets mutation or later file
  cleanup.
- Async preview still saves CPU for import by persisting candidate evidence,
  while import decisions remain fresh.
- Snapshot-valid candidate evidence from async preview prevents duplicate
  candidate measurement at import time. Repeated candidate measurement is a
  regression unless the snapshot is stale or evidence is incomplete.
- The quality simulator remains the policy oracle: existing scenario behavior
  is preserved and new incident regressions cover the evidence/decision
  boundary.
- End-to-end import and cleanup regressions prove stored preview/triage verdicts
  cannot authorize Beets mutation or file deletion.
- Operators can inspect persisted provenance and tell whether evidence was
  reused, recomputed, or backfilled for a decision.
- The active code stops carrying policy-shaped evidence names and overloaded
  result blobs through import paths.

---

## Scope Boundaries

- This does not retune quality thresholds, spectral thresholds, codec ranks, or
  provisional lossless policy.
- This does not build a broad golden-corpus snapshot of real request IDs; the
  gate is existing simulator scenarios plus focused incident regressions.
- This does not support cross-request/cross-release evidence reuse.
- This does not add content hashes or audio fingerprints for evidence identity.
  The cheap fileset snapshot defined in R6 is enough for this pass.
- This does not create a per-track relational evidence table. Per-track detail
  may remain audit/debug data.
- This does not migrate historical JSONB audit blobs into the new evidence
  model.
- This does not require surfacing provenance in the UI immediately, but
  provenance must be persisted.
- This does not preserve old active concepts through long-lived compatibility
  shims. Legacy decode is allowed only for historical audit rendering.

---

## Key Decisions

- Evidence is durable; decisions are derived. Persist measurements, not import
  authority.
- The active evidence model is one neutral album-quality evidence shape for
  both candidates and current Beets/request albums.
- V0 probe kind is policy, not evidence. Store one V0 metric plus neutral
  source-lineage facts and let policy decide when the probe matters.
- Use relational storage for active evidence. JSONB made it too easy to mix
  evidence, decisions, and audit payloads.
- Keep verified lossless as a boolean source-proof classification. It is
  computed only while acting on lossless-container files and can survive later
  lossy storage.
- Force import must stay boring: one bypass-distance context flag, no bespoke
  quality code.
- Candidate measurement and Beets mutation are separate boundaries. Async
  preview owns candidate measurement; import owns fresh decision computation
  and mutation. A mutating harness path must consume validated evidence instead
  of silently remeasuring unchanged candidate files.

---

## Dependencies / Assumptions

- The existing simulator suite remains the trusted policy safety net.
- Historical Beets albums may lack current evidence; backfill is required and
  must be conservative.
- Cheap fileset snapshots are sufficient to guard against accidental stale
  candidate evidence. Malicious or deliberate file tampering between preview and
  import is out of scope.
- Existing history/UI code may need legacy audit decoding while active import
  and quality paths move to the new evidence model.

---

## Outstanding Questions

### Resolve Before Planning

- None.

### Deferred to Planning

- [Affects R7, R8][Technical] Define the relational evidence columns, owner
  enum, uniqueness constraints, and migration order.
- [Affects R6, R24][Technical] Define the cheap fileset snapshot database
  representation, quiescence check, and exact mismatch provenance text.
- [Affects R20, R23, R27][Technical] Define the reducer/input shape that lets
  import, preview, triage, and simulator all consume current/candidate
  `AlbumQualityEvidence` pairs.
- [Affects R9, R11][Technical] Isolate legacy scalar/JSONB audit decoding so
  active paths cannot accidentally consume it.

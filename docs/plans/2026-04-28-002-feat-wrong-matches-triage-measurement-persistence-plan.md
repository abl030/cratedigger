---
title: Wrong Matches Triage Measurement Persistence
type: feat
status: active
date: 2026-04-28
origin: docs/brainstorms/wrong-matches-triage-measurement-persistence-requirements.md
---

# Wrong Matches Triage Measurement Persistence

## Overview

The wrong-matches spectral evidence feature shipped yesterday (PR #181) reads
per-candidate spectral and V0-probe evidence from the flat `download_log`
columns `spectral_grade`, `spectral_bitrate`, `v0_probe_kind`,
`v0_probe_avg_bitrate`. Auto-triage on every new wrong-match row already runs
spectral analysis via `preview_import_from_path`, but
`lib/wrong_match_triage.py::_triage_audit_payload` discards the resulting
`ImportResult.new_measurement` and `v0_probe`, so the four cells render as
`— —` for every triaged row. This plan persists that measurement onto the
same `download_log` row that `get_wrong_matches` already reads, so the cells
populate without changing the read-side contract.

The fix is a single new typed write helper on `PipelineDB` plus one extension
to the audit-write path inside triage; both auto-triage and the existing
`backfill_wrong_match_previews` CLI benefit because they share that audit
path. Live data confirms the visible WM population has no existing flat-column
data to overwrite (425 high-distance rows, all NULL on
`spectral_grade`/`v0_probe_*`), so the persistence write is purely additive.

---

## Problem Frame

Yesterday's PR #181 added per-candidate spectral and V0-probe display to the
Wrong Matches tab. The contract test pins
`spectral_grade`/`spectral_bitrate`/`v0_probe_kind`/`v0_probe_avg_bitrate` on
every entry payload, and the SELECT in `PipelineDB.get_wrong_matches` projects
them from the flat `download_log` columns. The display works for any row whose
flat columns are populated. In production those columns are populated only by
`log_download(spectral_grade=…, spectral_bitrate=…, v0_probe_*=…)` calls from
the actual import path — never reached for `high_distance` rejects, which is
the dominant WM population (425 rows live).

Auto-triage at `lib/download.py::_run_post_rejection_wrong_match_triage` runs
`triage_wrong_match` on every freshly-rejected wrong-match row (and the
`backfill_wrong_match_previews` CLI runs the same code path against visible
legacy rows). Triage calls `preview_import_from_download_log` →
`preview_import_from_path` → `run_preimport_gates` (spectral) +
`run_import_one --dry-run` (full measurement + V0 probe), and returns an
`ImportPreviewResult` carrying a populated `ImportResult` with
`new_measurement.spectral_grade`/`spectral_bitrate_kbps` and
`v0_probe.kind`/`avg_bitrate_kbps`.

The audit payload then writes only `verdict`/`decision`/`reason`/`stage_chain`
into `validation_result.wrong_match_triage` — the measurement is dropped.
The four flat columns the contract test pins stay NULL.

(see origin: `docs/brainstorms/wrong-matches-triage-measurement-persistence-requirements.md`)

---

## Requirements Trace

- R1. Auto-triage persists measurement so the four entry-payload cells
  populate for new rows
- R2. `backfill_wrong_match_previews` persists the same measurement, filling
  legacy rows in one operator-initiated run
- R3. No new endpoint, no new per-row "analyze" button — persistence is a
  side effect of triage that already runs
- R4. The four contract-pinned candidate-evidence keys populate after this
  ships, for any row whose triage produced a measurement
- R5. Rows whose preview legitimately produced no measurement (early-reject
  paths like `nested_layout`) continue to render as a dash
- R6. Backfill targets only currently-visible WM rows whose folder is on
  disk — matches today's `backfill_wrong_match_previews` filter
- R7. Backfill is operator-initiated via the existing CLI; no
  auto-on-deploy

**Origin acceptance examples:** AE1 (covers R1, R4), AE2 (covers R2, R6, R7),
AE3 (covers R5), AE4 (covers R3)

---

## Scope Boundaries

- Does not change the wrong-matches read-side contract (`ENTRY_REQUIRED_FIELDS`
  in `tests/test_web_server.py:3386` already pins the four keys)
- Does not change the SELECT in `PipelineDB.get_wrong_matches` — flat-column
  read path stays as shipped in PR #181
- Does not change `_triage_audit_payload` shape for the existing
  verdict/decision/stage_chain fields — only adds measurement persistence
  alongside it
- Does not extend persistence to scenarios excluded from WM
  (`audio_corrupt`, `spectral_reject`)
- Does not add a new web endpoint, new background worker, or new preview
  workflow
- Does not add per-row "analyze" / "preview" / "re-triage" UI actions
- Does not change the `Delete Lossless Opus` safety gate (R4-R6 of the prior
  feature) — that reads `current_spectral_grade` from `album_requests`,
  independent of per-candidate triage
- No schema migration — the four `download_log` columns already exist via
  migrations 001 and 007

---

## Context & Research

### Relevant Code and Patterns

- `lib/wrong_match_triage.py:27-49` — `_triage_audit_payload` is the seam
  that builds the JSONB audit blob. Currently writes only verdict/decision
  strings; needs the measurement extraction.
- `lib/wrong_match_triage.py:52-59` — `_persist_triage_audit` is the
  single dispatch point shared by every code path that calls into triage:
  `triage_wrong_match`, `triage_wrong_matches`, `backfill_wrong_match_previews`.
  Extending here covers all callers.
- `lib/import_preview.py:84` — `ImportPreviewResult.import_result` carries
  the full `ImportResult` Struct on `would_import` and `confident_reject`
  outcomes that reached `run_import_one`. Early-reject paths
  (`nested_layout`, `path_missing`, preimport_reject) leave it as `None` —
  these are R5 dashes.
- `lib/quality.py:1538-1572` — `ImportResult.new_measurement`
  (`AudioQualityMeasurement.spectral_grade`, `.spectral_bitrate_kbps`) and
  `ImportResult.v0_probe` (`V0ProbeEvidence.kind`, `.avg_bitrate_kbps`) are
  the exact fields the four flat columns mirror.
- `lib/pipeline_db.py:1210-1273` — `log_download` is the existing precedent
  for writing the four flat columns from typed Python parameters. The new
  helper mirrors its column list and SQL shape, scoped to UPDATE one row.
- `lib/pipeline_db.py:1394-1415` — `record_wrong_match_triage` is the
  existing JSONB-only triage write. The new measurement-update method sits
  beside it; both fire from the same `_persist_triage_audit` site.
- `tests/fakes.py:1542+` — `FakePipelineDB.record_wrong_match_triage`
  already mirrors the JSONB write. The new measurement-update method needs
  matching coverage.
- `tests/test_pipeline_db.py:2469-2509` — established pattern for
  asserting `get_wrong_matches` row shape and join behaviour. The new
  integration test extends this.

### Institutional Learnings

- `.claude/rules/code-quality.md` § "Test Taxonomy" — three of four
  categories apply here: pure (decision-purity not relevant; the work is
  CRUD), seam (the SQL UPDATE), orchestration (the triage write site),
  integration slice (round-trip from preview → triage → DB → read-side
  query).
- `.claude/rules/pipeline-db.md` — DML on `download_log` lives in
  `PipelineDB` methods. Migrations are the only path for DDL; not relevant
  here because the columns already exist.
- `.claude/rules/code-quality.md` § "Wire-boundary types" — the
  `ImportPreviewResult` and `ImportResult` are already `msgspec.Struct`.
  The persistence step pulls typed values out of the struct; no new
  wire-boundary types to add.
- Live data probe (2026-04-28): of 425 `high_distance` rejections visible
  in WM, zero have `spectral_grade` populated on the flat column today.
  Triage writes are purely additive — no overwrite of existing data in the
  WM-visible population.

### Patterns to Follow

- **Typed DML helper on `PipelineDB`** — pattern set by `log_download`,
  `record_wrong_match_triage`, `update_spectral_state`,
  `update_v0_probe_state` (all in `lib/pipeline_db.py`). Method takes
  named parameters, builds parameterised SQL, commits inside the helper.
- **Fake mirrors the real DB** — `FakePipelineDB` sibling method tracks
  the same state on `entry.extra` so consumer tests see the same row
  shape. `tests/test_fakes.py::TestPipelineDBFakeContract*` enforces
  signature parity at test time.
- **Audit dispatch through `_persist_triage_audit`** — single seam means
  one extension point covers both auto-triage and backfill.

---

## Key Technical Decisions

- **Persist into the four existing flat `download_log` columns** rather
  than a new JSONB sub-blob inside `wrong_match_triage`. The flat columns
  are exactly the four fields the contract test pins
  (`tests/test_web_server.py:3386`) and the SELECT in
  `PipelineDB.get_wrong_matches` already projects (PR #181). Reusing the
  existing read path means U3/U5 from PR #181 work as-is — no SELECT
  change, no contract-test edit, no frontend change. The semantic match
  is also clean: the columns mean "spectral and V0 evidence for this
  download attempt's folder", and triage measures the same folder.
- **Partial / non-destructive update.** Only write columns whose source
  value is non-null. Build the SET clause from the typed measurement,
  skipping fields the measurement doesn't carry. This protects rows that
  may have spectral set from an earlier source (today: zero in WM, but
  the rule is robust regardless).
- **Skip the write when `import_result` or `new_measurement` is None.**
  Early-reject preview paths (`nested_layout`, `path_missing`,
  preimport_reject) leave these as None. Calling the persistence helper
  with all-None values is a no-op; either short-circuit at the call site
  or have the helper itself early-return. R5 covers the display side.
- **Single dispatch site = `_persist_triage_audit`.** Adding the
  measurement write next to the JSONB write means auto-triage,
  per-row triage, batch triage, and backfill all benefit from one change
  — the brainstorm's "persistence is a side effect of triage" lands
  cleanly.
- **No CLI flag changes.** The default mode of `pipeline-cli triage
  backfill` (no flag) already calls `_persist_triage_audit` after every
  preview. Once that helper writes the measurement, the existing CLI
  surface is sufficient. Re-running backfill on a previously-triaged row
  is idempotent (overwrites same data with same data).
- **No new schema.** Migrations 001 and 007 already provide the four
  columns. Confirmed in `migrations/001_initial.sql:165-166` and
  `migrations/007_v0_probe_evidence.sql:8-27`.

---

## Open Questions

### Resolved During Planning

- *Flat columns vs JSONB write target?* Flat columns. Read path from PR
  #181 already wired; semantic match; no contract-test or SELECT edit.
- *Early-reject path treatment?* No special handling needed. R5 covers
  display ("dash for missing measurement"), and the triage write
  short-circuits when `import_result` is None. Live data shows
  `nested_layout` is rare.
- *CLI flag additions?* None. Default `backfill_wrong_match_previews`
  mode already routes through `_persist_triage_audit`; extending that
  helper extends the CLI for free.
- *Fake parity?* Required, with a contract-test guard. See U3.
- *Overwrite risk in production?* None for the WM-visible population —
  live data confirms 0/425 high-distance rows have flat spectral set
  today. Partial-update rule still adopted as defensive policy.

### Deferred to Implementation

- Whether to build the SET clause dynamically in Python (match the
  measurement fields one by one) or as `COALESCE(%s, column)` in SQL.
  Both achieve the partial-update rule; pick whichever is more readable
  in the helper. Decide once you see the surrounding code shape.
- Whether the helper signature accepts a typed `AudioQualityMeasurement`
  + `V0ProbeEvidence` pair, or four named parameters mirroring
  `log_download`. Prefer whichever matches the existing helper style
  best — `update_spectral_state` and `update_v0_probe_state` already
  exist as references.
- Whether to log a one-line INFO line per row when the measurement
  write fires (matches `_run_post_rejection_wrong_match_triage`'s
  existing log shape) or stay silent. Decide based on what helps live
  debugging.

---

## Implementation Units

- U1. **Add typed measurement-update helper to `PipelineDB`**

**Goal:** Provide the typed DML seam for writing measurement data onto a
single `download_log` row, mirroring the same column set the SELECT in
`get_wrong_matches` projects.

**Requirements:** R1, R2, R4

**Dependencies:** None

**Files:**
- Modify: `lib/pipeline_db.py`
- Modify: `tests/fakes.py` (mirror on `FakePipelineDB`)
- Test: `tests/test_pipeline_db.py`
- Test: `tests/test_fakes.py` (signature parity guard already exists; just
  ensure new method passes it)

**Approach:**
- Add a new method on `PipelineDB` that updates the four flat columns on
  one `download_log` row by id. Partial / non-destructive update —
  columns whose source value is `None` are left untouched. The four
  columns are `spectral_grade`, `spectral_bitrate`, `v0_probe_kind`,
  `v0_probe_avg_bitrate`. (Decision: also include
  `existing_min_bitrate`/`existing_spectral_bitrate`/`existing_v0_probe_*`
  if the audit data carries them — defer to implementation; minimum is
  the four columns the read path uses.)
- Mirror the method on `FakePipelineDB` writing into `entry.extra` so
  `get_wrong_matches` on the fake reads the same shape (the fake's
  `get_wrong_matches` already pulls these four keys from `entry.extra`
  per the U2 commit on PR #181).
- The fake-vs-real signature parity guard in
  `tests/test_fakes.py::TestPipelineDBFakeContractInternals` will fail
  the build if signatures drift; let it be the contract.

**Execution note:** Test-first. Build a minimal scenario (a row with NULL
columns + a call with populated values + assert UPDATE landed) before
writing the helper.

**Patterns to follow:**
- `lib/pipeline_db.py::log_download` (full INSERT with the same column
  set)
- `lib/pipeline_db.py::record_wrong_match_triage` (single-row UPDATE
  pattern, parameterised SQL, commit-in-helper)
- `lib/pipeline_db.py::update_spectral_state` and
  `update_v0_probe_state` (existing typed update helpers — likely the
  best stylistic match)
- `tests/fakes.py:1011+` — existing pattern of routing optional
  columns into `entry.extra`

**Test scenarios:**
- Happy path: row exists with NULL columns; call updater with
  `spectral_grade='genuine'`, `spectral_bitrate=950`,
  `v0_probe_kind='lossless_source_v0'`, `v0_probe_avg_bitrate=265` → all
  four columns are populated; method returns success indicator.
- Edge case (partial update): row exists with NULL columns; call updater
  with only `spectral_grade='suspect'` and `spectral_bitrate=320` (V0
  probe omitted / passed as None) → spectral columns set, V0 columns
  remain NULL. Pins the partial-update rule.
- Edge case (non-destructive): row already has
  `spectral_grade='genuine'`, `spectral_bitrate=950`; call updater with
  `spectral_grade=None`, `spectral_bitrate=None`,
  `v0_probe_kind='lossless_source_v0'`, `v0_probe_avg_bitrate=265` →
  spectral columns retain genuine/950, V0 columns now populated. Pins
  that None inputs do not wipe existing data.
- Edge case (no-op): row exists; call updater with all four params
  None → no SQL UPDATE fires (or UPDATE fires but is empty); row
  unchanged. Pins the empty-call short-circuit.
- Error path: call with a `download_log_id` that doesn't exist →
  helper returns the appropriate "not found" indicator (rowcount-based,
  same shape as `record_wrong_match_triage`'s `cur.rowcount > 0`).
- Fake parity: `tests/test_fakes.py::TestPipelineDBFakeContractInternals`
  test suite passes — fake signature matches real signature.
- Fake state: `FakePipelineDB.get_wrong_matches()` after a fake-side
  update call surfaces the four keys with the expected values, mirroring
  the real DB's behaviour.

**Verification:**
- `nix-shell --run "python3 -m unittest tests.test_pipeline_db tests.test_fakes -v"` passes
- Pyright clean on `lib/pipeline_db.py` and `tests/fakes.py`

---

- U2. **Persist measurement from triage's preview result**

**Goal:** Extend the audit-write path inside `lib/wrong_match_triage.py`
so every triage outcome that produced an `ImportResult` with a
measurement also fires U1's helper, populating the four `download_log`
columns the wrong-matches tab reads.

**Requirements:** R1, R2, R3, R4, R5

**Dependencies:** U1

**Files:**
- Modify: `lib/wrong_match_triage.py`
- Test: `tests/test_wrong_match_triage.py` (locate the existing test
  module first; add to it)

**Approach:**
- Inside `_persist_triage_audit` (or a sibling helper invoked from the
  same site), extract the measurement from
  `result.preview.import_result.new_measurement` and the V0 probe from
  `result.preview.import_result.v0_probe` when present, and call U1's
  helper with the four typed values. Keep the existing JSONB-only
  `record_wrong_match_triage` call intact and unchanged.
- When `result.preview` is None, or `result.preview.import_result` is
  None, or `new_measurement` is None and `v0_probe` is None, skip the
  measurement write (R5: early-reject paths legitimately have no
  measurement to persist).
- The order is: write JSONB audit first (existing behaviour), then write
  measurement (new). If the measurement write fails, the JSONB audit is
  preserved — auditability is more critical than measurement
  display.
- Both `triage_wrong_match` (post-rejection auto-triage) and
  `backfill_wrong_match_previews` (operator-initiated CLI) call into
  `_persist_triage_audit`, so this single change covers AE1 and AE2.

**Execution note:** Test-first. Stub the preview result with a populated
`ImportResult` and assert the measurement write fires; stub another with
`import_result=None` and assert it does not.

**Patterns to follow:**
- `lib/wrong_match_triage.py:_persist_triage_audit` and
  `_triage_audit_payload` — the existing audit-write site
- `lib/import_preview.py:_preview_result` and the `ImportPreviewResult`
  Struct — typed access to `import_result.new_measurement.*` and
  `import_result.v0_probe.*`

**Test scenarios:**
- Covers AE1 (R1, R4). Happy path: build a `WrongMatchTriageResult` with
  `preview.import_result.new_measurement` populated
  (`spectral_grade='genuine'`, `spectral_bitrate_kbps=950`) and
  `preview.import_result.v0_probe` populated
  (`kind='lossless_source_v0'`, `avg_bitrate_kbps=265`); call
  `_persist_triage_audit` with a `FakePipelineDB`; assert the JSONB
  triage audit was recorded AND the four measurement columns were
  updated on the row.
- Covers AE3 (R5). Early-reject path: build a result with
  `preview.import_result=None` (e.g.,
  `kept_uncertain` from a `nested_layout` early-return scenario); call
  `_persist_triage_audit`; assert the JSONB triage audit was recorded
  but no measurement update fired.
- Edge case: `preview.import_result.new_measurement is None` but
  `preview.import_result.v0_probe` is populated → only V0 columns
  update; spectral columns untouched.
- Edge case: `preview.import_result.new_measurement` is populated but
  `preview.import_result.v0_probe is None` → only spectral columns
  update; V0 columns untouched.
- Edge case: both `new_measurement` and `v0_probe` are None on the
  populated `import_result` (preview reached `run_import_one` but
  measurement was empty) → no measurement update fires; JSONB audit
  still written.
- Edge case: `result.preview is None` (the kept_would_import branch
  on the second `if preview.would_import` clause —
  `lib/wrong_match_triage.py:124` — currently passes None preview;
  needs handling) → no measurement update; JSONB audit still written
  with whatever shape the existing code produces.
- Integration with auto-triage path: call
  `triage_wrong_match(fake_db, log_id)` end-to-end with a stubbed
  `preview_import_from_download_log` returning a populated result;
  assert the row's flat columns populated.
- Integration with backfill path:
  `backfill_wrong_match_previews(fake_db, ...)` runs end-to-end against
  a fake with one wrong-match row; assert the row's flat columns
  populated after the call.
- Order regression: when U1's helper raises, the JSONB audit is still
  recorded (write order preserves auditability). Stub the helper to
  raise, assert JSONB audit row exists.

**Verification:**
- `nix-shell --run "python3 -m unittest tests.test_wrong_match_triage -v"` passes
- Pyright clean on `lib/wrong_match_triage.py`

---

- U3. **Round-trip integration slice + wrong-matches contract proof**

**Goal:** Pin the cross-layer behaviour the original brainstorm asked
for: a wrong-match row that goes through triage ends up with the four
candidate-evidence keys populated when the read-side route is invoked.
This is the test that would have caught yesterday's drop-on-the-floor
bug.

**Requirements:** R1, R2, R4

**Dependencies:** U1, U2

**Files:**
- Test: `tests/test_integration_slices.py` (or sibling — locate the
  existing slice file first)
- Optionally test: `tests/test_web_server.py` (a contract test
  asserting that a triaged fake row produces the four keys with values
  through the wrong-matches route)

**Approach:**
- Build a slice that wires `FakePipelineDB` + a stubbed
  `preview_import_from_download_log` that returns a populated
  `ImportPreviewResult`, calls `triage_wrong_match`, then calls
  `db.get_wrong_matches()`, asserts the resulting row dict contains the
  four keys with the expected values.
- Optionally extend the `TestWrongMatchesContract` class in
  `tests/test_web_server.py` with a single test that simulates the
  triaged row by setting the four extra columns on the mock
  `_DEFAULT_WRONG_MATCH_ROW` and calling `_get('/api/wrong-matches')`.
  This pins the end-to-end shape (DB row → route payload → entry dict)
  without re-testing what U2's tests already cover. Skip if the existing
  contract tests in PR #181 already cover this from the row-shape side
  (likely yes — `test_entry_surfaces_stored_spectral_and_v0_probe_evidence`
  already pins the row → entry mapping).
- The slice is the one new piece. The contract tests stay unchanged
  unless an obvious gap surfaces.

**Patterns to follow:**
- `tests/test_integration_slices.py::TestSpectralPropagationSlice`
  (similar shape: end-to-end slice with `FakePipelineDB`)
- `tests/helpers.py::make_request_row`,
  `make_import_result`, `make_validation_result` for setup builders

**Test scenarios:**
- Covers AE1 (R1, R4). Round trip: fresh wrong-match row in
  `FakePipelineDB`; stub `preview_import_from_download_log` to return
  `ImportPreviewResult` with `would_import=True`,
  `import_result.new_measurement.spectral_grade='genuine'`,
  `spectral_bitrate_kbps=950`,
  `import_result.v0_probe.kind='lossless_source_v0'`,
  `avg_bitrate_kbps=265`; call `triage_wrong_match`; call
  `db.get_wrong_matches()`; assert the returned row dict carries the
  four keys with the expected values.
- Covers AE2 (R2, R6, R7). Same shape, but driven through
  `backfill_wrong_match_previews(fake_db, request_id=..., dry_run=False)`
  to confirm the operator-initiated CLI path produces the same outcome.
- Covers AE3 (R5). Same setup but stubbed preview returns
  `import_result=None` (early-reject path); after triage,
  `db.get_wrong_matches()` row dict has the four keys present with
  `None` values — pinning the dash-for-missing-measurement display.

**Verification:**
- `nix-shell --run "python3 -m unittest tests.test_integration_slices -v"` passes
- All previously-passing wrong-matches tests in
  `tests/test_web_server.py` continue to pass (regression guard for
  PR #181's read-path contract)

---

## System-Wide Impact

- **Interaction graph:** The change touches `_persist_triage_audit`,
  which is the dispatch point for three callers: post-rejection
  auto-triage in `lib/download.py`, `triage_wrong_matches` (per-row /
  batch), and `backfill_wrong_match_previews`. All three benefit from
  the single change. No other call sites.
- **Error propagation:** Order is JSONB audit → measurement write. If
  measurement write fails, JSONB audit survives — operator can still
  see triage verdict in `wrong_match_triage`. Failure isolation pinned
  by U2's order-regression test.
- **State lifecycle risks:** Re-running backfill on a previously-triaged
  row updates the four columns to the new measurement (idempotent if
  the file is unchanged; refresh if it was edited). No partial-write
  hazard — the helper is a single UPDATE.
- **API surface parity:** No web/CLI surface changes. The
  `/api/wrong-matches` route's payload shape is untouched (PR #181
  pinned it, this plan just makes the data flow). The
  `pipeline-cli triage backfill` CLI is untouched in shape; behaviour
  changes from "audit only" to "audit + measurement" implicitly via the
  shared helper.
- **Integration coverage:** U3's slice is the cross-layer proof — the
  exact gap that yesterday's PR didn't have. Without it, U2's unit
  tests pass and the route's contract test passes, but the live
  behaviour still drops the data on the floor.
- **Unchanged invariants:**
  - `_triage_audit_payload`'s existing keys
    (`verdict`/`decision`/`reason`/`stage_chain`/`cleanup_eligible`/
    `source_path`) — unchanged. New behaviour is a sibling write,
    not a payload edit.
  - `record_wrong_match_triage`'s JSONB write target
    (`validation_result.wrong_match_triage`) — unchanged.
  - `get_wrong_matches` SELECT shape — unchanged. PR #181's columns
    work as-is.
  - `_build_wrong_match_groups` entry payload shape — unchanged. The
    four keys are already in the contract; values just stop being
    None.
  - DB schema — unchanged. No migration.
  - The `Delete Lossless Opus` safety gate — unchanged. It reads
    `current_spectral_grade` from `album_requests`, independent of
    `download_log` flat columns.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Triage measurement overwrites valid pre-existing data on the four columns | Live data confirms 0/425 high-distance WM rows have flat spectral set today; partial-update rule (skip None inputs) further guards. Pinned by U1's non-destructive edge case test. |
| The kept_would_import early-return branch (`lib/wrong_match_triage.py:124`) currently passes a None preview and the existing audit shape there gets brittle | U2 explicitly handles `result.preview is None`; existing tests still cover the JSONB shape. If the branch needs preview attached for measurement to flow, fix that branch too — pinned by U2 edge-case test. |
| Backfill on a large legacy population starves the regular pipeline | Visible WM scope is small (~83 candidates with on-disk folders per live UI); single-shot CLI run is ~15-40 min CPU. Operator-initiated, not auto-on-deploy (R7). |
| `import_result.new_measurement` shape drift | `ImportResult` is a `msgspec.Struct` (wire-boundary type per `.claude/rules/code-quality.md`). Decoded once at the harness boundary; downstream consumers — including U2 — work with the typed object. Field rename or type drift surfaces as a `msgspec.ValidationError` at the harness, not as silent NULLs in the DB. |
| Re-triaging a row that's been edited on disk produces a different measurement than the one cached on the row, and the operator can't tell which is which | Out of scope for this plan — addressed by `current_spectral_*` semantics on `album_requests` for the on-disk copy; per-attempt `download_log` columns reflect what triage saw at last triage time, which is the right semantic. Operator can re-run backfill if they want a refresh. |

---

## Documentation / Operational Notes

- After deploying U1+U2+U3, the operator runs
  `pipeline-cli triage backfill` (or whatever the existing CLI surface
  is — locate during implementation; the function is
  `backfill_wrong_match_previews` in `lib/wrong_match_triage.py`) once
  to fill in the legacy population. Expected: ~15-40 min runtime
  against the visible-with-folder set. New rows fill in automatically
  from the post-rejection auto-triage hook.
- No `cratedigger-db-migrate` activity expected on deploy — no schema
  change. Migration unit will report "Schema is up to date — nothing
  to apply."
- `cratedigger-web` restarts on `nixos-rebuild switch` and picks up the
  read-side change implicitly (no read-side change in this plan; the
  existing PR #181 SELECT works as-is). `cratedigger.service` (the
  5-min timer) picks up the new triage write path on its next cycle.
- Spot-check after deploy + backfill: open `music.ablz.au` Wrong Matches
  tab, confirm the spectral and V0 cells now show real values for rows
  whose preview reached `run_import_one` (the bulk of the population).
  Rows showing dashes after this should correspond to early-reject
  preview paths (R5).

---

## Sources & References

- **Origin document:** [docs/brainstorms/wrong-matches-triage-measurement-persistence-requirements.md](../brainstorms/wrong-matches-triage-measurement-persistence-requirements.md)
- Prior feature plan:
  [docs/plans/2026-04-28-001-feat-wrong-matches-spectral-evidence-plan.md](2026-04-28-001-feat-wrong-matches-spectral-evidence-plan.md)
- Prior feature PR: #181 (merged 2026-04-28; flake-bumped on doc1; live)
- Triage code path: `lib/wrong_match_triage.py`,
  `lib/download.py::_run_post_rejection_wrong_match_triage`,
  `scripts/pipeline_cli.py::backfill_wrong_match_previews` invocation
- Preview measurement source: `lib/import_preview.py:319-468`
  (`preview_import_from_path` builds the `ImportResult` from the
  harness)
- Read-side query: `lib/pipeline_db.py:1314-1362` (the `get_wrong_matches`
  SELECT extended in PR #181)
- Schema: `migrations/001_initial.sql:165-166`,
  `migrations/007_v0_probe_evidence.sql:8-27`
- Live data probe (2026-04-28):
  `pipeline-cli query "SELECT beets_scenario, COUNT(*), COUNT(*) FILTER
  (WHERE spectral_grade IS NOT NULL) FROM download_log WHERE
  outcome='rejected' AND validation_result->>'failed_path' IS NOT NULL
  GROUP BY beets_scenario"` confirmed 425 high-distance / 0 with
  spectral.

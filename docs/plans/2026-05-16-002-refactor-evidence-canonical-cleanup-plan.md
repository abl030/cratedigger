---
name: evidence-canonical-cleanup
description: Last-mile cleanup of the evidence/decision-boundary refactor — re-key evidence by content, delete the run_preimport_gates shim, delete PR #256's reject-time re-measurement, fix wrong-match triage lookup.
status: active
plan_type: refactor
created: 2026-05-16
deepened: never
related:
  - issue: "#257"
  - prs: ["#253", "#254", "#255", "#256"]
  - origin_plans:
      - docs/plans/2026-05-14-001-refactor-quality-evidence-decision-boundary-plan.md
      - docs/plans/2026-05-15-002-refactor-importer-never-measures-plan.md
      - docs/plans/2026-05-16-001-refactor-preview-never-decides-plan.md
---

# refactor: Evidence is canonical, owners are addressing — last-mile cleanup

## Summary

The four refactors that landed between 2026-05-14 and 2026-05-16 (#253) plus the same-day hotfixes (#254, #255, #256) were collectively chasing one architectural goal:

> One queue. Preview workers fill evidence. Importer works on evidence. Triage works on existing evidence. Evidence is never deleted unless files change.

Three pieces of legacy still violate that shape and one structural change makes it permanent. This plan lands all four as a clean break:

- **(A)** Wrong-match triage looks up evidence by `download_log_id` only, missing preview-worker rows owned by `import_job_candidate:job_id`. Lookup misses → `_preview_for_triage` re-measures. Fix the lookup; stop re-measuring.
- **(B)** PR #256 papered over (A) by writing a second evidence row under `download_log_candidate:dl_id` at reject time, via `_persist_candidate_evidence_for_reject` re-measuring already-measured audio. Once (A) lands, that helper is dead. Delete it.
- **(C)** `lib.preimport.run_preimport_gates` is a shim that bundles measurement and decision — the exact anti-pattern the three-PR refactor was eliminating. Two callers remain (`lib/import_preview.py:977`, `lib/download.py:1200`). Migrate them to the post-#254 pattern (`measure_preimport_state` + `preimport_decide`) and delete the shim + its legacy decision helper + its legacy denylist side-effect helper.
- **(D)** Re-key `album_quality_evidence` from `(owner_type, owner_id)` to `(mb_release_id, snapshot_fingerprint)`. Owners become addressing FKs on `import_jobs` / `download_log` / `album_requests`. Multiple "owners" for the same on-disk audio collapse into one canonical row. **Backfill-and-rekey migration**, not a TRUNCATE: existing evidence rows have their files table already populated, so `snapshot_fingerprint` is computable inside the migration. `mb_release_id` is JOINable from the owner table. Addressing FKs are backfilled in the same migration. No shim, no dual-write, no transition — but no operational re-measurement either. The mantra ("evidence should never be deleted unless files change") holds.

After A+B+C+D: `run_preimport_gates`, `_legacy_preimport_decision`, `_apply_legacy_denylist_side_effects`, and `_persist_candidate_evidence_for_reject` are gone. Evidence rows are keyed by content. Owners are FKs. One queue, one measurement, one row per on-disk audio. The framing matches the code.

---

## Problem Frame

### What the data model says today

`album_quality_evidence` (migration 017) is keyed by `(owner_type, owner_id)` with three owner kinds:

| Owner | Written by | Read by |
|-------|-----------|---------|
| `import_job_candidate:job_id` | Preview worker after `measure_preimport_state` | Importer at action time |
| `download_log_candidate:dl_id` | `_persist_candidate_evidence_for_reject` (PR #256) at reject time | Wrong-match triage |
| `request_current:request_id` | `ensure_current_evidence_for_action` (triage backfill) | Same triage step + future readers |

This key encodes *role* in the schema. But the role is already encoded by *which addressing entity holds the reference*. The owner column is a tautology with a worse failure mode: lookups have to know in advance which owner to ask for, and you get duplicate rows for the same audio when more than one address points at it.

### What the framing says

Evidence describes "the audio at this path right now". Its identity is the audio. Address points at it.

- A staged candidate at `/Incoming/auto-import/<dir>` with file inventory F has *one* evidence row. Preview writes it. Importer reads it. Triage reads it. Reject path reads it. All addressing entities (`import_job`, `download_log`) reference that single row.
- A library copy at `/mnt/virtio/Music/Beets/<artist>/<album>` with file inventory G has its own evidence row (different fingerprint because different audio). `album_requests.current_evidence_id` references it.
- The same MB release with two different rips on disk (e.g., a Discogs candidate and a MusicBrainz candidate, or a recovered backup vs a new download) cleanly produces two rows under the same `mb_release_id` with different fingerprints.

The clarifying example from planning: at import time the importer compares the staged candidate to the library copy. Today that comparison re-measures the library fresh via `BeetsDB.get_album_info()` (lib/beets_db.py:356) — no persisted-evidence lookup is involved. So the re-keying never crosses the importer's library-comparison path; the library side either has its own evidence row (different fingerprint) or no row at all (fresh measurement, same as today).

### Why A and B exist as one issue

A and B are the same bug seen from two sides. The triage path doesn't know how to find preview-worker evidence (A); PR #256 routed around it by making the reject path emit a second row keyed for triage (B). The right fix is to make the lookup work, then delete the workaround. Both are owner_type complexity; both go away when owners become FKs.

### Why C is in the same plan

The shim is independent code, but it carries the *same* anti-pattern (bundle measurement with decision) the rest of this refactor is removing. PR #254 was a regression caused by exactly the shim path leaking into the worker. The longer the shim lives, the more times that regression will happen. Re-keying touches every code path that constructs `AlbumQualityEvidenceOwner` — migrating the shim's two remaining callers fits naturally in the same edit.

---

## Scope Boundaries

### In scope

- A: wrong-match triage lookup becomes owner-agnostic (post-rekey: address-FK or fingerprint-driven).
- B: `_persist_candidate_evidence_for_reject` deleted, reject path stops re-measuring.
- C: `run_preimport_gates`, `_legacy_preimport_decision`, `_apply_legacy_denylist_side_effects` deleted; two remaining callers migrated to `measure_preimport_state` + `preimport_decide`; affected tests migrated off the `@patch('lib.preimport.run_preimport_gates')` shape.
- D: `album_quality_evidence` re-keyed from `(owner_type, owner_id)` to `(mb_release_id, snapshot_fingerprint)`; addressing FKs added to `import_jobs`, `download_log`, `album_requests`; old rows truncated; affected `import_jobs.preview_status` reset so the preview worker re-fills naturally on its next cycle.

### Out of scope (not deferred — never)

- Changes to the JSONB measurement payload columns on `album_quality_evidence` (codec, bitrate metrics, spectral grade, v0 fields). The columns and their semantics stay exactly as migration 017+019 define them.
- Changes to `lib.beets_db.BeetsDB.get_album_info` or how the importer compares candidate-vs-library quality. That path is already fresh-measure-only and is not the bug.
- The importer queue ownership boundary (importer is the only beets-mutating writer). No change.
- Spectral / V0 / decision-function semantics. The decision functions in `lib.quality` are unchanged.

### Deferred to Follow-Up Work

- Reaping orphan rows from `album_quality_evidence` when files actually change on disk. Today nothing prunes evidence; a "evidence is never deleted unless files change" enforcer is a separate concern. After this plan there is exactly one row per (release, fingerprint), so orphans are at worst inefficient, not incorrect.
- A retention/aging policy for old library-side `current_evidence` rows when a request is re-imported. Out-of-scope; same reason.

---

## Origin / Source Material

Origin issue: `#257` (verbatim framing carried forward as the architectural goal).

Predecessor plans (canonical post-refactor data model):
- `docs/plans/2026-05-14-001-refactor-quality-evidence-decision-boundary-plan.md` — introduced `AlbumQualityEvidence` and the owner family.
- `docs/plans/2026-05-15-002-refactor-importer-never-measures-plan.md` — importer reads persisted evidence, never re-measures.
- `docs/plans/2026-05-16-001-refactor-preview-never-decides-plan.md` — preview measures only; `preimport_decide` is pure.

The owner_type/owner_id schema in those plans was the right model for the bounded refactor they were each delivering. This plan reaches the end state those refactors were converging toward.

---

## High-Level Technical Design

*This section illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

### Snapshot fingerprint

`snapshot_fingerprint` is a deterministic hash over the audio inventory that already drives `audio_snapshot_matches` (lib/quality_evidence.py:189–235). Per-file tuple:

```
(relative_path, size_bytes, extension, container, codec)
```

Sorted by `relative_path`, JSON-encoded with stable key order, SHA-256 hex digest. Same fields `_snapshot_match_key` already uses. Mtime is deliberately excluded — the doc in 017 already explains why (ID3 tag mutation, virtiofs flake).

This means:
- The fingerprint is computable from the on-disk files (via `snapshot_audio_files(path)`).
- The fingerprint is computable from a persisted evidence row (read its `album_quality_evidence_files` rows, hash the same tuples).
- `audio_snapshot_matches` still does its existing per-file equality check — fingerprint identity is for *lookup*, not for *freshness*. Both jobs are needed: fingerprint finds candidate rows fast; per-file match confirms the row is still describing what's currently on disk.

### Schema after migration

```
album_quality_evidence
  id BIGSERIAL PK
  mb_release_id TEXT NOT NULL          -- NEW: denorm from owner table
  snapshot_fingerprint TEXT NOT NULL   -- NEW: SHA-256 over file tuples
  source_path TEXT NOT NULL            -- NEW: the on-disk root where measurement happened
  measured_at TIMESTAMPTZ NOT NULL
  ... all existing measurement columns (codec, container, bitrates, spectral, v0, verified_lossless, audio_corrupt, folder_layout, audio_file_count, filetype_band, matched_bad_audio_hash_*) ...
  -- DROPPED: owner_type, owner_id, idx_album_quality_evidence_owner,
  --          album_quality_evidence_one_per_owner
  UNIQUE (mb_release_id, snapshot_fingerprint)
  INDEX (mb_release_id)

album_quality_evidence_files
  -- unchanged

import_jobs
  ... existing columns ...
  candidate_evidence_id BIGINT REFERENCES album_quality_evidence(id) ON DELETE SET NULL   -- NEW

download_log
  ... existing columns ...
  candidate_evidence_id BIGINT REFERENCES album_quality_evidence(id) ON DELETE SET NULL   -- NEW

album_requests
  ... existing columns ...
  current_evidence_id BIGINT REFERENCES album_quality_evidence(id) ON DELETE SET NULL     -- NEW
```

### Lookup flow after rekey

```
Preview worker measures /Incoming/auto-import/<dir> for import_job J:
  files = snapshot_audio_files(path)
  fp = snapshot_fingerprint(files)
  evidence = upsert_evidence(mb_release_id=J.mb_release_id, snapshot_fingerprint=fp, source_path=path, ...)
  import_jobs.candidate_evidence_id = evidence.id

Importer at action time for J:
  evidence = SELECT * FROM album_quality_evidence WHERE id = J.candidate_evidence_id
  if not audio_snapshot_matches(J.source_path, evidence.files):
      # files changed on disk — re-measure path
  else:
      proceed with persisted evidence

Triage of download_log row D:
  evidence_id = D.candidate_evidence_id
  if evidence_id is null:
      # fall back via the request join (download_log has no import_job_id column;
      # it joins to import_jobs via request_id, picking the most recent import_job
      # for that request that still has candidate_evidence_id set).
      J = SELECT * FROM import_jobs
          WHERE request_id = D.request_id AND candidate_evidence_id IS NOT NULL
          ORDER BY created_at DESC LIMIT 1
      evidence_id = J.candidate_evidence_id if J else None
  if evidence_id is None:
      # genuine evidence-less row (rare post-migration because U2 backfilled both FKs)
      fall back to _preview_for_triage   # remeasures; logs the row as legacy
  else:
      evidence = SELECT * FROM album_quality_evidence WHERE id = evidence_id
      if audio_snapshot_matches(D.source_path, evidence.files):
          proceed without re-measuring

Reject path inside importer worker:
  # _handle_rejected_result no longer measures. It only:
  # 1. updates the import_job's outcome
  # 2. (optional) writes D.candidate_evidence_id = J.candidate_evidence_id so triage has a direct FK
  # No call to measure_preimport_state. No second evidence row.
```

### Shim removal

Two call sites today:

```
lib/import_preview.py:977   non-worker legacy nested rejection path
lib/download.py:1200        auto-import fallback (effectively dead in prod, live in tests)
```

Both replaced with the post-#254 pattern the worker already uses:

```
measurement = measure_preimport_state(path, ...)
decision    = preimport_decide(measurement, cfg, ...)
# caller handles persistence + denylist side effects directly
```

Then `run_preimport_gates`, `_legacy_preimport_decision`, `_apply_legacy_denylist_side_effects` are deleted (lib/preimport.py:672–902). Tests in `tests/test_download.py` that `@patch('lib.preimport.run_preimport_gates')` migrate to patching the two new seams or (better) to integration-slice form using `FakePipelineDB`.

---

## Key Technical Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Migration strategy | **Backfill, not TRUNCATE.** In one transaction: add new columns (`mb_release_id`, `snapshot_fingerprint`, `source_path` on evidence; `candidate_evidence_id` on `import_jobs`/`download_log`; `current_evidence_id` on `album_requests`). Backfill `mb_release_id` from the owner JOIN and `snapshot_fingerprint` from each row's existing `album_quality_evidence_files` records. Dedupe rows that collapse under the new key (keep most recent `measured_at`). Backfill addressing FKs from the old owner_type/owner_id pairs (including a cross-walk so each `download_log` gets the FK from its sibling `import_job_candidate` row when no direct `download_log_candidate` row exists). Drop old columns and constraints last. | The mantra says "evidence should never be deleted unless files change." TRUNCATE-then-reset would delete evidence wholesale when no files changed, violating the mantra and causing a one-time operational re-measurement storm. Backfill preserves measurement history, populates the new addressing FKs so first-week triage doesn't hit the `_preview_for_triage` cold path on every historical reject, and keeps the clean-break property — there are no shims, no dual-writes, just one atomic schema migration. |
| Snapshot fingerprint formula | SHA-256 over JSON-encoded sorted list of `(relative_path, size_bytes, extension, container, codec)` per file | Reuses the exact fields `_snapshot_match_key` already uses. Deterministic. Computable both from disk and from the files table (so the migration can backfill from the existing files rows without touching disk). Excludes mtime (017 doc explains why). |
| Addressing model | FK column on each addressing entity (`import_jobs.candidate_evidence_id`, `download_log.candidate_evidence_id`, `album_requests.current_evidence_id`), all `ON DELETE SET NULL` | Encodes "role" by which table holds the reference, not by a string column. Standard FK. Lookups become a JOIN, not a polymorphic query. |
| Triage FK chain | `download_log.candidate_evidence_id`; if NULL, JOIN through `download_log.request_id → import_jobs.request_id` (most recent import_job for the request that still has `candidate_evidence_id` set); if still NULL, fall back to `_preview_for_triage` (re-measure) and log the row as legacy-evidence-less | `download_log` has no direct `import_job_id` column today, so the fallback goes through `request_id`. The migration's FK backfill (above) populates `download_log.candidate_evidence_id` for every historical row that has any addressable evidence (direct or via sibling import_job), so the re-measurement fallback fires only for genuinely evidence-less legacy rows — not for every pre-deploy row. |
| Where the rekey lives in code | `lib/quality_evidence.py` writers compute fingerprint and `mb_release_id` from caller-supplied args; `PipelineDB.upsert_album_quality_evidence` writes new key; `load_album_quality_evidence` becomes `load_album_quality_evidence_by_id(id)` and `find_album_quality_evidence(mb_release_id, fingerprint)`; `_candidate_owners` is deleted | Code surface mirrors schema surface. No `Owner` dataclass after migration. |
| In-flight import_jobs at deploy | No `preview_status` reset needed. Backfill keeps every job's evidence intact and writes the FK back to `import_jobs.candidate_evidence_id` from the existing `import_job_candidate:job_id` row | Previous design (TRUNCATE + reset to `waiting`) caused operational shock. Backfill makes it a no-op for in-flight work — the worker doesn't need to re-measure anything. The `running` requeue concern from the prior design is moot. |
| Test re-keying | `FakePipelineDB` stores `album_quality_evidence` keyed by `(mb_release_id, snapshot_fingerprint)`; existing test sites using `AlbumQualityEvidenceOwner` migrate to passing `mb_release_id` + `snapshot_fingerprint` via a `helpers.py` builder | One round of test churn; no second shape to maintain. Builder makes future test sites cheap. |
| Migration number | `021` (not `020` — `020_recover_stuck_preview_uncertain_jobs.sql` already exists; verified at planning time) | Avoid the numbering collision flagged in review. |
| Commit shape for U2 + U3 | U2 (migration SQL) and U3 (code that reads/writes the new schema) **land in one commit**. The unit split in this plan is documentary — they describe two coherent halves of one atomic change | Between U2 and U3 the suite goes red: schema has dropped `owner_type`, code still references `AlbumQualityEvidenceOwner`. Atomic commit avoids a broken intermediate state for reviewers, CI, and local development. The deploy already runs migrate-before-services; the commit boundary needs the same atomicity. |

---

## System-Wide Impact

| Surface | Effect |
|---------|--------|
| Pipeline DB | Schema migration. `album_quality_evidence` re-keyed via in-place backfill: fingerprint computed from existing files rows, `mb_release_id` JOINed from old owner table, duplicates collapsed by `measured_at DESC`, three addressing FK columns added and populated. No evidence rows deleted unless files actually changed; in-flight `import_jobs` keep their evidence so the worker has nothing to re-measure. |
| Preview worker (`scripts/import_preview_worker.py`, `lib/import_preview.py`) | Computes fingerprint as part of `persist_candidate_evidence_from_measurement`. Sets `import_jobs.candidate_evidence_id` after upsert. |
| Importer worker (`lib/import_dispatch.py`, `lib/quality_evidence.py`) | Reads evidence by `import_jobs.candidate_evidence_id` FK rather than `(owner_type, owner_id)` lookup. Confirms freshness with `audio_snapshot_matches` (unchanged). |
| Reject path (`lib/download.py::_handle_rejected_result`) | No longer measures. Optionally copies `import_jobs.candidate_evidence_id` to `download_log.candidate_evidence_id` so triage has a direct FK. |
| Wrong-match triage (`lib/wrong_match_cleanup_decision.py`, `lib/wrong_match_triage.py`, `lib/import_evidence.py`) | Lookup follows FK chain (`download_log` → `candidate_evidence_id`, fallback to import_job FK). No `_candidate_owners` helper. `_preview_for_triage` re-measurement path becomes cold (only fires for evidence-less legacy rows). |
| Auto-import fallback (`lib/download.py:1200`) | Migrated off `run_preimport_gates` to direct `measure_preimport_state` + `preimport_decide`. |
| Non-worker preview path (`lib/import_preview.py:977`) | Same migration. |
| `lib/preimport.py` | `run_preimport_gates`, `_legacy_preimport_decision`, `_apply_legacy_denylist_side_effects` deleted. Public surface shrinks to `measure_preimport_state` + `preimport_decide`. |
| `tests/fakes.py` `FakePipelineDB` | Evidence dict re-keyed to `(mb_release_id, snapshot_fingerprint)`. New stub methods for `find_album_quality_evidence`, `load_album_quality_evidence_by_id`. Setters for addressing FK columns. |
| `tests/helpers.py` | New `make_evidence(mb_release_id, files, **measurement_overrides)` builder. |
| Web UI / API | None directly. Triage backend changes are wire-compatible with the existing `decide_wrong_match_cleanup` return shape. |
| `pipeline-cli` | None. |
| Operational | First deploy will cause the preview worker to re-measure some number of in-flight `import_jobs` on its next cycle. Expected cost: bounded (the queue size at any moment). No risk of stuck jobs because of the `preview_status='waiting'` reset. |

---

## Implementation Units

Each unit is dependency-ordered and lands as one logical commit. U-IDs are stable.

### U1. Snapshot fingerprint helper

**Goal:** Pure function `snapshot_fingerprint(files: list[AlbumQualityEvidenceFile]) -> str` deterministic over `(relative_path, size_bytes, extension, container, codec)`, sorted by `relative_path`, JSON-encoded with stable key order, SHA-256 hex digest. Plus a thin wrapper for "compute from disk path" that combines `snapshot_audio_files(path)` + `snapshot_fingerprint(files)`.

**Requirements:** Foundation for D. Doesn't change any caller yet.

**Dependencies:** None.

**Files:**
- `lib/quality_evidence.py` (add `snapshot_fingerprint` next to `snapshot_audio_files` around line 149)
- `tests/test_quality_evidence_fingerprint.py` (new)

**Approach:** Pure function. No DB. Mirror existing `_snapshot_match_key` field choice so freshness and identity stay coherent.

**Execution note:** Test-first. The fingerprint contract is load-bearing for the migration; pin determinism (same inputs → same hash across runs, sorted-order independence on input) before any caller depends on it.

**Patterns to follow:** Other pure helpers in `lib/quality_evidence.py` (e.g., `derive_folder_layout`, `derive_filetype_band`).

**Test scenarios:**
- Two identical file lists in different input order produce the same fingerprint.
- Changing any of `relative_path`, `size_bytes`, `extension`, `container`, `codec` changes the fingerprint.
- Changing `mtime_ns` does NOT change the fingerprint (regression guard against re-introducing mtime).
- `null` `codec` value is handled consistently (some files have null codec per the 017 schema) — equal-with-null is equal.
- Empty file list produces a stable, defined fingerprint (not an error, not the same as a single-file list).
- "Compute from path" wrapper agrees with "compute from already-snapshotted files" for the same directory.

**Verification:** Unit tests above pass. No caller changes yet; suite remains green.

---

### U2. Schema migration — backfill and re-key in place

**Goal:** Land migration `migrations/021_evidence_canonical_rekey.sql` that re-keys `album_quality_evidence` from `(owner_type, owner_id)` to `(mb_release_id, snapshot_fingerprint)` **without deleting any rows whose files haven't changed**. The migration runs in a single transaction and performs these steps in order:

1. **Add new columns (nullable initially, so backfill can populate them):**
   - `album_quality_evidence`: `mb_release_id TEXT`, `snapshot_fingerprint TEXT`, `source_path TEXT`. (NOT NULL constraints applied at the end after backfill.)
   - `import_jobs`: `candidate_evidence_id BIGINT REFERENCES album_quality_evidence(id) ON DELETE SET NULL`.
   - `download_log`: `candidate_evidence_id BIGINT REFERENCES album_quality_evidence(id) ON DELETE SET NULL`.
   - `album_requests`: `current_evidence_id BIGINT REFERENCES album_quality_evidence(id) ON DELETE SET NULL`.

2. **Backfill `mb_release_id` and `source_path`** on each existing evidence row by JOINing on the soon-to-be-dropped owner columns:
   - `owner_type='import_job_candidate'` → `mb_release_id` from `import_jobs.mb_release_id`, `source_path` from the import_job's staged path (column `source_path` already exists on import_jobs).
   - `owner_type='download_log_candidate'` → `mb_release_id` from `download_log.request_id → album_requests.mb_release_id`, `source_path` from `download_log.staged_path` (or equivalent).
   - `owner_type='request_current'` → `mb_release_id` from `album_requests.mb_release_id`, `source_path` from the beets library root for that album.
   - If any owner row resolves to a NULL `mb_release_id` (genuinely unmatched request), DELETE that orphan evidence row. These are *not* "files-changed deletions" — they're legacy rows that never had a release to address; logging the deletion in a one-off `EXCEPTION` log line is appropriate.

3. **Backfill `snapshot_fingerprint`** by computing the SHA-256 over `(relative_path, size_bytes, extension, container, codec)` tuples from each evidence row's `album_quality_evidence_files` records, JSON-encoded with sorted keys. Done inside the migration via a stored function or a `WITH` CTE — the fingerprint formula is deterministic enough to express in SQL using `digest()` from `pgcrypto` over a `string_agg(... ORDER BY relative_path)` of the per-file tuples.

4. **Dedupe rows that collapse under the new key.** For each `(mb_release_id, snapshot_fingerprint)` pair with more than one row, keep the row with the most recent `measured_at` and DELETE the rest. Before deleting, capture each duplicate row's id so the addressing-FK backfill (next step) can map old owner references to the canonical survivor's id.

5. **Backfill addressing FKs:**
   - `UPDATE import_jobs SET candidate_evidence_id = e.id FROM album_quality_evidence e WHERE e.owner_type='import_job_candidate' AND e.owner_id = import_jobs.id` — mapped through the dedupe table when the chosen evidence row was a duplicate (so the FK points at the surviving canonical row).
   - `UPDATE download_log SET candidate_evidence_id = e.id FROM album_quality_evidence e WHERE e.owner_type='download_log_candidate' AND e.owner_id = download_log.id`.
   - **Cross-walk for download_log rows with no direct ownership:** also `UPDATE download_log SET candidate_evidence_id = ij.candidate_evidence_id FROM import_jobs ij WHERE download_log.candidate_evidence_id IS NULL AND ij.request_id = download_log.request_id AND ij.candidate_evidence_id IS NOT NULL` — pick most recent import_job per download_log's request. This is the line that prevents first-week triage from hitting `_preview_for_triage` on every historical reject.
   - `UPDATE album_requests SET current_evidence_id = e.id FROM album_quality_evidence e WHERE e.owner_type='request_current' AND e.owner_id = album_requests.id`.

6. **Tighten constraints and drop the old keying:**
   - `ALTER TABLE album_quality_evidence ALTER COLUMN mb_release_id SET NOT NULL, ALTER COLUMN snapshot_fingerprint SET NOT NULL, ALTER COLUMN source_path SET NOT NULL`.
   - Add `CHECK (length(mb_release_id) > 0)` to reject the empty-string-not-NULL pathology.
   - `DROP INDEX idx_album_quality_evidence_owner`.
   - `ALTER TABLE album_quality_evidence DROP CONSTRAINT album_quality_evidence_one_per_owner`.
   - `ALTER TABLE album_quality_evidence DROP COLUMN owner_type, DROP COLUMN owner_id`.
   - Add `UNIQUE (mb_release_id, snapshot_fingerprint)` and `INDEX (mb_release_id)` for prefix lookups.

**Requirements:** D depends on this. U2 and U3 land in **one atomic commit** — the dropped columns and the code referencing them must change together so the suite stays green at every commit boundary.

**Dependencies:** U1 (the fingerprint helper exists in code; the migration's SQL implementation of the same hash must match U1's helper exactly — verified by a cross-check test that hashes a known fixture both ways and compares).

**Files:**
- `migrations/021_evidence_canonical_rekey.sql` (new)
- `tests/test_migrator.py` (regression — the migrator picks up the new file and applies it on a fresh test DB)
- `tests/test_evidence_rekey_migration.py` (new — fixture-based test: seed an old-shape DB with known rows, run migrate, assert post-state)

**Approach:** Single transaction. No `IF NOT EXISTS`. Uses `pgcrypto` for the SHA-256 fingerprint computation (verify it's already enabled on the doc2 cluster; if not, `CREATE EXTENSION IF NOT EXISTS pgcrypto` at migration top). Pre-deploy `pg_dump` is captured in the deploy checklist, not in the migration itself.

**Patterns to follow:** `migrations/017_album_quality_evidence.sql` (table shape conventions). For the in-SQL fingerprint computation, the closest pattern in the repo is — none directly comparable; the implementer should write a small `CREATE TEMPORARY FUNCTION` or a CTE that emits the same JSON-encoded tuple list U1 emits in Python.

**Test scenarios (in `tests/test_evidence_rekey_migration.py`):**
- Seeded DB has one `import_job_candidate:42` row with a single file `(track01.flac, 12345678, flac, flac, flac)`. After migration: that row has `mb_release_id` matching the import_job, `snapshot_fingerprint` matching `snapshot_fingerprint([file])` computed in Python (cross-check between SQL and Python implementations).
- Seeded DB has both an `import_job_candidate:J` row and a `download_log_candidate:D` row for the same audio (PR #256 duplicate scenario). After migration: one row survives, both `import_jobs.J.candidate_evidence_id` and `download_log.D.candidate_evidence_id` point at it.
- Seeded DB has a `download_log` with no direct ownership but a sibling `import_job` with evidence. After migration: `download_log.candidate_evidence_id` is populated via the cross-walk, pointing at the sibling's evidence row.
- Seeded DB has an evidence row whose owner resolves to a NULL `mb_release_id` (orphan — request never matched). After migration: that orphan row is deleted (the migration's exception logging captures it).
- Migration is idempotent — a second run is a no-op (`schema_migrations` already has 021).
- After the migration, `SELECT column_name FROM information_schema.columns WHERE table_name='album_quality_evidence' AND column_name IN ('owner_type','owner_id')` returns zero rows.
- After the migration, every existing `import_jobs.candidate_evidence_id` is non-NULL when the job had any evidence at all; every existing `download_log.candidate_evidence_id` is non-NULL when *any* path (direct or cross-walked) addressed evidence.
- The SHA-256 computed by the in-SQL fingerprint matches the SHA-256 computed by the Python helper from U1, for at least three different file-list shapes (single file, multiple files, file with NULL codec).

**Verification:** `pytest tests/test_migrator.py tests/test_evidence_rekey_migration.py -v` green. Before deploy, run against a pg_dump restore of prod and confirm row counts: `album_quality_evidence` row count is preserved minus the orphan-drop count (which should be a small number, logged); `import_jobs.candidate_evidence_id` is non-NULL for the same set of jobs that had evidence before.

---

### U3. Code: evidence reader/writer + dataclasses + fakes match new schema

> **Lands in the same commit as U2.** The schema migration in U2 drops `owner_type`/`owner_id`; code in this unit stops referencing them. Splitting them across commits leaves an intermediate state where the test suite cannot run.

**Goal:** Bring `lib/quality_evidence.py`, `lib/pipeline_db.py`, `lib/import_evidence.py`, and `tests/fakes.py`/`tests/helpers.py` in sync with the new schema. After this unit:
- `AlbumQualityEvidence` carries `mb_release_id: str`, `snapshot_fingerprint: str`, `source_path: str`. Drops `owner: AlbumQualityEvidenceOwner`.
- `AlbumQualityEvidenceOwner`, the `OWNER_*` constants, and `_candidate_owners` are deleted.
- `PipelineDB.upsert_album_quality_evidence(evidence)` upserts on `(mb_release_id, snapshot_fingerprint)`.
- `PipelineDB.load_album_quality_evidence_by_id(evidence_id)` and `PipelineDB.find_album_quality_evidence(mb_release_id, snapshot_fingerprint)` are the read paths.
- `PipelineDB.set_import_job_candidate_evidence(import_job_id, evidence_id)`, `set_download_log_candidate_evidence(download_log_id, evidence_id)`, `set_request_current_evidence(request_id, evidence_id)` write the addressing FKs.
- `persist_candidate_evidence_from_measurement` and friends compute fingerprint and write FK back to the addressing entity in the same call.
- `ensure_candidate_evidence_for_action(source_path, download_log_id=None, import_job_id=None)` follows FK chain instead of owner lookup.
- `FakePipelineDB` matches: dict keyed by `(mb_release_id, snapshot_fingerprint)`, FK columns on its in-memory request/import_job/download_log rows.

**Requirements:** D done. A done (the new `ensure_candidate_evidence_for_action` is owner-agnostic by construction).

**Dependencies:** U1, U2.

**Files:**
- `lib/quality_evidence.py`
- `lib/pipeline_db.py`
- `lib/import_evidence.py`
- `tests/fakes.py` (FakePipelineDB)
- `tests/helpers.py` (new `make_evidence` builder)
- `tests/test_fakes.py` (self-test for the new shape)
- `tests/test_quality_evidence.py` (existing tests migrated off `AlbumQualityEvidenceOwner`)
- `tests/test_pipeline_db.py` (writer/reader tests, if present in current shape)

**Approach:** Replace `AlbumQualityEvidenceOwner` parameters with `(mb_release_id, snapshot_fingerprint)` parameters or with `evidence_id` for FK-keyed lookups. The `persist_*` helpers gain a "compute fingerprint, upsert by (mb_release_id, fingerprint), write FK to addressing entity" sequence. Where today there's a polymorphic owner dispatch, after this there's just direct SQL keyed on the new columns.

**Patterns to follow:** Existing writer shape in `lib/quality_evidence.py:432–498` for the per-call upsert. `FakePipelineDB.upsert_album_quality_evidence` at `tests/fakes.py:1818` for the in-memory mirror.

**Test scenarios:**
- `FakePipelineDB`: upserting the same `(mb_release_id, snapshot_fingerprint)` twice produces one row (uniqueness invariant). Upserting different fingerprints produces two rows under the same mb_release_id.
- `FakePipelineDB.set_import_job_candidate_evidence` writes the FK; `import_job` row's `candidate_evidence_id` field reflects it.
- `tests.test_fakes` exercises every new stub end-to-end including the FK setters.
- `persist_candidate_evidence_from_measurement` writes the FK on the import_job in the same call (assert the FK is set after persist returns).
- `ensure_candidate_evidence_for_action(source_path, import_job_id=X)` returns the evidence row when the FK is set on the import_job and `audio_snapshot_matches` confirms files haven't changed.
- `ensure_candidate_evidence_for_action(source_path, download_log_id=Y)` returns the evidence row when `download_log.candidate_evidence_id` is set.
- `ensure_candidate_evidence_for_action(source_path, download_log_id=Y)` falls back to the import_job FK when `download_log.candidate_evidence_id` is NULL but `download_log.import_job_id` is set.
- Mocks are production-shaped (no synthetic str-int dicts that pass pyright but fail on real DictRow shape; per the contract-test rule in code-quality.md).

**Verification:** Full quality_evidence and fakes test modules pass. Pyright clean for all touched files.

---

### U4. (A) Wrong-match triage uses the FK chain

**Goal:** `decide_wrong_match_cleanup` (`lib/wrong_match_cleanup_decision.py`) and its helpers in `lib/wrong_match_triage.py` look up candidate evidence by following the FK chain. The chain has three rungs:

1. `download_log.candidate_evidence_id` (populated by the migration backfill for historical rows; populated by `_handle_rejected_result` for fresh rejects via U5's FK-copy).
2. If NULL: JOIN `download_log.request_id → import_jobs.request_id`, pick the most recent import_job for that request that still has `candidate_evidence_id IS NOT NULL`, use its FK. (`download_log` has no direct `import_job_id` column.)
3. If still NULL: fall back to `_preview_for_triage` (re-measure). Log the row id at WARN level — post-migration this should be near-zero traffic.

**Requirements:** Resolves (A) — owner-agnostic triage lookup.

**Dependencies:** U3.

**Files:**
- `lib/wrong_match_cleanup_decision.py`
- `lib/wrong_match_triage.py`
- `lib/import_evidence.py` (the FK-chain logic lives in `ensure_candidate_evidence_for_action`; this unit completes its implementation)
- `tests/test_wrong_match_cleanup_decision.py`
- `tests/test_wrong_match_triage.py`
- `tests/test_integration_slices.py` (add or update the triage-on-rejected-import slice)

**Approach:** `ensure_candidate_evidence_for_action(source_path, download_log_id=D)` is the one entry point. It internally walks the FK chain above. The triage caller's code shrinks. No new "find related import_job" join in the triage code itself — that responsibility lives inside `ensure_candidate_evidence_for_action`. The cross-walk JOIN (`download_log.request_id → import_jobs.request_id`) uses `ORDER BY import_jobs.created_at DESC LIMIT 1` to pick the most recent import_job with addressable evidence; this matches what the U2 migration backfill does, so live behavior and migrated state stay coherent.

**Patterns to follow:** Existing call site shape at `lib/wrong_match_cleanup_decision.py:103-121`.

**Test scenarios:**
- Triage on a download_log where `download_log.candidate_evidence_id` is set: returns the existing evidence; `measure_preimport_state` is NOT called (patched mock asserts zero calls); `_preview_for_triage` is NOT called.
- Triage on a download_log where `download_log.candidate_evidence_id` is NULL but a sibling `import_job` (same `request_id`) has `candidate_evidence_id` set: returns the sibling's evidence row; `measure_preimport_state` is NOT called.
- Multiple sibling import_jobs for the same request: the most recent (`created_at DESC`) one with non-NULL `candidate_evidence_id` wins.
- Triage on a download_log with no FK chain at all (genuinely evidence-less legacy row): falls back to `_preview_for_triage`, logs a structured WARN identifying the row id, increments a counter the operator can monitor (no new metric library — just a structured log line that's grep-able).
- Triage on a download_log whose evidence row's files no longer match what's on disk (`audio_snapshot_matches=False`): re-measures (existing behavior — files genuinely changed; this is the *only* mantra-aligned re-measurement path).
- Integration slice: rejected-import → triage path runs end-to-end with `FakePipelineDB`; the test patches `lib.preimport.measure_preimport_state` and asserts the mock was called exactly zero times on the happy path.

**Verification:** Suite green. On a live deploy, watch the `cratedigger-web` logs for the first triage action on a `high_distance` reject — `_preview_for_triage` and `measure_preimport_state` should not fire because the migration backfilled `download_log.candidate_evidence_id`.

---

### U5. (B) Delete `_persist_candidate_evidence_for_reject`

**Goal:** Remove `_persist_candidate_evidence_for_reject` (`lib/download.py:1558-1617`) and its call site in `_handle_rejected_result` (`lib/download.py:1672` plus the comment block 1660-1671). The reject handler stops measuring. It may, optionally, copy the import_job's `candidate_evidence_id` to the download_log row so the triage FK is set without a join — keep this as a tiny `set_download_log_candidate_evidence` call inside `_handle_rejected_result`, not a measurement.

**Requirements:** Resolves (B) — the dead-code helper introduced by PR #256 is removed.

**Dependencies:** U4 (the triage FK chain handles the case even when the download_log FK is NULL).

**Files:**
- `lib/download.py`
- `tests/test_integration_slices.py` (the existing post-#256 slice at lines ~7091-7242 updates its expectation: one evidence row, not two)
- `tests/test_download.py` (any unit test directly exercising `_handle_rejected_result`)

**Approach:** Delete the helper. Delete the call. Add the tiny FK-copy line (or skip it entirely and rely on U4's import_job-fallback path — implementer's call after seeing the code, both shapes are valid).

**Test scenarios:**
- Rejected-import slice: after `_handle_rejected_result` returns, `SELECT COUNT(*) FROM album_quality_evidence WHERE mb_release_id = X AND snapshot_fingerprint = F` is exactly 1 (was 2 before the change, one keyed under each owner).
- Rejected-import slice: `lib.preimport.measure_preimport_state` is patched and asserted called zero times by the reject path (was called once before the change).
- Rejected-import slice: the denylist is not touched by the reject path. (Denylist additions are the responsibility of the *decision* path, not the reject path; the reject handler only persists state.)
- Triage on the just-rejected row still finds evidence via the FK chain — either directly (`download_log.candidate_evidence_id` set by U5's FK-copy) or through the cross-walk to the sibling import_job.

**Verification:** Suite green. After deploy, manual: a fresh `high_distance` reject in production produces one evidence row, not two, in `album_quality_evidence` (joined back via the FK).

---

### U6. (C) Migrate `lib/import_preview.py:977` off the shim

**Goal:** Replace the `run_preimport_gates` call at `lib/import_preview.py:977` with direct `measure_preimport_state` + `preimport_decide` calls. The caller takes over the three side effects the shim used to bundle: (1) persist the measurement as evidence, (2) write the addressing FK on the import_job (post-rekey), (3) when the decision is `bad_audio_hash` or `spectral_reject`, add the offending Soulseek username(s) to the denylist via `PipelineDB.add_curator_ban` / equivalent — replicating what `_apply_legacy_denylist_side_effects` currently does.

**Requirements:** Resolves part of (C). The non-worker preview path stops bundling decision with measurement.

**Dependencies:** U3 (uses the new evidence write path).

**Files:**
- `lib/import_preview.py`
- `tests/test_import_preview.py` (and any test using this call site)

**Approach:** Mirror the worker code path (the post-#254 pattern that already exists in `_preview_import_from_path_worker_mode` — research located it inline at the worker entry around lines 469–527). Pull the four-call sequence (`measure_preimport_state` → `preimport_decide` → `persist_candidate_evidence_from_measurement` → conditional denylist add) into a tiny shared helper if both U6 and U7 end up open-coding the exact same block; otherwise inline it at each site. The implementer makes the call based on duplication after both sites are migrated. *Do not* keep `_apply_legacy_denylist_side_effects` alive for U6/U7 to call — that defers the shim removal and is exactly the kind of "still alive in tests" trap PR #254 was. Inline the denylist call instead, using `PipelineDB.add_curator_ban` (or whatever method names the shim helper currently uses).

**Test scenarios:**
- A rejected preview from this code path writes evidence keyed by the new `(mb_release_id, snapshot_fingerprint)` shape and sets `import_jobs.candidate_evidence_id` for the source job.
- A rejected preview where `decision.reason == 'bad_audio_hash'` adds the candidate's downloading username(s) to the curator-ban denylist. Assert the denylist row exists in `FakePipelineDB` after the call.
- A rejected preview where `decision.reason == 'spectral_reject'` adds the username(s) to the denylist. Same assertion shape.
- An accepted preview does NOT touch the denylist.
- Equivalence test: feed the same fixture to (a) the old `run_preimport_gates` path (before U6 lands) and (b) the new direct-call path, and assert byte-equal `ImportResult` + identical denylist mutations + identical evidence-row contents. This is the regression guard that catches the same class of bug PR #254 caused.
- Pyright clean.

**Verification:** Direct unit tests pass. The full test suite stays green.

---

### U7. (C) Migrate `lib/download.py:1200` off the shim

**Goal:** Migrate the auto-import **evidence-missing recovery path** at `lib/download.py:1200` to the same direct-call shape as U6. This path is *not dead* — it's the fallback `_process_finalized_download` falls into when `candidate_evidence_available=False` (i.e. preview-worker evidence is missing at importer dispatch time). It runs rarely in production but it does run, so it has to keep working. Replace the `run_preimport_gates` call with the same four-call sequence: `measure_preimport_state` → `preimport_decide` → `persist_candidate_evidence_from_measurement` → conditional denylist add for `bad_audio_hash` / `spectral_reject`.

**Requirements:** Resolves the remaining call site of (C).

**Dependencies:** U3.

**Files:**
- `lib/download.py`
- `tests/test_download.py`
- `tests/test_force_import_gates.py`

**Approach:** Same shape as U6. If U6's implementation extracted a tiny shared helper, reuse it here. Verify that no production caller of `_process_finalized_download` *expects* the shim's exact return shape — `run_preimport_gates` returns a `PreImportGateResult`, the new direct calls return `(measurement, decision)` separately. The caller's handling of the result needs to adapt; this is in-scope for U7 and should be part of the test scenarios.

**Test scenarios:**
- Each test in `tests/test_download.py` currently decorated `@patch('lib.preimport.run_preimport_gates')` is migrated to either (a) patch the two new seams (`measure_preimport_state` + `preimport_decide`) or (b) lift to an integration-slice form using `FakePipelineDB`. Prefer (b) — the integration-slice form is the regression guard that the denylist side effects, the evidence-write, and the decision dispatch all land correctly.
- A rejected evidence-missing fallback adds the candidate's username(s) to the denylist when the decision is `bad_audio_hash` or `spectral_reject` (mirror U6's coverage).
- An accepted evidence-missing fallback persists evidence keyed by the new shape, sets `import_jobs.candidate_evidence_id`, and proceeds to import without touching the denylist.
- Same coverage in `tests/test_force_import_gates.py`.
- After migration, `grep -rn 'run_preimport_gates' tests/ lib/` returns zero matches (the function definition is still alive at this point — it's deleted in U8 — but no callers remain).

**Verification:** `grep -rn 'run_preimport_gates' tests/` returns zero matches. The full test suite stays green.

---

### U8. (C) Delete the shim

**Goal:** Delete `run_preimport_gates`, `_legacy_preimport_decision`, `_apply_legacy_denylist_side_effects` from `lib/preimport.py` (currently at lines 672-902). After this unit, `lib/preimport.py` exports `measure_preimport_state` and `preimport_decide` and nothing else of the shim trio.

**Requirements:** Closes (C).

**Dependencies:** U6, U7.

**Files:**
- `lib/preimport.py`
- `tests/test_preimport.py` (drop tests specific to the shim; keep tests that cover `measure_preimport_state` and `preimport_decide` independently)

**Approach:** Mechanical deletion after U6 and U7 leave no callers. Run `grep -rn 'run_preimport_gates\|_legacy_preimport_decision\|_apply_legacy_denylist_side_effects' lib/ scripts/ tests/` to confirm zero matches before deleting the bodies, and again after to confirm the deletion was clean.

**Test scenarios:** None new — this unit is a deletion with no behavior change. The post-deletion check is the equivalence proof (tests that previously covered the shim now cover the same behavior through the direct-call shape exercised in U6/U7).

**Verification:** `grep -rn 'run_preimport_gates' lib/ scripts/` returns zero matches. `pyright lib/preimport.py` clean. Suite green.

---

### U9. Verification + deploy

**Goal:** Run the issue's verification checklist against a fresh deploy and capture the result.

**Requirements:** All of A+B+C+D live and observed working.

**Dependencies:** U1–U8.

**Files:**
- None (verification is operational, not code).

**Approach:** Per `.claude/rules/deploy.md`: commit and push (one logical change per commit, in the unit order above), update `cratedigger-src` flake input on doc1, `nixos-rebuild switch` on doc2. The `cratedigger-db-migrate` oneshot runs migration 020 before any service starts. Verify migration applied with `pipeline-cli query "SELECT * FROM schema_migrations ORDER BY version DESC LIMIT 5"`.

**Test scenarios:** N/A (operational verification).

**Verification (live, in order):**
1. `ssh doc2 'pipeline-cli query "SELECT version FROM schema_migrations WHERE version=21"'` returns the row.
2. `ssh doc2 'pipeline-cli query "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '\''album_quality_evidence'\'' AND column_name IN ('\''owner_type'\'','\''owner_id'\'')"'` returns 0.
3. `ssh doc2 'pipeline-cli query "SELECT COUNT(*) FROM information_schema.columns WHERE table_name='\''import_jobs'\'' AND column_name='\''candidate_evidence_id'\''"'` returns 1. Same for `download_log.candidate_evidence_id` and `album_requests.current_evidence_id`.
4. `ssh doc2 'grep -rn "run_preimport_gates" /nix/store/*/lib/preimport.py 2>/dev/null'` returns no matches in the deployed code.
5. `ssh doc2 'grep -rn "_persist_candidate_evidence_for_reject" /nix/store/*/lib/download.py 2>/dev/null'` returns no matches.
6. **Backfill sanity:** `pipeline-cli query "SELECT COUNT(*) FROM import_jobs WHERE candidate_evidence_id IS NOT NULL"` is non-zero (the migration backfilled in-flight jobs). `pipeline-cli query "SELECT COUNT(*) FROM download_log WHERE candidate_evidence_id IS NOT NULL"` is non-zero. `pipeline-cli query "SELECT preview_status, COUNT(*) FROM import_jobs GROUP BY preview_status"` shows roughly the same distribution as before deploy — the backfill design means in-flight jobs do NOT flip to `waiting` (regression guard against the abandoned TRUNCATE design).
7. **In-flight requeue (negative check):** no `import_jobs` rows are stuck in `running` with NULL `candidate_evidence_id` after one full importer cycle. If any exist, they're either pre-deploy orphans flagged by the migration's logged drops, or a backfill bug — investigate before declaring deploy green.
8. **Triage doesn't re-measure:** trigger a fresh wrong-match triage from the web UI on a known `high_distance` reject row that existed *before* the deploy. Confirm in `cratedigger-web` logs that neither `_preview_for_triage` nor `measure_preimport_state` fires — the migration's backfill populated `download_log.candidate_evidence_id` for historical rows. Grep `journalctl -u cratedigger-web --since '1 hour ago'` for any "legacy-evidence-less" WARN entries; these should be rare or absent.
9. **Fresh reject produces one evidence row:** create a fresh download → reject cycle in production. After it lands, query `pipeline-cli query "SELECT mb_release_id, snapshot_fingerprint, COUNT(*) FROM album_quality_evidence WHERE mb_release_id = '<known release>' GROUP BY mb_release_id, snapshot_fingerprint HAVING COUNT(*) > 1"` and confirm zero duplicate-fingerprint rows.

---

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| The in-SQL `snapshot_fingerprint` computation in U2's migration drifts from the Python implementation in U1 | Medium | High (fingerprint mismatch means new worker writes produce a different fingerprint than the migrated row, breaking dedupe and lookup) | Cross-check test in `tests/test_evidence_rekey_migration.py` hashes the same fixture both ways and asserts equality. JSON encoding rules in both implementations must agree on key ordering, separators, NULL representation, and string escaping. Recommend the implementer write the Python helper first (U1), generate fixture digests, and use those as expected values for the SQL test. |
| `pgcrypto` extension not enabled on the doc2 cluster (needed for SHA-256 in SQL) | Low | Medium (migration fails on first run) | Migration top includes `CREATE EXTENSION IF NOT EXISTS pgcrypto`. Implementer verifies once on doc2 with `psql -c "SELECT * FROM pg_extension WHERE extname='pgcrypto'"` before submitting the migration. |
| Backfill deletes an orphan row whose addressing entity (request/import_job/download_log) someone is actively investigating | Very low | Low | The migration's exception log captures the deleted row's id and owner so it's recoverable from `pg_dump` if anyone asks. |
| Duplicate-collapse picks the wrong canonical row (e.g., a stale measurement instead of the freshest) | Low | Medium (decisions get made on stale data) | Dedupe rule is `ORDER BY measured_at DESC LIMIT 1` per `(mb_release_id, snapshot_fingerprint)`. The migration's test scenario seeds a known older + newer pair and asserts the newer one survives. |
| Snapshot fingerprint collides for two genuinely different audio files (false positive) | Negligible | High if it happened | SHA-256 over `(relative_path, size_bytes, extension, container, codec)`. Two different rips of the same album differ in at least one of those for at least one file (different compressed size for FLAC, different codec for FLAC vs V0). Bit-identical files collapsing is the *correct* outcome. |
| A non-worker preview-path caller is missed during U6 migration | Low | Medium (worker path still works; shim is just lingering) | Pyright catches removed symbol references after U8. Final `grep -rn 'run_preimport_gates' lib/ scripts/ tests/` in U8 is the explicit closing check. |
| Tests using `@patch('lib.preimport.run_preimport_gates')` accidentally migrate to a shape that mocks the wrong layer | Medium | Medium (shipped bug, missed regression) | Prefer integration-slice migration over mock-swap in U6 and U7. The decision functions in `lib.quality`/`lib.preimport` are pure, cheap to call directly in tests, so the integration-slice form is the default. U6/U7 include explicit denylist-side-effect assertions to prevent the PR #254 class of bug. |
| Denylist side effects (bad_audio_hash, spectral_reject username adds) silently dropped when U8 deletes `_apply_legacy_denylist_side_effects` because U6/U7 didn't inline them | Medium | High (real abuse-prevention semantics drop, hard to spot in tests that don't assert denylist state) | U6 and U7 test scenarios assert the denylist row exists after rejection. Add to the U8 checklist: before deletion, grep `_apply_legacy_denylist_side_effects` callers — must be zero. |
| Pre-deploy `pg_dump` is forgotten and the migration is hard to revert | Low | High | Per `.claude/rules/deploy.md`, this is part of the deploy checklist. The plan's U9 deploy step explicitly captures the `pg_dump` command before `nixos-rebuild switch`. The backfill design also means revert is easier than the TRUNCATE design would have been — old `owner_type`/`owner_id` data is reconstructable from the addressing FKs by inversion. |
| Issue 257 says `lib/import_preview.py:553` but research confirmed the call site is now at line 977 — the issue's line number was stale | Resolved | N/A | Plan uses the verified line number (977). Implementer should still grep `run_preimport_gates` at U6 start to catch any further drift. |

---

## Deferred Implementation Notes

- The exact line numbers in `lib/download.py` (1200, 1558-1617, 1672-1679) and `lib/preimport.py` (672-786, 803-854, 857-902) and `lib/import_preview.py` (977) are pinned from research at planning time. Verify with `grep` at the start of each unit — they may drift slightly if other PRs land first.
- **Pre-deploy sizing check.** Before merging U2+U3, run `ssh doc2 'pipeline-cli query "SELECT COUNT(*) FROM album_quality_evidence; SELECT preview_status, COUNT(*) FROM import_jobs GROUP BY preview_status"'` to size the backfill. Hundreds of rows: migration runs in seconds. Tens of thousands: still seconds (the per-row work is a small SHA-256 and a JOIN), but the implementer should sanity-check the migration duration with `EXPLAIN ANALYZE` on a prod restore before deploy.
- **`pgcrypto` extension check.** Run `ssh doc2 'pipeline-cli query "SELECT * FROM pg_extension WHERE extname='\''pgcrypto'\''"'` once before writing U2. If not present, the migration's first statement is `CREATE EXTENSION IF NOT EXISTS pgcrypto` and the deploy needs to confirm the cratedigger DB role has the privilege (likely yes since the role owns the DB).
- Whether the FK-copy line in U5 (copying `import_jobs.candidate_evidence_id` to `download_log.candidate_evidence_id`) is necessary depends on whether U4's cross-walk path is sufficient for every triage call site. Implementer decides after seeing the actual code paths. Either shape is acceptable; the test scenarios in U4/U5 cover both.
- The exact handling of `request_current_owner` callers (if any remain after U3 is done) — should those rewrite to use `album_requests.current_evidence_id` directly, or should there be a thin `ensure_current_evidence_for_request(request_id)` helper that does the FK write + upsert in one call? Implementer's call.
- Whether to delete `_preview_for_triage` entirely or keep it as the cold-path fallback for legacy-evidence-less rows. The plan keeps it as a last-resort fallback per U4's design. Post-deploy, watch the "legacy-evidence-less" WARN logs for a week; if zero hits, a follow-up plan can delete it.
- The decision on whether to extract a tiny shared helper (`run_measurement_and_decide`) for the four-call sequence used by U6 and U7 is deferred to whichever unit lands second. If both sites end up open-coding the identical block, extract.

---

## Origin requirements trace

This plan is sourced from issue #257 directly (no upstream `*-requirements.md`). The issue's verification section maps to U9:

| Issue verification item | Where met |
|-------------------------|-----------|
| `grep -rn 'run_preimport_gates' lib/ scripts/` returns zero matches | U8 verification + U9 step 4 |
| `grep -rn '_persist_candidate_evidence_for_reject' lib/ scripts/` returns zero matches | U5 verification + U9 step 5 |
| For each high_distance reject, the count of evidence rows for the related album is exactly one | U5 test scenarios + U9 step 8 |
| First-time Wrong Matches triage on a reject row is instant (no `_preview_for_triage` fallback fires) | U4 test scenarios + U9 step 7 |

The issue's "Optional cleanup (D)" is in scope (user-directed: "do the optional cleanup wtf. not optional"), folded into U1–U3 + U9 verification steps 2–3.

# Pipeline DB Schema (key fields + JSONB audit blobs)

The pipeline DB is PostgreSQL. DSN: `10.20.0.11:5432/cratedigger`. Access via `pipeline-cli` on doc2, or from doc1 via `ssh doc2 'pipeline-cli ...'`.

Full schema lives in `migrations/*.sql`. This doc covers the fields that appear in debugging and the JSONB audit blobs.

## `album_quality_evidence` — active quality evidence

Active reusable album-quality evidence is stored relationally, not in JSONB.
Evidence is **content-addressed**: identity is `(mb_release_id,
snapshot_fingerprint)`. Addressing entities reference evidence rows via FK
columns (`import_jobs.candidate_evidence_id`,
`download_log.candidate_evidence_id`, `album_requests.current_evidence_id`).
The same audio collapses into one canonical row regardless of how many
addressing entities point at it; differing file inventories produce
different fingerprints under the same release id.

Key fields:

- `mb_release_id TEXT NOT NULL` — the MusicBrainz release this evidence
  describes.
- `snapshot_fingerprint TEXT NOT NULL` — SHA-256 over the per-file tuple
  `(relative_path, size_bytes, extension, container, codec)`, sorted by
  `relative_path`, JSON-encoded with stable key order. Computed by
  `lib.quality_evidence.snapshot_fingerprint`.
- `source_path TEXT NOT NULL` — the on-disk root where measurement
  happened.
- `UNIQUE (mb_release_id, snapshot_fingerprint)` plus
  `INDEX (mb_release_id)` for prefix lookups.
- `measured_at TIMESTAMPTZ` — when this evidence snapshot was measured.
- `codec`, `container`, `storage_format` — measured source/storage facts.
  For lineage-v3 rows, `storage_format` is the same bare codec label as
  `format`; bitrate/profile labels never live in either field.
- `target_format` — projected target policy from the typed import contract,
  independent of the measured source. It may be NULL.
- `target_is_cbr` — album-wide bitrate mode measured from the projected
  target/probe files. It belongs to the target contract, independently of
  both the downloaded source and materialized output measurements. A bare
  `MP3` target is incomplete without this explicit mode. Measurement-only
  rows for facts rejected before target policy is consulted leave both
  `target_format` and `target_is_cbr` NULL rather than guessing a mode.
- `lineage_version SMALLINT` — `1` marks historical rows whose storage/target
  projection is ambiguous; `3` marks separated source and target facts.
  Migration 050 marks all pre-existing rows as version 1, then changes the
  column default to 3 so an omitted future insert cannot silently create
  legacy evidence. New typed writers also persist version 3 explicitly.
- `min_bitrate_kbps`, `avg_bitrate_kbps`, `median_bitrate_kbps`, `format`,
  `is_cbr`, `spectral_grade`, `spectral_bitrate_kbps`,
  `was_converted_from` — the wrapped `AudioQualityMeasurement` facts.
- `audio_corrupt BOOLEAN`, `folder_layout TEXT` (`flat` | `nested`),
  `audio_file_count INTEGER`, `filetype_band TEXT`,
  `matched_bad_audio_hash_id`, `matched_bad_audio_hash_path` — the four
  folder/audio-integrity facts the importer's
  `full_pipeline_decision_from_evidence` reads as early-exit reject
  branches (U11).
- `v0_min_bitrate_kbps`, `v0_avg_bitrate_kbps`,
  `v0_median_bitrate_kbps`, `v0_source_lineage`,
  `v0_source_provenance`, `v0_proof_provenance` — neutral V0 metric and
  provenance. Legacy policy-shaped probe kinds are rejected here.
- `verified_lossless BOOLEAN` plus `verified_lossless_proof_origin`,
  `verified_lossless_source`, `verified_lossless_classifier`,
  `verified_lossless_detail` — proof provenance is present only when the
  boolean is true.

## `album_quality_evidence_files` — snapshot guard rows

Each active evidence row owns typed file-snapshot rows:

- `evidence_id BIGINT` — FK to `album_quality_evidence(id) ON DELETE CASCADE`.
- `ordinal INTEGER` and `relative_path TEXT` — stable sorted snapshot order.
- `size_bytes BIGINT`, `mtime_ns BIGINT`, `extension TEXT`,
  `container TEXT`, `codec TEXT` — file identity and container facts used to
  decide whether cached evidence is still valid.

Action provenance such as reused/recomputed/backfilled/fallback outcomes is not
stored in these evidence tables; preview/import/cleanup result surfaces own
that audit trail.

## `album_requests` — quality-tracking fields

- `status TEXT` — active vocabulary: `wanted`, `downloading`, `imported`,
  `manual`; terminal audit vocabulary: `replaced`. Ordinary transitions are
  fail-closed and use SQL compare-and-set against the exact observed/declared
  source status. `replaced` has no outgoing edge and is created only by the
  one-way `supersede_request_mbid` transaction.

  The explicit transition graph has 13 edges: `wanted → downloading/manual/
  imported`; `downloading → wanted/manual/imported`; `imported → wanted/manual/
  imported`; `manual → wanted/manual/imported`; and `wanted → wanted`.
  Status-only self-transitions for `wanted`, `imported`, and `manual` are true
  no-ops: they do not change `updated_at` or any other byte. There is no
  `downloading → downloading` edge because acquiring download ownership must
  remain an explicit compare-and-set operation.

  Once a row becomes `replaced`, its lifecycle, retry counters, scheduler
  fields, active download metadata, evidence pointer, and active search-plan
  pointer are frozen. Late workers use exact-status compare-and-set writes and
  stop when ownership has changed. A completed search may still append a stale
  forensic `search_log` row (`stale_reason='request_replaced'`), but it cannot
  advance the ancestor cursor or backoff. Search-plan generation, supersession,
  and manual cursor advance reject the replaced ancestor.
- `search_filetype_override TEXT` — transient CSV filetype list (e.g. `"lossless,mp3 v0,mp3 320"` or just `"lossless"`). Overrides global `allowed_filetypes` for search. Set by quality gate requeue paths and backfill. Cleared on quality gate accept. The `"lossless"` virtual tier matches FLAC, ALAC, and WAV.
- `target_format TEXT` — persistent user intent for desired format on disk (`"lossless"` or NULL). Set only by user action (CLI/web set-intent toggle). Never cleared by quality gate. When set, keeps lossless on disk (normalizes ALAC/WAV → FLAC) instead of converting to V0/target.
- `min_bitrate INTEGER` — current min track bitrate in kbps (from beets).
- `prev_min_bitrate INTEGER` — previous min_bitrate before last upgrade. Shows delta in UI.
- `verified_lossless BOOLEAN` — True only when imported from a spectral-verified genuine lossless source. Suspect lossless-container imports stay false even when they are accepted provisionally.
- `last_download_spectral_grade TEXT` — spectral grade of the most recent download attempt.
- `last_download_spectral_bitrate INTEGER` — estimated bitrate from the most recent download's spectral analysis.
- `current_spectral_grade TEXT` — spectral grade of files currently on disk in beets.
- `current_spectral_bitrate INTEGER` — spectral estimated bitrate of files currently on disk. NULL for genuine files (no cliff). Quality gate uses this for gate_bitrate.
- `current_lossless_source_v0_probe_min_bitrate INTEGER` — min track bitrate of the current comparable V0 probe produced from an accepted lossless-container source.
- `current_lossless_source_v0_probe_avg_bitrate INTEGER` — avg track bitrate of the current comparable lossless-source V0 probe. Suspect lossless-source grind-up compares against this value.
- `current_lossless_source_v0_probe_median_bitrate INTEGER` — median track bitrate of the current comparable lossless-source V0 probe. Stored for audit and future policy, not used by v1 decisions.
- `active_download_state JSONB` — persisted download state for async polling (filetype, enqueued_at, per-file username/filename/size). Set by `set_downloading()`, cleared on completion/timeout.

## `download_log` — quality-tracking fields

- `slskd_filetype TEXT` — the captured source filetype (`"flac"`, `"mp3"`)
  used to build the downloaded-quality label. Peer-advertised bitrate is not
  stored; quality decisions and displays use measured evidence.
- `actual_filetype TEXT` — what's on disk after download/conversion.
- `spectral_grade TEXT` — spectral analysis of the downloaded files.
- `spectral_bitrate INTEGER` — estimated original bitrate from spectral.
- `existing_min_bitrate INTEGER` — beets min bitrate before this download.
- `existing_spectral_bitrate INTEGER` — spectral estimate of existing files before download.
- `v0_probe_kind TEXT` — lineage for this attempt's optional V0 probe evidence. V0 probes run on every candidate and are operator-facing across the UI (Recents strip/detail, Wrong Matches; research kinds render qualified — "(from lossy)" / "(on-disk re-encode)"). Only `lossless_source_v0` is comparable for the provisional-lossless policy lane; `native_lossy_research_v0` and `on_disk_research_v0` are real V0-transcode research measurements excluded from that lane.
- `v0_probe_min_bitrate INTEGER`, `v0_probe_avg_bitrate INTEGER`, `v0_probe_median_bitrate INTEGER` — min/avg/median track bitrates for this attempt's probe.
- `existing_v0_probe_kind TEXT` — lineage of the comparable probe state used before this attempt, when present.
- `existing_v0_probe_min_bitrate INTEGER`, `existing_v0_probe_avg_bitrate INTEGER`, `existing_v0_probe_median_bitrate INTEGER` — point-in-time baseline probe values used for history rendering and audit.
- `outcome TEXT` — CHECK-constrained vocabulary: `success`, `rejected`, `failed`, `timeout`, `force_import`, `manual_import`, `curator_ban`, `measurement_failed`, `youtube_running`, `youtube_success`, `youtube_failed`. Migration 037 widened the constraint to admit the three `youtube_*` outcomes used by the YT rescue ingest worker.
- `source TEXT NOT NULL DEFAULT 'slskd'` — sourcing-channel discriminator added by migration 037. CHECK constraint admits `'slskd'` and `'youtube'`. The default backfilled every pre-037 row to `'slskd'` in one ALTER (no separate backfill script per the single-operator no-backfill-script rule). Consumers rendering `download_log` rows (`pipeline-cli show`, web routes' "recent attempts") use this column to distinguish channels.
- `youtube_metadata JSONB` — YT-specific audit payload added by migration 037. Nullable; populated only for `source='youtube'` rows. Typed at the read seam as `lib.youtube_ingest_service.YoutubeIngestMetadata: msgspec.Struct`. Carries `yt_url`, `browse_id`, `audio_playlist_id`, optional `expected_track_count`, `resolver_mapping_id`, `per_track_video_ids`, and terminal-state fields (`reason`, `stderr_excerpt`, `observed_track_count`).
- **Partial unique index `one_youtube_running_per_request` ON `download_log (request_id) WHERE source = 'youtube' AND outcome = 'youtube_running'`** — added by migration 037. Enforces idempotency at the DB layer: at most one in-flight YT rescue per `request_id` at any time. Application-level pre-insert checks would race; this index is atomic. Once the row transitions to a terminal `youtube_success` / `youtube_failed`, the index admits the next submission.

Interrupted request auto-import cleanup uses `outcome='failed'` with
`beets_scenario='abandoned_auto_import'` and a readable
`error_message`. This is an interruption audit row, not a source
rejection: cooldown lookback excludes this scenario, and the cleanup
does not write denylist, wrong-match, or bad-audio evidence. The audit
row and `downloading` to `wanted` reset are committed together only when
the request still owns the same `active_download_state.current_path`.

## `import_jobs` — shared importer queue

All beets-mutating import work is submitted to `import_jobs` and drained by
`cratedigger-importer`. Web force-import, web/manual import, automation
completed-download processing, and CLI force/manual import all share this table.

Key fields:

- `job_type TEXT` — `force_import`, `manual_import`, `automation_import`, or `youtube_import`.
- `status TEXT` — `queued`, `running`, `completed`, or `failed`.
- `request_id INTEGER` — the related `album_requests.id`.
- `dedupe_key TEXT` — active queue dedupe key. A partial unique index prevents
  duplicate queued/running jobs while allowing a later job after completion.
- `payload JSONB` — typed job input. Force/manual jobs carry `failed_path`;
  force jobs also carry `download_log_id` and optional `source_username`.
  YouTube jobs carry `staged_path`, `request_id`, `browse_id`, and
  `download_log_id`.
- `result JSONB`, `message`, `error` — terminal worker result visible to web
  and CLI callers.
- **Partial unique index `one_active_youtube_import_per_request` ON
  `import_jobs (request_id) WHERE job_type = 'youtube_import' AND status IN
  ('queued', 'running')`** — added by migration 038. Keeps the post-yt-dlp
  importer handoff request-scoped, so a second browse id cannot enqueue a
  parallel active YouTube import for the same request.
- `attempts`, `worker_id`, `started_at`, `heartbeat_at`, `completed_at` —
  claim and recovery metadata.
- `preview_status TEXT` — async readiness/audit stage: `waiting`, `running`,
  `evidence_ready`, legacy `would_import`, `confident_reject`, `uncertain`, or
  `error`. `evidence_ready` means candidate evidence exists for the final
  action-time check; it is not import authority. The legacy `would_import`
  token remains claimable for preview-disabled compatibility and old rows only.
  Workers must recompute the mutating decision from fresh current evidence plus
  snapshot-valid candidate evidence at import time. New jobs use `waiting` only
  when the async preview gate is enabled; preview-disabled or raw/default
  inserts are `would_import` immediately with
  `preview_message='Preview gate disabled'`.
- `preview_result JSONB`, `preview_message`, `preview_error` — durable
  no-mutation preview audit visible in Recents and CLI output. Stored verdicts
  are display/audit facts; they must not authorize import, cleanup, denylist, or
  request-current updates.
- `preview_attempts`, `preview_worker_id`, `preview_started_at`,
  `preview_heartbeat_at`, `preview_completed_at` — async preview claim and
  recovery metadata.
- `importable_at TIMESTAMPTZ` — set when preview produces `evidence_ready`, or
  at enqueue time when the preview gate is disabled; the serial importer claims
  queued jobs with `evidence_ready` and legacy `would_import`.

On importer startup, any pre-existing `running` job is treated as abandoned
state from a previous worker process, reset to `queued`, and retried
immediately. The importer also holds a DB advisory singleton lock while it
runs, so an accidentally-started second worker exits instead of requeueing a
live worker's job.

Covered job-backed terminal outcomes cross one DB transaction boundary. This
includes force/manual and validated automation dispatch outcomes, automation's
local `Completed` / `CompletionFailed` fallbacks, and request-backed preview
measurement failures. Their request transition (including retry-attempt
accounting), mandatory `download_log` audit, source denylist/cooldown writes,
and terminal `import_jobs` update commit together through the typed commands in
`lib/terminal_outcomes.py`. A request-transition conflict or job
compare-and-set conflict rolls back the entire bundle; callers must not perform
a second job finalization. Direct/no-job poller transitions retain their
existing non-queue behavior and are outside this job-owned transaction.

Async preview workers run outside the beets mutation lane. They claim queued
jobs with `preview_status='waiting'`, call the no-mutation import preview path,
persist candidate evidence when an owner exists, then either mark the job ready
for the final import-time check or fail the preview with audit details. This
lets spectral/measurement work run with tunable parallelism while beets writes
stay serial, without letting preview decisions become later mutation authority.

The preview gate is opt-in at deployment time. When disabled, no preview worker
is required for compatibility: `PipelineDB.enqueue_import_job()` and the schema
defaults both make jobs importable immediately as legacy `would_import` rows
with `preview_message='Preview gate disabled'`. Legacy completed/failed rows
from before async previews may also carry `would_import` so historical
terminal import history does not look like active preview backlog.
Rollback to pre-018 code requires queue reconciliation first: stop import
workers and reset queued or running `evidence_ready` rows to queued `waiting`
rows so old preview code recomputes them. Do not bulk-convert them to
`would_import`; that would restore preview-decision authority.
The Recents Imports endpoint lists only active `queued`/`running` jobs; terminal
`completed`/`failed` rows remain durable audit history and must not be rendered
as live queue work.

## `download_log.import_result` JSONB

`import_one.py` emits an `ImportResult` JSON blob (`__IMPORT_RESULT__` sentinel on stdout). Version 3 contains the downloaded `source_measurement`, the prior `current_measurement`, typed `target_quality_contract`, typed V0 probe evidence, the quality comparison, postflight verification (beets_id, path), the post-import `materialized_measurement`, and an attempt-local `spectral` audit. Source measurements always describe the downloaded bytes; a target such as `opus 128` is policy and V0 min/avg/median remain exclusively under `v0_probe`. `materialized_measurement` describes the bytes Beets actually stored. Historical v1/v2 rows are decoded only by `ImportResult`'s marked legacy projection (`legacy_projection_version`); new v3 rows never infer lineage from equality between values. Every import path (success, downgrade, transcode, provisional, suspect-lossless rejection, error, timeout, crash) logs to download_log.

```sql
SELECT import_result->>'decision',
       import_result->'source_measurement'->>'format',
       import_result->'target_quality_contract'->>'format',
       import_result->'comparison_basis'->>'new_metric',
       import_result->'v0_probe'->>'avg_bitrate_kbps',
       import_result->'materialized_measurement'->>'avg_bitrate_kbps',
       import_result->'materialized_measurement'->>'min_bitrate_kbps',
       import_result->'spectral'->>'grade',
       import_result->'spectral'->'per_track'->0->>'hf_deficit_db'
FROM download_log ORDER BY id DESC LIMIT 10;
```

## `download_log.validation_result` JSONB

`beets_validate()` returns a `ValidationResult` with the full candidate list from the harness. Every validation (success or rejection) stores this. Contains: all beets candidates with distance breakdown per component (album, artist, tracks, media, source, year...), full track lists per candidate, the item→track mapping (which local file matched which MB track), local file list, beets recommendation level, soulseek username, download folder, failed_path, denylisted users, corrupt files.

`validation_result.distance` and `validation_result.scenario` are the sole
writer inputs for the denormalized `download_log.beets_distance` and
`download_log.beets_scenario` query columns. `PipelineDB.log_download`
projects them centrally; writers must not pass the same values separately.
Payloads that genuinely omit those envelope keys, such as
`MeasurementFailure`, may supply explicit top-level metadata.

For `abandoned_auto_import` audit rows, `validation_result.failed_path`
points at the prefixed failed-import folder when a leftover staged
directory existed. A missing staged directory may produce the same audit
scenario without a `validation_result` body; `error_message` remains the
operator-facing reason.

```sql
-- Why was distance high?
SELECT validation_result->'candidates'->0->'distance_breakdown'
FROM download_log WHERE id = <id>;

-- Which local file matched which MB track?
SELECT m->'item'->>'path', m->'item'->>'title', m->'track'->>'title'
FROM download_log, jsonb_array_elements(validation_result->'candidates'->0->'mapping') AS m
WHERE id = <id>;
```

## `slskd_transfer_ledger` — transfer ownership and file evidence

Migration 045 creates one write-ahead row for every file Cratedigger attempts
to enqueue. The row is intent evidence until slskd accepts the POST; migration
051 adds nullable `accepted_at`, which is stamped immediately after acceptance.
Completion events add file paths only to already-confirmed rows; they never
promote pending intent. A definitively rejected POST therefore cannot gain
destructive authority from a later same-key human completion.

The durable ownership key is `(username, filename)`: slskd assigns a fresh
transfer ID when it retries the same queued file, so an attempt-local ID cannot
prove or disprove ownership of a later terminal record. Every terminal
`Completed,*` record with a confirmed queue key is removed individually using
its current slskd ID; a pending or unledgered key is never touched.

Migration 051 derives historical acceptance from the old positive evidence,
then removes the obsolete `transfer_id` and `completed_at` columns and their
indexes. `local_path` remains separate, authoritative file evidence: only
the completion event feed stamps it, and disk deletion still requires that
event-stamped path or another positive ownership signal. Terminal transfer
cleanup does not infer a filesystem path from the queue key.

## Persisted search plans (migration 014)

Search execution is plan-driven. Each wanted request owns a materialised
`search_plans` row with an ordered list of `search_plan_items` (the runnable
queries) and a cursor on `album_requests` (`active_plan_id`, `next_plan_ordinal`,
`plan_cycle_count`). `search_attempts` no longer selects queries; it remains
only as scheduler/backoff history. The pure generator that produces plan
items lives in `lib/search.py` and is keyed by `SEARCH_PLAN_GENERATOR_ID`
(`search-plan/<date>-<seq>`), which is bumped manually whenever generation-
affecting code or config changes — see "Generator id discipline" below.

### `search_plans`

| Column | Type | Notes |
|---|---|---|
| `id` | `SERIAL PRIMARY KEY` | |
| `request_id` | `INTEGER NOT NULL` | FK → `album_requests(id) ON DELETE CASCADE` |
| `generator_id` | `TEXT NOT NULL` | Mirrors `SEARCH_PLAN_GENERATOR_ID` at write time |
| `status` | `TEXT NOT NULL` | One of `active`, `superseded`, `failed_deterministic`, `failed_transient` |
| `failure_class` | `TEXT NULL` | `no_runnable_query`, `metadata_incomplete`, `resolver_unavailable`, `dependency_failure`, `unknown` |
| `metadata_snapshot` | `JSONB NULL` | Snapshot of the release metadata used to generate this plan |
| `provenance` | `JSONB NULL` | Bounded provenance: dropped tokens, deduped variants, omitted candidates |
| `error_message` | `TEXT NULL` | Sanitized human-readable error (no credentials / host paths) |
| `superseded_at` | `TIMESTAMPTZ NULL` | Set when an active plan flips to `superseded` |
| `superseded_by_plan_id` | `INTEGER NULL` | FK → `search_plans(id) ON DELETE SET NULL` |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT NOW()` | |

Indexes:

- `idx_search_plans_request_status (request_id, status)` — active-plan lookup
- `idx_search_plans_generator (generator_id)` — current vs old-generator scans
- `idx_search_plans_request_created_at (request_id, created_at DESC)` — supersession trail
- `uniq_search_plans_one_active_per_request (request_id) WHERE status = 'active'` — partial unique; one active plan per request
- Composite-unique `(id, request_id)` — supports the active-plan FK below

Plan statuses:

- **`active`** — current successful plan for this request. The cursor
  on `album_requests` points here. Only one per request (partial unique).
- **`superseded`** — was active, replaced by a newer successful plan.
  Stays readable for forensic audit; `superseded_by_plan_id` walks
  forward to the replacement.
- **`failed_deterministic`** — sticky for the current generator id
  (e.g. no runnable query for any tier). Reconciliation will not retry.
- **`failed_transient`** — retryable (resolver outage, dependency hiccup).
  Reconciliation retries on the next startup.

### `search_plan_items`

| Column | Type | Notes |
|---|---|---|
| `id` | `SERIAL PRIMARY KEY` | |
| `plan_id` | `INTEGER NOT NULL` | FK → `search_plans(id) ON DELETE CASCADE` |
| `ordinal` | `INTEGER NOT NULL CHECK (ordinal >= 0)` | Cursor position, 0-indexed |
| `strategy` | `TEXT NOT NULL` | Free-form: `default`, `unwild`, `unwild_year`, `track_<idx>`, ... |
| `query` | `TEXT NOT NULL CHECK (length(btrim(query, ' \t\n\r\f\v')) > 0)` | Runnable query — never blank |
| `canonical_query_key` | `TEXT NULL` | Normalised key for dedupe and per-query usefulness aggregation |
| `repeat_group` | `TEXT NULL` | Shared by intentionally-repeated default slots |
| `provenance` | `JSONB NULL` | Per-item provenance |
| | | UNIQUE `(plan_id, ordinal)` |

Indexes:

- `idx_search_plan_items_plan_ordinal (plan_id, ordinal)` — cursor reads
- `idx_search_plan_items_canonical_key (canonical_query_key)` — per-query rollups

### `album_requests` cursor fields

| Column | Type | Notes |
|---|---|---|
| `active_plan_id` | `INTEGER NULL` | Composite FK → `search_plans(id, request_id)`, `ON DELETE SET NULL (active_plan_id)` |
| `next_plan_ordinal` | `INTEGER NOT NULL DEFAULT 0` | Index into the active plan's items |
| `plan_cycle_count` | `INTEGER NOT NULL DEFAULT 0` | Increments only when the cursor wraps past the final ordinal |

Constraints:

- `album_requests_active_plan_owner_fkey (active_plan_id, id) → search_plans(id, request_id)` — guarantees the active plan belongs to this request, not another. Plan deletion only nulls `active_plan_id`; the request id stays intact.
- `next_plan_ordinal >= 0`, `plan_cycle_count >= 0`.

Index: `idx_album_requests_wanted_active_plan (status, active_plan_id) WHERE status = 'wanted'` supports the all-wanted reconciliation scan.

### `search_log` plan-context fields

Migration 014 adds nullable plan-context columns. Historical rows stay
valid with `NULL` plan context and any `outcome` value — including
`exhausted` — so legacy reporting remains queryable. The `outcome`
CHECK constraint is intentionally untouched.

| Column | Type | Notes |
|---|---|---|
| `plan_id` | `INTEGER NULL` | FK → `search_plans(id) ON DELETE SET NULL` |
| `plan_item_id` | `INTEGER NULL` | FK → `search_plan_items(id) ON DELETE SET NULL` |
| `plan_ordinal` | `INTEGER NULL` | Mirrors the executed item ordinal |
| `plan_strategy` | `TEXT NULL` | Mirrors the executed slot strategy |
| `plan_canonical_query_key` | `TEXT NULL` | For per-query stats grouping |
| `plan_repeat_group` | `TEXT NULL` | For per-repeat-group stats grouping |
| `plan_generator_id` | `TEXT NULL` | Stamped at log time so post-cutover stats can filter by current generator |
| `execution_stage` | `TEXT NULL` | `pre_attempt`, `accepted`, `stale_completion`, `reconciliation` |
| `attempt_consumed` | `BOOLEAN NULL` | True iff this row consumed a slot (advanced cursor) |
| `cursor_update_status` | `TEXT NULL` | `advanced`, `wrapped`, `unchanged`, `stale` |
| `stale_reason` | `TEXT NULL` | Short tag explaining why a row is stale (e.g. `regenerated_mid_flight`, `plan_or_ordinal_drift`) |
| `plan_cycle_snapshot` | `INTEGER NULL` | Snapshot of `plan_cycle_count` at log time, for cycle bucketing without rejoining the request row |

Indexes:

- `idx_search_log_plan_item (plan_item_id)`
- `idx_search_log_canonical_query_key (plan_canonical_query_key)`
- `idx_search_log_plan_id_created_at (plan_id, created_at DESC)`

### `search_log` outcomes — no-new-`exhausted` policy

Outcomes still recognised by the schema: `found` (matched + enqueued),
`no_match` (results but no suitable download), `no_results` (0 results
from slskd), `timeout`, `error`, `empty_query` (can't build query),
`exhausted` (legacy reset signal).

After the persisted-search-plans cutover, **new code never writes
`outcome='exhausted'`**. Plan wrap is the replacement: the executor
records a normal accepted-search outcome (`no_match`, `no_results`,
`error`, etc.) and the consumed-attempt DB method sets
`cursor_update_status = 'wrapped'` plus increments
`plan_cycle_count`. Historical `outcome='exhausted'` rows from before
the cutover stay valid and continue to render in the existing dashboard
position labelled as historical. See
`docs/persisted-search-plans-rollout.md` for the SQL spot-check that
confirms zero new exhausted rows after the deploy timestamp.

### Execution stage, attempt-consumed, cursor-update status

These four audit markers (`execution_stage`, `attempt_consumed`,
`cursor_update_status`, `stale_reason`) make pre-attempt failures,
accepted attempts, and stale post-regeneration completions
distinguishable in `search_log`:

- `execution_stage='pre_attempt'`, `attempt_consumed=false`,
  `cursor_update_status='unchanged'` — submission/setup failed before
  slskd accepted the search. Non-consuming. Backoff still applies.
- `execution_stage='accepted'`, `attempt_consumed=true`,
  `cursor_update_status='advanced'` — happy path; ordinal moved forward.
- `execution_stage='accepted'`, `attempt_consumed=true`,
  `cursor_update_status='wrapped'` — final ordinal; cursor wrapped to
  0 and `plan_cycle_count` incremented. **This replaces
  `outcome='exhausted'`** as the cycle-wrap signal.
- `execution_stage='stale_completion'`, `attempt_consumed=false`,
  `cursor_update_status='stale'`, `stale_reason=<tag>` — a regeneration
  superseded the active plan after the search was submitted. Log-only;
  active cursor / status / scheduling are not mutated.
- `execution_stage='reconciliation'` — emitted by startup
  reconciliation (rare). Not a normal slot execution.

### `candidates` JSONB

Top 20 peer scores per search, sorted by `(matched_tracks DESC, avg_ratio DESC)`. Each entry is a `lib.quality.CandidateScore` (`msgspec.Struct`):

```json
{"username": "peer", "dir": "...", "filetype": "lossless",
 "matched_tracks": 24, "total_tracks": 26, "avg_ratio": 0.91,
 "missing_titles": ["..."], "file_count": 26}
```

Empty array `[]` for `no_results` / `no_match` outcomes; `NULL` for `error`, `timeout`, `exhausted`, `empty_query`. Decoded at exactly one site per consumer (`web/routes/pipeline.py::get_pipeline_detail` and `scripts/pipeline_cli/show.py::cmd_show`) via `msgspec.convert(blob, type=list[CandidateScore])`.

### `final_state`

The slskd terminal state for the search (`Completed`, `ResponseLimitReached`, `TimedOut`, `Errored`, etc.). `NULL` on historical `exhausted` outcomes (no slskd round-trip) and on `pre_attempt` rows where slskd was never reached.

### Generator id discipline

`SEARCH_PLAN_GENERATOR_ID` in `lib/search.py` is the **single runtime
source** of "which generator output is current". CLI add, web add,
startup reconciliation, regeneration, and the executor all read this
constant. Bump it (date-stamped string,
e.g. `search-plan/2026-05-08-2`) **whenever** any of the following
change:

- generator output rules (which slots are emitted, in what order)
- query tokenisation
- the low-entropy token set (currently `the`, `you`, `from`, `and`)
- slot ordering / ranking
- dedupe behaviour
- repeat-group identity
- provenance shape

Plans whose `generator_id` differs from the current id are "old-
generator" plans. Startup reconciliation supersedes them with new
plans on the next cycle. Tests pin both the literal id and a
representative ladder snapshot, so any output drift forces
`tests/test_search.py::test_generator_id_constant_is_pinned` to fail
until the id is intentionally bumped.

## Search-plan iteration 2 (migrations 026–033)

Iteration 2 layers observability + detection state onto the
persisted-search-plans surface. Every column below was added by a
PR1 migration; PR3 wires the writes. The iter2 brainstorm and plan
docs (`docs/brainstorms/2026-05-25-search-plan-iteration-2-requirements.md`,
`docs/plans/2026-05-25-001-feat-search-plan-iteration-2-plan.md`) are
the requirement-id source of truth — each entry below points at the
R-id it satisfies.

### `search_log` forensics columns (migration 027, written by PR3 U11)

Seven nullable scalars that let triage SQL skip JSONB introspection
into `candidates`. Populated at log-write time by
`lib/pipeline_db/search_plan.py::log_search` via the matcher
(`lib/matching.py::check_for_match`) and search-executor layer.
Historical rows pre-deploy carry `NULL` in all seven; new rows
post-PR3 populate every applicable column.

| Column | Type | Notes |
|---|---|---|
| `rejection_reason` | `TEXT NULL` | Dominant matcher rejection from the top-scored candidate. One of `strict_count_mismatch`, `avg_ratio_low`, `cross_check_failed`, `all_skipped_pre_filter`, `bitrate_below_min`, `denylisted_user`, `cooldown`, `cap_truncation_no_survivors`. `NULL` on `outcome='found'` or when there were no candidates to reject. (R22) |
| `result_count_uncapped` | `INTEGER NULL` | slskd's true `responseCount` before the cratedigger 1000-cap is applied. `result_count` remains the post-cap count. Comparing the two surfaces saturated searches honestly. (R23) |
| `query_token_count` | `INTEGER NULL` | `len(query.split())` — total tokens, including duplicates and stopwords. (R24) |
| `query_distinct_token_count` | `INTEGER NULL` | `len(set(query.split()))` — distinct tokens. Low distinctiveness correlates with bag-of-words slskd searches that match too many peers. (R24) |
| `expected_track_count` | `INTEGER NULL` | The request's `total_tracks` snapshotted at search-execution time. Not slskd's result count, not a hardcoded value — the operator's expectation for this release. (R25) |
| `matcher_score_top1` | `REAL NULL` | The top candidate's composite score (`matched_tracks + avg_ratio`) from `candidates[0]`. `0.0` on `no_results` / `no_match` with empty candidate set. (R26) |
| `query_template` | `TEXT NULL` | Operator-readable shape derived from `plan_strategy` (e.g. `{artist} {title}`, `{artist} {track_N}`, `{catalog_number}`). Lets `GROUP BY query_template` surface which template shapes are productive vs noise. (R27) |

### `album_requests` observability columns (migration 028, written by PR3 U12 / U13 / U14)

Eight columns covering plan-wrap classification, VA detection,
unfindable categorisation, and long-tail-rescue audit. CHECK
constraints on the three enum-shaped TEXT columns surface typos as
constraint violations rather than silent corruption.

| Column | Type | Notes |
|---|---|---|
| `failure_class` | `TEXT NULL` | 5-bucket cycle classification: `A_zero_results_dominant`, `B_cands_never_match`, `D_found_but_no_import`, `E_mixed`, `resolved`. Written by `lib/search_plan_service.py` at plan-wrap inside the cursor-advance transaction (PR3 U12). `NULL` until the first cycle wraps. A wrap with zero searches in the cycle leaves it `NULL` (defensive: "no signal" is not a classification). CHECK enforces the enum. (R28) |
| `is_va_compilation` | `BOOLEAN NOT NULL DEFAULT FALSE` | VA detection flag set at enqueue by `lib/field_resolver_service.py::detect_va_compilation` (3-rule detector — canonical VA MBID match, Compilation release-group + divergent track credits, split-artist joinphrase). Consumed by `_generate_va_plan` in the generator. (R12) |
| `unfindable_category` | `TEXT NULL` | 4-bucket cohort taxonomy: `artist_absent`, `album_absent_artist_present`, `one_track_structural`, `wrong_pressing_available`. Written by `lib/unfindable_detection_service.py` on its daily cadence (PR3 U13). Cleared on long-tail-rescue (U14). CHECK enforces the enum. Partial index `idx_album_requests_unfindable_category` over rows where the column is non-NULL supports the operator triage scan. (R18, R19) |
| `unfindable_categorised_at` | `TIMESTAMPTZ NULL` | When the categoriser last ran for this request. Used by the detection job to pick the K oldest probes per run. |
| `last_artist_probe_at` | `TIMESTAMPTZ NULL` | Most recent artist-only catalog probe against slskd. Per-request probe cadence target is ~7 days. |
| `last_artist_probe_match_count` | `INTEGER NULL` | Result count from the last artist-only probe. Feeds the `artist_absent` vs `album_absent_artist_present` classifier branch. |
| `rescued_at` | `TIMESTAMPTZ NULL` | Long-tail-rescue audit timestamp. Set by the importer success path (PR3 U14, `lib/dispatch/` → `PipelineDB.mark_imported_with_rescue`) when a request that was carrying an `unfindable_category` transitions to `imported`. First-rescue-wins — immutable once set; Replace flows do not re-stamp it. (R21) |
| `prior_unfindable_category` | `TEXT NULL` | The `unfindable_category` value cleared by the rescue (same enum + CHECK as `unfindable_category`). Lets `SELECT prior_unfindable_category, COUNT(*) FROM album_requests WHERE rescued_at IS NOT NULL` surface which cohorts the watch loop actually rescues over time. (R21) |

R20 ("the system never stops searching") is enforced structurally: the
`cratedigger-unfindable.service` shares no code path with the regular
5-min search loop and an `ast.parse` walk over
`lib/unfindable_detection_service.py` + `scripts/run_unfindable_detection.py`
rejects any reference to cursor-mutation names.

### `album_requests.catalog_number` (migration 032, resolved at enqueue)

| Column | Type | Notes |
|---|---|---|
| `catalog_number` | `TEXT NULL` | Resolved at enqueue via the dual-source field resolver (MB + Discogs), populating the `catalog_number` plan-strategy slot the PR2 generator adds. |

### `album_tracks.track_artist` (migration 029, populated at enqueue)

| Column | Type | Notes |
|---|---|---|
| `track_artist` | `TEXT NULL` | Per-track artist persisted from the resolver output. Consumed by PR2's VA plan generation (`va_track_artist_*` slots). NULL until resolution succeeds for that track. |

### `album_request_field_resolutions` (migration 030, side table)

Tracks per-(request, field) resolution attempts for the four
network-dependent fields (`release_group_year`, `release_group_id`,
`track_artist`, `catalog_number`). Used by enqueue-time inline
resolution + the operator deploy-window backfill heredoc.

| Column | Type | Notes |
|---|---|---|
| `request_id` | `INTEGER NOT NULL` | FK → `album_requests(id) ON DELETE CASCADE` |
| `field_name` | `TEXT NOT NULL` | One of the four resolved fields above |
| `status` | `TEXT NOT NULL` | `resolved`, `unresolved_no_data`, `unresolved_4xx_client`, `unresolved_mirror_unavailable`, `unresolved_timeout`, `unresolved_field_missing_upstream` |
| `attempted_at` | `TIMESTAMPTZ NOT NULL DEFAULT NOW()` | Last attempt timestamp |
| (audit metadata) | | Details preserved per-row for forensic queries against upstream-data gaps |

Transient buckets (`unresolved_mirror_unavailable`, `unresolved_timeout`)
retry on the next enqueue path; permanent buckets (`unresolved_no_data`,
`unresolved_4xx_client`, `unresolved_field_missing_upstream`) record
the audit trail without retry.

### `request_search_summary` view (migration 031, consumed by PR4)

Per-request 14-day rollup over `search_log` for the future operator
triage surface. Plain VIEW, not materialised — operator triage
frequency is human-paced and the bounded scan cost rides on the
existing `idx_search_log_request_created_at` composite index.

| Column | Type | Notes |
|---|---|---|
| `request_id` | `INTEGER` | Group key |
| `total_searches` | `BIGINT` | Search count in the 14-day window |
| `with_cands_count` | `BIGINT` | Rows where `candidates` JSONB is non-empty |
| `found_count` | `BIGINT` | Rows where `outcome='found'` |
| `near_cap_count` | `BIGINT` | Rows where `result_count >= 950` — popular albums hitting the 1000-cap |
| `zero_results_count` | `BIGINT` | Rows where `result_count = 0` |
| `pre_filter_skips_total` | `BIGINT` | Sum of `pre_filter_skip_count` (column added by migration 025) |
| `first_strategy_with_cands` | `TEXT` | Oldest `plan_strategy` in the window that produced ≥1 candidate |
| `dominant_rejection_reason` | `TEXT` | `MODE()` over `rejection_reason` (R22 column) for non-NULL rows |
| `last_search_at` | `TIMESTAMPTZ` | `MAX(created_at)` |

The 14-day window is intentional — triage windows that need older
data should query `search_log` directly. (R29)

## `album_requests.manual_reason`

A free-form `TEXT` column populated by system flips that move a request to `status='manual'`. Currently unused — the persisted-search-plans cutover replaced the legacy variant ladder's `exhausted` flow with cursor wrap (no manual flip). The column stays for future operator-hold workflows that need a structured reason without overloading the human-authored `reasoning` field. Cleared (`NULL`) on every `reset_to_wanted` so re-queue starts with a clean slate.

## Wrong Matches and Force-Import

Albums rejected by beets validation (high distance, wrong pressing) are moved
to `failed_imports/` under the slskd download dir, with their `failed_path`
stored in `download_log.validation_result` JSONB. Wrong Matches cleanup consumes
already-persisted candidate/current evidence only; it never previews,
measures, or backfills evidence at delete time. Confident cleanup-eligible
force-mode rejects are deleted and cleared; would-import, uncertain, missing
evidence, stale evidence, active-job, and missing-path rows stay actionable for
manual review or converge.

Wrong Matches is a candidate/pressing-identity review surface, not a general
failed-import bucket. Folder/audio-integrity fact rejects (`audio_corrupt`,
`bad_audio_hash`, `nested_layout`, `empty_fileset`, `mixed_source`) and the
quality-only `spectral_reject` scenario are excluded from both the visible
queue and its automatic cleanup. SQL, the test fake, and post-rejection
cleanup all consume the neutral taxonomy in `lib/wrong_match_policy.py`; a new
non-match rejection scenario must be classified there once rather than copied
into each adapter.

The quarantine lifecycle view surfaces folders that are protected from the
disk reaper but no longer visible in Wrong Matches:

```bash
pipeline-cli triage quarantine --json
curl https://music.ablz.au/api/triage/quarantine
```

Both read-only adapters wrap the same service. It scans only immediate real
directories under the configured `<slskd_download_dir>/failed_imports/`; it
does not recurse, follow symlinks, delete, or infer ownership. The code-owned
`bad_files/` and `untracked_audio/` category roots are excluded rather than
misreported as album folders or recursively expanded. A visible Wrong Matches
row protects its immediate album root whether its persisted `failed_path` is a
legacy relative path (`failed_imports/Artist - Album`), an absolute path, or a
descendant of that album root. References outside the configured quarantine
do not claim local folders. A `status='replaced'` parent is frozen audit
history and is excluded by the shared default Wrong Matches visibility rule,
so its reference does not hide a quarantine folder. The explicit
`/api/wrong-matches?include_replaced=true` history view still surfaces those
rows without changing lifecycle triage.

Results are sorted by folder name and carry `name`, absolute `path`, and
`mtime_ns`. A genuinely absent `failed_imports/` root is a valid empty state.
Configuration, DB, validation-envelope, directory-read, and mid-scan race
errors fail the whole view as CLI exit `5` / HTTP `503`; partial state is never
presented as an empty or trustworthy orphan list. Deletion remains an explicit
operator decision through the existing Wrong Matches delete surfaces.

Historical rows may still contain
`download_log.validation_result.wrong_match_triage` from the retired preview
triage path. Recents History renders those old blobs as display-only audit
metadata. New cleanup does not write a replacement blob; its immediate outcome
is returned in the web/CLI summary.

After manual review, force-import bypasses the distance check. The request
handler or CLI command validates the row/path synchronously, then enqueues a
`force_import` job. `cratedigger-importer` runs the actual beets mutation.

**Path resolution**: old entries stored relative paths (`failed_imports/Foo - Bar`), new entries store absolute paths. Force-import resolves relative paths against `/mnt/virtio/music/slskd/` automatically.

Wrong Matches Converge is a web triage layer on top of the same queue. The UI
defaults each release to a `180` milli-distance loosen threshold, marks
candidate rows green when `validation_result.distance <= 0.180`, then posts to
`/api/wrong-matches/converge`. Green rows are enqueued as `force_import` jobs
and dismissed from the actionable Wrong Matches list without deleting their
folders; the queued job still owns the source path. When Converge runs,
non-green rows for that release are deleted from disk and cleared from the
review list.

1. Look up `download_log` entry by ID via `get_download_log_entry()` → extract `failed_path` from `validation_result` JSONB.
2. Resolve path (handle both relative and absolute) → verify files still exist.
3. Look up `mb_release_id` from `album_requests` via `request_id`.
4. Enqueue `import_jobs(job_type='force_import')` with a dedupe key for the `download_log` row.
5. `cratedigger-importer` claims the job and calls the existing dispatch path, including `import_one.py --force` (sets `MAX_DISTANCE=999` — everything else runs normally: conversion, spectral, quality comparison).
6. The worker marks the job `completed` or `failed`; the import internals still write `download_log` and `album_requests` outcomes.
7. If a queued force-import fails with a terminal, non-deferred pipeline rejection, the worker deletes the reviewed source directory and clears the actionable `failed_path` pointer from the original wrong-match row plus duplicate rejected rows for the same request/path. The failed job and `download_log` audit rows remain.

```bash
pipeline_cli.py force-import <download_log_id>
pipeline_cli.py import-jobs --status failed
pipeline_cli.py wrong-match-triage --apply --json
# or: POST /api/pipeline/force-import {"download_log_id": N}
```

`wrong-match-triage` is destructive and intentionally processes the full
Wrong Matches queue. It requires `--apply`, rejects scope flags, and returns
per-outcome counts matching the web `/api/wrong-matches/triage` summary.

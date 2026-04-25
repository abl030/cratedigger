# Pipeline DB Schema (key fields + JSONB audit blobs)

The pipeline DB is PostgreSQL. DSN: `192.168.100.11:5432/cratedigger`. Access via `pipeline-cli` on doc2, or from doc1 via `ssh doc2 'pipeline-cli ...'`.

Full schema lives in `migrations/*.sql`. This doc covers the fields that appear in debugging and the JSONB audit blobs.

## `album_requests` — quality-tracking fields

- `search_filetype_override TEXT` — transient CSV filetype list (e.g. `"lossless,mp3 v0,mp3 320"` or just `"lossless"`). Overrides global `allowed_filetypes` for search. Set by quality gate requeue paths and backfill. Cleared on quality gate accept. The `"lossless"` virtual tier matches FLAC, ALAC, and WAV.
- `target_format TEXT` — persistent user intent for desired format on disk (`"lossless"` or NULL). Set only by user action (CLI/web set-intent toggle). Never cleared by quality gate. When set, keeps lossless on disk (normalizes ALAC/WAV → FLAC) instead of converting to V0/target.
- `min_bitrate INTEGER` — current min track bitrate in kbps (from beets).
- `prev_min_bitrate INTEGER` — previous min_bitrate before last upgrade. Shows delta in UI.
- `verified_lossless BOOLEAN` — True only when imported from spectral-verified genuine FLAC→V0.
- `last_download_spectral_grade TEXT` — spectral grade of the most recent download attempt.
- `last_download_spectral_bitrate INTEGER` — estimated bitrate from the most recent download's spectral analysis.
- `current_spectral_grade TEXT` — spectral grade of files currently on disk in beets.
- `current_spectral_bitrate INTEGER` — spectral estimated bitrate of files currently on disk. NULL for genuine files (no cliff). Quality gate uses this for gate_bitrate.
- `active_download_state JSONB` — persisted download state for async polling (filetype, enqueued_at, per-file username/filename/size). Set by `set_downloading()`, cleared on completion/timeout.

## `download_log` — quality-tracking fields

- `slskd_filetype TEXT` — what Soulseek advertised (`"flac"`, `"mp3"`).
- `actual_filetype TEXT` — what's on disk after download/conversion.
- `spectral_grade TEXT` — spectral analysis of the downloaded files.
- `spectral_bitrate INTEGER` — estimated original bitrate from spectral.
- `existing_min_bitrate INTEGER` — beets min bitrate before this download.
- `existing_spectral_bitrate INTEGER` — spectral estimate of existing files before download.
- `outcome TEXT` — one of 6 values: `success`, `rejected`, `failed`, `timeout`, `force_import`, `manual_import`.

## `import_jobs` — shared importer queue

All beets-mutating import work is submitted to `import_jobs` and drained by
`cratedigger-importer`. Web force-import, web/manual import, automation
completed-download processing, and CLI force/manual import all share this table.

Key fields:

- `job_type TEXT` — `force_import`, `manual_import`, or `automation_import`.
- `status TEXT` — `queued`, `running`, `completed`, or `failed`.
- `request_id INTEGER` — the related `album_requests.id`.
- `dedupe_key TEXT` — active queue dedupe key. A partial unique index prevents
  duplicate queued/running jobs while allowing a later job after completion.
- `payload JSONB` — typed job input. Force/manual jobs carry `failed_path`;
  force jobs also carry `download_log_id` and optional `source_username`.
- `result JSONB`, `message`, `error` — terminal worker result visible to web
  and CLI callers.
- `attempts`, `worker_id`, `started_at`, `heartbeat_at`, `completed_at` —
  claim and recovery metadata.

On importer startup, any pre-existing `running` job is treated as abandoned
state from a previous worker process, reset to `queued`, and retried
immediately. The importer also holds a DB advisory singleton lock while it
runs, so an accidentally-started second worker exits instead of requeueing a
live worker's job.

## `download_log.import_result` JSONB

`import_one.py` emits an `ImportResult` JSON blob (`__IMPORT_RESULT__` sentinel on stdout). Contains: decision, conversion details, per-track spectral analysis (grade, hf_deficit, cliff detection per track), quality comparison (new vs prev bitrate), postflight verification (beets_id, path). Every import path (success, downgrade, transcode, error, timeout, crash) logs to download_log.

```sql
SELECT import_result->>'decision',
       import_result->'quality'->>'new_min_bitrate',
       import_result->'spectral'->>'grade',
       import_result->'spectral'->'per_track'->0->>'hf_deficit_db'
FROM download_log ORDER BY id DESC LIMIT 10;
```

## `download_log.validation_result` JSONB

`beets_validate()` returns a `ValidationResult` with the full candidate list from the harness. Every validation (success or rejection) stores this. Contains: all beets candidates with distance breakdown per component (album, artist, tracks, media, source, year...), full track lists per candidate, the item→track mapping (which local file matched which MB track), local file list, beets recommendation level, soulseek username, download folder, failed_path, denylisted users, corrupt files.

```sql
-- Why was distance high?
SELECT validation_result->'candidates'->0->'distance_breakdown'
FROM download_log WHERE id = <id>;

-- Which local file matched which MB track?
SELECT m->'item'->>'path', m->'item'->>'title', m->'track'->>'title'
FROM download_log, jsonb_array_elements(validation_result->'candidates'->0->'mapping') AS m
WHERE id = <id>;
```

## `search_log`

Every search attempt is logged to `search_log` with: `request_id`, `query` (normalized search term), `result_count`, `elapsed_s`, `outcome`, `created_at`. Failed searches also increment `search_attempts` on `album_requests` and trigger exponential backoff.

Outcomes: `found` (matched + enqueued), `no_match` (results but no suitable download), `no_results` (0 results from slskd), `timeout`, `error`, `empty_query` (can't build query).

## Force-Import (rejected downloads)

Albums rejected by beets validation (high distance, wrong pressing) are moved to `failed_imports/` under the slskd download dir, with their `failed_path` stored in `download_log.validation_result` JSONB. After manual review, force-import bypasses the distance check. The request handler or CLI command validates the row/path synchronously, then enqueues a `force_import` job. `cratedigger-importer` runs the actual beets mutation.

**Path resolution**: old entries stored relative paths (`failed_imports/Foo - Bar`), new entries store absolute paths. Force-import resolves relative paths against `/mnt/virtio/music/slskd/` automatically.

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
# or: POST /api/pipeline/force-import {"download_log_id": N}
```

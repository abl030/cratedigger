---
title: "feat: YouTube rescue ingest API"
type: feat
status: active
date: 2026-05-28
origin: docs/brainstorms/2026-05-28-youtube-rescue-ingest-api-requirements.md
---

# feat: YouTube rescue ingest API

## Summary

A new `cratedigger-youtube-ingest` long-running systemd worker, plus a paired CLI + HTTP API, that accepts `(request_id, browse_id)`, runs yt-dlp on the resolver-supplied YouTube Music playlist, enforces a hard track-count gate before staging, and enqueues an existing-shape `automation_import` job so the existing preview → importer chain processes the staged directory identically to a slskd auto-import. `download_log` doubles as both queue state and audit trail via a new `source` discriminator column and three new `youtube_*` outcome values; no new state on `album_requests`.

---

## Problem Frame

The YouTube Music album resolver (PR #383/#384) gave cratedigger a discovery surface — "given this MBID, which YT Music albums score how against the release group?" — but it's read-only metadata; it doesn't actually fetch audio. The natural complement is an ingestion path: take the resolver's output and produce a staged directory that the existing importer can consume. The brainstorm at `docs/brainstorms/2026-05-28-youtube-rescue-ingest-api-requirements.md` settles the product shape — the work below is the implementation.

Two architectural commitments shape every decision in this plan:

1. **No state-machine fork.** `album_requests.status` is never touched by this code path. `download_log` doubles as queue + audit so the web UI queue, triage filters, and orphan detection inherit YT visibility without new awareness.
2. **The boundary is the staged directory.** The YT worker stops at `/Incoming/auto-import/<artist>-<album>/` plus an `automation_import` row in `import_jobs`. Everything downstream — preview measurement, quality gates, beets distance, wrong-matches routing — is the existing pipeline's concern and is structurally unchanged. The YT path's success criterion is "we placed a same-shape staged directory in the same place slskd would have."

---

## Key Technical Decisions

- **KTD1. `download_log` extension with a `source` discriminator + dedicated JSONB blob.** A new `source TEXT NOT NULL DEFAULT 'slskd' CHECK (source IN ('slskd','youtube'))` column distinguishes sourcing channels. A new `youtube_metadata JSONB` column carries the YT-specific audit payload (typed `YoutubeIngestMetadata: msgspec.Struct`). Rationale: extends the existing audit row contract minimally; existing rows backfill to `'slskd'` via the column DEFAULT in a single-statement migration (per the single-operator no-backfill-script rule).

- **KTD2. Idempotency enforced via partial unique index, not application-level check.** `CREATE UNIQUE INDEX one_youtube_running_per_request ON download_log (request_id) WHERE source = 'youtube' AND outcome = 'youtube_running'`. Application-level "does an in-flight row already exist" checks have a race window between the read and the insert. Pushing it into the DB schema gives atomic R4 enforcement and aligns with how existing schema-level constraints model invariants.

- **KTD3. Introduce `youtube_import` as a new job_type with payload-supplied path.** The YT worker enqueues a new `youtube_import` row in `import_jobs` carrying the staged path in the payload. A new dispatcher in `scripts/importer.py::execute_youtube_import_job` and a parallel branch in `scripts/import_preview_worker.py::_front_gate_source_path` read the path from `import_jobs.payload` instead of from `album_requests.active_download_state`. Rationale: the alternative — reusing `automation_import` — would require fabricating a slskd-shaped `active_download_state` with synthetic peer usernames and completed-state markers, then routing YT through machinery designed for slskd peers (cooldown logic against fake usernames, orphan detection against synthetic transfer state). Cleaner to accept dispatch-logic duplication and keep channel separation explicit at the queue level. Side benefit: future non-slskd-non-YT sources (e.g. Bandcamp redownload) inherit the same payload-supplied-path pattern without faking more slskd state. KTD1 ("status is never touched by THIS code path") stays intact — the existing `mark_imported_with_rescue` performs the only `album_requests.status` write on import success, and is source-agnostic by design (R17).

- **KTD4. Service layer is the source of truth; CLI and HTTP API are thin adapters.** `lib/youtube_ingest_service.py` holds all logic; `lib/search_plan_service.py::SearchPlanService.advance_for_request` is the canonical pattern to mirror. Both wrappers re-export the service's `OUTCOME_HTTP_STATUS` / `OUTCOME_EXIT_CODE` maps so the outcome → status mapping is defined exactly once.

- **KTD5. R7 cross-validates resolver cache against MB; R10 gates against MB directly.** R7 (submission-time precheck) compares the resolver row's cached `total_mb_tracks` against the request MBID's canonical track count from the MB mirror; mismatch returns 422 with a "resolver state is stale, refresh first" signal. R10 (worker-side post-yt-dlp gate) compares the actual staged file count against the request MBID's canonical track count, sourcing exclusively from the MB mirror. The resolver cache is the upstream-state input to R7; it is NOT consulted by R10. Rationale: R7's drift-detection role requires comparing two sources (the resolver's snapshot vs current MB); R10's job is to verify what yt-dlp actually delivered matches what MB says we should have. Sharing a single source between the two checks defeats R7's purpose — a stale cache would silently pass R7 then produce a wrong-pressing import at R10.

- **KTD6. Two audit writes per job, no mid-flight progress.** `download_log` row goes `youtube_running` on submission (insert), transitions to `youtube_success` or `youtube_failed` on worker completion (single UPDATE). No per-track progress writes. Rationale: simplicity; matches the operator-observable affordance (download_log is the audit row, not a progress bar).

- **KTD7. Worker poll loop at 5-second cadence, no LISTEN/NOTIFY.** Matches the existing `cratedigger-importer` and `cratedigger-import-preview-worker` cadence. Rationale: an additional pubsub mechanism would be the only place in the codebase using it; consistency wins over a single-digit-seconds latency improvement that doesn't matter for a 2-5 minute yt-dlp invocation.

- **KTD8. Subprocess invocation uses `text=True, errors='replace'`.** Per `docs/solutions/subprocess-text-mode-utf8-strict-decode-crash.md`, `text=True` defaults to UTF-8 strict and decode happens inside `_communicate`, where `try/except` around the subprocess call does NOT catch it. YouTube titles routinely contain surrogates, em-dashes, and non-UTF-8 bytes. Every yt-dlp subprocess invocation in this work pairs `text=True` with `errors='replace'`. A RED test mirrors the docs/solutions pattern: a shim binary on PATH emits bare `0xE2` and the worker must survive it.

- **KTD9. Network hardening lives in the downstream wrapper, not the in-flake module.** `nix/module.nix` exposes `cratedigger-youtube-ingest.service` with no opinion on outbound network shape. `~/nixosconfig/modules/nixos/services/cratedigger.nix` layers on the VPN/namespace binding. Rationale: the in-flake module is portable; only the operator's host knows which VPN to bind to.

---

## High-Level Technical Design

### Sequence — happy-path rescue (covers F1)

```mermaid
sequenceDiagram
    autonumber
    participant Op as Operator (CLI or HTTP)
    participant Svc as YoutubeIngestService
    participant DB as PipelineDB
    participant W as YT ingest worker
    participant FS as /Incoming/auto-import
    participant IQ as import_jobs queue
    participant PW as preview worker
    participant Imp as importer

    Op->>Svc: submit(request_id, browse_id)
    Svc->>DB: validate request status ∈ {wanted, manual}
    Svc->>DB: lookup youtube_album_mapping(release_group, browse_id)
    Svc->>DB: precheck total_mb_tracks == request track_count
    Svc->>DB: INSERT download_log (source=youtube, outcome=youtube_running)
    Note over DB: partial unique index enforces R4 idempotency
    Svc-->>Op: 200 / exit 0 with download_log_id

    loop every 5s
        W->>DB: SELECT WHERE source=youtube AND outcome=youtube_running
        W->>W: derive playlist URL from resolver row
        W->>W: yt-dlp (text=True, errors='replace', no --ignore-errors)
        W->>W: count staged files vs expected (track-count gate)
        alt count matches
            W->>FS: stage to /Incoming/auto-import/<artist>-<album>/
            W->>IQ: enqueue youtube_import job (staged_path in payload)
            W->>DB: UPDATE download_log SET outcome=youtube_success
        else count mismatch or yt-dlp failure
            W->>W: discard temp directory
            W->>DB: UPDATE download_log SET outcome=youtube_failed (classified reason)
        end
    end

    PW->>IQ: claim youtube_import job (path from payload, not active_download_state)
    PW->>PW: measure_preimport_state → AlbumQualityEvidence
    Imp->>IQ: claim (preview_status=evidence_ready) — execute_youtube_import_job dispatcher
    Imp->>Imp: existing gates: beets distance, quality, wrong-matches OR auto-import
    Imp->>DB: mark_imported_with_rescue (source-agnostic; rescued_at populates)
```

### Outcome → status/exit mapping (service-layer single source of truth)

| Outcome | HTTP | exit | Meaning |
|---|---|---|---|
| `accepted` | 200 | 0 | row inserted; download_log_id returned |
| `request_not_found` | 404 | 2 | no album_requests row for the id |
| `wrong_state` | 409 | 4 | request status is not `wanted` or `manual` |
| `in_flight` | 409 | 4 | a `youtube_running` row already exists for this request |
| `no_resolver_mapping` | 422 | 3 | the browse_id is not in the resolver mapping for this request's release group |
| `track_count_precheck_failed` | 422 | 3 | resolver's cached track count ≠ request's MBID track count |
| `transient` | 503 | 5 | DB lock contention or transient failure during validation |

The worker's terminal outcomes (`youtube_success` / `youtube_failed`) are write-only — they're not returned to a caller, they appear in subsequent `pipeline-cli show` renderings.

### State transitions for `download_log.outcome` (YT rows only)

```
                  (insert at submission)
                          │
                          ▼
                  youtube_running
                          │
        ┌─────────────────┼─────────────────┐
        │ (terminal)      │ (terminal)      │ (terminal, on worker startup)
        ▼                 ▼                 ▼
youtube_success    youtube_failed     youtube_failed (reason=worker_died)
```

No intermediate states. Once terminal, the row is immutable.

---

## Requirements

All R-IDs carried verbatim from origin. Each maps to one or more units below.

### API surface

- R1. Both CLI subcommand and HTTP API endpoint exist; both wrap the shared service-layer method. Outcome → exit-code / HTTP-status mappings per the table above.
- R2. Input contract: `(request_id, browse_id)`. Browse_id MUST be the YouTube Music album browseId form (the resolver's `yt_browse_id` column).
- R3. Accepts rescue submissions only for requests in `wanted` or `manual` status; all other statuses rejected with 409.
- R4. Idempotency enforced at DB layer via partial unique index — second submission while one is in-flight returns 409 with the existing download_log_id.
- R5. API returns 200 with new `download_log_id` as soon as the row is persisted; does not wait for yt-dlp.

### Resolver coupling

- R6. Submission validates a resolver mapping row exists for the request's release group AND contains the supplied browse_id; missing rows return 422.
- R7. Submission additionally validates resolver's cached `total_mb_tracks` matches the MBID's expected track count; mismatch returns 422 BEFORE worker invocation.
- R8. Worker derives the yt-dlp invocation URL from the resolver row's `yt_url` (or constructs from `audio_playlist_id`).

### Worker behavior

- R9. yt-dlp invoked without `--ignore-errors`; output codec is whatever `bestaudio` heuristic picks (typically Opus from YouTube Music); no transcoding at staging time.
- R10. Worker verifies `count(staged audio files) == MBID expected track count` BEFORE any file moves into `/Incoming/auto-import/`. Mismatch (less or more) aborts with `outcome='youtube_failed'`, reason `track_count_mismatch`.
- R11. On track-count success, worker stages to `/Incoming/auto-import/<artist>-<album>/` and inserts a `youtube_import` row in `import_jobs` carrying the staged path AND request_id in the payload (no `active_download_state` write).
- R12. Anonymous YT access only — no Google account, no OAuth, no cookies. Age-gated content fails per R20's taxonomy.

### Audit and persistence

- R13. New `source` discriminator column on `download_log` (`'slskd'` default, `'youtube'` for new rows).
- R14. New outcome values on `download_log`: `youtube_running` (in-flight), `youtube_success` (worker completed; import lifecycle is the importer's responsibility from there), `youtube_failed` (any failure).
- R15. YT metadata in a new `youtube_metadata` JSONB column (typed `YoutubeIngestMetadata` Struct): `yt_url`, `browse_id`, `audio_playlist_id`, per-track video IDs when known, resolver mapping row reference, classified failure reason on failure, verbatim yt-dlp stderr excerpt on failure, observed-vs-expected counts on count mismatch.
- R16. `download_log` row is the queue entry. Worker drain query: `SELECT ... WHERE source='youtube' AND outcome='youtube_running' ORDER BY created_at LIMIT N`.
- R17. ZERO new columns on `album_requests`. Existing `rescued_at` and `prior_unfindable_category` populate exactly as for slskd-sourced imports via the existing `mark_imported_with_rescue` path in `lib/pipeline_db.py`.

### Network hardening

- R18. Worker runs as its own `cratedigger-youtube-ingest.service` systemd unit defined in `nix/module.nix`. In-flake module does NOT opinionate on outbound network shape.
- R19. Downstream wrapper at `~/nixosconfig/modules/nixos/services/cratedigger.nix` is where the operator applies network-namespace / VPN hardening; the unit must be structured so all yt-dlp egress flows through the bound interface AND so DB reachability (PostgreSQL at `192.168.100.11:5432`) survives whatever wrap is applied.

### Failure modes and observability

- R20. yt-dlp failures classified into structured reasons in the JSONB: `youtube_404`, `youtube_age_gated`, `youtube_region_locked`, `youtube_video_removed`, `youtube_transient_network`, `youtube_unknown`. Verbatim stderr also captured.
- R21. No auto-retry. Failed rescue is terminal for that submission; operator decides whether to resubmit.
- R22. On worker startup, sweep for orphaned `youtube_running` rows and mark them `youtube_failed` with reason `worker_died`. Mirrors `requeue_running_import_jobs` pattern in `lib/pipeline_db.py`.
- R23. `pipeline-cli show <request_id>` rendering surfaces YT rescues in the same chronological "recent attempts" view it already uses for slskd attempts; `source` column distinguishes channels.

---

## Implementation Units

### U1. Migration 037 — `download_log` source discriminator + youtube metadata column + idempotency index

- **Goal:** Extend `download_log` schema with the source discriminator, YT metadata JSONB column, and partial unique index that enforces R4 at the DB layer. Single-statement DEFAULT-based backfill of existing rows; no separate backfill script.
- **Requirements:** R4, R13, R15, R16
- **Dependencies:** none (foundation)
- **Files:**
  - `migrations/037_download_log_youtube_source.sql` (create)
  - `tests/test_migrator.py` (extend — verify the migration row + new schema)
- **Approach:**
  - `ALTER TABLE download_log ADD COLUMN source TEXT NOT NULL DEFAULT 'slskd' CHECK (source IN ('slskd','youtube'))` — DEFAULT handles every existing row in one statement
  - `ALTER TABLE download_log ADD COLUMN youtube_metadata JSONB` (nullable; only populated for `source='youtube'` rows)
  - `CREATE UNIQUE INDEX one_youtube_running_per_request ON download_log (request_id) WHERE source = 'youtube' AND outcome = 'youtube_running'`
  - No CHECK constraint widening the `outcome` column — `outcome` is currently free-form TEXT; the application enforces the YT outcome vocabulary. Out of scope to retrofit broader CHECK constraints on existing outcomes
- **Patterns to follow:** existing migrations in `migrations/` (numbered SQL files; one logical change each). Migration `001_initial.sql` shows the existing `download_log` schema; migration `034_youtube_album_mappings.sql` shows a recent CHECK-constrained column.
- **Test scenarios:**
  - Migration applies cleanly on a fresh DB and on an existing DB seeded with `download_log` rows from migration 001 — backfilled rows have `source='slskd'`, `youtube_metadata IS NULL`
  - Partial unique index rejects a second `(request_id, source='youtube', outcome='youtube_running')` insertion with `psycopg2.errors.UniqueViolation`; permits a second insertion once the first transitions to `youtube_success` or `youtube_failed`
  - CHECK constraint rejects `INSERT ... source='spotify'`
  - **Covers AE3.** Idempotency at the DB layer.
- **Verification:** `nix-shell --run "python3 -m unittest tests.test_migrator -v"` passes; `pipeline-cli query "SELECT * FROM schema_migrations ORDER BY version DESC LIMIT 1"` shows version 037 post-deploy.

### U2. PipelineDB methods + advisory lock namespace + read-seam updates

- **Goal:** Add the typed write/read methods the service and worker need, the new advisory-lock namespace for the worker singleton, the new `youtube_import` job_type constant + typed payload helper, and update every existing read seam that renders `download_log` rows to include the new `source` and `youtube_metadata` columns.
- **Requirements:** R4, R11, R13, R14, R15, R16, R17, R22, R23
- **Dependencies:** U1
- **Files:**
  - `lib/pipeline_db.py` (extend)
  - `lib/import_queue.py` (extend — new `IMPORT_JOB_YOUTUBE` constant + `youtube_import_payload(staged_path, ...)` builder)
  - `tests/test_pipeline_db.py` (extend with real-PG round-trip per Rule A)
  - `tests/test_import_queue.py` (extend — payload roundtrip + job_type enum coverage)
  - `tests/fakes.py` (extend `FakePipelineDB` with the new methods)
  - `tests/test_fakes.py` (extend with self-tests for the new fake methods)
- **Execution note:** Write the real-PG round-trip test FIRST per `.claude/rules/test-fidelity.md` Rule A — every key in the dict written via `insert_youtube_running` MUST round-trip back through `get_download_log_entry`. The album_title bug (resolver PR round 2) shipped because this test wasn't written.
- **Approach:**
  - New methods (full type signatures TBD during implementation):
    - `insert_youtube_running(request_id, browse_id, audio_playlist_id, yt_url, expected_track_count) -> int` — inserts the row; raises a typed exception on idempotency violation (catches `UniqueViolation` from the partial index and re-raises as `YoutubeInFlightError`)
    - `update_youtube_terminal(download_log_id, outcome, metadata_struct)` — atomic UPDATE of `outcome` + `youtube_metadata`. `metadata_struct` is encoded via `msgspec.to_builtins(...)` for the JSONB column (NOT `dataclasses.asdict` — see CLAUDE.md wire-boundary discipline)
    - `find_next_youtube_pending(limit=1) -> list[dict]` — worker drain query; ORDER BY `created_at` to give FIFO semantics
    - `find_orphan_youtube_running() -> list[int]` — startup sweep: returns ids of `youtube_running` rows for the worker to mark failed (mirrors `requeue_running_import_jobs` shape)
  - New advisory lock namespace constant: `ADVISORY_LOCK_NAMESPACE_YOUTUBE_INGEST = 0x59544942494E` (or any new unique 64-bit int — picking from the existing namespace constants in `lib/pipeline_db.py` ~line 123)
  - New job_type constant in `lib/import_queue.py`: `IMPORT_JOB_YOUTUBE = "youtube_import"` alongside existing `IMPORT_JOB_AUTOMATION` / `IMPORT_JOB_FORCE` / `IMPORT_JOB_MANUAL` constants. Typed payload builder: `youtube_import_payload(staged_path: str, request_id: int, browse_id: str) -> dict[str, Any]` that returns the `{staged_path, request_id, browse_id}` JSONB shape consumed by `execute_youtube_import_job` (U9) and `_front_gate_source_path` (U9). Payload values are typed via `msgspec.Struct` at the read seam to enforce wire-boundary discipline.
  - Read-seam updates: extend `_DOWNLOAD_LOG_HISTORY_SELECT` to include `source` and `youtube_metadata`; update `get_download_log_entry`, `get_download_history`, `get_download_history_batch` so consumers receive them. Decode `youtube_metadata` via `msgspec.convert(..., type=YoutubeIngestMetadata)` at the read seam (not in callers)
- **Patterns to follow:**
  - `lib/pipeline_db.py::log_download` (line ~3345) — the existing slskd download_log insert; new method mirrors its shape but is YT-shaped
  - `lib/pipeline_db.py::mark_imported_with_rescue` (line ~2230) — atomic-write pattern with autocommit handling
  - `lib/pipeline_db.py::requeue_running_import_jobs` (line ~1373) — startup orphan sweep template
  - `tests/test_pipeline_db.py::TestYoutubeAlbumMappings::test_upsert_round_trip_preserves_every_field` — canonical Rule A real-PG round-trip pattern
- **Test scenarios:**
  - `insert_youtube_running` writes every input field readably via `get_download_log_entry` (real-PG round-trip — **Rule A**). For each field in the input, assert it's present and equal in the read-back dict
  - `insert_youtube_running` raises `YoutubeInFlightError` when a `youtube_running` row already exists for the same request_id (DB layer's `UniqueViolation` caught and re-raised typed)
  - `update_youtube_terminal` transitions a `youtube_running` row to `youtube_success` AND writes a serializable `YoutubeIngestMetadata` blob; the blob round-trips through `msgspec.convert` after a fresh DB read (production-shape fidelity per `.claude/rules/code-quality.md` § "Contract Test Mocks")
  - `find_next_youtube_pending(limit=N)` returns rows ORDER BY created_at; excludes terminal-state rows; excludes `source='slskd'` rows
  - `find_orphan_youtube_running` returns the right ids; calling `update_youtube_terminal` on each with `youtube_failed`+`worker_died` resolves the orphan list to empty
  - Read seam: a `download_log` row with `source='youtube'`, terminal outcome, and populated `youtube_metadata` renders correctly through `get_download_history` AND `get_download_history_batch`
  - `FakePipelineDB` stubs match real-method shape; `tests/test_fakes.py::TestFakePipelineDB` self-test exercises each new method
- **Verification:** `nix-shell --run "python3 -m unittest tests.test_pipeline_db tests.test_fakes -v"` passes; `pyright` clean on the full repo.

### U3. Typed service layer — `lib/youtube_ingest_service.py`

- **Goal:** Pure service layer that holds all the logic: input validation, idempotency, resolver coupling, track-count precheck, and the worker's per-job runtime. CLI and API wrap this; nothing else.
- **Requirements:** R1, R2, R3, R5, R6, R7, R8, R10, R11, R12, R20
- **Dependencies:** U2
- **Files:**
  - `lib/youtube_ingest_service.py` (create)
  - `tests/test_youtube_ingest_service.py` (create — authoritative coverage)
- **Execution note:** Service-first-then-glue per `docs/solutions/architecture/service-first-then-glue.md`. Service tests must be green BEFORE U4/U5/U6 land. Hard guardrails (track-count gate, idempotency check) short-circuit BEFORE any subprocess / filesystem / network IO.
- **Approach:**
  - `YoutubeIngestMetadata: msgspec.Struct, kw_only=True` — the JSONB blob shape:
    - required: `yt_url: str`, `browse_id: str`, `audio_playlist_id: str | None`
    - optional / terminal-state-only: `reason: str | None`, `stderr_excerpt: str | None`, `observed_track_count: int | None`, `expected_track_count: int | None`, `per_track_video_ids: list[str] | None`, `resolver_mapping_id: int | None`
  - `SubmitOutcome` string-literal type and `SubmitResult: msgspec.Struct` with `outcome: SubmitOutcome`, `download_log_id: int | None`, `detail: str | None`:
    - `accepted`, `request_not_found`, `wrong_state`, `in_flight`, `no_resolver_mapping`, `track_count_precheck_failed`, `transient`
  - `RunOutcome` and `RunResult` Struct for the worker per-job entrypoint: `youtube_success`, `youtube_failed`, with classified `reason` on failure
  - `OUTCOME_HTTP_STATUS: dict[SubmitOutcome, int]` and `OUTCOME_EXIT_CODE: dict[SubmitOutcome, int]` re-exported for wrappers (single source of truth)
  - `YoutubeIngestService` class with kwarg-DI ports: `pdb: PipelineDBSource`, `ytdlp_runner_fn`, `clock_fn`, `stage_dir_fn` — production callers wire to real impls; tests inject fakes
  - Method `submit(request_id: int, browse_id: str) -> SubmitResult` — does all validation, no IO. Returns `accepted` only after a successful `insert_youtube_running`
  - Method `run_job(download_log_id: int) -> RunResult` — the worker's per-job entrypoint; calls `ytdlp_runner_fn`, enforces track-count gate, stages files, enqueues a `youtube_import` job (payload carries the staged path; no `active_download_state` write), writes terminal `download_log` state via `update_youtube_terminal`
- **Patterns to follow:**
  - `lib/search_plan_service.py::SearchPlanService` (advance_for_request, line ~480) — canonical service-first-then-glue shape with typed Result + outcome string literal
  - `lib/youtube_album_service.py` — wire-boundary `msgspec.Struct, kw_only=True` with `PersistedTrack` / `PersistedDistance` / `PersistedYoutubeRow` as the recent canonical examples
  - `lib/quality.py` — pure decision helpers; pattern for "no IO inside the decider" still applies inside `submit()`'s validation logic
- **Test scenarios:** organized by outcome branch; one test per branch + edge cases.
  - **Happy path (Covers AE1):** valid wanted request, valid resolver mapping, track counts match → outcome `accepted`, download_log_id populated, `FakePipelineDB.download_logs` has a row with `outcome='youtube_running'`
  - **Wrong state (Covers AE2):** request status is `imported` → outcome `wrong_state`, no row inserted
  - **Wrong state — all forbidden statuses:** subTest table over `{downloading, imported, replaced}` → each returns `wrong_state`
  - **Happy path from `manual` (Covers AE9):** valid manual request, valid resolver mapping, track counts match → outcome `accepted`, identical assertions to the wanted happy path; slskd-residue cleanup in `/Incoming/post-validation/` is delegated per D2
  - **In flight (Covers AE3):** prior `youtube_running` row exists → outcome `in_flight`, existing download_log_id returned in result.detail
  - **Request not found:** request_id doesn't exist → outcome `request_not_found`
  - **No resolver mapping (Covers AE4):** browse_id not in any mapping for the request's release group → outcome `no_resolver_mapping`
  - **Track-count precheck mismatch (Covers AE5):** resolver mapping exists but `total_mb_tracks` differs from request MBID's expected → outcome `track_count_precheck_failed`
  - **Transient DB failure:** `FakePipelineDB` raises a transient exception → outcome `transient`
  - **`run_job` happy path (Covers AE7):** mocked ytdlp_runner produces exactly N audio files for an N-track MBID; service stages them, enqueues `youtube_import` (payload contains staged path + request_id + browse_id), transitions row to `youtube_success`
  - **`run_job` track-count mismatch — too few (Covers AE6):** mocked ytdlp_runner produces N-1 files; service short-circuits BEFORE staging, transitions row to `youtube_failed` reason=`track_count_mismatch`, nothing in `/Incoming/auto-import/`
  - **`run_job` track-count mismatch — too many:** same shape, N+1 files; same outcome reason
  - **`run_job` yt-dlp non-zero exit (Covers F4):** classifies exit codes into the structured reason taxonomy per R20; verbatim stderr captured in metadata
  - **`run_job` UTF-8 surrogate in yt-dlp stderr (Covers KTD8):** mocked ytdlp_runner emits raw `0xE2` in stderr; service handles it without crash (verifies the `errors='replace'` discipline)
  - **OUTCOME_HTTP_STATUS / OUTCOME_EXIT_CODE completeness:** every value in `SubmitOutcome` has an entry in both maps; pyright-clean
- **Verification:** `nix-shell --run "python3 -m unittest tests.test_youtube_ingest_service -v"` passes with full branch coverage; no test uses `@unittest.skip`.

### U4. CLI subcommand — `pipeline-cli youtube-rescue <request_id> <browse_id>`

- **Goal:** Thin CLI wrapper around `YoutubeIngestService.submit`. Outcome → exit-code mapping; outputs the new `download_log_id` on success.
- **Requirements:** R1, R5
- **Dependencies:** U3
- **Files:**
  - `scripts/pipeline_cli.py` (extend)
  - `tests/test_pipeline_cli.py` (extend)
- **Approach:**
  - New subparser `youtube-rescue` with `request_id: int` (positional) and `browse_id: str` (positional)
  - `cmd_youtube_rescue` constructs the service with production deps (real `PipelineDB`), calls `service.submit(request_id, browse_id)`, prints structured outcome to stdout (`--json` flag for machine output following the existing CLI convention), returns the exit code from `OUTCOME_EXIT_CODE`
  - Docstring lists every exit code value
- **Patterns to follow:**
  - `scripts/pipeline_cli.py::cmd_search_plan_advance` (line ~2436) — canonical CLI wrapper for a typed-Result service
  - `scripts/pipeline_cli.py::cmd_youtube_album` (line ~2171) — adjacent reference for YT-related CLI shape and `--json` flag handling
- **Test scenarios:**
  - subTest table: each `SubmitOutcome` → expected exit code per `OUTCOME_EXIT_CODE`
  - Success path prints `download_log_id` to stdout (plain text and `--json` modes)
  - Failure paths print classified outcome to stderr; exit code matches mapping
  - `--json` flag produces structured output (machine-readable shape)
- **Verification:** `nix-shell --run "python3 -m unittest tests.test_pipeline_cli -v"` passes.

### U5. HTTP API route — `POST /api/pipeline/<id>/youtube-rescue`

- **Goal:** Thin HTTP wrapper around `YoutubeIngestService.submit`. Pydantic request body, parse_body adapter, outcome → status-code mapping. Add to route audit gate.
- **Requirements:** R1, R5
- **Dependencies:** U3
- **Files:**
  - `web/routes/youtube.py` (extend — module already exists from resolver work)
  - `web/server.py` (extend the `_FUNC_POST_PATTERNS` registration)
  - `tests/test_web_server.py` (extend with contract test + add to `TestRouteContractAudit.CLASSIFIED_ROUTES`)
- **Approach:**
  - `YoutubeRescueRequest(BaseModel)` Pydantic model with `browse_id: str` (the request_id comes from the URL path, not the body)
  - `post_pipeline_youtube_rescue(h, request_id, body)` handler — calls `parse_body(h, body, YoutubeRescueRequest)`, constructs service with production deps, maps `result.outcome` → `OUTCOME_HTTP_STATUS[outcome]` → HTTP status, returns JSON `{download_log_id, outcome, detail}`
  - Register the path pattern in `web/server.py` with a description (audit gate requires non-empty description)
  - Add the route pattern to `TestRouteContractAudit.CLASSIFIED_ROUTES` (audit fails without it)
- **Patterns to follow:**
  - `web/routes/pipeline.py::post_pipeline_search_plan_advance` (line ~988) — canonical HTTP wrapper for a typed-Result service with `parse_body` adapter and status-code mapping
  - `web/routes/youtube.py` — the existing resolver HTTP route module; same module hosts the new rescue route to keep YT surfaces co-located
  - `tests/test_web_server.py::TestPipelineSearchPlanAdvanceContract` (line ~2924) — canonical contract test pattern
  - `tests/test_pydantic_route_audit.py` — POST handlers reading body MUST go through `parse_body`; audit enforces
- **Test scenarios:**
  - Contract: every `SubmitOutcome` → expected HTTP status per `OUTCOME_HTTP_STATUS`
  - `REQUIRED_FIELDS = {"download_log_id", "outcome", "detail"}` on every response (200 + non-200); `_assert_required_fields` helper
  - Request validation: malformed body (missing `browse_id`, wrong type) → 400 via `parse_body`'s `ValidationError → 400` handling
  - `TestRouteContractAudit.CLASSIFIED_ROUTES` includes the new pattern; audit passes
  - `TestPydanticRouteAudit` accepts the handler (uses `parse_body`)
  - **Production-shape mock fidelity** (`.claude/rules/code-quality.md`): the mocked `PipelineDB` returns a row with `datetime` timestamps and real `uuid.UUID` for any UUID columns, not synthetic strs; or pair with an integration slice that round-trips through real serialization
- **Verification:** `nix-shell --run "python3 -m unittest tests.test_web_server tests.test_pydantic_route_audit -v"` passes; `pipeline-cli routes` lists the new route with non-empty description.

### U6. Worker script — `scripts/youtube_ingest_worker.py`

- **Goal:** Long-running drainer that polls `download_log` for `youtube_running` rows, invokes yt-dlp via the service's `run_job`, handles startup orphan recovery. Mirrors `scripts/importer.py` shape.
- **Requirements:** R9, R10, R11, R16, R18, R20, R21, R22
- **Dependencies:** U2, U3
- **Files:**
  - `scripts/youtube_ingest_worker.py` (create)
  - `tests/test_youtube_ingest_worker.py` (create)
- **Execution note:** The yt-dlp subprocess invocation MUST use `text=True, errors='replace'` per KTD8. Add a RED test FIRST (shim binary on PATH emitting bare `0xE2` in stderr) that verifies the worker survives non-UTF-8 yt-dlp output.
- **Approach:**
  - argparse: `--poll-interval` (default 5.0s, matching importer), `--temp-dir` (where yt-dlp writes before staging), `--db-dsn` (or env var fallback)
  - Main loop:
    1. Acquire `ADVISORY_LOCK_NAMESPACE_YOUTUBE_INGEST` advisory singleton lock (release on shutdown via `with` statement)
    2. Startup orphan sweep — call `find_orphan_youtube_running`, call `update_youtube_terminal(id, 'youtube_failed', {reason: 'worker_died', ...})` on each
    3. Poll loop:
       - `find_next_youtube_pending(limit=1)` → if none, sleep `poll_interval`
       - If one, construct `YoutubeIngestService` with production deps, call `service.run_job(download_log_id)`
       - Catch unexpected exceptions, log them, write `youtube_failed` with reason `worker_unhandled_exception` and the stack trace excerpt — never let an unhandled exception kill the worker
  - Subprocess invocation helper: `_run_ytdlp(url, output_dir, timeout_sec) -> (exit_code, stderr_excerpt, file_list)`. Uses `subprocess.run(..., text=True, errors='replace', capture_output=True, timeout=...)`. yt-dlp argv: `[ytdlp_bin, '--no-ignore-errors', '-f', 'bestaudio', '--output', '<template>', '--max-downloads', str(expected_count), '--', url]` — the `--` separator before the URL positional is required defense-in-depth so a future resolver-row drift producing a `yt_url` starting with `-` cannot be parsed as a flag. **Stderr capture is capped at 4 KiB after the `errors='replace'` decode** before being returned for storage in `YoutubeIngestMetadata.stderr_excerpt`; truncation happens in this helper so the JSONB column has a bounded shape regardless of how verbose yt-dlp gets on stuck-pattern failures (429 storms, region-locked playlists)
  - Signal handling: graceful shutdown on SIGTERM (let in-flight job complete; release advisory lock; exit cleanly so `Restart=on-failure` doesn't fire)
- **Patterns to follow:**
  - `scripts/importer.py` — canonical long-running worker template (advisory lock acquisition, startup orphan sweep at `recover_abandoned_running_jobs`, drain loop with `--poll-interval`, signal handling)
  - `lib/pipeline_db.py::ADVISORY_LOCK_NAMESPACE_IMPORTER` constant (line ~123) — adjacent namespace constants
  - `docs/solutions/subprocess-text-mode-utf8-strict-decode-crash.md` — required for yt-dlp invocation
- **Test scenarios:**
  - **Startup orphan sweep:** seed `FakePipelineDB` with 2 `youtube_running` rows; run startup sweep; both transition to `youtube_failed` with reason `worker_died`
  - **Drain loop happy path:** queue one `youtube_running` row; provide a fake `ytdlp_runner_fn` that returns N files; worker processes the row, terminal state is `youtube_success`, a `youtube_import` job_type appears in `FakePipelineDB.import_jobs` with `staged_path` populated in the payload
  - **Stderr cap (Covers KTD8 size bound):** mocked ytdlp_runner emits 100 KiB of stderr text; `_run_ytdlp` returns a 4 KiB excerpt; assert no JSONB row in `download_log` ever stores more than the cap regardless of input volume
  - **`--` separator argv shape:** assert the constructed argv list contains `'--'` immediately before the URL positional; regression guard against future implementations that drop the separator
  - **Drain loop empty queue:** no `youtube_running` rows → worker sleeps `poll_interval`; assert sleep called with right value (mock `time.sleep`)
  - **UTF-8 surrogate in yt-dlp stderr (RED test mirroring docs/solutions pattern):** shim binary on PATH emits raw `0xE2` in stderr; worker calls `_run_ytdlp`; assert no UnicodeDecodeError, stderr captured with replacement char
  - **Unhandled exception in service.run_job:** mocked service raises a non-classified exception; worker catches, writes `youtube_failed` reason=`worker_unhandled_exception`, continues to next iteration (does not crash)
  - **Signal handling:** SIGTERM during sleep → worker exits cleanly with exit code 0
  - **Advisory lock contention:** second worker startup with the lock already held → exit immediately with logged message (don't spin)
- **Verification:** `nix-shell --run "python3 -m unittest tests.test_youtube_ingest_worker -v"` passes; manual smoke on doc2 against a real (low-traffic) YT playlist URL after deploy.

### U9. Importer + preview-worker dispatch for `youtube_import` job_type

- **Goal:** Wire the new `youtube_import` job_type into the existing preview → importer chain. New dispatcher in `scripts/importer.py` (`execute_youtube_import_job`) and parallel branch in `scripts/import_preview_worker.py::_front_gate_source_path` that source the staged path from `import_jobs.payload['staged_path']` instead of from `album_requests.active_download_state`. Reuses the rest of the existing per-job pipeline (preview measurement, quality gate, beets distance, wrong-matches OR auto-import, `mark_imported_with_rescue` for the terminal status flip).
- **Requirements:** R11, R17
- **Dependencies:** U2 (job_type constant + payload helper must exist)
- **Files:**
  - `scripts/importer.py` (extend — new `execute_youtube_import_job` dispatcher, registration in the job_type → handler map)
  - `scripts/import_preview_worker.py` (extend — new branch in `_front_gate_source_path` for `IMPORT_JOB_YOUTUBE`)
  - `tests/test_importer.py` (extend)
  - `tests/test_import_preview_worker.py` (extend)
- **Execution note:** Add the new dispatcher BEFORE the YT worker (U6) lands so an `automation_import`-shaped staging flow in U6 has somewhere to dispatch to. Concretely: this unit's tests run against the existing automation_import-shaped pipeline with a `youtube_import` job_type as the only differentiator — they should all pass with U1/U2 merged but before U6 is wired.
- **Approach:**
  - `execute_youtube_import_job(db, job, ...)` reads `staged_path` from `job.payload` (decoded via `msgspec.convert(job.payload, type=YoutubeImportPayload)` for wire-boundary safety) and calls into the same per-job import pipeline as `execute_automation_import_job` — but the path comes from the payload, NOT from `active_download_state` (which is left untouched per KTD1). On import success, the existing `mark_imported_with_rescue` is called source-agnostically (R17); `rescued_at` and `prior_unfindable_category` populate for free
  - `_front_gate_source_path` extension: when `job.job_type == IMPORT_JOB_YOUTUBE`, return `job.payload['staged_path']` directly (after the `msgspec.convert` type check); skip the `active_download_state` lookup entirely
  - No cooldown side-effects: the slskd cooldown machinery is keyed on peer usernames which YT never produces. The dispatcher path explicitly does NOT invoke `denylist_user` or any peer-tracking call. Regression guard in tests.
- **Patterns to follow:**
  - `scripts/importer.py::execute_automation_import_job` — the structural sibling. New dispatcher mirrors its shape but reads path from payload instead of `active_download_state`.
  - `scripts/import_preview_worker.py::_front_gate_source_path` for `IMPORT_JOB_AUTOMATION` — the structural sibling for the preview-worker branch
  - `lib/pipeline_db.py::mark_imported_with_rescue` — source-agnostic terminal-state write; no change needed here, just confirmation that it's the single call site
- **Test scenarios:**
  - **Happy path (Covers AE7):** queue a `youtube_import` job with `staged_path=/Incoming/auto-import/<artist>-<album>/` and the request's MBID; preview worker measures → importer imports → `mark_imported_with_rescue` flips status to `imported` and writes `rescued_at`
  - **Rescue from `manual` (Covers AE9):** same as above but the request started in `manual` status; the final state transition is `manual → imported`; rescued_at populated
  - **Long-tail rescue audit (Covers AE8):** request previously had `unfindable_category='wrong_pressing_available'`; YT rescue completes via this dispatcher; `prior_unfindable_category` is populated with the prior value atomically via the existing `mark_imported_with_rescue` write
  - **Preview-worker path resolution:** for an `IMPORT_JOB_YOUTUBE` job, `_front_gate_source_path` returns the payload's `staged_path`; for an `IMPORT_JOB_AUTOMATION` job, returns the existing `active_download_state`-derived path; the two branches are independent
  - **No cooldown leakage:** dispatcher runs through wrong-matches routing (beets distance fails) and through quality-reject routing; assert `FakePipelineDB.denylist` remains empty (no slskd peer to denylist, no synthetic peer key being smuggled in)
  - **Payload type-validation:** a malformed `import_jobs.payload` (missing `staged_path` key) raises `msgspec.ValidationError` at the read seam; the dispatcher logs and skips the job rather than crashing the worker
- **Verification:** `nix-shell --run "python3 -m unittest tests.test_importer tests.test_import_preview_worker -v"` passes; manual smoke after deploy confirms a YT rescue ends with `rescued_at` populated and the album in beets.

---

### U7. Nix module wiring — `cratedigger-youtube-ingest.service` + yt-dlp packaging

- **Goal:** Define the new long-running systemd unit in the in-flake `nix/module.nix`. Expose options the downstream wrapper consumes for network hardening. Package `pkgs.yt-dlp` for the worker's PATH (worker-only, not added to other services' runtimePath).
- **Requirements:** R18, R19
- **Dependencies:** U6
- **Files:**
  - `nix/module.nix` (extend)
  - `nix/package.nix` (extend — runtime deps for the worker)
  - `nix/shell.nix` (extend — yt-dlp available in the dev shell so tests can shim it)
- **Approach:**
  - New systemd unit: `cratedigger-youtube-ingest.service`, `Type=simple`, `Restart=on-failure`, `RestartSec=5`, **`restartIfChanged = true`** (deliberate — picks up code changes on deploy)
  - `ExecStart` invokes the worker script via `pkgs.writeShellScriptBin` wrapper, with `yt-dlp` on PATH
  - `Requires=cratedigger-db-migrate.service` so migration 037 is applied before the worker starts
  - `Environment=PIPELINE_DB_DSN=...` per existing pattern
  - **No `RuntimeMaxSec`** for this unit (long-running daemon — `RuntimeMaxSec` would force periodic restarts); a per-job yt-dlp timeout lives in the worker script itself
  - Per `docs/solutions/runtimemaxsec-vs-type-oneshot-systemd-incompatibility.md`: confirmed this is `Type=simple`, not `Type=oneshot`; `RuntimeMaxSec`-style controls would be a footgun here
  - New options surface for the downstream wrapper:
    - `services.cratedigger.youtubeIngest.enable: bool` (defaults to `false` so the in-flake module is opt-in)
    - `services.cratedigger.youtubeIngest.tempDir: path` (defaults to `${cfg.stateDir}/youtube-ingest-temp`)
    - `services.cratedigger.youtubeIngest.pollIntervalSeconds: int` (defaults to 5)
  - Downstream-wrapper-side hardening (NOT in this PR — but documented as the consumer contract in CLAUDE.md update at U8): `serviceConfig.NetworkNamespacePath`, `BindReadOnlyPaths`, etc., are layered on at `~/nixosconfig/modules/nixos/services/cratedigger.nix`
  - VM check: extend `flake.nix#checks.moduleVm` test scenarios to verify the new unit comes up cleanly (mocked yt-dlp on PATH so no real network calls)
- **Patterns to follow:**
  - `nix/module.nix` — existing service definitions for `cratedigger-importer` (line ~1035-1063), `cratedigger-import-preview-worker` (line ~1065-1088), `cratedigger-web` (line ~1090-1105). New unit mirrors the importer-shape exactly
  - `flake.nix#checks.moduleVm` (existing VM check pattern for module verification)
- **Test scenarios:**
  - VM check: unit comes up, advisory lock acquired, idle (no jobs to drain) — `journalctl -u cratedigger-youtube-ingest` shows the expected startup log
  - VM check: a second-instance attempt fails fast (advisory lock contention) per U6 worker behavior
  - VM check: `which yt-dlp` succeeds inside the unit's process environment
  - Migration ordering: the unit's `Requires=cratedigger-db-migrate` means a fresh VM boots the migration first, then the worker (verify via systemd dependency graph)
- **Verification:** `nix build .#checks.x86_64-linux.moduleVm` passes; after live deploy, `ssh doc2 'sudo systemctl is-active cratedigger-youtube-ingest'` returns `active`; `journalctl -u cratedigger-youtube-ingest --since "5 min ago"` shows startup sweep + idle poll messages.

### U8. Documentation — CLAUDE.md subsystem entry + schema doc update

- **Goal:** Update CLAUDE.md with a new subsystem entry for the YT ingest service. Update `docs/pipeline-db-schema.md` with the new `download_log.source` and `download_log.youtube_metadata` columns. Note the new advisory lock namespace in `docs/advisory-locks.md` if it exists, otherwise inline in CLAUDE.md.
- **Requirements:** none directly (documentation hygiene)
- **Dependencies:** U1, U2, U3, U6, U7, U9
- **Files:**
  - `CLAUDE.md` (extend the Subsystems section)
  - `docs/pipeline-db-schema.md` (extend the `download_log` column inventory)
  - `docs/advisory-locks.md` (extend if it exists)
- **Approach:**
  - CLAUDE.md subsystem entry summarizes: new service, where it lives in the architecture, what triggers it (operator-initiated only), what it produces (staged dir + `automation_import` job), the strict-pressing invariants enforced, and the audit chain (download_log row, source discriminator, JSONB blob). Cross-reference this plan and the brainstorm doc
  - Schema doc gets the new column rows + the partial unique index documented
  - Add a one-paragraph note on the downstream-wrapper contract for network hardening (what options the in-flake module exposes; what the wrapper is expected to set)
- **Patterns to follow:**
  - CLAUDE.md's existing "Subsystems" section structure
  - `docs/pipeline-db-schema.md`'s existing column-inventory shape
- **Test scenarios:**
  - `Test expectation: none -- documentation update`
- **Verification:** Manual review; `git grep "youtube-ingest"` in CLAUDE.md returns a hit.

---

## Acceptance Examples

Carried verbatim from `docs/brainstorms/2026-05-28-youtube-rescue-ingest-api-requirements.md` § "Acceptance Examples" and mapped to the U-IDs that satisfy them. AE bodies live in the origin doc; the mapping below is the traceability chain.

| AE | Coverage |
|---|---|
| AE1 | U3 (service happy-path test), U4 (CLI), U5 (API contract) |
| AE2 | U3 (wrong-state test) |
| AE3 | U1 (partial unique index), U3 (in-flight outcome), U5 (API contract returns existing download_log_id) |
| AE4 | U3 (no-resolver-mapping outcome) |
| AE5 | U3 (track-count precheck outcome) |
| AE6 | U3 (`run_job` track-count mismatch — both directions) |
| AE7 | U3 (`run_job` happy-path), U6 (worker drain loop), U9 (importer dispatcher processes the `youtube_import` job to terminal state) |
| AE8 | U9 (dispatcher calls `mark_imported_with_rescue` source-agnostically); existing `mark_imported_with_rescue` (`lib/pipeline_db.py:2230`) — no change needed |
| AE9 | U3 (rescue-from-`manual` path is identical to wanted), U9 (dispatcher transitions `manual → imported`); convergence handles `/Incoming/post-validation/` cleanup (D2 dependency) |
| AE10 | U2 (`find_orphan_youtube_running` query), U6 (startup orphan sweep) |
| AE11 | U2 (read-seam updates), existing `pipeline-cli show` and web routes (rendering inherited from the `source` column) |

---

## System-Wide Impact

- **`download_log` schema:** every consumer of `download_log` rows (`pipeline-cli show`, web routes rendering "recent attempts", any analytics queries) inherits the new `source` and `youtube_metadata` columns. U2 widens the read seams; consumers receive the columns automatically.
- **`import_jobs.job_type` vocabulary widens.** New `youtube_import` value alongside the existing `automation_import` / `force_import` / `manual_import`. Any future code branching on `job_type` (currently the importer dispatcher table and the preview-worker front-gate) must handle the new value; U9 covers both sites. Existing rows are unaffected (no schema migration needed for `import_jobs` — the column is already free-form TEXT and the new value is dispatcher-aware).
- **Advisory lock namespace registry:** new namespace constant added to `lib/pipeline_db.py`. Other workers are unaffected (different namespace integer).
- **New systemd unit:** the in-flake module's `services.cratedigger.youtubeIngest.enable = false` default means the unit ships dormant on first deploy. The downstream wrapper at `~/nixosconfig/modules/nixos/services/cratedigger.nix` enables and network-hardens it as a follow-up; once enabled, `restartIfChanged = true` ensures subsequent deploys pick up code changes automatically.
- **Egress traffic:** YouTube Music outbound is a new egress destination from doc2. Without the downstream wrapper's network namespace, traffic exits the host's default route. This is the operator's known constraint (the whole point of the wrapper layering); the plan structurally accommodates it but does NOT enable it.
- **Subprocess: yt-dlp.** New runtime dependency; new wire boundary. The UTF-8 strict-decode hazard is mitigated per KTD8 + the RED test in U6.

---

## Scope Boundaries

Carried from `docs/brainstorms/2026-05-28-youtube-rescue-ingest-api-requirements.md` § "Scope Boundaries".

- **UI work** — no frontend changes. The future rescue button is downstream consumer work.
- **Wrong-matches routing of low-quality YT rescues** — not a bug; intended consequence of "same gates apply." The YT path hands a staged directory to the importer; everything downstream is unchanged.
- **Codec normalisation at staging** — yt-dlp's `bestaudio` output passes through unchanged.
- **Authenticated YouTube access** — anonymous only.
- **Automatic retry of failed rescues** — terminal; operator decides on resubmit.
- **Automated YouTube rescue daemon** — operator-initiated only.
- **Cleanup of stale `/Incoming/post-validation/` files** when rescue from `manual` succeeds — delegated to existing convergence per invariant #7. If convergence does not yet cover this, it ships as a separate issue.

### Deferred to Follow-Up Work

- **Downstream wrapper update** — `~/nixosconfig/modules/nixos/services/cratedigger.nix` needs to set `services.cratedigger.youtubeIngest.enable = true` and layer on network-namespace / VPN binding. This is operator-side configuration on doc1, not a cratedigger PR. Listed here so it's not silently dropped post-merge.
- **Doc convergence audit** — confirm that existing convergence cleans up stale `/Incoming/post-validation/` files when a `manual`-status request transitions to `imported` via YT rescue. If not, file an issue.
- **`docs/solutions/` writeups** — once the worker has shipped and survived its first month, write up any new lessons (network-namespace+systemd patterns, yt-dlp gotchas) under `docs/solutions/`.

---

## Risks & Dependencies

### Risks

- **yt-dlp's bestaudio format selection drifts.** YouTube Music occasionally rotates audio encodings; yt-dlp adapts but a release may briefly produce unexpected codecs (e.g. Opus → AAC). Mitigation: the codec passes through the existing quality measurement pipeline honestly, so a codec change shows up as a quality-gate decision change, not a crash. No special handling needed in this code.
- **Track-count mismatch via YT Music silent edits.** YouTube Music occasionally adds bonus tracks to existing playlists (live cuts, demos). The resolver's cached `total_mb_tracks` would drift from the actual playlist's track count. Mitigation: the worker-side post-yt-dlp count gate (R10) catches drift between resolver cache and reality. Failure outcome is `track_count_mismatch`; operator can re-trigger the resolver to refresh and try again.
- **Network egress detection.** YouTube has occasionally returned 429 / CAPTCHA challenges to bare-IP scrapers. Mitigation: the downstream wrapper's VPN binding is the operator's defense; the in-flake module accepts this as the operator's responsibility. The structured `youtube_unknown` failure reason captures unclassified 4xx/5xx responses.
- **Subprocess UTF-8 strict-decode latent bug class.** Mitigated by KTD8 + the RED test in U6; explicit `errors='replace'` discipline on every yt-dlp subprocess call.

### Dependencies

- **D1. YouTube resolver mapping exists for the request's release group.** Submission validation depends on `youtube_album_mappings` being populated. If not, the rescue API returns 422.
- **D2. Existing convergence cleans up `/Incoming/post-validation/` orphans** when a `manual` request transitions to `imported` via YT rescue. If not yet covered, ships as a follow-up issue.
- **D3. `pkgs.yt-dlp` available in nixpkgs.** Confirmed; no pinning needed.
- **D4. Existing importer worker behavior unchanged.** This plan hands a staged directory + `automation_import` job to the importer; structural confidence.
- **D5. Resolver's cached `total_mb_tracks` is accurate per browse_id.** Submission precheck (R7) trusts this; worker-side gate (R10) is the authoritative check.

---

## Sources / Research

- **Origin:** `docs/brainstorms/2026-05-28-youtube-rescue-ingest-api-requirements.md` — the brainstorm doc this plan implements.
- **Canonical CLI ⇄ API symmetry pattern:** `lib/search_plan_service.py::SearchPlanService.advance_for_request` (service), `scripts/pipeline_cli.py::cmd_search_plan_advance` (CLI wrapper), `web/routes/pipeline.py::post_pipeline_search_plan_advance` (HTTP wrapper), `tests/test_web_server.py::TestPipelineSearchPlanAdvanceContract` (contract test). Service-first-then-glue lesson: `docs/solutions/architecture/service-first-then-glue.md`.
- **Canonical long-running worker:** `scripts/importer.py` — advisory singleton lock, startup orphan sweep, poll loop with `--poll-interval`. Adjacent: `scripts/import_preview_worker.py`.
- **Wire-boundary type discipline:** `.claude/rules/code-quality.md` § "Wire-boundary types"; recent reference impl `lib/youtube_album_service.py` (the `PersistedTrack` / `PersistedDistance` / `PersistedYoutubeRow` Structs).
- **Test fidelity rules:** `.claude/rules/test-fidelity.md` — Rule A (real-PG round-trip for new DB writes) is load-bearing for U2; Rule B (fakes mirror real exception contracts) applies to U3 and U6.
- **Subprocess UTF-8 hazard:** `docs/solutions/subprocess-text-mode-utf8-strict-decode-crash.md` — required pattern for U6 yt-dlp invocation.
- **systemd `Type` ↔ timeout directive:** `docs/solutions/deployment/runtimemaxsec-vs-type-oneshot-systemd-incompatibility.md` — informed KTD's choice of `Type=simple` + per-job timeout in the worker rather than `RuntimeMaxSec` on the unit.
- **Pipeline DB schema:** `docs/pipeline-db-schema.md` — current column inventory; U1 extends.
- **NixOS module layering:** `docs/nixos-module.md` — in-flake module / downstream wrapper split that R18/R19 + KTD9 reference.
- **CLAUDE.md archivist invariants** — single-operator rule (no backfill scripts; U1's single-statement DEFAULT-based migration), forward-only schema discipline, "the request is the source of truth" (R17 — no new state on `album_requests`).

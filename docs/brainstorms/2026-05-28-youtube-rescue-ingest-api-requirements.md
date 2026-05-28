---
date: 2026-05-28
topic: youtube-rescue-ingest-api
---

# YouTube Rescue Ingest API

## Summary

A new `cratedigger-youtube-ingest` background service exposes a CLI + HTTP API that, given an existing `wanted`-or-`manual` request and a resolver-supplied YouTube Music `browse_id`, runs yt-dlp under network-namespace hardening, enforces a hard track-count gate before staging, and hands the result to the existing importer worker. `download_log` doubles as both queue state and YouTube audit trail — no new state on `album_requests`. Operator-initiated only; the operator's rescue button is a downstream UI consumer, not in this scope.

---

## Problem Frame

A meaningful slice of the operator's long-tail catalogue genuinely cannot be sourced from Soulseek at any given moment — niche pressings, regional indie, BBC PlaySchool curiosities, demos that lived on one peer who logged off years ago. Cratedigger's existing watch loop will surface these unfindable requests forever (R20 — never stop searching), and rescue them naturally when a peer eventually appears. But for some albums, YouTube Music IS the only surface where the audio actually exists today, particularly for releases whose rightsholders uploaded official archival content directly to a YT Music album entity. Gelbison's EPs are the operator's working example: discoverable on YouTube Music, but absent from every Soulseek peer the watch loop has ever encountered.

The YouTube resolver landed last week (`docs/brainstorms/2026-05-27-youtube-music-album-resolver-requirements.md`, PR #383/#384). It answers "given this MBID, what are the matching YT Music albums and how well does each one score against the MB pressing?" It is read-only metadata — no audio, no ingestion. The natural sibling is an ingestion path: the resolver tells you WHAT to grab on YouTube; this API is the path that grabs it and pushes it through the same import pipeline a Soulseek download flows through. Same gates, same audit shape, same wrong-matches routing — yt-dlp is just a different way of populating `/Incoming/auto-import/`, and from the importer's perspective the staged directory is indistinguishable from any other.

The product narrative this enables: a request that's been categorised `unfindable` for weeks transitions to `imported` because the operator found it on YouTube Music and clicked rescue. That rescue event populates `rescued_at` + `prior_unfindable_category` exactly as a Soulseek rescue would — the long-tail-rescue audit trail stays source-agnostic, and a separate `download_log.source` discriminator one layer down tells the operator which channel actually delivered.

---

## Key Decisions

- **`download_log` doubles as both the in-flight queue and the audit trail.** A new sidecar table for YT ingest jobs would force the web UI queue, triage filters, and orphan-detection logic to learn about a new state machine. Instead, the existing `download_log` row's `outcome` field carries `youtube_running` while the worker is mid-rescue, and transitions to a terminal `youtube_success` / `youtube_failed` value when done. A new `source` discriminator column on the row (`'slskd'` default for existing rows, `'youtube'` for these) tells consumers which sourcing channel produced the attempt. The audit row IS the queue entry — same artifact, two purposes.

- **`album_requests.status` is never touched by the YT ingest path.** Flipping a row to `downloading` because yt-dlp is in flight would collide with the existing `poll_active_downloads` orphan-detection logic (which expects every `downloading` row to have a matching slskd transfer). Adding a sidecar status column would force every consumer of "request state" — the web UI queue, the wrong-matches view, triage filters — to learn about the new tag. The row stays `wanted` (or `manual`) throughout; the YT attempt is visible via the new `download_log` row, which existing render paths already display under "recent attempts."

- **All-or-nothing yt-dlp, with a hard track-count gate at the worker layer before staging.** Strict-pressing identity (R20) wins over partial-rescue convenience. yt-dlp is invoked without `--ignore-errors`; any single track failure aborts the run. Before files cross into `/Incoming/auto-import/`, the worker verifies `count(staged audio files) == source-aware expected track count` (MB mirror for MB requests, stored/exact cached Discogs count for Discogs-only requests). Less != wanted; more != wanted; only equal-then-content-validated reaches the importer. This makes the YT-import success criterion fully transitive: track count + beets distance + quality gate, three aligned gates.

- **Resolver-required input.** The API accepts the resolver's `browse_id` (or `audio_playlist_id`), not a raw YouTube URL. Submission-time validation requires an existing resolver mapping row for the request's release group; out-of-band YT URLs are rejected. Three structural wins: every rescue is guaranteed to have been beets-scored against MB before the rescue button is even clickable; the resolver's cached `total_mb_tracks` enables the pre-yt-dlp track-count check; the audit row naturally references the resolver row.

- **Async via a new `cratedigger-youtube-ingest.service` systemd unit.** Synchronous in-handler execution would block the HTTP request for the 2-5 minutes yt-dlp typically takes. An in-process background thread on `cratedigger-web` would tangle a long-running subprocess workload into the HTTP server's process. The dedicated service is its own ownership boundary, mirrors the existing importer/preview-worker pattern, AND lets the operator shape its outbound traffic (network namespace / VPN tunnel) without touching anything else. The Nix-level hardening lives in the downstream wrapper at `~/nixosconfig/modules/nixos/services/cratedigger.nix`; the in-flake `nix/module.nix` exposes the unit but does not opinionate on its network shape.

- **Quality gate posture: same gates apply, with no special treatment for YT.** Wrong-matches landing for YT rescues is the intended consequence of the gate architecture, not a bug to engineer around. yt-dlp's `bestaudio` output (Opus at ~128 kbps) passes through to staging unchanged; the existing quality-measurement pipeline reads it honestly and the default EXCELLENT gate rejects it. Auto-import requires the operator to pre-lower `min_bitrate` on the request (same mechanism as accepting a lower-tier slskd result). The YT ingest service hands off to the importer at the staged-directory boundary; everything downstream is the importer's concern.

---

## Actors

- **A1. Operator** — initiates the rescue via CLI or web API. Provides the request ID and a resolver-supplied browse_id. Not currently UI-driven (UI deferred).
- **A2. `cratedigger-youtube-ingest` service** — new long-running systemd worker. Polls `download_log` for `youtube_running` rows, invokes yt-dlp, enforces the track-count gate, stages files, enqueues the import job. Network-isolated by the downstream wrapper.
- **A3. Existing `cratedigger-importer` worker** — drains the import queue. Owns beets mutations. Unchanged by this work; sees the staged YT directory exactly as it would a slskd-staged directory.
- **A4. YouTube resolver service** — existing read-only metadata bridge. Provides the input contract: a `browse_id` (and derivable `audio_playlist_id` / `yt_url` / track-count) that the rescue API accepts and validates against.

---

## Key Flows

- **F1. Happy-path rescue from `wanted`**
  - **Trigger:** operator hits the API endpoint (or CLI subcommand) with `(request_id, browse_id)`
  - **Steps:**
    1. API validates request exists, status is `wanted` or `manual`, no `youtube_running` row already exists for this request
    2. API validates a resolver mapping row exists for the request source containing the supplied browse_id, and the cached/stored track count for that browse_id matches the request source's expected count
    3. API inserts a `download_log` row with `outcome='youtube_running'`, `source='youtube'`, and a YT-metadata JSONB blob; returns 200 with the new download_log_id
    4. The ingest worker picks up the row on its next poll cycle, derives the playlist URL from the resolver's `audio_playlist_id`, runs yt-dlp (no `--ignore-errors`)
    5. yt-dlp completes; worker verifies the actual staged track count matches the source-aware expected track count
    6. Worker stages the directory to `/Incoming/auto-import/<artist>-<album>/`, inserts an `import_jobs` row pointing to that path with the request id, updates the `download_log` row to `outcome='youtube_success'`
    7. The existing importer worker picks up the import_job and runs the standard pipeline (beets validation, quality gate, auto-import OR wrong-matches routing)
    8. On import success, the existing pipeline populates `album_requests.rescued_at` + `prior_unfindable_category` per existing behavior
  - **Outcome:** the album imports (or routes to wrong-matches) through the existing pipeline; the download_log row carries the full YT audit trail
  - **Covers:** R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R15

- **F2. Hard track-count mismatch — pre-staging abort**
  - **Trigger:** yt-dlp completes; worker counts files in the temp staging dir
  - **Steps:**
    1. Worker compares `count(staged audio files)` against the source-aware expected track count
    2. Counts don't match
    3. Worker discards the temp directory (nothing reaches `/Incoming/auto-import/`)
    4. Worker updates the `download_log` row to `outcome='youtube_failed'` with a `track_count_mismatch` reason and the observed-vs-expected counts in the JSONB
  - **Outcome:** the request stays `wanted` (or `manual`); the operator sees a failed rescue with a specific reason; no spurious wrong-matches routing happens
  - **Covers:** R7, R10, R19

- **F3. Rescue from `manual` — slskd residue handling**
  - **Trigger:** operator triggers rescue against a request in `manual` status (slskd-staged files exist in `/Incoming/post-validation/<this album>/`)
  - **Steps:** identical to F1 through step 7
  - **Outcome:** if YT rescue imports successfully, the request transitions to `imported`; the leftover slskd files in `/Incoming/post-validation/` become stale. Cleanup of those stale files is delegated to existing convergence (per invariant #7); if convergence does not yet cover this path, it ships as a follow-up issue, NOT as bespoke teardown inside the YT ingest service.
  - **Covers:** R3 (allowed-states), and depends on D2 (convergence coverage)

- **F4. yt-dlp failure modes (404, age-gate, region-lock, video removed, network timeout)**
  - **Trigger:** yt-dlp exits non-zero (any reason)
  - **Steps:**
    1. Worker captures yt-dlp's stderr + exit code
    2. Worker classifies the failure into a structured reason (`youtube_404`, `youtube_age_gated`, `youtube_region_locked`, `youtube_video_removed`, `youtube_transient_network`, `youtube_unknown`)
    3. Worker updates the `download_log` row to `outcome='youtube_failed'` with the classified reason and verbatim stderr captured in the JSONB
    4. Worker discards any partial files
  - **Outcome:** the request stays `wanted` (or `manual`); the operator gets a triage-able failure reason; they may retry later (a video that's region-locked today might not be tomorrow) or move on. No automatic retry.
  - **Covers:** R20, R21, R22

- **F5. Worker restart with orphaned `youtube_running` rows**
  - **Trigger:** the ingest worker process starts (cold start OR after crash)
  - **Steps:**
    1. On startup, worker scans `download_log` for rows with `outcome='youtube_running'` and `source='youtube'`
    2. Any such row represents a job that was in flight when the previous worker process died
    3. Worker marks each orphan as `outcome='youtube_failed'` with reason `worker_interrupted`
    4. Worker discards any partial staged files for those jobs
  - **Outcome:** the system self-recovers; requests return to their pre-rescue state; operator can retry
  - **Covers:** R23

- **F6. Idempotency — duplicate rescue submission**
  - **Trigger:** operator submits a second rescue API call for a request that already has a `youtube_running` download_log row
  - **Steps:**
    1. API discovers the existing in-flight row during validation
    2. API returns 409 with a reason indicating an in-flight rescue exists, plus the existing download_log_id for reference
  - **Outcome:** no duplicate worker job; the existing in-flight job continues
  - **Covers:** R4

---

## Requirements

### API surface

- R1. The system exposes both a CLI subcommand (`pipeline-cli`) and an HTTP API endpoint that wrap a shared service-layer method. Outcome → exit-code / outcome → HTTP-status mappings follow the existing CLI ⇄ API symmetry convention (200/0 success, 404/2 request not found, 409/4 wrong state OR in-flight rescue exists, 422/3 resolver row missing or track-count mismatch precheck, 503/5 transient).
- R2. The input contract is `(request_id, browse_id)`. The browse_id MUST be the YouTube Music album browseId form (the resolver's `yt_browse_id` column), not a raw URL or playlist ID.
- R3. The API accepts rescue submissions ONLY for requests in `wanted` or `manual` status. All other statuses (`downloading`, `imported`, `replaced`) are rejected with a 409 (wrong-state).
- R4. The API enforces idempotency: if a `download_log` row with `outcome='youtube_running'` and `source='youtube'` already exists for the given request, the API returns 409 with the existing download_log_id rather than enqueuing a second job.
- R5. The API returns 200 with the new `download_log_id` as soon as the row is persisted; it does NOT wait for yt-dlp to complete. The HTTP request returns in milliseconds.

### Resolver coupling

- R6. The API validates at submission time that a YouTube resolver mapping row exists for the request's release group AND contains the supplied browse_id. Submissions whose browse_id is not in the resolver's mapping for that release group are rejected with 422.
- R7. The API additionally validates at submission time that the selected resolver row has a defensible expected track count for the request source. MB requests compare the resolver's cached `total_mb_tracks` against the MB mirror; Discogs-only requests require stored `album_tracks` or an exact cached Discogs distance count. Submissions failing this precheck are rejected with 422 BEFORE the worker is invoked. This is a cheap fail-fast; the worker still re-verifies post-yt-dlp.
- R8. The API derives the yt-dlp invocation URL from the resolver row's `yt_url` (or constructs it from `audio_playlist_id`). The operator never supplies a URL directly.

### Worker behavior

- R9. yt-dlp is invoked WITHOUT `--ignore-errors`. Any single video failure in the playlist aborts the rescue. yt-dlp output codec is whatever its `bestaudio` heuristic picks (typically Opus from YouTube Music); no transcoding happens at staging time.
- R10. After yt-dlp exits zero, the worker verifies the actual count of audio files in the temp staging directory matches the request source's expected track count BEFORE any file moves into `/Incoming/auto-import/` (MB mirror count for MB requests; stored/exact cached Discogs count for Discogs-only requests). A mismatch (less or more) aborts the rescue with `outcome='youtube_failed'`, reason `track_count_mismatch`. The importer never sees a partial fileset.
- R11. On track-count success, the worker stages files to `/Incoming/auto-import/<artist>-<album>/` (the same convention as slskd-staged auto-import rescues) and inserts a row into the existing `import_jobs` table pointing at the staged path with the request id. From that point on, the existing importer worker handles everything.
- R12. The worker authenticates anonymously to YouTube (no Google account, no OAuth, no cookies). Age-gated content fails per R20's failure-mode taxonomy and is not retried with auth.

### Audit and persistence

- R13. A new `source` discriminator column on `download_log` distinguishes sourcing channels: `'slskd'` (the existing default, backfilled for pre-existing rows) and `'youtube'`. The column is the only structural addition to the table.
- R14. New `outcome` values on `download_log` capture the YT ingest lifecycle: `'youtube_running'` (in-flight), `'youtube_success'` (yt-dlp + staging + import-job-enqueue all completed; import lifecycle is the importer's responsibility from there), `'youtube_failed'` (any failure). Existing outcome values are unchanged.
- R15. YT-specific metadata rides in a new JSONB blob on the `download_log` row, carrying at minimum: `yt_url`, `browse_id`, `audio_playlist_id`, per-track video IDs (when known), resolver-mapping-row reference, classified failure reason (on failure), verbatim yt-dlp stderr excerpt (on failure), and the observed-vs-expected track counts (on track-count-mismatch).
- R16. The `download_log` row is the queue entry. The ingest worker's job-pickup query is `SELECT ... WHERE source='youtube' AND outcome='youtube_running' ORDER BY created_at LIMIT 1`. No separate jobs table exists for YT ingest.
- R17. `album_requests` gets ZERO new columns from this work. The existing `rescued_at` and `prior_unfindable_category` populate on YT-sourced imports exactly as they do on slskd-sourced imports — the audit chain is "rescued_at populated → look at download_log for the rescue attempt → source='youtube' tells you it was a YT rescue."

### Network hardening

- R18. The ingest worker runs as its own systemd unit (`cratedigger-youtube-ingest.service`), defined as a long-running service in the in-flake module at `nix/module.nix`. The in-flake module exposes the unit but does not opinionate on its outbound network shape.
- R19. The downstream wrapper at `~/nixosconfig/modules/nixos/services/cratedigger.nix` is the layer where the operator applies network-namespace / VPN hardening to the unit. The unit must be structured so all yt-dlp egress flows through the bound interface, AND so pipeline-DB reachability (PostgreSQL at `192.168.100.11:5432`) survives whatever VPN wrap is applied.

### Failure modes and observability

- R20. yt-dlp failures are classified into structured reasons captured in the `download_log` JSONB: `youtube_404`, `youtube_age_gated`, `youtube_region_locked`, `youtube_video_removed`, `youtube_transient_network`, `youtube_unknown`. The classification supports operator triage; verbatim stderr is also captured for debugging.
- R21. The system does NOT auto-retry failed rescues. A failed rescue is terminal for that submission; the operator decides whether to resubmit (potentially much later, when a region-locked video might become available).
- R22. On worker startup, the worker sweeps for orphaned `youtube_running` rows (jobs the previous worker process was running when it died) and marks them `outcome='youtube_failed'` with reason `worker_interrupted`. Same pattern as the existing importer's "requeue running jobs on startup."
- R23. The `pipeline-cli show <request_id>` rendering surfaces YT rescue attempts in the same chronological "recent attempts" view it already uses for slskd attempts. The `source` column lets the operator visually distinguish channels; the JSONB blob carries the YT-specific details.

---

## Acceptance Examples

- **AE1. Covers R1, R2, R3, R5.** Given a request in `wanted` status with MBID X and the operator submits `(request_id=42, browse_id=BR1)` where BR1 is a valid resolver-mapping entry for X's release group, when the API is called, the response is a 200 within milliseconds carrying the new download_log_id. The download_log row exists with `outcome='youtube_running'`, `source='youtube'`.

- **AE2. Covers R3.** Given a request in `imported` status, when the operator submits a YT rescue for it, the API returns 409 with a wrong-state reason. No download_log row is created.

- **AE3. Covers R4.** Given a request that already has a `download_log` row with `outcome='youtube_running'`, when the operator submits a second YT rescue for it, the API returns 409 with the existing download_log_id in the response body. No second row is created and no duplicate worker job runs.

- **AE4. Covers R6.** Given a request with MBID X and the operator submits `(request_id, browse_id=BR_UNKNOWN)` where BR_UNKNOWN has no resolver mapping row for X's release group, when the API is called, the response is 422 with a resolver-mapping-missing reason. No download_log row is created.

- **AE5. Covers R7.** Given a request with MBID X (expected 10 tracks) and the operator submits a browse_id whose resolver row's `total_mb_tracks` is 11, when the API is called, the response is 422 with a track-count-precheck-mismatch reason. yt-dlp is never invoked.

- **AE6. Covers R9, R10.** Given a successful submission for a 10-track MBID, when yt-dlp completes having grabbed only 9 tracks (one video age-gated mid-playlist, the `--ignore-errors`-less invocation aborted), the worker marks the download_log row `outcome='youtube_failed'`, reason `youtube_age_gated`. Nothing moves to `/Incoming/auto-import/`. Equivalently: if yt-dlp grabbed 11 files for a 10-track MBID (YT playlist has a bonus track), the worker marks the row `outcome='youtube_failed'`, reason `track_count_mismatch`.

- **AE7. Covers R10, R11.** Given a successful submission for a 10-track request, when yt-dlp completes successfully with exactly 10 audio files, the worker stages them to `/Incoming/auto-import/<artist>-<album>/`, inserts an `import_jobs` row pointing at that path with the request id, and updates the download_log row to `outcome='youtube_success'`. The request's `status` remains `wanted` (or `manual`) until the existing importer worker processes the import_job.

- **AE8. Covers R17.** Given a request that has been categorised `unfindable` for 3 weeks (with `unfindable_category='wrong_pressing_available'`), when a YT rescue ultimately imports successfully via the existing importer, the request's `rescued_at` is populated with the import timestamp AND `prior_unfindable_category` is populated with `'wrong_pressing_available'`. No new `youtube_rescued_at` field exists on `album_requests`. Querying `download_log` for this request shows a row with `source='youtube'`, `outcome='youtube_success'`.

- **AE9. Covers R3, F3.** Given a request in `manual` status with slskd-sourced files in `/Incoming/post-validation/<this album>/`, when the operator submits a YT rescue and it ultimately imports successfully, the request transitions to `imported` via the existing importer. The slskd files in `/Incoming/post-validation/` become stale and are handled by existing convergence (NOT by code added to the YT ingest service).

- **AE10. Covers R22.** Given the ingest worker process dies while a `youtube_running` row exists for request 42, when the worker process restarts, it immediately marks that row `outcome='youtube_failed'` with reason `worker_interrupted`. The request returns to its pre-rescue state and is rescue-resubmittable.

- **AE11. Covers R23.** Given a request that has had three rescue attempts (one slskd success that produced a wrong-matches routing in the past, two YT rescues — one failed for region-lock, one succeeded), when the operator runs `pipeline-cli show <request_id>`, the "recent attempts" rendering shows all three rows chronologically with their respective `source` discriminators and outcomes visible.

---

## Scope Boundaries

- **UI work.** No frontend changes in this scope. The future rescue button — including its placement, its visibility logic ("show only when unfindable"), its messaging about wrong-matches consequences, its picker for which resolver-supplied browse_id to use — is downstream consumer work, not a deliverable here. This API is what that button will eventually call.
- **Wrong-matches routing of low-quality YT rescues.** Not a bug — it is the intended consequence of "same gates apply." The YT ingest service hands a staged directory to the importer; everything the importer does (quality gate, beets distance, wrong-matches routing) is the importer's concern and is unchanged.
- **Codec normalisation at staging.** yt-dlp's `bestaudio` output (Opus) passes through unchanged. No FLAC conversion, no V0 transcode, no codec opinionation at the ingest layer. The existing measurement and quality-decision pipeline reads the source honestly.
- **Authenticated YouTube access.** Anonymous only. No OAuth, no cookies, no Google account, no `yt-dlp --cookies-from-browser`. Age-gated content fails per the structured failure taxonomy.
- **Automatic retry of failed rescues.** A failed rescue is terminal for that submission. No retry queue, no exponential backoff. The operator decides if and when to resubmit (a region-lock today may not be a region-lock tomorrow).
- **Automated YouTube rescue daemon.** No background process auto-rescues unfindable requests off YouTube without operator action. Rescue is always operator-initiated per the archivist invariant ("the system never auto-decides anything irreversible").
- **Cleanup of stale `/Incoming/post-validation/` files** when a rescue from `manual` imports successfully. Delegated to existing convergence per invariant #7. If convergence does not yet cover the post-validation-orphan case, that ships as a follow-up issue, NOT as bespoke teardown inside this service.

---

## Dependencies / Assumptions

- **D1. YouTube resolver service is the upstream metadata source.** Submission validation depends on the resolver's `youtube_album_mappings` table being populated for the request's release group BEFORE the rescue API is called. If the resolver hasn't been called for that release group yet, the rescue API returns 422 — the operator (or future UI) must trigger the resolver first.
- **D2. Existing convergence handles `/Incoming/post-validation/` cleanup after a successful rescue from `manual`.** If convergence does not yet cover this case, the cleanup ships as a follow-up issue. The YT ingest service does NOT take ownership of slskd-staged file cleanup.
- **D3. yt-dlp is packaged via Nix and available in the ingest service's environment.** yt-dlp is not currently a dependency in `nix/`; it must be added as part of this work.
- **D4. The existing importer worker's behavior is unchanged.** This API hands a staged directory + MBID to the importer via the existing `import_jobs` queue. Every gate, every wrong-matches routing rule, every quality decision is the importer's concern and remains unchanged.
- **D5. The resolver's cached `total_mb_tracks` is accurate for the supplied browse_id.** The submission-time precheck (R7) trusts the resolver's cached value. The worker-side post-yt-dlp count gate (R10) is the authoritative check; the precheck is fail-fast convenience, not authority.
- **D6. The new systemd unit can be made network-isolated via the downstream wrapper.** Assumed that the operator's existing NixOS hardening conventions (namespace, VPN binding, etc.) accept a long-running unit as a target without DB-reachability regressions.

---

## Outstanding Questions

### Resolve before planning

- None. The scope is structurally complete.

### Deferred to planning

- **Q1. Worker poll cadence vs LISTEN/NOTIFY.** Whether the ingest worker polls `download_log` on an interval or uses PostgreSQL `LISTEN/NOTIFY` to wake on insert. Affects perceived rescue latency by tens of seconds at most; planning chooses based on consistency with the existing importer/preview-worker pattern.
- **Q2. Track-count source for the worker-side gate.** Whether the worker reads expected track count from the resolver's cached `total_mb_tracks` (consistent with the submission-time precheck) or queries the local MB mirror directly (independent of resolver state). The precheck (R7) uses the resolver; the worker-side gate (R10) is the more critical of the two.
- **Q3. Audit-row write granularity.** Whether the `download_log` JSONB is written once at terminal-state, OR is also updated mid-run with progress information (tracks-downloaded counter, etc.). Affects operator's mid-flight observability; planning's call.

---

## Sources / Research

- **Upstream resolver brainstorm:** `docs/brainstorms/2026-05-27-youtube-music-album-resolver-requirements.md` — the prior brainstorm establishing the `youtube_album_mappings` table that this API's submission validation depends on. Shipped as PR #383/#384.
- **Pipeline DB schema:** `docs/pipeline-db-schema.md` — column inventory for `album_requests`, `download_log`, `import_jobs`. The `source` discriminator (R13) and new outcome values (R14) extend `download_log`'s existing JSONB-and-outcome shape.
- **CLI ⇄ API symmetry rule:** `CLAUDE.md` § "CLI ⇄ API surface symmetry" and `.claude/rules/code-quality.md` § "CLI ⇄ API Surface Symmetry" — both surfaces wrap a single service-layer method; outcome → exit-code / outcome → HTTP-status mappings follow the existing convention.
- **NixOS module layering:** `docs/nixos-module.md` — the in-flake module / downstream wrapper split that R18/R19 reference.
- **Archivist invariants:** `CLAUDE.md` § "Why this exists — the archivist frame" — R20 (never stop searching), strict pressing identity, "system never auto-decides anything irreversible," long-tail-rescue audit. These shape several of the Key Decisions above.
- **Forward-only single-operator discipline:** `.claude/rules/scope.md` § "Single-operator, no backwards-compat" — informs why the `download_log.source` column gets a one-shot backfill of `'slskd'` for pre-existing rows during the migration (not a long-lived backfill script).

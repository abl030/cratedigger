# Cratedigger — Music Acquisition Pipeline

A quality-obsessed music acquisition pipeline. Searches Soulseek via slskd, validates downloads against MusicBrainz via beets, auto-imports with spectral quality verification, or stages for manual review. Web UI at `music.ablz.au` browses the local MusicBrainz + Discogs mirrors and enqueues album requests.

Originally inspired by [mrusse/soularr](https://github.com/mrusse/soularr) ([Ko-Fi](https://ko-fi.com/mrusse)). Has since diverged into its own project — the pipeline DB is the sole source of truth, and the web UI is the album picker.

## Why this exists — the archivist frame

Cratedigger is a **music archival tool first, an acquisition pipeline second**. The operator is an archivist who has been on Soulseek for 20+ years and treats this work as preservation: most of the long-tail music here is genuinely vanishing — niche pressings, Australian indie, 7" singles, BBC PlaySchool records from 1972, demos that lived on one peer who logged off years ago. A real workflow loop is *buy out-of-print release on Discogs → rip → share back via slskd → catch the moments other archivists drop in with their own copies before they vanish again*. This is the inverse of a piracy tool — it's for the autistic-archivist subset of P2P users who file-share to save music before it drops off the radar.

**This frame is load-bearing. Every architectural invariant below flows from it:**

- **Strict pressing identity.** A request points at a specific MB release MBID or Discogs release ID. The matcher NEVER substitutes a sibling pressing automatically, even when the network has the same album in a slightly different track count. Different pressings ARE different releases — the operator's curation depends on the distinction. The 14-track 2008 Kid A reissue is not the 10-track 2000 original.
- **The system never stops searching.** Cadence is constant forever; never auto-throttled based on apparent unfindability. A request that's "unfindable" today might be findable tomorrow when a fresh peer arrives — that's the entire reason the watch loop exists. Surfacing the unfindable cohort to the operator is the right move; throttling search on it is a product violation.
- **The system never auto-decides anything irreversible.** Surfacing is fine ("we couldn't find this exact pressing but a sibling is available in FLAC"). Replace, remove-request, accept-different-pressing — those decisions belong to the operator. Cratedigger does not get to be "smart" on the operator's behalf.
- **No adapter code between MB and Discogs.** Both feed the same columns in the same shape. Curation is data-source-agnostic — a Discogs-sourced Kid A request gets the same strict-pressing treatment as an MB-sourced one.
- **Long-tail-rescue is a celebrated event.** When a request that's been categorised unfindable for weeks transitions to `imported` because a peer finally appeared with it, that's first-class audit data (`rescued_at` + `prior_unfindable_category` on `album_requests`). The product narrative justifies the watch-loop's existence; the audit trail proves it works.
- **Single-operator, no backwards-compat for the rest of the world.** Cratedigger has exactly one user: the operator who runs this fork. There is no "other people's installs." Forward-only migrations, no compat shims, no deprecated-but-kept helpers, backfills as throwaway one-shots (never committed scripts), no one-shot operational machinery carried for a phantom audience. The full rules — and why the accumulation of that machinery is itself a product bug — are in `.claude/rules/scope.md` (always loaded).

If a future design decision drifts toward "good enough" matches, "smart" defaults, auto-throttling apparently-dead requests, or carrying around dead one-shot infrastructure for "what if we need to rerun it," that drift is a bug. Push back.

## Critical invariants (read first — these will bite you)

1. **Run `hostname` at the start of every chat.** `proxmox-vm` = doc1, `doc2` = doc2, `framework` = Framework laptop. `DESKTOP-*` = Windows laptop. You are likely already on doc1 — do NOT ssh to doc1 from doc1.
2. **Windows laptop SSH access**: no native SSH key. A NixOS WSL2 instance has it via sops-nix. Run:
   ```
   mkdir -p ~/.ssh && wsl -d NixOS -- bash -c 'cat /run/secrets/ssh_key_abl030' > ~/.ssh/id_doc2 && chmod 600 ~/.ssh/id_doc2
   ssh -i ~/.ssh/id_doc2 abl030@doc2    # or abl030@proxmox-vm
   ```
   The key works for both machines. You may need `-o StrictHostKeyChecking=no` on first use.
3. **nixosconfig changes MUST be made on doc1.** The repo lives at `~/nixosconfig` on doc1. Doc1 has the git push credentials; doc2 and Windows do not. SSH to doc1 first, edit, commit, push, then deploy to doc2.
4. **Pipeline DB is PostgreSQL on doc2** (nspawn container `cratedigger-db` at `10.20.0.11:5432`; the live DSN is in `/var/lib/cratedigger/config.ini` and the `PIPELINE_DB_DSN` env on `cratedigger.service`). Data lives at `/mnt/virtio/cratedigger/postgres`. Access via `pipeline-cli` on doc2's PATH; for non-root SSH sessions, source `/run/secrets/cratedigger-pgpass` and export `PGPASSWORD` before running `pipeline-cli` (example below). The `10.20.0.0/24` nspawn subnet is doc2-local — raw TCP reachability to `10.20.0.11:5432` exists only on doc2; doc1 and the Framework laptop cannot reach it directly (query via `pipeline-cli` over SSH to doc2). Request statuses: `wanted`, `downloading`, `imported`, `manual`. Import queue statuses: `queued`, `running`, `completed`, `failed`.
5. **This is a curated music collection.** Multiple editions/pressings of the same album are intentional. NEVER delete or merge duplicate albums — they are different MusicBrainz releases (countries, track counts, labels) and the user wants them all. Beets must disambiguate them into separate folders.
6. **The pipeline self-heals — the request is the source of truth, everything else is derived.** Files, beets entries, wrong-matches folders, search plans, denylist, overrides, evidence — all derived state. Operator actions that touch identity supersede the row rather than mutate it, and let the pipeline rebuild from the new row. Audit trail (the frozen old row and its content-addressed child rows) is preserved by virtue of the old row never being mutated or deleted. The Replace operator action (`lib/mbid_replace_service.py`) is the canonical example: it flips the old row to `status='replaced'`, inserts a new row with `replaces_request_id` pointing back, and lets the next 5-minute cycle re-source from the new MBID. Request statuses also include `replaced` (terminal, frozen audit row).
7. **Don't duplicate convergence — reuse the cleanup paths that already exist.** When an operator action could leave behind orphans (in-flight slskd transfers, stale staging, dangling rows), prefer letting existing convergence pick them up over adding bespoke teardown to the action. Where convergence does not yet exist, file an issue and ship the closest direct cleanup in the action itself. Replace deliberately leaves in-flight slskd transfers running rather than building a partial cancellation path that would duplicate that work.
8. **Wildcard-all-artist-tokens stays.** `lib/search.py::wildcard_artist_tokens` replaces the first char of every artist token with `*` (not just the first token). Bypasses Soulseek's server-side artist-name bans, which are keyed on exact strings — "Mountain Goats" might be banned while "*ountain *oats" is not. Single-token wildcarding (just the first token) would leave second-token bans unaddressed. The all-tokens behaviour is deliberate and stays. See `docs/brainstorms/2026-05-25-search-plan-iteration-2-requirements.md` for the trade-off against precision loss.

## Subsystems

- **Web UI** (`music.ablz.au`) — single-page app, stdlib `http.server`, vanilla JS, no build step. `cratedigger-web` systemd service on doc2. Browse tab toggles between MusicBrainz and Discogs mirror; Discogs releases flow through the same pipeline as MB. See `docs/webui-primer.md`.
- **Beets** (Nix-managed via Home Manager, colocated with cratedigger — currently doc2) — library source of truth. All automated imports go through the JSON harness (`harness/beets_harness.py` via `run_beets_harness.sh`), never raw `beet import`. The `musicbrainz` plugin MUST be in the plugins list or beets returns 0 candidates. Always match by `candidate_id` (MB release UUID), never `candidate_index`. See `docs/beets-primer.md`.
- **Meelo** — self-hosted music server on doc1 (podman), scans beets library. After every auto-import, cratedigger triggers a Meelo rescan so the new album appears immediately. See `docs/meelo-primer.md`.
- **Plex** — second music browser, Docker container on Unraid (`tower`), reads the beets library via SMB. Cratedigger triggers a partial scan after each import via `lib/util.py::trigger_plex_scan`. **Note: Plex's refresh endpoint returns HTTP 200 for any path including invalid ones — HTTP 200 is not evidence the scan ran.** See `docs/plex-primer.md`.
- **Discogs mirror** (`discogs.ablz.au`) — ~19M releases, Rust JSON API, nspawn PostgreSQL on doc2. Beets' Discogs plugin is patched (Nix `substituteInPlace`) to hit this mirror, so numeric IDs route through it. See `docs/discogs-mirror.md`.
- **MusicBrainz mirror** (`http://192.168.1.35:5200`) — local MB mirror. See `docs/musicbrainz-mirror.md`.
- **Quality model** — codec-aware rank comparison (LOSSLESS > TRANSPARENT > EXCELLENT > ...). Every measurement classifies into a `QualityRank` band; gate compares against `cfg.quality_ranks.gate_min_rank` (default EXCELLENT). See `docs/quality-ranks.md` and `docs/quality-verification.md`.
- **User cooldowns** — global 3-day cooldowns for Soulseek users with 5 consecutive failures. See `docs/cooldowns.md`.
- **slskd client + event ingestion** (issue #146, complete) — all slskd HTTP calls go through the in-repo typed client `lib/slskd_client.py` (the `slskd-api` PyPI dep is gone; legacy endpoints keep its dict/list return shapes, events return msgspec Structs with the double-decoded `data` payload). Event ingestion runs at the top of every poll cycle (`lib/slskd_events.py::ingest_download_file_events`): it pages `/api/v0/events` newest-first from a persisted single-row cursor (`slskd_event_cursor`) and stamps each `DownloadFileComplete.localFilename` onto the matching `(username, remote filename)` file in `active_download_state`. **The stamp is the ONLY source of completed-file locations** — the reverse-engineering resolver was deleted in phase 3 (2026-07-02). An unstamped file at materialize time is a hard failure (`EVENT-PATH MISSING` at ERROR); the poller retries within `PROCESSING_MATERIALIZE_GRACE_S` (covers the benign completion-vs-event-write race, which resolves on the next cycle's ingest) and past it self-heals the request to `wanted` for re-download. `cancel_and_delete` is likewise event-driven: it deletes files at their stamped paths (fresh `recent_completion_paths` events-page lookup for not-yet-stamped completions, consuming both `DownloadFileComplete` and `DownloadDirectoryComplete`) and prunes only empty directories — never an inferred-folder rmtree. Module layout: `lib/download.py` (poll state machine + orchestration), `lib/download_processing.py` (staging/materialization/validation dispatch), `lib/slskd_transfers.py` (enqueue/status/cancel/transfer-ID helpers), `lib/slskd_events.py` (feed ingestion).
- **Persisted search plans** — every `wanted` request carries a deterministic plan generated by `lib.search.generate_search_plan` and persisted via `lib.search_plan_service.SearchPlanService`. Phase 2 reads `get_wanted_searchable(SEARCH_PLAN_GENERATOR_ID, ...)` and consumes plan-items by ordinal; the executor never recomputes variants. Atomic consumed-attempt writes guard against stale completions after mid-flight regeneration. Bump `SEARCH_PLAN_GENERATOR_ID` in `lib/search.py` whenever generator output changes. See `docs/persisted-search-plans-rollout.md`, `docs/pipeline-db-schema.md`.
- **Unfindable detection** — dedicated `cratedigger-unfindable.service` oneshot on a daily systemd timer (bounded batch per run, roughly weekly per-request cadence). Categorises wanted requests into a 4-bucket taxonomy (`artist_absent`, `album_absent_artist_present`, `one_track_structural`, `wrong_pressing_available`) and writes `album_requests.unfindable_category`. **Lives in its own systemd unit, NOT inline in the 5-min `cratedigger.service` loop**, because R20 ("the system never stops searching") forbids the regular search cadence from being throttled by detection state — the structural separation makes that invariant enforceable at the systemd level. Long-tail rescues (a previously-unfindable request that finally imports) populate `rescued_at` + `prior_unfindable_category` atomically with the import. See `lib/unfindable_detection_service.py`, `docs/search-plan-iter2-deploy.md`.
- **Triage subsystem** — `lib/triage_service.py` composes unfindable categorisation + field-resolution telemetry + search-log forensics into one typed `TriageResult: msgspec.Struct`. Operator-facing via `pipeline-cli triage show <id>` / `pipeline-cli triage list --filter=<spec>` and `GET /api/triage/<id>` / `GET /api/triage/list`. Filter syntax: `all`, `unfindable[:<category>]`, `data_quality[:<field>]`, `data_quality:status=<status>` (note `unresolved_4xx_client` lives in the **status** column, not `reason_code`), `data_quality:reason=<code>` (e.g. `http_400`), `search_not_converting`. `list_triage` is bounded to 4 DB queries at any page size (N+1 mitigation enforced by `tests/test_triage_service.py::TestListTriageN1Guard`). Read-only (`show`/`list`); replaced-row (frozen audit) inclusion is pinned by `test_list_includes_replaced_rows`. See `docs/search-plan-iter2-deploy.md`.
- **YouTube Music resolver + rescue ingest** — two adjacent subsystems for the long-tail rescue narrative. The **resolver** (`lib/youtube_album_service.py`, `web/routes/youtube.py`, `pipeline-cli youtube-album`) takes any MB release MBID or Discogs release ID and returns the set of matching YT Music album entities annotated with beets distance scores against every MB pressing in the release group; results cached in `youtube_album_mappings` (no TTL, refresh on demand). See `docs/brainstorms/2026-05-27-youtube-music-album-resolver-requirements.md`. The **rescue ingest** (`lib/youtube_ingest_service.py`, `scripts/youtube_ingest_worker.py`, `pipeline-cli youtube-rescue`, `POST /api/pipeline/<id>/youtube-rescue`) takes `(request_id, browse_id)` against an existing `wanted`/`manual` request, runs yt-dlp on the resolver-supplied playlist, enforces a hard track-count gate before staging, and enqueues a new `youtube_import` job_type for the existing preview→importer chain. `download_log` doubles as both queue (`outcome='youtube_running'`) and audit (`source='youtube'` + `youtube_metadata` JSONB); `album_requests.status` is NEVER touched by this code path — `mark_imported_with_rescue` performs the only status write on import success, source-agnostically. The worker is a separate systemd unit (`cratedigger-youtube-ingest.service`) so the downstream Nix wrapper can network-namespace its YouTube egress without touching the other services. See `docs/plans/2026-05-28-001-feat-youtube-rescue-ingest-api-plan.md` + `docs/brainstorms/2026-05-28-youtube-rescue-ingest-api-requirements.md`.
- **API discoverability** — `GET /api/_index` and `pipeline-cli routes` self-document every registered route / CLI subcommand with descriptions + Pydantic request models (POST routes). `TestRouteContractAudit::test_every_registered_route_has_a_description` enforces non-empty descriptions on every route. Description metadata lives in parallel `GET_DESCRIPTIONS` / `POST_DESCRIPTIONS` / `PATTERN_DESCRIPTIONS` dicts per `web/routes/*.py` module, merged in `web/server.py::Handler` mirroring the existing dispatch-table block.

## Infrastructure

- **doc1** (`192.168.1.29`): this repo lives at `/home/abl030/cratedigger`; primary interactive dev host.
- **doc2** (`192.168.1.35`): runs cratedigger (systemd oneshot, 5-min timer) and beets (Home Manager), plus MusicBrainz mirror (`:5200`) and slskd (`:5030`). Beets is colocated with cratedigger so the harness can invoke `beet import` locally.
- **Shared storage**: `/mnt/virtio` (virtiofs) — beets DB, pipeline DB data, music library are all accessible from both.
- **Nix deployment**: cratedigger is a flake input (`cratedigger-src`) in `~/nixosconfig/flake.nix`. Downstream wrapper at `~/nixosconfig/modules/nixos/services/cratedigger.nix` imports `inputs.cratedigger-src.nixosModules.default` and layers on sops + nspawn DB + redis + localProxy. See `docs/nixos-module.md` for the full option surface.

### Key paths

| Path | Machine | Purpose |
|------|---------|---------|
| `10.20.0.11:5432/cratedigger` | doc2 nspawn | Pipeline DB (PostgreSQL) |
| `/mnt/virtio/cratedigger/postgres` | shared | PostgreSQL data dir |
| `/mnt/virtio/Music/beets-library.db` | shared | Beets library DB |
| `/mnt/virtio/Music/Beets` | shared | Beets library (tagged files) |
| `/mnt/virtio/Music/Incoming` | shared | Staging root for validated downloads (`auto-import/` for request imports, `post-validation/` for manual redownload review) |
| `/mnt/virtio/Music/Re-download` | shared | READMEs for redownload targets |
| `/mnt/virtio/music/slskd` | doc2 | slskd download directory |
| `/var/lib/cratedigger` | doc2 | Runtime state (config.ini, lock, denylists) |

### Accessing doc2

```bash
ssh doc2 'sudo journalctl -u cratedigger -f'                  # tail logs
ssh doc2 'sudo journalctl -u cratedigger --since "5 min ago"'
ssh doc2 'sudo systemctl is-active cratedigger'
ssh doc2 'sudo systemctl start cratedigger --no-block'        # trigger run
ssh doc2 'sudo cat /var/lib/cratedigger/config.ini'
```

**IMPORTANT for Claude Code**: `systemctl start cratedigger` blocks until the oneshot finishes (minutes). **Always use `--no-block`** when starting via SSH from a Bash tool call. Never use `&` inside SSH quotes to background systemctl — SSH keeps the connection open waiting for all child processes regardless.

#### Querying the pipeline DB (do this, in this order)

1. **Run the query ON doc2.** doc1/Framework cannot reach the nspawn DB (`10.20.0.11:5432` times out — the `10.20.0.0/24` subnet is doc2-local). Only doc2 (where the nspawn container lives) connects reliably. `pipeline-cli` lives on doc2's PATH. For **write** SQL, `pipeline-cli query` won't work (it forces a read-only session) — use `psql "postgresql://cratedigger@10.20.0.11:5432/cratedigger"` on doc2 with `PGPASSWORD` exported.
2. **Pull the live schema first — never guess column names.** Query `information_schema.columns` for the table(s) you're about to touch, then write your real query against what's actually there. Column names are often not what you'd guess, and they drift across releases. The schema is deliberately NOT transcribed into this file — it would go stale. Pull it every time.
3. **Then write whatever query you need.**

Gotchas that cost a lot of time once:
- The pgpass secret is **env-format** (`PGPASSWORD=...`), not colon-pgpass — extract with `grep '^PGPASSWORD=' | cut -d= -f2`, not `cut -d:`.
- **Pass SQL via stdin heredoc, not as an argv string.** `$$`-dollar-quoting does NOT survive bash — it expands `$$` to the shell PID and corrupts the query. Heredoc lets you use normal `'…'` SQL literals.

```bash
ssh doc2 'export PGPASSWORD=$(sudo cat /run/secrets/cratedigger-pgpass | grep "^PGPASSWORD=" | cut -d= -f2); pipeline-cli query "$(cat)"' <<'SQL'
SELECT column_name FROM information_schema.columns
WHERE table_name = 'album_requests' ORDER BY ordinal_position;
SQL
```

### Web dev server

`scripts/web_dev_server.py` runs local route code against a real read-only PostgreSQL (`--data live-db`) or proxies `/api/*` to a remote backend while serving local frontend files (`--data prod-api`). Wrong Matches needs `live-db` on a host that can see the rejected folders on disk (`doc1`/`doc2` qualify; Framework/Windows don't). Canonical remote-dev flow (PG tunnel + double SSH forward + Range-header passthrough for audio scrubbing) in `docs/web-dev-server.md`.

## Repository layout

```
cratedigger.py    — Main loop + thin wrappers; delegates to lib/
album_source.py   — AlbumRecord, DatabaseSource abstraction
web/              — Web UI (server.py, routes/, mb.py, discogs.py, js/)
lib/              — Pipeline modules (see below)
harness/          — beets_harness.py (JSON protocol), import_one.py
migrations/       — Versioned SQL (NNN_name.sql), run by lib/migrator.py
scripts/          — pipeline_cli.py + dev/ops scripts
tests/            — shared infra in fakes.py + helpers.py
nix/              — package.nix, shell.nix, module.nix, VM check
flake.nix         — Outputs: devShell, nixosModules.default, checks.moduleVm
docs/             — Subsystem docs referenced from this file
docs/solutions/   — Compounding lessons from past bugs (YAML frontmatter; grep when debugging)
.claude/rules/    — Path-scoped auto-loaded rules
```

Decision-critical modules: `quality.py` (pure decision functions + typed dataclasses),
`measurement.py` (pure measurement, no decision logic), the `lib/dispatch/` package
(`dispatch_import_from_db` + quality gate; split by concern in issue #139 —
`core.py` orchestration, `evidence_gate.py`, `outcome_actions.py`, `quality_gate.py`,
`entry_points.py`, `subprocess_runner.py`, `manifest_guard.py`, `helpers.py`, `types.py`),
`pipeline_db.py` (PostgreSQL CRUD +
advisory locks — see `docs/advisory-locks.md`). `config.py`/`context.py` hold the
typed `CratediggerConfig` and the `CratediggerContext` that replaced module globals.

## Pipeline flow

```
Web UI (music.ablz.au)               CLI
      │                                │
      │ /api/add                       │ pipeline_cli.py add
      ▼                                ▼
┌──────────────────────────────────────────────┐
│           PostgreSQL (pipeline DB)            │
│  status: wanted → downloading → imported     │
│                                  manual      │
└──────────────────┬───────────────────────────┘
                   │
    ┌──────────────┴──────────────┐
    │ poll_active_downloads()     │ get_wanted()
    │ (resume previous)           │ (search new)
    ▼                             ▼
┌──────────────────────────────────────────────┐
│  Cratedigger (cratedigger.py + lib/download) │
│  Phase 1: poll → Phase 2: search + enqueue   │
└──────────────────┬───────────────────────────┘
                   │
         ┌─────────┴──────────┐
    source=request       source=redownload
    dist ≤ 0.15              │
         │              stage to /Incoming
         ▼              (manual review only)
    stage to /Incoming
         │
         ▼
    import_one.py
    (spectral → convert FLAC→V0 → quality compare → import)
         │
         ▼
      /Beets/       (cleanup /Incoming on success)
```

**All validated downloads stage under `/Incoming` first.** Request auto-imports stage under `/Incoming/auto-import`, while redownload/manual-review paths stage under `/Incoming/post-validation`. Don't assume a path under `/Incoming` is a redownload — request imports can be mid-move or mid-import there too.

### Two-track pipeline

- **Requests** (`source='request'`) — user-added via CLI or web UI. Auto-imported to beets if validation passes at distance ≤ 0.15. Converts FLAC→V0 (or target format), imports from `/Incoming/auto-import`, and cleans up.
- **Redownloads** (`source='redownload'`) — replacing bad source material. Always staged to `/Incoming/post-validation` for manual review, never auto-imported.

Schema fields, JSONB audit blobs, search_log outcomes, and the force-import flow live in `docs/pipeline-db-schema.md`.

## CLI ⇄ API surface symmetry

Every operator action lives on **both** `pipeline-cli` and the web API, wrapping the same service-layer method (e.g. `SearchPlanService.advance_for_request`); the CLI command and HTTP endpoint are thin adapters with matched exit-code/status-code mappings. Adding a capability to only one surface is a trap operators will trip on. The full layer-responsibility table, the worked example (`search-plan advance`), and the status/exit-code convention are in `.claude/rules/code-quality.md` § "CLI ⇄ API Surface Symmetry" (always loaded).

## Decision architecture

**Quality decisions live in ONE place** — `full_pipeline_decision_from_evidence` in `lib/quality.py` (simulator twin `full_pipeline_decision`) is the single source of truth for every importer decision. The two-worker contract (preview measures + persists `AlbumQualityEvidence`; importer reads evidence and decides), the "never re-create the decision elsewhere / never add a narrower check upstream" rule, the no-container-bitrate-fallback invariant (#257), and the album-test-set parity contract are all in `.claude/rules/code-quality.md` § "Quality decisions live in ONE place" (always loaded). The material below is the part that lives only here: how evidence is addressed, propagated, and owned.

**Evidence is content-addressed.** `album_quality_evidence` rows are keyed
by `(mb_release_id, snapshot_fingerprint)`; addressing entities reference
them via `import_jobs.candidate_evidence_id`,
`download_log.candidate_evidence_id`, and
`album_requests.current_evidence_id`. Triage walks the FK chain (direct →
cross-walk via `request_id` → measure as last resort). Evidence is never
deleted unless the files actually change.

**Evidence survives the candidate → library transition (lossless-source
gated).** After a successful import, `propagate_candidate_evidence_to_current`
(U10) inherits the candidate's measurement payload onto the library
evidence row. `verified_lossless_proof`, `verified_lossless`, and
`was_converted_from` propagate in **all** cases. `spectral_grade`,
`spectral_bitrate_kbps`, `v0_metric`, and `matched_bad_audio_hash_*`
propagate when the import is renamed-only OR when the candidate source
codec is lossless (FLAC / ALAC / WAV) — `LOSSLESS_CODECS` in `lib/quality.py`
is the canonical set. Non-lossless transcoded imports (MP3 → Opus etc.)
strip those fields onto NULL because a lossy source's spectral / V0
lineage is not meaningfully comparable against future candidates and
storing it on the library row would mislead triage.

For lossless-source-transcoded library rows, the propagated fields
describe the upstream source audio at import time, not the on-disk
file. Wrong-match cleanup triage compares future candidates against
this evidence to reject same-source duplicates.

**Search narrowing companion.** When `lossless_source_locked` fires —
in the importer (`lib/dispatch/core.py`) or wrong-match cleanup
triage (`lib/wrong_match_cleanup_service.py`) — the request's
`search_filetype_override` is narrowed to `"lossless"` via
`narrow_override_on_lossless_source_lock` (`lib/quality.py`). Future
search cycles only ask Soulseek for lossless tiers, so the lock
doesn't fire repeatedly against new peers serving the same lossy
file. No plan-generator change is needed — `generate_search_plan`
produces query strategies, and the filetype filter is applied
downstream in `enqueue.py::effective_search_tiers` from the request's
override column.

Known wart: library rows imported before this policy landed (2026-05-17)
have NULL spectral / V0 / bad-hash fields and may have lossy
`search_filetype_override` values. They keep the old behaviour —
wrong-match triage cannot reject same-source duplicates against them
and the search-narrowing only fires on new `lossless_source_locked`
events — until each row is re-imported or force-imported. Forward-only
by design; no backfill. See
`docs/brainstorms/2026-05-17-propagate-source-evidence-on-transcode-requirements.md`.

Pure decision helpers in `lib/quality.py`: `spectral_import_decision`,
`import_quality_decision`, `transcode_detection`, `quality_gate_decision`,
`determine_verified_lossless`, `dispatch_action`,
`compute_effective_override_bitrate`, `should_cooldown`,
`provisional_lossless_decision`, `measured_import_decision`, and
`get_decision_tree` (feeds the web UI Decisions tab).

The importer queue is the beets-mutating ownership boundary. Web, CLI, and the
automation poller enqueue import jobs; `cratedigger-importer` drains them
serially. Keep pure quality decisions; avoid adding new direct beets-mutating
entry points outside the importer worker. On startup the importer immediately
requeues any `running` import job left by a previous worker process, then
retries it; the worker holds a DB advisory singleton lock so only one importer
can drain the queue.

Wire-boundary types (harness, JSONB, subprocess stdout) are `msgspec.Struct`,
not `@dataclass` — see `.claude/rules/code-quality.md` § "Wire-boundary types".

## Deploying changes

All code deploys via Nix flake: push cratedigger (GitHub) → `nix flake update cratedigger-src` on doc1 → commit (SSH-signed) + push nixosconfig to **Forgejo** (`git.ablz.au`, token at `/run/secrets/forgejo/nixbot-token`) → `ssh doc2 'sudo fleet-update'`. **Never deploy from `github:abl030/nixosconfig` — since the 2026-06-10 Forgejo cutover, GitHub is a frozen, stale fallback** (the cratedigger repo itself still lives on GitHub; only the nixosconfig leg changed). Flake input updates MUST happen on doc1 (has the Forgejo token + signing key). `cratedigger.service` has `restartIfChanged = false` — the 5-min timer picks up new code; `cratedigger-web` and `cratedigger-db-migrate` restart on switch. Before `nix/module.nix` changes, run the VM check (`nix build .#checks.x86_64-linux.moduleVm`). Full command sequence + verification in `.claude/rules/deploy.md` (always loaded); the `/deploy` command runs it end-to-end.

## GitHub PR merges

Use GitHub **Create a merge commit** for this repo's PRs. Do not use
**Rebase and merge** or **Squash and merge**.

This keeps the PR attached to mainline history on GitHub while preserving the
individual commits that landed in the PR.

## Database migrations

Schema lives in `migrations/NNN_name.sql`; `cratedigger-db-migrate.service` runs them on every switch (fleet-update or break-glass rebuild) before the app services start (a failed migration blocks the app). Add a change by dropping a new numbered SQL file — no manual psql, **never** edit a shipped migration (frozen history), **never** add DDL inside `PipelineDB` methods. Full workflow (test with `tests.test_migrator`, back-up-before-destructive, post-deploy verify) in `.claude/rules/deploy.md` § "Database migrations" (always loaded).

## Running tests

```bash
nix-shell --run "bash scripts/run_tests.sh"                # full suite (~2 min), saves to /tmp/cratedigger-test-output.txt
grep "^FAIL\|^ERROR" /tmp/cratedigger-test-output.txt      # check after the fact
nix-shell --run "python3 -m unittest tests.test_X -v"      # single module
```

**ALWAYS use `nix-shell --run` for Python** — the dev shell provides psycopg2, sox, ffmpeg, music-tag. `.claude/rules/nix-shell.md` enforces this on `.py` edits. `.claude/rules/code-quality.md` covers test taxonomy (pure / seam / orchestration / slice), shared fakes/builders in `tests/fakes.py` + `tests/helpers.py`, and the new-work checklist that maps each kind of change to the tests you owe.

**Never re-run the full suite just to grep output differently.** Read `/tmp/cratedigger-test-output.txt`.

The suite has three Python-side gates that all must pass: JS syntax + JS unit tests, then `bash scripts/find_dead_code.sh` (vulture against the whitelist baseline at `tools/vulture/whitelist.py`), then the Python unittest discovery. The dead-code sweep fails the run as soon as a new vulture finding appears that isn't whitelisted — operator either deletes it or regenerates the baseline per CLAUDE.md § "Finding dead code".

### Skipped tests are an anti-pattern

**At our size, a test either runs or it doesn't exist.** No `@unittest.skipUnless`, no `raise unittest.SkipTest`, no env-gated "only when CRATEDIGGER_REAL_X is set", no "fixtures must be generated first." If you write a test, it runs every single invocation of `bash scripts/run_tests.sh` on a freshly-cloned dev shell. Period.

A skipped test is either:
1. **Irrelevant** — delete it. The test you "might run someday" with the right env var has never run and gates nothing. "OK (skipped=N)" in the test output is a lie that grows over time as the suite quietly shrinks by attrition while the headline number climbs.
2. **Mis-designed** — re-design it so it runs without external infrastructure. Use a Nix-provided binary (sox, ffmpeg, redis-server), generate fixtures in `setUp` from a synthetic in-process source, or move it to a slice with `FakeSlskdAPI` / `FakePipelineDB`. If you need a real slskd / real Redis / real audio, the test belongs as an out-of-band manual procedure, not as a `unittest.TestCase` masquerading as coverage.

The audit suite (`tests/test_skip_audit.py`) enforces this: it fails if any test in the suite reports as skipped. If you add a skip-gated test, the audit will fail and CI will block the merge. There is no allowlist.

Why: we hit this on 2026-05-20. The suite was reporting `OK (skipped=56)` for months. The 38 spectral fixture tests had never run since they were committed in March (hardcoded source path that didn't exist on any machine). The 14 slskd-gated tests had never run in the dev shell (no daemon available). The Redis slice had never run (nothing sets the env var). They were all aspirational coverage — pyright-clean but never executed once. Treat any future "let's just gate this on having X" the same way: it will silently rot, and the next person will believe it.

### Pre-commit hook

Install with: `ln -sf ../../scripts/pre-commit .git/hooks/pre-commit`. Runs pyright on staged `.py` files.

### Claude Code commands

- `/deploy` — full push → flake update → rebuild → verify sequence
- `/debug-download <id>` — query both JSONB audit blobs for a download_log entry
- `/check` — pyright + full test suite pre-commit quality gate
- `/refactor` — guided structural refactor pipeline

### Claude Code rules (auto-loaded when editing matching files)

- `code-quality.md` — type safety, TDD, logging, decision purity (always loaded)
- `nix-shell.md` — always use nix-shell for Python (`*.py`)
- `harness.md` — never discard harness data, typed dataclasses (`harness/`, `lib/beets.py`)
- `web.md` — vanilla JS, no build step (`web/`)
- `pipeline-db.md` — autocommit, migration discipline (`lib/pipeline_db.py`)
- `deploy.md` — flake flow, verify deployed code (always loaded)
- `scope.md` — clean-as-you-go (always loaded)

## Playwright MCP

Browser automation for testing `music.ablz.au`. Configured per-machine in `.mcp.json` (gitignored — platform paths differ). Always use HTTPS (http times out). See `docs/playwright-mcp.md` for setup.

## Debugging quality decisions

`pipeline-cli show / quality / debug-download / search-plan show / query` are the diagnostic entry points (`query` is read-only). Start with the simulator (`pipeline-cli quality <id>`) and add a failing scenario FIRST — see `.claude/rules/code-quality.md` § "Pipeline Decision Debugging — Simulator-First TDD". Command reference, the doc1→doc2 sops-pgpass SSH incantation, and the search-plan iter2 triage signals (`failure_class`, `unfindable_category`, `search_log.rejection_reason`) are in `docs/debugging-cli.md`.

## Finding dead code

vulture (static, `nix-shell --run "bash scripts/find_dead_code.sh"` — diffs against `tools/vulture/whitelist.py`). After deleting dead code, regenerate the whitelist and watch for **cascading orphans** — deleting one helper frequently exposes its now-unreferenced callees, so the next `vulture --make-whitelist` run surfaces them. Full tooling and the per-deletion cascading-orphan workflow are in `docs/dead-code.md` (which also records why production-coverage dead-code detection was tried and removed — issue #352).

## Critical rules

1. **NEVER use `beet remove -d`** — deletes files permanently (exceptions: ban-source endpoint and Replace action, both explicit user actions composed via `lib/release_cleanup.py::remove_and_reset_release`).
2. **NEVER import without inspecting the match** — always through the harness, never pipe blind input to `beet`.
3. **NEVER match by `candidate_index`** — always by MB release ID. Candidate ordering is not stable.
4. **NEVER match by release group** — always exact MB release ID. Release groups conflate pressings.
5. **Auto-import only for `source='request'`** — redownloads always stage for manual review.
6. **All scripts deploy via Nix** — no manual `cp` to virtiofs. Change code → push → flake update → rebuild.
7. **PostgreSQL must use `autocommit=True`** — prevents idle-in-transaction deadlocks. DDL migrations use separate short-lived connections with `lock_timeout`.

## Resolved — canonical RCs (don't re-investigate)

These are settled. Don't reopen the investigation; read the linked solution doc.

- **2026-04-20 Palo Santo data loss** — misplaced `duplicate_keys` block (top-level instead of under `import:`) made beets fall back to `[albumartist, album]` and the harness `remove` wiped a cross-MBID sibling. NOT a beets bug; do not reintroduce the `03bfc63` Cratedigger-owned replacement state machine. `docs/solutions/runtime-errors/palo-santo-duplicate-keys-data-loss.md`.
- **2026-04-14 Lucksmiths MBID drift** — deliberate out-of-band retag via `tagging-workspace/scripts/fix_reissues.py`, invisible to the audit trail. NOT a bug. `docs/solutions/runtime-errors/lucksmiths-mbid-drift-out-of-band-harness.md`.
- **2026-05-18 asciify_paths Plex mass-split** — `asciify_paths = true` + `beet move` renamed paths but not tags, splitting 1,178 albums into Plex ghost rows; fix is the Plex merge API, not Empty Trash. **Footgun:** any beets change that mutates rendered paths followed by `beet move` re-triggers this. `docs/solutions/runtime-errors/plex-asciify-paths-album-split.md`.

## Secrets

- slskd API key: sops-managed, injected into `config.ini` at runtime via the `cratedigger-secrets-split` oneshot (see `docs/nixos-module.md`).
- Discogs token: `~/.config/beets/secrets.yaml` on the beets host (doc2; not used by cratedigger directly).

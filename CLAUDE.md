# **RUN `hostname` AT THE START OF EVERY CHAT. proxmox-vm = doc1, doc2 = doc2, framework = Framework laptop (Linux). You are likely already on doc1 — do NOT ssh to doc1 from doc1. If hostname returns a Windows machine (e.g. DESKTOP-*), you're on the Windows laptop — see below for SSH access.**

# **Windows laptop SSH access**: There is no native SSH key on Windows. A NixOS WSL2 instance has the SSH key via sops-nix at `/run/secrets/ssh_key_abl030`. To get SSH access to doc1/doc2, run: `mkdir -p ~/.ssh && wsl -d NixOS -- bash -c 'cat /run/secrets/ssh_key_abl030' > ~/.ssh/id_doc2 && chmod 600 ~/.ssh/id_doc2` then SSH with `ssh -i ~/.ssh/id_doc2 abl030@doc2` or `ssh -i ~/.ssh/id_doc2 abl030@proxmox-vm`. The key works for both machines. You may need `-o StrictHostKeyChecking=no` on first use.

# **The pipeline DB is PostgreSQL (migrated from SQLite on 2026-03-25). It runs in an nspawn container on doc2 (192.168.100.11:5432). Access via `pipeline-cli` on doc2's PATH, or from doc1 via `ssh doc2 'pipeline-cli ...'`. Data lives at `/mnt/virtio/cratedigger/postgres` for portability. 4 statuses: wanted, downloading, imported, manual.**

# **NIXOSCONFIG CHANGES MUST BE MADE ON DOC1. The nixosconfig repo lives at `~/nixosconfig` on doc1. All edits, commits, and pushes MUST happen there — doc1 has the git push credentials. NEVER try to edit nixosconfig from doc2 or Windows. SSH to doc1 first, make the change, commit, push, then deploy to doc2.**

# **This is a curated music collection. Multiple editions/pressings of the same album are intentional and must be preserved. NEVER delete or merge duplicate albums — they are different MusicBrainz releases (different countries, track counts, labels, etc.) and the user wants them all. Beets must disambiguate them into separate folders on disk.**

# Cratedigger — Music Acquisition Pipeline

A quality-obsessed music acquisition pipeline. Searches Soulseek via slskd, validates downloads against MusicBrainz via beets, auto-imports with spectral quality verification, or stages for manual review. Includes a web UI at `music.ablz.au` for browsing MusicBrainz and adding album requests.

Originally inspired by [mrusse/soularr](https://github.com/mrusse/soularr) ([Ko-Fi](https://ko-fi.com/mrusse)). Has since diverged into its own project — the pipeline DB is the sole source of truth, and the web UI at `music.ablz.au` is the album picker.

## Web UI (music.ablz.au)

A single-page web app for browsing the local MusicBrainz mirror and Discogs mirror, viewing your beets library, and adding releases to the pipeline. Runs on doc2 as `cratedigger-web` systemd service. No build step — stdlib `http.server`, vanilla JS, single HTML file. For full details on architecture, API endpoints, frontend features, and deployment, read `docs/webui-primer.md`.

The browse tab has an MB/Discogs source toggle. When Discogs is selected, search, artist discography, master pressings, and release detail all hit the local Discogs mirror at `discogs.ablz.au`. Discogs releases can be added to the pipeline and flow through the same search → download → validate → import pipeline as MusicBrainz releases. External links are source-aware (musicbrainz.org vs discogs.com).

## Meelo

Meelo is the self-hosted music server that scans the beets library and serves a browseable catalogue with playback. After cratedigger auto-imports an album to beets, it triggers a Meelo scanner rescan so the new album appears in the UI immediately. Meelo runs on doc1 (proxmox-vm) as podman containers. For full details on architecture, API access, troubleshooting, and the scanner/refresh workflow, read `docs/meelo-primer.md`.

## Beets

Beets (v2.5.1, Nix-managed on doc1) is the library's source of truth — it matches albums against MusicBrainz, tags files, organizes them into `/Beets`, and maintains its own SQLite DB at `/mnt/virtio/Music/beets-library.db`. All automated imports go through the JSON harness (`harness/beets_harness.py` via `run_beets_harness.sh`), never raw `beet import`. The `musicbrainz` plugin MUST be in the plugins list or beets returns 0 candidates. Always match by `candidate_id` (MB release UUID), never `candidate_index`. For full details on config, commands, the harness protocol, and troubleshooting, read `docs/beets-primer.md`.

## Discogs Mirror (discogs.ablz.au)

A self-hosted mirror of the Discogs database (~19M releases), serving a Rust JSON API at `https://discogs.ablz.au`. Runs on doc2: nspawn PostgreSQL container + axum HTTP server. Monthly re-import from CC0 XML dumps. Source repo at `~/discogs-api` on doc1. Endpoints: `/api/search?artist=X&title=Y`, `/api/releases/{id}`, `/api/masters/{id}`, `/api/artists/{id}`, `/api/artists/{id}/releases`. For full details on architecture, deployment, debugging, and the NixOS module, read `docs/discogs-mirror.md`.

Beets' Discogs plugin is patched (Nix `substituteInPlace` in `beets.nix`) to use `discogs.ablz.au` instead of `api.discogs.com`. This makes `--search-id <numeric_id>` hit the local mirror during import.

## Repository Structure

```
cratedigger.py              — Search, match, enqueue logic + main(). Thin wrappers delegate
                           to lib/ modules for download processing and utilities.
album_source.py         — AlbumRecord, DatabaseSource abstraction
web/
  server.py             — Web UI server (http.server, JSON API)
  mb.py                 — MusicBrainz API helpers
  discogs.py            — Discogs mirror API helpers (search, artist/master/release lookups)
  index.html            — Frontend (vanilla JS, inline CSS)
lib/
  beets.py              — Beets validation (dry-run import via harness, returns ValidationResult)
  beets_db.py           — BeetsDB: read-only beets SQLite queries (AlbumInfo dataclass)
  config.py             — CratediggerConfig dataclass (typed config from config.ini)
  context.py            — CratediggerContext dataclass (replaces module globals for extracted functions).
                           Includes cooled_down_users cache populated at cycle start.
  download.py           — Async download polling, completion processing, slskd
                           transfer helpers. All functions accept ctx. Delegates
                           the shared pre-import gates (audio + spectral) to
                           lib/preimport.py via _process_beets_validation.
                           Key functions: poll_active_downloads(), process_completed_album(),
                           build_active_download_state(), reconstruct_grab_list_entry(),
                           rederive_transfer_ids(), grab_most_wanted() (enqueue-only, non-blocking).
  grab_list.py          — GrabList: wanted-album selection with priority/ordering
  import_dispatch.py    — Auto-import decision tree: runs import_one.py, uses
                           dispatch_action() flags for mark_done/failed/denylist/requeue.
                           Quality gate. dispatch_import_from_db() (force/manual
                           entry point) calls lib.preimport.run_preimport_gates
                           first, so the only gate force/manual skip is the beets
                           *distance* check — every other quality gate is shared.
  import_service.py     — Force-import/manual-import service layer, ImportOutcome dataclass
  preimport.py          — Shared pre-import quality gates (audio integrity + spectral
                           transcode detection). Single source of truth called by
                           both the auto path (lib.download) and the force/manual
                           path (lib.import_dispatch). Key functions:
                           run_preimport_gates(), inspect_local_files(),
                           PreImportGateResult dataclass.
  pipeline_db.py        — PipelineDB class (PostgreSQL CRUD, queries, get_download_log_entry).
                           Schema is NOT this class's responsibility — see lib/migrator.py.
                           Search logging: log_search(), get_search_history(), get_search_history_batch()
                           User cooldowns: add_cooldown(), get_cooled_down_users(), check_and_apply_cooldown()
                           RequestSpectralStateUpdate (typed spectral state writes)
  migrator.py           — Versioned SQL migrator. discover_migrations() parses
                           migrations/NNN_name.sql files; apply_migrations() runs unapplied
                           ones in transactions and records each in schema_migrations.
                           Idempotent. Driven by scripts/migrate_db.py from systemd.
  quality.py            — Pure decision functions + typed dataclasses:
                           Decision functions:
                           - spectral_import_decision(), import_quality_decision()
                           - transcode_detection(), quality_gate_decision()
                           - determine_verified_lossless(), is_verified_lossless() (legacy)
                           - effective_search_tiers() (merges search_filetype_override + target_format)
                           - should_clear_lossless_search_override() (intent toggle cleanup)
                           - should_cooldown() (global user cooldown decision)
                           - detect_release_source() (UUID → 'musicbrainz', numeric → 'discogs')
                           Dispatch functions:
                           - dispatch_action() → DispatchAction (mark_done/failed/denylist/requeue flags)
                           - compute_effective_override_bitrate(), extract_usernames()
                           - verify_filetype() (slskd file matching, moved from cratedigger.py)
                           Import result types:
                           - ImportResult, ConversionInfo, SpectralDetail, PostflightInfo
                           - AudioQualityMeasurement (on ImportResult as new_measurement/existing_measurement)
                           Validation result types:
                           - ValidationResult, CandidateSummary
                           Harness data types:
                           - HarnessItem, HarnessTrackInfo, TrackMapping
                           Async download state:
                           - ActiveDownloadState, ActiveDownloadFileState
                           Spectral state types:
                           - SpectralMeasurement (grade + bitrate pair, frozen dataclass)
                           Cooldown types:
                           - CooldownConfig (tunables for user cooldown system)
                           Other:
                           - DownloadInfo, SpectralContext, DispatchAction
  search.py             — Search query building, normalization, SearchResult dataclass (with outcome)
  spectral_check.py     — Spectral analysis (sox-based transcode detection)
  util.py               — Pure utilities: sanitize_folder_name, move_failed_import,
                           audio validation, track title cross-check, beets/meelo
                           wrappers, denylist helpers, logging setup
harness/
  beets_harness.py      — Beets interactive import harness (JSON protocol over stdin/stdout)
                           Serializes full AlbumMatch: distance breakdown, track mapping,
                           all AlbumInfo/TrackInfo fields, extra items/tracks with detail
  run_beets_harness.sh  — Shell wrapper to bootstrap Nix beets Python environment
  import_one.py         — One-shot beets import: emits ImportResult JSON on stdout.
                           Pure stage decisions: StageResult, preflight_decision(),
                           conversion_decision(), quality_decision_stage(), final_exit_decision(),
                           conversion_target(), target_cleanup_decision().
                           ConversionSpec + parse_verified_lossless_target() for format config.
                           Single convert_lossless(path, spec) for all format conversions.
                           Flags: --force, --override-min-bitrate, --request-id, --target-format,
                           --verified-lossless-target, --dry-run
migrations/
  001_initial.sql       — Baseline schema (frozen). All future schema changes are new
                           NNN_name.sql files in this directory; the migrator runs them in
                           order and records each in the schema_migrations tracking table.
  002_discogs_index.sql — Index on discogs_release_id for Discogs browse/add lookups
scripts/
  pipeline_cli.py       — CLI: list, add, status, retry, cancel, show, quality, query,
                           force-import, manual-import, set-intent, repair-spectral
  migrate_db.py         — CLI entry point for the schema migrator. Runs by the
                           cratedigger-db-migrate.service systemd unit on every nixos-rebuild.
  populate_tracks.py    — Populate tracks from MusicBrainz API
  run_tests.sh          — Test runner: saves output to /tmp/cratedigger-test-output.txt
tests/                  — Test suite (1400+ tests). Run: nix-shell --run "bash scripts/run_tests.sh"
  fakes.py              — FakePipelineDB (full PipelineDB stand-in: requests, download_logs,
                           denylist, cooldowns, status_history, spectral state, attempt counters,
                           assert_log helper) and FakeSlskdAPI (stateful transfers + users:
                           queued snapshots, add_transfer, set_directory, configurable errors,
                           call recording). Use these instead of MagicMock for stateful tests.
  helpers.py            — Shared builders + helpers: make_request_row, make_import_result,
                           make_validation_result, make_download_info, make_download_file,
                           make_grab_list_entry, make_spectral_context, make_ctx_with_fake_db,
                           patch_dispatch_externals (6-patch context manager for dispatch tests).
  test_fakes.py         — Self-tests for fakes.py and helpers.py builders.
  test_integration_slices.py — Integration slices (TestDispatchThroughQualityGate,
                           TestQualityGateVerifiedLosslessBypass, TestQualityGateSpectralOverride,
                           TestDispatchNoJsonResult, TestForceImportSlice, TestSpectralPropagationSlice).
                           Required for every new high-risk orchestration boundary.
  test_web_server.py    — Web route contract tests with REQUIRED_FIELDS sets per endpoint, plus
                           TestRouteContractAudit guard that introspects Handler._FUNC_*_ROUTES
                           and fails if any route is unclassified — enforces contract coverage
                           at test time, not at review time.
test_cratedigger.py         — Legacy verify_filetype tests (imports from lib/quality)
nix/                    — Nix-native distribution (PR series 2026-04-19)
  slskd-api.nix         — slskd-api PyPI build (single source of truth — was duplicated
                           between shell.nix and the downstream module before)
  package.nix           — Wraps slskd-api + python3.withPackages(...) into a pythonEnv
                           for both the dev shell and the NixOS module to consume
  shell.nix             — Dev shell definition. Top-level shell.nix is a 4-line shim
                           delegating here so `nix-shell` and `nix develop` resolve
                           to identical environments.
  module.nix            — Upstream NixOS module exposed as nixosModules.default.
                           Generic, paths-as-options, no sops/homelab assumptions.
                           Owns: configTemplate, preStartScript, slskdHealthCheck,
                           qualityRanksSection, all systemd unit definitions, and
                           the full options surface (services.cratedigger.*).
  tests/module-vm.nix   — NixOS VM check (nix flake check #moduleVm). Boots the
                           module against an ephemeral postgres and asserts:
                           migrator runs, config.ini renders, pipeline-cli works,
                           cratedigger-web responds 200. Catches breakage in the option
                           surface, prestart, systemd graph, wrapper PYTHONPATH.
flake.nix               — Outputs: packages.slskd-api, devShells.default,
                           nixosModules.default, checks.moduleVm
.claude/
  commands/beets-docs.md — Skill: look up beets RST docs from nix store
  rules/code-quality.md  — Type safety, TDD, test taxonomy, fakes/builders inventory,
                            new work checklist (which tests + infrastructure to use)
  rules/nix-shell.md     — Always use nix-shell for Python (path-scoped to *.py)
  rules/harness.md       — Never discard harness data, typed dataclasses (path-scoped)
```

## Infrastructure

- **doc1** (`192.168.1.29`): Runs beets (Home Manager), this repo lives at `/home/abl030/soularr`
- **doc2** (`192.168.1.35`): Runs Cratedigger (systemd oneshot, 5-min timer), MusicBrainz mirror (`:5200`), slskd (`:5030`)
- **Shared storage**: `/mnt/virtio` (virtiofs) — beets DB, pipeline DB, music library all accessible from both machines
- **Nix deployment**: Cratedigger is a flake input (`cratedigger-src`) in `~/nixosconfig/flake.nix`. The downstream wrapper at `~/nixosconfig/modules/nixos/services/cratedigger.nix` imports `inputs.cratedigger-src.nixosModules.default` for the upstream module and layers on sops + nspawn DB + redis + localProxy. All scripts deploy from the Nix store via `${inputs.cratedigger-src}/...` paths set up by the upstream module's `cfg.src` (default `../.`).

### Key Paths

| Path | Machine | Purpose |
|------|---------|---------|
| `192.168.100.11:5432/cratedigger` | doc2 nspawn | Pipeline DB (PostgreSQL, source of truth) |
| `/mnt/virtio/cratedigger/postgres` | Shared | PostgreSQL data dir (portable) |
| `/mnt/virtio/Music/beets-library.db` | Shared | Beets library DB |
| `/mnt/virtio/Music/Beets` | Shared | Beets library (tagged files) |
| `/mnt/virtio/Music/Incoming` | Shared | Staging area for validated downloads |
| `/mnt/virtio/Music/Re-download` | Shared | READMEs for redownload targets |
| `/mnt/virtio/music/slskd` | doc2 | slskd download directory |
| `/var/lib/cratedigger` | doc2 | Cratedigger runtime state (config.ini, lock file, denylists) |

### Accessing doc2

```bash
ssh doc2
sudo journalctl -u cratedigger -f                        # tail logs
sudo journalctl -u cratedigger --since "5 min ago"        # recent logs
sudo systemctl is-active cratedigger                       # check if running
sudo systemctl start cratedigger --no-block                # trigger run (oneshot — without --no-block it blocks until the entire run completes)
sudo cat /var/lib/cratedigger/config.ini                   # view generated config
```

**IMPORTANT for Claude Code**: `systemctl start cratedigger` blocks until the oneshot service finishes (minutes). Always use `--no-block` when starting via SSH from a Bash tool call. To start + tail logs:
```bash
# Step 1: start (returns immediately)
ssh doc2 'sudo systemctl start cratedigger --no-block'
# Step 2: tail logs (separate command, use run_in_background or timeout)
ssh doc2 'sudo journalctl -u cratedigger -f --since "5 sec ago"'
```
Never use `&` inside SSH quotes to background systemctl — SSH keeps the connection open waiting for all child processes regardless.

## Pipeline Flow

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
    │ (check previous downloads)  │ (search new)
    ▼                             ▼
┌──────────────────────────────────────────────┐
│  Cratedigger (cratedigger.py + lib/download.py)      │
│  Phase 1: poll → Phase 2: search + enqueue   │
└──────────────────┬───────────────────────────┘
                   │
         ┌─────────┴──────────┐
         │                    │
    source=request       source=redownload
    dist ≤ 0.15              │
         │              stage to /Incoming
         ▼              (manual review only)
    stage to /Incoming
    (temporary)
         │
         ▼
    import_one.py
    (spectral check → convert FLAC→V0 → quality compare → import)
         │
         ▼
      /Beets/
    (cleanup /Incoming after import)
```

**IMPORTANT**: ALL validated downloads stage to `/Incoming` first. For `source=request`, `import_one.py` auto-imports from `/Incoming` to `/Beets` and cleans up. For `source=redownload`, files stay in `/Incoming` for manual review. Don't assume files in `/Incoming` are redownloads — they may be mid-import.

## Two-Track Pipeline

- **Requests** (`source='request'`): User-added via CLI or web UI. Auto-imported to beets if beets validation passes at distance ≤ 0.15. Files stage temporarily in `/Incoming`, then `import_one.py` converts (if FLAC), imports to beets (`/Beets`), and cleans up `/Incoming`.
- **Redownloads** (`source='redownload'`): Replacing bad source material. Always staged to `/Incoming` for manual review, never auto-imported.

## Force-Import (rejected downloads)

Albums rejected by beets validation (high distance, wrong pressing) are moved to `failed_imports/` under the slskd download dir, with their `failed_path` stored in `download_log.validation_result` JSONB. After manual review, force-import bypasses the distance check and imports them.

**Path resolution**: Old entries stored relative paths (`failed_imports/Foo - Bar`), new entries store absolute paths. Force-import resolves relative paths against `/mnt/virtio/music/slskd/` automatically.

### How it works

1. Look up `download_log` entry by ID via `get_download_log_entry()` → extract `failed_path` from `validation_result` JSONB
2. Resolve path (handle both relative and absolute) → verify files still exist
3. Look up `mb_release_id` from `album_requests` via `request_id`
4. Call `import_one.py --force` (sets `MAX_DISTANCE=999` — everything else runs normally: conversion, spectral, quality comparison)
5. Log result to new `download_log` row with `outcome='force_import'`
6. Update `album_requests` status to `imported` on success

### Usage

```bash
# CLI
pipeline_cli.py force-import <download_log_id>

# Web API
POST /api/pipeline/force-import {"download_log_id": N}
```

### download_log outcomes

6 valid values: `success`, `rejected`, `failed`, `timeout`, `force_import`, `manual_import`

### search_log table

Every search attempt is logged to `search_log` with: `request_id`, `query` (normalized search term), `result_count`, `elapsed_s`, `outcome`, `created_at`. Failed searches also increment `search_attempts` on `album_requests` and trigger exponential backoff.

6 outcomes: `found` (matched + enqueued), `no_match` (results but no suitable download), `no_results` (0 results from slskd), `timeout`, `error`, `empty_query` (can't build query)

## Decision Architecture

All quality decisions are pure functions in `lib/quality.py` — no I/O, no database, fully unit-tested. The decision pipeline:

1. **`spectral_import_decision()`** — Pre-import: should we import this MP3/CBR download? (genuine/suspect/reject)
2. **`import_quality_decision()`** — Import-time: is this an upgrade or downgrade? (import/downgrade/transcode)
3. **`transcode_detection(spectral_grade, cfg)`** — Post-conversion: was this FLAC actually a transcode? Spectral grade is authoritative when available (suspect/likely_transcode = transcode, genuine/marginal = not transcode). Bitrate fallback uses `cfg.mp3_vbr.excellent` (default 210 kbps) only when spectral is unavailable — tracks retuning automatically (#66).
4. **`quality_gate_decision()`** — Post-import: accept, or re-queue for better quality?
5. **`determine_verified_lossless()`** — Single source of truth for verified lossless status. `is_verified_lossless()` is the legacy fallback for old download_log rows.
6. **`dispatch_action()`** — Post-import_one.py: map decision string to action flags (mark_done/failed, denylist, requeue, trigger_notifiers, quality_gate). Used by `dispatch_import()`.
7. **`compute_effective_override_bitrate()`** — Return the lower of container/spectral bitrate (conservative). Used for `--override-min-bitrate`.
8. **`verify_filetype()`** — Pre-search: does a slskd file dict match an allowed filetype spec? (VBR V0/V2, CBR, min bitrate, bitdepth/samplerate)
9. **`should_cooldown()`** — User cooldown: given a user's last N download outcomes, should they be temporarily skipped? Pure function, delegates from `check_and_apply_cooldown()` in pipeline_db.
10. **`get_decision_tree()`** — Returns the full pipeline decision structure as data (stages, rules, constants) for the web UI Decisions tab. Includes "dispatch" stage showing post-import action mapping. Contract tests in `test_quality_decisions.py` verify this matches the actual functions.

### Import logging (`download_log.import_result` JSONB)

`import_one.py` emits an `ImportResult` JSON blob (`__IMPORT_RESULT__` sentinel on stdout). Contains: decision, conversion details, per-track spectral analysis (grade, hf_deficit, cliff detection per track), quality comparison (new vs prev bitrate), postflight verification (beets_id, path). Every import path (success, downgrade, transcode, error, timeout, crash) logs to download_log.

```sql
SELECT import_result->>'decision', import_result->'quality'->>'new_min_bitrate',
       import_result->'spectral'->>'grade',
       import_result->'spectral'->'per_track'->0->>'hf_deficit_db'
FROM download_log ORDER BY id DESC LIMIT 10;
```

### Validation logging (`download_log.validation_result` JSONB)

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

### Type hierarchy

All types in `lib/quality.py`, fully typed with pyright, JSON round-trip serialization:

- **Import path**: `ImportResult` → `AudioQualityMeasurement` (new/existing), `ConversionInfo`, `SpectralDetail`, `PostflightInfo`
- **Validation path**: `ValidationResult` → `CandidateSummary` → `HarnessTrackInfo`, `HarnessItem`, `TrackMapping`. The four harness wire-boundary types plus `ChooseMatchMessage` are `msgspec.Struct`, not `@dataclass` — the strict-typed decoder at `lib/beets.py::beets_validate` catches int/null/type drift on the wire (issue #99). See `.claude/rules/code-quality.md` § "Wire-boundary types".
- **Dispatch path**: `DispatchAction` (action flags from `dispatch_action()`), `StageResult` (in `import_one.py` — pure stage decisions)
- **Async download path**: `ActiveDownloadState` → `ActiveDownloadFileState` (persisted to `album_requests.active_download_state` JSONB)
- **Spectral state**: `SpectralMeasurement` (grade + bitrate pair), `RequestSpectralStateUpdate` (typed DB write for last_download + current spectral)
- **Cooldown path**: `CooldownConfig` (tunables: threshold, duration, failure outcomes, lookback window)
- **Shared**: `DownloadInfo` (replaces untyped dl_info dict), `SpectralContext` (pre-import spectral gathering), `AlbumInfo` (beets DB queries in `lib/beets_db.py`)

## Quality Upgrade System

The pipeline automatically upgrades album quality toward VBR V0 from verified lossless sources. This is the core differentiator — it doesn't just download albums, it curates them.

### Gold Standard

The target quality for every album is: **FLAC downloaded from Soulseek → spectral analysis confirms genuine lossless → convert to VBR V0**. The VBR bitrate acts as a permanent quality fingerprint (genuine CD rips → ~240-260kbps, transcodes → ~190kbps). CBR 320 is never a final state — it's unverifiable.

### Codec-Aware Quality Ranks (issue #60, shipped 2026-04-11)

Quality comparison is now **rank-based**, not raw-bitrate-based. Every measurement classifies into a `QualityRank` band (UNKNOWN / POOR / ACCEPTABLE / GOOD / EXCELLENT / TRANSPARENT / LOSSLESS) via `lib.quality.quality_rank()`, and `compare_quality()` uses the rank as the primary comparison key. The quality gate compares against `cfg.quality_ranks.gate_min_rank` (default EXCELLENT). Cross-codec cases now work correctly:

- **Opus 128 ≡ MP3 V0** (both TRANSPARENT) → "equivalent"
- **FLAC > any lossy** (LOSSLESS > TRANSPARENT)
- **Unverifiable CBR 320** → TRANSPARENT but still `requeue_lossless` via the `is_cbr && !verified_lossless` branch

Every numeric threshold lives in `QualityRankConfig` (one dataclass) and can be retuned via Nix options at `services.cratedigger.qualityRanks.*` on the upstream module (`nix/module.nix` in this repo), which render into `[Quality Ranks]` in `/var/lib/cratedigger/config.ini` on every `nixos-rebuild`. The default `mp3_vbr.excellent=210` preserves the legacy 210kbps gate threshold for bare-codec measurements. **To retune: see `README.md` § "Tuning the quality rank model"** — every option documented with defaults, meaning, and when to retune, plus the three collection fields (`mp3_vbr_levels`, `lossless_codecs`, `mixed_format_precedence`) that are NOT Nix-exposed but live on the same dataclass. Full rationale in `docs/quality-ranks.md`; default drift caught by `TestQualityRankConfigDefaults` pin tests (#67). **The search filter (`[Search Settings] allowed_filetypes`) is deliberately permissive**: high-quality preferred tiers lead, bare-codec fallback tiers at the end (`aac, opus, ogg, mp3, wav`) catch anything the rank model understands so the rank model is the authoritative quality decision (not the search filter). README § "The search filter is deliberately permissive" has the full design.

**Key rule**: the `verified_lossless=True` bypass is now **tier-gated**. It imports on verdict `"better"` or `"equivalent"` but blocks on `"worse"`. This prevents a deliberately-too-low `verified_lossless_target` (Opus 64) from replacing a good existing album.

**Bitrate metric**: `cfg.quality_ranks.bitrate_metric` picks between `min` (legacy), `avg` (default, recommended for VBR codecs), and `median` (outlier-resistant — picks the middle track, ignoring quiet intros/outros, hidden tracks, and skits that would drag MIN down or skew AVG). Spectral cliff detection always uses min. See `docs/quality-ranks.md` for *when to prefer median*.

### Quality Gate (`_check_quality_gate_core()` in import_dispatch.py)

After every import, the quality gate runs `quality_gate_decision(current, cfg=cfg.quality_ranks)` which delegates to `gate_rank()` (the single source of truth for the rank-with-clamp computation, also called by the `pipeline-cli quality` simulator so the displayed label and the actual gate verdict can never disagree):

1. Classify the current measurement into a `QualityRank` via format label or bare-codec band table (`measurement_rank()`).
2. If a spectral estimate is set, clamp the rank to the minimum of (rank, spectral_rank) — catches fake 320s.
3. **Rank < `cfg.gate_min_rank`** → `requeue_upgrade`.
4. **CBR on disk + not verified_lossless + below LOSSLESS** → `requeue_lossless` (search for a FLAC source).
5. Otherwise → `accept`.

Lo-fi V0 at 207kbps now passes the gate via the `"mp3 v0"` label contract (`cfg.mp3_vbr_levels[0] = TRANSPARENT`) without needing the old `verified_lossless` blanket bypass.

### Two Key Concepts (don't confuse them)

- **`spectral_grade`**: "Does this file look like a transcode?" — answers whether spectral analysis found cliff artifacts or high-frequency deficits. Works on any file type. A CBR 320 with `spectral_grade=genuine` just means "no cliff detected" — it does NOT mean the source was lossless. On `album_requests`, split into `last_download_spectral_grade` (from the download) and `current_spectral_grade` (what's on disk). On `download_log`, just `spectral_grade` (point-in-time snapshot).
- **`verified_lossless`** (on `album_requests` only): "Did we verify this from a genuine FLAC?" — only set `TRUE` when: downloaded FLAC + spectral analysis said genuine + converted to V0 (or target format). This is the only way to prove source quality.

### How Downloads Flow by Type

**FLAC downloads** (in `import_one.py`):
1. Spectral check on raw FLAC → grade stored on album_requests
2. Convert FLAC → V0 via `convert_lossless(path, V0_SPEC)` for verification
3. Transcode detection: spectral grade is authoritative (genuine/marginal = not transcode, suspect = transcode). Bitrate fallback threshold (`cfg.mp3_vbr.excellent`, default 210 kbps) is used only when spectral is unavailable — tracks retuning automatically (#66).
4. Compare new V0 bitrate against existing on disk (override = `min(pipeline DB min_bitrate, current_spectral_bitrate)` — catches fake 320s)
5. If verified lossless AND `verified_lossless_target` configured (e.g. "opus 128"): convert original FLAC → target format, discard V0 (ephemeral verification artifact)
6. If upgrade → import to beets. `verified_lossless` set by import_one.py's verdict (not re-derived). When verified lossless, `current_spectral_bitrate` = actual min bitrate (not spectral cliff estimate).
7. Quality gate ranks the imported measurement; the `verified_lossless=True` bypass is **tier-gated** — it imports on rank-comparison verdict `better`/`equivalent` but blocks on `worse` (so a too-low `verified_lossless_target` like Opus 64 cannot replace a good existing album). See `docs/quality-ranks.md`.

**MP3 VBR downloads** (V0/V2):
1. No spectral check needed — VBR bitrate IS the quality signal
2. Import directly, quality gate classifies the measurement into a `QualityRank` (mp3_vbr band table) and accepts if the rank is at or above `cfg.quality_ranks.gate_min_rank` (default `EXCELLENT` ≈ 210kbps)

**MP3 CBR downloads** (320, 256, etc.):
1. Spectral check runs in `process_completed_album()` (cratedigger.py) — detects upsampled garbage via cliff detection
2. If spectral says SUSPECT → reject, denylist user
3. If spectral says genuine or marginal → import (something is better than nothing)
4. Quality gate: even when CBR rank is TRANSPARENT, the `is_cbr && !verified_lossless && rank < LOSSLESS` branch fires → re-queues with `search_filetype_override="lossless"` to find a verifiable lossless source

### Spectral Analysis (`lib/spectral_check.py`)

Uses `sox` bandpass filtering to detect transcodes. Measures RMS energy in 16 x 500Hz frequency slices from 12-20kHz, computes gradient between adjacent slices. A transcode has a sharp "cliff" at the original encoder's lowpass frequency. Genuine audio has gradual rolloff.

- **Cliff detection**: 2+ consecutive slices with gradient < -12 dB/kHz → SUSPECT
- **HF deficit**: avg energy at 18-20kHz vs 1-4kHz reference > 60dB → SUSPECT
- Album level: >60% tracks suspect → album SUSPECT
- Dependencies: `sox` (in Nix PATH)
- Performance: ~8s per track (30s trim), ~100s per 12-track album
- Full docs: `docs/quality-verification.md`

### Key Fields (`album_requests` table)

- `search_filetype_override TEXT` — Transient CSV filetype list (e.g. `"lossless,mp3 v0,mp3 320"` or just `"lossless"`). Overrides global `allowed_filetypes` for search. Set by quality gate requeue paths and backfill. Cleared on quality gate accept. The `"lossless"` virtual tier matches FLAC, ALAC, and WAV.
- `target_format TEXT` — Persistent user intent for desired format on disk (`"lossless"` or NULL). Set only by user action (CLI/web set-intent toggle). Never cleared by quality gate. When set, keeps lossless on disk (normalizes ALAC/WAV → FLAC) instead of converting to V0/target.
- `min_bitrate INTEGER` — Current min track bitrate in kbps (from beets).
- `prev_min_bitrate INTEGER` — Previous min_bitrate before last upgrade. Shows delta in UI.
- `verified_lossless BOOLEAN` — True only when imported from spectral-verified genuine FLAC→V0.
- `last_download_spectral_grade TEXT` — Spectral grade of the most recent download attempt.
- `last_download_spectral_bitrate INTEGER` — Estimated bitrate from the most recent download's spectral analysis.
- `current_spectral_grade TEXT` — Spectral grade of files currently on disk in beets.
- `current_spectral_bitrate INTEGER` — Spectral estimated bitrate of files currently on disk. NULL for genuine files (no cliff). Quality gate uses this for gate_bitrate.
- `active_download_state JSONB` — Persisted download state for async polling (filetype, enqueued_at, per-file username/filename/size). Set by `set_downloading()`, cleared on completion/timeout.

### Key Fields (`download_log` table)

- `slskd_filetype TEXT` — What Soulseek advertised ("flac", "mp3").
- `actual_filetype TEXT` — What's on disk after download/conversion.
- `spectral_grade TEXT` — Spectral analysis of the downloaded files.
- `spectral_bitrate INTEGER` — Estimated original bitrate from spectral.
- `existing_min_bitrate INTEGER` — Beets min bitrate before this download.
- `existing_spectral_bitrate INTEGER` — Spectral estimate of existing files before download.

### Downgrade Prevention (`import_one.py`)

- `--override-min-bitrate` arg: `dispatch_import()` passes `min(min_bitrate, current_spectral_bitrate)` from the pipeline DB. When spectral says the existing files are 128kbps but the container says 320kbps (fake CBR), the spectral truth is used so genuine upgrades aren't blocked.
- `mark_done()` respects `verified_lossless_override` from import_one.py instead of re-deriving via `is_verified_lossless()`. When verified lossless, `current_spectral_bitrate` is set to the actual V0 min bitrate (not the spectral cliff estimate, which can miscalibrate on genuine files).
- Spectral state writes always go through `RequestSpectralStateUpdate` — grade and bitrate are always written together (including explicit NULLs for genuine files with no cliff). This prevents stale spectral data from persisting after an upgrade.
- `--target-format` flag: when `target_format="lossless"` (or legacy `"flac"`), skips V0 conversion and keeps lossless on disk. ALAC/WAV sources are normalized to FLAC via `FLAC_SPEC`. Genuine lossless on disk is marked `verified_lossless`. Passed from `dispatch_import()` when `album_data.db_target_format` is set.
- `--verified-lossless-target` flag: target format after verified lossless (e.g. "opus 128", "mp3 v2", "aac 128"). Passed from `dispatch_import()` when `cfg.verified_lossless_target` is set. When the target has the same `.mp3` extension as V0, V0 files are removed before target conversion.
- `--force` flag: skips the distance check (`MAX_DISTANCE=999`) for force-importing rejected albums. Used by `pipeline_cli.py force-import` and `POST /api/pipeline/force-import`.
- Exit codes: 0=imported, 1=conversion failed, 2=beets failed, 3=path not found, 5=downgrade, 6=transcode (may or may not have imported as upgrade)

### New/Re-queued Album Priority

`get_wanted()` sorts by `search_attempts=0` first, then random. New requests and upgrade re-queues always get picked up on the next cycle.

### Web UI Controls

- **Recents tab** ("validation pipeline log"): Shows every download with full quality flow (slskd reported → actual on disk → spectral → existing). Badges: Upgraded, New import, Wrong match, Transcode, Quality mismatch. "On disk (before)" shows pre-import state.
- **Library tab**: Quality label per album (MP3 V0, MP3 320, etc.). Upgrade button. Accept button (sets avg bitrate for lo-fi edge cases). Intent toggle: Default / Lossless (keeps lossless on disk for specific albums).
- **Decisions tab**: Pipeline decision diagram generated from `get_decision_tree()` — shows FLAC/MP3 branching paths, all stages and rules with live thresholds from the code. A rank policy badge row at the top of the tab (gate min rank / bitrate metric / within-rank tolerance, issue #68) mirrors the same runtime cfg the backend uses, so operators see at a glance what `[Quality Ranks]` in the deployed `config.ini` is actually running. Interactive simulator calls `full_pipeline_decision()` via `/api/pipeline/simulate` with presets for known scenarios.
- **Ban source**: Denylists user + removes from beets + requeues.

### Edge Cases

- **Lo-fi recordings** (Mountain Goats boombox era): Genuine V0 from verified FLAC can produce ~207kbps. The `"mp3 v0"` label classifies as `TRANSPARENT` via `cfg.mp3_vbr_levels[0]` regardless of bitrate, so the gate accepts without needing a `verified_lossless` blanket bypass.
- **Mixed-source CBR** (e.g. 13 tracks at 320 + 1 track at 192): Looks like VBR to `COUNT(DISTINCT bitrate)` but isn't genuine V0. Quality gate ranks against the bare-codec band table — 192 lands in `GOOD` (< default `EXCELLENT`) → re-queues for upgrade.
- **Fake FLACs**: MP3 wrapped in FLAC container. Spectral detects cliff pre-conversion, V0 bitrate confirms post-conversion. Source denylisted, but file imported if better than existing.
- **Discogs-sourced albums**: Numeric IDs stored in `mb_release_id` for pipeline compat. Beets auto-routes numeric IDs to the Discogs plugin via `--search-id`. `detect_release_source()` in `lib/quality.py` distinguishes UUID vs numeric format for conditional UI rendering (links, source badges). The full pipeline (search, download, validate, import, quality gate) works identically for both sources.

## User Cooldowns (issue #39)

Global, temporary cooldowns for Soulseek users who consistently fail to deliver downloads. Separate from the per-request quality denylist (`source_denylist`) — cooldowns are global (not per-album) and time-bounded.

### How it works

After every timeout or beets rejection, `check_and_apply_cooldown(username)` queries the user's last 5 download outcomes globally (across all albums). If all 5 are failures (timeout/failed/rejected), a 3-day cooldown is inserted into `user_cooldowns`. During enqueue, cooled-down users are skipped with a distinct "on cooldown" log message.

### Tunables (`CooldownConfig` in `lib/quality.py`)

| Field | Default | Purpose |
|-------|---------|---------|
| `failure_threshold` | 5 | Consecutive failures before cooldown |
| `cooldown_days` | 3 | Cooldown duration |
| `failure_outcomes` | timeout, failed, rejected | Which outcomes count as failures |
| `lookback_window` | 5 | How many recent outcomes to check |

### Table: `user_cooldowns`

```sql
CREATE TABLE user_cooldowns (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    cooldown_until TIMESTAMPTZ NOT NULL,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

- `UNIQUE(username)` — one active cooldown per user, upsert extends it
- No `request_id` — this is global across all albums
- Expired rows are harmless (filtered by `cooldown_until > NOW()`)

### Data flow

1. **Trigger**: `_timeout_album()` (download.py) and `reject_and_requeue()` (album_source.py) call `db.check_and_apply_cooldown(username)` after logging the outcome
2. **Decision**: `check_and_apply_cooldown()` queries `download_log` for last N outcomes, delegates to `should_cooldown()` pure function
3. **Storage**: If triggered, upserts `user_cooldowns` with `cooldown_until = NOW() + 3 days`
4. **Cache**: `ctx.cooled_down_users` populated at cycle start in `cratedigger.py main()`, shared with Phase 1 thread. Updated in real-time when new cooldowns are applied mid-cycle.
5. **Enforcement**: `try_enqueue()` and `try_multi_enqueue()` in `lib/enqueue.py` skip users in `ctx.cooled_down_users` before checking the per-request denylist

### Re-cooldown behavior

After the 3-day cooldown expires, the user gets one chance. If they succeed, the success breaks their failure streak. If they fail, `check_and_apply_cooldown` sees 4 old failures + 1 new = 5 failures → immediate re-cooldown.

### Diagnostics

```bash
# View active cooldowns
pipeline-cli query "SELECT username, cooldown_until, reason FROM user_cooldowns WHERE cooldown_until > NOW()"

# View all cooldowns (including expired)
pipeline-cli query "SELECT * FROM user_cooldowns ORDER BY cooldown_until DESC"

# Top timeout offenders
pipeline-cli query "SELECT soulseek_username, COUNT(*) FROM download_log WHERE outcome = 'timeout' GROUP BY soulseek_username ORDER BY count DESC LIMIT 10"

# Manually seed cooldowns for all users with 5+ consecutive failures
psql -h 192.168.100.11 -U cratedigger cratedigger -c "INSERT INTO user_cooldowns ..."
```

## Deploying Changes

Flake input changes MUST be done on doc1 and pushed from there. Doc2 has no git push credentials. Doc2 is only for building/running.

**From any machine with SSH access (framework, doc1, Windows laptop):**
```bash
# 1. Edit code, commit, push (from wherever the repo lives)
git add <files> && git commit -m "description" && git push

# 2. Update Nix flake input (MUST be on doc1 — it has git push access)
ssh doc1 'cd ~/nixosconfig && nix flake update cratedigger-src && git add flake.lock && git commit -m "cratedigger: description" && git push'

# 3. Deploy to doc2 — runs cratedigger-db-migrate.service AND restarts cratedigger-web automatically
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
```

**From doc1 directly:**
```bash
cd ~/nixosconfig
nix flake update cratedigger-src
git add flake.lock && git commit -m "cratedigger: description" && git push
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
```

**IMPORTANT**: `restartIfChanged = false` on `cratedigger.service` — deploys don't restart Cratedigger itself. The 5-min timer picks up new code on the next cycle, or manually start. `cratedigger-web` and `cratedigger-db-migrate` use the systemd default (restart on change) so they pick up the new code immediately at switch time.

### Validating before deploy

The flake exposes a NixOS VM check that boots the upstream module against an ephemeral postgres + stub slskd:

```bash
nix build .#checks.x86_64-linux.moduleVm    # ~30s after first build
```

This catches: option surface breakage, prestart sed-substitution bugs, systemd dep graph cycles, wrapper script PYTHONPATH errors, missing python deps. It does NOT exercise slskd interaction or real downloads (those need fixture data — see the python suite). Run before any `nix/module.nix` change.

## Database Migrations

Schema changes go through versioned migration files. The deploy unit `cratedigger-db-migrate.service` (oneshot, `restartIfChanged = true`) runs the migrator on every `nixos-rebuild switch` BEFORE `cratedigger.service` and `cratedigger-web.service` start. Both services `requires` the migrate unit, so a failed migration blocks the app from coming up against an inconsistent schema.

**Layout:**
- `migrations/NNN_name.sql` — versioned, append-only. Each file runs in its own transaction, exactly once per DB.
- `lib/migrator.py` — discovers files, tracks applied versions in the `schema_migrations` table.
- `scripts/migrate_db.py` — CLI entry point invoked by the systemd unit.

**Adding a schema change:**
1. Drop a new file in `migrations/` named `NNN_describe_change.sql` (next number).
2. Plain SQL — no `IF NOT EXISTS` guards needed; versioned migrations only run once.
3. Test it: `nix-shell --run "python3 -m unittest tests.test_migrator -v"`
4. Commit, push, deploy. The migrator picks it up automatically on the next `nixos-rebuild switch`.

**Verifying after deploy:**
```bash
ssh doc2 'sudo systemctl status cratedigger-db-migrate.service --no-pager | head -10'
ssh doc2 'pipeline-cli query "SELECT version, name, applied_at FROM schema_migrations ORDER BY version DESC LIMIT 5"'
```

**If a migration fails:** `ssh doc2 'sudo journalctl -u cratedigger-db-migrate.service -n 50'`. The unit must be in `active (exited)` state for cratedigger/cratedigger-web to start.

**For destructive changes**, backup first: `ssh doc2 'pg_dump -h 192.168.100.11 -U cratedigger cratedigger' > /tmp/cratedigger_backup_$(date +%Y%m%d_%H%M%S).sql`

**Never** edit a migration file that has already shipped. Frozen history. To fix a mistake, add a new migration that corrects it.

**Never** add DDL inside `PipelineDB` methods. `PipelineDB.__init__` does NOT run migrations — it expects the schema to already be current. Migrations are exclusively `migrations/*.sql` applied by `lib/migrator.py`.

## NixOS Module

The upstream module lives in this repo at `nix/module.nix`, exposed via `nixosModules.default` in `flake.nix`. It's generic and homelab-agnostic: every secret is a `*File` path, the DB is a `dsn` string, no sops/nspawn/reverse-proxy assumptions.

`~/nixosconfig/modules/nixos/services/cratedigger.nix` is a thin homelab wrapper (~150 lines) that imports the upstream module and adds:
- sops-nix per-key secret materialization (`cratedigger-secrets-split` oneshot — see "Sops + per-key secrets" below)
- the nspawn PostgreSQL container for the pipeline DB
- the redis instance for the web UI cache
- the `homelab.localProxy.hosts` entry for `music.ablz.au`
- systemd `after`/`wants`/`restartTriggers` splicing in `container@cratedigger-db.service`

Key options on the upstream module (full set in `nix/module.nix`):

| Option | Default | Purpose |
|---|---|---|
| `enable` | `false` | Master switch |
| `user` / `group` | `"root"` | Service identity. Default root because slskd downloads + beets need broad fs access. |
| `src` | `../.` | Path to cratedigger source tree. Defaults to this flake's repo root. |
| `stateDir` | `/var/lib/cratedigger` | Runtime state (config.ini, lock file). |
| `slskd.apiKeyFile` | (required) | Path to a file containing the raw slskd API key (one line). |
| `slskd.downloadDir` | (required) | Where slskd downloads land. |
| `slskd.hostUrl` | `http://localhost:5030` | slskd HTTP base URL. |
| `pipelineDb.dsn` | (required) | PostgreSQL DSN. |
| `beetsValidation.{enable,distanceThreshold,stagingDir,trackingFile,verifiedLosslessTarget}` | sensible defaults | Beets validation config. |
| `web.{enable,port,beetsDb,redis.host,redis.port}` | port=8085 | Web UI config. The module does NOT enable redis — provide one. |
| `notifiers.meelo.{enable,url,usernameFile,passwordFile}` | disabled | Meelo notifier. |
| `notifiers.plex.{enable,url,tokenFile,librarySectionId,pathMap}` | disabled | Plex notifier. |
| `notifiers.jellyfin.{enable,url,tokenFile}` | disabled | Jellyfin notifier. |
| `healthCheck.{enable,onFailureCommand}` | enabled, no recovery | Pre-cycle slskd healthcheck. `onFailureCommand` runs to recover (e.g. `systemctl restart slskd.service`). |
| `releaseSettings.*` / `searchSettings.*` / `downloadSettings.*` | match config.ini defaults | Pipeline tunables. |
| `qualityRanks.*` | mirror of `QualityRankConfig.defaults()` | See README § "Tuning the quality rank model". |
| `timer.{enable,onBootSec,onUnitActiveSec}` | every 5 min | Cycle frequency. |
| `logging.{level,format,datefmt}` | INFO | Python logging config. |

The module:
1. Builds a Python environment with dependencies (`nix/package.nix`: psycopg2, music-tag, beets, msgspec, redis, slskd-api)
2. Wraps `cratedigger.py` / `pipeline_cli.py` / `migrate_db.py` / `web/server.py` in shell scripts with ffmpeg, sox, mp3val, flac in PATH
3. Renders `/var/lib/cratedigger/config.ini` at boot from option values, sed-substituting credentials read from each `*File` path
4. Pre-start: health-check slskd → render config.ini → start `cratedigger.py`

Systemd units:
- `cratedigger-db-migrate.service` — oneshot, `restartIfChanged = true`, `RemainAfterExit = true`. Runs the schema migrator on every `nixos-rebuild switch`. Both `cratedigger.service` and `cratedigger-web.service` `requires` it, so the app cannot start against an un-migrated DB.
- `cratedigger.service` — oneshot pipeline run. `restartIfChanged = false` (5-min timer picks up new code).
- `cratedigger.timer` — fires every 5 minutes (configurable via `timer.onUnitActiveSec`).
- `cratedigger-web.service` — long-running web UI for music.ablz.au.

### Sops + per-key secrets

sops-nix's `key = "..."` does NOT actually extract a single value from a multi-key dotenv file (it writes the whole `KEY=VALUE` envfile regardless — verified empirically; same gotcha is documented in `~/nixosconfig/modules/nixos/services/alerting.nix` for the gotify token). The upstream module wants raw values per file, so the homelab wrapper materializes them via a `cratedigger-secrets-split` oneshot at boot:

```nix
systemd.services.cratedigger-secrets-split = {
  before = ["cratedigger.service" "cratedigger-web.service" "cratedigger-db-migrate.service"];
  serviceConfig.ExecStart = pkgs.writeShellScript "cratedigger-secrets-split" ''
    set -euo pipefail
    install -d -m 0700 /run/cratedigger-secrets
    for key in SOULARR_SLSKD_API_KEY MEELO_USERNAME MEELO_PASSWORD PLEX_TOKEN JELLYFIN_TOKEN; do
      grep -m1 "^$key=" "${config.sops.secrets."soularr/env".path}" \
        | cut -d= -f2- | tr -d '\n' > "/run/cratedigger-secrets/$key"
      chmod 0400 "/run/cratedigger-secrets/$key"
    done
  '';
};
services.cratedigger.slskd.apiKeyFile = "/run/cratedigger-secrets/SOULARR_SLSKD_API_KEY";
services.cratedigger.notifiers.meelo.usernameFile = "/run/cratedigger-secrets/MEELO_USERNAME";
# ... etc
```

If you don't use sops or have one key per encrypted file, skip the splitter and point `apiKeyFile` directly at the secret path.

### Flake outputs

```
github:abl030/cratedigger
├── nixosModules.default              ← upstream NixOS module
├── packages.<system>.slskd-api        ← slskd-api PyPI build (not in nixpkgs)
├── devShells.<system>.default         ← test/dev environment
└── checks.<system>.moduleVm           ← NixOS VM test (boots module against ephemeral postgres)
```

Build the VM check: `nix build .#checks.x86_64-linux.moduleVm`.

## Running Tests

**ALWAYS use `nix-shell --run` to run tests and Python commands.** The dev shell (`shell.nix`) provides psycopg2, sox, ffmpeg, music-tag, slskd-api — without it, tests will fail with missing imports. Never run `python3` directly outside `nix-shell`.

**Use the test runner script** — it saves output to `/tmp/cratedigger-test-output.txt` so you can grep failures without re-running the full 2-minute suite:

```bash
nix-shell --run "bash scripts/run_tests.sh"           # full suite (~2 min), saves output
grep "^FAIL\|^ERROR" /tmp/cratedigger-test-output.txt     # check for failures after the fact
grep "^Ran " /tmp/cratedigger-test-output.txt              # quick pass/fail count
```

**NEVER re-run the full suite just to grep output differently.** Read `/tmp/cratedigger-test-output.txt` instead.

For single test modules during development:
```bash
nix-shell --run "python3 -m unittest tests.test_quality_decisions -v"
nix-shell --run "python3 -m unittest tests.test_import_result -v"
```

### Test Taxonomy & Shared Infrastructure

The test suite is organized into 4 categories with established patterns. **`.claude/rules/code-quality.md` is the canonical reference** — read it before adding new tests or new production code paths. Key infrastructure:

- **`tests/fakes.py`** — `FakePipelineDB` and `FakeSlskdAPI`: stateful fakes that record domain state. Use these instead of `MagicMock` for any test that reasons about state transitions.
- **`tests/helpers.py`** — Shared builders (`make_request_row`, `make_import_result`, `make_grab_list_entry`, etc.) and the `patch_dispatch_externals()` context manager. Always use these instead of hand-rolling test data.
- **`tests/test_integration_slices.py`** — End-to-end slices that exercise real code paths with minimal patching. Required for every new high-risk orchestration boundary.
- **`tests/test_web_server.py`** — Contract tests with `REQUIRED_FIELDS` per endpoint plus `TestRouteContractAudit`, a guard test that fails at test time if a new route is added without contract coverage. **Adding a route to `web/routes/` without classifying it in `CLASSIFIED_ROUTES` will fail the suite.** This is intentional.

**The "new work checklist" in `code-quality.md`** maps every kind of change (new pure function, new dispatch path, new web route, new slskd interaction, new dataclass, new PipelineDB method) to the tests you owe and the infrastructure you reuse. Read it before starting any non-trivial task.

### Pre-commit hook

A git pre-commit hook runs pyright on staged .py files automatically. Install with:
```bash
ln -sf ../../scripts/pre-commit .git/hooks/pre-commit
```

### Claude Code commands

- `/deploy` — full push → flake update → rebuild → verify sequence
- `/debug-download <id>` — query both JSONB audit blobs for a download_log entry
- `/check` — pyright + full test suite pre-commit quality gate

### Claude Code rules

Path-scoped rules in `.claude/rules/` auto-load when editing matching files:
- `code-quality.md` — type safety, TDD, logging, decision purity (always loaded)
- `nix-shell.md` — always use nix-shell for Python (loaded for `*.py`)
- `harness.md` — never discard harness data (loaded for `harness/`, `lib/beets.py`)
- `web.md` — vanilla JS, no build step (loaded for `web/`)
- `pipeline-db.md` — autocommit, idempotent migrations (loaded for `lib/pipeline_db.py`)
- `deploy.md` — flake flow, verify deployed code (always loaded)

## Playwright MCP (Web UI Testing)

The Playwright MCP server provides browser automation tools for testing the web UI at `https://music.ablz.au`. Configured in `.mcp.json` (not committed — platform-specific). Use `browser_navigate`, `browser_snapshot`, `browser_click`, `browser_fill_form`, `browser_console_messages`, etc.

### Setup

**Windows laptop**: Node.js installed via scoop. `.mcp.json` must use absolute paths because scoop shims aren't in the Claude Code process PATH:
```json
{
  "mcpServers": {
    "playwright": {
      "command": "C:\\Users\\abl030\\scoop\\apps\\nodejs\\current\\node.exe",
      "args": ["C:\\Users\\abl030\\scoop\\apps\\nodejs\\current\\bin\\node_modules\\@playwright\\mcp\\cli.js"]
    }
  }
}
```
Requires: `scoop install nodejs`, then `npm install -g @playwright/mcp@latest` (with PATH set), then `npx playwright install chromium` to download the browser binary (~183MB, stored in `%LOCALAPPDATA%\ms-playwright\`).

**Linux (doc1)**: Use npx directly — Node.js is available system-wide:
```json
{
  "mcpServers": {
    "playwright": {
      "command": "npx",
      "args": ["@playwright/mcp@latest"]
    }
  }
}
```
First run will auto-install the package. You may still need `npx playwright install chromium` for the browser binary.

### Usage notes

- Always use `https://music.ablz.au` (not http — connection will time out)
- `browser_snapshot` returns an accessibility tree (better than screenshots for automation)
- Use `browser_console_messages` with `level: "error"` to check for JS errors after interactions
- Use `browser_wait_for` with `textGone` to wait for loading states to resolve
- `.mcp.json` is gitignored (platform-specific paths) — each machine needs its own

## Critical Rules

1. **NEVER use `beet remove -d`** — deletes files from disk permanently (exception: ban-source endpoint which is an explicit user action)
2. **NEVER import without inspecting the match** — always use the harness, never pipe blind input to beet
3. **NEVER match by candidate_index** — always match by MB release ID (candidate ordering is not stable)
4. **NEVER match by release group** — always exact MB release ID. Release groups conflate different pressings.
5. **Auto-import only for `source='request'`** — redownloads always stage for manual review
6. **All scripts deploy via Nix** — no manual `cp` to virtiofs. Change code → push → flake update → rebuild
7. **PostgreSQL must use `autocommit=True`** — prevents idle-in-transaction deadlocks. DDL migrations run on separate short-lived connections with `lock_timeout`. See the PostgreSQL audit in git history (commit ca579e3).

## Debugging Quality Decisions

When an album has unexpected quality behavior, use these CLI commands on doc2:

```bash
# Full album state: quality columns, download history with import decisions
pipeline-cli show <request_id>

# Quality simulator: current gate status + what would happen for common downloads
pipeline-cli quality <request_id>

# Raw JSONB audit data for a specific download attempt
pipeline-cli debug-download <download_log_id>

# Ad-hoc SQL through the existing DB connection (read-only session)
pipeline-cli query "SELECT id, status, artist_name, album_title FROM album_requests WHERE status = 'wanted' LIMIT 5"

# Multi-line SQL without shell quoting
pipeline-cli query - <<'SQL'
SELECT id, artist_name, album_title, min_bitrate, current_spectral_bitrate
FROM album_requests
WHERE current_spectral_bitrate IS NOT NULL
ORDER BY updated_at DESC
LIMIT 10
SQL
```

`pipeline-cli quality` runs `full_pipeline_decision()` with the album's actual state and shows whether genuine FLAC, V0, CBR 320, or suspect FLAC would be imported or rejected.

`pipeline-cli show` displays the quality columns from `album_requests` (min_bitrate, prev_min_bitrate, verified_lossless, last_download_spectral_grade/bitrate, current_spectral_grade/bitrate, search_filetype_override, target_format) and renders the `ImportResult` JSONB from each download history entry showing the decision chain and measurements.

`pipeline-cli query` executes arbitrary SQL in a session with `default_transaction_read_only = on`, so it is safe for diagnostics but will reject writes. Add `--json` when you need machine-readable output.

## Known Issues

- **Track name matching**: `album_match()` uses fuzzy filename matching — can match wrong pressings with same title. Track title cross-check added as post-match gate but won't catch all cases.
- **Discogs analysis tab**: The disambiguate/analysis tab requires MusicBrainz recording IDs. Not available for Discogs-browsed artists (see #81).
- **Discogs cover art**: Discogs CC0 dump has no images. Discogs-only releases have no cover art in browse UI (see #82).

## MusicBrainz API

Local mirror at `http://192.168.1.35:5200`:
```bash
# Search releases
curl -s "http://192.168.1.35:5200/ws/2/release?query=artist:ARTIST+AND+release:ALBUM&fmt=json"

# Get release with tracks
curl -s "http://192.168.1.35:5200/ws/2/release/MBID?inc=recordings+media&fmt=json"

# Get release group
curl -s "http://192.168.1.35:5200/ws/2/release-group/RGID?inc=releases&fmt=json"
```

## Secrets

- slskd API key: sops-managed, injected into config.ini at runtime
- Discogs token: `~/.config/beets/secrets.yaml` on doc1 (not used by Cratedigger directly)

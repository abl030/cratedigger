# **RUN `hostname` AT THE START OF EVERY CHAT. proxmox-vm = doc1, doc2 = doc2. You are likely already on doc1 — do NOT ssh to doc1 from doc1.**

# **NEVER run destructive SQLite DDL (ALTER TABLE RENAME, DROP TABLE) directly against production databases. SQLite auto-commits DDL even inside BEGIN/COMMIT blocks — a failed migration WILL lose data. Always: (1) copy the DB file first (`cp pipeline.db pipeline.db.bak`), (2) test the migration on the copy, (3) only then run on production. This rule exists because a migration wiped the entire album_requests table (303 rows) on 2026-03-24.**

# Soularr — Music Download Pipeline

A Soulseek download engine driven by a SQLite pipeline database. Searches Soulseek via slskd, validates downloads against MusicBrainz via beets, auto-imports or stages for manual review.

Forked from [mrusse/soularr](https://github.com/mrusse/soularr). This fork has diverged significantly — Lidarr is optional, replaced by a pipeline DB as the source of truth.

## Meelo

Meelo is the self-hosted music server that scans the beets library and serves a browseable catalogue with playback. After soularr auto-imports an album to beets, it triggers a Meelo scanner rescan so the new album appears in the UI immediately. Meelo runs on doc1 (proxmox-vm) as podman containers. For full details on architecture, API access, troubleshooting, and the scanner/refresh workflow, read `docs/meelo-primer.md`.

## Beets

Beets (v2.5.1, Nix-managed on doc1) is the library's source of truth — it matches albums against MusicBrainz, tags files, organizes them into `/Beets`, and maintains its own SQLite DB at `/mnt/virtio/Music/beets-library.db`. All automated imports go through the JSON harness (`harness/beets_harness.py` via `run_beets_harness.sh`), never raw `beet import`. The `musicbrainz` plugin MUST be in the plugins list or beets returns 0 candidates. Always match by `candidate_id` (MB release UUID), never `candidate_index`. For full details on config, commands, the harness protocol, and troubleshooting, read `docs/beets-primer.md`.

## Repository Structure

```
soularr.py              — Main Soularr script (~2400 lines)
album_source.py         — AlbumRecord, DatabaseSource, LidarrSource abstraction
config.ini              — Config template (not used in production — Nix generates it)
lib/
  pipeline_db.py        — PipelineDB class (SQLite CRUD, queries, schema)
harness/
  beets_harness.py      — Beets interactive import harness (JSON protocol over stdin/stdout)
  run_beets_harness.sh  — Shell wrapper to bootstrap Nix beets Python environment
  import_one.py         — One-shot beets import (pre-flight, convert, import, post-flight verify)
scripts/
  pipeline_cli.py       — CLI: list, add, status, retry, cancel, show, migrate
  lidarr_sync.py        — Sync Lidarr wanted albums into pipeline DB
  populate_tracks.py    — Populate tracks from MusicBrainz API
tests/
  test_pipeline_db.py   — 42 tests for PipelineDB
  test_pipeline_cli.py  — 9 tests for CLI
  test_album_source.py  — 14 tests for AlbumSource
  test_beets_validation.py — 18 tests for beets validation
  test_track_crosscheck.py — 15 tests (track title cross-check)
test_soularr.py         — Isolated tests for verify_filetype (AST extraction)
```

## Infrastructure

- **doc1** (`192.168.1.29`): Runs beets (Home Manager), this repo lives at `/home/abl030/soularr`
- **doc2** (`192.168.1.35`): Runs Soularr (systemd oneshot, 5-min timer), MusicBrainz mirror (`:5200`), slskd (`:5030`)
- **Shared storage**: `/mnt/virtio` (virtiofs) — beets DB, pipeline DB, music library all accessible from both machines
- **Nix deployment**: Soularr is a flake input (`soularr-src`) in nixosconfig. All scripts deploy from the Nix store via `${inputs.soularr-src}/...`

### Key Paths

| Path | Machine | Purpose |
|------|---------|---------|
| `/mnt/virtio/Music/pipeline.db` | Shared | Pipeline DB (source of truth) |
| `/mnt/virtio/Music/beets-library.db` | Shared | Beets library DB |
| `/mnt/virtio/Music/Beets` | Shared | Beets library (tagged files) |
| `/mnt/virtio/Music/Incoming` | Shared | Staging area for validated downloads |
| `/mnt/virtio/Music/Re-download` | Shared | READMEs for redownload targets |
| `/mnt/virtio/music/slskd` | doc2 | slskd download directory |
| `/var/lib/soularr` | doc2 | Soularr runtime state (config.ini, lock file, denylists) |

### Accessing doc2

```bash
ssh doc2
sudo journalctl -u soularr -f                    # tail logs
sudo journalctl -u soularr --since "5 min ago"    # recent logs
sudo systemctl is-active soularr                   # check if running
sudo systemctl start soularr &                     # trigger run (DON'T block — it's a oneshot)
sudo cat /var/lib/soularr/config.ini               # view generated config
```

## Pipeline Flow

```
Lidarr (optional)                    CLI / Dashboard
      │                                    │
      │ lidarr_sync.py                     │ pipeline_cli.py add
      ▼                                    ▼
┌──────────────────────────────────────────────┐
│           pipeline.db (SQLite)               │
│  status: wanted→searching→downloading→       │
│          validating→staged→imported           │
└──────────────────┬───────────────────────────┘
                   │ get_wanted()
                   ▼
┌──────────────────────────────────────────────┐
│  Soularr (soularr.py + album_source.py)      │
│  search Soulseek → download → validate       │
└──────────────────┬───────────────────────────┘
                   │
         ┌─────────┴──────────┐
         │                    │
    source=request       source=redownload
    dist ≤ 0.15              │
         │              stage to /Incoming
         ▼              (manual review)
    import_one.py
    (convert → import)
         │
         ▼
      /Beets/
```

## Two-Track Pipeline

- **Requests** (`source='request'`): User-added via Lidarr/CLI. Auto-imported to beets if beets validation passes at distance ≤ 0.15.
- **Redownloads** (`source='redownload'`): Replacing bad source material from LLM review. Always staged to `/Incoming` for manual review, never auto-imported.

## Deploying Changes

```bash
# 1. Edit code, commit, push
cd ~/soularr
git add . && git commit -m "description" && git push

# 2. Update Nix flake input
cd ~/nixosconfig
nix flake update soularr-src
nix fmt
git add flake.lock && git commit -m "soularr: description" && git push

# 3. Deploy to doc2
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'

# 4. Verify (daemon-reload if service unit changed)
ssh doc2 'sudo systemctl daemon-reload; sudo systemctl start soularr' &
```

**IMPORTANT**: `restartIfChanged = false` on the service — deploys don't restart Soularr. The 5-min timer picks up new code on the next cycle, or manually start.

## NixOS Module

Located at: `nixosconfig/modules/nixos/services/soularr.nix`

Key options under `homelab.services.soularr`:
- `enable` — enable service + timer
- `downloadDir` — slskd download directory
- `beetsValidation.enable` — enable beets validation
- `beetsValidation.harnessPath` — path to harness (defaults to `${inputs.soularr-src}/harness/...`)
- `pipelineDb.enable` — use pipeline DB instead of Lidarr
- `pipelineDb.dbPath` — path to SQLite DB

The module:
1. Builds a Python environment with dependencies (requests, pyarr, music-tag, slskd-api)
2. Wraps `soularr.py` in a shell script
3. Generates `config.ini` at runtime from sops secrets
4. Pre-start: health-check slskd → sync Lidarr → integrity-check DB → start Soularr

## Running Tests

```bash
cd ~/soularr
python3 -m unittest discover tests -v    # all 83 tests
python3 -m unittest tests.test_pipeline_db -v   # just pipeline DB
python3 -m unittest tests.test_track_crosscheck  # just track matching
```

## Critical Rules

1. **NEVER use `beet remove -d`** — deletes files from disk permanently
2. **NEVER import without inspecting the match** — always use the harness, never pipe blind input to beet
3. **NEVER match by candidate_index** — always match by MB release ID (candidate ordering is not stable)
4. **Auto-import only for `source='request'`** — redownloads always stage for manual review
5. **All scripts deploy via Nix** — no manual `cp` to virtiofs. Change code → push → flake update → rebuild

## Known Issues

- **SQLite on virtiofs**: Has corrupted multiple times. `PRAGMA synchronous = NORMAL` was the cause — removed, now using SQLite defaults. Migration to PostgreSQL planned.
- **Track name matching**: `album_match()` uses fuzzy filename matching — can match wrong pressings with same title. Track title cross-check added as post-match gate but won't catch all cases.
- **`searching` status not updating**: `update_status()` in main loop doesn't reliably persist — cosmetic issue, doesn't affect functionality.

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

- Lidarr API key: sops-managed, injected via `LIDARR_API_KEY` env var in pre-start
- slskd API key: sops-managed, injected into config.ini at runtime
- Discogs token: `~/.config/beets/secrets.yaml` on doc1 (not used by Soularr directly)

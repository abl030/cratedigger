# **RUN `hostname` AT THE START OF EVERY CHAT. proxmox-vm = doc1, doc2 = doc2. You are likely already on doc1 — do NOT ssh to doc1 from doc1. If hostname returns a Windows machine (e.g. DESKTOP-*), you're on the Windows laptop — see below for SSH access.**

# **Windows laptop SSH access**: There is no native SSH key on Windows. A NixOS WSL2 instance has the SSH key via sops-nix at `/run/secrets/ssh_key_abl030`. To get SSH access to doc1/doc2, run: `mkdir -p ~/.ssh && wsl -d NixOS -- bash -c 'cat /run/secrets/ssh_key_abl030' > ~/.ssh/id_doc2 && chmod 600 ~/.ssh/id_doc2` then SSH with `ssh -i ~/.ssh/id_doc2 abl030@doc2` or `ssh -i ~/.ssh/id_doc2 abl030@proxmox-vm`. The key works for both machines. You may need `-o StrictHostKeyChecking=no` on first use.

# **The pipeline DB is PostgreSQL (migrated from SQLite on 2026-03-25). It runs in an nspawn container on doc2 (192.168.100.11:5432). Access via `pipeline-cli` on doc2's PATH, or from doc1 via `ssh doc2 'pipeline-cli ...'`. Data lives at `/mnt/virtio/soularr/postgres` for portability. Only 3 statuses: wanted, imported, manual.**

# Soularr — Music Download Pipeline

A Soulseek download engine driven by a PostgreSQL pipeline database. Searches Soulseek via slskd, validates downloads against MusicBrainz via beets, auto-imports or stages for manual review. Includes a web UI at `music.ablz.au` for browsing MusicBrainz and adding album requests.

Forked from [mrusse/soularr](https://github.com/mrusse/soularr). This fork has diverged significantly — Lidarr is optional (used only as a mobile album picker), replaced by the pipeline DB as the source of truth.

## Web UI (music.ablz.au)

A single-page web app for browsing the local MusicBrainz mirror, viewing your beets library, and adding releases to the pipeline. Runs on doc2 as `soularr-web` systemd service. No build step — stdlib `http.server`, vanilla JS, single HTML file. For full details on architecture, API endpoints, frontend features, and deployment, read `docs/webui-primer.md`.

## Meelo

Meelo is the self-hosted music server that scans the beets library and serves a browseable catalogue with playback. After soularr auto-imports an album to beets, it triggers a Meelo scanner rescan so the new album appears in the UI immediately. Meelo runs on doc1 (proxmox-vm) as podman containers. For full details on architecture, API access, troubleshooting, and the scanner/refresh workflow, read `docs/meelo-primer.md`.

## Beets

Beets (v2.5.1, Nix-managed on doc1) is the library's source of truth — it matches albums against MusicBrainz, tags files, organizes them into `/Beets`, and maintains its own SQLite DB at `/mnt/virtio/Music/beets-library.db`. All automated imports go through the JSON harness (`harness/beets_harness.py` via `run_beets_harness.sh`), never raw `beet import`. The `musicbrainz` plugin MUST be in the plugins list or beets returns 0 candidates. Always match by `candidate_id` (MB release UUID), never `candidate_index`. For full details on config, commands, the harness protocol, and troubleshooting, read `docs/beets-primer.md`.

## Repository Structure

```
soularr.py              — Main Soularr script (~2400 lines)
album_source.py         — AlbumRecord, DatabaseSource, LidarrSource abstraction
config.ini              — Config template (not used in production — Nix generates it)
web/
  server.py             — Web UI server (http.server, JSON API)
  mb.py                 — MusicBrainz API helpers
  index.html            — Frontend (vanilla JS, inline CSS)
lib/
  pipeline_db.py        — PipelineDB class (PostgreSQL CRUD, queries, schema)
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
| `192.168.100.11:5432/soularr` | doc2 nspawn | Pipeline DB (PostgreSQL, source of truth) |
| `/mnt/virtio/soularr/postgres` | Shared | PostgreSQL data dir (portable) |
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
│           PostgreSQL (pipeline DB)            │
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

Flake input changes (step 2) MUST be done on doc1 and pushed from there. Doc2 has no git push credentials. Doc2 is only for building/running.

**From doc1 (or Windows laptop via SSH to doc1):**
```bash
# 1. Edit code, commit, push (from wherever the repo lives)
cd ~/soularr
git add . && git commit -m "description" && git push

# 2. Update Nix flake input (MUST be on doc1 — it has git push access)
ssh doc1 'cd ~/nixosconfig && nix flake update soularr-src && git add flake.lock && git commit -m "soularr: description" && git push'

# 3. Deploy to doc2
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'

# 4. Restart the web UI (soularr itself picks up changes on next timer cycle)
ssh doc2 'sudo systemctl restart soularr-web'
```

**From doc1 directly:**
```bash
# Steps 2-4 without the ssh wrapper
cd ~/nixosconfig
nix flake update soularr-src
git add flake.lock && git commit -m "soularr: description" && git push
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
ssh doc2 'sudo systemctl restart soularr-web'
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

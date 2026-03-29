# **RUN `hostname` AT THE START OF EVERY CHAT. proxmox-vm = doc1, doc2 = doc2. You are likely already on doc1 — do NOT ssh to doc1 from doc1. If hostname returns a Windows machine (e.g. DESKTOP-*), you're on the Windows laptop — see below for SSH access.**

# **Windows laptop SSH access**: There is no native SSH key on Windows. A NixOS WSL2 instance has the SSH key via sops-nix at `/run/secrets/ssh_key_abl030`. To get SSH access to doc1/doc2, run: `mkdir -p ~/.ssh && wsl -d NixOS -- bash -c 'cat /run/secrets/ssh_key_abl030' > ~/.ssh/id_doc2 && chmod 600 ~/.ssh/id_doc2` then SSH with `ssh -i ~/.ssh/id_doc2 abl030@doc2` or `ssh -i ~/.ssh/id_doc2 abl030@proxmox-vm`. The key works for both machines. You may need `-o StrictHostKeyChecking=no` on first use.

# **The pipeline DB is PostgreSQL (migrated from SQLite on 2026-03-25). It runs in an nspawn container on doc2 (192.168.100.11:5432). Access via `pipeline-cli` on doc2's PATH, or from doc1 via `ssh doc2 'pipeline-cli ...'`. Data lives at `/mnt/virtio/soularr/postgres` for portability. Only 3 statuses: wanted, imported, manual.**

# **NIXOSCONFIG CHANGES MUST BE MADE ON DOC1. The nixosconfig repo lives at `~/nixosconfig` on doc1. All edits, commits, and pushes MUST happen there — doc1 has the git push credentials. NEVER try to edit nixosconfig from doc2 or Windows. SSH to doc1 first, make the change, commit, push, then deploy to doc2.**

# **This is a curated music collection. Multiple editions/pressings of the same album are intentional and must be preserved. NEVER delete or merge duplicate albums — they are different MusicBrainz releases (different countries, track counts, labels, etc.) and the user wants them all. Beets must disambiguate them into separate folders on disk.**

# Soularr — Music Download Pipeline

A Soulseek download engine driven by a PostgreSQL pipeline database. Searches Soulseek via slskd, validates downloads against MusicBrainz via beets, auto-imports or stages for manual review. Includes a web UI at `music.ablz.au` for browsing MusicBrainz and adding album requests.

Forked from [mrusse/soularr](https://github.com/mrusse/soularr). This fork has diverged significantly — the pipeline DB is the sole source of truth, and the web UI at `music.ablz.au` is the album picker.

## Web UI (music.ablz.au)

A single-page web app for browsing the local MusicBrainz mirror, viewing your beets library, and adding releases to the pipeline. Runs on doc2 as `soularr-web` systemd service. No build step — stdlib `http.server`, vanilla JS, single HTML file. For full details on architecture, API endpoints, frontend features, and deployment, read `docs/webui-primer.md`.

## Meelo

Meelo is the self-hosted music server that scans the beets library and serves a browseable catalogue with playback. After soularr auto-imports an album to beets, it triggers a Meelo scanner rescan so the new album appears in the UI immediately. Meelo runs on doc1 (proxmox-vm) as podman containers. For full details on architecture, API access, troubleshooting, and the scanner/refresh workflow, read `docs/meelo-primer.md`.

## Beets

Beets (v2.5.1, Nix-managed on doc1) is the library's source of truth — it matches albums against MusicBrainz, tags files, organizes them into `/Beets`, and maintains its own SQLite DB at `/mnt/virtio/Music/beets-library.db`. All automated imports go through the JSON harness (`harness/beets_harness.py` via `run_beets_harness.sh`), never raw `beet import`. The `musicbrainz` plugin MUST be in the plugins list or beets returns 0 candidates. Always match by `candidate_id` (MB release UUID), never `candidate_index`. For full details on config, commands, the harness protocol, and troubleshooting, read `docs/beets-primer.md`.

## Repository Structure

```
soularr.py              — Main Soularr script (~2100 lines)
album_source.py         — AlbumRecord, DatabaseSource abstraction
config.ini              — Config template (not used in production — Nix generates it)
web/
  server.py             — Web UI server (http.server, JSON API)
  mb.py                 — MusicBrainz API helpers
  index.html            — Frontend (vanilla JS, inline CSS)
lib/
  beets.py              — Beets library helpers (query bitrate, check existing albums)
  config.py             — SoularrConfig dataclass (typed config from config.ini)
  grab_list.py          — GrabList: wanted-album selection with priority/ordering
  pipeline_db.py        — PipelineDB class (PostgreSQL CRUD, queries, schema)
  quality.py            — Quality classification and upgrade logic
  search.py             — Search query building and normalization
  spectral_check.py     — Spectral analysis (sox-based transcode detection)
harness/
  beets_harness.py      — Beets interactive import harness (JSON protocol over stdin/stdout)
  run_beets_harness.sh  — Shell wrapper to bootstrap Nix beets Python environment
  import_one.py         — One-shot beets import (pre-flight, convert, import, post-flight verify)
scripts/
  pipeline_cli.py       — CLI: list, add, status, retry, cancel, show, migrate
  populate_tracks.py    — Populate tracks from MusicBrainz API
tests/
  test_album_source.py  — 11 tests for AlbumSource
  test_beets_validation.py — 19 tests for beets validation
  test_config.py         — 41 tests for SoularrConfig
  test_grab_list.py      — 60 tests for GrabList
  test_pipeline_cli.py   — 7 tests for CLI
  test_pipeline_db.py    — 33 tests for PipelineDB
  test_quality_classification.py — 17 tests for quality classification
  test_search.py         — 32 tests for search query building
  test_spectral_check.py — 39 tests for spectral analysis
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
Web UI (music.ablz.au)               CLI
      │                                │
      │ /api/add                       │ pipeline_cli.py add
      ▼                                ▼
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

## Quality Upgrade System

The pipeline automatically upgrades album quality toward VBR V0 from verified lossless sources. This is the core differentiator — it doesn't just download albums, it curates them.

### Gold Standard

The target quality for every album is: **FLAC downloaded from Soulseek → spectral analysis confirms genuine lossless → convert to VBR V0**. The VBR bitrate acts as a permanent quality fingerprint (genuine CD rips → ~240-260kbps, transcodes → ~190kbps). CBR 320 is never a final state — it's unverifiable.

### Quality Gate (`_check_quality_gate()` in soularr.py)

After every import, the quality gate decides what to do next. It checks these conditions in order:

1. **`verified_lossless=TRUE` + any bitrate** → **DONE**. We verified this from genuine FLAC. Low V0 bitrate (e.g. 207kbps) on lo-fi music is fine — the source is proven lossless.
2. **`min_bitrate < 210kbps`** → **RE-QUEUE** for upgrade. Bad quality, search for better.
3. **CBR on disk** (all tracks same bitrate) **+ not verified_lossless** → **RE-QUEUE for FLAC only** (`quality_override="flac"`). CBR is unverifiable — spectral analysis can detect obvious upsamples but cannot prove a CBR file came from lossless source.
4. **VBR above 210kbps** → **DONE**. VBR bitrate is trustworthy.

### Two Key Concepts (don't confuse them)

- **`spectral_grade`** (on both `download_log` and `album_requests`): "Does this file look like a transcode?" — answers whether spectral analysis found cliff artifacts or high-frequency deficits. Works on any file type. A CBR 320 with `spectral_grade=genuine` just means "no cliff detected" — it does NOT mean the source was lossless.
- **`verified_lossless`** (on `album_requests` only): "Did we verify this from a genuine FLAC?" — only set `TRUE` when: downloaded FLAC + spectral analysis said genuine + converted to V0. This is the only way to prove source quality.

### How Downloads Flow by Type

**FLAC downloads** (in `import_one.py`):
1. Spectral check on raw FLAC → grade stored on album_requests
2. Convert FLAC → V0 via ffmpeg (`-q:a 0`)
3. Compare new V0 bitrate against existing on disk (pipeline DB `min_bitrate` overrides beets via `--override-min-bitrate`)
4. If upgrade → import to beets, set `verified_lossless=TRUE` if spectral=genuine
5. Quality gate accepts regardless of bitrate if verified_lossless

**MP3 VBR downloads** (V0/V2):
1. No spectral check needed — VBR bitrate IS the quality signal
2. Import directly, quality gate checks min_bitrate against 210kbps threshold

**MP3 CBR downloads** (320, 256, etc.):
1. Spectral check runs in `process_completed_album()` (soularr.py) — detects upsampled garbage via cliff detection
2. If spectral says SUSPECT → reject, denylist user
3. If spectral says genuine or marginal → import (something is better than nothing)
4. Quality gate sees CBR + not verified_lossless → re-queues with `quality_override="flac"` to find lossless source

### Spectral Analysis (`lib/spectral_check.py`)

Uses `sox` bandpass filtering to detect transcodes. Measures RMS energy in 16 x 500Hz frequency slices from 12-20kHz, computes gradient between adjacent slices. A transcode has a sharp "cliff" at the original encoder's lowpass frequency. Genuine audio has gradual rolloff.

- **Cliff detection**: 2+ consecutive slices with gradient < -12 dB/kHz → SUSPECT
- **HF deficit**: avg energy at 18-20kHz vs 1-4kHz reference > 60dB → SUSPECT
- Album level: >60% tracks suspect → album SUSPECT
- Dependencies: `sox` (in Nix PATH)
- Performance: ~8s per track (30s trim), ~100s per 12-track album
- Full docs: `docs/quality-verification.md`

### Key Fields (`album_requests` table)

- `quality_override TEXT` — CSV filetype list (e.g. `"flac,mp3 v0,mp3 320"` or just `"flac"`). Overrides global `allowed_filetypes` for this album.
- `min_bitrate INTEGER` — Current min track bitrate in kbps (from beets).
- `prev_min_bitrate INTEGER` — Previous min_bitrate before last upgrade. Shows delta in UI.
- `verified_lossless BOOLEAN` — True only when imported from spectral-verified genuine FLAC→V0.
- `spectral_grade TEXT` — Latest spectral analysis result ("genuine", "suspect", "marginal").
- `spectral_bitrate INTEGER` — Estimated original bitrate from cliff detection (kbps).

### Key Fields (`download_log` table)

- `slskd_filetype TEXT` — What Soulseek advertised ("flac", "mp3").
- `actual_filetype TEXT` — What's on disk after download/conversion.
- `spectral_grade TEXT` — Spectral analysis of the downloaded files.
- `spectral_bitrate INTEGER` — Estimated original bitrate from spectral.
- `existing_min_bitrate INTEGER` — Beets min bitrate before this download.
- `existing_spectral_bitrate INTEGER` — Spectral estimate of existing files before download.

### Downgrade Prevention (`import_one.py`)

- `--override-min-bitrate` arg: soularr.py passes the pipeline DB `min_bitrate` to import_one.py. When existing files are known garbage (e.g. `min_bitrate=0`), this overrides the beets comparison so genuine V0 at 227kbps isn't rejected as "downgrade from 320kbps."
- Exit codes: 0=imported, 1=conversion failed, 2=beets failed, 3=path not found, 5=downgrade, 6=transcode (may or may not have imported as upgrade)

### New/Re-queued Album Priority

`get_wanted()` sorts by `search_attempts=0` first, then random. New requests and upgrade re-queues always get picked up on the next cycle.

### Web UI Controls

- **Recents tab** ("validation pipeline log"): Shows every download with full quality flow (slskd reported → actual on disk → spectral → existing). Badges: Upgraded, New import, Wrong match, Transcode, Quality mismatch.
- **Library tab**: Quality label per album (MP3 V0, MP3 320, etc.). Upgrade button. Accept button (sets avg bitrate for lo-fi edge cases).
- **Ban source**: Denylists user + removes from beets + requeues.

### Edge Cases

- **Lo-fi recordings** (Mountain Goats boombox era): Genuine V0 from verified FLAC can produce ~207kbps. `verified_lossless=TRUE` lets this pass the quality gate.
- **Mixed-source CBR** (e.g. 13 tracks at 320 + 1 track at 192): Looks like VBR to `COUNT(DISTINCT bitrate)` but isn't genuine V0. Quality gate uses min_bitrate (192 < 210) → re-queues.
- **Fake FLACs**: MP3 wrapped in FLAC container. Spectral detects cliff pre-conversion, V0 bitrate confirms post-conversion. Source denylisted, but file imported if better than existing.
- **Discogs-sourced albums**: Numeric IDs instead of MB UUIDs. Cannot use upgrade pipeline. See `TODO.md`.

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
- `pipelineDb.enable` — use pipeline DB as album source
- `pipelineDb.dbPath` — PostgreSQL connection string

The module:
1. Builds a Python environment with dependencies (requests, music-tag, slskd-api, psycopg2)
2. Wraps `soularr.py` in a shell script
3. Generates `config.ini` at runtime from sops secrets
4. Pre-start: health-check slskd → integrity-check DB → start Soularr

## Running Tests

```bash
cd ~/soularr
nix-shell                                          # enter dev shell with dependencies
python3 -m unittest discover tests -v              # all tests
python3 -m unittest tests.test_pipeline_db -v      # just pipeline DB
python3 -m unittest tests.test_track_crosscheck    # just track matching
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

1. **NEVER use `beet remove -d`** — deletes files from disk permanently (exception: ban-source endpoint which is an explicit user action)
2. **NEVER import without inspecting the match** — always use the harness, never pipe blind input to beet
3. **NEVER match by candidate_index** — always match by MB release ID (candidate ordering is not stable)
4. **NEVER match by release group** — always exact MB release ID. Release groups conflate different pressings.
5. **Auto-import only for `source='request'`** — redownloads always stage for manual review
6. **All scripts deploy via Nix** — no manual `cp` to virtiofs. Change code → push → flake update → rebuild
7. **PostgreSQL must use `autocommit=True`** — prevents idle-in-transaction deadlocks. DDL migrations run on separate short-lived connections with `lock_timeout`. See the PostgreSQL audit in git history (commit ca579e3).

## Known Issues

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

- slskd API key: sops-managed, injected into config.ini at runtime
- Discogs token: `~/.config/beets/secrets.yaml` on doc1 (not used by Soularr directly)

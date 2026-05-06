# music.ablz.au — Web UI Primer

## What It Is

A single-page web app for browsing MusicBrainz and adding album releases to the Cratedigger pipeline. Replaces Lidarr as the album picker. Served at `https://music.ablz.au`.

## Architecture

```
Browser → https://music.ablz.au
           → nginx (localProxy on doc2, ACME cert)
             → localhost:8085
               → web/server.py (stdlib http.server)
                 → PostgreSQL (pipeline DB, nspawn container 192.168.100.11)
                 → SQLite (beets library, /mnt/virtio/Music/beets-library.db, read-only)
                 → MusicBrainz API (local mirror, 192.168.1.35:5200)
```

- **No build step, no npm, no framework** — stdlib `http.server`, vanilla JS, single HTML file
- Runs on doc2 as `cratedigger-web` systemd service
- Python env shared with cratedigger (psycopg2, requests, etc.)

## Files

| File | Purpose |
|------|---------|
| `web/server.py` | HTTP server with JSON API endpoints |
| `web/mb.py` | MusicBrainz API helpers (search, artist discography, releases) |
| `web/discogs.py` | Discogs mirror API helpers (search, artist releases, master pressings) |
| `web/index.html` | Frontend — single HTML file with inline CSS + JS |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Serves the HTML UI |
| `/api/search?q=...` | GET | Search MB for artists |
| `/api/artist/<mbid>` | GET | Artist's release groups + official/bootleg classification |
| `/api/release-group/<mbid>` | GET | All releases for a release group (paginated from MB) |
| `/api/release/<mbid>` | GET | Full release details with tracks |
| `/api/pipeline/add` | POST | Add a release to the pipeline DB `{"mb_release_id": "..."}` or `{"discogs_release_id": "..."}` |
| `/api/pipeline/status` | GET | Pipeline DB status counts + wanted list |
| `/api/pipeline/<id>` | GET | Single request details |
| `/api/pipeline/force-import` | POST | Queue force-import for a rejected download `{"download_log_id": N}`; returns `202` + job id |
| `/api/manual-import/import` | POST | Queue manual import for a matched folder |
| `/api/wrong-matches` | GET | Group rejected downloads by release for triage |
| `/api/wrong-matches/explorer` | GET | List files for one wrong-match candidate, including extracted tags and audio-preview URLs |
| `/api/wrong-matches/audio` | GET | Stream an individual wrong-match audio file with byte-range support |
| `/api/wrong-matches/converge` | POST | Queue every wrong-match candidate within a release's loosen threshold and delete the rest |
| `/api/wrong-matches/triage` | POST | Re-run wrong-match auto-triage for a single candidate |
| `/api/wrong-matches/delete-transparent-non-flac` | POST | Bulk-delete wrong-match folders whose exact library copy is already transparent and whose pending downloads are non-FLAC |
| `/api/import-jobs` | GET | List recent import queue jobs |
| `/api/import-jobs/timeline` | GET | List active queued/running import jobs in Recents queue order |
| `/api/import-jobs/<id>` | GET | Poll a single import queue job |
| `/api/library/artist?name=...` | GET | Albums by artist from beets library (MB vs Discogs source) |
| `/api/discogs/search?q=...` | GET | Search Discogs mirror (artist or release mode via `type=` param) |
| `/api/discogs/artist/<id>` | GET | Artist's releases grouped by master (via `/api/artists/{id}/releases`) |
| `/api/discogs/master/<id>` | GET | All pressings within a Discogs master release |
| `/api/discogs/release/<id>` | GET | Full Discogs release details with tracks |

## Frontend Features

- **Source toggle** — MB / Discogs toggle in the browse tab header. Switches all search, artist, and release views between MusicBrainz and Discogs data sources.
- **Search** — debounced text search, returns artists (or releases in album mode)
- **Artist discography** — grouped by type (Albums, EPs, Singles, etc.)
  - Split into "own work" vs "Appearances" using artist-credit matching
  - Bootleg-only release groups collapsed at bottom
- **In Library section** — shows what you already own from beets, with MB/Discogs badges
- **Release editions** — when you expand a release group, shows all editions sorted by date
  - Official releases first, bootleg/promo collapsed
  - Releases already in pipeline DB or beets library are badged
  - Click release metadata to open MB release page in new tab
- **Add button** — adds release to pipeline DB (same logic as `pipeline-cli add`)
- **Pipeline tab** — status dashboard (wanted/imported/manual counts + wanted list)
- **Wrong Matches tab** — the old Complete-folder manual-import page is gone;
  the tab now opens straight into Wrong Matches. Import actions queue work and
  poll `import_jobs`, so long beets imports do not block the web request.
  Failed queued force-imports remove the reviewed wrong-match source from the
  actionable list while preserving the failed job/download audit.
- **Recents Queue subview** — Recents has History and Queue subviews. Queue
  shows import jobs in beets-import order, with preview states (`waiting`,
  `previewing`, `importable`, `uncertain`, `failed`) and preview messages
  visible before the serial importer claims work.
- **Wrong Matches Converge** — each release starts with a `180` milli-distance
  loosen threshold. Candidates at or below that threshold turn green; Converge
  queues those folders as force-import jobs and deletes the non-green folders
  in one action, then removes the release row without repainting the whole
  review pane.
- **Wrong Matches explorer** — expanding a candidate now shows the original
  downloaded folder names captured from the Soulseek user, a per-file explorer,
  extracted audio tags, and inline browser playback for supported audio files.
- **Wrong Matches bulk cleanup** — top-level cleanup deletes pending non-FLAC
  wrong-match folders for releases that already have an exact transparent copy
  in beets, leaving FLAC candidates for manual review.
- **Wrong Matches triage** — new download-path wrong-match rejections are
  previewed immediately after their `download_log` row is created. The
  `/api/wrong-matches/triage` endpoint can rerun the same policy for one
  candidate. Triage deletes only `cleanup_eligible` confident rejects;
  would-import and uncertain candidates stay visible for operator review. The
  action and reason are stored in
  `download_log.validation_result.wrong_match_triage` and are surfaced in
  Recents History: collapsed cards show a triage chip, and expanded download
  history shows action, preview, reason, and stage-chain detail.
- **Decisions tab** — pipeline decision diagram generated from `get_decision_tree()` with FLAC/MP3 branching paths, all stages/rules/thresholds from live code. Includes a "dispatch" stage showing post-import action mapping (mark_done/failed, denylist, requeue) driven by `dispatch_action()`. Interactive simulator calls the value-preview adapter through `/api/pipeline/simulate` with presets for known scenarios.

## Dev Server Workflows

`scripts/web_dev_server.py` exists so you can edit local frontend files without
deploying `music.ablz.au`. The important split is:

- `--data live-db` — run local route code against a read-only PostgreSQL
  session and the backend host's filesystem.
- `--data prod-api` — serve local `web/` files while proxying `/api/*` to some
  other read-only backend.

For Wrong Matches, the backend must be able to read the actual rejected folders
from disk. A laptop with only DB access is not enough because the explorer and
audio endpoints open the real files. In this homelab, `doc1` and `doc2` are the
useful backend hosts.

A practical "develop anywhere" loop is:

```bash
# backend host shell
PIPELINE_DB_DSN=postgresql://cratedigger@127.0.0.1:15432/cratedigger \
  nix-shell --run "python3 scripts/web_dev_server.py --data live-db --host 127.0.0.1 --port 8096"

# local machine shell
ssh -N -L 18096:127.0.0.1:8096 <backend-host>
nix-shell --run "python3 scripts/web_dev_server.py --data prod-api --prod-base-url http://127.0.0.1:18096 --host 127.0.0.1 --port 8096"
```

If the backend host does not have direct reachability to `192.168.100.11:5432`,
add an SSH tunnel there first:

```bash
ssh -N -L 15432:192.168.100.11:5432 doc2
```

The local proxy forwards byte-range headers, so wrong-match audio preview and
seek still work through the tunnel.

## NixOS Configuration

The upstream module declares the web options at `nix/module.nix` in this repo:

```nix
services.cratedigger.web = {
  enable = mkOption { type = types.bool; default = false; };
  port = mkOption { type = types.port; default = 8085; };
  beetsDb = mkOption { type = types.str; description = "Path to beets-library.db (read-only)"; };
  redis = {
    host = mkOption { type = types.str; default = "127.0.0.1"; };  # follows services.cratedigger.redis by default
    port = mkOption { type = types.port; default = 6379; };
  };
};
services.cratedigger.redis.enable = mkOption { type = types.bool; default = true; };
```

Enabled in this homelab via `~/nixosconfig/hosts/doc2/configuration.nix` (the upstream module now owns `redis-cratedigger.service`; the homelab wrapper only supplies site-specific wiring such as reverse proxy defaults):

```nix
# in hosts/doc2/configuration.nix — picks up the wrapper's defaults
homelab.services.cratedigger.enable = true;
# the wrapper sets services.cratedigger.web.enable = true; on its own
```

What this creates on doc2:
- `cratedigger-web.service` — simple type, restart on failure, ExecStart wraps `web/server.py` with the python env from `nix/package.nix`
- `cratedigger-importer.service` — long-lived worker that drains queued
  force/manual/automation imports after DB migrations have run
- `cratedigger-import-preview-worker.service` — long-lived async preview worker
  that prepares queued jobs for the serial importer when
  `services.cratedigger.importer.preview.enable = true`; defaults to two worker
  loops via `services.cratedigger.importer.previewWorkers`
- `redis-cratedigger.service` — provided by the upstream module as `services.redis.servers.cratedigger`
- `music.ablz.au` nginx reverse proxy via `homelab.localProxy.hosts` (homelab wrapper)
- Cloudflare DNS + ACME cert auto-provisioned

## Deployment

Code changes in `web/` deploy via the normal cratedigger flake update:

```bash
cd ~/cratedigger && git add web/ && git commit -m "..." && git push
cd ~/nixosconfig && nix flake update cratedigger-src && nix fmt
git add flake.lock && git commit -m "..." && git push
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
```

The service auto-restarts when the Nix store path changes.

After deploy, check `systemctl status cratedigger-import-preview-worker
cratedigger-importer` and the worker journals. The Recents Queue subview shows
only active queued/running import jobs as they move from waiting preview, to
previewing, to importable, to importing. Completed, failed, or preview-rejected
rows are history/audit rows, not live queue rows.
On doc2 the homelab wrapper opts into the preview gate explicitly. Deployments
that leave `services.cratedigger.importer.preview.enable = false` should not
start the preview worker; their newly queued jobs are importable immediately.

## MusicBrainz API Usage

All queries hit the local mirror at `http://192.168.1.35:5200/ws/2`.

Key endpoints used:
- `artist?query=NAME&fmt=json` — artist search
- `release-group?artist=MBID&inc=artist-credits&fmt=json` — discography with credits
- `release?artist=MBID&status=official&inc=release-groups&fmt=json` — official release RG IDs
- `release?release-group=MBID&inc=media&fmt=json` — all releases for a release group (paginated)
- `release-group/MBID?fmt=json` — release group metadata
- `release/MBID?inc=recordings+artist-credits+media&fmt=json` — full release with tracks

## Beets Library Integration

Reads `/mnt/virtio/Music/beets-library.db` (SQLite, read-only) to show what you own:
- Queries `albums` table by `albumartist LIKE %name%`
- Distinguishes MB imports (UUID in `mb_albumid`) from Discogs imports (numeric ID or `discogs_albumid` set)
- Also checks individual release MBIDs against beets for the "in library" badge on editions

## Known Issues

- **Born to Run bug** — some release groups with 100+ releases intermittently fail to render in the frontend. Likely a JS rendering or caching issue. Needs browser dev tools to debug.
- **Beatles loading time** — ~6 seconds to load due to fetching official release RG IDs (1000+ release groups, 2000+ releases). Acceptable but could be cached.
- **No auth** — internal network only. If exposed externally, needs auth added.
- **No websocket/live updates** — pipeline status is fetched on tab switch, not live.

# music.ablz.au — Web UI Primer

## What It Is

A single-page web app for browsing MusicBrainz and adding album releases to the Soularr pipeline. Replaces Lidarr as the album picker. Served at `https://music.ablz.au`.

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
- Runs on doc2 as `soularr-web` systemd service
- Python env shared with soularr (psycopg2, requests, etc.)

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
| `/api/pipeline/force-import` | POST | Force-import a rejected download `{"download_log_id": N}` |
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
- **Decisions tab** — pipeline decision diagram generated from `get_decision_tree()` with FLAC/MP3 branching paths, all stages/rules/thresholds from live code. Includes a "dispatch" stage showing post-import action mapping (mark_done/failed, denylist, requeue) driven by `dispatch_action()`. Interactive simulator calls `full_pipeline_decision()` via `/api/pipeline/simulate` with presets for known scenarios.

## NixOS Configuration

In `soularr.nix`:

```nix
# Options under homelab.services.soularr.web:
web = {
  enable = mkEnableOption "music.ablz.au web UI";
  port = mkOption { type = types.port; default = 8085; };
  beetsDb = mkOption { type = types.str; default = "/mnt/virtio/Music/beets-library.db"; };
};
```

Enabled in doc2's `configuration.nix`:
```nix
homelab.services.soularr.web.enable = true;
```

Creates:
- `soularr-web` systemd service (simple, restart on failure)
- `music.ablz.au` nginx reverse proxy via localProxy
- Cloudflare DNS + ACME cert auto-provisioned

## Deployment

Code changes in `web/` deploy via the normal soularr flake update:

```bash
cd ~/soularr && git add web/ && git commit -m "..." && git push
cd ~/nixosconfig && nix flake update soularr-src && nix fmt
git add flake.lock && git commit -m "..." && git push
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
```

The service auto-restarts when the Nix store path changes.

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

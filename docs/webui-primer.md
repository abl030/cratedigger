# music.ablz.au — Web UI Primer

## What It Is

A single-page web app for browsing MusicBrainz and adding album releases to the Cratedigger pipeline. Replaces Lidarr as the album picker. Served at `https://music.ablz.au`.

## Architecture

```
Browser → https://music.ablz.au
           → nginx (localProxy on doc2, ACME cert)
             → localhost:8085
               → web/server.py (stdlib http.server)
                 → PostgreSQL (pipeline DB, nspawn container 10.20.0.11)
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
| `/api/wrong-matches/triage` | POST | Evidence-only full-queue Wrong Matches cleanup; requires `{"confirm_all_wrong_matches": true}` |
| `/api/import-jobs` | GET | List recent import queue jobs |
| `/api/import-jobs/timeline` | GET | List active queued/running import jobs in Recents queue order |
| `/api/import-jobs/<id>` | GET | Poll a single import queue job |
| `/api/library/artist?name=...` | GET | Albums by artist from beets library (MB vs Discogs source) |
| `/api/discogs/search?q=...` | GET | Search Discogs mirror (artist or release mode via `type=` param) |
| `/api/discogs/artist/<id>` | GET | Artist's releases grouped by master (via `/api/artists/{id}/releases`) |
| `/api/discogs/master/<id>` | GET | All pressings within a Discogs master release |
| `/api/discogs/release/<id>` | GET | Full Discogs release details with tracks |

## Frontend Features

- **Source toggle** — a labelled **Source** MB / Discogs switch in the browse tab
  header. The selected source is the *primary* discography for all search, artist,
  and release views; the other source only *fills in* releases the primary is
  missing, surfaced as the appended "Only on …" section. A live hint line under the
  switch spells this out ("MusicBrainz is primary · Discogs fills the rest …") and
  flips when you toggle, so the primary/complement relationship is never a mystery.
- **Search** — debounced text search, returns artists (or releases in album mode)
- **Unified artist page (#575 PR4)** — one scrolling page (the old
  Discography / Analysis / Library / Compare sub-tabs are gone), sectioned by
  availability: **In library / In flight / Missing / Appearances /
  Bootleg-only releases**, each grouped by type (Albums, EPs, Singles, etc.).
  Ownership ("own work" vs Appearances) uses artist-credit matching; Missing =
  official own-work release groups the beets library doesn't hold; In flight =
  requests currently `downloading` or `manual` (`wanted` is ambient after the
  full-library backfill and stays a badge). Two slow feeds decorate the page
  after the fast render, without re-rendering: `/api/artist/compare` appends an
  "Only on Discogs/MusicBrainz" complement section (silently skipped on hosts
  without the Discogs mirror), and `/api/artist/<id>/disambiguate` adds
  unique-track / covered-by chips to rows plus colour-dot recordings
  breakdowns inside expanded release groups (MB artists only). Expanding an
  in-library pressing's detail offers a lazy **Library detail** panel (path,
  download history, status / min-bitrate / intent controls) fetched from
  `/api/beets/album/<id>`.
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
- **Recents evidence schema (#575 PR2)** — History list rows carry a compact
  monospace `IN … HAVE …` evidence strip (measured incoming bitrate/spectral/
  V0 probe vs on-disk at download time); rows with no measurements (download-
  phase failures) show none. Expanded download-history blocks render a fixed
  Source / Spectral / Bitrate / Distance vocabulary (em-dash when unknown, one
  label/value pair per row so columns never shift), with the server-classified
  badge as the header — the same words as the list badges, never the raw
  outcome enum. Force imports show `overridden` in the Distance row instead of
  beets' misleading 0.000. Debug internals (Preview / Reason / Stages) sit
  behind a collapsed `forensics` toggle per attempt.
- **Comparison basis rendering (request 6039)** — rows whose
  `import_result` JSONB carries the persisted `comparison_basis` render the
  decision's own comparison: the verdict line names the deciding metric,
  values, and ranks ("Upgrade: MP3 avg 196k (good) → avg 288k
  (transparent)"), the strip's IN/HAVE sides show `fmt metric value · rank`,
  and the detail grid gains a "Compared" row (with a verified-lossless-bypass
  note when that changed the outcome). Rows predating the field fall back to
  the legacy min-bitrate labels — the ones that rendered a real avg 196→288
  rank upgrade as "MP3 V2 to MP3 V2". See docs/quality-verification.md
  § "Comparison basis".
- **Wrong Matches Converge** — each release starts with a `180` milli-distance
  loosen threshold. Candidates at or below that threshold turn green; Converge
  queues those folders as force-import jobs and deletes the non-green folders
  in one action, then removes the release row without repainting the whole
  review pane.
- **Wrong Matches explorer** — expanding a candidate now shows the original
  downloaded folder names captured from the Soulseek user, a per-file explorer,
  extracted audio tags, and inline browser playback for supported audio files.
- **Wrong Matches cleanup** — one top-level action runs over the full Wrong
  Matches queue. It consumes existing evidence only, deletes force-mode
  confident cleanup-eligible rejects, and leaves would-import, uncertain,
  missing-evidence, stale-evidence, active-job, and missing-path candidates for
  review. The result is shown as a summary toast and the pane refreshes.
- **Wrong Matches history** — old rows with
  `download_log.validation_result.wrong_match_triage` still render their
  historical chip/detail in Recents. New cleanup does not write that blob.

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

If the backend host does not have direct reachability to `10.20.0.11:5432`,
add an SSH tunnel there first:

```bash
ssh -N -L 15432:10.20.0.11:5432 doc2
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

# Meelo Primer

## What Meelo Is

Meelo is a self-hosted music server that scans a library of tagged audio files, builds a browseable catalogue with artist/album/release/track hierarchy, fetches external metadata (MusicBrainz, Discogs, Genius, Wikipedia, AllMusic, etc.), and serves a web UI for browsing and playback. It uses Kyoo's transcoder for on-the-fly audio transcoding.

Upstream: https://github.com/Arthi-chaud/Meelo
Wiki: https://github.com/Arthi-chaud/Meelo/wiki

## Where Meelo Runs

- Host: proxmox-vm (doc1, VMID 104, NixOS)
- IP: 192.168.1.29
- Service: OCI containers managed by NixOS `virtualisation.oci-containers` (podman)
- Port: 5001 (nginx gateway inside container network, mapped to host)
- External access: https://meelo.ablz.au via nginx reverse proxy (localProxy)
- Data dir: /mnt/virtio/meelo
- Music library: /mnt/virtio/Music (the entire Music tree, mounted read-only into containers)

## API Access

Meelo has three APIs, each with different auth:

| API | Base URL | Auth | Purpose |
|-----|----------|------|---------|
| Server (REST) | `http://localhost:5001/api/` | `x-api-key` header | Albums, releases, tracks, libraries |
| Scanner | `http://localhost:5001/scanner/` | JWT `Authorization: Bearer` | Scan, refresh, clean |
| Matcher | `http://localhost:5001/matcher/` | None (read-only status) | External metadata matching |

### Credentials

- **Web UI login**: username `abl030`, password `billand1`
- **API key** (for server API): stored in `API_KEYS` env var in sops-encrypted `meelo.env`. Retrieve at runtime: `sudo podman exec meelo-server printenv API_KEYS`
- **JWT** (for scanner API): obtain via login endpoint, valid for ~100 days

### API Examples

```bash
# Server API — uses x-api-key header
API_KEY=$(sudo podman exec meelo-server printenv API_KEYS)
curl -s "http://localhost:5001/api/libraries" -H "x-api-key: $API_KEY"
curl -s "http://localhost:5001/api/albums?query=search+term" -H "x-api-key: $API_KEY"

# Scanner API — uses JWT from login
JWT=$(curl -s -X POST "http://localhost:5001/api/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"abl030","password":"billand1"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
curl -s -X POST "http://localhost:5001/scanner/refresh?album=<slug>&force=true" \
  -H "Authorization: Bearer $JWT"

# Matcher API — no auth needed
curl -s "http://localhost:5001/matcher/"
```

## Accessing proxmox-vm (doc1)

```bash
# SSH to doc1
ssh proxmox-vm

# Check all meelo container services
ssh proxmox-vm 'systemctl list-units "podman-meelo-*" --no-pager'

# Check a specific container
ssh proxmox-vm 'systemctl status podman-meelo-server.service --no-pager'

# Tail server logs
ssh proxmox-vm 'journalctl -u podman-meelo-server.service -f'

# Tail scanner logs
ssh proxmox-vm 'journalctl -u podman-meelo-scanner.service -f'

# Check container health
ssh proxmox-vm 'podman ps --filter name=meelo --format "table {{.Names}}\t{{.Status}}"'

# Restart all meelo containers (restart the network service triggers all dependents)
ssh proxmox-vm 'sudo systemctl restart podman-meelo-server.service'

# Web UI
# https://meelo.ablz.au
```

## Architecture — Container Stack

Meelo runs as 9 interconnected containers on a shared podman network called `meelo`:

| Container | Image | Port | Purpose |
|-----------|-------|------|---------|
| meelo-db | postgres:alpine3.14 | 5432 (internal) | PostgreSQL database |
| meelo-mq | rabbitmq:4.2-alpine | 5672 (internal) | Message queue for async tasks |
| meelo-search | meilisearch:v1.5 | 7700 (internal) | Full-text search engine |
| meelo-server | arthichaud/meelo-server | 4000 (internal) | Core API server (NestJS) |
| meelo-scanner | arthichaud/meelo-scanner | 8133 (internal) | File scanner (Go) — reads library, extracts metadata |
| meelo-matcher | arthichaud/meelo-matcher | 6789 (internal) | External metadata matcher (MusicBrainz, Discogs, etc.) |
| meelo-transcoder | zoriya/kyoo_transcoder | 7666 (internal) | Audio transcoder for playback |
| meelo-front | arthichaud/meelo-front | 3000 (internal) | Web frontend (SvelteKit) |
| meelo-nginx | nginx:1.29.4-alpine | 5000→host:5001 | Internal gateway routing /api, /scanner, /matcher, / |

Container dependencies:
```
meelo-db ←── meelo-server ←── meelo-scanner
                  ↑               ↑
meelo-mq ←── meelo-matcher   meelo-front
                                  ↑
meelo-search                 meelo-nginx (gateway)
meelo-transcoder
```

The nginx gateway inside the stack routes:
- `/api/` → meelo-server:4000
- `/scanner/` → meelo-scanner:8133
- `/matcher/` → meelo-matcher:6789
- `/` → meelo-front:3000

The host's localProxy nginx then proxies `meelo.ablz.au` → `localhost:5001`.

## NixOS Configuration

### Module

- Module: `/home/abl030/nixosconfig/modules/nixos/services/meelo.nix`
- Registered in: `/home/abl030/nixosconfig/modules/nixos/services/default.nix`

### Module Options

Options under `homelab.services.meelo`:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| enable | bool | false | Enable Meelo music server |
| dataDir | str | /var/lib/meelo | Directory for persistent state (postgres, config, search, rabbitmq, transcoder cache) |
| mediaDir | str | /mnt/data/Media/Music | Path to music library (mounted read-only) |
| port | port | 5000 | Host port for the web UI |
| tag | str | latest | Meelo image version tag |

### Host Config (proxmox-vm)

In `/home/abl030/nixosconfig/hosts/proxmox-vm/configuration.nix`:

```nix
homelab.services.meelo = {
  enable = true;
  dataDir = "/mnt/virtio/meelo";
  mediaDir = "/mnt/virtio/Music";
  port = 5001;
};
```

Note: `doc2` has `meelo.enable = false` — it was previously considered but Meelo runs only on proxmox-vm.

### What The Module Creates

1. **Podman network**: `podman-network-meelo.service` creates the `meelo` network before any container starts
2. **9 OCI containers**: Each as a systemd service (`podman-meelo-*.service`)
3. **Sops secret**: `meelo/env` decrypted from `secrets/meelo.env` (dotenv format)
4. **Nginx reverse proxy**: `meelo.ablz.au` via `homelab.localProxy.hosts`
5. **Uptime monitoring**: Uptime Kuma monitor for `https://meelo.ablz.au/`
6. **NFS watchdog**: Watches `mediaDir` mount for the server container
7. **tmpfiles rules**: Creates dataDir subdirectories (postgres, config, search, rabbitmq, transcoder_cache)
8. **ExecStartPre hooks on meelo-server**:
   - `waitForMeili`: Waits for MeiliSearch to be healthy, cancels stale enqueued tasks
   - `initConfig`: Templates settings.json from Nix store, substitutes API keys from sops env

## Directory Layout (on proxmox-vm)

| Path | Purpose | Owner |
|------|---------|-------|
| /mnt/virtio/meelo | Meelo persistent state root | root:root |
| /mnt/virtio/meelo/postgres | PostgreSQL data | root:root |
| /mnt/virtio/meelo/config | settings.json (generated at startup) | root:root |
| /mnt/virtio/meelo/search | MeiliSearch data | root:root |
| /mnt/virtio/meelo/rabbitmq | RabbitMQ data | root:root |
| /mnt/virtio/meelo/transcoder_cache | Kyoo transcoder cache | root:root |
| /mnt/virtio/Music | Music library (read-only mount into containers) | — |

## Configuration: settings.json

Meelo's behavior is controlled by `settings.json`, which is **generated at container startup** by the NixOS module. The source of truth is the Nix expression in `meelo.nix`, not the file on disk.

### How settings.json Is Generated

1. Nix builds a `settingsJson` file in the Nix store with placeholder tokens (`__DISCOGS_TOKEN__`, `__GENIUS_TOKEN__`)
2. At `ExecStartPre` of meelo-server, `initConfig` copies this to `/mnt/virtio/meelo/config/settings.json`
3. It sources the sops env file and substitutes the API key placeholders with real values
4. The file is then available to meelo-server and meelo-scanner (both mount `/config`)

### Current Settings

```json
{
  "trackRegex": [
    "/data/[^/]+/(?P<AlbumArtist>[^/]+)/(?:[^/]*\\s-\\s)?(?P<Album>[^/]+)/(?:(?P<Disc>\\d+)-)?(?P<Index>\\d+)\\s*[.\\-]?\\s*(?P<Track>[^/]+?)\\.[^.]+$",
    "/data/[^/]+/Compilations/(?P<Album>[^/]+)/(?:(?P<Disc>\\d+)-)?(?P<Index>\\d+)\\s*[.\\-]?\\s*(?P<Track>[^/]+?)\\.[^.]+$",
    "/data/[^/]+/Non-Album/(?P<AlbumArtist>[^/]+)/(?P<Track>[^/]+?)\\.[^.]+$",
    "/data/Live/(?P<AlbumArtist>[^/]+)/(?P<Album>[^/]+)/(?P<Track>[^/]+?)\\.[^.]+$"
  ],
  "metadata": {
    "source": "embedded",
    "order": "preferred",
    "useExternalProviderGenres": true
  },
  "compilations": {
    "artists": ["Various Artists", "Various", "VA"],
    "useID3CompTag": true
  },
  "providers": {
    "musicbrainz": {},
    "wikipedia": {},
    "allmusic": {},
    "metacritic": {},
    "lrclib": {},
    "discogs": { "apiKey": "<from sops>" },
    "genius": { "apiKey": "<from sops>" }
  }
}
```

### Track Regex Explained

Meelo uses named capture groups to parse file paths into metadata. The regex is tried in order; first match wins.

**Regex 1 — Standard Beets**: `<library>/Artist/YYYY - Album/NN Track.ext`
- Captures: `AlbumArtist`, `Release`, optional `Disc`, `Index`, `Track`
- The `(?:[^/]*\\s-\\s)?` makes the year prefix optional
- `Release` (not `Album`) — embedded tags provide `Album`, path provides `Release`

**Regex 2 — Compilations**: `<library>/Compilations/Album/NN Track.ext`
- No `AlbumArtist` captured (implied Various Artists)
- Captures: `Release`, optional `Disc`, `Index`, `Track`

**Regex 3 — Singletons**: `<library>/Non-Album/Artist/Track.ext`
- No album, no track index
- Captures: `AlbumArtist`, `Track`

**Regex 4 — Live/mix recordings**: `Live/Artist/Album/Track.ext`
- No track index (live recordings often lack numbering)
- Captures: `AlbumArtist`, `Release`, `Track`

### Metadata Resolution

- `source: "embedded"` — primary metadata comes from file tags (ID3, Vorbis, etc.)
- `order: "preferred"` — embedded tags take priority; path-parsed values fill in gaps
- This means the `trackRegex` is mainly a fallback/supplement, not the primary metadata source

### How Release Identity Works (fixed 2026-03-13)

The regex captures the album folder name as `Release` (not `Album`). Embedded tags provide `Album`. This means:

- Embedded `album` tag → Meelo `Album` (groups releases under one album)
- Path folder name → Meelo `Release` (distinguishes editions via `%aunique{}` suffixes)

Example: `2006 - Let Me Introduce My Friends` and `2007 - Let Me Introduce My Friends [2007]` become one album with two releases.

If both the embedded `album` tags differ (e.g., `Interpol` vs `Interpol [EP]`), Meelo correctly creates separate albums — these are different works, not different editions.

See `docs/008-meelo-release-identity.md` for the full analysis.

After changing the regex, existing files need a **forced refresh** to pick up the new parsing (see Rescanning Library below). A one-off script `scripts/meelo_refresh_brackets.sh` was used to refresh all affected albums.

## Secrets

Meelo uses one sops-encrypted env file:

- Sops source: `/home/abl030/nixosconfig/secrets/meelo.env`
- Decrypted at runtime via sops-nix
- Contains: database credentials, MeiliSearch master key, RabbitMQ credentials, Discogs token, Genius token, and other env vars
- All containers receive this env file via `environmentFiles = [envFile]`

### Editing Secrets

```bash
# From the nixosconfig directory
sops secrets/meelo.env
# This opens the decrypted dotenv file in $EDITOR
# Save and close to re-encrypt
```

Or use the `/sops-decrypt` skill.

## How To Edit Meelo's Configuration

### Changing settings.json (track regex, providers, metadata behavior)

1. Edit `/home/abl030/nixosconfig/modules/nixos/services/meelo.nix`
2. Modify the `settingsJson` expression (lines ~57-86)
3. Format and rebuild:
   ```bash
   cd /home/abl030/nixosconfig
   nix fmt
   sudo nixos-rebuild switch --flake .#proxmox-vm
   ```
4. The new settings.json is templated on next meelo-server start
5. Restart meelo-server to pick up changes:
   ```bash
   sudo systemctl restart podman-meelo-server.service
   ```

### Changing container images, ports, volumes, or environment

1. Edit `/home/abl030/nixosconfig/modules/nixos/services/meelo.nix`
2. Modify the relevant container definition under `virtualisation.oci-containers.containers`
3. Format and rebuild as above

### Changing host-level settings (dataDir, mediaDir, port)

1. Edit `/home/abl030/nixosconfig/hosts/proxmox-vm/configuration.nix`
2. Modify the `homelab.services.meelo` block
3. Format and rebuild as above

### Adding new API keys or env vars

1. `sops secrets/meelo.env` — add the new variable
2. If it needs substitution into settings.json, add a placeholder in `settingsJson` and a sed replacement in `initConfig`
3. Otherwise, containers pick up new env vars automatically after rebuild + restart

## Key Concepts: Albums vs Releases

Meelo models music as:

- **Album**: A grouped work (e.g., "Let Me Introduce My Friends")
- **Release**: A specific variant of that album (e.g., "11-track Swedish CD" vs "13-track US CD")
- **Browsing**: Shows one entry per album (the "master release")
- **Album page**: Shows all releases of that album

This is upstream design, documented at https://github.com/Arthi-chaud/Meelo/wiki/Albums-&-Releases

### Fixed: Release Collapsing (2026-03-13)

Previously, two different pressings with the same album title collapsed into a single Meelo release because the regex captured the folder name as `Album` (not `Release`). Fixed by renaming the capture group to `Release` — embedded tags now provide `Album`, path provides `Release`. See `docs/008-meelo-release-identity.md` for full analysis.

### Known Issue: Singles Classified as Studio Albums

Meelo classifies album type in two stages: name-based heuristics (scanner) then MusicBrainz lookup (matcher). Singles are only detected by name if the title ends with "- Single", which beets doesn't add. The MB matcher has mappings for album, EP, compilation, live, remix, demo — but **not single** (intentionally omitted in PR #793).

- **Upstream tracking**: [Issue #1267](https://github.com/Arthi-chaud/Meelo/issues/1267) (milestone v3.11.0) — will read `MusicBrainz Album Type` from embedded tags, which beets already writes.
- **Action**: Wait for v3.11.0. No local fix needed.

## Startup Sequence

1. `podman-network-meelo.service` creates the `meelo` network
2. `podman-meelo-db.service` starts PostgreSQL
3. `podman-meelo-mq.service` starts RabbitMQ
4. `podman-meelo-search.service` starts MeiliSearch
5. `podman-meelo-server.service`:
   - ExecStartPre: waits for MeiliSearch health, cancels stale tasks
   - ExecStartPre: templates settings.json with API keys
   - Starts the API server
6. `podman-meelo-scanner.service` starts (depends on server)
7. `podman-meelo-matcher.service` starts (depends on server + mq)
8. `podman-meelo-transcoder.service` starts (depends on db)
9. `podman-meelo-front.service` starts (depends on server + scanner)
10. `podman-meelo-nginx.service` starts (depends on server + front + matcher + scanner) — exposes port 5001

## Troubleshooting

### MeiliSearch Stale Task Backlog

If meelo-server crash-loops, MeiliSearch can accumulate enqueued tasks. The server has a hardcoded 5s `waitForTask` timeout — if the queue is deep, index creation times out and the server crashes again. The `waitForMeili` script in `ExecStartPre` handles this automatically by cancelling stale enqueued tasks before server start.

To manually clear the MeiliSearch queue:

```bash
# Get the MeiliSearch key
MEILI_KEY=$(sudo podman exec meelo-server printenv MEILI_MASTER_KEY)

# Check pending tasks
sudo podman exec meelo-search wget -qO- \
  --header="Authorization: Bearer $MEILI_KEY" \
  "http://127.0.0.1:7700/tasks?statuses=enqueued,processing&limit=10"

# Cancel all stuck tasks
sudo podman exec meelo-search wget -qO- --post-data="" \
  --header="Authorization: Bearer $MEILI_KEY" \
  "http://127.0.0.1:7700/tasks/cancel?statuses=enqueued,processing"
```

To check the RabbitMQ queue:

```bash
sudo podman exec meelo-mq rabbitmqctl list_queues
```

### Debugging Containers

**Important**: `sudo podman exec` runs commands inside the container as the container's user — it does NOT give root access inside the container. You cannot use `sudo` inside the container. Use `sudo podman logs <container-name>` to read container logs instead of trying to `cat` log files inside the container.

```bash
# View container logs (preferred method)
sudo podman logs meelo-nginx
sudo podman logs meelo-server

# Or via journalctl
sudo journalctl -u podman-meelo-server.service --no-pager -n 50

# Run a command inside a container
sudo podman exec meelo-server printenv SOME_VAR
```

### Container Won't Start

```bash
# Check the specific container's logs
sudo journalctl -u podman-meelo-<name>.service --no-pager -n 50

# Check if the network exists
sudo podman network ls | grep meelo

# Recreate network if missing
sudo systemctl restart podman-network-meelo.service

# Check if data directories exist
ls -la /mnt/virtio/meelo/
```

### DNS / Resolver

Podman uses **aardvark-dns** for container name resolution, NOT Docker's `127.0.0.11` embedded DNS. Do not add `resolver 127.0.0.11` to nginx configs — it will break all upstream resolution. Container names are resolved via podman's network DNS automatically at connection time.

### Album Art Not Showing

Meelo reads album art from **embedded tags only** (`source: "embedded"` in settings.json). It does NOT fall back to `cover.jpg` files in the folder. If art is missing:

1. **Check embedded art exists** in the MP3:
   ```bash
   # Use ffprobe (always available, unlike python mutagen)
   ffprobe -v quiet -show_entries stream=codec_name,width,height -select_streams v:0 /path/to/file.mp3
   ```
2. **Check for corrupt embedded art** — some files have APIC frames with 0x0 pixel data. Meelo registers these as "has art" but can't display them. Fix by re-embedding from a valid cover.jpg.
3. **Check for stale DB entries** — if files were scanned during a server crash, metadata (including art) may be partially registered. Fix by deleting the release from the DB and restarting the scanner to re-register.

#### How release_illustrations works (learned 2026-03-14)

The `release_illustrations` table has `disc` and `track` columns. **Only rows with `disc=NULL AND track=NULL` are treated as the release-level cover art.** Rows with specific disc/track values are per-track illustrations (e.g., different art extracted from different tracks on the same release).

When the scanner extracts art during a forced refresh, it creates `release_illustrations` entries with `disc` and `track` set to the specific track it extracted from (e.g., disc=1, track=2). The API considers these track-level illustrations, NOT release-level covers — `?with=illustration` returns `"illustration": null` even though illustration rows exist in the DB.

A fresh scan (not refresh) creates illustrations with `disc=<N>` and `track=NULL`, which the API does treat as release-level covers.

**To check if a release has a working cover** (not just any illustration):
```bash
sudo podman exec meelo-db psql -U meelo -d meelo -c \
  "SELECT ri.id, ri.disc, ri.track, i.\"blurhash\"
   FROM release_illustrations ri
   JOIN illustrations i ON ri.\"illustrationId\" = i.id
   WHERE ri.\"releaseId\" = <release_id>;"
# If all rows have non-NULL track, the release has NO cover art visible in the UI
```

**To fix**: Delete everything for the affected artist/releases (tracks, files, releases, albums) and let the scanner re-register from scratch. Do NOT just delete illustration records — this orphans the relationship. Do NOT use forced refresh to fix art — it creates track-level illustrations, not release-level ones.

#### Fixing album art: nuclear delete and re-scan (the ONLY safe approach)

**NEVER manually delete rows from `release_illustrations` or `illustrations` to fix art problems.** Deleting illustration records while leaving the release intact creates a state where the release exists but has no art, and a forced refresh will NOT fix it.

**IMPORTANT**: `DELETE FROM releases` does NOT cascade to tracks or files. Tracks get `releaseId` set to NULL (ON DELETE SET NULL), files are untouched. The scanner then sees the files as "already registered" and skips them. You MUST delete in this order: tracks → files → releases → albums.

The only reliable fix:
```bash
# 1. Delete tracks that reference the affected files
sudo podman exec meelo-db psql -U meelo -d meelo -c \
  "DELETE FROM tracks WHERE \"sourceFileId\" IN (SELECT id FROM files WHERE path ILIKE 'Artist Name/%');"

# 2. Delete the file records (scanner skips files it already knows about)
sudo podman exec meelo-db psql -U meelo -d meelo -c \
  "DELETE FROM files WHERE path ILIKE 'Artist Name/%';"

# 3. Delete the releases (cascades to release_illustrations)
sudo podman exec meelo-db psql -U meelo -d meelo -c \
  "DELETE FROM releases WHERE id IN (<release_ids>);"

# 4. Delete orphaned albums
sudo podman exec meelo-db psql -U meelo -d meelo -c \
  "DELETE FROM albums WHERE id IN (<album_ids>)
   AND NOT EXISTS (SELECT 1 FROM releases WHERE \"albumId\" = albums.id);"

# 5. Restart scanner — it will re-register the files fresh
sudo systemctl restart podman-meelo-scanner.service
# Scanner restart also stops meelo-front and meelo-nginx (dependencies)
# After scanner finishes, start them manually:
sudo systemctl start podman-meelo-front.service
sudo systemctl start podman-meelo-nginx.service
```

**Verify the fix actually worked** — check the API, not just the DB:
```bash
JWT=$(curl -s -X POST "http://localhost:5001/api/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"abl030","password":"billand1"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
curl -s "http://localhost:5001/api/releases/<slug>?with=illustration" \
  -H "Authorization: Bearer $JWT" | python3 -m json.tool
# illustration field must NOT be null
```

This ensures the scanner creates proper release-level illustrations from a clean state.

#### Deleting stale entries from the DB

When a scan crash leaves stale/wrong metadata, delete the affected entries and let the scanner re-register them:

```bash
# Find the release
sudo podman exec meelo-db psql -U meelo -d meelo -c \
  "SELECT r.id, r.name, r.slug FROM releases r
   JOIN albums a ON r.\"albumId\" = a.id
   WHERE a.name ILIKE '%album name%';"

# Delete a release (cascades to tracks, files, illustrations)
sudo podman exec meelo-db psql -U meelo -d meelo -c \
  "DELETE FROM releases WHERE id = <release_id>;"

# If the album has no remaining releases, delete it too
sudo podman exec meelo-db psql -U meelo -d meelo -c \
  "DELETE FROM albums WHERE id = <album_id>
   AND NOT EXISTS (SELECT 1 FROM releases WHERE \"albumId\" = <album_id>);"

# Restart scanner to re-register the files
sudo systemctl restart podman-meelo-scanner.service
```

### Server Crash: "Master release of album should be release of said album"

This crash (`InvalidRequestException` in `album.service.js:491`) occurs when a forced refresh changes an album's name (because embedded tags now have different values). The album/release relationship logic breaks because the old album entity expects its master release to still belong to it, but the refresh moved it to a new album.

**Fix**: Don't force-refresh albums whose tags have changed. Instead, delete the old entry from the DB and let the scanner re-register it fresh (see "Deleting stale entries" above).

### Server Crash: Year 0000 / NULL Dates

Some beets library files have no date tag, causing `releaseDate: "0000-01-01"` which PostgreSQL rejects with `date/time field value out of range: "0000-01-01 00:00:00"`. This crashes the server as an unhandled ORM error. The scanner retries these files on every restart, causing a crash-restart loop.

**Symptoms**: Server crash-loops with Postgres date errors in logs. Scanner repeatedly tries to POST the same files.

**Fix**: Either fix the source files (add valid date tags) or delete the problematic files from the beets library. See the "Fixing NULL dates" procedure below.

To identify affected files:
```bash
# Find beets tracks with no date
beet ls -f '$path :: $year' year:0
# Or find tracks where year is empty
beet ls -f '$path' year::^$
```

### Settings Not Updating

settings.json is only templated at meelo-server startup. After changing the Nix config and rebuilding:
```bash
sudo systemctl restart podman-meelo-server.service
# Verify
cat /mnt/virtio/meelo/config/settings.json | jq
```

### Rescanning Library

**Scan** (new files only): The scanner compares file paths on disk against registered paths in the DB. Only files not yet registered get parsed and ingested. Already-registered files are skipped entirely. Trigger from the Meelo web UI or:
```bash
sudo systemctl restart podman-meelo-scanner.service
```

**Refresh** (re-parse existing files): Re-reads embedded tags and re-applies path regex to already-registered files. By default, only re-processes files whose mtime/size changed. Use **force=true** to re-process all files regardless.

When to use force refresh:
- After changing `trackRegex` in settings.json (e.g., the `Album` → `Release` rename)
- After re-tagging files with beets without changing file paths
- After any config change that affects how metadata is parsed

How to force refresh:
1. **Via UI**: Settings > Libraries > Refresh Metadata, tick the "force" checkbox
2. **Via scanner API** (preferred — supports album-level targeting):

```bash
# Get a JWT token
JWT=$(curl -s -X POST "http://localhost:5001/api/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"abl030","password":"billand1"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# Force refresh a SINGLE ALBUM (use album slug from DB or URL)
curl -s -X POST "http://localhost:5001/scanner/refresh?album=<album-slug>&force=true" \
  -H "Authorization: Bearer $JWT"

# Force refresh an entire library (WARNING: ~36k files, takes HOURS)
curl -s -X POST "http://localhost:5001/scanner/refresh?library=beets&force=true" \
  -H "Authorization: Bearer $JWT"

# Other selectors: release=<slug>, song=<slug>, track=<slug>
# Exactly ONE selector required per request.
```

**Important**: A normal scan will NEVER pick up regex changes for existing files. You MUST use a forced refresh.

**Warning**: The scanner has NO cancel/abort endpoint. Once a task is queued, it runs to completion. The only way to stop a running task is `sudo systemctl restart podman-meelo-scanner.service`, which kills all queued tasks. On restart, the scanner auto-queues a scan of each library (fast — just path comparisons, skips registered files). Always prefer album-level refreshes over full-library refreshes.

**Warning**: Restarting the scanner cascades — `meelo-front` and `meelo-nginx` depend on it and will stop too. After a scanner restart, you may need to manually start them:
```bash
sudo systemctl start podman-meelo-front.service
sudo systemctl start podman-meelo-nginx.service
```

### Querying the Database

The PostgreSQL database is accessible from inside the `meelo-db` container:

```bash
# Interactive psql session
sudo podman exec -it meelo-db psql -U meelo -d meelo

# One-off queries
sudo podman exec meelo-db psql -U meelo -d meelo -c "SELECT ..."

# Useful queries:
# Find an album
sudo podman exec meelo-db psql -U meelo -d meelo -c \
  "SELECT id, name, slug FROM albums WHERE name ILIKE '%search%';"

# List releases for an album
sudo podman exec meelo-db psql -U meelo -d meelo -c \
  "SELECT id, name, slug FROM releases WHERE \"albumId\" = <album_id>;"

# List tracks with file paths for a release
sudo podman exec meelo-db psql -U meelo -d meelo -c \
  "SELECT t.name, t.\"trackIndex\", t.\"discIndex\", f.path
   FROM tracks t JOIN files f ON t.\"sourceFileId\" = f.id
   WHERE t.\"releaseId\" = <release_id>
   ORDER BY t.\"discIndex\", t.\"trackIndex\";"
```

## Monitoring

- Uptime Kuma: monitors `https://meelo.ablz.au/`
- NFS watchdog: monitors the music library mount for the server container
- Loki logs: `{host="proxmox-vm", unit="podman-meelo-server.service"}`

## Relationship to Other Services

```
Beets (canonical tagger)
  → writes tagged files to /mnt/virtio/Music/Beets/
  → Meelo scans /mnt/virtio/Music/ (all subdirs: Beets, AI, Live, etc.)

Lidarr + Soularr + slskd (download pipeline on doc2)
  → downloads and validates music
  → beets imports to /Beets
  → Meelo picks up new files on next scan

Plex / Jellyfin (other media servers)
  → also read from /mnt/virtio/Music/ or NFS equivalents
  → independent of Meelo
```

## Documentation Links

| Resource | URL |
|----------|-----|
| Upstream repo | https://github.com/Arthi-chaud/Meelo |
| Wiki | https://github.com/Arthi-chaud/Meelo/wiki |
| Albums & Releases | https://github.com/Arthi-chaud/Meelo/wiki/Albums-&-Releases |
| Refreshing Metadata | https://github.com/Arthi-chaud/Meelo/wiki/Refreshing-Metadata |
| Release identity analysis | docs/008-meelo-release-identity.md (local) |

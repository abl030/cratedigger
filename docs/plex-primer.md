# Plex Primer

## What Plex Is

Plex Media Server is a self-hosted media catalogue used here as **one of
several** music browsers — alongside Meelo. Cratedigger triggers a Plex
library scan after every successful import so newly-imported albums show
up without waiting for Plex's scheduled scan.

Upstream: https://www.plex.tv/

In this homelab Plex is **less integrated than Meelo** — we don't drive
its DB or settings from Nix; the container is configured manually on
Unraid and we only talk to its HTTP API.

## Where Plex Runs

- Host: `tower` (Unraid)
- SSH: `ssh root@tower`
- Container: `binhex-plexpass` (image `binhex/arch-plexpass`)
- Internal IP: `192.168.1.6` (resolves from `plex.ablz.au`)
- External: https://plex.ablz.au (via local nginx proxy on `tower`)
- App data: `/mnt/user/appdata/binhex-plexpass` → `/config` (in container)
- Music mount: `/mnt/remotes/192.168.1.12_Music/Beets` → `/prom_music` (SMB
  remount of doc2's Music share — note this is a **remote mount on tower**,
  so inotify is unreliable and Plex falls back to scheduled/triggered scans)

Library sections (Music = section 3):

| Section ID | Title | Container path | Use |
|------------|-------|----------------|-----|
| 1 | Movies | `/media3/Movies` | — |
| 2 | TV Shows | `/media3/TV Shows` | — |
| 3 | **Music** | `/prom_music` | **Cratedigger writes here via beets** |
| 4 | YouTube | `/media3/YouTube` | — |
| 5 | Rainbow Relaxation | `/media3/Books/…` | Audiobooks (separate music section) |

## How Cratedigger Talks To Plex

Single function: `lib/util.py::trigger_plex_scan(cfg, imported_path)`.

- Called from `lib/import_dispatch.py` after every successful import that
  sets `action.trigger_notifiers = True` (auto, force, manual paths).
- Sends `GET <plex_url>/library/sections/<id>/refresh?path=<...>&X-Plex-Token=<...>`.
- Best-effort — failures don't block the import.

The `path_map` config translates the cratedigger-side filesystem path
(`/mnt/virtio/Music/Beets/...`) into the Plex container's path
(`/prom_music/...`). **Beets stores paths as relative to its
`directory:` root, so `imported_path` is normally relative.** The
substitution handles both relative and absolute inputs and warns on
unmappable absolute paths — see PR #236 and
`docs/solutions/runtime-errors/plex-partial-scan-silent-200.md`.

### Cratedigger config (`/var/lib/cratedigger/config.ini`)

```ini
[Plex]
url = https://plex.ablz.au
token_file = /run/cratedigger-secrets/PLEX_TOKEN
library_section_id = 3
path_map = /mnt/virtio/Music/Beets:/prom_music
```

`token_file` is sops-managed (`secrets/cratedigger.env` on doc1, decrypted
into `/run/cratedigger-secrets/` at runtime via the
`cratedigger-secrets-split` oneshot — see `docs/nixos-module.md`).

## API Access

Auth is a single token passed as `?X-Plex-Token=...` or `X-Plex-Token`
header. The token has full server access — treat it like a password.

Plex's API quirks (read these before debugging):

> **HTTP 200 means "request well-formed"; it does NOT mean the scan
> ran.** Plex returns 200 for valid paths, paths outside any section,
> and complete nonsense.
>
> The only way to verify a scan happened is to **observe library state
> change** (search for the title, query the section's `contentChangedAt`
> timestamp).
>
> See `docs/solutions/runtime-errors/plex-partial-scan-silent-200.md`.

### Useful endpoints

```bash
# Read token from doc2 (where cratedigger has it)
TOKEN=$(ssh doc2 'sudo cat /run/cratedigger-secrets/PLEX_TOKEN')

# List library sections (find IDs and Locations)
curl -s "https://plex.ablz.au/library/sections?X-Plex-Token=$TOKEN" | xmllint --format -

# Section details (Music = section 3)
curl -s "https://plex.ablz.au/library/sections/3?X-Plex-Token=$TOKEN" | xmllint --format -

# Trigger a partial scan of one folder (must be an absolute container path)
curl -s "https://plex.ablz.au/library/sections/3/refresh?path=/prom_music/Artist/Album&X-Plex-Token=$TOKEN"

# Trigger a full library scan (no path arg)
curl -s "https://plex.ablz.au/library/sections/3/refresh?X-Plex-Token=$TOKEN"

# Search for an album by title (type=9 = album)
curl -s "https://plex.ablz.au/library/sections/3/search?type=9&title=<URL-encoded>&X-Plex-Token=$TOKEN" \
  | grep -oE 'title="[^"]*"|key="[^"]*"'

# Inspect album metadata + file paths (use ratingKey from search above)
curl -s "https://plex.ablz.au/library/metadata/<ratingKey>/children?X-Plex-Token=$TOKEN" | xmllint --format -

# Server preferences (scheduled scan interval, FSEvent settings, etc.)
curl -s "https://plex.ablz.au/:/prefs?X-Plex-Token=$TOKEN" | grep -E 'Schedule|FSEvent|watchMusic'
```

### Path encoding

The `path=` argument must be URL-encoded. Slashes are encoded as `%2F`,
spaces as `%20`. Python's `urllib.parse.quote(path, safe='')` is what
`trigger_plex_scan` uses.

## Accessing Tower

```bash
ssh root@tower

# Find the Plex container
docker ps --format "{{.Names}} | {{.Image}}" | grep -i plex

# Inspect its mounts (where is Music coming from?)
docker inspect binhex-plexpass --format \
  '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{println}}{{end}}'

# Tail Plex Media Server logs (inside the container)
docker exec binhex-plexpass \
  tail -f "/config/Library/Application Support/Plex Media Server/Logs/Plex Media Server.log"

# Or from outside via Unraid's mount
ls "/mnt/user/appdata/binhex-plexpass/Library/Application Support/Plex Media Server/Logs/"

# Check what files Plex can see in the music library (from the container's view)
docker exec binhex-plexpass ls "/prom_music/Artist Name/" 2>&1 | head
```

## Plex's Own Scan Behavior

Two background mechanisms can also pick up library changes — these are
why broken triggers can look like they "sometimes work":

1. **`ScheduledLibraryUpdatesEnabled` / `ScheduledLibraryUpdateInterval`** —
   currently set to **daily** (86400s). Plex does a full library scan
   every 24h regardless of triggers.
2. **`FSEventLibraryUpdatesEnabled` / `FSEventLibraryPartialScanEnabled`** —
   on, but the music library lives on a remote SMB mount on tower
   (`/mnt/remotes/192.168.1.12_Music/Beets`), where Linux inotify is
   unreliable. Don't rely on this for music.

Both are tunable via the web UI: **Settings → Library**.

## Debugging "Album not appearing in Plex"

Walk this checklist top to bottom:

1. **Confirm cratedigger triggered it.** Look for the matching log line:
   ```bash
   ssh doc2 'sudo journalctl -u cratedigger-importer --since "1 hour ago" | grep "PLEX:"'
   # Successful trigger looks like:
   # PLEX: triggered partial scan for <Artist>/<Year> - <Album> (HTTP 200)
   # The (HTTP 200) means nothing about whether the scan worked — see step 4.
   ```
   If absent: either `action.trigger_notifiers` was False (rare, but check
   `docs/quality-verification.md`), the import didn't `mark_done`, or
   `cfg.plex_url`/`token` is missing.

2. **Confirm the album is actually on disk** — both from doc2 and from
   tower's perspective:
   ```bash
   ssh doc2 'ls "/mnt/virtio/Music/Beets/<Artist>/<Year> - <Album>/"'
   ssh root@tower 'ls "/mnt/remotes/192.168.1.12_Music/Beets/<Artist>/<Year> - <Album>/"'
   ```
   If only doc2 sees it: SMB mount on tower is stale — restart the
   remote-share mount via Unraid UI or check `mount.cifs`.

3. **Confirm the URL cratedigger sent has the correct container path.**
   Set log level to DEBUG temporarily, or replicate the call by hand —
   the path should start with `/prom_music/...`. If you see a relative
   path being sent, the `path_map` regression has returned (#236).

4. **Reproduce the scan manually with the absolute container path** and
   verify the album appears via search:
   ```bash
   TOKEN=$(ssh doc2 'sudo cat /run/cratedigger-secrets/PLEX_TOKEN')
   ENC=$(python3 -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1],safe=''))" \
     "/prom_music/<Artist>/<Year> - <Album>")
   curl -s -o /dev/null -w "HTTP %{http_code}\n" \
     "https://plex.ablz.au/library/sections/3/refresh?path=$ENC&X-Plex-Token=$TOKEN"
   sleep 8
   curl -s "https://plex.ablz.au/library/sections/3/search?type=9&title=<title>&X-Plex-Token=$TOKEN" \
     | grep -oE 'title="[^"]*"|key="[^"]*"'
   ```
   If the manual scan works but cratedigger's didn't, the bug is in
   `trigger_plex_scan` or `path_map`.

5. **Inspect Plex's stored file paths** for that album to detect stale
   metadata pointing at moved/deleted files:
   ```bash
   # Get ratingKey from the search above, then:
   curl -s "https://plex.ablz.au/library/metadata/<ratingKey>/children?X-Plex-Token=$TOKEN" \
     | grep -oE 'file="[^"]*"'
   ```
   If those file paths don't exist on disk anymore, you have stale
   metadata — Plex will show "this album is unavailable" until the next
   scan reconciles. A force-reimport of the same album with a different
   format/MBID is a common cause: beets moves the files, Plex's old
   metadata is left pointing at the old paths.

6. **Check Plex's own logs** for scanner activity in the relevant window:
   ```bash
   ssh root@tower 'docker exec binhex-plexpass \
     grep -E "Scanner|Section 3" \
     "/config/Library/Application Support/Plex Media Server/Logs/Plex Media Server.log" \
     | tail -50'
   ```

## What's Out of Scope For This Primer

- Movie/TV/YouTube libraries — only Music (section 3) is integrated with
  cratedigger.
- Plex container settings on Unraid (we don't manage them via Nix).
- Plex Pass features (transcoding, sync, mobile) — not used by this
  pipeline.
- Authentication beyond a single owner token. We don't expose Plex to
  Plex.tv accounts.

## Documentation Links

| Resource | URL |
|----------|-----|
| Plex API (unofficial) | https://www.plexopedia.com/plex-media-server/api/ |
| Library refresh endpoint | https://www.plexopedia.com/plex-media-server/api/library/refresh/ |
| Path-map silent-failure lesson | docs/solutions/runtime-errors/plex-partial-scan-silent-200.md |
| Meelo primer (sister doc) | docs/meelo-primer.md |

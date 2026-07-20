# Plex Primer

## What Plex Is

Plex Media Server is a self-hosted media catalogue used here alongside
Jellyfin. Cratedigger triggers a Plex
library scan after every successful import so newly-imported albums show
up without waiting for Plex's scheduled scan.

Upstream: https://www.plex.tv/

In this homelab we don't drive Plex's DB or settings from Nix; the container is configured manually on
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

- Called from `lib/dispatch/` after every successful import that
  sets `action.trigger_notifiers = True` (automatic and force paths).
- Sends `GET <plex_url>/library/sections/<id>/refresh?path=<...>&X-Plex-Token=<...>`.
- Best-effort — failures don't block the import.

### "Recently Added" pin on upgrades (migration 040)

An upgrade re-import replaces an album's on-disk files (and the extension
usually changes, e.g. FLAC/MP3 → Opus), so the partial refresh above makes
Plex re-stamp the album's `addedAt` to now and it wrongly jumps to the top of
"Recently Added". To preserve the original date, cratedigger now also *reads
and edits* Plex metadata (the only place it does more than the refresh GET):

- **Capture** (`lib/plex_pin_service.py::capture_plex_added_at_pin`, called in
  `import_dispatch.py` *before* the refresh): locate the album by its container
  folder path (`Media.Part.file` prefix match — robust to the extension change)
  and stash its current `addedAt` as a `pending` row in `plex_added_at_pins`.
  A genuinely-new album isn't in Plex yet, so nothing is captured — the table
  self-selects upgrades.
- **Reconcile** (`reconcile_plex_added_at_pins`, called each 5-min cratedigger
  cycle): for each pending pin past a 180s settle window, re-find the album and,
  if Plex bumped its `addedAt`, `PUT …/library/sections/<id>/all?type=9&id=<rk>&addedAt.value=<orig>&addedAt.locked=1`. The `addedAt.locked=1` is
  load-bearing — without it the next metadata refresh re-stamps the date.

The Plex read/edit client lives in `lib/util.py` (`plex_find_album_by_path`,
`plex_set_added_at`) with verify-then-unverified SSL fallback. Both are
best-effort; failures never block an import or the cycle.

The Plex and Jellyfin orchestration modules deliberately remain separate.
Their shared capture/reconcile outline is smaller than their backend
contracts: epoch integer versus ISO string, Plex field lock versus Jellyfin
landed detector/TTL, and one album write versus album plus Audio children. A
strategy-driven shared core would move those differences rather than simplify
them. A third media backend must first be compared with both lifecycles;
extract only when a common engine materially reduces behavior, otherwise keep
a backend-owned module.

Terminal pin rows (`done` and `skipped`) are convergence bookkeeping, not
audit history. Phase 0 prunes them after 90 days using a strict age boundary;
`pending` rows survive regardless of age.

### How paths get to Plex

Beets stores file paths as **relative** to its `directory:` root, so
`imported_path` is normally something like `Artist/Album` (no leading
slash). `trigger_plex_scan` runs two transforms in sequence:

1. **Absolutize** if relative and `beets_directory` is set
   (`Artist/Album` → `/mnt/virtio/Music/Beets/Artist/Album`).
2. **Translate host → container** if `path_map` is set
   (`/mnt/virtio/Music/Beets/Artist/Album` → `/prom_music/Artist/Album`).

If both are set (this homelab), the two compose. If only `path_map` is
set, the path_map substitution itself anchors relative paths under the
container prefix as a fallback. If only `beets_directory` is set
(bare-metal Plex on the same host as beets), the absolutize step is
sufficient by itself. See PR #236 and
`docs/solutions/runtime-errors/plex-partial-scan-silent-200.md` for the
five-week silent-failure story behind this.

### Cratedigger config (`/var/lib/cratedigger/config.ini`)

```ini
[Beets]
directory = /mnt/virtio/Music/Beets

[Plex]
url = https://plex.ablz.au
token_file = /run/cratedigger-secrets/PLEX_TOKEN
library_section_id = 3
path_map = /mnt/virtio/Music/Beets:/prom_music
```

`token_file` is sops-managed (`secrets/cratedigger.env` on doc1, decrypted
into `/run/cratedigger-secrets/` at runtime via the
`cratedigger-secrets-split` oneshot — see `docs/nixos-module.md`).

### Plex on a different setup (other deployments)

The path-shape concerns are configurable; nothing is hardcoded:

| Deployment | Set in cratedigger config | Result |
|------------|--------------------------|--------|
| Plex in Docker, music mounted at a different container path | `[Beets] directory` + `[Plex] path_map = /host/beets:/container/path` | Absolutize then translate |
| Plex on the same host as beets (bare metal, no remap) | `[Beets] directory` only | Absolutize, send absolute |
| Plex with the same path on host and container (already absolute somehow) | `[Plex] path_map = /shared/root:/shared/root` | Idempotent translation |
| Nothing configured | (warning logged) | Plex receives a relative path and silently no-ops |

Via the Nix module, the equivalent options are
`services.cratedigger.beets.config.directory` and
`services.cratedigger.notifiers.plex.pathMap`.

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

### Library deletion refresh

After a verified library delete, Cratedigger walks upward only within the
configured Beets root and asks Plex to refresh the nearest existing ancestor
(normally the artist folder), never the now-missing album path. Existing path
mapping still translates host to Plex paths. Results say `submitted` and expose
the exact target; even HTTP 200 remains submission evidence, not proof a scan
ran. Failures are visible warnings and do not roll back the completed delete.

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

## Path-rename footgun — Plex splits the library on mass renames

**Any beets config change that mutates rendered paths followed by a
`beet move` will split affected albums into two Plex entries.** This
happened May 2026 when `asciify_paths = true` was enabled — ~12% of the
music library (1,178 albums) ended up split across a primary album row
(holding tracks whose filenames Unicode didn't touch) and a ghost row
(holding the renamed tracks). Plex's scanner does not reconcile
many-file renames during a transition: the old paths' track rows become
dead, the new paths get fresh track rows, and the fresh rows spawn a
second album row instead of folding into the existing one.

`Empty Trash + Clean Bundles` does NOT fix this — it deletes the dead
track entries but leaves the ghost album rows intact, and on remote SMB
mounts (where inotify is unreliable) it surfaces additional splits the
prior partial scans missed.

The fix is Plex's metadata merge endpoint:

```
PUT /library/metadata/{primary_rk}/merge?ids={ghost_rk1},...
```

Two scripts in `scripts/` automate the audit + cleanup:

- `scripts/plex_dupes_audit.py` — finds duplicate `(artist, title, year)`
  groups, classifies each as `same_folder` (asciify split) or
  `diff_folder` (legit multi-edition pressing OR mid-reconciliation
  ghost).
- `scripts/plex_dupes_merge.py` — merges same-folder ghosts into their
  primary, preserving play counts. Dry-run by default; `--commit` to
  execute; `--limit N` for smoke testing.

See `docs/solutions/runtime-errors/plex-asciify-paths-album-split.md`
for the full incident write-up, the operator runbook, and why
`beet write` / `mbsync` can't be used to align tags with paths after
the fact.

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

# Jellyfin Primer

## What Jellyfin Is

Jellyfin is a self-hosted media server used here alongside Plex. Cratedigger triggers a Jellyfin library
refresh after every successful import, and (since issue #574) pins album
"added" dates across upgrades so re-acquisitions don't pollute "Recently
Added".

Upstream: https://jellyfin.org/

## Where Jellyfin Runs

- External: https://jelly.ablz.au
- Version at integration time: 10.11.11
- Music library: `/mnt/fuse/Media/Music/Beets` — the same files Cratedigger
  sees at `/mnt/virtio/Music/Beets`. That exact prefix swap is
  `[Jellyfin] path_map`; `Incoming` and `failed_imports` are outside Jellyfin's
  scope.
- The music library sits on a fuse mount, so inotify (Jellyfin's realtime
  monitor) is unreliable — like Plex on its SMB remount. Changes land via
  the triggered refresh or the scheduled scan.

## How Cratedigger Talks To Jellyfin

Scan notifier: `lib/util.py::trigger_jellyfin_scan(cfg, imported_path)` — called from
`lib/dispatch/` after every successful import that sets
`action.trigger_notifiers = True`. It maps the final beets album directory to
Jellyfin's view of the same path and sends exactly one filesystem-change
notification:

```http
POST /Library/Media/Updated
Content-Type: application/json

{"Updates":[{"Path":"/mnt/fuse/Media/Music/Beets/Artist/Album","UpdateType":"Modified"}]}
```

Jellyfin 10.11.11 ignores `UpdateType`, resolves the exact existing album, or
walks upward to the nearest indexed ancestor for a genuinely new album. That
ancestor can be the artist or, for a first album by a new artist, the music
library root. Reporting the album directory is enough for an extension
change: folder validation removes the vanished MP3 item and creates/probes the
new Opus item. A new album's first refresh reads embedded tags and media info
and runs its metadata/image providers. The endpoint is asynchronous; HTTP 204
means queued, never converged.

There is deliberately no collection refresh and no broad fallback. If the
album path cannot be mapped, the notifier logs and stops. Jellyfin exposes no
single request that both reconciles changed children and forbids all metadata
or image consideration on the affected existing album/ancestor;
`Media/Updated` is its narrowest supported filesystem-change boundary. The
acceptance VM therefore proves the outcomes Cratedigger can require: new-album
metadata and cover art land, an existing curated album remains unchanged, and
a separate library is not scanned.

For library deletion, Cratedigger locates the exact `MusicAlbum` by the former
mapped Beets path and refreshes that item after destructive locks are released.
This is a separate deletion-observation contract and retains its bare targeted
refresh request (without the post-import metadata-mode query).
If no exact item is found it uses the configured library item. A 404 from any
targeted `/Items/{id}/Refresh` immediately falls back to
`POST /Library/Refresh`. A 2xx response is only submission evidence:
Cratedigger checks the former path again, reports observed absence when it can,
and emits a warning when the item was never observable or remains present.
Failures are surfaced as warnings on the already completed delete rather than
rolling library state back.

### "Recently Added" pin on upgrades (migration 046, issue #574)

An upgrade re-import replaces an album's on-disk files. The Jellyfin rescan
deletes the album's old Audio items and creates new ones, stamping each new
item's `DateCreated` from file ctime (= import time) — and sometimes
recreates the MusicAlbum item too (observed live: 1 of 3 upgrades).
Jellyfin's "Recently Added"/Latest row orders albums by their **children's**
`DateCreated`, so the upgraded album wrongly jumps to the top even when the
album item kept its original date.

Cratedigger preserves the original date with a capture-then-reconcile loop —
the Jellyfin sibling of the Plex `addedAt` pin (migration 040), with two
deliberate differences:

- **Capture** (`lib/jellyfin_pin_service.py::capture_jellyfin_date_created_pin`,
  called in `lib/dispatch/core.py` *before* the refresh): locate the album by
  its folder path (exact `Path` match after the `path_map` translation) and
  stash the **maximum `DateCreated` across its Audio children** (the value
  Jellyfin actually uses to order grouped music Latest), plus a snapshot of
  the album and Audio item ids, as a `pending` row.
  A genuinely-new album isn't in Jellyfin yet, so nothing is captured — the
  table self-selects upgrades.
- **Reconcile** (`reconcile_jellyfin_date_created_pins`, each 5-min
  cratedigger cycle): a pin is acted on only once the rescan is **observable**
  — an item id differs from the snapshot **or an Audio item's date becomes
  newer than the captured maximum**. Only newer album/Audio dates are clamped
  back to that maximum, preserving the album's prior Recently Added position.
  Until it lands the pin stays `pending` (up to a 48h TTL → `expired`).

Why the landed-detector instead of Plex's fixed 180s settle window + field
lock:

1. **No lock exists.** Jellyfin has no `DateCreated.locked`, so restoration
   happens after the asynchronous album update becomes observable.
2. **Ids are not enough.** Jellyfin's metadata service rewrites an existing
   same-path Audio item's `DateCreated` from file ctime when its mtime changes.
   The newer-date detector catches that case even though the item id survives.
3. **Only forward bumps matter.** Existing tracks can naturally have dates
   older than the album maximum. They are left alone; only a date newer than
   the captured maximum can move the grouped album forward in Latest.

The Plex and Jellyfin orchestration modules deliberately remain separate.
Their shared outline is smaller than their backend contracts: epoch integer
versus ISO string, Plex field lock versus Jellyfin landed detector/TTL, and one
album write versus album plus Audio children. A strategy-driven shared core
would move those differences rather than simplify them. A third media backend
must first be compared with both lifecycles; extract only when a common engine
materially reduces behavior, otherwise keep a backend-owned module.

Terminal pin rows (`done`, `skipped`, and Jellyfin's `expired`) are convergence
bookkeeping, not audit history. Phase 0 prunes them after 90 days using a strict
age boundary; `pending` rows survive regardless of age.

### Editing items: the full-dto rule

Jellyfin's item update endpoint (`POST /Items/{id}`) **replaces** the item's
metadata — any field omitted from the body is wiped. The setter
(`lib/util.py::jellyfin_set_date_created`) therefore always fetches the full
dto (`GET /Items/{id}?userId=<any user>` — the single-item GET requires a
userId; the first user from `/Users` is used) and posts it back with only
`DateCreated` changed. Never post a partial body. Verified live on 10.11
(2026-07-10): genres, provider ids, premiere date all survive the round-trip;
the POST returns 204.

### Cratedigger config (`/var/lib/cratedigger/config.ini`)

```ini
[Jellyfin]
url = https://jelly.ablz.au
token_file = /run/cratedigger-secrets/JELLYFIN_TOKEN
library_id = <music-library-item-id>
path_map = /mnt/virtio/Music/Beets:/mnt/fuse/Media/Music/Beets
```

`path_map` composes with `[Beets] directory` exactly like the Plex one
(absolutize relative `imported_path`, then prefix-swap — see
`docs/plex-primer.md` § "How paths get to Plex"). It drives both the path
notification and the pin's exact album lookup. `library_id` is independent
and is used only as the fallback target for the separate library-deletion
observer. Via the Nix module these are
`services.cratedigger.notifiers.jellyfin.libraryId` and `.pathMap`.

## API Access

Auth is an admin API key passed as the `X-Emby-Token` header. Endpoints the
integration uses (all verified on 10.11):

```bash
TOKEN=$(ssh doc2 'sudo cat /run/cratedigger-secrets/JELLYFIN_TOKEN')

# Find an album by title (path is the authoritative join — check it)
curl -s -H "X-Emby-Token: $TOKEN" \
  "https://jelly.ablz.au/Items?recursive=true&includeItemTypes=MusicAlbum&searchTerm=<title>&fields=Path,DateCreated&limit=5"

# Audio children of an album (the rows that drive Recently Added)
curl -s -H "X-Emby-Token: $TOKEN" \
  "https://jelly.ablz.au/Items?parentId=<albumId>&includeItemTypes=Audio&fields=DateCreated"

# Full item dto (userId required on the single-item GET)
curl -s -H "X-Emby-Token: $TOKEN" "https://jelly.ablz.au/Items/<id>?userId=<uid>"

# Update an item — FULL dto only (see the full-dto rule above); returns 204
curl -s -X POST -H "X-Emby-Token: $TOKEN" -H "Content-Type: application/json" \
  --data @dto.json "https://jelly.ablz.au/Items/<id>"

# Libraries + their paths
curl -s -H "X-Emby-Token: $TOKEN" "https://jelly.ablz.au/Library/VirtualFolders"
```

Gotchas:

- **There is no path-filter on `/Items`** — an unrecognized `path` param is
  ignored and the query degenerates to an unfiltered recursive sweep (slow
  enough to 504 through the proxy). The finder narrows by album-title /
  artist search and verifies by exact `Path` equality instead.
- `DateCreated` for new items comes from file **ctime**, not scan time — so
  re-stamped dates equal the import time even if the scan runs hours later.
- **The finder matches the album folder path exactly.** If a deployment's
  beets path format rendered per-disc subfolders (this one doesn't — paths
  are flat `$albumartist/$year - $album/$track`), Jellyfin's album `Path`
  wouldn't equal the beets album folder and no pin would be captured. That
  degrades safely — the album is simply unprotected against Recently-Added
  pollution; a false match is impossible (paths are unique) and a stale pin
  marks itself `skipped`.

## Debugging "upgrade shows in Recently Added"

1. Was a pin captured? Look for `JELLYFIN PIN: captured DateCreated=…` in
   `cratedigger-importer` logs around the import; the row lands in
   `jellyfin_date_created_pins`.
2. Did it reconcile? The 5-min cycle logs
   `JELLYFIN PIN reconcile: pinned=… waiting=… expired=…`. A pin stuck on
   `waiting` means the rescan hasn't visibly landed yet (check that the
   refresh trigger fired and the scheduled scan schedule).
3. Check the album's dates directly (album + children endpoints above) —
   after a successful pin, no album or Audio child date is newer than the
   captured pre-upgrade maximum. Naturally older child dates stay untouched.

## Documentation Links

| Resource | URL |
|----------|-----|
| Jellyfin API docs | https://api.jellyfin.org/ |
| Plex primer (sister doc — the migration-040 pin) | docs/plex-primer.md |

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

**This lane's refresh does not converge, by the same source-level finding the
post-import reconciler below exists to route around.** A targeted
`/Items/{id}/Refresh` cannot reap the item it targets — Jellyfin computes
deletion one level up, from the PARENT folder's own disk-vs-DB child-set
diff — so after an exact-album library delete the album's `MusicAlbum` item
is expected to remain a childless orphan (its `Audio` children are the ones
actually removed, and only as an incidental side effect of the very refresh
this lane submits) until a scan reaches the parent by some other route (a
later sibling import's own notifier, or an operator-triggered library scan).
The `"exact album item … remains observable after refresh submission"`
`warning` this lane emits is therefore the EXPECTED steady state for this
lane too, not a transient condition awaiting convergence — this code path is
unchanged by issue #1203 item 2; only its documentation is corrected here.

### Post-import vanished-path reconciliation (issue #1203 item 2)

A path-changing re-import (see `docs/beets-primer.md` § the `path_disambig`
recurrence hazard) renames an album's folder without deleting anything. The
`Media/Updated` call above only ever names the NEW path, so Jellyfin never
learns the OLD path is gone — its `MusicAlbum` item there survives a
completed full scan and shows no artwork, because item identity is a hash of
the path and the rename minted a brand-new item at the new path instead of
migrating the old one (live incident: request 8964's David Bowie import,
`catalognum` populated for the first time, `1969 - David Bowie [1969]` →
`[SBL 7912]`).

After both "Recently Added" pin captures and both new-path notifiers run
(`lib/dispatch/core.py::_trigger_post_import_notifiers`), Cratedigger snapshots
every album directory Beets currently holds for the request's release id
BOTH before Beets launch and again here, and diffs the two
(`lib.beets_db.BeetsDB.get_current_album_directories`,
`lib.dispatch.core._vanished_album_directories`) — this before/after diff is
the PRIMARY, authoritative source of vanished pre-upgrade paths.
`postflight.replaced_albums` (the harness's mid-import serialization) is only
a SECONDARY source unioned in: it can already show the album's NEW path by
the time it's captured — verified live for the Bowie row above
(`download_log` 40213, request 8964: `replaced_albums` records
`album_path = .../David Bowie/1969 - David Bowie [SBL 7912]` for removed
beets album 19881, which is exactly the path beets album 19882 now
occupies) and fingerprinted three more times by
`jellyfin_date_created_pins`, whose `album_item_id IS NULL` floor-pin rows
(3 of 262 live) are the signature of a path-changing upgrade whose old path
could not be found — so it cannot be trusted alone. For each distinct vanished
path from either source, Cratedigger calls
`lib.library_delete_notifiers.notify_library_delete` with
`allow_escalation=False`.

**On Jellyfin this call never refreshes anything — it only finds the item by
its former path and reports what it found.** That is not merely "the
collection-wide fallback is refused"; it is a different action from the
destructive-delete caller's find → refresh → re-observe flow entirely. The
reason is a source-level finding against Jellyfin 10.11
(`MediaBrowser.Controller/Entities/Folder.cs::ValidateChildrenInternal2`):
deletion of a vanished item is computed as the PARENT folder's own disk-vs-DB
child-set diff, so an item is never deleted by refreshing ITSELF —
`POST /Items/{id}/Refresh` validates that item's own children, and is
structurally incapable of reaping the item. Worse, the album's directory is
already gone by the time this call would run, so enumerating it during that
refresh raises `DirectoryNotFoundException`; the same method's `IOException`
handler logs and swallows it instead of returning, so the refresh proceeds
with an EMPTY observed disk and deletes every one of the album's child
`Audio` rows (files on disk are untouched — Jellyfin's `DeleteFileLocation`
stays false — but their Jellyfin metadata/user-data is gone). Live-verified
against a real Jellyfin 10.11.11 instance: a probe `/Items/{id}/Refresh`
against one of the live orphans this issue describes deleted 17 `Audio` rows
outright, while the album item itself persisted throughout — consistent with
deletion being computed one level up, at the parent.

So the reconciler's Jellyfin outcome is one of two shapes, and both are
final — neither is a "try again later":

- The item is found by its former path: a `warning` `DeleteNotification`
  naming the exact item id and former path, stating it was found but NOT
  refreshed and that reaping it is an operator action.
- No item is found by its former path: a `skipped` `DeleteNotification`.

Both shapes are logged, one line per outcome (`lib/dispatch/core.py::_reconcile_vanished_replaced_album_paths`,
one line per provider — always Plex and Jellyfin both, even when only one
is configured). To find these in the journal:

```bash
journalctl -u cratedigger | grep 'MEDIA SERVER RECONCILE:'
```

Whether Cratedigger should ever be authorized to delete a Jellyfin item
directly, or to refresh the parent artist (risking every sibling album on a
broken/slow mount, since Jellyfin gives that scope no fail-safe), are both
operator decisions tracked separately — this reconciler does neither, on
purpose. **Contrast with Plex**, whose equivalent leg genuinely self-heals —
see `docs/plex-primer.md`.

This snapshot-diff-and-reconcile call MUST run after the Jellyfin pin capture
below, never before: `capture_jellyfin_date_created_pin` reads
`postflight.replaced_albums`'s old paths synchronously to find the
pre-upgrade item and snapshot its date. This is kept as a standing ordering
guarantee for every media-server action this reconciler could ever take, not
a claim that today's find-only Jellyfin action is itself destructive.

### "Recently Added" pin on upgrades (migrations 046 + 053, issue #574)

An upgrade re-import replaces an album's on-disk files. The Jellyfin rescan
deletes the album's old Audio items and creates new ones, stamping each new
item's `DateCreated` from file ctime (= import time) — and sometimes
recreates the MusicAlbum item too (observed live: 1 of 3 upgrades).
Jellyfin's music "Recently Added"/Latest row orders albums by the **MusicAlbum
item's own** `DateCreated` (children only qualify the album for inclusion —
verified in Jellyfin source, `BaseItemRepository.GetLatestItemList`), and
**item identity is an MD5 of the item path**
(`LibraryManager.GetNewItemIdInternal`), so a re-created album — same path or
new path — jumps to the top.

The path-identity fact makes **path-changing upgrades** the hard case (live
incident 2026-07-16: Arcade Fire "B-Sides & Rarities", whose folder moved
`2007 - …` → `0000 - …` when MusicBrainz dropped the release date): the
pre-upgrade items exist only at the *old* path, and the *new* path holds
nothing until the rescan lands. Jellyfin has no provider-id reconciliation
and never migrates an item across a path change — delete-old + create-new in
one scan pass is structural, so the pin system must absorb it.

Cratedigger preserves the original date with a capture-then-reconcile loop —
the Jellyfin sibling of the Plex `addedAt` pin (migration 040), with two
deliberate differences:

- **Capture** (`lib/jellyfin_pin_service.py::capture_jellyfin_date_created_pin`,
  called in `lib/dispatch/core.py` *before* the refresh): locate the album by
  its folder path (exact `Path` match after the `path_map` translation) and
  stash the **maximum `DateCreated` across its Audio children**, plus a
  snapshot of the album and Audio item ids, as a `pending` row. Lookup order
  is the new imported path, then each **replaced beets album's old path**
  (`postflight.replaced_albums`, threaded from the harness dup-guard's
  allowed removals) — that's where the pre-upgrade items live after a
  path-changing upgrade. When Plex already knows the album, its preserved
  pre-upgrade `addedAt` is an older historical floor: the Jellyfin pin uses
  the earlier value. This prevents a prior Jellyfin rebuild or broken refresh
  from becoming the new baseline forever.
  When nothing is findable anywhere but replaced albums prove the import was
  an upgrade, a **floor pin** (migration 053: `album_item_id` NULL, empty
  children snapshot) is written from the pipeline's own floor — the earlier
  of Plex's preserved `addedAt` and the oldest `created_at` across the
  request's replace chain — so an upgrade can never look newer than when the
  pipeline first knew of its files. A genuinely-new album has no replaced
  albums and isn't in Jellyfin yet, so nothing is captured — the table
  self-selects upgrades.
- **Reconcile** (`reconcile_jellyfin_date_created_pins`, each 5-min
  cratedigger cycle): a pin is acted on only once the rescan is **observable**
  — an item id differs from the snapshot (a NULL snapshot matches any album:
  the floor-pin case) **or an Audio item's date becomes newer than the
  captured maximum**. An **absent album waits** rather than closing: after a
  path-changing upgrade the pinned (new) path only exists in Jellyfin once
  the rescan lands; still-absent at TTL closes as `skipped`. A landed album
  with zero Audio children is the mid-scan window and also waits. Only newer
  album/Audio dates are clamped back to the captured value, preserving the
  album's prior Recently Added position. Until a landing signal appears the
  pin stays `pending` (up to a 48h TTL → `expired`).

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

### Cratedigger runtime config

The NixOS module supplies this as an immutable store file; conventional
deployments may choose another immutable path.

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

# Beets Primer for Cratedigger

This document defines the Beets ownership and runtime contract Cratedigger
consumes. Read it before changing imports, validation, the harness, library
operations, or deployment wiring.

## What is Beets?

Beets is the canonical source of truth for the currently installed tagged
music library. It handles:

- **Matching** — identifying which MusicBrainz (or Discogs) release an album is
- **Tagging** — writing corrected metadata (artist, album, track names, year, genre, etc.) into file tags
- **File structure** — renaming and organizing files into `Artist/Year - Album/Track Title.mp3`
- **Cover art** — fetching from Cover Art Archive, embedding into files
- **Lyrics** — fetching synced lyrics from a local LRCLIB mirror
- **Library DB** — SQLite database tracking every album and track

When Cratedigger acquires and validates an album, Beets performs the actual
catalog, tagging, and filesystem import.

## Ownership boundary

The deployment and its librarian own Beets: the package and plugin closure,
effective configuration, canonical SQLite catalog, library files, current
release identities, paths, tags, persistent state, secret delivery, and
operator maintenance. Cratedigger owns exact requested-release identity, the
acquisition lifecycle and history, and durable capture proof. An out-of-band
retag, move, byte change, or deletion changes the current Beets world; it does
not rewrite which pressing Cratedigger sought or erase a witnessed capture.
Cratedigger never projects holdings into its PostgreSQL database, correlates a
sibling pressing, or treats a nearby release as the requested release.

## Compatibility cohorts

The locked Nixpkgs package is this repository's standalone package/dev-shell
reference only. Deployment continues to supply the production package, exact
interpreter, immutable `BEETSDIR`, external state, library, catalogue, secrets,
and plain operator `beet`; Cratedigger validates that capability but never
selects it. `beets-tip` is a checks-only advance-warning input, while the
reviewed 730-day manifest runs disposable harness/delete contracts for each
final release. These checks do not establish live-catalog upgrade or downgrade
safety. LRCLIB and unrelated external plugin behaviour are deployment-owned
configuration, outside the compatibility promise.

Cratedigger has exactly three Beets mutation lanes:

1. The serial importer worker drives the JSON harness for admitted imports and
   same-release duplicate replacement.
2. An explicitly operator-authorized library deletion resolves one exact Beets
   album primary key and drives the exact-album delete child.
3. The import-time MusicBrainz merge retag runs `beet modify -a -M -W -y`
   under an anchored `mb_albumid::^<old-id>\Z` query and a `mb_albumid=<new-id>`
   assignment (`lib/beets_retag.py`), from inside whichever importer lane
   holds the request's exact import claim — the automation processing owner,
   or a claimed force-import job (#1080). Issue #1087 replaced the original
   `beet mbsync -M` primitive: `mbsync` maps library items onto a fetched
   release's tracks by recording id, which a release-only merge (MusicBrainz
   merges the release but not the underlying recordings) does not preserve —
   so it silently retagged nothing on the common case. `beet modify` sets one
   field by query; it needs no candidate mapping and makes no network call.

Lane 3 is deliberately the narrowest of the three, and every clause is
load-bearing:

- **`-a` targets Albums, not Items.** `Album.try_sync(inherit=True)` (the
  default) fans every inheritable fixed attribute — `mb_albumid` is one — out
  to every item and stores it. Drop `-a` and the query matches ITEMS instead:
  each item's own `mb_albumid` moves while the ALBUM row's does not, leaving
  the library split into disagreeing identity fields.
- **Identity only, never a tag write, never layout.** `-M` (`--nomove`) and
  `-W` (`--nowrite`) are mandatory; `modify` otherwise honours `import.move` /
  `import.write`, which the config contract pins to `yes`. `-W` is the one
  that matters today: without it `modify` calls `item.try_write()` on every
  matched file, rewriting tags to disk while the DB already reports success
  — a divergence the DB-only post-retag guard cannot see. `-M` is
  belt-and-braces rather than something reachable right now: `mb_albumid` is
  in no path template (`$albumartist`, `$year`, `$album`, and the
  `%aunique` disambiguator all derive from other fields), so retagging it
  alone cannot itself relocate a file under the current path configuration.
  It stays because `modify -a` with `-M` dropped would relocate the album on
  any FUTURE config that makes `mb_albumid` path-relevant — minting new
  Jellyfin item identities (identity is a hash of the path), risking the
  documented Plex album-split footgun, and pruning the vacated directory's
  `clutter`, which includes the `cratedigger.json` verified-lossless
  sidecar — and there is no cost to keeping the flag now. The accepted
  residual of `-W`: if the import later rejects, installed files stay
  tagged with the merged-away id while the beets DB already holds the
  survivor. That divergence is dormant — `beet update` skips any item whose
  on-disk mtime has not advanced past what the DB recorded
  (`beets/ui/commands/update.py`), so an untouched file set is never
  re-read as stale — and it self-corrects the next time a successful import
  replaces the album.
- **One album.** The regex is anchored, so it can only name albums filed under
  exactly the merged-away release id.
- **Only on `mbid_not_found`, only under an exact import claim**, and only
  when MusicBrainz observably redirects the stored id to a survivor Beets just
  offered as a candidate. Two claims qualify, and they are the two the
  importer already takes: the automation processing owner
  (`status='processing'` + `active_automation_import_job_id`) and a `running`
  `force_import` job on an unowned, non-`replaced` row. Force import is the
  same path as any other import with the Beets distance overridden, so it
  follows a merge through the same seam (#1080).
- **Both `RELEASE` advisory locks are held** across the retag and the request
  rekey, because the retag mutates two release identities at once and the
  destructive operator lanes fence per release from other processes.
- **The library moves before the row.** Beets keys album duplicate detection on
  `mb_albumid`, so rekeying first would land a second album.
- **The library is not touched until the rekey is known to be possible.** The
  rekey write has exactly two `UniqueViolation` refusals — another request
  already holds the survivor (`UNIQUE(mb_release_id)`, including a frozen
  `replaced` ancestor) and an evidence row already exists at
  `(survivor, snapshot_fingerprint)` — and both are plain reads
  (`PipelineDB.merge_rekey_collision`), taken under the release locks
  immediately before the retag. Retagging first and discovering the refusal
  afterwards leaves the installed album at the survivor and the request at the
  merged-away id; nothing re-derives that, because the collision that refused
  the write is still there on the next attempt, which this same pre-check now
  refuses before the library is read at all. Note that a curated collection
  deliberately holds multiple pressings per album, so a rival at the survivor
  is a normal state, not an anomaly (invariant 5).
- **A blocked rekey is audited too.** It is the one non-ready outcome no retry
  can clear — every other one (mirror silent, lock held, retag not ready)
  describes a world the next cycle re-derives — and the force lane carries no
  rejection of its own to explain it, because force imports despite the
  verdict and then meets the merged-away id inside `harness/import_one.py`
  as a bare `mbid_missing`. So the seam records a durable `download_log` row
  (`outcome='failed'`) naming the collision, once per execution that reaches
  the branch: one per operator force action, one per completed-download
  validation, deliberately not deduplicated. This is also how a split
  identity left behind by an earlier execution surfaces — the pre-check
  refuses before the library is read, so the seam never learns the library
  already moved, and the blocked audit is the operator's evidence.
- **The residual race is audited, and stops the force launch.** No lock covers
  "another request acquires this release id" (nor "another lane writes
  evidence at the survivor"), so the pre-check narrows that window without
  closing it. If it loses, the seam records a durable `download_log` row
  naming the split (invariant 11's Recents audit evidence, not a log line),
  and the force lane refuses to launch Beets at the id whose library album
  *this execution* just moved away — continuing would report the pre-#1080
  `mbid_missing` while the library had silently moved. That refusal is scoped
  to the split this execution created; a pre-existing one arrives as the
  blocked audit above. The request stays runnable throughout; resolving two
  requests over one release is an operator decision.

Authority: "this was supposed to be just at import time. we go, oh, re=direct.
re-tag and then import." and "update the request row definitely. we don't care
about the old ID. the new ID is -the- ID" —
https://github.com/abl030/cratedigger/issues/1059

Every other Cratedigger Beets access is observational. Deployment-owned plain
`beet` remains a powerful trusted-librarian command outside Cratedigger's
serialization. Startup admission neither locks Beets against concurrent
operator mutation nor authorizes unrelated operator commands. Do not run
operator mutations while automation is active; quiesce the importer first and
re-run the checker afterward when the effective contract may have changed.

## Portable runtime contract

The NixOS module consumes the same deployment-neutral capability another
packager, container, or conventional service can supply:

- one compatible Beets Python package and its exact interpreter;
- one immutable, non-secret `BEETSDIR` containing the effective configuration;
- the canonical SQLite database and canonical library root;
- a distinct absolute, persistent, host-local Beets state file writable by
  the importer and librarian but read-only to the main, preview, and web roles;
- one designated mutable secret include containing exactly a non-empty
  `discogs.user_token` scalar and no other key; and
- deployment-owned readiness edges that complete storage, state, and secret
  provisioning before guarded applications start.

The deployment must keep the database, root, state file, interpreter, config
directory, and secret-include path consistent across Beets, Cratedigger's
six-field `[Beets]` runtime section, the harness, checker, and operator
environment. The config directory stays immutable to every application
identity. The state file must not live beneath it or share catalog storage; a
common host path is `/var/lib/beets/state.pickle`. The secret include is a
fixed-schema credential channel, never a mutable configuration overlay. Encode
a hostile-looking token as a scalar value rather than interpolating YAML, and
restart every guarded application after config or token rotation; there is no
live reload.

Each top-level application performs intrinsic exactly-once startup admission in
a fresh Beets configuration context. Hard failures include a missing or
incompatible runtime, mismatched database/root/state/interpreter, mutable
non-secret config, invalid state capability, unsafe import/path policy, an
inactive required plugin (`musicbrainz`, `permissions`, `inline`), or a
missing, multiple, wrong, or non-token-only designated secret include. The
merge retag (`lib/beets_retag.py`) needs no plugin — since #1087 it runs
`beet modify`, which makes no network call and needs no candidate mapping —
so `mbsync` is no longer in this required set; it stays active in the
deployed configuration for the operator's own manual use, and is still
reported observationally (`BeetsPluginContract.mbsync`). Approved
MusicBrainz endpoint drift is warning-only. The checker rejects only named
harness conflicts: active `convert.auto` or `convert.auto_keep` is unsafe, while
intentional metadata/artwork hooks such as `fetchart`, `embedart`, `scrub`,
`lyrics`, and `lastgenre` remain supported. The standalone
`cratedigger-check-beets-config` runs the same contract for deployment and
operator checks, but systemd alone is not the enforcement boundary.

On NixOS, the consumer supplies this capability through
`services.cratedigger.beets.runtime.{package,configDir,expectedLibrary,expectedDirectory,expectedStateFile,expectedSecretInclude,readinessUnits}`.
Cratedigger may offer `nix/beets.nix` as a compatible package factory, but the
deployment instantiates and owns it, its config, state, secrets, storage, and
plain `beet`. See [`docs/nixos-module.md`](nixos-module.md) and
[`examples/cratedigger.nix`](../examples/cratedigger.nix).

### IMPORTANT: `musicbrainz` is a Plugin

In modern beets (2.x), `musicbrainz` is a **plugin** that must be explicitly listed in the plugins string. Without it, beets returns 0 candidates for every album and all imports fail silently. This has bitten us multiple times.

## Configuration

Beets configuration is deployment-owned. Keep non-secret configuration in an
immutable directory (a Nix store directory in the NixOS composition), keep the
token-only include at its declared runtime path, and provision the host-local
state file separately. Change and deploy the owning configuration, restart the
guarded applications, then run `cratedigger-check-beets-config` and plain
`beet config` under the trusted operator environment. Never hand-edit the
effective config or secret include in place.

### Required production config shape (key settings)

```yaml
# Library
directory: /mnt/virtio/Music/Beets
library: /mnt/virtio/cratedigger/beets-db/beets-library.db
statefile: /var/lib/beets/state.pickle
include:
  - /run/beets/secrets.yaml  # exactly discogs.user_token

# Import behavior
import:
  copy: false        # Don't copy — move files into library structure
  move: true         # Move files (rename into Artist/Year - Album/Track Title.ext)
  write: true        # Write tags to files
  incremental: true  # Skip previously-seen directories
  incremental_skip_later: true

# Path templates
paths:
  default: $albumartist/$year - $album%aunique{albumartist album,path_disambig}/$track $title
  comp: Compilations/$album%aunique{albumartist album,path_disambig}/$track $title
  singleton: Non-Album/$artist/$title
# path_disambig is an inline-plugin album field: the first non-empty of
# albumdisambig / releasegroupdisambig / catalognum / label / str(year).
# It is never empty by construction — beets' %aunique renders an album's
# OWN value for the chosen disambiguator, and an empty value renders NO
# bracket (plain path → collides into the sibling pressing's folder; the
# Passenger incident, 2026-07-18). Ties fall back to beets' album-id
# bracket, which is also never empty. Contract-tested against real beets
# in tests/test_harness_beets2_contract.py.

# MusicBrainz — dedicated local mirror guest
musicbrainz:
  host: 192.168.1.43:5200
  https: false
  ratelimit: 100

# Matching
match:
  strong_rec_thresh: 0.10
  medium_rec_thresh: 0.25
  preferred:
    countries: [AU, US, "GB|UK"]
    media: ["Digital Media|File", CD]
    original_year: true

# Active plugins
plugins: musicbrainz mbsync discogs fetchart embedart lyrics lastgenre scrub info missing duplicates edit fromfilename ftintitle the inline permissions
```

### Active Plugins

| Plugin | Purpose | Auto? |
|--------|---------|-------|
| `musicbrainz` | MB lookups (REQUIRED — without it, 0 candidates) | — |
| `mbsync` | `beet mbsync` command — refetches an album by its stored `mb_albumid`, follows a MusicBrainz merge redirect, and rewrites the ID. Not required by Cratedigger since #1087 (the merge retag now runs `beet modify`, `lib/beets_retag.py`) — kept active for the operator's own manual use | — |
| `discogs` | Discogs lookups (fallback for obscure releases) | — |
| `fetchart` | Downloads cover art from CAA/iTunes/Amazon | Yes |
| `embedart` | Embeds cover art into audio file tags | Yes |
| `lyrics` | Fetches synced lyrics from local LRCLIB | Yes |
| `lastgenre` | Fetches genre tags from Last.fm | Yes |
| `scrub` | Strips old tags before writing new ones | Yes |
| `info` | `beet info` command for inspecting tags | — |
| `missing` | `beet missing` command — lists tracks beets expects but can't find on disk | — |
| `duplicates` | `beet duplicates` command | — |
| `edit` | `beet edit` command — hand-edit metadata in an external editor during import/ops | — |
| `fromfilename` | Guesses metadata from filenames when tags are missing | — |
| `ftintitle` | Moves "feat." from artist to title field | — |
| `the` | Handles "The" prefix in artist names | — |
| `inline` | Lets config.yaml define computed item/album fields in Python — powers `path_disambig` (the never-empty %aunique disambiguator in the path templates below) | — |
| `permissions` | Sets imported file/art mode to 0664 and dir mode to 02775 (setgid) | Yes |

`permissions` exists so media servers (Jellyfin) can read album art: beets'
native `fetchart` writes art via `mkstemp` (forces 0600 regardless of umask)
then renames it into place, and nothing else chmods it — without the plugin,
art lands 0600 and Jellyfin throws `UnauthorizedAccessException` trying to
read it. Its `art_set` listener (`fix_art`) fixes the mode on BOTH initial
import and a manual `beet fetchart` re-fetch (issue #570 defect 1).

`dir: 02775` is setgid, not plain `0775` — that bit is load-bearing: it's
what lets every child album dir beets creates underneath inherit the
library's group, which is what makes it safe to run the library group-owned
by a shared consumer group (e.g. `users`) so media servers can both read art
and write NFO/artwork alongside it. `fix_library_modes` (`lib/permissions.py`)
only touches directories, never files — it's the post-import
belt-and-suspenders pass that re-asserts `02775` on dirs the plugin's
per-item listener misses (empty/intermediate dirs), and `reset_umask()` sets
the process umask to `0o002` (group-writable) at every pipeline entry point.
Full non-root + setgid recipe: `docs/nixos-module.md` § "Running non-root +
filesystem permissions".

### Cover Art Config

```yaml
fetchart:
  auto: true
  minwidth: 300     # Reject thumbnails too small to display
  maxwidth: 500     # CAA serves pre-built 500px thumbnails (no local resize needed)
  quality: 75       # JPEG compression for non-CAA sources
  sources:          # Priority order:
    - coverart      # MusicBrainz Cover Art Archive — best quality
    - itunes        # Apple Music
    - amazon
    - albumart      # albumart.org
    - cover_art_url # URL from MB release
    - filesystem    # Local cover.jpg — LAST resort (prevents tiny legacy art shadowing)
```

**Why maxwidth: 500 matters**: Embedded art is duplicated in EVERY track. At the old average (1138KB/cover), embedding across 83K tracks = ~91GB. At 500px (~71KB), it's ~6GB. An 85GB saving.

**Artwork floor**: `minwidth: 300` prevents unusably small embedded artwork from entering the curated library.

## Library Structure

### Paths

| Path | Purpose |
|------|---------|
| `/mnt/virtio/Music/Beets` | Tagged library — organized by beets path templates |
| `/mnt/virtio/cratedigger/beets-db/beets-library.db` | SQLite database — the DB source of truth |
| `/mnt/virtio/cratedigger/beets-db/beets-import.log` | Import log |
| `/mnt/virtio/Music/AI` | Staging area — raw copies from `/Me`, pre-import |
| `/mnt/virtio/Music/Incoming` | Auxiliary import staging — `/Incoming/auto-import` for YouTube rescues, `/Incoming/post-validation` for redownload/manual-review staging; Soulseek request imports stay at their exact processing-owner path |
| `/mnt/virtio/Music/Re-download` | Re-download queue — each album has a README.md explaining why |

### File Organization

Beets enforces this structure:
```
/mnt/virtio/Music/Beets/
  Artist Name/
    Year - Album Title/
      01 Track Title.mp3
      02 Track Title.mp3
      ...
  Compilations/
    Album Title/
      01 Track Title.mp3
      ...
```

### Library Format

Lossless sources first produce a temporary MP3 VBR V0 quality probe, then use
the configured storage target (commonly Opus) before Beets import. Conversion
maps only audio and preserves the source tag surface Beets matches on, while
deleting the art-in-tag surfaces (`METADATA_BLOCK_PICTURE`, legacy
`COVERART`/`COVERARTMIME`) — Beets matches on the staged tags, then applies
fresh canonical tags (issue #863; the discarded V0 probe still strips
everything). Every album derivative is staged before commit, so the source
is removed only after all files converted successfully with nonempty outputs.
On any conversion failure every source is retained and the typed failure audit
is returned to preview.

Legacy formats remain in the library where no later managed import replaced
them. The active target is configuration, not a hard-coded MP3-only invariant.

### Library Stats (as of 2026-03-24)

- **Tracks**: 83,643
- **Albums**: 7,582+
- **Album Artists**: 3,890
- **Size**: ~618 GB
- **Formats**: 81,578 MP3, 1,761 M4A, 183 FLAC, 62 WMA, 35 OPUS, 24 OGG

## The Beets Harness

The harness (`harness/beets_harness.py`) is a custom `ImportSession` subclass that replaces beets' interactive terminal prompts with a JSON protocol over stdin/stdout. This is how Cratedigger (and all automated imports) communicate with beets.

### Why the Harness Exists

`beet import` is designed for interactive terminal use — it prints colored text, waits for keyboard input, and has no machine-readable output. The harness subclasses `ImportSession` and overrides `choose_match()`, `choose_item()`, `get_duplicate_action()`, and `should_resume()` to communicate via newline-delimited JSON instead.

### Running the Harness

**NEVER run `python harness/beets_harness.py` directly.** The system Python doesn't have beets installed. Always use the shell wrapper:

```bash
./harness/run_beets_harness.sh [options] /path/to/album
```

The wrapper execs `$CRATEDIGGER_BEETS_PYTHON` (the admitted external Beets
environment's interpreter) on `beets_harness.py`. In production that variable
and `BEETSDIR` come from `lib/util.py::beets_subprocess_env()`, which reads the
deployment-owned six-field `[Beets]` runtime contract; in the dev shell the
shell hook exports the test environment. A missing interpreter is an
actionable error with no per-user config fallback. Dispatch also passes its
snapshotted Beets DB/root/config-dir/Python
authority explicitly to `import_one.py`: the child opens the DB/root pair
directly for preflight and postflight, while its nested harness resolves the
same library through `BEETSDIR` and that config directory's `config.yaml`.
Thus a runtime-config change between dispatch and child launch cannot redirect
either child or harness library reads.

### Harness Options

| Flag | Purpose |
|------|---------|
| `--search-id MBID` | Restrict search to a specific MB release ID |
| `--noincremental` | Don't skip previously-seen directories |
| `--pretend` | Dry run — show matches but don't import |
| `--upstream` | Use upstream musicbrainz.org instead of local mirror |

### JSON Protocol

The harness communicates over stdin/stdout using newline-delimited JSON (NDJSON).

**Harness → Controller (stdout)**:

```jsonc
// Session lifecycle
{"type": "session_start", "paths": [...], "pretend": false, "library": "...", "directory": "..."}
{"type": "session_end"}

// Match decision needed
{"type": "choose_match", "task_id": 0, "path": "...", "cur_artist": "...", "cur_album": "...",
 "item_count": 12, "items": [...], "candidates": [...], "recommendation": "strong"}

// Duplicate detected. duplicate_candidates is the exact beets album set that
// Beets will remove if the controller answers {"action": "remove"}.
{"type": "resolve_duplicate", "path": "...", "duplicate_count": 1,
 "duplicate_mbids": ["..."], "duplicate_album_ids": [123],
 "duplicate_candidates": [{"beets_album_id": 123, "mb_albumid": "...",
   "discogs_albumid": "", "album_path": "...", "item_count": 10}]}

// Import completed (added 2026-03-24)
{"type": "album_imported", "album_id": 123, "artist": "...", "album": "...",
 "mb_albumid": "...", "path": "...", "item_count": 12, "items": [...]}

// Per-track import event (added 2026-03-24)
{"type": "item_imported", "item_id": 456, "artist": "...", "title": "...", "track": 1, "path": "..."}
```

**Controller → Harness (stdin)**:

```jsonc
// Accept a match — ALWAYS use candidate_id, NEVER candidate_index
{"action": "apply", "candidate_id": "mb-release-uuid"}

// Skip this album
{"action": "skip"}

// Import with existing metadata (no MB match)
{"action": "asis"}

// Duplicate resolution. Cratedigger may answer remove only after validating
// exactly one duplicate candidate whose release identity matches the target.
{"action": "keep"}     // Keep both editions
{"action": "remove"}   // Let Beets atomically remove the old copy and import the new copy
{"action": "merge"}    // Merge into existing album entry
{"action": "skip"}     // Skip (don't import)
```

### Current library membership and paths

`lib.beets_db.BeetsDB.resolve_current_release()` is the one read authority for
current installed membership and location. It accepts a typed
`ReleaseIdentity`, enumerates every exact Beets album row, and returns one of
three frozen outcomes: `CurrentBeetsUnique`, `CurrentBeetsMissing`, or
`CurrentBeetsAmbiguous`. A unique result includes the album primary key, every
item primary key and absolute path, the single album directory, and the exact
MB/Discogs selectors. Empty albums, split-directory albums, and duplicate exact
identities are ambiguous and therefore cannot authorize a path-dependent
operation.

Discogs IDs are queried in both `discogs_albumid` (current layout) and numeric
`mb_albumid` (legacy layout). No title, artist, release-group, folder, or sibling
fallback exists. Batch presence/detail APIs expose only usable unique results;
they never collapse two exact rows by `LIMIT 1` or dictionary overwrite.
Library presence badges use this exact live read independently from durable
capture history and linked proof. A failed Beets read is an API error, never
evidence of absence and never a request transition. See
`docs/webui-primer.md` for the independent badge vocabulary and
`docs/debugging-cli.md` for the grouped A/B/C world-audit contract; detailed
evidence authority remains canonical in `docs/quality-verification.md`.

The deployment supplies the canonical database/root pair as both effective
Beets configuration and `beets.runtime.expectedLibrary` /
`beets.runtime.expectedDirectory`; the module records those values in the
guarded `[Beets]` runtime section.
`open_beets_db()` and the zero-argument `BeetsDB()` constructor both open that
runtime pair. Passing paths directly to `BeetsDB(...)` or web `--beets-db` is
for explicit development/test injection.

### CRITICAL: Always Match by candidate_id, Never candidate_index

Candidate ordering is **NOT stable** between beets runs. MB mirror updates, timing, and internal sorting change the order. Using `candidate_index` has caused wrong imports in the past. Always find the candidate whose `album_id` matches your target MB release ID.

The harness supports both `candidate_id` (preferred) and `candidate_index` (legacy). The `candidate_id` field matches against `candidate.info.album_id`.

### Discogs matches keep their id in `discogs_albumid`, never `mb_albumid`

beets' native `discogs` plugin sets `AlbumInfo.album_id`/`releasegroup_id` to
**numeric** Discogs ids, and beets core maps `album_id -> mb_albumid`. Left
alone, a Discogs import would write a bare integer into `mb_albumid` and the
`MUSICBRAINZ_ALBUMID` file tag — downstream MusicBrainz taggers choke on it
(Jellyfin does `new Guid(tag)`, which throws `FormatException` and aborts the
whole album's metadata fetch).

The harness neutralizes this at apply time
(`harness/beets_harness.py::_neutralize_discogs_provider_ids`, called from
`_apply_decision`): for a chosen **Discogs** candidate it blanks
`album_id`/`releasegroup_id` before beets writes anything, so `mb_albumid`/
`mb_releasegroupid` end up empty and the release id lives ONLY in
`discogs_albumid`. MusicBrainz matches (UUID `album_id`) are untouched — this
is the layout the rest of Cratedigger already assumes (`duplicate_keys =
[mb_albumid, discogs_albumid]`, `lib/beets_db.py`) (issue #570).

### The album_imported Event

Added 2026-03-24 to fix the "silent import" problem. Previously, a successful import produced no JSON output at all — the harness went straight from `choose_match` to `session_end` with nothing in between. Now every successful import emits an `album_imported` event with full track details. This is essential for pipeline automation — you need to know what actually happened.

## import_one.py

The one-shot import script used by Cratedigger for auto-importing `source='request'` albums. Lives at `harness/import_one.py`.

### Flow

```
1. Pre-flight: is this MBID already in beets? → exit 0 if yes
2. Strictly validate mapped audio and collect spectral/source evidence
3. Stage the V0 probe and configured storage conversion as an album transaction
4. Drive harness: --search-id MBID --noincremental
   → Find candidate matching MBID
   → Check distance ≤ 0.5
   → If Beets reports duplicates, inspect the exact duplicate candidate set
   → Answer remove only for exactly one same-release duplicate
   → Apply match
5. Post-flight: verify MBID appeared in beets DB
6. Cleanup: remove staged files (beets moved them to /Beets)
7. Update pipeline DB: status → imported
```

### Guarded Beets Replacement

Beets owns duplicate replacement again for safe upgrades. The Palo Santo root
cause was a misplaced Beets `duplicate_keys` config block, not Beets' atomic
replacement model. Cratedigger now keeps the startup guard that requires
`import.duplicate_keys.album` to be exact release identifiers only:
`mb_albumid` and `discogs_albumid`. Including mutable metadata such as
`albumartist` or `album` can make same-release upgrades with normalized
metadata drift miss Beets' duplicate callback and fall back to Cratedigger's
temporary cleanup path.

The controller answers `{"action": "remove"}` only when Beets reports exactly
one duplicate album and that album's exact release identity matches the import
target. MusicBrainz rows match on `mb_albumid`; Discogs rows match on
`discogs_albumid`. Missing identity, multiple duplicates, or a mismatched
identity fails closed with `duplicate_remove_guard_failed` before Beets can
remove anything.

The harness also patches Beets 2.9 duplicate lookup inside the harness process.
Beets builds the duplicate query before applying provider metadata to library
field names, so a MusicBrainz candidate still carries `album_id` while the
library column is `mb_albumid`. The harness maps duplicate lookup metadata
through Beets' media-field names first; otherwise exact release-id
`duplicate_keys` silently query an empty `mb_albumid` and never reach the
guarded duplicate callback.

Guard failures are source failures. Dispatch records the structured
would-remove set in `download_log.import_result`, denylists the source/user,
and moves the staged files to `Incoming/duplicate-remove-guard/`. That folder is
preserved for diagnostics only; it is separate from Wrong Matches and has no v1
UI.

The old Cratedigger stale-row cleanup and sibling canonicalization state
machine has been removed. Beets owns the atomic delete-and-replace operation;
Cratedigger's job is to fail before mutation when Beets' would-remove set is
not exactly one same-release album.

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Imported successfully (or already in beets) |
| 1 | FLAC conversion failed |
| 2 | Beets import failed (harness error, high distance, post-flight fail) |
| 3 | Album path not found |
| 4 | Target MBID not found in beets candidates |
| 7 | Duplicate-remove guard failed before Beets removed anything |

### Constants

```python
BEETS_DB = "/mnt/virtio/cratedigger/beets-db/beets-library.db"
HARNESS_TIMEOUT = 300   # 5 min for match selection
IMPORT_TIMEOUT = 1800   # 30 min for actual import (fetchart, embedart, lyrics can be slow)
max_distance = 0.5      # Reject matches above this distance
```

## Beets Distance

The match "distance" is a 0.0–1.0 score where 0.0 = perfect match and 1.0 = completely wrong.

| Distance | Meaning | Action |
|----------|---------|--------|
| 0.00–0.10 | Strong match (exact or near-exact metadata) | Auto-apply |
| 0.10–0.25 | Medium match (minor differences — capitalization, punctuation) | Auto-apply with review |
| 0.25–0.50 | Weak match (significant differences — may be wrong edition) | Manual review |
| 0.50+ | Poor match (probably wrong album entirely) | Reject |

The pipeline uses 0.15 as the threshold for auto-staging redownloads and 0.50 as the hard reject threshold in `import_one.py`.

### What Affects Distance

- **Track count mismatch** — big penalty
- **Track title differences** — proportional to number of different titles
- **Track length differences** — proportional to total length deviation
- **Artist/album name differences** — proportional to edit distance
- **Missing tracks** — `extra_tracks > 0` means MB has more tracks than local files

## Trusted operator Beets commands

These are deployment-owned plain `beet` commands. Run them with the same
immutable `BEETSDIR`, interpreter, database, root, state file, and secret
authority admitted for Cratedigger. Quiesce the serial importer first, target
one exact album primary key, inspect the selection before mutation, and run the
standalone checker again before resuming automation.

```bash
# Search library
beet ls "Artist" "Album"                    # List items matching query
beet ls -a "Artist"                          # List albums matching query
beet ls -f '$track $title :: $path' "Artist" # Custom format

# Inspect
beet info -l "Artist" "Album"                # Show all tag fields for items
beet info -l -a "Artist" "Album"             # Show album-level fields
beet missing -c "Artist" "Album"             # Count missing tracks
beet missing "Artist" "Album"                # List missing tracks

# Library health
beet stats                                   # Track/album/artist counts
beet bad "Artist" "Album"                    # Check for corrupt files (needs badfiles plugin)
beet duplicates                              # Find duplicate albums

# Modify one already-inspected exact album (CAREFUL — changes library)
beet modify -a -M -W 'id:123' field=value     # DB-only edit; defer move/write
beet write 'album_id:123'                     # Write admitted DB metadata to tags
beet move -p -a 'id:123'                      # Preview a path move first
beet move -a 'id:123'                         # Apply only after reviewing the preview
beet remove -a 'id:123'                       # Catalog-only removal; files stay on disk
```

`beet move` and path-affecting configuration can rename library trees and mint
new Plex/Jellyfin identities. Never run a collection-wide move as routine
maintenance. Review one exact album, ensure notifier/rescan consequences are
understood, and keep it outside a Cratedigger deployment or cutover. Concurrent
plain-`beet` mutation is not locked against the importer.

### Dangerous Commands — NEVER Use Without Approval

```bash
beet remove -d ...     # -d DELETES FILES FROM DISK. If those files came from a
                       # niche source that can't be re-acquired, they're gone forever.

beet import ...        # Raw imports bypass exact-request inspection and the JSON harness.
beet import -A ...     # "As-is" imports are especially forbidden.

printf 'a\n' | beet import ...  # Blindly accepts ANY match without inspection.
                                # Use the harness instead — it lets you verify MBID and distance.
```

Cratedigger's explicit Bad Rip, Replace, and library-delete actions are the
narrow exceptions: they resolve one current exact album primary key and route
destructive removal through the admitted-runtime exact-delete child in
`lib/beets_delete.py`. Selector-based `beet remove -d` is retired. The
effective Beets `clutter` list includes the exact derived filename
`cratedigger.json`, allowing Beets to prune a directory whose managed audio
and sidecar are all gone. Any file that does not match the configured clutter
patterns prevents pruning and remains untouched.

Library delete (`POST /api/beets/delete` / `pipeline-cli library-delete`) is a
separate exact-album-PK operation using the same admitted Beets runtime. It
does not write Beets SQLite directly and does not use stock `beet remove -d`:
the admitted Beets 2.x removes metadata before its filesystem loop. Instead,
`harness/delete_album.py` keeps the album row as the retry manifest while Beets
removes and verifies exact item paths, exact art, `cratedigger.json`, and
configured clutter. Paths are confined to `directory:` with realpath/symlink
checks. Before mutation, the harness also proves that its active `library:`
and `directory:` resolve to the exact SQLite path and root used by the web/CLI
preflight. Beets metadata is removed with `delete=False` only after every
owned artifact is absent; album, item, and flexible-field rows share one outer
transaction with explicit rollback on any exception.

The child commits that final metadata transaction before its JSON result can
reach the parent, so a process exit or malformed/truncated acknowledgement is
commit-ambiguous. The synchronous endpoint deliberately does not infer success
from later metadata absence: it returns `delete_incomplete`, preserves the
PostgreSQL request, skips Plex/Jellyfin notification, and retains the preflight
album, artist, exact former path, and pipeline state for explicit operator
recovery. Its message says that filesystem deletion is unconfirmed and Beets
metadata may be gone; it never claims files were deleted. Honest automatic
crash recovery would require a durable queued delete owned by the serialized
importer worker. That larger architecture is outside this endpoint hardening.

This endpoint is the existing destructive Web/CLI surface being hardened; it
does not create a second general Beets-mutating entry point. Confirmed success
still requires the child result plus a fresh exact album-and-item
metadata postcondition. Optional pipeline purge remains last, and
Plex/Jellyfin notification still waits until the destructive locks are
released.

Unknown content is never recursively guessed away. It remains on disk, appears
in `preserved_paths`, and blocks directory pruning. An error enumerating an
album directory fails closed before metadata removal; it is never interpreted
as an empty directory. Owned-path presence uses strict `lstat`: only
`FileNotFoundError` means absent, while every other probe error fails closed
before Beets metadata removal. Partial I/O leaves both the Beets row and pipeline row
available for retry. Zero newly deleted files is
successful only when the complete postcondition was already satisfied.

## The Beets SQLite Database

Located at `/mnt/virtio/cratedigger/beets-db/beets-library.db`. Two main tables:

### `albums` table (key fields)

| Column | Type | Purpose |
|--------|------|---------|
| `id` | INTEGER | Primary key |
| `albumartist` | TEXT | Album artist (used for grouping) |
| `album` | TEXT | Album title |
| `mb_albumid` | TEXT | MusicBrainz release ID |
| `year` | INTEGER | Release year |
| `path` | BLOB | Filesystem path to album directory |
| `added` | REAL | Timestamp when imported |

### `items` table (key fields)

| Column | Type | Purpose |
|--------|------|---------|
| `id` | INTEGER | Primary key |
| `album_id` | INTEGER | FK to albums.id |
| `artist` | TEXT | Track artist |
| `title` | TEXT | Track title |
| `track` | INTEGER | Track number |
| `disc` | INTEGER | Disc number |
| `path` | BLOB | Filesystem path to audio file |
| `mb_trackid` | TEXT | MusicBrainz recording ID |
| `length` | REAL | Duration in seconds |

### Useful Queries

```sql
-- Check if an MBID is imported
SELECT id, albumartist, album, path FROM albums WHERE mb_albumid = 'uuid-here';

-- Count tracks for an album
SELECT COUNT(*) FROM items WHERE album_id = 123;

-- Find all items for an album
SELECT track, title, path FROM items WHERE album_id = 123 ORDER BY disc, track;

-- Check for orphan albums (in DB but path missing)
SELECT id, albumartist, album, path FROM albums
WHERE NOT EXISTS (SELECT 1 FROM items WHERE items.album_id = albums.id);
```

**Note**: Paths are stored as BLOBs (bytes) in the DB, not TEXT. When querying from Python:
```python
raw = row[0]
if isinstance(raw, bytes):
    raw = raw.decode("utf-8", errors="replace")
```

## MusicBrainz Mirror

The dedicated MusicBrainz guest serves the local mirror at
`192.168.1.43:5200`. Beets is configured to use it instead of the public MB
API.

- **Web UI**: `http://192.168.1.43:5200`
- **API**: `http://192.168.1.43:5200/ws/2/`
- **Replication**: Daily from upstream MetaBrainz at 03:00
- **Rate limit**: 100 req/s (vs 1 req/s on public API)

### API Examples

```bash
# Search for a release
curl -s "http://192.168.1.43:5200/ws/2/release?query=artist:Artist+AND+release:Album&fmt=json"

# Get release with tracks
curl -s "http://192.168.1.43:5200/ws/2/release/MBID?inc=recordings+media&fmt=json"

# Get all releases in a release group
curl -s "http://192.168.1.43:5200/ws/2/release-group/RGID?inc=releases&fmt=json"
```

**Newly seeded releases**: If you seed a release on upstream musicbrainz.org, it won't appear in the local mirror until the next daily replication. Use `--upstream` flag on the harness to query upstream directly for fresh seeds.

## Retagging, moving, and catalog-only removal

These are librarian decisions, not acquisition-history rewrites. Stop the
importer, resolve and inspect the exact Beets album primary key, back up the
catalog, and use the plain-`beet` exact-ID forms above. A release-identity retag
may make the old requested pressing render as Captured plus Missing and the new
held identity as Untracked; that is truthful. Do not merge the pressings in
Cratedigger or change the old request's durable proof.

For a release-identity retag, use the admitted JSON harness—not raw
`beet import`—so the operator selects the exact candidate ID and inspects the
match. With the importer stopped and the exact album path verified:

```bash
printf '{"action":"apply","candidate_id":"NEW-MBID"}\n{"action":"merge"}\n' | \
  ./harness/run_beets_harness.sh \
    --search-id "NEW-MBID" --noincremental "/exact/current/album/path"
```

For a known metadata-field edit, use exact-ID `beet modify -a -M -W` so the
initial edit neither moves files nor writes tags; write only after inspection.
Preview any resulting path change with
`beet move -p -a 'id:<pk>'` before applying the exact move. For a catalog-only
removal, use `beet remove -a 'id:<pk>'`; never add `-d`. Resume automation only
after the harness/operator command has exited and the standalone checker passes.

## Audio Health & Validation

### Current State (2026-03-24)

A full library health check found:
- **mp3val** fixed 11,268 MP3 header issues (frame sync errors, Xing header mismatches, garbage at EOF)
- **ffmpeg full decode** found 81 MP3s with actual audio corruption across 23 albums
- **5 M4A files** with corrupt AAC frames
- **1 OPUS file** with muxing issues

### Available Tools

| Tool | Format | What it checks | Speed |
|------|--------|---------------|-------|
| `mp3val` | MP3 | Frame headers, Xing headers, stream structure | Fast (header only) |
| `mp3val -f` | MP3 | Same + auto-fix (rewrite headers, trim garbage) | Fast |
| `flac -t -s` | FLAC | CRC-verified full decode (FLAC has built-in checksums) | Fast |
| Cratedigger strict FFmpeg policy (`-max_error_rate 0`, audio-scoped error detection, `-map 0:a`, null output) | All | Complete mapped-audio decode with typed outcomes; positive exit on stable readable bytes is bad audio | Slow (~18 files/s) |

Plain FFmpeg defaults can print recoverable decoder errors and still return
zero, so `ffmpeg -v error -i FILE -f null -` alone is not an integrity proof.
Cratedigger also ignores tags, pictures, chapters, and exit-zero stderr at this
read-only boundary. Kept conversion outputs preserve the tag surface Beets
matches on and strip only the embedded-art surfaces (#863); the discarded V0
probe strips everything; Beets writes canonical tags fresh after matching.

### Validation Script

`tagging-workspace/scripts/audio_health_check.py` in the separate
tagging-workspace repo:
```bash
python3 scripts/audio_health_check.py --ext mp3 --workers 8  # Full MP3 decode, 8 parallel
python3 scripts/audio_health_check.py --ext flac              # FLAC integrity check
python3 scripts/audio_health_check.py                          # All formats
```

### badfiles Plugin

Beets has a built-in `badfiles` plugin that provides:
- `beet bad [QUERY]` — on-demand file corruption check
- `check_on_import: yes` — validate files before importing (interactive prompt: abort/skip/continue)

Currently NOT enabled. The `check_on_import` option triggers interactive prompts that break the JSON harness. See GitHub issue #2 for the plan to integrate audio validation into Cratedigger's post-download pipeline instead.

## Deploying Beets Config Changes

Beets config belongs to the deployment, not this module. Change the owning
immutable config and fixed-schema token delivery, keep all six runtime fields
aligned, restart the guarded Cratedigger applications after config or token
rotation, and run the standalone checker before releasing automation. In the
homelab this is a signed nixosconfig change followed by the normal fleet
deployment; a conventional service or container owes the equivalent atomic
config replacement, readiness, restart, and checker evidence.

```bash
# Verify after deploy
ssh doc2 'cratedigger-check-beets-config --role importer'
ssh doc2 'beet config >/dev/null && echo OK'             # same operator BEETSDIR
ssh doc2 'beet version'                                  # deployment-owned package
```

Never edit the effective config or token include in place. Replace and deploy
them through their owner so restart triggers and readiness edges run. The
module VM asserts that the external configuration is immutable, the runtime
contract is admitted intrinsically at application startup, and importer-only
state write capability remains separate from config storage.

## Troubleshooting

### "0 candidates" for every album
The `musicbrainz` plugin is not loaded. Check that `plugins` string includes `musicbrainz` in the beets config.

### Import writes wrong path structure
The `Library()` constructor needs ALL FOUR args: `dbpath, directory, get_path_formats(), get_replacements()`. Without `path_formats` and `replacements`, it uses the hardcoded default `$artist/$album/$track $title` instead of the user's config. The harness handles this correctly.

### "incremental" skips directories
Beets remembers every directory it's imported from. Use `--noincremental` flag on the harness (sets `config["import"]["incremental"] = False`) to re-process.

### Harness hangs with no output
Usually means beets is waiting for a network request (MB lookup, cover art fetch). Check that:
- MB mirror is reachable: `curl -s http://192.168.1.43:5200/ws/2/release?query=test&fmt=json | head`
- No firewall blocking the application host from the dedicated mirror guest
- Not stuck on chroma fingerprinting (disabled by default, but check `chroma.auto`)

### Files with special characters in paths
**NEVER use bash shell commands** (`ls`, `rm`, `find`, `cd`) on paths containing quotes, ampersands, unicode dashes, or CJK characters. Bash mangles embedded quotes. Always use Python (`os.listdir`, `os.path.exists`, `shutil.rmtree`, `pathlib`).

### "Controller disconnected — aborting"
The harness's stdin closed before it finished reading decisions. This happens when you pipe input but don't provide enough lines for all prompts (e.g., `choose_match` needs one line, then `resolve_duplicate` needs another).

### scrub plugin strips tags from source files
In pretend mode, the harness returns `Action.SKIP` instead of the candidate to prevent beets from calling `apply()`. The old approach (copy=False, move=False, write=False) still let beets write to the DB and run scrub, which corrupted source files.

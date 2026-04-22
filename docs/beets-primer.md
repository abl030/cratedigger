# Beets Primer for Cratedigger

This document describes how beets is set up, configured, and used in the music pipeline. It's written for the Cratedigger fork's Claude context — if you're modifying anything that touches beets imports, validation, or the harness, read this first.

## What is Beets?

Beets is the canonical source of truth for the tagged music library at `/Beets`. It handles:

- **Matching** — identifying which MusicBrainz (or Discogs) release an album is
- **Tagging** — writing corrected metadata (artist, album, track names, year, genre, etc.) into file tags
- **File structure** — renaming and organizing files into `Artist/Year - Album/Track Title.mp3`
- **Cover art** — fetching from Cover Art Archive, embedding into files
- **Lyrics** — fetching synced lyrics from a local LRCLIB mirror
- **Library DB** — SQLite database tracking every album and track

When Cratedigger downloads an album and it passes validation, beets is what actually imports it into the library.

## Version & Installation

- **Version**: 2.5.1 (Python 3.13.11)
- **Installed via**: Nix Home Manager on doc1 (`192.168.1.29`)
- **Nix module**: `/home/abl030/nixosconfig/modules/home-manager/services/beets.nix`
- **Binary**: `/etc/profiles/per-user/abl030/bin/beet`
- **Package override**: Custom `overrideAttrs` patches the lyrics plugin to point at local LRCLIB (`http://192.168.1.35:3300/api`) instead of public `lrclib.net`

### IMPORTANT: `musicbrainz` is a Plugin

In beets 2.5.1, `musicbrainz` is a **plugin** that must be explicitly listed in the plugins string. Without it, beets returns 0 candidates for every album and all imports fail silently. This has bitten us multiple times.

## Configuration

**Generated config**: `~/.config/beets/config.yaml` (managed by Nix — do NOT edit directly)
**Secrets**: `~/.config/beets/secrets.yaml` (Discogs token, included via beets `include:` directive)

To change the config:
1. Edit the Nix module: `/home/abl030/nixosconfig/modules/home-manager/services/beets.nix`
2. Rebuild: `cd /home/abl030/nixosconfig && nix fmt && sudo nixos-rebuild switch --flake .#proxmox-vm`
3. Verify: `cat ~/.config/beets/config.yaml` — confirm changes landed

### Current Config (Key Settings)

```yaml
# Library
directory: /mnt/virtio/Music/Beets
library: /mnt/virtio/Music/beets-library.db

# Import behavior
import:
  copy: false        # Don't copy — move files into library structure
  move: true         # Move files (rename into Artist/Year - Album/Track Title.ext)
  write: true        # Write tags to files
  incremental: true  # Skip previously-seen directories
  incremental_skip_later: true

# Path templates
paths:
  default: $albumartist/$year - $album%aunique{}/$track $title
  comp: Compilations/$album%aunique{}/$track $title
  singleton: Non-Album/$artist/$title

# MusicBrainz — local mirror on doc2
musicbrainz:
  host: 192.168.1.35:5200
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
plugins: musicbrainz discogs fetchart embedart lyrics lastgenre scrub info missing duplicates edit fromfilename ftintitle the
```

### Active Plugins

| Plugin | Purpose | Auto? |
|--------|---------|-------|
| `musicbrainz` | MB lookups (REQUIRED — without it, 0 candidates) | — |
| `discogs` | Discogs lookups (fallback for obscure releases) | — |
| `fetchart` | Downloads cover art from CAA/iTunes/Amazon | Yes |
| `embedart` | Embeds cover art into audio file tags | Yes |
| `lyrics` | Fetches synced lyrics from local LRCLIB | Yes |
| `lastgenre` | Fetches genre tags from Last.fm | Yes |
| `scrub` | Strips old tags before writing new ones | Yes |
| `info` | `beet info` command for inspecting tags | — |
| `missing` | `beet missing` command — lists tracks beets expects but can't find on disk | — |
| `duplicates` | `beet duplicates` command | — |
| `fromfilename` | Guesses metadata from filenames when tags are missing | — |
| `ftintitle` | Moves "feat." from artist to title field | — |
| `the` | Handles "The" prefix in artist names | — |

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

**Meelo threshold**: Cover art under ~2KB shows as black boxes in the Meelo media server. The `minwidth: 300` setting prevents this.

## Library Structure

### Paths

| Path | Purpose |
|------|---------|
| `/mnt/virtio/Music/Beets` | Tagged library — organized by beets path templates |
| `/mnt/virtio/Music/beets-library.db` | SQLite database — the DB source of truth |
| `/mnt/virtio/Music/beets-import.log` | Import log |
| `/mnt/virtio/Music/AI` | Staging area — raw copies from `/Me`, pre-import |
| `/mnt/virtio/Music/Incoming` | Processing staging root — `/Incoming/auto-import` for request auto-imports, `/Incoming/post-validation` for redownload/manual-review staging |
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

**MP3 VBR V0** — all imports are converted from FLAC before beets import. The conversion happens in `import_one.py` via ffmpeg (`-codec:a libmp3lame -q:a 0`). FLAC files are deleted after successful conversion.

Some legacy imports are m4a, ogg, opus, wma, or even ape — these came from the original `/Me` library and weren't converted. New imports should always be MP3 V0.

### Library Stats (as of 2026-03-24)

- **Tracks**: 83,643
- **Albums**: 7,582+
- **Album Artists**: 3,890
- **Size**: ~618 GB
- **Formats**: 81,578 MP3, 1,761 M4A, 183 FLAC, 62 WMA, 35 OPUS, 24 OGG

## The Beets Harness

The harness (`harness/beets_harness.py`) is a custom `ImportSession` subclass that replaces beets' interactive terminal prompts with a JSON protocol over stdin/stdout. This is how Cratedigger (and all automated imports) communicate with beets.

### Why the Harness Exists

`beet import` is designed for interactive terminal use — it prints colored text, waits for keyboard input, and has no machine-readable output. The harness subclasses `ImportSession` and overrides `choose_match()`, `choose_item()`, `resolve_duplicate()`, and `should_resume()` to communicate via newline-delimited JSON instead.

### Running the Harness

**NEVER run `python harness/beets_harness.py` directly.** The system Python doesn't have beets installed. Always use the shell wrapper:

```bash
./harness/run_beets_harness.sh [options] /path/to/album
```

The wrapper:
1. Finds the Nix-managed `beet` binary
2. Follows the wrapper chain to find `.beet-wrapped`
3. Extracts the Python interpreter and PYTHONPATH from the Nix environment
4. Runs `beets_harness.py` with the correct Python + site-packages

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

// Duplicate detected
{"type": "resolve_duplicate", "path": "...", "duplicate_count": 1, "existing_mbids": ["..."]}

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

// Duplicate resolution
{"action": "keep"}     // Keep both editions
{"action": "remove"}   // Remove old, keep new
{"action": "merge"}    // Merge into existing album entry
{"action": "skip"}     // Skip (don't import)
```

### CRITICAL: Always Match by candidate_id, Never candidate_index

Candidate ordering is **NOT stable** between beets runs. MB mirror updates, timing, and internal sorting change the order. Using `candidate_index` has caused wrong imports in the past. Always find the candidate whose `album_id` matches your target MB release ID.

The harness supports both `candidate_id` (preferred) and `candidate_index` (legacy). The `candidate_id` field matches against `candidate.info.album_id`.

### The album_imported Event

Added 2026-03-24 to fix the "silent import" problem. Previously, a successful import produced no JSON output at all — the harness went straight from `choose_match` to `session_end` with nothing in between. Now every successful import emits an `album_imported` event with full track details. This is essential for pipeline automation — you need to know what actually happened.

## import_one.py

The one-shot import script used by Cratedigger for auto-importing `source='request'` albums. Lives at `harness/import_one.py`.

### Flow

```
1. Pre-flight: is this MBID already in beets? → exit 0 if yes
2. Convert FLAC → MP3 VBR V0 (ffmpeg, -q:a 0)
3. Drive harness: --search-id MBID --noincremental
   → Find candidate matching MBID
   → Check distance ≤ 0.5
   → Apply match
4. Post-flight: verify MBID appeared in beets DB
5. Cleanup: remove staged files (beets moved them to /Beets)
6. Update pipeline DB: status → imported
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Imported successfully (or already in beets) |
| 1 | FLAC conversion failed |
| 2 | Beets import failed (harness error, high distance, post-flight fail) |
| 3 | Album path not found |
| 4 | Target MBID not found in beets candidates |

### Constants

```python
BEETS_DB = "/mnt/virtio/Music/beets-library.db"
HARNESS_TIMEOUT = 300   # 5 min for match selection
IMPORT_TIMEOUT = 1800   # 30 min for actual import (fetchart, embedart, lyrics can be slow)
MAX_DISTANCE = 0.5      # Reject matches above this distance
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

## Common Beets Commands

These run on doc1 where beets is installed.

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

# Modify (CAREFUL — changes library)
beet move "Artist" "Album"                   # Rename files to match current path template
beet write "Artist" "Album"                  # Write DB metadata back to file tags
beet update "Artist" "Album"                 # Read file tags into DB (opposite of write)
beet remove -a "Artist" "Album"              # Remove from DB only (files stay on disk)
```

### Dangerous Commands — NEVER Use Without Approval

```bash
beet remove -d ...     # -d DELETES FILES FROM DISK. If those files came from a
                       # niche source that can't be re-acquired, they're gone forever.
                       # To re-tag, use: beet import --search-id <new-mbid> <path>

beet import -A ...     # Imports "as-is" with no MB match. Everything needs proper matching.

printf 'a\n' | beet import ...  # Blindly accepts ANY match without inspection.
                                # Use the harness instead — it lets you verify MBID and distance.
```

## The Beets SQLite Database

Located at `/mnt/virtio/Music/beets-library.db`. Two main tables:

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

Local mirror on doc2 (`192.168.1.35:5200`). Beets is configured to use this instead of the public MB API.

- **Web UI**: `http://192.168.1.35:5200`
- **API**: `http://192.168.1.35:5200/ws/2/`
- **Replication**: Daily from upstream MetaBrainz at 03:00
- **Rate limit**: 100 req/s (vs 1 req/s on public API)

### API Examples

```bash
# Search for a release
curl -s "http://192.168.1.35:5200/ws/2/release?query=artist:Artist+AND+release:Album&fmt=json"

# Get release with tracks
curl -s "http://192.168.1.35:5200/ws/2/release/MBID?inc=recordings+media&fmt=json"

# Get all releases in a release group
curl -s "http://192.168.1.35:5200/ws/2/release-group/RGID?inc=releases&fmt=json"
```

**Newly seeded releases**: If you seed a release on upstream musicbrainz.org, it won't appear in the local mirror until the next daily replication. Use `--upstream` flag on the harness to query upstream directly for fresh seeds.

## Reimporting / Re-tagging

To change an album's match (e.g., wrong edition, want a different MB release):

```bash
# DON'T do this:
beet remove -d ...          # Deletes files!
beet remove ... && reimport # Loses files if reimport fails

# DO this — re-tag in place:
printf '{"action": "apply", "candidate_id": "NEW-MBID"}\n{"action": "merge"}\n' | \
  ./harness/run_beets_harness.sh --search-id "NEW-MBID" --noincremental "/mnt/virtio/Music/Beets/Artist/Year - Album"
```

The reimport detects that files are already in the library directory and updates the DB entry in place. With `move: true`, files get renamed if the new metadata changes the path template.

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
| `ffmpeg -v error -i FILE -f null -` | All | Full decode — proves audio data is valid | Slow (~18 files/s) |

### Validation Script

`scripts/audio_health_check.py` in the tagging-workspace repo:
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

Beets config is Nix-managed. The cycle:

```bash
# 1. Edit the module
vim /home/abl030/nixosconfig/modules/home-manager/services/beets.nix

# 2. Format and rebuild
cd /home/abl030/nixosconfig
nix fmt
sudo nixos-rebuild switch --flake .#proxmox-vm

# 3. Verify
cat ~/.config/beets/config.yaml     # Check changes landed
beet config                          # Beets' own config dump
beet ls -a | head -5                 # Quick smoke test
```

**IMPORTANT**: Never edit `~/.config/beets/config.yaml` directly — Home Manager will fail with "would be clobbered" on the next rebuild.

## Deploying to doc2

doc2 runs Cratedigger but needs access to beets via the harness. The harness shell wrapper (`run_beets_harness.sh`) bootstraps from doc1's Nix beets environment. Since both machines share `/mnt/virtio`, the harness files are accessible from doc2 without copying.

**NEVER cross-build from doc1** (`--target-host doc2` is slow). Always:
```bash
# Push nixosconfig to GitHub first
cd ~/nixosconfig && git push

# Then build locally on doc2
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
```

## Troubleshooting

### "0 candidates" for every album
The `musicbrainz` plugin is not loaded. Check that `plugins` string includes `musicbrainz` in the beets config.

### Import writes wrong path structure
The `Library()` constructor needs ALL FOUR args: `dbpath, directory, get_path_formats(), get_replacements()`. Without `path_formats` and `replacements`, it uses the hardcoded default `$artist/$album/$track $title` instead of the user's config. The harness handles this correctly.

### "incremental" skips directories
Beets remembers every directory it's imported from. Use `--noincremental` flag on the harness (sets `config["import"]["incremental"] = False`) to re-process.

### Harness hangs with no output
Usually means beets is waiting for a network request (MB lookup, cover art fetch). Check that:
- MB mirror is reachable: `curl -s http://192.168.1.35:5200/ws/2/release?query=test&fmt=json | head`
- No firewall blocking doc2
- Not stuck on chroma fingerprinting (disabled by default, but check `chroma.auto`)

### Files with special characters in paths
**NEVER use bash shell commands** (`ls`, `rm`, `find`, `cd`) on paths containing quotes, ampersands, unicode dashes, or CJK characters. Bash mangles embedded quotes. Always use Python (`os.listdir`, `os.path.exists`, `shutil.rmtree`, `pathlib`).

### "Controller disconnected — aborting"
The harness's stdin closed before it finished reading decisions. This happens when you pipe input but don't provide enough lines for all prompts (e.g., `choose_match` needs one line, then `resolve_duplicate` needs another).

### scrub plugin strips tags from source files
In pretend mode, the harness returns `Action.SKIP` instead of the candidate to prevent beets from calling `apply()`. The old approach (copy=False, move=False, write=False) still let beets write to the DB and run scrub, which corrupted source files.

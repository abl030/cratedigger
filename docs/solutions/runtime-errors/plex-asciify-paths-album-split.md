---
title: "Beets asciify_paths splits ~12% of the Plex library into ghost albums"
date: 2026-05-19
category: runtime-errors
problem_type: data-integrity
component: plex-notifier
tags:
  - plex
  - beets
  - path-rendering
  - asciify_paths
  - unicode
  - mass-rename
related_commits:
  - d227fb55ec96a773402389ac69ee7920906f9813  # nixosconfig: asciify_paths + languages=[en]
---

# Beets asciify_paths splits ~12% of the Plex library into ghost albums

## Context

The user enabled `asciify_paths = true` in beets to transliterate non-Latin
scripts (JP / KR) in rendered filesystem paths. Single config change,
followed by a one-time `beet move` across the whole library to re-render
existing albums. Two days later, a beets DB lookup for a specific album
("Nancy & Lee") showed it in a single folder on disk — but Plex displayed
it as **two separate album entries**. Broad audit across the music
section found this had happened to **~1,200 albums (~12% of the library)**.

## Root cause — Plex scanner can't reconcile mass file renames

Three facts combine:

1. **`asciify_paths` runs `unidecode` on rendered path components only.**
   - Curly apostrophe `’` (U+2019) → straight `'` (U+0027)
   - Ellipsis `…` (U+2026) → `...` (then beets' path sanitizer further
     replaces trailing dots → `.._`)
   - Yen sign `¥` → `Y`, etc.
   - Filenames AND folder names are affected.
2. **Beets DB rows and ID3 tags are NOT touched.** The commit message is
   explicit: "DB and on-disk tags untouched." So `title=Fiona’s Room`
   (curly) persists in the ID3 frame even after the filename becomes
   `07 Fiona's Room.mp3` (straight).
3. **Plex's scanner uses filesystem paths to identify track files.** When
   the scanner re-walks the library and finds a file at a path it doesn't
   recognise, it creates a **new** Plex track row. It doesn't reconcile
   "the file I had at the curly path is the same content as this new file
   at the straight path." For a single file's rename, Plex sometimes
   matches by guid; for a mass rename across thousands of files, it
   doesn't.

What you get:

- The original Plex album row still holds entries for the **old**
  curly-quote filenames (now dead — files no longer exist at those paths).
- The Plex scanner creates a **second** album row for the **new**
  straight-quote filenames. Plex's matcher does not fold this new row
  into the existing one even though `album_artist + album + year` tags
  are identical (the existing row's "slot" for that track number is
  already occupied by the dead entry).
- For albums where the **folder name itself** changed (e.g. `¥$` → `Y=$`,
  `The Academy Is…` → `The Academy Is.._`), the whole ghost album points
  at the old folder path.
- For albums where only some **track filenames** changed (because only
  those tracks had Unicode punctuation in titles), the ghost holds only
  the renamed tracks; the primary keeps the non-Unicode tracks.

So a single `beet move` after `asciify_paths = true` is enabled creates
a **one-shot mass-split** that scales with the number of Unicode-affected
filenames in the library.

## Why "Empty Trash + Clean Bundles" doesn't fix it

The natural first move is to use Plex's UI to clean up dead file
references (Settings → Manage → Libraries → ⋯ → Empty Trash + Clean
Bundles). This **drops the dead curly-path track entries from the
primary album row** but does NOT remove the ghost album row. Worse, on
the rescan it triggers, Plex sometimes **discovers more renamed files**
that prior partial scans missed (especially on remote SMB mounts where
inotify is unreliable — see `docs/plex-primer.md` § Plex's own scan
behaviour) and creates additional ghost rows.

In this incident:

| State | total albums | dup groups | same-folder splits | diff-folder splits |
|-------|-------------|-----------|-------------------|--------------------|
| Pre-cleanup | 9,409 | 1,152 | 714 | 438 |
| After Empty Trash + rescan | 9,454 | 1,241 | 1,178 | 63 |
| After scripted merge | 8,276 | 63 | 0 | 63 (legit edition dupes) |

Empty Trash converted diff-folder splits into same-folder splits (the
ghost row got re-pointed at the new folder once its old folder was
recognised as gone) and surfaced more splits the partial scans had
missed.

## The fix — Plex merge API

Plex exposes a metadata merge endpoint:

```
PUT /library/metadata/{primary_rk}/merge?ids={ghost_rk1},{ghost_rk2},...
```

This re-parents the ghost rows' tracks under the primary, preserving
play counts and ratings on the primary, and deletes the ghost album
rows. Idempotent: re-running on an already-merged ghost returns 404
(harmless).

Two repo scripts:

- `scripts/plex_dupes_audit.py` — fetches all section-3 albums, finds
  duplicate `(artist, title, year)` groups, classifies each as
  `same_folder` (asciify split) or `diff_folder` (legit multi-edition
  OR mid-reconciliation ghost), writes JSON.
- `scripts/plex_dupes_merge.py` — reads the JSON, picks the
  highest-track-count member as primary, merges the rest. Defaults to
  dry-run; `--commit` executes; `--limit N` for smoke testing.

**Usage (homelab-specific URLs and paths):**

```bash
# 1. Audit
mkdir -p /tmp/plex-asciify-cleanup
TOKEN=$(ssh doc2 'sudo cat /run/cratedigger-secrets/PLEX_TOKEN')
curl -sS "https://plex.ablz.au/library/sections/3/all?type=9&X-Plex-Token=$TOKEN" \
  -o /tmp/plex-asciify-cleanup/plex_albums.xml
PLEX_TOKEN=$TOKEN python3 scripts/plex_dupes_audit.py \
  /tmp/plex-asciify-cleanup/plex_albums.xml \
  > /tmp/plex-asciify-cleanup/dupes.json

# 2. Dry-run review
PLEX_TOKEN=$TOKEN python3 scripts/plex_dupes_merge.py \
  /tmp/plex-asciify-cleanup/dupes.json

# 3. Smoke test on one group
PLEX_TOKEN=$TOKEN python3 scripts/plex_dupes_merge.py \
  /tmp/plex-asciify-cleanup/dupes.json --commit --limit 1

# 4. Full run
PLEX_TOKEN=$TOKEN python3 scripts/plex_dupes_merge.py \
  /tmp/plex-asciify-cleanup/dupes.json --commit

# 5. Re-audit; expect dup_groups to drop to the legit-edition floor
curl -sS "https://plex.ablz.au/library/sections/3/all?type=9&X-Plex-Token=$TOKEN" \
  -o /tmp/plex-asciify-cleanup/plex_albums.final.xml
PLEX_TOKEN=$TOKEN python3 scripts/plex_dupes_audit.py \
  /tmp/plex-asciify-cleanup/plex_albums.final.xml \
  > /tmp/plex-asciify-cleanup/dupes.final.json
```

In this incident: 1,178 merges issued, all returned HTTP 200, 0 failures
(1 idempotent 404 for the smoke-test group on the full run — expected).
Resulting state: 8,276 distinct albums, 63 residual dup groups — all
manually verified as **legitimate multi-edition pressings** (Aphex Twin
Drukqs 2001 [Vertigo] vs 2018 reissue, Arcade Fire Funeral [V2 Japan] vs
[Merge Records], etc.). These match cratedigger invariant #5 ("Multiple
editions/pressings of the same album are intentional. NEVER delete or
merge.") and are left alone.

## Why a fresh-scan would also work, but you don't want it

If you nuked Plex section 3 and re-added it from scratch:

- Plex walks the asciified filesystem fresh.
- Each file's tags (still curly) determine album grouping.
- All Nancy & Lee tracks share `album=Nancy & Lee`, `album_artist=Nancy
  Sinatra, Lee Hazlewood`, `year=2022` → one album row, 13 tracks.
- No splits anywhere.

The mismatch between curly-tag titles and straight-path filenames is
**harmless to Plex's grouping** because Plex groups by tag, not by path.
The split bug is purely a property of Plex's scanner failing to
reconcile mass path renames during a *transition*, not a property of the
final tag-vs-path divergence.

Don't do this in practice — you'd lose play counts, ratings, "added at"
history, and watch state. The merge API approach preserves all of that.

## Guidance

### Before enabling any beets option that affects path rendering

Anything that mutates the rendered path of existing files — `asciify_paths`,
new `paths:` templates, changed `path_sep_replace`, etc. — is a
**path-rename event** as far as Plex is concerned. On a remote SMB-mounted
library, Plex's scanner WILL split affected albums into ghost rows when
it eventually re-walks them.

If you change one of these settings:

1. Decide whether to flip the setting OR run `beet move` to back-render
   existing files — these are independent decisions. If you only flip the
   setting, future imports use the new rendering and existing files stay
   put (no split). If you `beet move`, you accept the one-shot mass-split
   cost.
2. If you accept the cost, **plan to run `scripts/plex_dupes_audit.py` +
   `plex_dupes_merge.py` after the next full Plex scan completes** to
   collapse the splits. Don't expect Plex's Empty Trash to fix it —
   that operation makes the inventory worse, not better.
3. Re-running the audit after every subsequent Plex full scan (daily by
   default) catches any albums whose splits weren't visible during the
   initial cleanup.

### Why fixing the tags wouldn't have helped either

You might think: "just `beet write` to push asciified tags into the ID3
frames so Plex's matcher sees identical tag signatures." Two problems:

1. **Beets `write` only flushes fields where the DB differs from the
   file.** Asciify only rendered paths; the DB title field still holds
   the curly version. `beet write -p` against a freshly-asciified library
   shows zero pending changes for title/album/artist fields. To actually
   write asciified tags you'd need to unidecode the DB rows first
   (no built-in command), which diverges your DB from canonical
   MusicBrainz metadata — a worse problem than the Plex splits.
2. **`mbsync` re-fetching from MusicBrainz returns the same curly
   titles.** MB's house style stores curly apostrophes; aliases follow
   the same convention. `mbsync` won't normalise. (`mbsync` with
   `import.languages=["en"]` IS useful for non-Latin → English alias
   translation on JP/KR releases — but that's orthogonal to the
   apostrophe split.)

The merge API is the right tool: keep tags as canonical MB metadata,
keep paths asciified for filesystem ergonomics, paper over Plex's
scanner deficiency with one operator action.

## When to apply

- Flipping `asciify_paths`, `path_sep_replace`, or any beets `paths:`
  template change that mutates rendered paths.
- Running `beet move` across a non-trivial slice of the library after any
  config change.
- Reporting "Plex shows two of every album" or "album split across two
  entries" after a beets config change.
- Periodically as a hygiene audit — running `plex_dupes_audit.py` is
  free and read-only.

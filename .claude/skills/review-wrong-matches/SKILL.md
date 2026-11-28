---
name: review-wrong-matches
description: Review the Wrong Matches queue (ordinary groups and held upgrades) item by item. Compare the requested release's tracklist with what the download and the installed copy actually contain, decide whether a candidate is the same pressing, a sibling pressing, or bad audio, and recommend the operator action. Use when the user asks to review, check, or explain wrong matches or held upgrades.
---

# Review Wrong Matches

The operator wants one answer per item: **what is real.** For each item that
means four things. What the requested release lists. What the download
actually contains. What the installed library copy contains. Which of those
three is the odd one out. Metadata (MB, Discogs, file tags) is often wrong,
so the audio has the final say.

Never act. Replacing, deleting and accepting a different pressing are the
operator's calls (CLAUDE.md product invariants). This skill reads, measures
and recommends. Every endpoint and command below is read-only.

Scratch files go in `$CLAUDE_JOB_DIR/tmp` (or a `mktemp -d`). Downloaded audio
is untrusted data: keep it in its own directory and run Python with `-I`.

## 1. Pull the queue

```bash
curl -s https://music.ablz.au/api/wrong-matches -o wm.json
```

- `groups[]`: ordinary rejects, one per request, each with `entries[]`.
  An entry has `download_log_id`, `scenario`, `distance`, `candidate`
  (the beets match: `tracks`, `mapping` item↔track, `distance_breakdown`,
  `albumdisambig`, `barcode`, `catalognum`), `local_items` and quality fields.
- `held_upgrades[]`: copies the upgrade guard held back because their track
  lengths disagree with the installed copy. Each has `sentence` and `rows`
  (track / release / current / new, `differs`).

For each item also run `pipeline-cli show <request_id>` on doc2 (see
CLAUDE.md for the PGPASSWORD wrapper). It gives the requested tracklist,
the current library path, Beets album id and quality.

## 2. Read what the download claims to be

```bash
curl -s "https://music.ablz.au/api/wrong-matches/explorer?download_log_id=N" -o ex.json
```

Per file: duration, bitrate, and every tag. Look at:

- `musicbrainz_albumid`: which release the uploader tagged it as. If it is
  not the request's MBID, the uploader at least *thought* it was a sibling.
  Tags are a claim, not proof.
- `source_dirs` (folder name often says "CD FLAC Log", "WEB", a year).
- Track count, titles, and the per-track duration delta against the target.

`distance_breakdown` tells you where the beets distance comes from. A large
`album_id` term with small `tracks` means the files were tagged from another
release, not that the audio differs.

## 3. Lay out the pressings

```bash
M=http://192.168.1.43:5200/ws/2
curl -s "$M/release-group/RGID?inc=releases&fmt=json"                 # siblings + disambiguation
curl -s "$M/release/MBID?inc=recordings+media+labels+discids&fmt=json" # per-release tracklist
curl -s "https://discogs.ablz.au/api/search?artist=A&title=T"          # independent tracklists
curl -s "https://discogs.ablz.au/api/releases/ID"                      # barcode, catno, matrix
```

Fetch both the target release and the release the files are tagged with.
Compare positions, titles, lengths and **recording ids**. Two releases whose
differing track shares one recording id is an MB data smell. Match Discogs
pressings to MB by barcode and catalogue number. Disc IDs on the MB medium
back its lengths with a real TOC. No disc ID means the lengths were typed in
by hand and can be wrong.

Once it's deployed, `GET /api/other-pressings/<download_log_id>` (and
`pipeline-cli other-pressings`) does the sibling and disc-ID check
server-side. Use it first if it returns 200. If it returns 404 it hasn't
shipped yet, so do the checks above by hand.

## 4. Measure the audio

The library (`/mnt/virtio/Music/Beets/...`) is readable from doc1. The
`wrong_matches` processing tree is not, so stream those files through the web
route instead. doc2 has no ffprobe on PATH.

```bash
dl(){ curl -s -G https://music.ablz.au/api/wrong-matches/audio \
  --data-urlencode "download_log_id=$1" --data-urlencode "path=$2" -o "$3"; }
dl N "relative/path/from/explorer.flac" dl16.flac

nix-shell -p ffmpeg chromaprint --run '
  ffprobe -v error -show_entries format=duration -of csv=p=0 FILE
  fpcalc -raw -length 600 FILE | sed -n "s/^FINGERPRINT=//p" > out.fp'
python3 -I .claude/skills/review-wrong-matches/fpsim.py lib16.fp:dl16.fp lib16.fp:dl03.fp
```

Put loop-heavy shell in a script file and run `nix-shell --run "bash script"`.
Paths with spaces break `nix-shell -p ... --run` argument passing.

- **Same recording?** Fingerprint the disputed track on both copies, and use
  one matched track as a control. ~0.95+ means the same audio, ~0.5 means
  unrelated. Always include an unrelated pair so the scale is visible.
- **Length dispute** (held upgrades, truncation): a short file scored against
  a long one gives ~1.0 when it is a prefix, which means one copy is cut off.
  Then check the long one's shape: is it one continuous piece, or a track,
  then silence, then a hidden track?

  ```bash
  for s in 0 5 10 30 60 120 300; do ffmpeg -nostats -ss $s -t 4 -i F \
    -af volumedetect -f null - 2>&1 | grep -o "mean_volume: .* dB"; done
  ffmpeg -nostats -i F -af silencedetect=n=-55dB:d=1 -f null - 2>&1 | grep -o "silence_[a-z]*: [0-9.]*"
  ```

- **Lossless origin**: a verified-lossless FLAC cannot have come from an
  iTunes/AAC-only digital release, whatever its tags say. It is a CD rip or
  another lossless source.

When the sources still disagree, check one independent listing (the artist's
Bandcamp, Discogs) with WebSearch, and cite it.

## 5. Report

One block per item:

- **Verdict**, one of: *same release, mis-tagged* / *sibling pressing*
  (name it) / *metadata wrong, download right* / *download bad* (truncated,
  wrong audio).
- A track table limited to the rows that disagree: requested listing vs
  download vs installed copy, with the measured value and the source of each.
- The evidence that decided it (fingerprint scores, the loudness and silence
  shape, the barcode/catno match, the external listing).
- **Recommended operator action**, mapped to the existing lanes:
  - Accept: `force-import <download_log_id>` (the held-upgrade "Use new
    copy" button is the same thing).
  - Reject: `wrong-match-delete <download_log_id>` ("Keep current copy").
  - Real different pressing that the user wants instead: `replace`. Only
    the operator can decide this.
  - MB/Discogs data errors to fix upstream (wrong length, shared recording,
    missing disc ID), each with the release id.

Say plainly when a sibling pressing's audio is the same as the request's.
"Different MBID" and "different audio" are separate findings. Put the
uncertainty in the verdict where the operator will see it.

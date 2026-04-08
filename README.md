# Soularr (abl030 fork)

A Soulseek download engine for music libraries, driven by a PostgreSQL pipeline database. Searches Soulseek via [slskd](https://github.com/slskd/slskd), validates downloads against [MusicBrainz](https://musicbrainz.org/) via [beets](https://beets.io/), auto-imports to a beets library with quality verification, or stages for manual review.

Originally forked from [mrusse/soularr](https://github.com/mrusse/soularr). This fork has diverged significantly: the pipeline DB is the sole source of truth, and a web UI at `music.ablz.au` is the album picker.

## How it works

```
Web UI / CLI                 slskd (Soulseek)           beets
      |                            |                       |
      |  add album                 |                       |
      v                            |                       |
Pipeline DB (PostgreSQL)           |                       |
      |                            |                       |
      |  Phase 1: poll_active_downloads()                  |
      |    check status of previous downloads              |
      |    complete/timeout/retry                          |
      |                            |                       |
      |  Phase 2: get_wanted()     |                       |
      |    search + enqueue ------>|                       |
      |    set status=downloading  |  download (async)     |
      |    return immediately      |<-----------           |
      |                            |                       |
      |  (next 5-min cycle)        |                       |
      |    poll sees completion    |                       |
      |    validate against MBID --|---------------------->|
      |                            |                       |
      |  source=request            |                       |
      |    spectral analysis       |                       |
      |    FLAC->V0 conversion     |                       |
      |    quality gate            |  auto-import -------->| -> /Beets
      |                            |                       |
      |  source=redownload         |                       |
      |    stage to /Incoming      |  (manual review)      |
      |                            |                       |
      |  ImportResult + ValidationResult JSON               |
      |<---------------------------------------------------+
```

## Quality decision pipeline

All quality decisions are pure functions in `lib/quality.py` with full unit test coverage:

1. **`spectral_import_decision()`** -- Pre-import: should we import this CBR download?
2. **`import_quality_decision()`** -- Is this an upgrade or downgrade? Genuine FLAC->V0 always wins.
3. **`transcode_detection()`** -- Post-FLAC-conversion: was the FLAC a transcode?
4. **`quality_gate_decision()`** -- Post-import: accept, or re-queue for better quality?
5. **`determine_verified_lossless()`** -- Single source of truth for verified lossless status.

## Audit trail

Every download stores two JSONB blobs in `download_log` for complete auditability:

**`import_result`** -- from `import_one.py` via `ImportResult` dataclass:
- Decision (import/downgrade/transcode_upgrade/transcode_downgrade/error)
- Per-track spectral analysis (grade, HF deficit, cliff detection per track)
- Conversion details (FLAC->V0 or configurable target format, post-conversion bitrate)
- Quality comparison (new vs existing bitrate, verified_lossless flag)
- Postflight verification (beets ID, track count, imported path)

**`validation_result`** -- from `beets_validate()` via `ValidationResult` dataclass:
- Full beets candidate list with distance breakdown per component (album, artist, tracks, media, source, year...)
- Track mapping: which local file matched which MusicBrainz track
- Local file list (path, title, bitrate, format) vs MB track list (title, length, track_id)
- Beets recommendation confidence level
- Soulseek username, download folder, failed_path (used by force-import), denylisted users, corrupt files

```sql
-- Why was this rejected?
SELECT validation_result->'candidates'->0->'distance_breakdown',
       import_result->>'decision',
       import_result->'spectral'->>'grade'
FROM download_log WHERE id = <id>;

-- Which tracks matched?
SELECT m->'item'->>'title' as local, m->'track'->>'title' as mb
FROM download_log, jsonb_array_elements(validation_result->'candidates'->0->'mapping') AS m
WHERE id = <id>;
```

All types are fully typed dataclasses with pyright enforcement and JSON round-trip serialization:
`ImportResult`, `ValidationResult`, `CandidateSummary`, `HarnessItem`, `HarnessTrackInfo`, `TrackMapping`, `DownloadInfo`, `SpectralContext`, `AlbumInfo`, `ActiveDownloadState`, `ActiveDownloadFileState`.

## Pipeline CLI diagnostics

`pipeline-cli` already has the pipeline DB connection configured, so use it for ad-hoc debugging instead of hand-rolling `psql` or Python one-offs.

```bash
# Inline SQL (runs in a read-only DB session)
pipeline-cli query "SELECT id, status, artist_name, album_title FROM album_requests WHERE status = 'wanted' LIMIT 5"

# Multi-line SQL without shell quoting
pipeline-cli query - <<'SQL'
SELECT id, artist_name, album_title, min_bitrate, current_spectral_bitrate
FROM album_requests
WHERE current_spectral_bitrate IS NOT NULL
ORDER BY updated_at DESC
LIMIT 10
SQL

# JSON output for scripting
pipeline-cli query --json "SELECT id, outcome, import_result FROM download_log ORDER BY id DESC LIMIT 3"
```

## What's different from upstream

- **PostgreSQL pipeline DB** replaces Lidarr as the source of truth
- **Web UI** (`music.ablz.au`) for browsing MusicBrainz and adding albums
- **Beets validation** -- every download validated against target MusicBrainz release ID
- **Auto-import** with FLAC->V0 conversion (or configurable target: Opus, MP3 V2, AAC, FLAC on disk), spectral analysis, quality gating
- **Async downloads** -- non-blocking: enqueue downloads, persist state to DB, poll on next run. Downloads span multiple 5-minute cycles. No more blocking `while True` loop.
- **Parallel Soulseek searches** -- `ThreadPoolExecutor` fires all searches concurrently, ~2x speedup (see `docs/parallel-search.md`)
- **Typed decision pipeline** -- pure functions in `quality.py`, typed dataclasses throughout
- **Full audit trail** -- every decision stored as queryable JSONB in PostgreSQL
- **Centralized beets queries** -- `BeetsDB` class in `lib/beets_db.py`
- **Force-import** -- manually import rejected downloads via CLI (`force-import <id>`) or web API
- **Comprehensive test suite** (`nix-shell --run "bash scripts/run_tests.sh"`)

## MusicBrainz mirror

All MusicBrainz lookups hit a local mirror at `http://192.168.1.35:5200` (doc2), not the public API. This avoids rate limits and provides sub-second response times. The mirror runs [musicbrainz-docker](https://github.com/metabrainz/musicbrainz-docker) and replicates nightly.

The web UI (`web/mb.py`) and beets both query this mirror. Beets is configured with `musicbrainz.host: 192.168.1.35:5200`.

```bash
# Search releases
curl -s "http://192.168.1.35:5200/ws/2/release?query=artist:radiohead+AND+release:ok+computer&fmt=json"

# Get release with tracks
curl -s "http://192.168.1.35:5200/ws/2/release/<MBID>?inc=recordings+media&fmt=json"
```

## Verified lossless target format

After verifying a FLAC download is genuine (via spectral analysis + V0 conversion), the pipeline can convert to a configurable target format instead of keeping V0. Set in `config.ini`:

```ini
[Beets Validation]
# Target format after verified lossless. Empty = keep V0 (default).
# The V0 conversion always runs first as a verification step.
# Examples: "opus 128", "opus 96", "mp3 v2", "mp3 192", "aac 128"
verified_lossless_target = opus 128
```

Supported formats:

| Config value | ffmpeg codec | Output | Notes |
|---|---|---|---|
| `opus 128` | libopus VBR 128kbps | `.opus` | ~half the bitrate of V0 at equivalent quality |
| `opus 96` | libopus VBR 96kbps | `.opus` | Good for space-constrained libraries |
| `mp3 v0` | LAME VBR quality 0 | `.mp3` | Same as default (no target needed) |
| `mp3 v2` | LAME VBR quality 2 | `.mp3` | ~190kbps, smaller than V0 |
| `mp3 192` | LAME CBR 192kbps | `.mp3` | Fixed bitrate |
| `aac 128` | AAC VBR 128kbps | `.m4a` | Apple ecosystem |
| *(empty)* | *(none)* | `.mp3` | Keep V0 — the default |

The V0 verification step always runs first regardless of target format. The V0 bitrate proves the FLAC was genuine lossless (>210kbps = genuine, <210kbps = transcode). Only after verification does the target conversion run from the original FLAC. If the FLAC is a transcode, the target conversion is skipped and V0 is kept.

## Running tests

```bash
nix-shell --run "bash scripts/run_tests.sh"    # full suite, saves to /tmp/soularr-test-output.txt
nix-shell --run "python3 -m unittest tests.<module> -v"  # single module
```

## Deployment

Deployed via NixOS. The NixOS module builds a Python environment with dependencies and runs Soularr as a systemd oneshot on a 5-minute timer.

## Credits

- **Original Soularr**: [Michael Russell](https://github.com/mrusse) -- [mrusse/soularr](https://github.com/mrusse/soularr)
- **Libraries**: [slskd-api](https://github.com/bigoulours/slskd-python-api), [music-tag](https://github.com/KristoforMaynworWormo/music-tag), [psycopg2](https://www.psycopg.org/)

## License

[MIT](LICENSE) (same as upstream)

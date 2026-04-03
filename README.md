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
5. **`is_verified_lossless()`** -- Was this imported from a spectral-verified genuine FLAC?

## Audit trail

Every download stores two JSONB blobs in `download_log` for complete auditability:

**`import_result`** -- from `import_one.py` via `ImportResult` dataclass:
- Decision (import/downgrade/transcode_upgrade/transcode_downgrade/error)
- Per-track spectral analysis (grade, HF deficit, cliff detection per track)
- Conversion details (FLAC->V0, post-conversion bitrate)
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

## What's different from upstream

- **PostgreSQL pipeline DB** replaces Lidarr as the source of truth
- **Web UI** (`music.ablz.au`) for browsing MusicBrainz and adding albums
- **Beets validation** -- every download validated against target MusicBrainz release ID
- **Auto-import** with FLAC->V0 conversion, spectral analysis, quality gating
- **Async downloads** -- non-blocking: enqueue downloads, persist state to DB, poll on next run. Downloads span multiple 5-minute cycles. No more blocking `while True` loop.
- **Parallel Soulseek searches** -- `ThreadPoolExecutor` fires all searches concurrently, ~2x speedup (see `docs/parallel-search.md`)
- **Typed decision pipeline** -- pure functions in `quality.py`, typed dataclasses throughout
- **Full audit trail** -- every decision stored as queryable JSONB in PostgreSQL
- **Centralized beets queries** -- `BeetsDB` class in `lib/beets_db.py`
- **Force-import** -- manually import rejected downloads via CLI (`force-import <id>`) or web API
- **920+ tests** including spectral analysis with real audio fixtures and live slskd integration tests

## MusicBrainz mirror

All MusicBrainz lookups hit a local mirror at `http://192.168.1.35:5200` (doc2), not the public API. This avoids rate limits and provides sub-second response times. The mirror runs [musicbrainz-docker](https://github.com/metabrainz/musicbrainz-docker) and replicates nightly.

The web UI (`web/mb.py`) and beets both query this mirror. Beets is configured with `musicbrainz.host: 192.168.1.35:5200`.

```bash
# Search releases
curl -s "http://192.168.1.35:5200/ws/2/release?query=artist:radiohead+AND+release:ok+computer&fmt=json"

# Get release with tracks
curl -s "http://192.168.1.35:5200/ws/2/release/<MBID>?inc=recordings+media&fmt=json"
```

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

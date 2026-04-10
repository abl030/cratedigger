# Cratedigger

A quality-obsessed music acquisition pipeline. Searches Soulseek via [slskd](https://github.com/slskd/slskd), validates downloads against [MusicBrainz](https://musicbrainz.org/) via [beets](https://beets.io/), and curates a library toward verified lossless sources — automatically.

Cratedigger doesn't just download albums. It siphons the best available quality out of Soulseek over time: downloading, verifying via spectral analysis, converting, comparing against what's already on disk, and re-queuing for upgrades when better sources appear.

> This project was originally inspired by [mrusse/soularr](https://github.com/mrusse/soularr), a clever script that connected Lidarr with Soulseek. Cratedigger has since diverged into its own thing — PostgreSQL pipeline DB, beets validation, spectral quality verification, async downloads, a web UI — but the original idea of bridging Soulseek into a music library workflow came from mrusse's work. Thank you.
>
> If you'd like to support the original author: [buy mrusse a coffee](https://ko-fi.com/mrusse).

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

## Features

- **PostgreSQL pipeline DB** as the sole source of truth for album requests, download state, and quality history
- **Web UI** (`music.ablz.au`) for browsing a local MusicBrainz mirror and adding albums
- **Beets validation** -- every download validated against the target MusicBrainz release ID before import
- **Auto-import** with FLAC->V0 conversion (or configurable target: Opus, MP3 V2, AAC, FLAC on disk), spectral analysis, and quality gating
- **Async downloads** -- non-blocking: enqueue downloads, persist state to DB, poll on next run. Downloads span multiple 5-minute cycles.
- **Parallel Soulseek searches** -- `ThreadPoolExecutor` fires all searches concurrently
- **Spectral quality verification** -- sox-based transcode detection catches fake FLACs and upsampled MP3s
- **Quality upgrade system** -- automatically re-queues albums when better sources become available. CBR -> lossless -> verified V0.
- **User cooldowns** -- global, temporary cooldowns for Soulseek users who consistently timeout or fail (5 consecutive failures = 3-day cooldown)
- **Force-import** -- manually import rejected downloads via CLI or web API
- **Full audit trail** -- every decision stored as queryable JSONB in PostgreSQL
- **Typed decision pipeline** -- pure functions in `quality.py`, typed dataclasses throughout, pyright enforced
- **Comprehensive test suite** -- 1400+ tests (`nix-shell --run "bash scripts/run_tests.sh"`) with a 4-category taxonomy (pure / seam / orchestration / integration slice), shared `FakePipelineDB`/`FakeSlskdAPI` fakes, builders for typed data, and a route contract audit guard that fails at test time if a new web endpoint is added without frontend contract coverage

## Quality decision pipeline

All quality decisions are pure functions in `lib/quality.py` with full unit test coverage:

1. **`spectral_import_decision()`** -- Pre-import: should we import this CBR download?
2. **`import_quality_decision()`** -- Is this an upgrade or downgrade? Genuine FLAC->V0 always wins.
3. **`transcode_detection()`** -- Post-FLAC-conversion: was the FLAC a transcode?
4. **`quality_gate_decision()`** -- Post-import: accept, or re-queue for better quality?
5. **`determine_verified_lossless()`** -- Single source of truth for verified lossless status.
6. **`should_cooldown()`** -- Global user cooldown: skip users with 5+ consecutive failures for 3 days.

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
`ImportResult`, `ValidationResult`, `CandidateSummary`, `HarnessItem`, `HarnessTrackInfo`, `TrackMapping`, `DownloadInfo`, `SpectralContext`, `AlbumInfo`, `ActiveDownloadState`, `ActiveDownloadFileState`, `CooldownConfig`.

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

## MusicBrainz mirror

All MusicBrainz lookups hit a local mirror at `http://192.168.1.35:5200` (doc2), not the public API. This avoids rate limits and provides sub-second response times. The mirror runs [musicbrainz-docker](https://github.com/metabrainz/musicbrainz-docker) and replicates nightly.

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

The test layer follows a 4-category taxonomy documented in `.claude/rules/code-quality.md`:

- **Pure function tests** — direct input → output, exhaustive subTest tables for decision matrices
- **Seam tests** — interface boundaries (subprocess argv, route contract fields, SQL shape)
- **Orchestration tests** — assert domain state via `FakePipelineDB`/`FakeSlskdAPI`, not mock call shapes
- **Integration slices** — real code paths in `tests/test_integration_slices.py`, minimal patching, required for every high-risk orchestration boundary

Shared infrastructure lives in `tests/fakes.py` (stateful fakes) and `tests/helpers.py` (typed data builders + the `patch_dispatch_externals()` context manager). New web routes must be classified in `TestRouteContractAudit.CLASSIFIED_ROUTES` — the suite fails at test time if a route is added without contract coverage.

## Deployment

Deployed via NixOS. The NixOS module builds a Python environment with dependencies and runs Cratedigger as a systemd oneshot on a 5-minute timer.

## Credits

This project grew out of [mrusse/soularr](https://github.com/mrusse/soularr) by [Michael Russell](https://github.com/mrusse) -- the original idea of bridging Soulseek into a music library workflow. If you appreciate that idea, [buy mrusse a coffee](https://ko-fi.com/mrusse).

**Libraries**: [slskd-api](https://github.com/bigoulours/slskd-python-api), [music-tag](https://github.com/KristoforMaynworWormo/music-tag), [psycopg2](https://www.psycopg.org/), [beets](https://beets.io/)

## License

[MIT](LICENSE)

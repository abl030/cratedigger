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
      |  get_wanted()              |                       |
      |-----> search ------------->|                       |
      |                            |  download             |
      |                            |<-----------           |
      |                            |                       |
      |    validate against target MBID ------------------>|
      |                            |                       |
      |  source=request            |                       |
      |    spectral analysis       |                       |
      |    FLAC->V0 conversion     |                       |
      |    quality gate            |  auto-import -------->| -> /Beets
      |                            |                       |
      |  source=redownload         |                       |
      |    stage to /Incoming      |  (manual review)      |
      |                            |                       |
      |  mark_done() + ImportResult JSON                   |
      |<---------------------------------------------------+
```

## Quality decision pipeline

All quality decisions are pure functions in `lib/quality.py` with full unit test coverage:

1. **`spectral_import_decision()`** -- Pre-import: should we import this CBR download? Compares spectral analysis of new files vs existing.
2. **`import_quality_decision()`** -- Is this an upgrade or downgrade? Genuine FLAC->V0 always wins.
3. **`transcode_detection()`** -- Post-FLAC-conversion: was the FLAC a transcode? (V0 bitrate < 210kbps = fake)
4. **`quality_gate_decision()`** -- Post-import: accept, or re-queue for better quality?
5. **`is_verified_lossless()`** -- Was this imported from a spectral-verified genuine FLAC?

`import_one.py` emits a typed `ImportResult` JSON blob on stdout. The full JSON is stored in `download_log.import_result` (JSONB) for complete auditability:

```sql
SELECT import_result->>'decision',
       import_result->'quality'->>'new_min_bitrate',
       import_result->'spectral'->>'grade'
FROM download_log ORDER BY id DESC LIMIT 10;
```

## What's different from upstream

This fork is a significant rewrite:

- **PostgreSQL pipeline DB** replaces Lidarr as the source of truth
- **Web UI** (`music.ablz.au`) for browsing MusicBrainz and adding albums
- **Beets validation** -- every download validated against target MusicBrainz release ID
- **Auto-import** with FLAC->V0 conversion, spectral analysis, quality gating
- **Typed decision pipeline** -- pure functions in `quality.py`, typed dataclasses throughout (`ImportResult`, `DownloadInfo`, `SpectralContext`, `AlbumInfo`)
- **Centralized beets queries** -- `BeetsDB` class in `lib/beets_db.py`
- **421 tests** including spectral analysis with real audio fixtures and live slskd integration tests

## Running tests

```bash
nix-shell --run "python3 -m unittest discover tests -v"
```

## Deployment

Deployed via NixOS. The NixOS module builds a Python environment with dependencies and runs Soularr as a systemd oneshot on a 5-minute timer.

## Credits

- **Original Soularr**: [Michael Russell](https://github.com/mrusse) -- [mrusse/soularr](https://github.com/mrusse/soularr)
- **Libraries**: [slskd-api](https://github.com/bigoulours/slskd-python-api), [music-tag](https://github.com/KristoforMaynworWormo/music-tag), [psycopg2](https://www.psycopg.org/)

## License

[MIT](LICENSE) (same as upstream)

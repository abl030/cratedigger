# Audio Quality Type System — Tracking Doc

Created 2026-04-03. Tracks the AudioFileSpec refactoring and planned extensions.

## What was done (2026-04-03)

### AudioFileSpec dataclass (`lib/quality.py`)
- Frozen dataclass: `codec`, `extension`, `quality`, audio metadata, `lossless` property
- Extension-to-codec mapping tables (`_EXT_TO_CODEC`, `_CONFIG_NAME_TO_CODEC`, `CODEC_TO_EXT`)
- `_m4a_codec_heuristic()` — disambiguates ALAC vs AAC (bitrate >700 or bitDepth → ALAC)
- `parse_filetype_config()` / `file_identity()` / `filetype_matches()` — typed API
- `verify_filetype()` rewritten as thin bridge
- `CATCH_ALL_SPEC` sentinel, `"*"` / `"any"` config support

### ALAC/m4a fix
- `.m4a` files now match `alac` config (was broken: `"m4a" != "alac"`)
- `album_track_num()`, `album_match()`, `download_filter()` all use `spec.extension`
- `SoularrConfig.allowed_specs` property added

### Consolidated AUDIO_EXTENSIONS
- Replaced 8 duplicate `_AUDIO_EXTS` sets across the codebase with imports from `lib/quality.py`
- `AUDIO_EXTENSIONS` (bare) and `AUDIO_EXTENSIONS_DOTTED` (with dot prefix)

### Generalized lossless conversion
- `convert_lossless_to_v0()` handles FLAC, ALAC (.m4a), WAV
- `is_verified_lossless()` accepts any lossless extension
- Note: sox can't read .m4a, so spectral analysis skips ALAC. Transcode detection
  falls back to bitrate threshold. verified_lossless still works via the override path
  in import_one.py (`will_be_verified_lossless = converted > 0 and not is_transcode`).

### Catch-all fallback
- `find_download()` retries with `"*"` when normal filetypes fail and no search_filetype_override
- `try_enqueue()` / `try_multi_enqueue()` merge all cached dirs for catch-all
- `album_track_num()` counts all audio files without same-extension constraint

### Tests
- 93 tests in `tests/test_audio_file_spec.py`
- 3 new tests in `tests/test_quality_decisions.py` (ALAC/WAV verified lossless)
- 878 total tests passing

## Done: Known issues cleanup (2026-04-03, commits 9ef4fc6, cf8d6a1)

All three issues resolved:
1. `_try_filetype` helper extracted — no more catch-all loop duplication
2. `verify_filetype` uses pre-parsed specs via `cfg.allowed_specs`
3. `allowed_specs` cached via `_allowed_specs` tuple in `__post_init__`

## Done: AudioQualityMeasurement (2026-04-03)

Frozen dataclass in `lib/quality.py` representing ground truth about a set of audio files.

```python
@dataclass(frozen=True)
class AudioQualityMeasurement:
    min_bitrate_kbps: int | None = None
    is_cbr: bool = False
    spectral_grade: str | None = None
    spectral_bitrate_kbps: int | None = None
    verified_lossless: bool = False
    was_converted_from: str | None = None
```

### What changed
- `import_quality_decision(new: AQM, existing: AQM | None, is_transcode)` — was 5 scalar params
- `quality_gate_decision(current: AQM)` — was 4 scalar params
- `override_min_bitrate` concept moved to callers — they construct `existing` with resolved bitrate
- All callers updated: `quality_decision_stage()`, `_check_quality_gate()`, `full_pipeline_decision()`
- Decision tree metadata updated for web UI
- 879 tests passing, pyright 0 errors

## Done: Measurements on ImportResult (2026-04-03)

QualityInfo deleted, SpectralInfo → SpectralDetail (per-track only). ImportResult now
carries `new_measurement` and `existing_measurement` (AudioQualityMeasurement objects).
The same type flows through decision functions AND the audit trail.

### What changed
- QualityInfo fields moved: bitrates → measurements, process data → ConversionInfo
- SpectralInfo slimmed: grade/bitrate → measurements, per_track stays on SpectralDetail
- `from_dict` migrates v1 JSONB on read (old rows still deserialize)
- `_extract_import_fields` (web) and `pipeline_cli.py` handle both v1/v2 formats
- 881 tests passing, pyright 0 errors

### Next: AudioQualityState
The accumulated quality posture on `album_requests`. Would replace the scattered columns
(min_bitrate, prev_min_bitrate, verified_lossless, spectral_grade, spectral_bitrate,
on_disk_spectral_*). The measurement type is now proven in production — this is the
natural next step when needed.

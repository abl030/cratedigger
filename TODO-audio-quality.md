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
- `find_download()` retries with `"*"` when normal filetypes fail and no quality_override
- `try_enqueue()` / `try_multi_enqueue()` merge all cached dirs for catch-all
- `album_track_num()` counts all audio files without same-extension constraint

### Tests
- 93 tests in `tests/test_audio_file_spec.py`
- 3 new tests in `tests/test_quality_decisions.py` (ALAC/WAV verified lossless)
- 878 total tests passing

## Known issues to clean up

### 1. Catch-all loop duplication in find_download
The catch-all fallback copy-pastes ~50 lines of the normal filetype enqueue loop.
Extract into a helper: `_try_filetype(album, results, filetype, grab_list) -> bool`.

### 2. verify_filetype bridge overhead
Creates two `AudioFileSpec` objects per call. Called in a tight loop during search
result caching (all files x all filetypes). Options:
- Pre-parse `cfg.allowed_filetypes` into specs once, pass to the caching loop
- Or make `verify_filetype` use the pre-parsed specs directly

### 3. allowed_specs is an uncached property
`SoularrConfig` is frozen, so `@property` re-parses on every access. Since config
is immutable, compute once at init time using `__post_init__` or a module-level cache.

## Next: AudioQualityMeasurement

The second natural dataclass from the analysis. Represents "what we actually measured
about a set of audio files" — ground truth from ffprobe + spectral.

```python
@dataclass(frozen=True)
class AudioQualityMeasurement:
    min_bitrate_kbps: int
    is_cbr: bool
    spectral_grade: str | None
    spectral_bitrate_kbps: int | None
    verified_lossless: bool
    was_converted_from: str | None  # "flac", "m4a", None
```

### Why
- Quality decision functions currently take 4-5 loose scalar parameters
- Debugging means pulling values from JSONB and mentally mapping to function args
- Every decision is comparing two measurements (new vs existing)
- The measurement IS the audit trail — log it and you have complete state

### Where it simplifies
- `import_quality_decision(new: Measurement, existing: Measurement | None)` — replaces 5 params
- `quality_gate_decision(current: Measurement)` — replaces 4 params
- `ImportResult` sub-objects (`QualityInfo`, `SpectralInfo`, `ConversionInfo`) collapse into one
- download_log JSONB becomes a serialized measurement — queryable, self-documenting

### Third type: AudioQualityState
The accumulated quality posture on `album_requests`. Not needed until the measurement
type is in place. Would replace the scattered columns (min_bitrate, prev_min_bitrate,
verified_lossless, spectral_grade, spectral_bitrate, on_disk_spectral_*).

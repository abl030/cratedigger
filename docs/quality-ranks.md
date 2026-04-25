# Codec-Aware Quality Ranks

**Issue #60** introduced a rank-based comparison model so the pipeline can
compare audio quality across codecs correctly. This page documents the model,
the default band values, and how to retune them via `config.ini`.

## Why ranks instead of raw bitrate

The legacy pipeline compared quality using `min_bitrate_kbps` alone. Two bugs
fell out of that:

1. **Cross-codec downgrade loop.** After a FLAC → Opus 128 conversion, the
   measured Opus bitrate lands around 95-135 kbps. Beets stores that. On the
   next cycle, a new MP3 V0 download (~245 kbps) "won" the raw bitrate
   comparison and replaced the perceptually equivalent Opus with MP3.
2. **Too-low verified-lossless target silently won.**
   `verified_lossless_target = "opus 64"` produced a 64 kbps Opus file that
   bypassed every downgrade check because `verified_lossless=True` was a
   blanket override.

The rank model fixes both by classifying every measurement into a perceptual
band (`QualityRank`) and comparing bands first, bitrates second.

## The `QualityRank` bands

```
LOSSLESS     100   FLAC, ALAC, WAV
TRANSPARENT   60   MP3 V0, MP3 CBR 320, Opus 128+, AAC 192+
EXCELLENT     50   MP3 V1-V2, MP3 CBR 256, Opus 96, AAC 144+
GOOD          40   MP3 V3-V4, MP3 CBR 192, Opus 64, AAC 112+
ACCEPTABLE    30   MP3 V5-V9, MP3 CBR 128, Opus 48, AAC 80+
POOR          20   below acceptable floor
UNKNOWN        0   not enough info to classify
```

Integer spacing leaves room for inserting new bands later. The rank is never
persisted — it's always recomputed from `(format, bitrate, is_cbr)` + config.

## Label vs bare-codec resolution

`quality_rank(format_hint, bitrate_kbps, is_cbr, cfg)` resolves a measurement
through six steps, in order:

1. Both `format_hint` and `bitrate_kbps` are `None` → `UNKNOWN`.
2. `format_hint` first token in `cfg.lossless_codecs` → `LOSSLESS`.
3. Explicit VBR label (`"mp3 v0"`, `"mp3 v2"`, ...) → index into
   `cfg.mp3_vbr_levels` (10-tuple indexed by V0..V9). VBR labels are
   self-certifying — the bitrate is irrelevant because V0 is V0.
4. Explicit bitrate label (`"opus 128"`, `"mp3 320"`, `"aac 192"`) → classify
   the declared numeric bitrate against the matching codec's `CodecRankBands`.
   The label is a contract; the actual measured bitrate is ignored.
5. Bare codec name (`"MP3"`, `"Opus"`, `"AAC"` from beets `items.format`) →
   classify the measured `bitrate_kbps` against the band table. `"MP3"`
   with `is_cbr=True` uses `cfg.mp3_cbr`, otherwise `cfg.mp3_vbr`.
6. Unknown codec → `UNKNOWN`.

The **label path** (step 3-4) is what makes lo-fi V0 imports work without the
old `verified_lossless` blanket bypass: a 207 kbps file with `format="mp3 v0"`
still classifies as `TRANSPARENT` because V0 is V0 regardless of what the
encoder actually produced on quiet material.

## `compare_quality()` semantics

Primary key is the rank. Within the same rank:

- **LOSSLESS always equivalent** — FLAC bitrate variance (800-1100) has no
  quality meaning.
- **Different codec families** (Opus vs MP3 vs AAC vs FLAC) → **equivalent**.
  This is the core cross-codec parity fix.
- **Same codec family, either side carries an explicit label** → equivalent.
  A V0 label and a "mp3 320" label at the same rank are both contracts.
- **Same codec family, both bare codec names** → compare the configured
  metric (`avg_bitrate_kbps`, `median_bitrate_kbps`, or `min_bitrate_kbps`)
  with `cfg.within_rank_tolerance_kbps` tolerance.

## Bitrate metric — `min` vs `avg` vs `median`

VBR codecs have legitimate per-track variance. Opus 128 unconstrained VBR
regularly lands individual tracks between 95-150 kbps depending on material;
MP3 V0 can range 160-270. Using the minimum across an album penalizes
legitimately encoded VBR with quiet passages.

Three metrics are supported:

- **`avg`** (default) — album-mean per-track bitrate. Robust to VBR variance.
- **`median`** — middle per-track bitrate. Outlier-resistant. Recommended
  when albums commonly contain a single very-quiet intro/outro, hidden
  tracks, or short interludes that drag MIN down or skew AVG away from the
  typical track quality. The median ignores them entirely. See *When to
  prefer median* below.
- **`min`** — minimum per-track bitrate. Legacy behavior; conservative but
  prone to false negatives on lo-fi VBR.

Spectral cliff detection and `transcode_detection()` continue to use `min`
regardless of this setting — those care about the worst track, not the
typical one. The transcode-detection spectral-fallback threshold is now
derived from `cfg.mp3_vbr.excellent` (issue #66) so it tracks any retuning
of the gate threshold automatically — operators who lower the gate to
accept lower-quality V0 also implicitly lower what counts as "credible
V0" for the spectral fallback.

`measurement_rank()` is the single dispatch point. Each metric reads its
matching field on `AudioQualityMeasurement` (`avg_bitrate_kbps`,
`median_bitrate_kbps`, `min_bitrate_kbps`); if the configured metric's
field is `None`, classification falls back to `min_bitrate_kbps` so legacy
measurements still classify correctly.

### When to prefer `median`

Pick `median` over `avg` when your library has a meaningful number of
albums where a small minority of tracks pulls the average around. The
canonical cases:

- **Lo-fi V0 with one clean closer.** A bedroom-pop V0 sitting at ~190 kbps
  with a single "studio" track at 270 kbps. AVG drifts to ~200; MEDIAN
  stays at ~190 — a faithful representation of the album's actual quality
  contract.
- **Hidden tracks / silence outros.** A 90-second silent track at 60 kbps
  on an otherwise V0 album. MIN tanks to 60 (POOR), AVG dips by ~15-20
  kbps; MEDIAN ignores the outlier entirely.
- **Skits and interludes.** Hip-hop and concept albums frequently have
  10-20 second skits encoded at much lower bitrates. MEDIAN stays anchored
  to the typical full-length track.

Pick `avg` when your library is mostly even-bitrate VBR encodes — AVG and
MEDIAN converge on those, and AVG is the cheaper, more familiar metric.
The defaults stay on `avg`; switch to `median` only when one of the cases
above is biting you.

Computation: AVG is `sum/count`. MEDIAN is `statistics.median()` — the
middle value, or the mean of the two middle values for even track counts.
Both are computed in Python in `BeetsDB.get_album_info()` because the
beets library is SQLite (no native percentile aggregator).

## Default band values

All numbers live in `lib.quality.QualityRankConfig` defaults and in the
`[Quality Ranks]` section of `config.ini`.

### Opus (unconstrained VBR)

| Band | Threshold (kbps) |
|------|------------------|
| transparent | 112 |
| excellent | 88 |
| good | 64 |
| acceptable | 48 |

**Why 112 for transparent?** `ffmpeg -b:a 128k` unconstrained VBR averages
120-135 kbps on typical music — 112 leaves headroom for legitimate sparse
material. `excellent=88` matches Opus 96 quality (hydrogenaudio/Kamedo2
4.65/5 listening test). Full rationale in `docs/opus-encoding.md`.

### MP3 VBR (LAME V0-V9 targets)

| Band | Threshold (kbps) |
|------|------------------|
| transparent | 245 |
| excellent | 210 |
| good | 170 |
| acceptable | 130 |

The 210 threshold matches the legacy `QUALITY_MIN_BITRATE_KBPS=210` constant,
so bare-codec MP3 VBR measurements from beets keep behaving as they did
before the rank model. 245 adds a "V0 target" band above. The V0/V2/V4/etc.
mapping via `mp3_vbr_levels` handles labeled conversions separately.

`QUALITY_MIN_BITRATE_KBPS` is now defaults-only — gate behavior is driven
by `cfg.quality_ranks.gate_min_rank`, and every numeric threshold (including
this 210) lives in `QualityRankConfig` and can be retuned in the
`[Quality Ranks]` section of `config.ini`.

### MP3 CBR

| Band | Threshold (kbps) |
|------|------------------|
| transparent | 320 |
| excellent | 256 |
| good | 192 |
| acceptable | 128 |

Unverifiable CBR only reaches TRANSPARENT at 320 because the pipeline can't
prove a CBR file came from lossless source. Spectral cliff detection may
clamp it down further.

### AAC

| Band | Threshold (kbps) |
|------|------------------|
| transparent | 192 |
| excellent | 144 |
| good | 112 |
| acceptable | 80 |

Hydrogenaudio consensus places the "not worth going higher for music" ceiling
for AAC at 192.

## The verified-lossless guardrail

`import_quality_decision()` used to blanket-bypass on `verified_lossless=True`.
It now tier-gates the bypass:

- `verified_lossless=True` + verdict `"better"` or `"equivalent"` → import.
- `verified_lossless=True` + verdict `"worse"` → **downgrade** (blocked).

This prevents a deliberately-too-low `verified_lossless_target` (Opus 64,
Opus 48) from replacing a good existing album. The cratedigger process also logs
a warning at startup when `verified_lossless_target` classifies below
`gate_min_rank`, so operators see the contradiction before it bites.

## Tuning via config.ini

Every knob is optional — missing keys fall back to the dataclass defaults.
Partial overrides work (e.g. set only `opus.transparent = 120` and everything
else stays at defaults).

```ini
[Quality Ranks]
bitrate_metric = avg
gate_min_rank = excellent
within_rank_tolerance_kbps = 5

opus.transparent = 112
opus.excellent = 88
opus.good = 64
opus.acceptable = 48

mp3_vbr.transparent = 245
mp3_vbr.excellent = 210
mp3_vbr.good = 170
mp3_vbr.acceptable = 130

mp3_cbr.transparent = 320
mp3_cbr.excellent = 256
mp3_cbr.good = 192
mp3_cbr.acceptable = 128

aac.transparent = 192
aac.excellent = 144
aac.good = 112
aac.acceptable = 80

# Collection fields (issue #65). All three are CSV. Defaults are sensible —
# you almost certainly do not need to set these.
mp3_vbr_levels = TRANSPARENT,EXCELLENT,EXCELLENT,GOOD,GOOD,ACCEPTABLE,ACCEPTABLE,ACCEPTABLE,ACCEPTABLE,ACCEPTABLE
lossless_codecs = flac,lossless,alac,wav
mixed_format_precedence = mp3,aac,opus,flac
```

### Collection fields (mp3_vbr_levels / lossless_codecs / mixed_format_precedence)

These three fields configure rank-model behavior at the codec-identity layer
rather than the bitrate-band layer. They were previously dataclass-only;
issue #65 wires them through the INI parser.

- **`mp3_vbr_levels`** — comma-separated list of exactly 10 rank names
  (V0..V9). Maps each LAME VBR V-level to a rank when the format hint is
  an explicit `mp3 v0` / `mp3 v3` / etc. label. Defaults assume LAME's
  documented V-level quality contract:
  - V0 → TRANSPARENT, V1-V2 → EXCELLENT, V3-V4 → GOOD, V5-V9 → ACCEPTABLE.
  - Tighten if you don't trust LAME's claim that V2 is transparent.
  - Loosen if you encode at V4 and want it to pass the gate.
- **`lossless_codecs`** — comma-separated set of codec strings (lowercased,
  deduplicated). The first token of `format_hint` is checked against this
  set; a match short-circuits to LOSSLESS. Default: `flac, lossless, alac,
  wav`. Add `ape, dsf, wavpack` if your library carries them.
- **`mixed_format_precedence`** — comma-separated **ordered** tuple. When an
  album has tracks in multiple codecs (rare — usually a manually merged
  album), `_reduce_album_format()` walks this list in order and picks the
  first codec that appears on disk. The default `mp3, aac, opus, flac` is
  worst-first, so a mixed FLAC+MP3 album classifies as MP3 (the
  conservative choice). Reorder if you want a different "canonical codec"
  policy.

Validation:

- `mp3_vbr_levels` must have **exactly 10** entries; any other count raises
  `ValueError` at startup.
- Each `mp3_vbr_levels` entry must be a valid `QualityRank` name
  (case-insensitive).
- Empty values (`key = `) fall through to the default; an explicit list
  containing only whitespace/commas raises so the user gets a diagnostic
  instead of silently losing all entries.
- All codec strings are lowercased on parse — `FLAC,Alac,WAV` is identical
  to `flac,alac,wav`.

Reload by restarting `cratedigger-web` (the web simulator reads this file on
every request) and waiting for the next `cratedigger.timer` fire (5 min).

## Diagnostic tooling

- `pipeline-cli quality <request_id>` — shows the current rank, the
  configured policy, and simulates every common download scenario against
  the runtime cfg.
- `pipeline-cli import-preview` — runs the unified no-mutation preview path.
  Use `--download-log-id` or `--request-id --path` for real folders, or
  `--values --values-json '{...}'` for typed simulator inputs. Real-folder
  preview may run audio validation, spectral analysis, and temp-workspace
  conversion; it is not a metadata-only check.
- New Wrong Matches rows created by the download import path run the same
  preview-backed triage immediately. Confident cleanup-eligible rejects are
  deleted and cleared; would-import and uncertain candidates remain visible.
  The triage action and reason are persisted under
  `download_log.validation_result.wrong_match_triage` for audit.
- `/api/pipeline/constants` (web) — surfaces `rank_gate_min_rank` and
  `rank_bitrate_metric` alongside other constants for the Decisions tab UI.
- `/api/pipeline/simulate?...` (web) — accepts the same params as
  `full_pipeline_decision()` plus `existing_format`, `existing_is_cbr`,
  `new_format` so the web simulator classifies the same way production does.
- `/api/import-preview` (web) — returns the common preview verdict shape for
  real-folder preview and typed values.

## Related

- Issue #60 (this PR)
- Issue #31 — original quality pipeline bugs that drove this rewrite
- `docs/opus-encoding.md` — Opus 128 rationale and listening test references

# Quality Verification

See also `docs/audio-classification-research.md` for the April 2026 research
log on classifier trust, corpus evaluation, false-positive risk, and external
prior art.

## Gold Standard Pipeline

The highest quality standard for the library:

1. **Download lossless** (FLAC, ALAC, WAV) from Soulseek
2. **Verify with spectral analysis** — confirm the lossless file is genuinely lossless (not a lossy transcode wrapped in a lossless container)
3. **Convert to VBR V0** — `ffmpeg -codec:a libmp3lame -q:a 0`
4. **Import to beets** — the VBR V0 bitrate acts as an ongoing quality fingerprint

Why VBR V0 and not keep FLAC? Because VBR bitrate IS the quality signal. A genuine CD rip converted to V0 produces ~240-260kbps. A transcode produces ~190kbps. CBR 320 would hide this information.

## Current Verification Methods

### 1. VBR V0 Bitrate Check (implemented)

After FLAC-to-V0 conversion, the resulting bitrate reveals source quality:
- **Genuine lossless**: ~220-280kbps (varies by musical complexity)
- **Transcode from ~192kbps**: ~190-210kbps
- **Transcode from ~128kbps**: ~160-180kbps

Threshold: `cfg.mp3_vbr.excellent` (default 210 kbps), read by `transcode_detection()` in `lib/quality.py`. The legacy `TRANSCODE_MIN_BITRATE_KBPS = 210` module constant is still exported as the default and used when no cfg is passed; operators who retune `[Quality Ranks] mp3_vbr.excellent` in `config.ini` automatically move both the gate threshold and this fallback (#66).

Limitation: Only works when we download FLAC and convert. Doesn't catch bad MP3 downloads (e.g. 320kbps that was upsampled from 128kbps).

### 2. Spectral Band Energy Analysis (research phase)

Uses `sox` bandpass filtering to measure energy ratios in high-frequency bands relative to a 1-4kHz reference band. Genuine high-quality audio has consistent energy across the spectrum. Transcodes show a sharp drop at the original encoding's lowpass cutoff frequency.

#### Test Results (2026-03-28)

```
Label                                  ref RMS     14-16k%  16-18k%  18-20k%
Genuine FLAC (lossless)                0.118154      4.1%     2.7%     1.8%
Genuine V0 (from FLAC)                 0.118188      4.2%     2.8%     1.8%
Genuine 320 (from FLAC)                0.118158      4.1%     2.7%     1.8%
TRANSCODE 128->320                     0.112545      4.2%     1.0%     0.0%
TRANSCODE 192->320                     0.114673      4.2%     2.2%     0.7%
Hot Garden Stomp (suspect 320)         0.075199      0.7%     0.6%     0.4%
```

Observations:
- Genuine V0 is spectrally identical to FLAC — the conversion preserves the quality fingerprint
- The **18-20kHz band** is the most discriminating: 0.0% for 128 transcode vs 1.8% for genuine
- The **16-18kHz band** separates 192 transcodes: 1.0% (128-transcode) vs 2.2% (192-transcode) vs 2.7% (genuine)
- Hot Garden Stomp (320kbps, 1993 cassette) has less high-frequency energy than a 128->320 transcode — source was likely ~96kbps or lower
- LAC (Lossless Audio Checker) is useless for this purpose — reported "Clean" on all files including obvious transcodes

#### Method

```python
# For each track, measure RMS energy in bandpass-filtered ranges
sox file.mp3 -n sinc 1000-4000 stat    # Reference band (1-4kHz)
sox file.mp3 -n sinc 14000-16000 stat  # High frequency band 1
sox file.mp3 -n sinc 16000-18000 stat  # High frequency band 2
sox file.mp3 -n sinc 18000-20000 stat  # High frequency band 3

# Calculate: band_energy / reference_energy * 100 = percentage
# Genuine: 14-16k > 2.5%, 16-18k > 2.0%, 18-20k > 1.0%
# Suspect: any band significantly below these thresholds
```

Dependencies: `sox` (in nixpkgs)

#### LAME Lowpass Table (from source code)

| Bitrate (kbps) | Lowpass (Hz) | 14-16k% | 16-18k% | 18-20k% |
|----------------|-------------|---------|---------|---------|
| 96             | 15,100      | < 1%    | < 1%    | < 1%    |
| 128            | 17,000      | normal  | ~1%     | ~0%     |
| 160            | 17,500      | normal  | ~1%     | ~0%     |
| 192            | 18,600      | normal  | normal  | < 1%    |
| 256            | 19,700      | normal  | normal  | reduced |
| 320 CBR        | 20,500      | normal  | normal  | normal  |
| V0             | **disabled** | normal  | normal  | normal  |
| V2             | 18,671      | normal  | normal  | < 1%    |
| Lossless (CD)  | 22,050      | normal  | normal  | normal  |

Source: LAME `lame.c` `optimum_bandwidth()` function.

#### The 16kHz Shelf (strongest single indicator)

All MP3 encoders have a fundamental limitation: there is no scale factor band 21 (sfb21) for frequencies above ~16kHz. This forces the encoder to choose between less accurate representation above 16kHz or less efficient storage below. The result is a characteristic energy step-down ("shelf") at 16kHz that is:

- Present in **ALL** MP3 files regardless of bitrate
- **NOT** present in genuine lossless, vinyl rips, or cassette rips
- The strongest single automated indicator of MP3 origin

To detect the shelf, check the ratio: `energy(14-16kHz) / energy(16-18kHz)`
- Genuine lossless: ratio close to **1.0** (gradual decrease)
- MP3 transcode: ratio **3x-10x** (sharp cliff at 16kHz)

#### Edge Cases

- **Lo-fi recordings** (boombox, cassette, AM radio): Naturally have limited high-frequency content. The energy ratio approach handles this because it compares RELATIVE to the 1-4kHz band, not absolute levels. But very lo-fi material may have low ratios simply due to recording quality, not transcoding.
- **Classical/acoustic music**: May have less high-frequency energy than rock/electronic, but still maintains relative proportions. Need wider thresholds.
- **Cassette recordings**: Tape hiss adds energy across all frequencies including high bands. Genuine cassette rips may actually show MORE high frequency energy (as noise) than clean digital recordings.
- **Natural rolloff vs. artificial cutoff**: Vinyl and cassette have gradual, smooth HF rolloff. MP3 transcodes have sharp, blocky cutoffs. The shape matters more than the location.

#### Performance

Sox bandpass + stats takes ~0.5-1s per band per track. For 4 bands on a 12-track album: ~24-48s.

**Optimisation**: Analyse only the first 30 seconds: `sox "$file" -n trim 0 30 sinc 16k-18k stats`. Cuts time by ~75% with negligible accuracy loss (encoding parameters are consistent throughout a track).

### 3. Existing Tools Evaluated

| Tool | Works? | Notes |
|------|--------|-------|
| **LAC** (losslessaudiochecker) | **No** | In nixpkgs but useless — said "Clean" on 128→FLAC transcode |
| **spectro** (`pip install spectro`) | Maybe | Has automated `check` command with built-in thresholds, worth testing |
| **fakeflac** (GitHub) | Maybe | FFT + backward sweep for discontinuity, Python + scipy |
| **FLAC_Detective** (GitHub) | Maybe | 11-rule scoring system, claims to handle vinyl/cassette edge cases |
| **auCDtect** | No | Windows only, only analyses WAV for CD origin detection |
| **Fakin' The Funk** | No | Windows-only GUI |

### 4. Published Research

- **D'Alessandro & Shi (2009)**: "MP3 Bit Rate Quality Detection through Frequency Spectrum Analysis" — 97% overall accuracy using SVM on 100 frequency bands in the 16-20kHz range. Seminal paper.
- **FLAD**: Neural network (EfficientNet) achieving 99.75% accuracy. Analyses 2.4-20kHz, suggesting lossy artifacts exist in mid-frequencies too, not just at the cutoff. Heavy deps (PyTorch).

## V2: Spectral Gradient Analysis (tested 2026-03-28)

The wide-band energy ratio approach (v1) produces too many false positives on lo-fi and quiet music. V2 uses 500Hz slices and detects the **shape** of the rolloff instead of absolute levels.

### Method

1. Divide 12-20kHz into 16 x 500Hz slices
2. Measure RMS energy in each slice via `sox file -n trim 0 30 sinc {lo}-{hi} stat`
3. Compute gradient (dB/kHz) between adjacent slices
4. **Cliff detection**: 2+ consecutive slices with gradient steeper than -12 dB/kHz
5. **HF deficit**: average dB of top 4 slices (18-20kHz) vs reference band (1-4kHz)

### Per-track classification

- **SUSPECT**: cliff detected, OR HF deficit > 60dB
- **MARGINAL**: HF deficit 40-60dB, no cliff
- **GENUINE**: HF deficit < 40dB, no cliff

### Album-level classification

- **LIKELY_TRANSCODE**: >75% of tracks SUSPECT
- **SUSPECT**: >60% of tracks SUSPECT
- **GENUINE**: <60% suspect
- Never auto-reject; flag for review

### Two-tier verification

1. **FLAC downloads**: Run spectral check pre-conversion. If cliff detected → transcode in container. Still convert to V0 — if bitrate > existing on disk, import as upgrade.
2. **MP3 downloads (especially CBR 320)**: Run spectral check post-download. Cliff + high deficit = upsampled garbage.
3. **Already converted V0 with good bitrate (≥ `cfg.mp3_vbr.excellent`, default 210 kbps)**: Skip spectral check — the V0 conversion already proved source quality.

### Tuning results (Mountain Goats library, 65 albums)

Tested across the entire Mountain Goats catalogue — a worst-case scenario as the band's early work (1991-2000) was recorded on boomboxes and cassette recorders with genuinely minimal high-frequency content.

At `HF_DEFICIT_SUSPECT=60dB + cliff detection`:
- **19 correctly flagged SUSPECT** (confirmed bad source, transcodes, or upsampled 320s)
- **46 correctly GENUINE** (including lo-fi albums with good V0 conversion bitrates)
- **0 false positives** on albums with verified good sources
- Successfully catches: cliffs at 16kHz (128kbps transcodes), cliffs at 18kHz (192kbps), upsampled CBR 320, terrible pre-pipeline rips

Albums that were downloaded as FLAC, converted to V0 at or above the `cfg.mp3_vbr.excellent` threshold (default 210 kbps), and have no cliff: always pass — confirming the V0 conversion is the primary quality proof.

### What the spectral check catches that V0 conversion doesn't

- **CBR 320 downloads**: V0 conversion only happens for FLACs. Native MP3 320 downloads skip conversion entirely. Spectral check catches upsampled garbage (e.g. Hot Garden Stomp at 52dB deficit, Songs for Peter Hughes at 72dB + cliffs).
- **Pre-pipeline imports**: Albums imported before the pipeline existed have no download history or V0 conversion data. Spectral check is the only way to assess their quality.

### Reference: HF deficit ranges observed

| Source quality | HF deficit range | Cliffs? |
|---------------|-----------------|---------|
| Genuine CD rip (FLAC) | 28-46dB | None |
| Genuine V0 from FLAC | 32-48dB | None |
| Lo-fi genuine (Mountain Goats boombox era) | 42-59dB | None |
| Transcode 192→anything | 53-67dB | Often (at 18kHz) |
| Transcode 128→anything | 71-84dB | Always (at 16kHz) |
| Upsampled CBR 320 (from ~96kbps) | 52-97dB | Sometimes |
| Quiet jazz/classical (genuine CD) | 33-57dB | None |
| Children's choir (genuine CD) | 31-62dB | None |

## TODO

- [ ] Integrate spectral gradient check into pipeline (import_one.py or cratedigger.py)
- [ ] Add spectral quality score to pipeline DB (new column) and web UI display
- [ ] Run spectral check on CBR 320 MP3 downloads post-download
- [ ] Run spectral check on FLACs pre-conversion to catch transcodes early
- [ ] Skip spectral check for albums that already passed V0 conversion (≥ `cfg.mp3_vbr.excellent`, default 210 kbps)
- [ ] Test `spectro` pip package as second-opinion validation
- [ ] Performance: 16 sox calls per track x 30s trim ≈ 8s/track, ~100s per 12-track album

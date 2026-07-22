# Spectral calibration research: MP3 / LAME

Issue #829 Phase 0. Cratedigger's spectral cliff detector was built and
tuned against LAME's default lowpass ladder, so this codec is "home turf."
Goal: pin down what our code assumes today, verify it against LAME's
actual source/docs, and size the risk that a peer's MP3 came from a
non-LAME encoder whose ladder doesn't match ours.

## Summary

- Our `LAME_LOWPASS` table in `lib/spectral_check.py` is a **verbatim
  transcription of LAME's own `optimum_bandwidth()` lookup table**
  (`freq_map[]` in `libmp3lame/lame.c`) — not a listening-test estimate,
  a copy of the encoder's source. Every value matches upstream `master`
  today, and that CBR table has been stable across the "modern" LAME
  lineage (3.90 → 3.100+); it is not a moving target.
- The real fragility is **encoder identity, not LAME version**. A peer's
  MP3 could plausibly come from Xing/Helix (fixed 16 kHz lowpass at *any*
  bitrate), Fraunhofer (anecdotally ~16 kHz even at CBR 320 in some
  builds), or Shine (no lowpass filter and no psychoacoustic model at
  all). Our cliff-to-bitrate mapping silently assumes LAME authorship on
  every file it grades.
- VBR (`-V0`..`-V9`) cutoffs are lower and less sharply banded than CBR,
  and V0-V2 carry an `-Y`/sfb21 wrinkle that can soften the cliff right
  where our detector looks for it.
- `--lowpass -1` / `-k` full-bandwidth encodes exist in the wild but are
  rare, historically buggy in LAME itself, and discouraged by LAME's own
  developers — low risk, not zero.

## How cratedigger measures today

`lib/spectral_check.py` slices 12-20 kHz into 500 Hz bands (`SLICE_FREQS`,
16 slices) plus a 1-4 kHz reference band. Each slice's RMS (via `sox ...
sinc <lo>-<hi> stat`) becomes dB; `detect_cliff()` looks for
`MIN_CLIFF_SLICES = 2` consecutive slices whose gradient drops below
`CLIFF_THRESHOLD_DB_PER_KHZ = -12.0` dB/kHz and reports the frequency of
the first slice in that run as `cliff_freq_hz`. A coarser `hf_deficit_db`
(reference dB minus the average of the top four slices) grades
suspect/marginal (`HF_DEFICIT_SUSPECT = 60.0`, `HF_DEFICIT_MARGINAL =
40.0`) when there's no sharp cliff but the whole top end is quietly gone.

`estimate_bitrate_from_cliff()` maps a detected cliff back to an assumed
CBR bitrate via hand-picked midpoint ranges between consecutive
`LAME_LOWPASS` entries:

```python
LAME_LOWPASS = [
    (15100, 96), (15600, 112), (17000, 128), (17500, 160),
    (18600, 192), (19400, 224), (19700, 256), (20500, 320),
]
```

(`lib/spectral_check.py` lines 31-40.) At the album level,
`classify_album()` grades `likely_transcode` at ≥75% suspect tracks,
`suspect` at ≥`ALBUM_SUSPECT_PCT = 60.0`%, else `genuine`; `analyze_album()`
reports the **minimum** per-track estimate (worst-track-wins, always via
`min` regardless of the configured `bitrate_metric` —
`docs/quality-ranks.md` § "Bitrate metric"). The CBR band table
(`mp3_cbr.*`) is explicitly calibrated to line up with these cliff
numbers ("a cliff-detected 192 IS a `good`-band reading, by construction").

## LAME lowpass ladder — CBR

`optimum_bandwidth()` in `libmp3lame/lame.c` (`freq_map[]`, a
`bitrate → lowpass Hz` array; LAME snaps an arbitrary CBR bitrate to the
nearest row via `nearestBitrateFullIndex()`):

| CBR (kbps) | LAME lowpass (Hz) | In our table? |
|---|---|---|
| 8/16/24/32/40/48/56/64/80 | 2000/3700/3900/5500/7000/7500/10000/11000/13500 | no (below our floor) |
| **96** | **15100** | yes |
| **112** | **15600** | yes |
| **128** | **17000** | yes |
| **160** | **17500** | yes |
| **192** | **18600** | yes |
| **224** | **19400** | yes |
| **256** | **19700** | yes |
| **320** | **20500** | yes |

Confirmed against both `lameproject/lame` (upstream) and the
`gypified/libmp3lame` mirror — identical `freq_map[]` literal in both
(<https://github.com/gypified/libmp3lame/blob/master/libmp3lame/lame.c>,
<https://github.com/lameproject/lame/blob/master/libmp3lame/lame.c>).
**Every value in our `LAME_LOWPASS` matches exactly.**

Two behavioral wrinkles from the same function: mono CBR/ABR input gets
`lowpass *= 1.5` (mono needs less bitrate per frame, leaving headroom for
a higher cutoff — a mono-sourced file's cliff could sit above the
stereo-table value for its true bitrate, so `estimate_bitrate_from_cliff()`
could **under-estimate** it); and LAME resamples input >48 kHz down to
48 kHz max, and at CBR <~104 kbps (and low VBR quality) resamples to a
lower rate before encoding, so at low bitrates the lowpass is partly a
consequence of the resampled Nyquist limit rather than a deliberate
filter choice (<https://wiki.hydrogenaudio.org/index.php/LAME>).

## LAME lowpass ladder — VBR presets (V0-V6)

VBR is internally driven by a continuous quality index (0=best..9=worst,
extended to 10), interpolated with `linear_int(a, b, m)` between two
tables depending on the VBR algorithm — `vbr_rh` (`--vbr-old`) vs
`vbr_mtrh` (`--vbr-new`, default since 3.98):

| Quality index | `vbr_rh` lowpass (Hz) | `vbr_mtrh` lowpass (Hz) |
|---|---|---|
| 0 (≈V0) | 19500 | 24000 (≈ unfiltered, above Nyquist) |
| 1 (≈V1) | 19000 | 19500 |
| 2 (≈V2) | 18600 | 18500 |
| 3 (≈V3) | 18000 | 18000 |
| 4 (≈V4) | 17500 | 17500 |
| 5 (≈V5) | 16000 | 17000 |
| 6 (≈V6) | 15600 | 16500 |
| 7 | 14900 | 15600 |
| 8 | 12500 | 15200 |
| 9 | 10000 | 7230 |
| 10 | 3950 | 3950 |

Source: WebFetch of `libmp3lame/lame.c` `lame_init_params()`, quoting the
literal `int const x[11]` arrays for `vbr_rh` and `vbr_mtrh`
(<https://github.com/gypified/libmp3lame/blob/master/libmp3lame/lame.c>).
The V-level-to-index correspondence (V0=index 0 … V9=index 9) is LAME's
documented CLI contract; the exact index arithmetic inside
`lame_init_params` was not independently re-derived — treat row labels as
`[unverified]` to one-index precision, though the overall monotonic shape
is solid.

Independent corroboration from real-world file measurements, likely
reflecting version/psy-model/ATH variation on top of the raw table
(<https://wiki.hydrogenaudio.org/index.php/LAME>): `-b 320`
(CBR) 20094–20627 Hz, `-V1` 19383–19916 Hz, `-V2` 18671–19205 Hz, `-V3`
17960–18494 Hz, `-V4` 17249–17782 Hz, `-V5`–`-V6` 16538–17071 Hz. `-V0`
has no fixed number in that source, consistent with `vbr_mtrh`'s 24000 Hz
table entry — "as much bandwidth as the format allows."

**Implication:** our cliff→bitrate table is CBR-only. Labeled VBR (`mp3
v0`, etc.) bypasses `spectral_check.py`'s bitrate estimate in the rank
model entirely — routed via `mp3_vbr_levels` instead
(`docs/quality-ranks.md`). But *unlabeled* bare-codec VBR MP3s still get
spectral-checked, and a detected cliff still runs through the CBR-shaped
`estimate_bitrate_from_cliff()` — a mapping never designed for VBR. At V2
(a common rip default), the real cutoff (~18.6-19.2 kHz) happens to land
inside our 18050-19000 CBR-192 bucket — coincidence, not design.

## Version stability (3.97 → 3.100)

- No changelog entry found describing a *revision* of the `freq_map[]` Hz
  values across 3.97/3.98/3.99/3.100 — today's `master` matches our
  table. `[unverified beyond absence of contrary changelog evidence]`; I
  did not diff every tagged release literally.
- The **VBR algorithm default did change**: `--vbr-new` (`mtrh`) became
  default in LAME 3.98, superseding `--vbr-old` (`rh`), default in
  3.90-3.97 — a real, documented shift that changes the effective VBR
  ladder (the two columns above diverge most at V0/V8/V9) depending on
  which LAME build produced a file
  (<https://wiki.hydrogenaudio.org/index.php/LAME>; Doom9 corroboration:
  "Prior to LAME 3.98, the --vbr-new switch enabled the new VBR mode.
  This is now the default VBR mode" — <http://forum.doom9.org/archive/index.php/t-165982.html>).
- LAME 3.98.3/3.99-alpha2 changed bit-reservoir growth-cap handling (not
  itself a lowpass change), and a changelog entry records "Fixed bug with
  lowpass filters when using VBR with a 64kbps or lower min bitrate
  setting" (exact version `[unverified]`) — neither matters much for
  cratedigger's 12-20 kHz window, well above that low-bitrate regime.
- **Takeaway:** the CBR ladder is old and stable — Phase 3 shouldn't
  expect to find CBR drift by LAME version. If Phase 3 finds inconsistent
  VBR cliff behavior across a real-file sample, check pre-/post-3.98
  encoder version (`vbr-old` vs `vbr-new`) before assuming measurement
  noise.

## Non-LAME MP3 encoders — divergence table

The real risk surface: cratedigger validates arbitrary Soulseek peer
uploads, and nothing guarantees LAME authorship — older rips and hardware
rippers commonly used Xing, Fraunhofer, or (rarely) Helix/Shine.

| Encoder | Lowpass behavior | Divergence from our table | Confidence |
|---|---|---|---|
| **LAME** (all modern) | Bitrate/quality-dependent ladder above | None — calibration source | Verified (primary source) |
| **Xing / Helix** (`hmp3`, RealProducer-era) | **Fixed 16 kHz lowpass by default**, any bitrate, unless `-HF`/`-HF2` (needs VBR quality ≥80) | A Xing 320 kbps file still shows a ~16 kHz cliff → our table reads it as ~112-128 kbps CBR, a severe **under-estimate** | Verified — <https://wiki.hydrogenaudio.org/index.php?title=Helix_MP3_Encoder>, <https://www.speedguide.net/forums/viewtopic.php?t=111037> |
| **Fraunhofer / FhG** | Anecdotal "brickwall at 16 kHz" even at CBR 320, vs LAME's ~20.5 kHz at the same bitrate | Same failure mode as Xing if true: genuine 320 kbps reads as ~112-128 kbps | `[unverified]` — forum/community testimony only (FhG's reference encoder is closed-source); not FhG documentation or source. <https://community.adobe.com/feature-requests-545/use-lame-mp3-encoder-to-dramatically-improve-quality-1436130>, <https://www.head-fi.org/threads/my-fraunhofer-vs-lame-test-results.49368/> |
| **Shine** (fixed-point, embedded) | **No lowpass filter, no psychoacoustic model at all** — LAME logs an explicit polyphase-lowpass transition band; Shine's log has no equivalent | No detectable cliff regardless of quality → likely graded `genuine` even though overall quality is poor (missing psy model). False-**negative** on quality, not a bandwidth issue our detector can see | `[unverified precisely]` but consistent across independent sources: <https://github.com/toots/shine>, <https://news.ycombinator.com/item?id=30998693> |
| **BladeEnc / other late-90s** | Not researched for this doc | `[unverified]` — flag for Phase 3 if any turn up live | Not researched |

**Net read:** our cliff table is a *LAME* lowpass-inversion table, not a
generic MP3 one — correct when the encoder was LAME (the de facto
standard for high-quality MP3 since the early 2000s), but systematically
**under-estimating** true bitrate for Xing/Helix or (anecdotally) FhG
material, pushing those files toward a lower rank than deserved. Safer
than false-accepting a bad file, but it can cause unnecessary re-searches
on files that were already fine.

## CBR/VBR nuances

- **sfb21 / `-Y` switch (V0-V2 only).** Scale-factor band 21 (roughly
  ≥16 kHz on 44.1 kHz material) has no independent scale factor in the
  bitstream, so encoding it with full precision can force LAME to lower
  *global* gain, inflating bitrate across the whole frame. `-V3`-`-V9`
  default to `-Y` (don't chase sfb21 precision if it bloats bitrate);
  `-V0`-`-V2` do **not**, so those tiers spend extra bits on top-band
  accuracy instead of filtering more aggressively
  (<https://wiki.hydrogenaudio.org/index.php?title=LAME_Y_switch>). Effect:
  at V0-V2 the top slices may show bit-starved-but-present energy rather
  than a clean rolloff — our detector may see *less* of a cliff there
  than a naive Hz-vs-bitrate model predicts.
- **Joint stereo.** LAME's default (`-m j`) lets the encoder choose
  per-frame (~40×/sec at 44.1 kHz) between L/R and mid/side coding; joint
  stereo is default when `-V` > 4 or fixed bitrate ≤160 kbps, plain
  stereo otherwise (<https://www.mankier.com/1/lame>, corroborated by
  <https://sourceforge.net/p/lame/bugs/454/>). M/S coding concentrates
  energy in the sum channel; our sox band-RMS measurement doesn't
  explicitly downmix, so this is at most a second-order effect on which
  channel's rolloff we measure — `[unverified]` beyond that general
  mechanism; no source quantifies a direct M/S-vs-cutoff interaction.
- **`--lowpass -1` / full-bandwidth encodes exist but are rare and
  discouraged.** `--lowpass -1` documented-disables LAME's automatic
  filter; `-k` is a related full-bandwidth switch. Both exist in the wild
  ("preserve everything" users) but LAME's developers discourage this —
  reproducing >~16 kHz / <20 Hz mostly wastes bits — and `-k` has a
  documented history of not reliably disabling the filter in some 3.98
  builds (bug #302). When present, produces a file with **no cliff
  regardless of true bitrate** — same false-negative shape as Shine, but
  for LAME (<https://mediamonkey.com/forum/viewtopic.php?t=105263>,
  <https://sourceforge.net/p/lame/bugs/302/>,
  <https://sourceforge.net/p/bonkenc/discussion/85470/thread/aafde90b/>).

## Implications for calibration

1. **The CBR table needs no re-derivation** — it's a direct source
   transcription matching upstream `master`. Phase 3 should *validate* it
   against real LAME CBR files (confirm cliffs land where predicted; tune
   `CLIFF_THRESHOLD_DB_PER_KHZ`/`MIN_CLIFF_SLICES` if noisier than
   assumed), not re-derive the Hz values.
2. **The VBR path is the actual gap.** `estimate_bitrate_from_cliff()`
   reuses CBR bucket boundaries for any detected cliff regardless of
   `is_cbr`. It's gone unnoticed because the VBR ladder sits close to the
   CBR ladder at mid-tiers (V2 ≈ CBR-192, V4 ≈ CBR-128), but this has
   never been verified against real VBR files; V0/V1 has no corresponding
   bucket at all, though that correctly falls through to "no cliff,
   genuine" by accident of the table's open top end.
3. **Non-LAME encoder identity is undetectable from spectral evidence
   alone today.** Nothing distinguishes "no cliff, genuinely high
   quality" from "no cliff, Shine/full-bandwidth," or "cliff read as
   CBR-128, actually Xing/FhG at CBR-320." Phase 3 should check
   suspect-graded files for other fingerprints (LAME writes an
   identifiable Xing/LAME VBR header + version string; Xing/Helix/FhG
   have their own header signatures) before trusting an unusual cliff
   reading. The fail direction is safe-ish but not free: under-estimating
   quality triggers conservative behavior (requeue, no accept) rather
   than false-accepting a bad file — consistent with "never auto-decide
   anything irreversible" — but it costs needless search churn on files
   that were actually fine, worth measuring in Phase 3.

## Predictions for Phase 3 (testable)

| Setting | Expected cliff Hz (if LAME) | Our bucket → estimated bitrate | Prediction holds if... |
|---|---|---|---|
| LAME CBR 128 | ~17000 (±300) | 15400-17250 → 128 | Cliff detected in 16700-17300 Hz |
| LAME CBR 192 | ~18600 | 18050-19000 → 192 | Cliff detected in 18300-18900 Hz |
| LAME CBR 320 | ~20500 | ≥19550 → 320 | **No cliff** — our slices top out at 19500-20000, at/past a 20500 cutoff; if Phase 3 finds cliffs reliably appearing in the last slice for known-320 files, our slice window is too narrow |
| LAME `-V2` (VBR, unlabeled) | ~18671-19205 | 18050-19000 → 192, or 19000-19550 → 256 (straddles our boundary at 19000) | A meaningful fraction of genuine V2 files land right on the 192/256 boundary — expect noisy bitrate estimates for V2 specifically (table mismatch, not measurement noise) |
| LAME `-V0`/`-V1` (VBR, unlabeled) | ~19383-19916, or unfiltered if `vbr_mtrh` | No matching cliff | **No cliff, `genuine`** — correct outcome; confirm it isn't landing in the 19550+/320 bucket and getting mis-labeled CBR-320 |
| Xing/Helix file, any true bitrate | ~16000 (fixed) | 15400-17250 → 128 | Any Xing/Helix file reads as ~128 kbps regardless of true quality — sharpest test of the non-LAME risk if a known-Xing high-bitrate file can be sourced |
| Shine-encoded file | No cliff at all | No cliff → `genuine` (false negative on quality, not bandwidth) | Confirms "invisible to bandwidth detection"; would need a separate signal (missing Xing/LAME header, flat/noisy spectrum below 12 kHz) to catch Shine's real quality problem |
| `--lowpass -1` / full-bandwidth LAME | No cliff (content-dependent rolloff only) | No cliff → `genuine` | Rare in practice; same blind spot as Shine if found |
| Mono-sourced CBR file | ~1.5× stereo-table value for the same bitrate | Systematic under-estimate | If Phase 3 samples include mono-source rips (common for older mono originals), expect our bitrate estimate to read low relative to tags/history |

## Sources

- `lib/spectral_check.py`, `docs/quality-ranks.md` — this repo.
- LAME source, CBR `optimum_bandwidth()`/`freq_map[]` + VBR quality-index
  tables (`vbr_rh`/`vbr_mtrh` in `lame_init_params()`):
  <https://github.com/gypified/libmp3lame/blob/master/libmp3lame/lame.c>,
  <https://github.com/lameproject/lame/blob/master/libmp3lame/lame.c>
- LAME source, ABR/VBR preset struct (`safejoint`, no lowpass column):
  <https://fossies.org/linux/quicktime4linux/thirdparty/lame-3.93.1/libmp3lame/presets.c>,
  <https://github.com/lameproject/lame/blob/master/libmp3lame/presets.c>
- LAME USAGE doc (`--lowpass`, `--lowpass-width`, `-Y`):
  <https://raw.githubusercontent.com/lameproject/lame/master/USAGE>
- Hydrogenaudio LAME page: <https://wiki.hydrogenaudio.org/index.php/LAME>
- Hydrogenaudio `-Y` switch page:
  <https://wiki.hydrogenaudio.org/index.php?title=LAME_Y_switch>
- Hydrogenaudio Helix MP3 Encoder page:
  <https://wiki.hydrogenaudio.org/index.php?title=Helix_MP3_Encoder>
- Hydrogenaudio High-frequency content in MP3s page (general rationale
  only, no encoder-specific Hz comparisons found there):
  <https://wiki.hydrogenaudio.org/index.php?title=High-frequency_content_in_MP3s>
- Doom9 archive, LAME 3.98 `--vbr-new` default change:
  <http://forum.doom9.org/archive/index.php/t-165982.html>
- Fossies, LAME 3.99.5 → 3.100 changelog diff:
  <https://fossies.org/diffs/lame/3.99.5_vs_3.100/ChangeLog-diff.html>
- Speedguide forum, Xing vs LAME:
  <https://www.speedguide.net/forums/viewtopic.php?t=111037>
- Adobe Community + Head-Fi, Fraunhofer 16 kHz brickwall claim (forum
  testimony only, not FhG documentation):
  <https://community.adobe.com/feature-requests-545/use-lame-mp3-encoder-to-dramatically-improve-quality-1436130>,
  <https://www.head-fi.org/threads/my-fraunhofer-vs-lame-test-results.49368/>
- Shine encoder repo + HN discussion (no psychoacoustic model, no
  lowpass filter): <https://github.com/toots/shine>,
  <https://news.ycombinator.com/item?id=30998693>
- MediaMonkey forum, `--lowpass -1` confirmation; LAME bug #302 (`-k` not
  reliably disabling filter in 3.98b6); fre:ac/BonkEnc forum on
  "Disable all filtering" and developer pushback on full-bandwidth
  encodes: <https://mediamonkey.com/forum/viewtopic.php?t=105263>,
  <https://sourceforge.net/p/lame/bugs/302/>,
  <https://sourceforge.net/p/bonkenc/discussion/85470/thread/aafde90b/>
- Mankier LAME man page + bug #454, default joint-stereo threshold:
  <https://www.mankier.com/1/lame>, <https://sourceforge.net/p/lame/bugs/454/>

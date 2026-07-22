# Spectral calibration research: Vorbis

Issue #829 Phase 0. Cratedigger's cliff detector (`lib/spectral_check.py`)
was built and tuned against LAME's lowpass ladder (`LAME_LOWPASS`, 8 rows,
96-320 kbps). Vorbis (`.ogg`) is a real Soulseek population — old game
rips, Spotify-stream rips, and legacy YouTube DASH audio all circulate as
native Vorbis — and none of it goes through an encoder whose ladder we've
checked. This doc pins down libvorbis's/aoTuV's actual quality-level
lowpass table from source, compares it against our LAME-shaped bucket
math, and sizes the risk.

## Summary

- **libvorbis has its own explicit, per-quality-level hard lowpass table**
  (`_psy_lowpass_44` in `xiph/vorbis/lib/modes/psych_44.h`), just like LAME's
  `freq_map[]`. It only applies at low/mid quality — from **q6 (~192 kbps)
  upward, reference libvorbis imposes NO artificial lowpass at all**
  (sentinel `999.`, "unbounded, limited only by source/Nyquist"). This is
  the biggest structural difference from LAME, which keeps trimming right
  up through its top 320 kbps row (20.5 kHz).
- **aoTuV's tuning is not as separate from "reference" as the branding
  suggests.** Its Beta2-era changes were merged into mainline libvorbis
  1.1 (2004), and further aoTuV work was merged again from libvorbis 1.3.4
  onward (<https://en.wikipedia.org/wiki/Vorbis>). `xiph/vorbis` `master`
  today already carries aoTuV DNA; a direct source diff (below) shows
  aoTuV's *current* fork is still 0.4-0.9 kHz more conservative than
  reference at q0/q4/q5, but both ladders share the same nominal-bitrate
  steps and both go uncapped at the same point (q6+).
- **Spotify is a real, identifiable Vorbis population on Soulseek**, via
  tools that dump Spotify's raw stream unmodified (Zotify, Soggfy) —
  Free/Normal ≈ 160 kbps (`-q5`), Premium/Very-High ≈ 320 kbps (`-q9`),
  independently measured by SoundExpert at ~151 kbps and ~313 kbps.
  `-q9`/320 kbps is squarely in libvorbis's no-lowpass zone.
- **Our slice window may not cover the one Vorbis cutoff most likely to
  appear in the wild.** Reference libvorbis q5 (~160 kbps, Spotify's
  free/normal tier) cuts at **20.1 kHz** — past `SLICE_FREQS`'s last band
  (19500-20000 Hz). A genuine 160 kbps Spotify rip could plausibly show
  **no cliff at all**, not because it's transparent, but because our
  window ends ~100 Hz short of libvorbis's own filter. aoTuV's q5
  (19.5 kHz) lands just inside the window; reference's doesn't. This is
  the sharpest, most testable finding here.
- **Where a Vorbis cliff IS caught, mapping it through `LAME_LOWPASS`
  systematically overestimates quality**, because Vorbis preserves more
  high-frequency content per kbps than LAME at matched nominal bitrates.
  This isn't cosmetic: `lib/quality/compare.py` folds
  `spectral_bitrate_kbps` into rank comparison via `mp3_cbr` band
  thresholds (128=acceptable, 192=good, 256=excellent, 320=transparent).
  A lawfully-cliffed Vorbis file can read as "transparent" under those
  bands when its true source was materially worse.
- Managed/streaming (ABR) mode reuses the same baseline quality tables
  internally, so the VBR predictions below should transfer to Spotify's
  presumed managed-bitrate delivery — but bit-reservoir constraints can
  still shift the *effective* per-passage cutoff, which one fixed
  30-second window won't average out reliably.

## Encoder landscape: libvorbis, aoTuV, and Spotify

**Reference libvorbis.** `xiph/vorbis/lib/vorbisenc.c` (`vorbis_encode_setup_vbr`/
`_managed` → `vorbis_encode_setup_init`) drives everything off
per-samplerate `ve_setup_data_template` structs (`xiph/vorbis/lib/modes/setup_44.h`
for 44.1/48 kHz), built from a `quality_mapping` array of quality floats
and a matching `psy_lowpass` array —
<https://raw.githubusercontent.com/xiph/vorbis/master/lib/vorbisenc.c>,
<https://raw.githubusercontent.com/xiph/vorbis/master/lib/modes/setup_44.h>.
This is the codec behind `ffmpeg -c:a libvorbis` and most modern Linux
`oggenc` installs — anywhere the tool doesn't call out a "tuned" variant.

**aoTuV.** Aoyumi's fork, "based on Xiph.Org's libvorbis," improves
coding quality mainly via the psychoacoustic model
(`aotuv_hf_weighting` in `psy.c`) and bitrate allocation
(<https://hydrogenaudio.org/index.php/topic,72797.0.html>). Beta2-era
work merged into libvorbis 1.1 in 2004 (commit "Put AoTuV tunings merge
(along with bugfixes) on the mainline" —
<https://github.com/xiph/vorbis/commit/50c1cc2d20>); later merges landed
from 1.3.4 onward (<https://en.wikipedia.org/wiki/Vorbis>). Later betas
(b3 through the long-lived b6.03, "based on libvorbis 1.3.2," refreshed
against 1.3.4-1.3.7 through 2021) were never folded back —
<https://ao-yumi.github.io/aotuv_web/>. aoTuV b4.51 ("Release 1",
recommended through mid-2007) and b5/b6.x were the Hydrogenaudio
default for most of Vorbis's enthusiast era —
<https://wiki.hydrogenaudio.org/index.php?title=Recommended_Ogg_Vorbis>.
`oggenc2.exe` bundled aoTuV-tuned libvorbis and was the standard
command-line frontend for scene/enthusiast rips; some scene command
lines **disabled the lowpass filter entirely**
(`--advanced-encode-option lowpass_frequency=99`, the documented API
disable value — see managed-bitrate section) —
<https://forum.dbpoweramp.com/forum/other-topics/developers-corner/10112-ogg-vorbis-lancer-20060131-release>.
A no-lowpass low-bitrate Vorbis file is real in the wild, not a
theoretical edge case. Full aoTuV source:
<https://github.com/AO-Yumi/vorbis_aotuv>; a "Lancer" patch set on top
(piping/threading fixes only, no psychoacoustic changes) also circulated:
<https://github.com/enzo1982/vorbis-aotuv-lancer>. Vorbis was also a
common in-game audio codec (FMOD/Wwise-era titles bundled `.ogg` assets
directly); scene rippers of games and CDs alike used this same
oggenc2+aoTuV tooling.

**Spotify.** Spotify's own support material (quoted verbatim across
multiple threads) lists three Vorbis quality ratings: "q3 (~96 kbps) ...
q5 (~160 kbps) ... q9 (~320 kbps)" — secondary-sourced, not independently
re-verified against a live Spotify page here, so treat the exact
q3/q2 low-tier mapping as **[unverified]**. The q5/q9 tiers are
corroborated independently: SoundExpert measured Spotify's actual stream
at "Vorbis VBR at 151.1 kbps" (free) and "313.4 kbps" (premium) using
"Xiph's libvorbis 1.3.1 encoder" —
<http://soundexpert.org/articles/-/blogs/spotify-uses-vorbis-q5-and-q9-for-audio-streaming>.
Wikipedia's Vorbis page separately notes "The Spotify audio streaming
service primarily uses Vorbis as well as AAC"
(<https://en.wikipedia.org/wiki/Vorbis>). Two tools capture the *exact,
unmodified* Spotify Vorbis stream (not a re-encode) — the direct source
of any "Spotify rip" `.ogg` a peer might share: **Zotify** ("Free
accounts are limited to 160kbps, while premium accounts can get up to
320kbps," native format Ogg Vorbis —
<https://github.com/zotify-dev/zotify>) and **Soggfy** (intercepts
Spotify's own OGG parser during playback for "an exact copy of the
original file... with no loss in quality"; "160Kb/s or 320Kb/s for free
and premium plans" — <https://github.com/Rafiuth/Soggfy>).

**Legacy YouTube DASH audio.** Before YouTube standardized on Opus,
webm DASH audio-only streams were served as Vorbis under itag **171**
(128 kbps) — confirmed in a live `youtube-dl -F` listing quoted in an
upstream issue: "171 webm audio only DASH audio 162k, vorbis@128k,
2.53MiB" — <https://github.com/ytdl-org/youtube-dl/issues/21509>. A
paired higher-bitrate itag **172** also existed; sources disagree on its
exact rate (192 vs. 256 kbps quoted in different tables) — mark that
number **[unverified]**; itag 171 = 128 kbps Vorbis is solid. Relevant
because cratedigger's YouTube rescue-ingest path pulls audio via
`yt-dlp`, and a sufficiently old cached stream could still hand back one
of these itags rather than Opus/AAC.

## Quality-level lowpass ladder (source-verified)

`xiph/vorbis/lib/modes/psych_44.h::_psy_lowpass_44` (reference libvorbis, current
`master` — <https://raw.githubusercontent.com/xiph/vorbis/master/lib/modes/psych_44.h>)
and the matching array in AO-Yumi's aoTuV mirror
(<https://raw.githubusercontent.com/AO-Yumi/vorbis_aotuv/master/lib/modes/psych_44.h>),
indexed against each project's own `quality_mapping_44`/
`rate_mapping_44_stereo` in `setup_44.h`. The stereo nominal-bitrate
ladder (halve the raw `rate_mapping` array — it's stored
per-channel-equivalent — for the commonly quoted stereo figure) is
**identical** between reference and aoTuV from q-1 upward; only the
lowpass column and aoTuV's extra q-2 step differ:

| `-q` | nominal avg bitrate (stereo) | reference libvorbis lowpass | aoTuV lowpass |
|---|---|---|---|
| -2 | ~32 kbps | *(unsupported)* | 12.9 kHz |
| -1 | ~45 kbps (aoTuV: ~48) | 13.9 kHz | 13.8 kHz |
| 0 | ~64 kbps | 15.1 kHz | 14.7 kHz |
| 1 | ~80 kbps | 15.8 kHz | 15.6 kHz |
| 2 | ~96 kbps | 16.5 kHz | 16.5 kHz |
| 3 | ~112 kbps | 17.2 kHz | 17.1 kHz |
| 4 | ~128 kbps | 18.9 kHz | 18.0 kHz |
| 5 | ~160 kbps | **20.1 kHz** | 19.5 kHz |
| 6 | ~192 kbps | no cap (`48.` sentinel) | no cap |
| 7 | ~224 kbps | no cap (`999.`) | no cap |
| 8 | ~256 kbps | no cap | no cap |
| 9 | ~320 kbps | no cap | no cap |
| 10 | ~500 kbps | no cap | no cap |

Wikipedia independently cites the same 45-500 kbit/s stereo ladder for
`-q-1`..`-q10` (<https://en.wikipedia.org/wiki/Vorbis>), and Hydrogenaudio
separately confirms the aoTuV-b3+-vs-reference 45/48 kbps split at `-q-1`
(<https://wiki.hydrogenaudio.org/index.php?title=Recommended_Ogg_Vorbis>)
— both cross-check the source extraction above.

Two structural notes for calibration: the "no cap" rows use two
different sentinel values (`48.` at q6, `999.` from q7 up) in both
forks' source, but both exceed any real 44.1/48 kHz Nyquist —
functionally both just mean "no encoder-imposed lowpass." And
`_psy_lowpass_44`'s own source comment shows the *previous* table
(11 values, no q-1 row) — the ladder has moved over time even within
"reference," so a pre-1.1 (pre-2004) build could differ. Low real-world
risk: 1.1 shipped in 2004 and essentially nothing still circulating
predates it.

## Managed-bitrate nuances

Vorbis is natively VBR-by-quality (`vorbis_encode_setup_vbr`): request a
quality float, bitrate floats freely. **Managed mode**
(`vorbis_encode_setup_managed`, used for streaming) instead takes
explicit min/nominal/max bitrate targets — nominal-only yields ABR-like
behavior, min=nominal=max forces CBR —
<https://xiph.org/vorbis/doc/vorbisenc/vorbis_encode_setup_managed.html>.
Internally, managed setup still selects its baseline lowpass by locating
the requested bitrate against the *same* `rate_mapping`/
`quality_mapping` tables (`vorbisenc.c` ~lines 880-890: `hi->lowpass_kHz
= setup->psy_lowpass[is]*(1.-ds) + setup->psy_lowpass[is+1]*ds`,
interpolated from the nearest baseline) —
<https://raw.githubusercontent.com/xiph/vorbis/master/lib/vorbisenc.c>.
A managed 160 kbps stream should therefore land on essentially the same
lowpass as native `-q5` VBR — the ladder above should transfer to
Spotify-style ABR delivery, not just command-line `-q` encodes.

Two caveats for Phase 3: (1) **bit-reservoir pressure can still bite
locally** — `bitrate_reservoir`/`bitrate_av_damp` let short-term bitrate
deviate from the average, so a transient-dense passage could get a
tighter cutoff than the nominal-bitrate row predicts, a sparse one a
looser one; our fixed 30-second window samples one point in that
variation, not the track as a whole. (2) **the explicit override is real
and used** — `OV_ECTL_LOWPASS_SET` clamps to `[2, 99]` kHz, and 99 is the
documented way to disable the filter entirely (`vorbisenc.c` ~lines
1165-1172), exposed via `oggenc --advanced-encode-option
lowpass_frequency=` and used by scene groups (cited above). Cutoff
absence is no more proof of quality for Vorbis than for LAME's own
`--lowpass -1`.

## Where the LAME table misreads Vorbis

`estimate_bitrate_from_cliff()` (`lib/spectral_check.py:132-156`) buckets
any detected `cliff_freq_hz` against `LAME_LOWPASS`'s midpoints
(96/112/128/160/192/224/256/320 kbps at 15100/15600/17000/17500/18600/
19400/19700/20500 Hz). Feeding genuine Vorbis cliffs from the table above
through that same bucket math:

| Genuine Vorbis source | Real cliff | LAME-bucket estimate | Error |
|---|---|---|---|
| aoTuV q-2, ~32 kbps | 12.9 kHz | 96 kbps (floor bucket) | **+200%** |
| reference q-1, ~45 kbps | 13.9 kHz | 96 kbps | **+113%** |
| reference q0, ~64 kbps | 15.1 kHz | 96 kbps | **+50%** |
| reference q2, ~96 kbps | 16.5 kHz | 128 kbps | +33% |
| reference q3, ~112 kbps | 17.2 kHz | 128 kbps | +14% |
| aoTuV q4, ~128 kbps | 18.0 kHz | 160 kbps | +25% |
| reference q4, ~128 kbps | 18.9 kHz | 192 kbps | +50% |
| aoTuV q5, ~160 kbps | 19.5 kHz | 256 kbps | +60% |
| reference q5, ~160 kbps | 20.1 kHz | **no cliff in-window** | undefined |
| reference/aoTuV q6+, ≥192 kbps | none (uncapped) | no cliff → genuine | correct |

Every case where a cliff *is* detected reads as a substantially higher
LAME-equivalent bitrate than the true source, because Vorbis preserves
more top-end per kbps than LAME at matched nominal bitrates — the same
efficiency gap that makes Vorbis generally regarded the stronger codec at
a given size. One-directional bias, not noise.

**The q5/160 kbps row is the sharpest concrete example**, because it's
also the single most likely real-world Vorbis population (Spotify
Normal/Free via Zotify/Soggfy, or any native `-q5` rip). `SLICE_FREQS`
stops at `19500-20000` Hz (`range(12000, 20000, 500)`); reference
libvorbis's own q5 cutoff, 20.1 kHz, sits *past* that window.
`detect_cliff()` needs two consecutive steep-gradient slices
(`MIN_CLIFF_SLICES = 2`) fully inside the measured range to fire — if the
real drop only begins at/after 20 kHz, the last slice pair may show only
the leading edge, not enough to trip `CLIFF_THRESHOLD_DB_PER_KHZ =
-12.0`. A genuine 160 kbps Spotify-sourced `.ogg` could plausibly score
`genuine` — not because it's transparent, but because our detector can't
see that far.

**This is decision-relevant, not cosmetic.** `lib/quality/compare.py`
folds a detected `spectral_bitrate_kbps` into rank comparison as a clamp
bound, calibrated against `QualityRankConfig.mp3_cbr`'s bands
("128=acceptable, 192=good, 256=excellent, 320=transparent" —
`lib/quality/compare.py` docstring, ~lines 160-165). A real aoTuV q5
(160 kbps) file with a genuinely-detected 19.5 kHz cliff buckets to
**256 kbps ("excellent")**; a file dodging detection entirely (the
reference q5/20.1 kHz case) skips the clamp and keeps whatever rank its
tag metadata implies. Either way, the pipeline can end up treating a
160 kbps lossy Vorbis source as materially better than it is when
comparing it against a replacement candidate.

## Implications for calibration

A dedicated Vorbis lowpass ladder is viable and cheap to build — the
numbers above are direct source extractions, not estimates, and the
reference-vs-aoTuV spread is small (≤0.9 kHz) everywhere both forks
overlap. Three things make it more than "swap in a second lookup table":

1. **`estimate_bitrate_from_cliff()` has no codec awareness** — called
   with only a frequency, no encoder hint. A Vorbis-specific bucket table
   needs something upstream to know "this cliff came from an `.ogg`
   source"; the extension is known at `analyze_track()` time but isn't
   threaded through to the estimator.
2. **The dangerous gap is the missed-cliff risk at q5/160 kbps** — the
   most common real-world tier — because our slice window ends at 20 kHz
   while reference libvorbis's own filter sits at 20.1 kHz. Extending
   `SLICE_FREQS` by one more 500 Hz slice (to 20500) closes this
   specific gap regardless of whether a full ladder ships.
3. **q6+ (≥192 kbps) is a real "no signal" zone for cliff detection** in
   Vorbis specifically (LAME keeps a real filter through 320). Any
   Vorbis judgment above ~160 kbps must rely on `hf_deficit_db`/natural
   rolloff shape, not `detect_cliff()` — worth confirming in Phase 3
   whether genuine q6-q10 Vorbis ever trips a false "marginal" on
   `hf_deficit_db` alone (smooth psychoacoustic attenuation without a
   hard wall could plausibly cross `HF_DEFICIT_MARGINAL = 40.0` dB on
   fine material — needs real files, not source-reading, to answer).

## Predictions for Phase 3 (testable)

| Source | Expected cliff (Hz) | Our bucket → estimate today | Prediction holds if... |
|---|---|---|---|
| Reference libvorbis `-q0` (~64 kbps) | ~15100 (±300) | <15400 → 96 | Cliff lands 14800-15400 Hz; estimate reads 96, not 64 |
| Reference libvorbis `-q2` (~96 kbps) | ~16500 | 15400-17250 → 128 | Cliff lands 16200-16800 Hz |
| Reference libvorbis `-q4` (~128 kbps) | ~18900 | 18050-19000 → 192 | Cliff lands 18600-19200 Hz |
| aoTuV `-q4` (~128 kbps, oggenc2/aoTuV b4.51+ rip) | ~18000 | <18050 → 160 | Cliff lands 17700-18300 Hz — **aoTuV and reference land in different buckets at the identical nominal bitrate** |
| Reference libvorbis `-q5` / Spotify Normal (~160 kbps) | ~20100 | **likely no cliff** (past window) | **Headline test**: feed a known-160-kbps reference-libvorbis or Spotify-Normal `.ogg` through `analyze_track`; if it grades `genuine` with `cliff_detected=False`, that confirms the window gap and justifies extending `SLICE_FREQS` |
| aoTuV `-q5` / oggenc2 rip (~160 kbps) | ~19500 | 19000-19550 → 256 | Cliff lands 19200-19800 Hz — just inside the window; noisier detection than reference |
| Spotify Premium / Zotify/Soggfy Very-High (~320 kbps, `-q9`) | none (uncapped) | no cliff → `genuine` | Correct if confirmed — verify it isn't landing as a false cliff from container/decode artifacts |
| Legacy YouTube itag 171 (128 kbps Vorbis, webm) | ~18900 (reference) or ~18000 (if aoTuV-tuned) | 18050-19000 → 192, or <18050 → 160 | If `yt-dlp` ever hands back this itag on an old cached stream, same reference-vs-aoTuV bucket ambiguity as the `-q4` row applies |
| Scene rip with `lowpass_frequency=99` (disabled) | none, regardless of bitrate | no cliff → `genuine` | A deliberately low-bitrate (e.g. `-q0`, ~64 kbps) file reads genuine purely because the encoder-side filter was off — same blind-spot class as LAME's `--lowpass -1` |

## Sources

- This repo: `lib/spectral_check.py`, `lib/quality/compare.py`, `docs/quality-ranks.md`.
- libvorbis source, `master`, retrieved 2026-07-23: <https://raw.githubusercontent.com/xiph/vorbis/master/lib/vorbisenc.c>, <https://raw.githubusercontent.com/xiph/vorbis/master/lib/modes/setup_44.h>, <https://raw.githubusercontent.com/xiph/vorbis/master/lib/modes/psych_44.h>
- libvorbis repo + aoTuV-merge commit: <https://github.com/xiph/vorbis>, <https://github.com/xiph/vorbis/commit/50c1cc2d20>
- aoTuV source mirror (AO-Yumi): <https://raw.githubusercontent.com/AO-Yumi/vorbis_aotuv/master/lib/modes/psych_44.h>, <https://raw.githubusercontent.com/AO-Yumi/vorbis_aotuv/master/lib/modes/setup_44.h>, <https://github.com/AO-Yumi/vorbis_aotuv>
- aoTuV project page/changelog: <https://ao-yumi.github.io/aotuv_web/>
- aoTuV+Lancer patch repo: <https://github.com/enzo1982/vorbis-aotuv-lancer>
- Scene-era oggenc2/aoTuV lowpass-disable convention (dbpoweramp forum): <https://forum.dbpoweramp.com/forum/other-topics/developers-corner/10112-ogg-vorbis-lancer-20060131-release>
- Hydrogenaudio, recommended encoder/settings history + aoTuV-vs-reference q-1 bitrate split: <https://wiki.hydrogenaudio.org/index.php?title=Recommended_Ogg_Vorbis>
- Hydrogenaudio forum, aoTuV psychoacoustic-model divergence summary: <https://hydrogenaudio.org/index.php/topic,72797.0.html>
- Wikipedia, Vorbis (nominal bitrate table, aoTuV merge history, Spotify usage): <https://en.wikipedia.org/wiki/Vorbis>
- Xiph libvorbisenc API docs (VBR vs. managed-bitrate setup): <https://xiph.org/vorbis/doc/vorbisenc/vorbis_encode_setup_managed.html>, <https://xiph.org/vorbis/doc/vorbisenc/overview.html>
- SoundExpert, measured Spotify Vorbis bitrates (libvorbis 1.3.1, -q5/-q9): <http://soundexpert.org/articles/-/blogs/spotify-uses-vorbis-q5-and-q9-for-audio-streaming>
- Zotify (direct Spotify-stream Vorbis downloader): <https://github.com/zotify-dev/zotify>
- Soggfy (Spotify OGG stream interceptor): <https://github.com/Rafiuth/Soggfy>
- Legacy YouTube DASH audio itag 171 (128 kbps Vorbis), live `youtube-dl -F` listing quoted in issue: <https://github.com/ytdl-org/youtube-dl/issues/21509>
- Legacy YouTube DASH audio itag 172 (256 kbps claimed; bitrate **[unverified]**, sources disagree): <https://github.com/ytdl-org/youtube-dl/issues/1596>

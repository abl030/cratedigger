# Quality Verification

See also `docs/audio-classification-research.md` for the April 2026 research
log on classifier trust, corpus evaluation, false-positive risk, and external
prior art.

## Gold Standard Pipeline

The highest quality acquisition path for the library:

1. **Download lossless** (FLAC, ALAC, WAV) from Soulseek
2. **Verify with spectral analysis** — confirm the lossless file is genuinely lossless (not a lossy transcode wrapped in a lossless container)
3. **Convert to VBR V0** — `ffmpeg -codec:a libmp3lame -q:a 0`
4. **Import to beets** — the VBR V0 probe remains an auditable source fingerprint

VBR bitrate is useful evidence, not verification by itself. A genuine CD rip
converted to V0 commonly produces ~240-260kbps while a lossy transcode commonly
lands lower, but only an explicit verified-lossless proof completes acquisition,
and that proof lock is absolute in every import mode. The proof requires
affirmative spectral evidence, or the narrow V0 trust override after spectral
analysis ran and disagreed.

## Current Verification Methods

There is no quality acceptance floor. A structurally usable exact-release copy
may be retained regardless of codec, bitrate, or rank. Post-import policy then
has three outcomes: verified-lossless proof completes acquisition; an
unverified `TRANSPARENT` installed copy with its own `genuine` spectral fact
stays wanted but narrows to lossless-only; every other unverified copy stays
wanted on the full search surface.

### Audio integrity full-decode gate

Before spectral or conversion work, `lib.util.validate_audio` enumerates the
stable audio-file set, proves each source readable, and maps only audio through
a complete null-output decode:

```text
ffmpeg -hide_banner -nostdin -v error -max_error_rate 0
  -abort_on empty_output_stream
  -err_detect:a crccheck+bitstream+buffer+explode
  -vn -sn -dn -i INPUT -map 0:a
  -map_metadata -1 -map_chapters -1 -f null -
```

Metadata is deliberately irrelevant. FFmpeg must parse the container to find
audio, but tags, pictures, lyrics, chapters, and exit-zero stderr are neither
classified nor persisted. The validator never rewrites or repairs the source.

The policy interprets process outcomes at the boundary FFmpeg actually
documents: zero is completion; documented exit 69 is a counted decode failure;
another positive exit on unchanged, fully readable bytes is honestly recorded
as `ffmpeg_failed_unclassified`; a validation timeout on those same bytes is
`decode_timeout`. All three are bad audio. In contrast, read/permission
failure, source change, missing FFmpeg, or signal termination means the
measurement itself failed and cannot blame the peer.

Preview persists completed content facts in
`album_quality_evidence.audio_validation`, then the importer alone decides via
`full_pipeline_decision_from_evidence`. Corrupt audio follows the standard
denylist plus `failed_imports/bad_files` quarantine path and resumes searching.
Request-owned processing keeps that quarantine beneath the same private
processing root, so the exact-owner cleanup journal can atomically rename the
complete source directory before terminal acknowledgement even when Incoming
is another filesystem. Other importer lanes retain their configured staging
quarantine. The quarantine plan excludes both post-import Wrong Matches
deletion passes.
A `measurement_failed` attempt writes no denylist and its
retained source path is protected from the disk reaper. Diagnostics are capped
at 16 files and 2 KiB per normalized stderr excerpt; success carries no
stderr.

Lossless conversion uses the same strict input decoder flags. Kept outputs
preserve the source tag surface Beets matches on (`-map_metadata 0`) while
deleting every art-in-tag surface (`METADATA_BLOCK_PICTURE`, legacy
`COVERART`/`COVERARTMIME`; picture streams are already excluded by
`-map 0:a`) — issue #863: Beets must MATCH the staged album on its tags
before it applies fresh canonical metadata, so stripping all tags inflated
apply-time distance, while untrusted embedded art has no business surviving
conversion. The discarded V0 probe output still strips everything.
Conversion stages every derivative on the source filesystem and commits the
album only after every file succeeds with a nonempty output. Any batch failure
removes only temporary derivatives, retains every source, records bounded
`ConversionInfo` diagnostics, and revalidates the retained sources to
distinguish corrupt audio from an encoder/materialization world failure.

### 1. VBR V0 source probe (implemented)

After lossless-to-V0 conversion, the resulting bitrate reveals source quality:
- **Genuine lossless**: ~220-280kbps (varies by musical complexity)
- **Transcode from ~192kbps**: ~190-210kbps
- **Transcode from ~128kbps**: ~160-180kbps

The codec rank bands may use these bitrates for relative comparison, but
`transcode_detection()` has no grade-blind bitrate fallback. Missing or errored
spectral analysis cannot be converted into a positive quality fact. The one
verification exception is the fixed V0 trust override: a comparable source
probe with avg ≥230kbps and min ≥200kbps can override an affirmative
`suspect`/`likely_transcode` spectral disagreement.

Limitation: This source-probe signal exists only when a lossless-container
candidate can be converted or temporarily probed. It does not by itself prove
native MP3 downloads (e.g. 320kbps that was upsampled from 128kbps).

### Provisional lossless-source probes

When a supported lossless-container download (FLAC, ALAC, WAV, or ALAC-in-M4A)
is spectrally `suspect` or `likely_transcode`, the importer no longer has to
discard the source outright. It records the MP3 V0 probe produced from that
source as `lossless_source_v0` attempt evidence and compares the probe average
against the current linked evidence row's V0 metric with `subject='source'`.

Policy:

- Missing current comparable source probe: import as
  `provisional_lossless_upgrade`.
- Candidate probe average above the current comparable probe by more than
  `QualityRankConfig.within_rank_tolerance_kbps`: import as
  `provisional_lossless_upgrade`.
- Candidate probe average equal, worse, or within tolerance: reject as
  `suspect_lossless_downgrade`.
- Missing candidate probe on a suspect lossless source: reject as
  `suspect_lossless_probe_missing`.

Provisional imports are deliberately not verified lossless. They may still use
the configured lossless-source storage target, but `verified_lossless` remains
false, the source is denylisted, normal post-import notifications run, and the
search policy keeps acquisition open. Automation returns the request to
`wanted`; terminal persistence retains an operator search stop current when
the request row is locked, while recording the same quality decision and
narrowing fields.

Exception: a comparable `lossless_source_v0` probe at avg ≥230kbps and min
≥200kbps is treated as stronger evidence than a suspect/`likely_transcode`
spectral grade. Those imports are verified lossless and follow the normal
`imported` path instead of the provisional keep-searching path.

Native lossy and on-disk V0 probes are research evidence only. Active evidence
stores them as `subject='installed', provenance='measured'`; they are not source
anchors and do not affect the provisional comparison lane.

### 2. Spectral Band Energy Analysis (historical v1 method)

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

- **SUSPECT**: cliff detected, OR HF deficit ≥ 69dB
- **MARGINAL**: HF deficit 65-69dB, no cliff
- **GENUINE**: HF deficit < 65dB, no cliff

The 65/69 pair is MEASURED, not tuned (issue #829 Phase 5 PR3). It
replaces the original guessed 40/60, which flagged 4-17% of genuine
lossless CONTROLS as marginal-or-worse across the four calibration arms;
the measured pair flags 1-8% of the same controls. Provenance:
`docs/research/calibration-data/README.md` § "What this data
established". Forward-only — rows already carrying a grade keep the grade
the old thresholds produced.

### Album-level classification

- **LIKELY_TRANSCODE**: >75% of tracks SUSPECT
- **SUSPECT**: >60% of tracks SUSPECT
- **GENUINE**: <60% suspect
- Never auto-reject; flag for review

### Verification and explicit non-verification lanes

1. **Lossless-container downloads**: Run spectral check pre-conversion. Genuine
   or marginal sources continue through the verified-lossless path. Suspect or
   likely-transcode sources still produce a V0 source probe, but they use the
   provisional lossless-source comparison lane instead of becoming verified.
2. **MP3 downloads (especially CBR 320)**: Run spectral check post-download. Cliff + high deficit = upsampled garbage.
3. **High-band native VBR MP3**: The named policy may deliberately skip
   spectral analysis. This is an explicit non-verification lane: bitrate can
   drive relative rank, but it cannot mint verified-lossless proof.

If a candidate-side required scan is missing or errors, preview records
`measurement_failed`; if fresh analysis of an installed HAVE is missing or
errors, dispatch records `have_analysis_error`. Both are environment failures,
not quality verdicts: the request returns to ordinary wanted searching with no
denylist or narrowing consequence. A later attempt measures again from scratch.

## Proof gate v3 — the ultrasonic deficit leg (issue #829 Phase 5 PR3)

### What `verified_lossless` claims

> **No evidence of lossy origin was found by the tests we have** — the
> in-window cliff (production's album spectral GRADE, the union of the
> cliff and HF-deficit legs) and the ultrasonic deficit. **Not** "this is
> bit-faithful to a lossless source."

That wording is deliberate and binding. The Apple/CoreAudio family
applies essentially no lowpass in the measured band (2.1 dB down at
21.5 kHz), so no spectral leg has anything to see: measured on 17 paired
launders, 10 reach proof and the conditional false-accept is 91-92%. Two
further 2026-07-31 threads proved no *spectral* instrument separates the
class (`docs/research/calibration-data/homogeneity/`,
`.../shape-analysis/`). The claim is bounded rather than abandoned
because its failure modes are now **enumerated** rather than unknown.
Copy must never widen it to "guaranteed bit-perfect".

### The leg

`U = mean_over_tracks[ref_db(1-4kHz) − mean(20.5-22kHz)]`, measured by
`lib/spectral_check.py` and persisted album-level as
`ultrasonic_deficit_db`. Promotion is denied when `U ≥ 59.5`.

Normalising against the album's OWN midband is what makes the statistic
comparable across masters — in absolute dB a quiet record's genuine
ultrasonic content is indistinguishable from a loud record's launder
leakage. It replaced v2.1's RELATIVE affirmative-content test, which
conflated "no gap" with "this album never had the dynamic range to show
one" and promoted 12 FLAC-container launders on exactly such albums.

**A denial is not a rejection.** The album imports normally, carries no
spectral verified-lossless proof, and stays on the search surface.

The boundary is exactly that, and it is worth stating precisely because
one review round got it wrong. The leg's authority is the PROOF: it is
read at `determine_verified_lossless` and `mint_verified_lossless_proof`
and nowhere else. It must NOT reach the V0 trust override
(`v0_verified_override` in `lib/quality/pipeline.py`,
`v0_verified_lossless_override` in `harness/import_one.py`), because that
flag routes the import — it decides whether the album is compared on its
measured quality or handed to the provisional-lossless lane, three of
whose four outcomes are confident rejects that also denylist the offering
peer. A denial that re-routed that choice would discard albums on exactly
the HF-poor genuine-lossless cohort the V0 probe exists to rescue. The
override is therefore keyed on the probe alone, and the harness derives it
from a second, leg-free `determine_verified_lossless` call.

What a denial DOES cost, beyond the proof, is everything the proof
licensed: no `verified_lossless_target` conversion (the album keeps its
V0 grind — a higher-bitrate artifact, and the decider gates the
post-import quality gate's format on the same fact), no verified-lossless
bypass in the measured comparison, and a post-import gate that requeues
instead of accepting. So an unproved candidate compared against an
EQUIVALENT-OR-BETTER installed copy can still lose that comparison as an
ordinary `downgrade` — the proof is the tie-breaker on an `equivalent`
verdict, and in every measured world that is the mechanism (identical
ranks on both sides), not a rank loss. That is the proof doing its job, not
the leg passing a verdict: the existing copy is untouched and the request
stays searchable.

### Threshold provenance

Frozen 2026-07-31 by re-running the frozen scorer's own leg logic
(`docs/research/calibration-data/score_v3.py.frozen`) over all four
committed arms at production's single measurement window — 100 genuine
controls, 300 FLAC-container launders:

| T | leaked launders | genuine denied |
|---|---:|---:|
| 57.0 | 0 | 43/100 |
| **59.5** | **0** | **34/100** |
| 61.5 | 0 | 28/100 |
| 62.0 | 1 | 26/100 |

The binding minimum — the lowest `U` over launder albums where this leg
is the last line — is 61.55 dB, so 59.5 carries 2.05 dB of headroom.

**Known residual.** Production evaluates two of the frozen scorer's three
legs: its cliff leg is the album spectral grade, and it has no ceiling
leg (which needs per-track slice vectors production does not persist).
Against that reduced set one launder of 300 survives above 57.03 dB
(TRAINING / `t-vorbisq5-flac` / Autechre, caught by the ceiling leg
alone). Dropping the threshold to catch it would leave 0.03 dB of
headroom — a coincidence, not a margin — and deny 9 more genuine albums
per 100.

### Decode-path scope

`ultrasonic_deficit_db` is **not comparable across decode paths**. The
same bits measure 50.26 dB through `_ffmpeg_to_wav` at 48 kHz versus
47.17 dB sox-native at 44.1 kHz — a +3.09 dB skew, larger than the whole
2.05 dB margin (isolated on request 8923, the only
`was_converted_from='alac'` control). The threshold was frozen on
sox-native decodes, so the leg refuses to gate a value from any other
path, in either direction. The sox-versus-ffmpeg routing table lives in
`lib/quality/filetypes.py` and both the analyzer and the gate read it, so
the router and the threshold's scope cannot drift apart.

### `ultrasonic_deficit_db IS NULL` is three states

They are named separately in the decision path and never conflated:

| state | reason | why |
|---|---|---|
| R19 preserved source | `preserved_source_spectral` | a converted copy wearing its source's pre-capture spectral; the source was converted away, so it can never be re-measured |
| legacy | `legacy_measurement` | measured before the capture shipped; re-measurable in principle |
| unmeasurable | `not_measured` | the capture ran and honestly reported no value — the 20.5-22 kHz bands were outside the file's Nyquist |

All three **withhold**: the leg asserts nothing and every pre-v3 rule
applies unchanged. Withholding is neither evidence nor denial, and most
of the library is permanently in that state.

### `verified_lossless_classifier` names the model

The classifier names WHICH MODELS proved a row. The full ladder, including
the AAC frame-lattice leg, is in § "`verified_lossless_classifier` with two
legs" below; the ultrasonic leg's own rung is:

- `spectral_verified_lossless` — the cliff/grade gate alone.
- `spectral_verified_lossless_v3` — the ultrasonic leg ADJUDICATED and
  passed.

v3 is never stamped merely because v3 code ran; a withheld leg proved
nothing new. `spectral_measurement_version` is deliberately not this
axis — it is a measurement-shape version, and 47 live proofs carry
version 2 while having been proved under the old gate.

**Existing stamps are never retroactively demoted.** A row whose source
is gone cannot be re-proved and must not thereby lose the proof it holds;
proof carry-forward preserves it verbatim.

### Measuring a change to this policy

`scripts/decision_differential.py` re-decides real persisted
`album_quality_evidence` rows through the real decider and reports
changed rows by field — the decision-level sibling of
`scripts/render_differential.py`, sharing its diff engine and its
two-tree runbook (recipe in the module docstring). A quality-policy
change is measured on the live corpus, not reasoned about from the diff.

Two flags decide whether the run can SEE a promotion-gate change, and a
zero from a run without them is not evidence:

- `--counterfactual` drops each candidate's persisted proof first. A row
  that already holds a proof never runs the promotion the gate governs,
  so the as-persisted arm's honest answer for a proof-gate change is zero.
- `--pair-with` supplies the installed album's evidence per candidate
  (`{"id": <candidate id>, "current": {<evidence row>}}`), the way
  production decides. Unpaired, every row is a fresh request with nothing
  installed, and no branch that compares against a HAVE — including the
  provisional lane's confident rejects — is reachable at all.

## AAC frame-lattice leg (issue #829 AAC-lattice leg)

The fourth proof leg. PR-A measured and persisted the capture; PR-B reads
it as policy.

Every spectral leg above measures the *shape of the spectrum*, and the
Apple/CoreAudio family defeats all of them by applying essentially no
lowpass in the measured band. The Derrien detector measures something
else entirely: AAC quantises MDCT coefficients on a fixed 1024-sample
frame grid, and re-analysing the decoded PCM at the *right* offset
recovers scalefactor-band structure that audio which was never
AAC-encoded does not have. A genuine file's sweep over all 1024 candidate
offsets is flat; an AAC-derived file's sweep spikes at the true offset.
qaac/CoreAudio primes 2112 samples, so its lattice lands at
`1024 - (2112 mod 1024) = 960`; ffmpeg's native AAC primes 1024 samples
and lands at 0; a genuine album's tracks share no such constant.

`lib/aac_lattice.py` is the typed production port of
`docs/research/calibration-data/derrien/aacdet.py.frozen`, held to that
frozen port's own mathematical self-validation by
`tests/test_aac_lattice.py`. It computes `mode=high` frame selection only
and drops the NAC probe — both measured dead in
`docs/research/calibration-data/derrien-refinement/`. numpy is a
production dependency for it; scipy deliberately is not (`erfinv` is
bisection over `math.erf`).

**The cohort gate is a cost decision, not a policy one.** The measurement
costs on the order of tens of seconds of CPU per track, so
`lib/measurement.py::measure_preimport_state` runs it only when the
candidate fileset contains lossless containers AND the album spectral
grade is `genuine`/`marginal` — the promotion-plausible cohort, which is
exactly where an AAC launder that already survived every spectral leg
would be. Tracks are selected in sorted relative-path order and scoring
stops at 6 successful tracks.

**A detector failure is evidence, never a failure of the preview job.**
96 kHz input has no scalefactor-band table at all and cannot be scored;
the rate is pre-screened with ffprobe so such a track costs a probe rather
than a full decode. That, an undecodable file, and any detector exception
are recorded per track. A failure of the measurement itself is logged and
costs the album its lattice and nothing else.

**Absolute modal offsets are decode-path-relative.** The 960 / 0 values
above were calibrated through FLAC decodes. A container whose decoder
applies encoder-delay priming — ALAC/M4A in particular — shifts the sample
origin, so its recovered offsets are not directly comparable to those
constants. Offset *concentration* (k tracks sharing one offset) is
unaffected, which is the statistic the deployable rule uses; a rule keyed
on the literal value 960 would not be. **The leg therefore never reads an
absolute offset**, and `AacLatticeProofLeg` does not even carry one.

Persistence (`album_quality_evidence`, migration 069): `aac_lattice_tracks`
is the per-track JSONB array (filename plus `offset`/`z`/`proba`, or an
`error` string); `aac_lattice_modal_offset`, `aac_lattice_modal_count`,
`aac_lattice_scored_tracks` and `aac_lattice_max_z` are the album
statistics broken out for SQL. NULL across all five means **never
measured**; `aac_lattice_scored_tracks = 0` means **measured and nothing
scored**. The capture follows the V0 tuple's preserve guard — a
same-address writer that never ran the cohort gate does not erase it.

### The proof leg (PR-B)

`lib/quality/decisions.py::aac_lattice_proof_leg` is the only thing that
reads the capture as policy. Two independent denial conditions, either
sufficient:

| condition | constant | measured basis |
|---|---|---|
| `modal_count >= 4` | `AAC_LATTICE_PROOF_DENY_MODAL_COUNT` | parameter-free; analytic FP floor ~0.0023 albums per 5000, 0/17 genuine control albums, 17/17 recall on four `qaac` variants and 16/17 on the fifth |
| `max_z > 12` | `AAC_LATTICE_PROOF_DENY_MAX_Z` | 0/197 genuine control tracks, 0/1136 wild peer tracks, ~0.1 FP albums per 5000 under the Gumbel tail fit |

A denial fires on **any** amount of scored evidence: `modal_count` is
bounded by `scored_tracks` so the concentration rule implies four scored
tracks by construction, but the contrast rule does not and must not — one
track at z = 28 is evidence.

A **pass** needs `scored_tracks >= 4` (`AAC_LATTICE_PROOF_MIN_SCORED_TRACKS`,
deliberately the same integer): below that the concentration rule could
not have fired whatever the audio was, so "it did not fire" is not a
finding. That world **withholds**, as does an unmeasured row.

Withheld asserts nothing in either direction. That is where essentially
the whole library sits — the capture is gated to the promotion-plausible
cohort and no row measured before it shipped has one — and a withheld leg
leaves the entire decision bit-identical.

**The leg's authority is the proof, never the lane.** A denial withholds
the verified-lossless proof and nothing else. It never re-routes the
provisional-lossless lane — whose decisions are confident rejects that
denylist the offering peer — and it introduces no denylist class of its
own: every denylist a denied album attracts is the pre-existing
post-import policy any unproved album attracts.

What a denial *does* cost is the proof's comparison privilege, and that
is not a leg verdict. An album with no proof is compared on what it
measures, so against a better installed copy it can lose that comparison
and not be imported — in the MEASURED lane, with a comparison basis the
operator can read, exactly as any unproved album can. That residual is
byte-identical to the ultrasonic leg's and is pinned as such
(`tests/test_quality_classification.py::TestAacLatticeProofGate`,
`tests/test_quality_generated.py::TestAacLatticeProofLegProperties`);
the lane boundary is the one PR3 shipped a blocking defect on, and this
leg is pinned against it from birth.

**Where the leg is evaluated.** Both decision twins build it once per call
from the candidate's persisted capture. The dry-run harness — which is
what mints the proof that becomes the candidate evidence row's — reads the
capture off the preview sidecar it already decodes for its spectral grade
(`lib/import_preview.py::_write_preview_spectral_evidence_file` →
`harness/import_one.py::_preview_candidate_evidence`). It never measures
the lattice itself. A legacy non-preview harness call, a non-lossless
candidate, and a candidate outside the cohort gate all have no capture on
that wire, and all withhold.

### `verified_lossless_classifier` with two legs

- `spectral_verified_lossless` — the cliff/grade gate alone.
- `spectral_verified_lossless_v3` — the ultrasonic leg adjudicated and
  passed; the lattice leg did not adjudicate.
- `spectral_verified_lossless_v4` — **both** legs adjudicated and passed.

A lattice pass with no ultrasonic adjudication is the base name, not v4:
the names are a ladder of what was tested and skipping a rung must not buy
the top one. As with v3, a name is never stamped merely because the code
ran.

### Tuning results (Mountain Goats library, 65 albums) — HISTORICAL

Tested across the entire Mountain Goats catalogue — a worst-case scenario as the band's early work (1991-2000) was recorded on boomboxes and cassette recorders with genuinely minimal high-frequency content.

**Superseded by the four-arm calibration**, which is why the thresholds
moved to 65/69: this 65-album single-artist corpus is what the guessed
40/60 pair was fitted on, and it could not see the 4-17% control
false-flag rate the 60,102-measurement corpus exposed. Retained as the
provenance of the numbers it produced.

At the historical `HF_DEFICIT_SUSPECT=60dB + cliff detection`:
- **19 correctly flagged SUSPECT** (confirmed bad source, transcodes, or upsampled 320s)
- **46 correctly GENUINE** (including lo-fi albums with good V0 conversion bitrates)
- **0 false positives** on albums with verified good sources
- Successfully catches: cliffs at 16kHz (128kbps transcodes), cliffs at 18kHz (192kbps), upsampled CBR 320, terrible pre-pipeline rips

Historically, albums downloaded as FLAC and converted to high-bitrate V0 with
no cliff passed this corpus. That observation informs the V0 override, but
bitrate alone is not proof in the current policy.

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

## Edge cases

- **Lo-fi recordings** (Mountain Goats boombox era): genuine V0 from verified FLAC can produce ~207 kbps. The `"mp3 v0"` label can still classify as `TRANSPARENT`, but rank never completes acquisition. Proof completes it; an unverified transparent installed copy narrows to lossless-only only when its own spectral grade is `genuine`.
- **Mixed-source CBR** (e.g. 13 tracks at 320 + 1 track at 192): looks like VBR to `COUNT(DISTINCT bitrate)` but is not genuine V0. There is no acceptance floor and no grade-blind CBR narrowing. Unless the installed result is both `TRANSPARENT` and spectrally `genuine`, it remains wanted on the full search surface.
- **Fake FLACs**: MP3 wrapped in a lossless container. Spectral detects the
  cliff pre-conversion, and the V0 probe becomes comparable source-subject
  evidence. Source denylisted, file imported only as provisional when the probe
  is meaningfully better than the current comparable source probe, and the
  request stays wanted.
- **Discogs-sourced albums**: numeric IDs stored in `mb_release_id` for pipeline compat. Beets auto-routes numeric IDs to the Discogs plugin via `--search-id`. `detect_release_source()` in `lib/release_identity.py` distinguishes UUID vs numeric format for conditional UI rendering. The full pipeline (search, download, validate, import, quality gate) works identically for both sources.

## Downgrade prevention

- `--override-min-bitrate` arg: preview/dispatch derive the comparison floor from linked current evidence or the same attempt's fresh HAVE audit. When spectral says the installed files are 128 kbps but the container says 320 kbps (fake CBR), the spectral truth is used so genuine upgrades are not blocked. Request-row quality stamps never feed this value. **This existing-side spectral floor is one-sided, so it applies only when the symmetric clamp does NOT govern the pair (issue #813 Finding 1). The disarm predicate is exactly `_shared_spectral_bitrates`' firing condition — `spectral_classes_comparable` since issue #829 Phase 5 PR2b — and that identity is the argument: the override is safe to drop only because something else then represents the installed album by its real content. When the clamp governs, adding the one-sided override on top would compare the candidate's raw container bitrate against the existing's spectral floor and mint a phantom "better" for an identical transcode (Deerhunter *Rhapsody Original*, download_log 37725: a 256/spectral-192 candidate scored an upgrade over an identical 256/spectral-192 installed copy). Disarming on the WIDER "both sides have a class" would open a window where neither mechanism fires and a known-fake installed copy keeps its inflated container — download_log 29525, Clue to Kalo *Lily Perdida*: a CBR-320 HAVE graded `likely_transcode` with a cliff-derived class of 128 blocking a genuinely better VBR 234 candidate. Deerhunter is unreachable through that window: same codec, same basis, i.e. comparable.** The floor itself only ever consumes a class `lib/quality/spectral_interpretation.py` calls decision-grade — an invertible ladder (MP3, Vorbis q0–q4) with an authorizing album verdict. An AAC's natural rolloff, an Opus stream, an HE-AAC stream or an unresolved codec contributes nothing and the container bitrate stands (download 37946).
- `ImportResult.verified_lossless_proof` is the sole acquisition claim. `AudioQualityMeasurement` contains only byte observations; evidence persistence derives its CHECK-tied convenience boolean from proof presence rather than re-deriving verification from a measurement.
- Spectral request-state writes always go through `RequestSpectralStateUpdate` so the historical grade/bitrate stamps stay atomic. Active decisions use the linked evidence row's spectral fact, not those request scalars.
- `--target-format` flag: when `target_format="lossless"` (or legacy `"flac"`), keeps lossless on disk. ALAC/WAV sources are normalized to FLAC via `FLAC_SPEC`. A temporary V0 probe is still produced when needed for provisional source comparison. Keeping a lossless container does not itself verify it; the import needs affirmative proof.
- `--verified-lossless-target` flag: target format after verified lossless, and the configured lossless-source storage target for accepted provisional imports (e.g. "opus 128", "mp3 v2", "aac 128"). Passed from `dispatch_import_core()` when `cfg.verified_lossless_target` is set. When the target has the same `.mp3` extension as V0, V0 files are removed before target conversion.
- `--force` flag: skips the distance check (`max_distance=999`) for force-importing rejected albums. Used by `pipeline_cli.py force-import` and `POST /api/pipeline/force-import`.
- Exit codes: 0=imported, 1=conversion failed, 2=beets failed, 3=path not found, 5=downgrade or suspect-lossless rejection, 6=transcode/provisional path (may or may not have imported as an upgrade).

## Comparison basis — the persisted decision explanation

Every `compare_quality()` call returns a `QualityComparisonBasis`
(`lib/quality/evidence_types.py`): the verdict plus which branch fired
(`rank`, `spectral_tiebreak`, `spectral_candidate_bound`, `metric_tiebreak`,
`label_contract_same_rank`,
`cross_family_same_rank`, `lossless_same_rank`, `metric_missing`,
`transcode_rank_regression`), the per-side ranks, the values that decided
that branch (spectral-clamped values on a clamped rank comparison or
`spectral_tiebreak`, the candidate's own class against the HAVE's raw
metric on `spectral_candidate_bound`, raw configured-metric values on
`metric_tiebreak`), and
the per-side statistic actually classified (`min`/`avg`/`median` — the
configured metric falls back to min when unmeasured). An explicit codec
label such as `opus 128` is instead persisted as `contract`: the label's
declared bitrate is policy, not a measured statistic. A temporary V0 probe
may still inform source quality, but it never becomes an `OPUS`
measurement. `import_quality_decision()` stamps
`verified_lossless_bypass=True` only when the bypass changed the outcome
(an "equivalent" verdict imported).

The basis rides `MeasuredImportDecisionResult.comparison_basis` →
`ImportResult.comparison_basis` (harness stdout + `download_log.import_result`
JSONB), the decision dict's `comparison_basis` key (as `msgspec.to_builtins`
plain dict — the dict crosses json.dumps'd API responses), the evidence
action file, and the dispatch-synthesized reject `ImportResult`. Re-typing
back from the dict goes through `comparison_basis_from_decision()` — the one
converter.

### Codec-aware spectral participation (issue #829 Phase 5 PR2b)

Every decision seam that consumes a spectral number now consumes a
**decision-grade class** from `lib/quality/spectral_interpretation.py`, not
the raw `spectral_bitrate_kbps` column. `decision_class_kbps()` is the one
accessor; `None` means the spectral leg withholds, and a withheld opinion
falls through to rank and the other evidence — it is never a rejection and
never an accusation. Two consequences are worth stating separately because
both changed shipped behaviour:

**The shared clamp is no longer grade-tolerant.** It used to fire on any
two spectral estimates, on the theory that two independent measurements
agreeing is corroborating evidence. The four-arm calibration measured what
those estimates actually are on an album production already graded
`genuine`: natural rolloff, the false positive this whole project exists to
remove. A class now exists only when the album verdict authorizes a
spectral finding — the same `SPECTRAL_TRANSCODE_GRADES` gate
`compute_effective_override_bitrate` always applied — so two `genuine`
albums are not clamped at all and their raw metrics decide. This resolves
an inconsistency rather than adding one: the two mechanisms disagreed about
the grade before.

**One asymmetric bound exists: `spectral_candidate_bound`** (issue #911,
request 8902 Iron & Wine *Fall 2007*). When a candidate carries a
decision-grade transcode class and the current copy is KNOWN non-transcode
with no class of its own, the candidate is bounded by its own class and the
verdict is decided on **rank alone** — imported only when the bounded rank
is strictly better than the current raw rank. Without it, a fake CBR 320
whose measured cliff puts its real content at 160 manufactures a
`transparent` rank, displaces a genuine 160, and is displaced back forever.
Five gates keep it narrow (accusation-capable decision-grade candidate;
bare measured codec label, not a contract; HAVE with an affirmative
non-transcode grade; HAVE with no class; and the bound must actually bind).
The branch clamps only the CANDIDATE's displayed value — the HAVE keeps its
real measured statistic, and the renderers are per-side accordingly.

### Stage 1 / Stage 2 parity (issue #813 Finding 1)

`spectral_import_decision` (Stage 1, pre-import spectral gate,
`lib/quality/decisions.py`) and `compare_quality` (Stage 2, the full
codec-aware comparison this section documents) are two separate decision
surfaces over the same evidence. Stage 1's only operative effect is its
`"reject"` verdict, which short-circuits `full_pipeline_decision` /
`full_pipeline_decision_from_evidence` before Stage 2 ever runs — every
other Stage 1 verdict defers unconditionally. A generated property
(`tests/test_quality_generated.py::TestGeneratedSimulatorInvariants
::test_stage1_never_contradicts_stage2`) drives both deciders directly over
the same evidence and asserts Stage 1 never rejects a candidate Stage 2
would score `"better"` — the audit found and fixed two independent gaps,
both inside the shared spectral clamp (`_shared_spectral_bitrates`, which
since issue #829 Phase 5 PR2b fires when the two sides' spectral CLASSES
are comparable — `spectral_classes_comparable` — rather than merely when
both carry a `spectral_bitrate_kbps`):

1. **Same-rank tiebreak** (`spectral_tiebreak` branch). The coarse
   `QualityRank` band can bucket two genuinely UNEQUAL clamped spectral
   values together; before this fix the same-rank tiebreak fell through to
   the fully-unclamped raw metric, which can reverse a real spectral
   ordering (a worse-spectral candidate winning purely on a higher declared
   container). A TRUE spectral tie (clamped values EQUAL) still defers to
   the raw metric exactly as before — this is what lets Mark DeNardo
   (request 1308, tied spectral 128==128) still import on its higher raw
   container. **Requires BOTH sides spectral-bound** (`spectral <= raw` on
   each side individually) — a PR #827 review finding: gating on only
   `rank_new_value != rank_existing_value` without consulting which side is
   actually bound turns the branch into a stealth `metric_tiebreak` with no
   `±5kbps` tolerance whenever one (or neither) side is bound, since the
   "clamped" value on an unbound side is just its raw metric.
2. **CBR/VBR band-table mismatch** (`rank` branch). An MP3 class ladder is
   calibrated to `QualityRankConfig.mp3_cbr`'s thresholds
   (128=acceptable, 192=good, 256=excellent, 320=transparent), not
   `mp3_vbr`'s more generous ones. A side whose clamp is spectral-bound now
   classifies via CBR bands regardless of that side's own `is_cbr` — but
   **only when BOTH sides are bound** (another PR #827 review finding)
   **and only for MP3** (issue #829 Phase 5 PR2b): forcing CBR on one bound
   side while an unbound side keeps its own (possibly more generous VBR)
   table mixes a spectral-calibrated number against a raw-metric number
   under two different band tables, which can itself invert the ordering,
   and only MP3 routes on `is_cbr` at all. A side whose clamp did NOT bind
   (raw is the tighter value) always classifies with its own encoding mode.

#### The cross-codec domain (issue #829 Phase 5 PR2c)

**Scope: this closes the cross-codec half of #828 item 1.** That item names
two deliberately-unpatrolled classes. The second — unbound /
self-inconsistent evidence, where a side's raw container measures lower
than its own spectral estimate — is untouched and remains recorded-only;
the paragraph beginning "Stage 1 remains load-bearing" below is its record.


That property's world space is same-codec by construction. The exclusion
used to be justified by "the spectral bucket table is MP3/LAME-calibrated
and the preimport gate only fires on MP3-shaped candidates, so a
cross-codec spectral pairing is itself evidence of a mismatch, not an
independent decision-logic gap". **Issue #829 falsified that.**
`collect_attempt_spectral_audit` measured every codec through the LAME
table and persisted the result as decision-facing evidence, so an ordinary
fresh measurement produced exactly that pairing routinely — download 37946
is a 256 kbps AAC whose natural rolloff read as "MP3 128 transcode" and
drove a live cross-codec clamp. It *was* an independent decision-logic gap.

The four-arm calibration then settled the question the old scoping called
out of scope: **there is no common currency**, so the comparison is refused
rather than rescaled. A 17 kHz cliff means ~160 kbps in MP3 and 256–320 in
AAC. `docs/research/spectral-calibration-findings.md` states the resulting
rule for this property verbatim — "cross-codec spectral comparison is
undefined and fails closed", not a translation table.

The domain is patrolled by a second property,
`TestGeneratedSimulatorInvariants::test_stage1_never_consumes_an_inadmissible_existing_class`,
over `inadmissible_spectral_pair_worlds` — worlds whose two classes
`spectral_classes_comparable` refuses, in each of its three reasons
(`cross_codec_legacy_bucket`, `mixed_derivation_basis`,
`right_not_decision_grade`). The invariant is admissibility, not ordering:

> **Stage 1 must not reject on a spectral comparison Stage 2 is not
> permitted to make**, and an inadmissible existing-side class must move
> nothing at Stage 1 — the verdict has to equal the verdict the same world
> produces with that evidence absent entirely.

Two things follow. The first clause forbids `stage1 == "reject"` outright
on that whole domain, which *subsumes* the older no-contradiction checker
there for every possible Stage 2 — that checker's antecedent can never
hold. And the property drives `full_pipeline_decision` itself, because the
Stage-1 seam (which class reaches `spectral_import_decision`) is the thing
under test.

Until issue #829 Phase 5 PR2d it was the only Stage-1 property that did.
The no-contradiction property's harness reproduced that wiring inline —
forced to, because it needs a Stage-2 verdict in exactly the worlds where
Stage 1 short-circuits and production never computes one — and was
therefore blind by construction to every mutant planted in the seam. PR2d
removed the reason: the decider now reports the Stage-2 counterfactual
itself (see below), so both properties drive the same owner and differ in
*invariant*, not in fidelity.

Note for anyone re-reading the older scoping: `cross_family_same_rank`
returning `"equivalent"` unconditionally is a fact about **that branch**,
not about cross-codec worlds. That branch only fires at the *same* rank; a
cross-codec pair at different ranks takes `rank`, and
`spectral_candidate_bound` and `metric_tiebreak` are reachable cross-codec
too — all three can emit `"better"`. The negative is a code fact rather
than a sample: only `cross_family_same_rank` hardcodes `"equivalent"`.
Measured over a 46,286-world sweep of MP3-candidate worlds with the
pre-PR2b Stage-1 seam simulated, 1,142 worlds flipped Stage 1 to
`"reject"` and 326 of those carried a Stage-2 `"better"`.

Stage 1 remains load-bearing and was NOT folded into Stage 2: the property
is deliberately scoped to internally-consistent evidence (`spectral <=` the
side's own raw metric, the domain `_shared_spectral_bitrates` assumes — see
`StageParityWorld`'s docstring). A self-inconsistent existing measurement
(raw container measured LOWER than its own spectral estimate) is outside
that domain and is a residual case only Stage 1's coarse spectral-vs-
spectral comparison protects — this shape is reachable from an ORDINARY
FRESH measurement, not only cross-snapshot carry-forward: `analyze_album`
(`lib/spectral_check.py`) aggregates the album-level spectral estimate as
`min()` over only the tracks with a detected cliff (container-independent
per-track buckets), computed independently of the album's overall grade
threshold, so a single outlier track's cliff can produce a spectral
estimate well above the album's real average container bitrate even while
the album's overall grade stays "genuine". Likewise a candidate with no
existing spectral estimate at all (`spectral_import_decision` returns
`"import_no_exist"`, deferring by design — absence of a measurement is not
evidence the installed copy is genuine).

#### The Stage-2 counterfactual (issue #829 Phase 5 PR2d)

"Stage 1 rejected this, and Stage 2 would have said *better*" is the exact
disagreement issue #813 is about — and until PR2d it was computed nowhere,
so no operator surface could show it. `full_pipeline_decision` now runs
Stage 2 on the short-circuit path too and reports the result under two
audit keys on the decision dict (both twins, every path):

| key | meaning |
| --- | --- |
| `stage2_import_if_stage1_deferred` | the Stage-2 decision the same world reaches once Stage 1's short-circuit is lifted |
| `comparison_basis_if_stage1_deferred` | that run's full `QualityComparisonBasis`, as JSON-plain builtins |

Both are `None` on every other path. **They are reporting, never a decision
input** — no branch anywhere reads them. The counterfactual decides on a
throwaway result dict and exactly those two values are lifted back, and the
early return happens where it always did.

A short-circuit **always** reports a decision, even when Stage 2 cannot be
evaluated at all: a `ValueError` from the tail is swallowed (a reporting
field must not be able to turn a clean Stage-1 reject into a crash) and
reported as `STAGE2_COUNTERFACTUAL_UNAVAILABLE`. That sentinel exists
because `None` already means "Stage 1 never short-circuited" — "the audit
could not run" is a different fact and the operator is entitled to both.
The basis is exempt and stays `None` whenever the counterfactual never
reached a comparison (the provisional lane, the lossless-source lock),
which is a real outcome rather than a failure.

On the lossless-source branches the counterfactual reached through the
evidence entrypoint is *always* the provisional lane, and that is a
property of the decider rather than of the tests: Stage 1's carve-out
(`provisional_source_candidate and has_provisional_probe_input`) spares
every lossless-source candidate that has probe evidence, so the only ones
that short-circuit are the ones with none.

`pipeline-cli quality <id>` prints an `if stage 1 had deferred:` line under
the chain, and the values-mode preview API returns the whole decision dict.
The web forensics card's `stage_chain` rows are unchanged — both producers
(`lib/import_preview.py::_stage_chain_from_simulation` and
`lib/wrong_match_cleanup_service.py::_stage_chain_from_decision`) enumerate
fixed key allowlists, so the new keys cause no persisted-JSONB drift;
surfacing the counterfactual there is a follow-up.

The invariants ship as pin+property pairs:
`tests/test_quality_classification.py::TestStage2CounterfactualAudit` and
the three `TestGeneratedSimulatorInvariants` properties named
`test_the_counterfactual_is_reported_exactly_when_stage1_short_circuits`,
`test_the_stage1_reject_decision_is_unchanged_by_its_audit` and
`test_the_reported_counterfactual_is_what_stage_2_decides`.

Version 4 import results persist five disjoint concerns:

- `source_measurement` is measured from the downloaded bytes before mutation;
- `verified_lossless_proof` is the optional acquisition claim, deliberately
  separate from every measurement;
- `v0_probe` is the temporary research/provisional encode;
- `target_quality_contract` is configured policy used explicitly by comparison
  and gate ranking. It owns the target bitrate mode (`is_cbr`) as well as its
  label, so source/output CBR observations cannot change projected MP3 rank;
  bare `MP3` therefore requires an explicit projected or materialized CBR/VBR
  fact, while labels such as `mp3 v0`, `mp3 320`, and `opus 128` remain
  self-describing; and
- `materialized_measurement` is built from the postflight Beets album info
  after conversion and import. It records the actual stored codec plus
  min/avg/median bitrate. This is deliberately separate from
  `comparison_basis` (the policy explanation), and `v0_probe` (a temporary
  research encode). Audit UIs must use the materialized measurement for claims
  about output bytes and must leave historical output unknown when that field
  is absent. V1/v2 rows pass through the explicit legacy projection and carry
  `legacy_projection_version`; only that quarantined reader preserves their
  historically ambiguous `new_measurement` shape.

Every new measured format is a bare codec label (`FLAC`, `MP3`, `AAC`, `Opus`,
`Vorbis`, `WMA`):
profile/bitrate labels such as `mp3 v0` and `opus 128` belong only to the
target contract. Source measurements cannot carry `was_converted_from`; that
field describes materialized output lineage. These rules are enforced at the
v4 wire decoder/encoder and again before evidence persistence. Active evidence
rows carry `lineage_version=4`: spectral and V0 facts add `subject`
(`installed` | `source`) and `provenance` (`measured` | `carried`), while
verified-lossless lives only in its proof object. Migration 055 maps old field
names best-effort; current-evidence loaders treat v1/v3 rows as rebuild-required
rather than guessing v4 meaning. Preview reuses a decision-usable spectral fact
only after exact-release and snapshot authorization; missing, unusable, or
changed HAVE evidence is remeasured. The import action independently
reauthorizes the exact installed Beets album before deciding. A same-snapshot
repair preserves its original `measured_at` and atomic neutral facts so
historical Recents cards remain pre-attempt evidence.

Beets's native `items.format` is normalized only at the library projection
boundary: its observed `OGG` label becomes bare `vorbis`, and `Windows Media`
becomes bare `wma`, before the value reaches rank policy or evidence storage.
Canonical labels otherwise retain Beets's existing spelling/case. This is a
closed alias map, not tokenization: an unfamiliar multiword label reaches the
evidence validator unchanged and fails closed rather than being guessed.

**Motivation (request 6039 / download_log 36608):** a genuine avg 196→288
rank upgrade (GOOD → TRANSPARENT) rendered as "Upgrade: MP3 V2 to MP3 V2"
because every UI label re-derived from min bitrate (194 on both sides).
The web UI (`web/classify.py::_verdict_from_basis`, the Recents evidence
strip, and the detail grid's "Compared" row) renders the persisted basis
verbatim when present; rows predating the field fall back to the legacy
min-based labels. Never re-derive a comparison for display — that
re-derivation is how the display learned to lie.

**Metric labels are truthful at synthesis too (download_log 36660):** the
same lie can be injected one seam earlier — the decision layer used to
synthesize comparison measurements with `avg` fabricated `= min` (the
lossless-conversion path carries only the post-conversion min across the
flat decision interface), so a persisted basis read "avg 216k" while the
files' real avg was 255. Explicit targets now persist the codec contract
(`OPUS 128 contract`); without a contract, synthesized measurements leave
unmeasured stats `None` so `_selected_bitrate_with_source` falls back to the
min and labels it `min`. Guarded by the `assert_basis_metrics_truthful` generated
property (`tests/test_quality_generated.py`) and the request-8781 pins in
`tests/test_quality_classification.py`.

## Further research

- [ ] Test `spectro` pip package as second-opinion validation.
- [ ] Reduce spectral-analysis cost (16 sox calls per track x 30s trim is
  roughly 8s/track, or ~100s per 12-track album).


## Evidence addressing, propagation, and ownership

> Relocated from CLAUDE.md (2026-07-04 doc simplification) — this is canonical policy, not narrative.

### Consistency boundary: observation plus action-time fail-closed

Quality evidence is an observation of files, not a lock or transaction token.
Cratedigger has no shared transaction across pipeline PostgreSQL, Beets SQLite,
the candidate filesystem, and the library filesystem. A snapshot fingerprint
therefore says which files one observation described; it cannot prove that
those independently mutable authorities remain unchanged through a later Beets
subprocess and filesystem mutation.

The supported contract is deliberately narrower:

1. Preview measures or reuses facts and persists them for later consumption. It
   never authorizes a library mutation.
2. At the import boundary, the importer reauthorizes the queue-owned candidate
   action path against its linked evidence.
3. Independently, it resolves the exact installed release from current Beets
   authority and reauthorizes HAVE evidence against that library snapshot.
4. Missing, stale, incomplete, invalid, or ambiguous authority fails closed
   before Beets launch. Automation records the failure and returns to a
   runnable state so a later cycle can re-derive the world.
5. Only then does the unified evidence decision permit or reject the import.

This is a bounded action-time consistency check, not a preview-to-import TOCTOU
proof. Per-file hashes, repeated scans, and moving the check later can strengthen
or move the observation point, but they cannot eliminate the final race while
the two databases and filesystem subjects remain independent. A privileged
process racing files after the action gate is outside this threat model. If the
world changes or fails after the gate, the ownership, audit, terminalization,
and self-heal lifecycle handles that failure; the request is never parked to
simulate atomicity.

**Evidence is content-addressed.** `album_quality_evidence` rows are keyed
by `(mb_release_id, snapshot_fingerprint)`; addressing entities reference
them via `import_jobs.candidate_evidence_id`,
`download_log.candidate_evidence_id`, and
`album_requests.current_evidence_id`. Triage walks the FK chain (direct →
cross-walk via `request_id` → measure as last resort). Evidence is never
deleted unless the files actually change.

**`source_path` is immutable capture provenance, not live path authority.**
It records where the evidence snapshot was first measured. A same-address
upsert may fill a legacy blank value, but it never replaces an existing
nonblank path when the same bytes are later observed in staging, quarantine,
or the Beets library. Candidate actions validate their active job path against
the stored file manifest and carry that transient path separately from the
evidence row. Current-library consumers resolve the exact release through the
fresh typed Beets authority, then validate that current path against the same
fingerprint. Neither boundary treats historical `source_path` as the location
to launch or scan.

When an exact release leaves Beets, `clear_on_disk_quality_fields` unlinks
`album_requests.current_evidence_id` together with the other installed-state
fields. The content-addressed evidence row remains as audit history; only its
claim to describe the request's current files is removed.

**Evidence survives the candidate → library transition by explicit markers.**
After a successful import, `propagate_candidate_evidence_to_current` builds a
new row from the installed library snapshot. Installed bitrate, format,
container, file inventory, and other on-disk facts are freshly measured.
Installed-subject spectral and V0 facts never cross a fingerprint change.
The ordinary enrichment path remeasures those facts against the installed
snapshot when policy needs them.

**The canonical acquisition-fact set is exactly:** verified-lossless proof,
source-subject spectral, and the source-subject V0 anchor. These facts cannot be
re-derived from converted library bytes, so they carry to the new row with
`provenance='carried'`. Their `subject='source'` continues to say that they
describe the upstream acquisition bytes, not the installed derivative.
Propagation reads those markers directly; codec names and conversion shape are
never used as a lineage heuristic. Wrong-match cleanup may compare future
candidates against these explicit source anchors. Rebuilds, migrations, and
operator one-shots must reference this canonical set rather than restating a
subset.

The same rule governs every evidence rebuild: proof and source-subject facts
carry unconditionally with provenance `carried`; installed-subject facts are
remeasured and can only have provenance `measured`. Proof is conceptually a
source acquisition fact, so it needs only its provenance marker.

**A changed installed snapshot is linked before neutral enrichment, but it is
not immediately action authority.** The new content-addressed row must exist
first so the preview-owned spectral and V0 writers can target its exact id and
fingerprint. Such a rebuild sets `current_enrichment_required=true`; action
loaders keep failing closed on every unchanged retry until that exact row has
both a spectral result and either a V0 metric or the persisted once-only V0
attempt marker. Source-subject spectral/V0 facts may satisfy the gate because
they survive byte changes by definition; installed-subject facts do not and
must be measured again. The marker is monotonic for a content address, so a
same-address upsert from another writer cannot erase the retry gate. It need
not be cleared after enrichment: completeness of the required facts is what
makes the row authoritative.

**A genuine spectral grade does not prove source bitrate for fullband codecs.**
Opus can retain a fullband cutoff at low bitrates, so a native Opus scan may
look spectrally genuine without establishing that the acquisition was
high-bitrate or transparent. More importantly, scanning an installed Opus copy
derived from a lossless source measures the encoder output, not the upstream
acquisition; persisting that result as source lineage would discard the fact we
need to retain. That is why lossless-derived installed spectral facts are
forbidden: the source-subject spectral fact carries instead. A native-Opus
genuine grade may still participate in the documented narrowing policy, but it
is weak spectral evidence, never proof of bitrate or verified-lossless
acquisition.

**Missing or incomplete current evidence converges at the failure point.** A
current-evidence row's spectral scan and on-disk V0 research normally
complete during import preview — but a request whose downloads always
fail never reaches preview, so its HAVE snapshot (and therefore the
Recents IN/HAVE strip) stays absent or partial forever. Download-phase
failure finalizers (`_timeout_album` and the materialize-grace reset in
`lib/download.py`) therefore perform two fail-soft steps. Before recording
the failed attempt, `prepare_current_evidence_for_failure`
(`lib/import_preview.py`) loads or backfills only the exact release's
canonical current snapshot. Even an already-linked complete row is freshly
reauthorized against the exact Beets identity and current fingerprint; when
both still match, the immutable evidence row is reused without rewriting it.
After the download log and request-state reset are safely persisted,
`enrich_incomplete_current_evidence_for_request`
plans exactly the missing pieces (`plan_current_evidence_enrichment`, pure),
measures the on-disk copy directly, and persists through the
preview-owned helpers — measuring only the pieces the plan reports missing,
never re-probing an already-attempted V0 snapshot (the V0 research marker is
once-only), and refusing stale on-disk state. (The HAVE spectral helper is
*not* once-only — a fresh audit overwrites a disagreeing grade; see the
fresh-audit-wins policy below.)
Adapter
or backfill failures and actual measurement work consume the per-cycle
`CratediggerContext.evidence_enrichment_budget`; complete or authoritatively
absent library copies cost nothing. Over time the failed-download cohort's
evidence converges without delaying or bypassing download cleanup. Automation
failure finalizers also reset the request to `wanted`. Terminal persistence
checks the operator search stop under its request-row lock for every `wanted`
transition, including rejection, HAVE-abort, and local-completion bundles. It
retains policy fields plus attempt/backoff accounting without clearing the
stop. An operator lifecycle command already waiting behind that lock retries
against the committed status, so neither concurrency ordering loses the
operator action.

**A blank `source_path` is policy-incomplete.** The field is required capture
provenance, even though live path authority comes from the active job or fresh
Beets resolution. Legacy 2026-05 library backfills wrote `source_path=''` and
therefore left evidence without an auditable capture location. Before issue
#711, that
incomplete HAVE side silently disabled all three spectral protections in the
import comparison (download_log 37206: a ~96k transcode replaced a better copy
as a "better" avg-bitrate tiebreak). A decision-usable, matching HAVE fact is a
prerequisite: preview reuses it when authorized, otherwise fresh analysis must
succeed or the attempt aborts as `have_analysis_error`.
`policy_incomplete_reasons`
therefore rejects blank paths; the action loader
(`ensure_current_evidence_for_action`) and the preview loader
(`load_current_evidence_for_preview`) rebuild such rows from beets.
When the on-disk files are unchanged (the legacy-backfill case) the
rebuilt row shares the `(mb_release_id, snapshot_fingerprint)` content
address, so the upsert fills the blank `source_path` in place — same row id,
FK untouched. A nonblank capture path is immutable and is never rewritten by
another observation of that address. If the files have changed since capture,
backfill writes
a fresh row, repoints the FK, and persists the changed-snapshot enrichment
gate described above. Either way enrichment can then complete the surviving
row. The candidate-reuse preview fast path first verifies the content
snapshot, then projects the candidate spectral fact from that content-addressed
evidence without scanning those bytes again. HAVE remains independent: preview
freshly resolves the exact Beets release and matches its linked current
evidence snapshot. A complete matching HAVE with a decision-usable grade
projects its persisted spectral fact without another scan; changed,
incomplete, or unusable HAVE evidence follows the existing analysis and
`persist_exact_current_spectral_from_attempt` path. A
changed candidate snapshot misses the front gate and runs full preview
measurement again.

**HAVE spectral persistence is fail-closed on absence and fresh-audit-wins on
disagreement (issue #815).** Two rules govern how the request's on-disk (HAVE)
spectral fact is written:

- **Bail, never infer.** The candidate download's spectral is NEVER adopted as
  the request's on-disk state. When the on-disk audit of the installed files
  yields no measurement — a stale/missing Beets path, an analyzer error — the
  measurement helper (`lib/measurement.py::_persist_spectral_state`) writes
  nothing; the container bitrate remains the HAVE comparison fallback. The old
  "reasonable proxy" branch that adopted a candidate's grade is deleted: on
  2026-05-12 it wrote a rejected fake-320's `likely_transcode`/128 as the HAVE
  state of a genuine 192 copy, the evidence seeder froze it, and on 2026-07-21
  it drove a real library downgrade (request 4351, dl 37742). Same fail-closed
  doctrine as #762/#723 — if we cannot ascertain evidence of the on-disk files,
  we surface the absence rather than infer.
- **Fresh-audit-wins.** A SUCCESSFUL fresh on-disk HAVE audit of matched-
  fingerprint bytes (grade non-null, no error) re-persists grade + bitrate over
  a disagreeing persisted installed-subject value, with
  `spectral_provenance='measured'` (`persist_current_spectral_measurement` and
  `persist_exact_current_spectral_from_attempt`). This replaced the old
  fill-only-if-NULL policy, which silently discarded a fresh genuine/160 audit
  and let the frozen 128 landmine keep deciding. Missing, incomplete, or
  changed evidence is measured on the next preview; complete matching evidence
  with a decision-usable grade is reused. Guards preserved: a
  FAILED fresh audit never clears a persisted grade (fail-soft `incomplete`),
  and an R19 lossless-sourced row keeps its source spectral — an installed-
  derivative scan is never persisted as its grade (`preserve_existing_source_spectral`
  plus the DB CHECK `album_quality_evidence_lossless_lineage_spectral_subject`).

`spectral_provenance='measured'` means the analyzer ran over the exact bytes the
snapshot fingerprint identifies; anything else is `'carried'` (a source-lineage
fact propagated across a byte change) and is never authoritative against a fresh
audit of the installed bytes.

**Search narrowing companion.** When `lossless_source_locked` fires —
in the importer (`lib/dispatch/core.py`) or wrong-match cleanup
triage (`lib/wrong_match_cleanup_service.py`) — the request's
`search_filetype_override` is narrowed to `"lossless"` via
`narrow_override_on_lossless_source_lock` (`lib/quality/dispatch_actions.py`). Future
search cycles only ask Soulseek for lossless tiers, so the lock
doesn't fire repeatedly against new peers serving the same lossy
file. No plan-generator change is needed — `generate_search_plan`
produces query strategies, and the filetype filter is applied
downstream in `lib/enqueue.py::effective_search_tiers` from the request's
override column.

A second lossless-only narrowing applies when an attempt proves that
the exact installed HAVE copy is both **TRANSPARENT** under the canonical
codec rank bands and spectrally **genuine**. The importer uses the independent
attempt-local HAVE audit; validation rejects may use only the request's linked,
complete current-evidence row. Candidate spectral results and the legacy
request scalar are not substitutes. MP3, AAC, Opus, Vorbis, and WMA participate
through `measurement_rank`; unknown codec families, merely EXCELLENT lossy
copies, and missing/failed/suspect/marginal audits fail open and do not
authorize lossless-only narrowing. Ordinary downgrade convergence still
removes the exact rejected tier from an existing search ladder. A positive
result writes only
`search_filetype_override="lossless"`: `target_format` remains untouched, and
`search_tiers` disables the catch-all fallback for that override. The normal
forever cadence continues, now searching only for the remaining meaningful
upgrade: lossless. That narrowing is monotonic across successful retained
imports: post-import persistence and the quality gate preserve an existing
`"lossless"` override even when the new retained-copy decision would normally
propose the full search surface. Only verified-lossless terminal acceptance or
explicit operator intent may clear it; evidence/decision failures retain their
separate fail-open recovery policy. Every other unverified retained copy that
starts unrestricted stays wanted on the full search surface. Only
verified-lossless proof ends acquisition, in every import mode.

Older library rows may still have NULL spectral / V0 / bad-hash facts. The
deploy transition materializes each member of the canonical acquisition-fact
set defined above that already exists in request history, but it never invents
missing facts. Wrong-match and narrowing policy wait for a complete linked
evidence row; fresh attempts remeasure incomplete or changed installed bytes.
`lossless_source_locked` remains a separate defense-in-depth narrowing path. See
`docs/brainstorms/2026-05-17-propagate-source-evidence-on-transcode-requirements.md`.

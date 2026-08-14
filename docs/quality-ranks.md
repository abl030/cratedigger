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
TRANSPARENT   60   MP3 V0, MP3 320, Opus 112+, AAC 192+, Vorbis 192+, WMA 320
EXCELLENT     50   MP3 V1-V2, MP3 256+, Opus 88+, AAC 144+, Vorbis 160+, WMA 256+
GOOD          40   MP3 V3-V4, MP3 192+, Opus 64+, AAC 112+, Vorbis 112+, WMA 192+
ACCEPTABLE    30   MP3 V5-V9, MP3 128+, Opus 48+, AAC 80+, Vorbis 96+, WMA 128+
POOR          20   below acceptable floor
UNKNOWN        0   not enough info to classify
```

Integer spacing leaves room for inserting new bands later. The rank is never
persisted — it's always recomputed from `(format, bitrate)` + config. Note the
absent third input: `is_cbr` used to select between two MP3 ladders and no
longer participates in any rank (issue #1145).

## Label vs bare-codec resolution

`quality_rank(format_hint, bitrate_kbps, cfg)` resolves a measurement
through six steps, in order:

1. Both `format_hint` and `bitrate_kbps` are `None` → `UNKNOWN`.
2. `format_hint` first token in `cfg.lossless_codecs` → `LOSSLESS`.
3. Explicit VBR label (`"mp3 v0"`, `"mp3 v2"`, ...) → index into
   `cfg.mp3_vbr_levels` (10-tuple indexed by V0..V9). VBR labels are
   self-certifying — the bitrate is irrelevant because V0 is V0.
4. Explicit bitrate label (`"opus 128"`, `"mp3 320"`, `"aac 192"`,
   `"vorbis 192"`, `"wma 320"`) → classify
   the declared numeric bitrate against the matching codec's `CodecRankBands`.
   The label is a contract; the actual measured bitrate is ignored.
5. Bare codec name (`"MP3"`, `"Opus"`, `"AAC"`, `"Vorbis"`, `"WMA"` from
   beets `items.format`) → classify the measured `bitrate_kbps` against the
   band table. Every family, MP3 included, has exactly one table.
6. Unknown codec → `UNKNOWN`.

**No step reads an encoding mode** (issue #1145). MP3 used to route to
`cfg.mp3_cbr` or `cfg.mp3_vbr` on an `is_cbr` boolean derived from per-track
bitrate uniformity — not an encoder-mode detector, and worth a 75 kbps swing
in the transparent floor. A measured MP3 now ranks on its bitrate alone.
Step 3 still exists, but its only producer is Cratedigger's own lossless → V0
conversion naming the target it converted to; nothing reads a `-V` level out
of a file's own LAME header.

The **label path** (step 3-4) is what makes lo-fi V0 imports work without the
old `verified_lossless` blanket bypass: a 207 kbps file with `format="mp3 v0"`
still classifies as `TRANSPARENT` because V0 is V0 regardless of what the
encoder actually produced on quiet material.

## `compare_quality()` semantics

Primary key is the rank. Within the same rank:

- **LOSSLESS always equivalent** — FLAC bitrate variance (800-1100) has no
  quality meaning.
- **Different codec families** (Opus vs MP3 vs AAC vs Vorbis vs WMA vs FLAC) → **equivalent**.
  This is the core cross-codec parity fix.
- **Same codec family, either side carries an explicit label** → equivalent.
  A V0 label and a "mp3 320" label at the same rank are both contracts.
- **Same codec family, both bare codec names** → compare the configured
  metric (`avg_bitrate_kbps`, `median_bitrate_kbps`, or `min_bitrate_kbps`)
  with `cfg.within_rank_tolerance_kbps` tolerance.

## Provisional lossless-source comparison

Unproven lossless-container sources use a separate evidence lane from generic
rank comparison. When FLAC, ALAC, WAV, or ALAC-in-M4A reaches V0 probing but
its verified-lossless proof will NOT be minted — a `suspect`/
`likely_transcode` grade, a proof-leg denial on an otherwise genuine grade, or
any other `determine_verified_lossless` refusal — the candidate is not treated
as verified lossless and does not rely on final stored-format rank. Entry is
keyed on proof absence, never the grade alone (issue #990): a genuine-graded
source whose proof the ultrasonic leg denies is exactly as unproven as a
suspect one, and skipping the anchor for it let equal transcode-lineage copies
re-import forever (request 2066). The V0-avg trust override still bypasses
this lane before it is consulted.

Instead, `provisional_lossless_decision()` compares the candidate
`lossless_source_v0` probe average against the request's current comparable
lossless-source V0 probe average:

- No current comparable source probe: `provisional_lossless_upgrade`.
- Candidate average more than `within_rank_tolerance_kbps` above current:
  `provisional_lossless_upgrade`.
- Candidate average equal, worse, or within tolerance:
  `suspect_lossless_downgrade`.
- Missing candidate probe on an accused source, or against a comparable
  anchor: `suspect_lossless_probe_missing`. Unaccused and unanchored,
  the candidate falls through to the measured policy instead.

This deliberately reuses `within_rank_tolerance_kbps` so near-equal source
probes do not churn the library, but it does not use rank bands, codec-family
parity, or the configured `bitrate_metric`. V1 policy uses the probe average;
probe min and median are persisted for audit and future tuning. Native lossy
and on-disk V0 probes can be stored as research evidence, but their lineage is
non-comparable and they must not update the current source-probe baseline.

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

Spectral cliff detection continues to use `min` regardless of this setting —
it cares about the worst track, not the typical one. `transcode_detection()`
no longer reads any bitrate: absent or errored spectral analysis fails
closed as a transcode verdict. The former spectral-fallback threshold
derived from the MP3 band table (issue #66) is gone — missing or
errored evidence cannot be converted into a positive quality fact.

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

Current web-library labels and ranks use the positive-track average as well.
The bounded `check_mbids_detail()` projection exposes the existing minimum as
`beets_bitrate` and the mean as `beets_avg_bitrate`; artist rows expose the
same values in bps as `min_bitrate` and `avg_bitrate`. Frontend overlays keep
the explicit `library_min_bitrate` floor for display/controls and use
`library_avg_bitrate` for the label and rank. This current-state rule is
separate from persisted decision history: rows without `comparison_basis`
retain their legacy min-derived wording byte for byte.

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

Container bitrate is the *only* Opus signal there is. Opus ≥32 kbps is
statistically indistinguishable from genuine lossless on every calibration
arm (94–100% no-cliff, deficit 43–48 dB against a control's 44–48), so the
spectral leg asserts nothing about an Opus album at any bitrate — audit-only,
unconditional. Measured tables: `docs/research/spectral-opus.md` §
"Measured — Phase 3/4 results".

### MP3 (one ladder, all measured MP3)

| Band | Threshold (kbps) |
|------|------------------|
| transparent | 320 |
| excellent | 256 |
| good | 192 |
| acceptable | 128 |

One table for every measured MP3, whatever its encoding mode (issue #1145).
Unverifiable MP3 only reaches TRANSPARENT at 320 because the pipeline cannot
prove a measured MP3 came from a lossless source, and an inferred mode is not
proof. Spectral cliff detection may clamp it down further.

Until #1145 there were two tables 75 kbps apart, selected by `is_cbr` — a
boolean derived from per-track bitrate *uniformity*, which is not an encoder
mode. Measured on the live DB (2026-08-14): 13,993 of 14,011 MP3 evidence rows
carry a bare codec label, so that boolean decided ~99.9% of MP3 ranks, and 570
of the 5,151 `is_cbr=False` rows have `median − min ≤ 2 kbps` — the CBR jitter
shape, ranked through the generous VBR table purely because one track differed.

**Nothing promotes a measured MP3 above this table.** A `-V` level in a file's
LAME header is peer-writable and is deliberately not read: an earlier revision
of this issue minted an `mp3 vN` contract from it, which let any file carrying
`-V 0` outrank measured, accusing spectral evidence. `mp3_vbr_levels` (step 3)
still exists for labels Cratedigger itself produced when converting a lossless
source to V0 — a target we chose, not a claim a stranger made.

**A spectral-bound value classifies via this table when the shared clamp
binds.** The MP3 spectral class (`lib/spectral_check.py`'s `LAME_LOWPASS`
table: 96/112/128/160/192/224/256/320, or the detector-space ladder in
`lib/quality/spectral_interpretation.py` when a raw `cliff_hz` was captured)
is a *nominal kbps class* drawn from the same MP3 ladder these thresholds are,
so a class of 192 lands in the `good` band arithmetically. Before #1145 this
paragraph had to say "only when BOTH sides are bound AND the side is MP3",
because the other MP3 table was more generous and reading a class through it
inflated the rank; with one table there is no second table to be inflated by,
and `_classify_with_cbr_bands` is gone. `both_spectral_bound` survives — it
still gates the same-rank `spectral_tiebreak`, which is only like-for-like
when both clamped values ARE spectral classes (issue #813 Finding 1).

**That is a statement about the class VALUES, not about accuracy** (issue
#829 Phase 5 PR2c — this paragraph used to say a cliff-detected 192 "IS a
`good`-band reading, by construction", which reads as a calibration claim
the measurement does not support, and says nothing at all for a non-MP3
album). The four-arm calibration measured `detect_cliff` reporting the
first slice of the steep run — roughly one tier BELOW the encoder's actual
lowpass — so the shipped `LAME_LOWPASS` table systematically under-rates
MP3s: a real CBR-192 buckets as 160 on 75% of tracks. That is why PR2a
derived `MP3_DETECTOR_CLASS_BUCKETS` in *detector* space for rows carrying
a raw `cliff_hz`, and why the two derivations are never compared against
each other (`spectral_classes_comparable` → `mixed_derivation_basis`).

The claim is narrow in the other direction too, and it is worth stating
precisely: **Vorbis q0–q4 has its own invertible ladder**
(`VORBIS_DETECTOR_CLASS_BUCKETS`, `VORBIS_TOP_CLASS_KBPS`;
`LADDER_CODEC_FAMILIES` is `{mp3, vorbis}`), and its classes ARE
decision-grade — a same-Vorbis pair is comparable and clamps. Those classes
simply classify through the **Vorbis** band table further down this page,
never this MP3 one. So the accurate statement is "no other codec's class
reaches *this* table", not "no other codec has a ladder". The two families
that genuinely have none are AAC — whose cliff is a one-sided content
*floor*, never a class — and Opus/HE-AAC, which assert nothing at all. See
`docs/research/spectral-calibration-findings.md`.

### Spectral scan selection — codec only

Every MP3 and every lossless candidate is spectrally scanned at preview; no
other codec is (none has a calibrated cliff policy). There is no bitrate
threshold and no mode test.

There used to be: a VBR MP3 whose album average cleared 210 kbps skipped the
scan, on the premise that a high-average VBR MP3 is self-evidently genuine.
Issue #1145 removed it, because neither half of that premise is evidence
about provenance. `is_vbr` is a self-declaration — `inspect_local_files`
reads mutagen's `bitrate_mode`, i.e. the Xing/Info/VBRI header the encoder
wrote. The average is genuinely measured from the frames, but a transcode
re-encoded at a high bitrate genuinely *has* a high average, so clearing the
threshold said nothing about the source. **Measurement decides; no
presumption.**

`lib/quality/gates.py::spectral_gate_trigger` and
`lib.measurement._needs_spectral_check` are the two readers. The one-kbps
boundary disagreement the threshold used to create is gone with the
threshold; two deliberate divergences remain, both one-directional (the
mirror withholds an opinion where production measured, never the reverse):

1. `_needs_spectral_check` answers "run" for a lossless candidate (preview
   must produce affirmative evidence for it), while the simulator mirror
   reports `skipped_flac`, because the Stage 1 verdict for a FLAC comes from
   convert → V0 → `transcode_detection` rather than from the MP3 preimport
   gate. Same codec, two different questions.
2. The two are not given the same input. `_needs_spectral_check` reads the
   candidate's filetype STRING and substring-tests it for `mp3`; the mirror
   reads an already-resolved `codec_family`, which
   `resolve_measured_codec_family` fails closed to `None` on a mixed-codec
   album. So `filetype="m4a, mp3"` runs in production and reports
   `skipped_uncalibrated_codec` in the mirror. Keying the mirror off the
   string instead would reconcile them and reintroduce the codec blindness
   issue #829 Phase 5 PR2b removed, so the divergence is recorded on both
   docstrings rather than closed.

The one remaining bypass is an exact CD-rip bit verification, which is
stronger evidence than a spectral estimate rather than an assumption about
one.

### AAC

| Band | Threshold (kbps) |
|------|------------------|
| transparent | 192 |
| excellent | 144 |
| good | 112 |
| acceptable | 80 |

Hydrogenaudio consensus places the "not worth going higher for music" ceiling
for AAC at 192.

**No spectral class ever classifies through this table.** An AAC cliff is a
one-sided *content floor*, never a bitrate and never a transcode
accusation: 94–96% of all AAC cliffs on every calibration arm land in
13–18 kHz, produced by encoder rates from 96 all the way to 320 kbps across
ffmpeg-native, libfdk and Apple CoreAudio alike. Measured tables:
`docs/research/spectral-aac.md` § "Measured — Phase 3/4 results".

### Vorbis (quality-based VBR)

| Band | Threshold (kbps) |
|------|------------------|
| transparent | 192 |
| excellent | 160 |
| good | 112 |
| acceptable | 96 |

These conservative album-average thresholds approximate the reference
encoder's q2/q3/q5/q6 regions. Vorbis uses one table: the unreliable generic
`is_cbr` inference never selects a second table. Ogg remains a container and
search selector, not a rank family; an Opus stream inside Ogg is still `opus`,
while a true Vorbis stream is `vorbis`.

**This is the table a Vorbis spectral class classifies through** — q0–q4 is
the second (and only other) invertible ladder, and it replicated exactly on
all four calibration arms. Measured tables:
`docs/research/spectral-vorbis.md` § "Measured — Phase 3/4 results".

### WMA

| Band | Threshold (kbps) |
|------|------------------|
| transparent | 320 |
| excellent | 256 |
| good | 192 |
| acceptable | 128 |

WMA uses one conservative table mirroring MP3 CBR. It does not branch on
`is_cbr`. WMA was deliberately never spectrally calibrated and never will
be — the only Linux-encodable variant is ffmpeg's clean-room `wmav2`, which
would calibrate the wrong encoder — so it stays audit-only forever. Reasoning
and the one measured run: `docs/research/spectral-wma.md`.

Band rank and spectral authority are separate. A Vorbis or WMA measurement at
or above the transparent threshold does not become spectrally genuine: a
`suspect` or `likely_transcode` grade cannot authorize lossless-only search
narrowing merely because the container bitrate is high.

## The verified-lossless guardrail

`import_quality_decision()` used to blanket-bypass on `verified_lossless=True`.
It now tier-gates the bypass:

- `verified_lossless=True` + verdict `"better"` or `"equivalent"` → import.
- `verified_lossless=True` + verdict `"worse"` → **downgrade** (blocked).

This prevents a deliberately-too-low `verified_lossless_target` (Opus 64,
Opus 48) from replacing a good existing album. The cratedigger process also
logs a warning at startup when `verified_lossless_target` classifies below the
canonical TRANSPARENT rank: a first acquisition bearing verified-lossless
proof completes terminally, so an under-quality target would stop further
automatic searching.

## Tuning via config.ini

Every knob is optional — missing keys fall back to the dataclass defaults.
Partial overrides work (e.g. set only `opus.transparent = 120` and everything
else stays at defaults).

```ini
[Quality Ranks]
bitrate_metric = avg
within_rank_tolerance_kbps = 5

opus.transparent = 112
opus.excellent = 88
opus.good = 64
opus.acceptable = 48

mp3.transparent = 320
mp3.excellent = 256
mp3.good = 192
mp3.acceptable = 128

aac.transparent = 192
aac.excellent = 144
aac.good = 112
aac.acceptable = 80

vorbis.transparent = 192
vorbis.excellent = 160
vorbis.good = 112
vorbis.acceptable = 96

wma.transparent = 320
wma.excellent = 256
wma.good = 192
wma.acceptable = 128

# Collection fields (issue #65). All three are CSV. Defaults are sensible —
# you almost certainly do not need to set these.
mp3_vbr_levels = TRANSPARENT,EXCELLENT,EXCELLENT,GOOD,GOOD,ACCEPTABLE,ACCEPTABLE,ACCEPTABLE,ACCEPTABLE,ACCEPTABLE
lossless_codecs = flac,lossless,alac,wav
mixed_format_precedence = wma,mp3,vorbis,aac,opus,flac
```

### Collection fields (mp3_vbr_levels / lossless_codecs / mixed_format_precedence)

These three fields configure rank-model behavior at the codec-identity layer
rather than the bitrate-band layer. They were previously dataclass-only;
issue #65 wires them through the INI parser.

- **`mp3_vbr_levels`** — comma-separated list of exactly 10 rank names
  (V0..V9). Maps each LAME VBR V-level to a rank when the format hint is
  an explicit `mp3 v0` / `mp3 v3` / etc. label. The only producer of such a
  label is Cratedigger's own lossless → V0 conversion naming its target; a
  peer's LAME header is never read (#1145). Defaults assume LAME's
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
  first codec that appears on disk. The default `wma, mp3, vorbis, aac, opus,
  flac` is worst-first, so a mixed FLAC+MP3 album classifies as MP3 (the
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
- Wrong Matches cleanup consumes already-persisted evidence only. Confident
  cleanup-eligible force-mode rejects are deleted and cleared; would-import,
  uncertain, missing-evidence, and stale-evidence candidates remain visible.
  Each cleanup outcome persists a typed
  `download_log.validation_result.wrong_match_triage` audit. Recents renders
  the audit as display-only evidence beside the original Beets rejection.
- `/api/import-preview` (web) — returns the common preview verdict shape for
  real-folder preview and typed values.

## Related

- Issue #60 (this PR)
- Issue #31 — original quality pipeline bugs that drove this rewrite
- `docs/opus-encoding.md` — Opus 128 rationale and listening test references
- `docs/research/spectral-calibration-findings.md` — the 60,102-measurement,
  four-arm record every per-codec spectral constant is derived from, plus
  the six per-codec documents beside it (`docs/research/spectral-mp3-lame.md`,
  `docs/research/spectral-aac.md`, `docs/research/spectral-opus.md`,
  `docs/research/spectral-vorbis.md`, `docs/research/spectral-wma.md`,
  `docs/research/spectral-transcode-detection.md`) and the raw data under
  `docs/research/calibration-data/`

## Tuning reference (Nix options)

> Relocated from the README (tier-2 doc run, 2026-07-04) — this is the operational tuning reference; the sections above are the model rationale.

Every threshold, enum, and per-codec band in the rank model is tunable via Nix options on the deployment side. The runtime parses them from `[Quality Ranks]` in the immutable application config built by the Nix module. Full rationale and per-band justification lives in [`docs/quality-ranks.md`](docs/quality-ranks.md); this section is the tuning reference.

### Where to tune

All options live under `services.cratedigger.qualityRanks.*` and are declared by the upstream NixOS module at [`nix/module.nix`](nix/module.nix) in this repo. Set them anywhere in your NixOS config that imports `cratedigger.nixosModules.default` — typically a host config or a homelab wrapper. Rebuild creates a new immutable config store path; Cratedigger picks it up on its next timer fire.

**Source of truth**: `QualityRankConfig.defaults()` in `lib/quality/ranks.py`, pinned by `TestQualityRankConfigDefaults` in `tests/test_quality_decisions.py`. The Nix options mirror those defaults for declarative visibility -- you should be able to open `cratedigger.nix` and read your current policy without grepping Python. Drift between Python and Nix is caught at cratedigger test time: bump a default in either repo, the pin test fails and reminds you to update the other.

### Nix-exposed options

**Policy scalars:**

| Option | Type | Default | Meaning |
|---|---|---|---|
| `bitrateMetric` | enum (`min`, `avg`, `median`) | `"avg"` | Which per-album bitrate statistic feeds rank classification. `avg` is robust to VBR per-track variance. `median` is outlier-resistant -- prefer when albums commonly have quiet intros/hidden tracks/skits that skew `avg`. `min` is legacy and penalizes legitimately-encoded lo-fi VBR. See `docs/quality-ranks.md` "When to prefer median". |
| `withinRankToleranceKbps` | int | `5` | Same-rank equivalence window in kbps. Two bare-codec measurements in the same rank tier within this tolerance are "equivalent"; outside it, one is "better"/"worse". |

**Per-codec band tables** (`bands.<codec>.{transparent,excellent,good,acceptable}`, all in kbps, used when the format hint is a bare codec string like `"MP3"` rather than an explicit label like `"mp3 v0"`):

| Codec | transparent | excellent | good | acceptable | Notes |
|---|---|---|---|---|---|
| `bands.opus`   | 112 | 88  | 64  | 48  | Unconstrained Opus VBR averages 120-135 kbps typical / 95-150 kbps per track. 112 leaves headroom for sparse material; 88 matches Opus 96 hydrogenaudio quality. |
| `bands.mp3`    | 320 | 256 | 192 | 128 | One table for every measured MP3 (issue #1145). Unverifiable measured MP3 is only `transparent` at 320 because we can't prove it came from a lossless source, and an inferred encoding mode is not proof. Nothing promotes a measured MP3 above this table; `mp3_vbr_levels` applies only to a label Cratedigger's own conversion produced. Below that → requeue for a FLAC source to re-verify. |
| `bands.aac`    | 192 | 144 | 112 | 80  | Hydrogenaudio consensus places the "no meaningful quality gain above here" ceiling for music at 192 kbps. |
| `bands.vorbis` | 192 | 160 | 112 | 96  | Conservative q2/q3/q5/q6-region approximation. One table; `is_cbr` does not change routing. |
| `bands.wma`    | 320 | 256 | 192 | 128 | Conservative WMA Standard table mirroring MP3 CBR. One table; `is_cbr` does not change routing. |

Leaving every option at its default produces exactly `QualityRankConfig.defaults()` -- the defaults above are the shipping values.

### Collection fields (NOT exposed via Nix -- edit `lib/quality/ranks.py` directly)

Three fields are part of the rank model but are NOT surfaced as Nix options because they're rarely-if-ever retuned outside of development. They live on `QualityRankConfig` in `lib/quality/ranks.py`, are parseable from `[Quality Ranks]` as CSV (see #65), and default to sensible values. If you want to tune them, the cleanest path is editing the dataclass defaults and updating `TestQualityRankConfigDefaults` to pin the new values. Extending `nix/module.nix` to render them is a trivial follow-up if you find yourself retuning them often.

- **`mp3_vbr_levels`** -- 10-tuple mapping LAME V-levels to ranks (V0..V9). The V-level is an **explicit label contract** -- when a Cratedigger conversion produced `"mp3 v0"`, the rank model reads V0 from this tuple and bypasses `bands.mp3` entirely. This is why a 207 kbps lo-fi V0 still classifies as TRANSPARENT. A file's own LAME header is never read (#1145), so no peer-supplied tag reaches this tuple.

  **Default ladder**: `V0=TRANSPARENT, V1-V2=EXCELLENT, V3-V4=GOOD, V5-V9=ACCEPTABLE`

  **When to retune**: tighten if you don't trust LAME's claim that V2 is transparent (move V1/V2 to EXCELLENT → GOOD). Loosen if you encode at V4 locally and want your own rips to pass the gate (move V4 up to EXCELLENT).

- **`lossless_codecs`** -- set of codec identity strings that **short-circuit to LOSSLESS** regardless of measured bitrate. Checked against the first whitespace-separated token of the format hint during rank classification. If the format hint starts with any of these, the rank model skips bitrate-based classification entirely and returns LOSSLESS.

  **Default**: `{"flac", "lossless", "alac", "wav"}`

  **When to retune**: add `"ape"`, `"dsf"`, or `"wavpack"` if your library carries them. Remove nothing -- removing entries is a footgun that would reclassify genuine lossless files as UNKNOWN.

- **`mixed_format_precedence`** -- ordered tuple used by `_reduce_album_format()` when an album on disk has tracks in multiple codecs (rare -- usually a manually-merged album). Walked in order; the first codec that appears on the album becomes the album's canonical codec for rank classification. Order matters.

  **Default**: `("wma", "mp3", "vorbis", "aac", "opus", "flac")` -- worst codec wins, so a mixed FLAC+MP3 album classifies as MP3 (conservative).

  **When to retune**: reverse to `("flac", "opus", "aac", "vorbis", "mp3", "wma")` if you'd rather have mixed-format albums classified by the *best* codec on disk (less conservative -- you'll accept more as "good enough"). The default is the conservative choice for a curated library.

### How to tune and deploy

The exact deploy flow depends on where you set the options. For a host config that imports `cratedigger.nixosModules.default` directly:

```bash
$EDITOR hosts/<your-host>/configuration.nix   # tweak services.cratedigger.qualityRanks.*
git commit -am "cratedigger: retune <what>" && git push
sudo nixos-rebuild switch --flake .
```

For the abl030 homelab (this project's reference deployment):

```bash
# On doc1 — has git push credentials for nixosconfig
cd ~/nixosconfig
$EDITOR hosts/doc2/configuration.nix          # tweak services.cratedigger.qualityRanks.*
git add hosts/doc2/configuration.nix
git commit -S -m "fix(cratedigger): retune <what>"
# Push the signed commit to the Forgejo deployment root, then:
env -u SSH_AUTH_SOCK fleet-deploy doc2
```

### How to verify the new config is live

1. **Read the active immutable file** -- derive the active wrapper and its fixed
   config argument, never glob store generations:

   ```bash
   ssh doc2 'wrapper=$(systemctl show -P ExecStart cratedigger.service | sed -n "s/.*path=\([^ ;]*\).*/\1/p"); config=$(grep -o "/nix/store/[^\"]*cratedigger-config.ini" "$wrapper"); grep -A30 "^\[Quality Ranks\]" "$config"'
   ```

   The section should show the exact values from the Nix edit.

2. **Check the runtime picks them up** -- `ssh doc2 'pipeline-cli quality <any_request_id>'`. The output prints the active `bitrate_metric` and simulates decisions with the configured codec thresholds. Mismatch means Cratedigger hasn't restarted since the rebuild (it's a 5-min timer) -- wait a cycle or `sudo systemctl start cratedigger --no-block`.

3. **Simulate against the live config** -- `ssh doc2 'pipeline-cli import-preview --values --values-json '"'"'{"is_flac": false, "min_bitrate": 200, "is_cbr": false}'"'"''`. The preview path loads the same runtime rank config the importer uses, so the verdict reflects your tuning (the web Decisions tab that used to render these values was removed in #575).

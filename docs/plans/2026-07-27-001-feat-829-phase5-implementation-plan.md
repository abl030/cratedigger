# Phase 5 — Per-Codec Spectral Model Implementation (issue #829)

**Status (2026-07-29)**: **PR1, PR2a, PR2b, PR2c and PR2d are merged and
deployed.** The codec-blind defect that opened the issue is fixed and live.
**PR3 is next**, and its design changed materially — read §1.5 before §3.
**Issue**: https://github.com/abl030/cratedigger/issues/829
**Predecessor plan**: `docs/plans/2026-07-22-001-feat-829-spectral-calibration-plan.md` (Phases 0–4)

This document is the durable pick-up point. A new session should be able to
read this file plus the linked issue comments and start the next PR without
replaying the research.

| PR | what | state |
|---|---|---|
| PR1 | evidence primitive capture (migration 065) | merged, deployed 2026-07-27 |
| PR2a | per-codec interpretation module | merged (#922) |
| PR2b | codec-aware decider seam + #911 anti-loop | merged (#927), **deployed + live-verified** |
| PR2c | #828-item-1 cross-codec parity closure | merged (#931) |
| PR2d | Stage-2 counterfactual audit; kills the #827 harness's blindness | merged (#934) |
| — | calibration record committed to the repo | merged (#929) |
| **PR3** | **proof gate v3** | **next — design revised, see §1.5** |
| PR4 | tiered verdict persistence + display | not started |
| PR5 | research tables, docs, teardown | teardown done; docs remain |

The temporary calibration instance is **torn down** (2026-07-29): DB dropped,
encode matrices and ground-truth FLACs deleted, 1,003 calib-owned slskd files
swept via its own ledger before the DB went. The measurements survive in
`docs/research/calibration-data/` — that directory is now the only surviving
evidence for every constant here.

### Research status (2026-07-30) — ongoing; PR3 has not started

Research did **not** stop when this plan was written, and a further round of
measurement ran on 2026-07-30. It is committed under
`docs/research/calibration-data/` as four new datasets, each with its own
README carrying exact commands, tool versions and column layouts:

| directory | what it is |
|---|---|
| `launder-matrix/` | 31 encoder variant IDs × 19 ground-truth albums, built as real launders, measured through the unmodified production analyzer and scored with a byte-identical copy of the frozen gate in single-window mode. 7,681 track rows. |
| `derrien/` | a validated numpy port of Derrien's MDCT-lattice AAC-transcode detector (JAES 2019), with a 430-row paired arm and a 1500-row unlabelled arm from the quarantine trees. |
| `floor-analysis/` | re-reading of the four committed arms plus the new matrix: preconditions, leave-one-album-out bitrate floors, a PROVED/BOUNDED/DENIED framing. No new audio. |
| `provenance/` | read-only AccurateRip / CUETools DB / MusicBrainz DiscID probes over the library's 38 FLAC album directories — a non-spectral axis. |

`docs/research/spectral-calibration-findings.md` § "2026-07-30 — further
measurement, ongoing" summarises all four and names which existing claims they
touch.

**This is a status note, not a design change.** These datasets are recorded
descriptively and deliberately draw no conclusions; the investigation was left
open. Nothing in §1.5 or §3 below has been revised by them, and no decision in
this plan should be read as either confirmed or overturned on their account.
Each new README ends with an "Unexplored directions" section listing open
questions and the data that would answer them.

**PR3 has not started.**

**Update 2026-07-31 — the research phase is closed.** A four-thread round ran
against the committed data and **drew** the conclusions the 2026-07-30 round
deferred. Four more datasets are committed under
`docs/research/calibration-data/`, each stating its verdict up front:
`homogeneity/` (within-album dispersion — **no**, falsified with the sign
inverted), `shape-analysis/` (the slice vector as a shape — **no**; SNR > 1 ⇔
the frozen gate already catches the class), `derrien-refinement/` (**yes,
partial** — a parameter-free offset-concentration rule closes the whole
Apple/CoreAudio family at proof grade), and `provenance-round2/` (**partial** —
three round-1 bugs corrected, 27 of 42 lossless albums bit-verified, badge tier
only). `docs/research/spectral-calibration-findings.md` § "2026-07-31 —
four-thread verification round: conclusions" carries all four. One correction
lands on this plan: see §1.5 item **f**. **Research is closed; PR3 is
unblocked.**

---

## 1.5 Corrections since this plan was written — READ FIRST

Five claims in the original plan are wrong or overstated. They were found by
checking the plan against the committed data and the live DB, and each one
changes PR3. Item **f** was added on 2026-07-31 and is a correction to item
**c** itself, not to the original plan.

**a. The un-backfillable cohort is ~40%, not 93%.** §3's "93% of existing
verified-lossless proofs sit on rows whose source no longer exists" counts
rows with lossless *lineage*. But when the target is FLAC the conversion
output IS the lossless file, so the source is not gone. Measured 2026-07-29:

| | rows |
|---|---:|
| verified-lossless proofs | 15,547 |
| source genuinely gone (lossless source → lossy current codec) | **6,273** (40%, 6,251 albums) |
| still lossless on disk, re-measurable in principle | **8,273** (53%) |

The consequence PR3 must absorb: more than half the proof cohort *could* be
re-measured. The verified-lossless re-measurement sweep was declined on
2026-07-27 partly on cost grounds computed against the wrong number. Whether
to revisit that is an operator decision, not a PR3 default.

**b. Ship the proof gate SINGLE-WINDOW, not multi-window.** The plan
specifies legs evaluated at offsets 0/60/120/180 and unioned. **Production
measures one window** — `_ffmpeg_to_wav(trim_seconds=30)`, no offset logic
anywhere in `lib/spectral_check.py` — so the plan as written is not
implementable without new capture. Ablation over all four arms
(re-running the frozen scorer's own logic):

| mode | max leak-free `T` | genuine denied there |
|---|---:|---:|
| window-0 only | 61.50 | 28/100 |
| 0+60 | 62.90 | 31/100 |
| union 0/60/120/180 | 64.00 | 34/100 |

Window 0 **is** production exactly, and is the *strongest* single window
(299/300 launders denied vs 296/294/295 for the others). At `T=62` window-0
leaks exactly one album of 300, by 0.45 dB — a threshold-calibration
artifact, because the max-over-windows shifts the genuine distribution up as
much as the launder one. On the leak-vs-cost frontier single-window weakly
dominates at every operating point.

**Recommended: window 0, `T ≈ 59.5`** — leak-free on all four arms with
2.05 dB headroom, at 34/100 genuine denials, which is *cheaper* than the
union config's 36/100. Windows 3 and 4 add zero fraud catches and two extra
genuine denials. Each extra window costs a full re-decode plus 21 sox
band-RMS passes per track, on every import.

**c. `apple-256→FLAC` was never run through the proof gate.** It is listed as
a fraud class in this plan and in the calibration README, but it exists in
exactly one committed file — `probe_pair.tsv.gz`, the V0/Opus re-encode
experiment. It is absent from `results*`, `extended*` and `multiwin*`, so it
was never spectrally measured and never entered the gate evaluation. The
frozen scorer's `FLAC_FRAUDS` is only mp3-128 / opus-96 / vorbis-q5.

**The gate is validated against three FLAC-container fraud classes on four
arms, not four.**

**MEASURED 2026-07-30 — the gate does not catch the class.** Full method and
data: `docs/research/calibration-data/apple-arm/`.

| | T = 62 | T = 59.5 |
|---|---:|---:|
| Apple launders reaching PROOF (n=17) | **10** | **10** |
| genuine controls reaching PROOF | 11 | 10 |
| conditional P(launder proof \| genuine proof) | 91% | **100%** |

At `T = 59.5` the launder proof-set is byte-for-byte the same album set as the
genuine proof-set — zero discriminating power. Pooled over two arms: 91–92%
conditional false accept. Apple CVBR-256 applies essentially no lowpass in the
measured band (2.1 dB down at 21.5 kHz), so no leg has anything to see;
production grades all 17 launders `genuine`. Lowering the threshold does not
help. The harness reproduces the published 34/100 genuine-denial figure
exactly, so this is not an artifact of an unusual album set.

**Consequence for PR3: see §1.7 — the proof's claim is reframed, not
abandoned.**

**Additional constraint discovered by the same arm — `ultrasonic_deficit_db`
is not comparable across decode paths.** The same bits measure **50.26 through
`_ffmpeg_to_wav` @48 kHz versus 47.17 sox-native @44.1 kHz, a +3.09 dB skew** —
larger than the gate's entire 2.04 dB four-arm margin. A carried or propagated
value from a non-sox-native container (ALAC, M4A, WMA) is on a different scale
from a native FLAC measurement, and **PR3 must not gate both against one
threshold.** Isolated on request 8923, the only `was_converted_from='alac'`
control; the other 16 reproduce their stored value to 1e-7.

**d. The published safety margin is stale.** "T=62 sits 4.7 dB below the
tightest observed value" was computed on TRAINING (67.9) and ROUND-2 (66.7).
ROUND-3 contains launders at 64.04 and 65.94, so the true four-arm margin is
**2.04 dB**. The margin halved on the arm that passed the blind test.

**e. The SBR object-type gate is not mandatory, and is not a PR3 blocker.**
Probed every candidate file on doc2: **zero HE-AAC** anywhere (409 AAC-LC and
39 ALAC in the library, 22 ALAC in slskd). HE-AAC also cannot structurally
reach the proof gate — `converted_count` only counts files
`_is_lossless_file` accepts, and HE-AAC probes as plain `aac`. And the
genuinely dangerous case, HE-AAC laundered into a FLAC container, has no AAC
object type left to read; that case is what the ultrasonic leg is for.
`sbr_present` in `lib/quality/spectral_interpretation.py` is currently dead
plumbing — PR3 should wire it or delete it (`scope.md`); leaning delete.

**f. CORRECTION 2026-07-31 — §1.5c's scope is the spectral gate alone, and the
Apple class had never been scored against the detector we already have.**
"The gate does not catch the class" stands exactly as written *for the spectral
gate*, and two independent 2026-07-31 threads now prove no spectral instrument
can (`docs/research/calibration-data/homogeneity/`,
`.../shape-analysis/` — the AAC/Apple paired shape delta is ≤ 0.24 dB in all 20
bands and its SNR against genuine album-to-album variation is 0.031–0.086).
But the Apple family was **never scored against the Derrien MDCT-lattice
detector**: `launder-matrix/FINAL_REPORT.txt` § D's gate ∪ Derrien union table
excludes the five `qaac-*` variants through its producing script's hardcoded
`ORDER` list, so no `qaac-*` row exists there. Scored on 2026-07-31
(`docs/research/calibration-data/derrien-refinement/`):

- The **existing pooled Derrien rule already closes the class completely** —
  **0 of 10** Apple launder albums survive the spectral ∪ Derrien union, and
  17/17 are flagged. Nothing new had to be measured, only scored.
- A **parameter-free offset-concentration rule** — ≥ 4 tracks of an album
  recovering the same MDCT frame offset — gives proof-grade coverage of the
  whole Apple/CoreAudio family (100 % of `qaac-cvbr256`, `qaac-cvbr320`,
  `qaac-tvbr91`, `qaac-abr192`; 16/17 of `qaac-cbr128`) at an **analytic**
  false-positive floor of ~0.0023 albums per 5000. Genuine frame offsets are
  uniform over 0–1023 (182 distinct in 197 tracks, zero within-album k ≥ 2
  coincidences); the mechanism is exact (CoreAudio primes 2112 samples →
  2112 mod 1024 = 64 → lattice offset 960; ffmpeg primes 1024 → offset 0), and
  it reproduces on an independent second Apple build.
- **The permanently open spectral residual is therefore `lame-v0`,
  `vorbis-q10`, and roughly half of `ffmpeg`-native AAC 256/320** (best union:
  5/10 and 4/10 survive under `offset k ≥ 4 OR z > 12`). The first two carry no
  AAC lattice at all, so no Derrien-family rule can reach them.

**§1.7's reframe is intact and still governs.** `verified_lossless` continues to
mean "no evidence of lossy origin was found by the tests we have"; that wording
simply gains a possible future fourth leg. Nothing in this item authorises PR3
to widen the claim, to ship a Derrien leg, or to change any threshold — PR3's
scope is unchanged, and a detector leg is a separate, later decision with its
own operator authority.

---

## 1.7 What "verified lossless" claims — reframed, and binding on PR3

The Apple result (§1.5c) falsifies the unqualified bar this project was
working to. "Zero fraud albums receive proof" holds for the three measured
classes and **fails at ~91% for Apple CVBR-256 → FLAC**. The stamp cannot
honestly mean "proven bit-faithful to a lossless source."

**Operator decision 2026-07-30: keep the name, bound the claim.**

> *"verified lossless inasmuch as we can — we still call it verified lossless
> but at least now we know what we can't know"*

`verified_lossless` therefore means, precisely:

> **No evidence of lossy origin was found by the tests we have** — the
> in-window cliff, the album ceiling, and the ultrasonic deficit. Not "this is
> bit-faithful to a lossless source."

This is the strongest claim the evidence supports, and it is more useful than
the unqualified version because its failure modes are **enumerated** rather
than unknown. PR3 shipping under this framing is honest; PR3 shipping under
the old framing would not be.

What PR3 owes because of it:

- **Do not widen the claim in copy.** Anything reading as "guaranteed
  bit-perfect" is now known-false. The honest register is "no evidence of
  lossy origin by these tests", with the limits discoverable.
- **Denial semantics are unchanged and remain load-bearing** — withhold proof,
  never reject, denylist or accuse. The archivist invariant survives this
  reframe intact.
- **The named blind spot goes in the record, not just the research doc**, so
  an operator reading a proof knows what it does and does not cover.
- PR4's tier copy inherits the same constraint (§3, PR4).

The boundary is falsifiable and written down: any future discriminator that
separates the Apple class moves it. The V0/Opus probe axis has already been
tried and fails against this class too.

---

## 1.6 Ground truth is being reacquired

Deleting the corpus at teardown was a mistake: it makes correction (c)
unclosable without reacquisition, and forecloses any future statistic that
needs the original audio.

**20 corpus albums were re-seeded into prod on 2026-07-29**, chosen from
`docs/research/calibration-data/corpus-manifest.json` — calib-proven
acquirable, not already in prod, spectrum-diverse and including the
deliberate traps (Loveless, Kind of Blue, the Gould Goldberg Variations,
Music for Airports). All carry `target_format=lossless` and
`search_filetype_override=lossless`, so the pipeline retains FLAC rather than
converting to the default Opus target.

When they land they are the ground truth for closing (c): encode each through
qaac CVBR-256 → FLAC, measure the ultrasonic extension with the production
analyzer, run the frozen scorer. Note the qaac VM (nixosconfig#46) may need
re-standing.

#### These are borrowed rows — remove them when the work is done

**They are not part of the operator's curated collection.** They were seeded
to serve issue #829 and should be removed once (c) is closed. Recorded here
because a request id is the only handle that survives this session.

| id | artist — album | MB release id |
|---|---|---|
| 8916 | Daft Punk — Random Access Memories | `e69e2f55-a2c0-472e-b30b-f43b565b3fbe` |
| 8917 | Aphex Twin — Syro | `29625757-3991-4cfa-8d8a-3e1d5bd1f0d5` |
| 8918 | Autechre — Tri Repetae | `d7838033-55fe-4fa4-949a-7bb96cc88839` |
| 8919 | Squarepusher — Feed Me Weird Things | `8b8fab44-18b2-45fb-ae0f-d161fd2c6ca1` |
| 8920 | M83 — Hurry Up, We're Dreaming | `bd6ea0c6-f5cd-4abf-828a-38df69ad1969` |
| 8921 | Red Hot Chili Peppers — Californication | `ae9e09df-5029-30ec-bf1c-8d4a905f8c02` |
| 8922 | Metallica — Death Magnetic | `5073f8ac-59a6-4190-8d36-c1a510a84fcf` |
| 8923 | Converge — Jane Doe | `c0c80905-b460-4385-b84d-b068eb14bf5a` |
| 8924 | Arvo Pärt — Tabula Rasa | `25edd502-d703-49bb-962f-7a3a679b5dbb` |
| 8925 | J. S. Bach — The Goldberg Variations | `b304abb5-c039-494f-bc50-37490aca74c5` |
| 8926 | Miles Davis — Kind of Blue | `e32a3f0b-1c19-3170-bb1c-650893774744` |
| 8927 | John Coltrane — A Love Supreme | `4fa9c6d8-731e-3870-8b5b-580811b2d4e0` |
| 8928 | GoGo Penguin — Man Made Object | `9f27f788-6729-42e2-9d42-4d68efa9a0a1` |
| 8929 | The Velvet Underground — & Nico | `21117289-def5-3149-8346-56eb317a1087` |
| 8930 | Simon and Garfunkel — Bridge Over Troubled Water | `a32c1775-4378-3a30-a341-41560aa41c2b` |
| 8931 | Fleetwood Mac — Rumours | `4734e007-0972-30d7-9ffe-d864f08982c9` |
| 8932 | My Bloody Valentine — Loveless | `cd32c6cf-f979-39e7-a4ec-157d3a560d06` |
| 8933 | Grouper — Ruins | `aaed190c-fc3a-43c2-acdd-aa06b390b9cf` |
| 8934 | Duster — Stratosphere | `79acc86e-b12b-4a4a-ad7d-7c9f928438a3` |
| 8935 | Brian Eno — Ambient 1: Music for Airports | `1496daaf-4b0b-3596-a252-0d3a7068f6e5` |

Contiguous range `8916..8935`, so a census is one query:

```sql
SELECT id, artist_name, album_title, status, final_format
FROM album_requests WHERE id BETWEEN 8916 AND 8935 ORDER BY id;
```

Removal is an operator action through the normal surfaces
(`pipeline-cli library-delete` for anything imported, `pipeline-delete` for
the request rows) — **not** a raw SQL delete, and **not** before the Apple
measurement is done. Check the range against the live library first: if the
operator has meanwhile decided to keep one, it stops being a borrowed row.

**Do not delete the FLACs again before the measurement.** If a future
teardown is proposed, the corpus audio is the one artifact that cannot be
regenerated from anything in the repo — that is the mistake this section
exists to prevent repeating.

---

## 1. What is settled

Every codec family is validated on **four independent arms** — TRAINING (34
albums), ROUND-1 (15), ROUND-2 (27), ROUND-3 (24) — totalling **60,102
production-primitive measurements**.

| domain | verdict | evidence |
|---|---|---|
| MP3 / LAME | decision-grade ladder, ±1 tier 94–100% | 4 arms |
| Vorbis q0–q4 | decision-grade ladder, medians replicate exactly | 4 arms |
| Vorbis q5+ | extension-only, ~85% invisible in-window | 4 arms |
| AAC (ffmpeg-native, libfdk, **Apple/CoreAudio**) | **content floor only, never a bitrate** | 4 arms |
| Opus ≥32k | **no spectral signal — audit-only, unconditional** | 4 arms |
| HE-AAC (SBR) | reads as lossless; object-type pre-gate mandatory | 4 arms |
| FLAC/lossless containers | cliff = fake detector; v3 ultrasonic proof gate | round-3 fresh blind PASS |
| cross-codec MP3↔Vorbis | valid **in inferred-class space**, 98% ordering accuracy | 4 arms pooled |

### Constants to implement (all measured, do not re-derive)

**MP3 detector-space buckets** (cliff Hz → nominal kbps class):
`<15000→96 | <16000→128 | <17250→160 | <18250→192 | <19250→256 | ≥19250→320`

**Vorbis ladder** (cliff Hz → nominal kbps class):
`<15250→64 | <16500→96 | <17750→112 | <19000→128 | ≥19000→160`

**AAC content floor**: a cliff anywhere in 13000–18000 Hz floors at only
96–128-class; ≥18500 lifts to ~190-class; <13000 is junk. 94–96% of all AAC
cliffs land in 13–18 kHz on every arm, produced by everything from 96 kbps to
320 kbps across all three encoder families. **An AAC cliff is never a
transcode accusation.**

**HF-deficit thresholds**: marginal 65 dB, suspect 69 dB (replacing 40/60).
Control false-flag rate 1–8% across arms vs 4–17% before.

**Ultrasonic proof leg** (v3): `U = mean_over_tracks[ref_db(1–4kHz) −
mean(20.5–22kHz)]`, deny promotion when `U ≥ 62`. Derived from the binding
constraint (training 67.9, round-2 66.7; 4.7 dB margin ≈ 4× inter-arm spread).

**Window**: extend slices to 22 kHz (20 × 500 Hz from 12000). Version-stamped.

### Verdict tiers (measured, 4 arms; 100 genuine vs 300 launders pooled)

| tier | fired legs | genuine | launders |
|---|---|---|---|
| 1 | in-window cliff | **0** | 99 |
| 2 | ceiling + no-ultrasonic | 18 | 155 |
| 3 | ceiling only | 2 | 1 |
| 4 | no-ultrasonic only | 16 | 45 |
| 5 | none → PROOF | 64 | **0** |

Tier 1 is a **positive transcode detection** with a 0/100 false-positive rate.
Tier 5 is absolute: no launder ever reached proof on any arm.

**Base-rate caveat (load-bearing for UI copy):** the corpus is 1 genuine : 3
launders *by construction*. Those are per-class detection rates, NOT posterior
odds. Real peer-shared content is 49.2% mp3 / 48.3% flac / 1.7% AAC / 0.43%
opus. Tier 2 in production means "worth a look", not "probably fake".

---

## 2. Decisions and their authority

- **Proof-semantics reframe** — promotion `provisional → verified lossless`
  requires (a) no in-window transcode cliff, (b) no album-level ceiling flag,
  (c) affirmative ultrasonic content. Failing legs ⇒ stays provisional +
  triage surface; never rejects, denylists or accuses.
  Authority: *"right. this is kind of our idea with provisional lossless
  source."* — https://github.com/abl030/cratedigger/issues/829#issuecomment-5056061513

- **Verification gates 1 and 2 dropped** (prod `failed_imports/` sweep; VL
  re-measurement over 15,495 proof rows).
  Authority: *"1 and 2 don't really buy us anything, lets behin the coding
  ound"* — https://github.com/abl030/cratedigger/issues/829#issuecomment-5086552837
  **Consequence:** existing `verified_lossless` stamps remain proofs under the
  OLD model's assumptions and are NOT re-scored by this project. Standing
  caveat, indefinite.

- **Quality-core review process** — PRs changing quality-decision logic get
  fable-tier review and the **merge is held for operator approval**. Applies to
  PR2 and PR3 below at minimum.

---

## 3. PR sequence

Dependency order. Each PR ships the invariant **PAIR** (deterministic pin +
generated property) per `.claude/rules/code-quality.md`, and every invariant
checker owes a known-bad self-test.

### PR1 — Evidence primitive + measurement capture (no decision change)

- Migration: add `cliff_hz`, `codec_family`, `spectral_measurement_version` to
  `album_quality_evidence`. Forward-only; legacy rows keep old semantics behind
  the version stamp (no backfill — `scope.md`).
- `lib/spectral_check.py`: extend the slice window to 22 kHz (20 slices),
  version-stamped. Capture raw `cliff_hz` rather than only the bucketed output.
- Measurement side (`lib/measurement.py`, `harness/import_one.py`) persists the
  new primitive alongside existing fields.
- `spectral_bitrate_kbps` becomes a codec-aware *derived interpretation*, not a
  stored truth (still written for compatibility this PR; PR2 changes its
  derivation).
- **No decider behaviour changes in this PR.** Pure capture.
- Owed: real-PG round-trip test (Rule A, `test-fidelity.md`), pin + property.

### PR2 — Codec-aware interpretation + decider seam (**the dl 37946 fix**)

- Per-codec interpretation module: MP3 buckets, Vorbis ladder, AAC floor,
  Opus/HE-AAC audit-only. Constants from §1.
- **SBR pre-classification gate** (AAC object type 5/29) → audit-only +
  `sbr_present` as an evidence fact. Mandatory: HE-AACv1-64 reads as lossless.
- `spectral_gate_trigger` (`lib/quality/gates.py`) and the shared spectral
  clamp become **codec-aware**. The clamp currently receives only
  `is_flac`/`is_cbr`/`is_vbr` and cannot see codec — that is the root defect.
- **Cross-codec comparison rule**: compare in *inferred-class* space, never in
  cutoff space, and only when BOTH sides have an invertible ladder (MP3,
  Vorbis q0–q4). AAC contributes a one-sided floor. Opus/HE-AAC contribute
  nothing. No ladder on either side ⇒ **no comparison** (unknown, not equal).
- Extend the #827 `StageParityWorld` parity property domain with these
  semantics — closes #828 item 1 properly.
- Correct the three shipped statements of the falsified scoping claim:
  `StageParityWorld` docstring, `docs/quality-verification.md` § stage parity,
  the #813 audit record.
- **Quality core: fable review, merge held for operator approval.**

### PR3 — Proof gate v3 (ultrasonic leg + deficit re-threshold) — NEXT

**§1.5 revises this section. Where they disagree, §1.5 wins.**

- Replace the relative affirmative leg with the level-invariant ultrasonic
  deficit. Cliff and ceiling legs unchanged.
- **Single window, `T ≈ 59.5`** — NOT `T = 62`, and NOT the multi-window
  union, which production cannot produce (§1.5b). Production's one window is
  window 0 of the calibration data, so PR1's capture already supplies
  everything the legs need. Confirm the exact threshold by re-running the
  ablation before freezing it.
- HF-deficit thresholds → 65 / 69.
- Denial semantics per §2: stays provisional, surfaces in triage, never
  rejects or accuses. **Withholding proof is not rejecting an album.**
- Mint `verified_lossless_classifier = 'spectral_verified_lossless_v3'` —
  reuse the existing column, do NOT add a new one. It is already the
  "which model proved it" axis, is written at exactly one site
  (`lib/quality/decisions.py::mint_verified_lossless_proof`), survives proof
  carry-forward verbatim, and is currently rendered to zero operator
  surfaces. **Do not use `spectral_measurement_version` for this** — it is a
  measurement-shape version, and 47 proofs carry `smv=2` while having been
  proved under the OLD gate; 7 of those are rows v3 denies, so a surface
  reading it as "v3-proved" would mislabel the seven worst rows in the
  library.
- **Never retroactively demote.** Existing stamps remain proofs under the old
  model per §2's authority. A row whose source is gone cannot be re-proved
  and must not thereby lose the proof it holds.
- Display of the proof generation belongs to **PR4**, not here — PR3 writes
  the discriminator (unreconstructable afterwards), PR4 renders it. Doing the
  display in PR3 means paying the Rule D 36k-row live-corpus differential for
  copy PR4 immediately rewrites.
- **Ship under §1.7's reframed claim.** The proof means "no evidence of lossy
  origin by these tests", not bit-faithfulness. Copy must not widen it.
- **Do not gate `ultrasonic_deficit_db` from different decode paths against
  one threshold** (§1.5c): sox-native vs `_ffmpeg_to_wav`@48 kHz differ by
  +3.09 dB on identical bits, more than the whole 2.04 dB margin. Either
  normalise the measurement path or scope the threshold by path — and note
  15,399 of 15,547 proofs carry `spectral_provenance='carried'`, so carried
  values are the common case, not the edge.
- **Quality core: fable/opus review, merge held for operator approval.**
- Reference implementation: `docs/research/calibration-data/score_v3.py.frozen`
  — its `_window_legs` / `gate` are the shape to port. The `.frozen` suffix
  keeps it out of Pyright/Ruff/Vulture; it does not type-check and must not
  be "fixed".
- Owed before or with PR3: the **live decision differential** (re-decide real
  evidence pairs through the real decider on both trees, report changed rows
  by field). It has been prototyped three times in this series and never
  kept. PR3 changes what mints proofs — the highest blast radius left — so it
  should be a committed `scripts/` harness with a Rule D-style clause, not a
  fourth throwaway.

#### PR3 hard constraint — a large cohort can never be backfilled

> **CORRECTED 2026-07-29 — see §1.5a.** The "93%" below counts lossless
> *lineage*, which is not the same as a vanished source: when the target is
> FLAC the conversion output IS the lossless file. The genuinely
> un-backfillable cohort is **6,273 rows / 6,251 albums (40%)**, and **8,273
> proofs (53%) still have lossless on disk and are re-measurable in
> principle**. The constraint below is real but applies to 40% of the
> cohort, not 93%. The original 2026-07-27 measurement is retained for
> provenance.

Measured on prod 2026-07-27 (superseded figures):

| | rows | albums |
|---|---:|---:|
| evidence rows derived from lossless (`was_converted_from` ∈ flac/wav/alac) | **15,222** | **6,346** |
| …of those, also carrying verified-lossless proof | **14,391** | |
| all verified-lossless proof rows | 15,501 | |

Why they are unreachable: `lib/import_preview.py::preserve_existing_source_spectral`
(R19) makes the preview worker **refuse** to persist an installed-subject
spectral for a lossless-sourced copy — it returns `skipped`, *"lossless-sourced
copy keeps its source spectral (R19)"*. That rule is correct and must stay:
Opus is fullband and scans clean at any bitrate, so measuring the derivative
would launder a transcode-like source into apparently-genuine. The row wears
its SOURCE's spectral.

But the operator's default target is `verified_lossless_target = opus 128`, so
for this cohort the source FLAC was converted away and is gone. The ultrasonic
statistic PR3's proof leg needs can only be computed from that source. It
therefore **cannot be backfilled by re-measuring anything on disk** — the only
genuine backfill is re-downloading the 6,346 albums' FLACs, which is not
proposed.

Consequences PR3 must implement, not discover:

1. **`ultrasonic_deficit_db IS NULL` is three different states** and they must
   be distinguishable: (a) preserved-source, never measurable here (R19
   skipped); (b) legacy row predating `spectral_measurement_version = 2`;
   (c) genuinely unmeasured. Treating them alike mis-handles most of the
   library. The version stamp separates (b); R19/`was_converted_from` +
   lineage separates (a).
2. **The proof gate must not retroactively demote.** Existing stamps remain
   proofs under the old model per the §2 authority; PR3 applies going forward.
   A row that cannot be re-proved because its source is gone must not thereby
   lose the proof it already holds.
3. **Operator surfaces must say which model proved a row.** Otherwise "verified
   lossless" silently means two different things across the library.
4. Fresh imports are unaffected — they capture the source-side statistic at
   import time, which is exactly what PR1 wired up.

### PR4 — Tiered verdict persistence + display semantics

- Persist the **fired-leg set** and derived tier, not just the boolean.
- Tier 1 surfaces as a transcode finding, reconciled with the existing
  `likely_transcode` spectral grade so there is ONE statement, not two.
- Tier 2 → triage priority ("spectral evidence of a codec ceiling; may be a
  band-limited master"). Never an accusation, never auto-action.
- Tier 4 → "not spectrally provable" — explicitly distinct from tier 2.
- Audit-only codecs (Opus, HE-AAC, and AAC above the floor) **stop receiving
  `likely_transcode` stamps** in operator-facing surfaces.
- Operator surfaces show the tier: `pipeline-cli quality`, web evidence panel.
- UI copy must respect the base-rate caveat in §1.
- **Also PR4's, added 2026-07-29:** render the proof *generation* that PR3
  mints into `verified_lossless_classifier`, so "verified lossless" stops
  meaning two different things silently. The surfaces are enumerated in the
  #829 realignment comment; the badge path needs an evidence join because
  `album_requests` carries only the bool. This is Rule D territory (36k-row
  live-corpus differential) and should be done once, here, rather than twice.
- **Also PR4's:** the web forensics card does not carry PR2d's Stage-2
  counterfactual — `pipeline-cli quality` does. Leaving the persisted
  stage-chain builders alone was verified correct (fixed key allowlists, zero
  JSONB drift), so this is additive display work.
- **Also PR4's:** the convergence-signal surfacing from the 2026-07-27
  issue comments — inter-candidate `cliff_hz` constancy as a
  "the network has converged, stop searching?" triage prompt. `cliff_hz` is
  the primitive that makes it measurable and it has been accruing since
  PR1 shipped.

### PR5 — Research tables, docs, teardown

- Commit the calibration tables into `docs/research/spectral-*.md` (the six
  Phase 0 docs get their measured Phase 3/4 tables).
- Update `docs/quality-verification.md`, `docs/quality-ranks.md`, CLAUDE.md.
- Ownership-ordered calib teardown (see §5).

---

## 4. Known residuals (ship with these documented, do not chase)

1. **Apple CVBR-256 → FLAC launder** — **CORRECTED 2026-07-29, see §1.5c.**
   This was never run through the proof gate. `t-apple256-flac` exists only
   in `probe_pair.tsv.gz` (the V0/Opus experiment) and is absent from
   `results*`, `extended*` and `multiwin*`; the frozen scorer's `FLAC_FRAUDS`
   is mp3-128 / opus-96 / vorbis-q5 only. "Survives the v3 gate" is an
   **inference** from `.m4a` statistics (96–99% no-cliff vs control 99–100%;
   deficit 45–49 vs 44–48 dB), not a measurement — and running the gate on
   TRAINING's Apple encodes promotes roughly 19 of 34. The 402 paired
   examples were deleted with the encode trees.
   Still the lowest perceptual severity of any fraud class (near-transparent
   source), but the gate is validated against **three** FLAC-container fraud
   classes on four arms, not four. Closing this is what §1.6's reacquisition
   is for.
2. **Lossy-side band assertions are weak on HF-poor material** — round-3
   surfaced this: `t-aac128-mp3320` exposure fell to 18/24, five of six misses
   on HF-poor albums. Non-blocking (MP3 containers can never receive lossless
   proof). Wants the same competence-precondition treatment the affirmative leg
   got: assert nothing rather than assert weakly.
3. **Portishead's `t-aac128-mp3320` miss (36%)** — not HF-poor, fits no
   documented residual class. Unexplained.
4. **No-cliff asserts nothing** — the high end of every ladder is invisible.
   This is a permanent property, not a defect.

---

## 5. Artifacts and teardown — DONE 2026-07-29

**The calibration instance is torn down.** What follows is the record of what
was kept, what was destroyed, and the one thing that should not have been.

**Kept, and now the only surviving evidence for every constant in this plan:**
`docs/research/calibration-data/` (PR #929) — 60,102 measurements across four
arms, the ultrasonic extension, the per-window sweeps, the ground-truth audit,
the V0/Opus probe experiment, `score_v3.py.frozen`, and
`corpus-manifest.json` (115 exact MB release ids). 27 MB of TSV, 2.4 MB
gzipped.

**Destroyed:** the encode matrices (~584 GB apparent), the 38 GB ground-truth
FLAC corpus, the calib beets library and state, `cratedigger_calib`, and
`/var/lib/cratedigger-calib`. 1,003 calib-owned files were swept from the
shared slskd dir using calib's own transfer ledger **before** the DB was
dropped — 42.76 GiB, excluding 40 paths prod also claimed and touching no
protected quarantine tree. That ordering was the load-bearing part and it
held.

**The mistake:** deleting the ground-truth FLACs. It was proposed on the
reasoning that the manifest's exact MBIDs make the corpus reacquirable, which
is true but incomplete — it forecloses closing §1.5c, and any future
statistic that needs the original audio. §1.6 records the reacquisition.
Every measurement derived from the corpus survives; the audio did not.

Historical inventory (all paths now gone), under
`/mnt/virtio/Music/calibration-tmp/`:

| path | contents |
|---|---|
| `measurements/results*.tsv` | 60,102 measurements, 4 arms, raw slice vectors |
| `measurements/extended*.tsv` | 20–22 kHz extension slices |
| `measurements/multiwin*.tsv` | per-window sweeps (offsets 60/120/180) |
| `measurements/score_v3.py` | **frozen** scorer, T_ultra=62 |
| `measurements/run_round3.sh` | one-command blind-round pipeline |
| `measurements/build_holdout3_src.py` | blind-set assembly + contamination guard |
| `encodes*/` | 371 GB of encode matrices — **prunable** once tables are committed |
| `quarantine-archive/` | 47 GB of calib wrong-pressing rejects — disposable |

**Teardown order is load-bearing** (from the Phase 0–4 plan):
1. Final calib cycle → its own convergence/reapers clean its slskd state.
2. Sweep calib's remaining files in the shared slskd dir using calib's
   event-stamped `local_path`s / ledger **before** dropping the DB — once the
   DB is gone its ownership evidence is gone and leftovers become permanently
   unreapable (prod's fail-closed reaper never touches unowned files).
3. Corpus FLACs are keepers (real verified-lossless pressings): import to prod
   via the normal request flow, or archive.
4. `DROP DATABASE cratedigger_calib`; stop the transient units.

The transient units had already been GC'd by an earlier prod deploy
(`nixos-rebuild switch` → `daemon-reload` kills `systemd-run` units silently),
so no final calib cycle was possible — the ledger sweep was done directly
against the DB instead, which is why step 2's ordering mattered so much.

---

## 6. Process notes

- Focused tests while converging; whole-repo threaded Pyright + full suite once
  on the final committed tree before the first branch push.
- Implementation and review run in **sub-agents** to preserve orchestrator
  context; review-until-clean iteration is the orchestrator's job (tell
  implementers to self-review ONCE).
- PRs merged with **Create a merge commit**, never squash/rebase.
- Deploy + live-verify after merge per `.claude/rules/deploy.md`.

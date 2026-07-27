# Phase 5 — Per-Codec Spectral Model Implementation (issue #829)

**Status**: research COMPLETE and four-arm validated; **no code written yet**.
**Issue**: https://github.com/abl030/cratedigger/issues/829
**Predecessor plan**: `docs/plans/2026-07-22-001-feat-829-spectral-calibration-plan.md` (Phases 0–4)

This document is the durable pick-up point. A new session should be able to
read this file plus the linked issue comments and start the next PR without
replaying the research.

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

### PR3 — Proof gate v3 (ultrasonic leg + deficit re-threshold)

- Replace the relative affirmative leg with the level-invariant ultrasonic
  deficit `U ≥ 62` (§1). Cliff and ceiling legs unchanged.
- Legs evaluated per measurement window (offsets 0/60/120/180), **unioned** —
  any window tripping any leg denies promotion.
- HF-deficit thresholds → 65 / 69.
- Denial semantics per §2: stays provisional, surfaces in triage, never
  rejects or accuses.
- **Quality core: fable review, merge held for operator approval.**
- Reference implementation: `calibration-tmp/measurements/score_v3.py`
  (frozen 2026-07-26; its `_window_legs` / `gate` are the shape to port).

#### PR3 hard constraint — the lossless-derived cohort can never be backfilled

**93% of existing verified-lossless proofs sit on rows whose source no longer
exists.** Measured on prod 2026-07-27:

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

### PR5 — Research tables, docs, teardown

- Commit the calibration tables into `docs/research/spectral-*.md` (the six
  Phase 0 docs get their measured Phase 3/4 tables).
- Update `docs/quality-verification.md`, `docs/quality-ranks.md`, CLAUDE.md.
- Ownership-ordered calib teardown (see §5).

---

## 4. Known residuals (ship with these documented, do not chase)

1. **Apple CVBR-256 → FLAC launder** — spectrally invisible, confirmed on all
   four arms (96–99% no-cliff vs control 99–100%; deficit 45–49 vs 44–48 dB).
   Survives the v3 gate. Lowest perceptual severity of any fraud class
   (near-transparent source). 402 paired examples banked for any future
   discriminator experiment.
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

## 5. Artifacts and teardown

All under `/mnt/virtio/Music/calibration-tmp/` (doc2 + doc1, virtiofs):

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

**Calib instance is currently RUNNING** (three `systemd-run` transient units +
timer, `processing_dir` and `--var-dir` on virtiofs since 2026-07-27 — they
were on doc2's internal disk and filled it to 91%). Two round-3 stragglers
(Harold Budd *The Pearl* rid 90, Keith Jarrett *Köln Concert* rid 92) are still
`wanted` and still searching. Keep the instance alive until PR5.

---

## 6. Process notes

- Focused tests while converging; whole-repo threaded Pyright + full suite once
  on the final committed tree before the first branch push.
- Implementation and review run in **sub-agents** to preserve orchestrator
  context; review-until-clean iteration is the orchestrator's job (tell
  implementers to self-review ONCE).
- PRs merged with **Create a merge commit**, never squash/rebase.
- Deploy + live-verify after merge per `.claude/rules/deploy.md`.

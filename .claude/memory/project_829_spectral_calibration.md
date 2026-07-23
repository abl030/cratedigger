---
name: project-829-spectral-calibration
description: "Issue #829: codec-blind spectral seam found live 2026-07-22; operator chose full empirical per-codec calibration project (~1 week, heavy tokens OK)"
metadata:
  type: project
---

2026-07-22: Operator observation ("an AAC got stamped genuine today") falsified PR #827's
cross-codec scoping rationale for #828 item 1. The chain, all verified in code + live DB:

- `collect_attempt_spectral_audit` (harness/import_one.py + preview worker) is **codec-blind**
  and its output is persisted into `album_quality_evidence.spectral_grade/spectral_bitrate_kbps`
  as `subject=source, provenance=measured` — decision-facing, not audit-only.
- `full_pipeline_decision_from_evidence` feeds candidate spectral straight to the decider
  (lib/quality/pipeline.py:1282); `spectral_gate_trigger` (lib/quality/gates.py) only sees
  is_flac/is_cbr/is_vbr — the `is_mp3` condition of `_needs_spectral_check` has NO decider mirror.
- Net: MP3/LAME-calibrated grades on AAC/Opus drive Stage 1 + shared clamp + transcode detection.
  Live cross-codec clamp firing: dl 37946 (req 6387, AAC `likely_transcode 128` vs MP3 existing
  spectral 128 → tie-defer → rank/better → imported). An ordinary AAC-128's ~16-17 kHz rolloff
  reads as "MP3 128 transcode" through LAME_LOWPASS — false accusation shape.
- Three-domain taxonomy: MP3 calibrated / lossless load-bearing (fake-FLAC + VL proof,
  untouchable) / lossy non-MP3 uncalibrated (the defect domain).

**Operator decision: NO quick codec gate.** Issue #829 is the resolution: temporary pipeline DB
+ fresh ground-truth FLAC corpus (quality test-set albums + ~30 picks incl. band-limited traps),
full encoder×bitrate matrix (LAME/AAC/Opus/Vorbis/WMA + 2nd-gen fraud shapes), research docs in
docs/research/, tabulate rolloff, then per-codec decision-grade-or-audit-only model + #827 parity
domain extension. ~1 week, heavy token burn accepted. Interim mitigation NOT shipped (open
question on #829). Corrections recorded: #828 comment 5045751335, #813 pointer comment.

Related: [[project-813-819-822-series]], [[project-812-spectral-tie-fix]].

**Phase 1 LIVE (2026-07-22 21:30, opus ops agent):** 3 transient units on doc2
(cratedigger-calib-{cycle,preview,importer}), DB cratedigger_calib @ migration 62, tree at
/mnt/virtio/Music/calibration-tmp/. Prod backup FIRST: /mnt/virtio/cratedigger-backups/
cratedigger_pre-calib_20260722_205932.sql.gz (138MB, verified). Plan committed PR #830.
Canary req 1 (Daft Punk RAM 2013 CA CD) proved full chain; self-healing to import.
**FOOTGUNS (issue comment 5046382840 has full detail):** (1) calib pipeline-cli MUST append
`--dsn <calib>` — wrapper hardcodes prod dsn, env var does NOT steer CLI DB; workers also need
PIPELINE_DB_DSN env. (2) DB admin via `nsenter -t <nspawn-leader> -a runuser -u postgres`
(machinectl/systemd-run -M bus-unreachable). (3) set-intent lossless leaves
search_filetype_override NULL on wanted rows — guarded UPDATE per seeded request. (4) fresh
instance needs empty beets lib created with shipped beets BEFORE first import (BeetsDB
hard-fails on missing file). Transient units die on doc2 reboot — re-arm next session.

**Corpus SEEDED (2026-07-22 22:00):** 38/38 — rids 2-7 = test-set albums (exact prod pressings:
Mark DeNardo 1308, Mtn Goats Flux 4514, Deerhunter Rhapsody 6795, Lil Wayne DD3 3779,
Taboo VI 257, Tyler Lambert's Grave 249), rids 8-39 = 32 spectrum picks (manifest with MBIDs:
issue #829 comment 5046791015). All 39 rows target_format=lossless + override=lossless,
verified. Loop consuming at lossless scope. FLAC retention proven via real decider
(target lossless → target_final_format=flac, verified_lossless=True, no conversion).
**--dsn footgun CORRECTED: global option, must go BEFORE the subcommand**
(`pipeline-cli --dsn <calib> query ...`); appended after subcommand = loud argparse error,
not silent prod hit. Wrong-pressing FLAC rejects are retained + valid ground truth
(operator: harvest them tomorrow too). Session ended with units running overnight;
NEXT SESSION: check import coverage, harvest rejects, re-arm units if doc2 rebooted,
then Phase 0 research docs + Phase 2 encode matrix on doc1.

**Phase 0 COMPLETE (2026-07-23, PR #831, 6 sonnet subs):** docs/research/spectral-{mp3-lame,
aac,opus,vorbis,wma,transcode-detection}.md merged. Headlines: LAME_LOWPASS is byte-exact vs
LAME source BUT is LAME-not-MP3 calibration (Xing/Helix = fixed 16kHz at any bitrate); libfdk
AAC CBR caps at 17kHz from 96kbps up; Apple AAC has NO published table (empirical only);
Opus = flat fullband from ~12kbps → audit-only ≥64k; Vorbis has a source ladder but q6+ is
uncapped and reference q5 (Spotify 160k) cuts at 20.1kHz — PAST our 12-20kHz slice window;
WMA DROPPED from Phase 2 matrix (ffmpeg wmav2 ≠ the real WMP9 encoder); bare cliff provably
blind to clean same-codec transcodes. Plan deltas + prediction tables: issue comment
5052550261. Overnight corpus: 25/39 imported (337 FLACs, 11GB), 13 hunting, 53 rejected-FLAC
harvest pile across 21 albums. NEXT: Phase 2 encode matrix on doc1 + harvest promotion pass.

**validate_audio gap found via #829 corpus sweep (2026-07-23):** ffmpeg rc=0 conceals
recoverable frame corruption (Syro tr08 imported corrupt; flac -t fails it) — filed #835 with
invariant + fix directions + prod-library sweep remediation. Corrupt track excluded from matrix.

**Phase 2 COMPLETE (2026-07-23 09:26):** 19,698 encodes (402 tracks x 49 variants, 0 failures,
157GB) at calibration-tmp/encodes/ + manifest.tsv. Corpus 34 albums: 27 Beets (Flux + Taboo VI
landed mid-shutdown, verified) + 7 fingerprint-attributed quarantine harvests. Wrong-match
retention HELD (my deletion claim was wrong — all 53 in failed_imports; #833 is operator issue
on the move-to-quarantine behavior). Da Drought 3 = fake FLAC (caught, dropped). Syro tr08
corrupt (excluded, #835). NEXT: Phase 3 measurement over the matrix + prediction scoring.

**Phase 3 COMPLETE (2026-07-23):** 19,698/19,698 measured, 0 errors; tables+scorecard = issue
comment 5053754812; raw = calibration-tmp/measurements/results.tsv (incl. 16-slice vectors).
HEADLINES: fdk 17k cap CONFIRMED (fdk 192-320 all read "MP3 128"); detect_cliff reports first
steep slice ~1 tier BELOW encoder lowpass (LAME 192→est 160 @75%) — rebuckets must be derived
in DETECTOR space from this corpus; window truncation starts at LAME-224 (not 320) + misses
below 12k (CBR-64); HE-AACv1-64 reads AS LOSSLESS (100% no-cliff, mandatory SBR gate);
opus→flac fakes invisible; HF_DEFICIT thresholds flag 61%/14% of genuine lossless as
marginal/suspect (metric mis-thresholded for real music — trap albums did their job).
NEXT: Phase 4 synthesis/model design (quality-core PR series, fable review, operator holds).

**Verification plan (2026-07-23, operator):** ground truth ultrasonic-audited CLEAN (0/34
ceiling signatures; 4 albums are true 96k hi-res); 24-album HOLDOUT cohort seeding (blind —
sealed until verifier built; incl. web-native provenance-murky picks); prod failed_imports =
read-only adversarial test set for the new verifier; VL stamps recorded as old-model-relative.
Ultrasonic 20-22k band = prototyped Phase 4 feature (kills invisible-launder family).

**Holdout SEEDED (2026-07-23): 25 albums rids 40-64** (list said 24, enumerated 25 — all in),
calib re-armed on rotated prod store paths, 4 downloading at sign-off, blind protocol active
(sealed until verifier built). Manifest: issue comment. Prod healthy.

**Phase 4 DESIGN FORMALIZED (2026-07-23, issue comment):** 3 operator decisions recorded
verbatim — (1) Apple AAC gap closes via nixosconfig#46 (qaac on wsl host, hermes token used,
old claude-token memory path fixed); (2) VL stamps RE-MEASURED under new model, policy after
failure totals known; (3) zero-false-accept holdout bar (leaning, confirm post-measurement).
Model: per-codec verdict table + cliff_hz primitive + window/ultrasonic extension + deficit
re-threshold + codec-aware seam + parity extension + VL proof-version. Phase 5 = 4-PR series,
fable review, operator merge holds.

**Apple arm COMPLETE (2026-07-23 14:0x):** VM qaac-encoder (192.168.1.135, nixosconfig#46,
prom VM 122) delivered 1608 genuine Apple encodes; measured. HEADLINE: apple-cvbr256 invisible
in-window (98% no-cliff, control-like grades) AND carries real 20-22k energy → defeats the
ultrasonic band too = the ONE known residual launder blind spot (lowest perceptual severity;
402 paired examples banked for discriminator experiments). apple-cbr128 visible + correctly
bucketed. Manifest 21,306 rows. AAC content-floor verdict unchanged. bash IFS-in-mode-loop bug
+ empty-shard-resume bug caught by smoke test (apple_matrix.sh).

**Phase 4 ANALYSIS COMPLETE (2026-07-23, issue comment):** derived constants — MP3 detector-
space buckets (<15000→96|16000→128|17250→160|18250→192|19250→256|≥19250→320-class), Vorbis own
ladder, AAC floor table (13-18k floors at 96-128 → never accuse), deficit 65/69 (control FP
5.5/1.5% track, ~0 album), window→22k ADOPTED with SPLIT SEMANTICS (10% genuine false-cliffs
at 20k → high-band cliffs = tier-up info only; lossless launder test = ultrasonic hard-floor
≥75dB — catches t-opus96-flac 88%). Apple-256 residual confirmed no cheap separator. Phase 5
re-derives exact numbers jointly (window+buckets+thresholds = one unit). AWAITING: operator go
for Phase 5 PR series; holdout still acquiring.

**BLIND ROUND 1: FAIL (2026-07-23):** launder arm collapsed on opus/vorbis→flac (2/15, 1/15) —
75dB ultra-floor never validated vs training frauds (sinc leakage = 35-60dB apparent energy above
codec ceilings) + split semantics excluded the ≥19250 cliffs where ceilings live. Controls
PERFECT (0/15 false flags). REWORK: ceiling-step detector + two-signal flagging + band-assert
buckets, training-data only; round-2 blind = remaining 10 holdout albums (calib still pulling).
Phase 5 PARKED until fresh blind PASS. Lesson: validate every derived constant against training
frauds before freezing the scorer.

**REWORK DERIVED (2026-07-23 eve):** album-mean-profile ceiling step + position consistency →
opus-flac 34/34; 4 control flags = REAL ceilings (Gould 1982 early-digital, Loveless, FMWT,
Taboo VI) — mastering ceilings physically identical to codec ceilings, fundamental ambiguity.
→ SEMANTICS REFRAME proposed on issue (needs operator nod): ceiling/no-affirmative-ultrasonic
content WITHHOLDS VL PROOF (never rejects/accuses); bar becomes zero-frauds-RECEIVE-PROOF.
vq5 caught via affirmative-content leg; apple-256 residual unchanged. NEXT: operator nod →
threshold qualification → scorer v2 freeze → round-2 blind (cohort seeding).

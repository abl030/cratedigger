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

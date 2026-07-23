# Per-Codec Spectral Calibration — Project Plan (issue #829)

**Status**: Phases 0–3 COMPLETE (2026-07-23). Phase 4 (synthesis/model design) is next.
Holdout acquisition in flight (blind). Detailed per-phase records live as issue comments.
**Issue**: https://github.com/abl030/cratedigger/issues/829
**Origin**: #828 item 1 correction (2026-07-22) — the codec-blind spectral seam.

## Status log

- **Phase 0 ✅** (2026-07-23, PR #831): six research docs in `docs/research/`
  (mp3-lame, aac, opus, vorbis, wma, transcode-detection), each with testable
  Phase 3 predictions. Plan deltas: WMA dropped from the matrix; MP3
  encoder-identity control recorded as unavailable in nixpkgs.
- **Phase 1 ✅** (2026-07-22/23): temp instance on doc2 (scratch nspawn DB
  `cratedigger_calib`, three `systemd-run` transient units, prod backed up
  first). Final corpus: **34 verified-lossless albums** — 27 from the calib
  beets tree + 7 harvested from the `failed_imports/` quarantine, attributed
  strictly by calib ledger `attempt_fingerprint`. One fake-FLAC caught and
  dropped at the harvest bar (Da Drought 3, 27/29 tracks cliffing).
- **Phase 2 ✅** (2026-07-23): encode matrix — **19,698 files = 402 tracks ×
  49 variants, 157 GB, zero failures** (`calibration-tmp/encodes/` +
  `manifest.tsv`). One corrupt ground-truth track excluded → filed #835
  (validate_audio rc=0 conceals recoverable frame corruption).
- **Phase 3 ✅** (2026-07-23): all 19,698 files measured with the production
  primitives (raw 16-slice vectors captured), zero errors. Headlines:
  libfdk's 17 kHz CBR cap confirmed (FDK 192–320 all read as "MP3 128");
  `detect_cliff` reads one tier low (~1 slice below the encoder lowpass, so
  LAME-192 buckets as 160 at 75%); window truncation already bites at
  LAME-224/256 (84–89% invisible) and below 12 kHz (CBR-64); HE-AACv1 64k
  reads as *lossless* (SBR gate is mandatory); opus→flac fakes are fully
  invisible; the HF-deficit metric flags 61%/14% of genuine lossless as
  marginal/suspect (mis-thresholded for real music); Vorbis q5 (Spotify
  tier) misses the window at scale. Tables + full scorecard on the issue.
- **Verification additions** (operator direction, 2026-07-23): ground-truth
  circularity audit via a 20–22 kHz ultrasonic probe — **clean, 0/34
  ceiling signatures** (4 albums are true 96 kHz masters); a 24-album
  **holdout corpus** now being acquired under a blind protocol (sealed until
  the verifier exists); prod's `failed_imports/` designated the read-only
  adversarial test set for the new verifier. Standing caveat recorded:
  existing `verified_lossless` stamps are proofs under the OLD model's
  assumptions.
- **Phase 4 (next)**: per-codec verdict table; detector-space bucket
  re-derivation from the corpus; SBR pre-classification gate; slice-window
  extension decision (upward + the ultrasonic band); HF-deficit metric
  redesign; evidence-schema primitive (cutoff Hz?) + migration story;
  #827 parity-property domain extension; restated verified-lossless proof
  semantics. Then Phase 5 implementation (quality-core PRs: fable review,
  merges held for operator approval) and the ownership-ordered teardown.

## Why

The spectral subsystem's cliff→bitrate table (`lib/spectral_check.py::LAME_LOWPASS`)
and its grade thresholds are calibrated to exactly one encoder: LAME MP3. But the
attempt spectral audit (`collect_attempt_spectral_audit`, `harness/import_one.py` +
the preview worker) measures every codec, persists its output as decision-facing
evidence (`album_quality_evidence.spectral_grade` / `spectral_bitrate_kbps`,
`subject=source`, `provenance=measured`), and the decider's gate mirror
(`spectral_gate_trigger`, `lib/quality/gates.py`) receives only
`is_flac`/`is_cbr`/`is_vbr` — it cannot see codec. The measurement-side gate's
`is_mp3` condition (`lib/measurement.py::_needs_spectral_check`) has no decider
mirror. Net: MP3-calibrated spectral grades on AAC/Opus/Vorbis candidates drive
Stage 1, the shared spectral clamp, and transcode detection in live production
(canonical live example: download_log 37946, 2026-07-22 — an ordinary AAC's natural
~16–17 kHz rolloff stamped `likely_transcode 128` and clamped cross-codec against
an MP3 existing).

Operator decision (2026-07-22, recorded on #828/#829): solve this with real
empirical calibration — a ground-truth corpus, a full encoder×bitrate matrix, and
per-codec research — not a codec gate at the decision seam.

The three-domain taxonomy the project preserves:

- **MP3** — calibrated today (LAME buckets); the matrix re-validates it as a control.
- **Lossless sources (FLAC/WAV/ALAC)** — spectral stays load-bearing exactly as-is:
  cliff = fake-FLAC detector, `genuine` = the affirmative input verified-lossless
  proof requires. Untouchable.
- **Lossy non-MP3 (AAC/Opus/Vorbis/WMA)** — uncalibrated; the defect domain this
  project defines semantics for.

## Goal

An empirically calibrated, per-codec spectral model: for each codec/encoder family,
either a real cutoff→quality mapping with measured confidence bounds, or an
evidence-backed verdict that spectral is not decision-grade for that codec
(→ audit-only, fail-closed). Then extend the #827 Stage-1/Stage-2 parity property's
domain with the defined semantics (closing #828 item 1 properly) and correct the
three shipped statements of the falsified scoping claim (`StageParityWorld`
docstring, `docs/quality-verification.md` § stage parity, the #813 audit record).

## Phases

### Phase 0 — Research sweep (docs first)

Research documents under `docs/research/`: `spectral-mp3-lame.md`, `spectral-aac.md`
(ffmpeg native vs libfdk vs Apple; HE-AAC/SBR breaks naive cliff detection),
`spectral-opus.md` (CELT band allocation; expectation: no usable bitrate→cutoff
mapping), `spectral-vorbis.md`, `spectral-wma.md`, and
`spectral-transcode-detection.md` (prior art: hydrogenaudio lore, Lossless Audio
Checker, the auCDtect lineage; what distinguishes native-low-bitrate from
transcoded beyond a bare cliff).

### Phase 1 — Ground-truth corpus via a temporary cratedigger instance

Topology is forced by the network (verified 2026-07-22): slskd is a microVM at
`192.168.21.2` on a doc2-local DMZ bridge — unreachable from doc1 — so the
acquisition pipeline runs on doc2. Encode/measure phases run on doc1 over the
shared `/mnt/virtio` files.

- **Database**: scratch `cratedigger_calib` in the existing nspawn PG cluster
  (`10.20.0.11`). Advisory locks are database-scoped (no prod collision); schema
  via the migrator; teardown is one `DROP DATABASE`.
- **Filesystem** (virtiofs-shared, visible from both hosts):

  ```
  /mnt/virtio/Music/calibration-tmp/
  ├── state/            # config.ini, BEETSDIR, denylists, lock
  ├── Incoming/         # staging (auto-import/, post-validation/)
  ├── Beets/            # temp beets library tree (FLACs land here)
  ├── beets-library.db
  ├── encodes/          # Phase 2 matrix output (written from doc1)
  └── measurements/     # Phase 3 raw analyzer output + manifests
  ```

- **Config**: `CRATEDIGGER_RUNTIME_CONFIG` (`lib/config.py`) points every
  entrypoint at a clone of prod's rendered config.ini with: calib DSN, calib
  staging/beets paths, notifiers blanked, same slskd + mirror + quality ranks.
  The main loop takes `--config-dir` instead. Calib BEETSDIR is a clone of the
  module-rendered config.yaml with library/directory swapped; same pinned beets.
- **Runtime**: no deploys, no nixosconfig changes, no committed units. Prod's
  installed store-path binaries + three `systemd-run` transient units
  (`cratedigger-calib-cycle` on an `--on-unit-inactive=120` transient timer,
  `cratedigger-calib-preview`, `cratedigger-calib-importer`). Transient units die
  on reboot; the agent re-arms them next session.
- **Seeding**: `pipeline-cli add <exact MB release ID>` + `set-intent lossless`
  per request (FLAC-only search + keep-lossless target). Corpus = the quality
  test-set albums (`TestLiveBugReproductions`) + ~30 picks across a wide spectrum,
  deliberately including false-positive traps (pre-1975 masters, vinyl rips,
  lo-fi/shoegaze/ambient, dense metal, full-spectrum electronic, classical,
  loudness-war pop). Acceptance bar per album: verified-lossless proof. YouTube
  rescue is excluded (not a lossless source).
- **Shared-slskd safety**: #571 good-citizen ownership makes two instances on one
  slskd safe by construction — each instance's ledgers scope its own searches,
  transfers, purges, and disk reaping; foreign keys and unledgered state are never
  touched, in either direction.

### Phase 2 — Encode matrix (doc1)

From each ground-truth FLAC: LAME CBR 320/256/224/192/160/128/96/64 + V0/V2/V4/V6;
AAC (ffmpeg native + libfdk, HE-AAC at low rates); Opus and Vorbis ladders; WMA if
ffmpeg encodes it sanely; plus second-generation fraud shapes (MP3-128→FLAC,
MP3-128→AAC-256, AAC-128→MP3-320, Opus-96→FLAC, …). Deterministic manifest;
encoders pinned from nixpkgs.

### Phase 3 — Measure and tabulate (doc1)

Run the production analyzer over everything, capturing raw per-track cliff
frequency (Hz), not just LAME-bucket outputs. Tables: codec × encoder × setting →
rolloff distribution, committed inside the research docs. Four questions the data
must answer:

1. Does codec X have a stable bitrate→cutoff mapping at all? (Opus expected: no.)
2. Within codec X, is native-low-bitrate distinguishable from transcoded-from-lossy?
3. False-positive rate on genuinely band-limited lossless material (the traps)?
4. What common currency makes cross-codec spectral comparison meaningful — the
   semantics the parity-property domain extension needs.

### Phase 4 — Synthesis and model design

Per-codec verdict (decision-grade with tables, or audit-only fail-closed);
evidence semantics (store cutoff Hz as the primitive?); migration story for
persisted LAME-bucketed evidence rows; parity-property domain-extension design;
UI display semantics for non-decision-grade codecs.

### Phase 5 — Implementation

Staged PRs, each with the pin + generated-property PAIR; fable-tier review with
merges held for operator approval (quality-core rule). Temporary-instance
teardown.

## Teardown (order is load-bearing)

1. Final calib cycle → its own convergence/reapers clean its slskd state.
2. Sweep calib's remaining files in the shared slskd download dir using calib's
   event-stamped `local_path`s / ledger **before** dropping the DB — once the DB
   is gone its ownership evidence is gone, and leftovers become permanently
   unreapable debris (prod's fail-closed reaper never touches unowned files, by
   design).
3. Corpus FLACs are keepers (real verified-lossless pressings): import into prod
   via the normal request flow or archive, then remove `calibration-tmp`
   (encodes/measurements pruned after the research tables are committed).
4. `DROP DATABASE cratedigger_calib`; stop remaining transient units.

## Open questions (tracked on #829)

- Interim mitigation while the project runs (lossy non-MP3 spectral still drives
  live decisions today) — flagged, not decided.
- Scratch storage: ~100–200 GB on `/mnt/virtio` for the matrix (prunable).

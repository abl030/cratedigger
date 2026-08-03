---
name: project-829-research-round-closed
description: "Issue #829: 2026-07-31 four-thread research round CLOSED with conclusions; Derrien offset rule = proof-grade Apple discriminator; PR3 unblocked"
metadata:
  type: project
---

2026-07-31. The #829 research loop was closed by four parallel bounded
experiments (opus agents), all evaluated leave-one-album-out with FP priced on
all genuine controls. PR #950 (the 07-30 no-conclusions research state) merged
first; the 07-31 round record committed as its own docs PR.

**Verdicts:**
- **Within-album homogeneity: NO** — hypothesis falsified sign-inverted
  (laundering *inflates* dispersion for V0/Vorbis, does nothing for AAC; paired
  oracle at coin flip on the AAC class; LOAO multivariate AUC 0.508).
  Methodological keeper: null-feature control (`ref_sd`) scores AUC 0.60 from
  corpus composition alone — any AUC <=0.60 on that data is noise.
- **Rolloff shape: NO** — AAC/Apple shape delta <=0.24 dB across all 20 bands;
  SNR 0.03–0.09 vs genuine-to-genuine distance. Law: gate-caught classes have
  shape SNR > 1, escape classes < 1 — transparency is a measured property of
  production granularity. Also: matrix `qaac-cvbr256` == apple-arm A
  (215/215 byte-identical; qaac deterministic) — only arm B (20 albums) is
  independent Apple evidence.
- **Derrien: YES partial** — the **offset-concentration rule** (>=4 tracks at
  one MDCT frame offset; parameter-free; analytic FP ~0.002/5000 albums)
  catches 100% of the Apple/CoreAudio family (mechanism: CoreAudio primes 2112
  samples -> offset 960; ffmpeg primes 1024 -> offset 0). PREMISE CORRECTION:
  Apple was never scored against Derrien (hardcoded ORDER excluded qaac-*) —
  the existing pooled rule already closed it (0/10 survive spectral-union-
  Derrien). NAC probe and mode=low are dead. z>6.914 is triage-grade only
  (Gumbel tail -> ~490 FP/5000). Permanent spectral residual: lame-v0,
  vorbis-q10, ~half of ffmpeg-AAC 256/320.
- **Provenance: PARTIAL** — three bugs fixed (ARv2 was compared against
  `checksum_450` — AR's `checksum` field is v1 OR v2 undifferentiated;
  track-1 off-by-one skip 2941->2940; float64 promotion in offset-scan prefix
  sums). Result: 25/38 FLAC albums (66%) bit-verified vs 9 before; CTDB
  convention reverse-engineered (zlib.crc32, stride 5880, host db.cue.tools).
  Ceiling 42 lossless albums = 0.49% of library -> positive-only badge tier.

**Operator decisions queued:** (1) productize offset rule as v4 AAC-lattice
proof leg (~50 s/track, promotion-time, numpy dep); (2) provenance badge tier;
(3) PR3 start. PR3 design unchanged by all of this — ships under section-1.7
bounded claim; blind-spot copy can now cite measured boundaries.

Related: [[project-829-phase5-pr2-shipped]], [[project-829-spectral-calibration]].

**PR3 status 2026-07-31:** implemented, twice-reviewed (blocking
denial-semantics defect found by independent review and fixed: the leg decides
the PROOF, never the LANE — v0_verified_override stays leg-free), receipts
minted, **PR #966 open awaiting operator merge approval**. Differential: 0
changed rows as-persisted; counterfactual 52 rows, imported flips 0. Next after
merge: deploy+verify, then Derrien offset-leg PR pair, then PR4 (copy once,
generation-aware), PR5. Provenance tier deferred to issue #962.

**PR3 SHIPPED 2026-07-31:** PR #966 merged (fedd28ce), deployed (nixosconfig
47ad3981), cycle-verified (invocation 1694ed6f). v3 leg live at T=59.5.
Reflection issue #967 filed (current=None pin blindness audit is its top item).
Merged research/agent worktrees cleaned. Next: Derrien offset-leg PR pair
(corpus rows 8916-8935 held for its live validation), then PR4, PR5.

**PR-A SHIPPED 2026-08-01:** AAC-lattice capture live (PR #968, merge fcc82e52,
nixosconfig 65c6d64c, migration 069, cycle 6d52ec33 verified). Port proven
bit-exact vs frozen reference by adversarial review; 11/11 mutants killed after
F1-F5 hardening. Preview worker now captures lattice evidence on the
promotion-plausible cohort (lossless + genuine/marginal grade, 6-track cap,
rate pre-screen). Key facts: ~49s/track was under contention, uncontended
~2-3s; ALAC absolute modal offsets are decode-path-shifted so the leg reads
CONCENTRATION only; capture is opt-in via captured default at
measure_and_persist_candidate_evidence (sync classify path pays nothing).
PR-B (the leg, operator-held) in progress.

**PR-B OPEN 2026-08-01:** AAC-lattice proof leg + v4 classifier, PR #969,
twice-reviewed (FIX-THEN-SHIP -> fixed: stale differential zero, doc
overclaim, vacuous L2b oracle), receipts minted, AWAITING OPERATOR MERGE.
Live validation: 48/48 research offset agreement on corpus albums; ffmpeg
launders denied by concentration alone; wild quarantine catch = Lil Wayne
Da Drought 3 (k=6 offset 0). First live v4 proof: evidence 34681. After
merge+deploy: corpus rows 8916-8935 release decision, then PR4/PR5.

**PR-B SHIPPED 2026-08-01:** PR #969 merged (6d133a90), deployed (nixosconfig
80f86734), cycle f609d5c6 verified. v4 proof gate complete and live. Next: PR4
(tiered verdicts+display, Rule D heavy), PR5, corpus 8916-8935 release
decision, §1.5a re-measurement sweep decision.

**2026-08-01 decisions:** corpus 8916-8935 DELETED (verified 0 rows both DBs);
§1.5a re-measurement sweep CLOSED as moot by measurement (7053/7187 proofs are
opus — sources gone; the "8273 re-measurable" figure was stale); convergence
prompt split out of PR4 into its own follow-up. PR4 (tiers+display, Rule D +
screenshot loop) started. doc2 login shell is ZSH — always `ssh doc2 'bash -s'`
for loops; zsh doesn't word-split $var.

**2026-08-01 late:** Badlands incident → two rulings (config decides formats;
proof-blind ranking). Conversion fix PR #972 OPEN awaiting held merge (775 rows
None->opus128, SHIP verdict). PR4 (branch feat/829-pr4-verdict-tiers, worktree
agent-abfa200bd895415c9) review-fixed except N3 4-surface completion in flight;
then receipts+PR. Tiers 2/3 reserved (no ceiling leg producer).

**Both PRs OPEN 2026-08-01:** #972 (conversion fix, SHIP-verdicted) + #973 (PR4 tiers+display, 0/0+0 differentials, 6 surfaces). Awaiting operator merges; #973 may need trivial rebase after #972. Then deploy both, PR5 + convergence-prompt PR remain.

**SHIPPED 2026-08-01:** #972+#973 merged (main 8baead3d), deployed (4f1d7583), cycle 70a08116 verified. Remaining: PR5, convergence-prompt PR, reflection.

**PR5 SHIPPED 2026-08-01:** #974 merged (docs-only, prediction scorecards, teardown verified). Remaining: convergence-prompt PR, post-series reflection, operator call on 168GB disposable launder audio.

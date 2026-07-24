---
name: project-859-sidecar-manifest-poisoning
description: "2026-07-24: #859 P0 fixed+deployed (PR #861) — preview sidecar relocated to shared tempfile writer; widest-boundary rule added; reflection #862"
metadata:
  type: project
---

2026-07-24: Issue #859 (P0, third incident in the #663 hardening chain after #844→#853→#858) CLOSED. RCA: #858 made automation previews operate in place on owned canonical albums, but the preview sidecar writer (a parallel implementation of the importer's already-safe tempfile writer) wrote `preview-spectral-evidence.json` INTO the canonical album dir; `_canonical_manifest_complete`'s exact-equality guard then deferred every automation import forever behind a generic message.

Fix (PR #861, merge 645de735, deployed under strict hold, nixosconfig pin 39c95925):
- ONE shared writer/remover in `lib/evidence_action_file.py` (tempfile, outside album dirs), used by both `lib/dispatch/evidence_gate.py` and `lib/import_preview.py`; unconditional removal in both preview outermost `finally` blocks.
- `CompletionDeferred.detail` now surfaces in the job message; deferred stays a terminal failed job, poller owns retries (documented in `_dispatch_outcome_from_completion`).
- Invariant PAIR in `tests/test_preview_manifest_generated.py` (composed pin with REAL writer + REAL materializer, exact-listing property, known-bad self-tests). RED evidence: `extra=['preview-spectral-evidence.json']` + MaterializeGuarded.
- The manifest guard's exact equality was deliberately NOT weakened.

Recovery: exactly 2 poisoned sidecars (reqs 2401/8886), removed by exact filename during the hold window; controlled cycle 963bb1b8 + ordinary successor 9afca336 verified. 8886 Bleed American → `imported` (P0 path proven live); 2401 → honest `high_distance 0.1714` reject → `wanted`.

Process lesson encoded permanently: `.claude/rules/code-quality.md` § "Invariants live at the widest boundary the change touches" + CLAUDE.md critical invariant 9 (canonical processing albums are exact media manifests). Both #853 and #859 were cross-worker composition failures that module-scope tests and diff-scoped review structurally could not catch.

Residuals in reflection issue #862: (1) pin the sidecar path as outside the album tree (relocation-only revert survives the current pin because the finally cleanup masks it); (2) real-beets dry-run manifest-purity pin in `test_harness_beets2_contract.py`. RESOLVED watch item: the validate-vs-apply distance divergence (8887: 0.082 → 0.5637 reject) is NOT burst/mirror strain — it is the #835 conversion metadata-strip (commit 4c172faf, 2026-07-23, operator-authorized policy item 1: conversion drops source metadata because beets applies fresh metadata after conversion). Hidden false premise: beets must MATCH on the staged tags before it can apply fresh ones, so converted albums are matched tagless (filenames+lengths only). Blast radius: automation applies after the Jul 23 deploy only — Markus Mehr 0.4883, Rave 92 0.3663, Bleed American 0.3359 imported; 8887 falsely rejected + SevenNines wrongly denylisted. Pre-strip automation applies were all ≤0.14; the Jul 21-22 high-distance applies were operator force imports, not inflation. SHIPPED 2026-07-24 as #863 / PR #864 (merge 26e20afc, pin 53a46f91, cycle bd0384fd verified): _KEPT_OUTPUT_METADATA_ARGS preserves the match-tag surface, deletes only art-in-tag surfaces (operator: untrusted embedded art = attack surface); V0 probe still strips; apply-time distance now persisted as ImportResult.apply_beets_distance; real-ffmpeg invariant pair in tests/test_conversion_metadata_generated.py. The rc=2 reject wrote NO denylist/cooldown row — no cleanup was needed; 8887 re-attempts organically.

#865 card-message follow-up SHIPPED 2026-07-24 (PR #866, merge fecd64ae, pin 4b89a3d4, cycle f8f74160): RunImportOutcome.failure_reason (apply-reject/timeout/MBID-skip) + _harness_failure_error shared message decision; CompletionFailed.reason suffixes job messages; card Distance row shows apply distance (DownloadHistoryViewRow.apply_beets_distance + withApplyDistance). LIVE PROOF of the whole #863 chain: dl 38197 (req 3356) succeeded with apply_beets_distance=0.1205 persisted — apply distance back in the normal band vs the tagless-era 0.33-0.56 cluster.

Related: [[project-829-spectral-calibration]], [[feedback-review-loop-at-orchestrator]], [[feedback-pin-and-fuzz-pair]]

---
name: 1089-slice1-merge-rekey-shipped
description: 2026-08-13 operator merge-rekey button shipped+deployed+live-verified; 316/8832 rekeyed, drift 3→1; remaining 1089 slices are Frozen ghost + Slipknot decision
metadata:
  type: project
---

2026-08-13: #1089 slice 1 (operator merge-rekey button) SHIPPED — PR #1139, 4 opus review
rounds, deployed `c7b46e3a` / nixosconfig `f192196b`, live smoke pressed all three
buttons: #316 and #8832 rekeyed onto survivors (evidence lineage moved, drift 3→1,
on-disk +2), #8792 refused `not_merged` correctly. Audit strict violations 677→673.

Remaining #1089 scope (issue stays OPEN): Frozen ghost album (beets album 19823,
request 8871 live-wanted — `beet remove` one-shot + RCA on failed-import-leaves-album +
library-root bucket-C invariant), Slipknot #8792 decision (deferred: bonus-disc album
6612 shares MBID with 18672), latent pair #1838/#8815 (beets still at old id; flow:
operator retags beets → drift row appears → button; tag-WRITING retag fails the
evidence witness closed — sanctioned `-W` no-write shape does not).

Process notes: [[orchestrator-briefs-become-defects]] held — every correction round
minted ≥1 false claim, caught only by the next fresh reviewer (4 rounds, fresh opus
each time; round-1 reviewer prescribing fixes means rounds 2+ need NEW reviewers).
Merged-tree re-gate caught main sitting RED on the 32 KiB adapter limit (two green
PRs exceeded it combined) — residuals filed as #1140.

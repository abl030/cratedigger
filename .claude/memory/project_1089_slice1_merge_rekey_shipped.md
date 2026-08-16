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

SLICE 2 SHIPPED 2026-08-13 (PR #1146, deployed 9cec6d42): recovery-side beets
crash-debris removal (operator grant, metadata-only delete-child mode, zero
denylist writes, wired into all three producers), library-root bucket-C
invariant (0 live firings / 8,488 albums), graceful importer drain
(KillMode=mixed + TimeoutStopSec=10min — R1 BLOCKING: without mixed, the drain
routed the incident world around the fix). False-claim streak: 7-for-7
correction rounds across both slices minted ≥1, all caught by next fresh
reviewer. Issue #1089 now open ONLY for the Slipknot #8792 decision.

CLOSED 2026-08-13: Slipknot dupe (album 6612, 8-track bonus-disc partial) removed
after title-containment check (one apparent miss = "Vermillion" tag misspelling);
real RCA was the June force import's already_in_beets=false miss (request-keyed
resolution lineage, fixed #1067, residual #1138) — replace machinery never engaged.
Tracked audit PASS, new_members=0, state shrank 641→619. First green gate since 08-08.

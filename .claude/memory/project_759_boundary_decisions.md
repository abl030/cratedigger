---
name: project-759-boundary-decisions
description: Issue #759 Cratedigger–Beets ownership boundary — all 10 design points settled 2026-07-24 in 7 decision comments; implementation plan owed
metadata:
  type: project
---

2026-07-24: All ten "Remaining design decisions" in issue #759 settled in chat and
posted as Decisions 1–7 (comments on the issue; index comment maps bullets→decisions).
Headlines: three-fact model (achievement / live-resolved holding / quality-at-import
with lazy decision-gate revalidation — no projection table, no scheduled reconciler);
acquisition-fact set (VL proof, source-subject spectral/V0) carries UNCONDITIONALLY
across every evidence rebuild — library drift never strips verified lossless;
sibling retag = no correlation machinery, just `current_beets_missing` + new
"Untracked" badge; audit codes classified into three buckets (Cratedigger integrity /
projection health / Beets library health); ownership inversion — operator brings
their own beets config, cratedigger consumes + validates a config contract at
startup (fail closed), `cratedigger-beet` retires, DB stays at
/mnt/virtio/cratedigger/beets-db/ (no move).

Much of #759's body was already stale when discussed: #762 / PR #800 (2026-07-21)
had dropped `imported_path` (migration 061) and replaced path-name audit codes with
identity/content ones.

Next: implementation plan per the issue's Desired outcome (nixosconfig beets module
+ operator `beet` on doc1/doc2, module inversion, audit bucket grouping, Untracked
badge, docs/CLAUDE.md ownership-framing rewrite).

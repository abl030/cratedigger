---
name: 1122-1127-series-complete
description: 2026-08-13 the #1119/#1122/#1127 residual-triage series shipped (PRs #1132/#1133/#1135), deployed 657d155f, all three issues closed; #1119 and most of #1127 declined as over-hardening
metadata:
  type: project
---

2026-08-13: triaged post-ship reflection issues #1119/#1122/#1127; operator trimmed scope to "someone actually gets hurt" items only — #1122 items 1-4 + #1127 item 3 — and explicitly declined the test/fixture hardening (#1119 audit, #1127 VM gate fixture) as over-engineering; all three issues CLOSED with rationale, residuals in #1136.

Shipped + deployed (cratedigger 657d155f, nixosconfig 38b52fca, cycle 73f2d308 verified): PR #1132 (triage scans FOUR quarantine roots + CLI socket-relayed after an EACCES blocker vs the 0700 processing tree; Wrong Matches card renders `detail`), PR #1133 (durable force-import wrong-match receipt replay, success-keyed + era-marker predicate — naive version would have replayed 619 historical rows, 477 through the delete lane), PR #1135 (`protected_staging_roots` guards every prune site incl. the harness success path; metadata-gate doc corrections).

**Why:** every PR needed a correction round; ALL decisive review findings were assumption-layer (live-corpus measurement, identity/permission reality, cross-file invariant tracing), none diff-layer. The sonnet-implement/opus-review-loop again earned its cost.

**How to apply:** brief PR2-style replay/backfill designs with an era/lane marker + success-keyed receipts from the start; any CLI touching the 0700 processing tree must socket-relay (#1063 cohort); mixed-home @given properties get no fuzz depth (issue #1136 item 1). Related: [[agents-idle-wait-die-at-gates]].

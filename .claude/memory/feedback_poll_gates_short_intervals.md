---
name: poll-gates-short-intervals
description: doc1 gates are FAST now (~2 min); poll at 60-90s, never long sleeps
metadata:
  type: feedback
---

The canonical suite/final gate is ~120s and the full fuzz burst ~150s on doc1
(post-#1131/#1156/#1226/#1229 optimization series). On 2026-08-25 the agent
slept 7-9 minutes while "waiting" for both, so the operator watched an idle
machine and reasonably suspected the fuzz burst wasn't running (filed #1256;
the zero-CPU window postdated completion).

**Why:** long sleeps based on stale duration expectations waste wall clock and
manufacture misleading idle windows the operator then has to investigate.

**How to apply:** poll background gates at 60-90s intervals, or just run them
foreground with a 5-minute timeout — they fit. Related:
[[heavy-gates-serial-on-epi]] still applies to CONCURRENCY, not duration.

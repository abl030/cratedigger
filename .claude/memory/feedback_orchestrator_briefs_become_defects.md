---
name: orchestrator-briefs-become-defects
description: Implementers treat an orchestrator's brief as authoritative, so a wrong detail in the brief ships as a confident implementation — verify facts before asserting them
metadata:
  type: feedback
---

When orchestrating, a factual claim in a brief is not a suggestion — the
implementer builds it, defends it in review, and writes tests that pin it. A
wrong detail therefore becomes a defect with a confident implementation and a
green suite behind it.

**Why:** 2026-08-12, the six-PR batch (#1081/#1078/#1085/#1086/#1083). Three
instances in one run:

- The #1078 issue body I wrote asserted "`SERVICE_UNITS` already splits exactly
  the way this needs — no new grouping is required". False: it groups a
  `Type=simple` always-on daemon with three self-terminating oneshots. Built
  verbatim, it made `acquire` wait 7200s then fail — the exact deadlock the
  issue existed to remove.
- My MUST FIX said move YouTube "into the post-hold drain **with the controlled
  workers**", implying a narrow set. Result: `acquire` reached HELD having never
  re-read `cratedigger.service`, making its HELD weaker than the one
  `verify_held` trusts, with the migration running in that gap.
- I relayed a reviewer's "18×120s" cost figure to an implementer without
  checking it, twice. The real number was ≈84×120s. Three attempts, three wrong
  numbers in one comment, in a repo whose recent commit `16c5e7ad` was literally
  "stop the docstrings over-claiming".

**How to apply:**
- Verify a fact before putting it in a brief, or mark it explicitly as "check
  this" rather than stating it.
- Never relay a number, line count, or measurement you did not verify. Say
  "the reviewer measured X — confirm it yourself" or omit it.
- When a review finding and your own brief disagree, suspect the brief.
- Own it in the correction message. Implementers calibrate on whether the
  orchestrator's claims are load-bearing; saying "this was my error" keeps them
  willing to push back, which is where the next catch comes from.

Related: [[review-loop-at-orchestrator]], [[pin-and-fuzz-pair]].

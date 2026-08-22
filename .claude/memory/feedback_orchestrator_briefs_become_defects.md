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

Recurred 2026-08-22 in the #1211 series (PRs #1242/#1243/#1244/#1245), three
more instances in one run. Two shipped and were caught only by independent
review; the third never shipped, because the implementer verified the count
before writing it -- which is the mitigation below working, not another
failure of it:

- Claimed the 71 `db=... # type: ignore[arg-type]` findings are the shape
  `finalize_claimed_dispatch`/`make_ctx_with_fake_db` "exist to replace".
  Zero of them sit at either bridge's target. The implementer wrote it into
  an always-loaded rule file, and the reviewer reproduced the dead end live.
  Root cause: I conflated "matches the `db=` ignore shape" (a count I did
  verify) with "absorbable by those bridges" (a causal claim I did not).
- Cited `bc = BASELINE.get(rel, {})` as the ratchet's equality check. No
  identifier `BASELINE` exists, and that line is not the check -- a paraphrase
  typed from memory rather than read from the file.
- Told an implementer to write "the other seven sites reference it by name".
  The real count was five. The implementer grepped, caught it, and said so.

The generalisation worth keeping: **a brief is an unreviewed claim surface.**
Every other artifact in the pipeline gets an independent read; the brief never
does, and the implementer treats it as settled fact. Filed as item 3 of #1246.

**How to apply:**
- Verify a fact before putting it in a brief, or mark it explicitly as "check
  this" rather than stating it.
- Separate what you measured from what you inferred. The #1211 failures were
  all inference wearing a measurement's clothes -- a real count with a false
  causal claim attached, a real mechanism with an invented symbol name.
- Never relay a number, line count, or measurement you did not verify. Say
  "the reviewer measured X — confirm it yourself" or omit it.
- When a review finding and your own brief disagree, suspect the brief.
- Own it in the correction message. Implementers calibrate on whether the
  orchestrator's claims are load-bearing; saying "this was my error" keeps them
  willing to push back, which is where the next catch comes from.

Related: [[review-loop-at-orchestrator]], [[pin-and-fuzz-pair]].

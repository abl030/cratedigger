---
name: feedback-fable-review-quality-core
description: Operator wants fable-tier review + merge held for approval on PRs touching the critical quality-decision surface
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 79896fa5-25d5-4857-904c-185bcb8da540
  modified: 2026-07-22T11:43:23.273Z
---

2026-07-22, during the #813 series: the operator overrode the default opus-reviewer rule for PR #827 — "i think a fable reviewer for this one... its the critical quality part of the code. dont merge this automatically."

**Why:** The quality-decision surface (lib/quality/compare.py, pipeline.py, decisions.py) is the highest-stakes code in cratedigger; the fable review indeed found 2 blocking regressions that a sonnet implementer's full gate run (pyright 0, suite green, 20k fuzz) had passed. Opus remains the default reviewer tier for everything else ([[feedback-opus-for-reviews]]).

**How to apply:** For future PRs that materially change quality-decision logic (not mere display/tooling around it): recommend a fable reviewer, and hold the merge for explicit operator approval with a decision package (implementer report + review verdict + live flip-surface characterization). Don't extend this to routine PRs — it was scoped to the quality core.

---
name: feedback-blind-fable-bughunt
description: "When you think you've found a bug, hand it to a fresh fable subagent blind (no hypothesis) to find independently"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 14a82ccb-8561-4527-8fab-6175f28bf954
  modified: 2026-07-21T13:53:16.297Z
---

When you think you've found the root cause of a bug, do NOT trust your own
hypothesis — hand the bug to a fresh **fable** subagent with **minimal
context**, give it the SYMPTOM only, **withhold your hypothesis entirely**, and
ask it to find the mechanism independently.

**Why:** anti-confirmation-bias. If a blind fresh agent independently lands on
the same root cause, you have real confidence; if it finds something else, even
better. Your own investigation is prone to steering toward the theory you
already formed.

**How to apply:**
- Give the subagent: the observable symptom, the identifiers/data to reproduce
  it, and access instructions (DB query pattern, repo path, how to run the
  code). NOT: your theory, your suspected file/function, or leading facts you
  derived (e.g. "the analyzer is deterministic" — let it re-derive that).
- Model: the user first asked for **fable** here, but **fable ran out of usage
  credits** (2026-07-21, the トクマルシューゴ EXIT spectral discrepancy) and told
  me to **cancel it and use opus**. So default the blind-bughunt agent to
  **opus** when fable credits are exhausted. This is distinct from code review:
  [[feedback-opus-for-reviews]] already runs diff reviews on opus.
- The user also noted **fuzzing helps here** — pair the blind hunt with the
  generated-first house method: give the agent a method hint ("build a
  Hypothesis harness over the real code", `docs/generated-testing.md`) but still
  NOT the hypothesis. See [[project-548-generated-testing]] (generated-first is
  the house bug-hunting method).

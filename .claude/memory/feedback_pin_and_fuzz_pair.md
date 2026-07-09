---
name: feedback-pin-and-fuzz-pair
description: Every invariant ships as a PAIR (deterministic pin + generated property) in one PR; never offer a deterministic-only alternative in subagent briefs
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 0b22cb5a-2427-470e-bf56-fe9d619ef487
---

Every named invariant ships as a **PAIR** in the same PR: one deterministic pin test AND one Hypothesis generated property (plus known-bad self-tests). Operator's framing: defining an invariant and only pinning it "is like getting 90% of the way through a race and then sitting down."

**Why:** PR #560 shipped the #550-phase-2 isolation invariant with a deterministic pin only. Root cause: the old `/refactor` skill carried its own pre-#548 test vocabulary (a parallel process doctrine that never mentioned fuzzing), and my implementer brief inherited that framing by offering an "or a focused deterministic test" alternative. PR #561 retrofitted the property — and its single-point mutant (revert `attempt_fingerprint` to `""`) proved the property was the load-bearing half. PR #562 codified the pair rule and rewrote the skill (test doctrine sourced from code-quality.md only; Opus adversarial reviews default, codex optional).

**How to apply:** (1) Subagent implementation briefs state the PAIR requirement verbatim — never offer a deterministic-only fallback. (2) Process docs/skills must not carry their own test vocabulary — link to `.claude/rules/code-quality.md` § Red/Green TDD as the single source. (3) After the fix, mutation-qualify: revert the fix's single point and show the generated property kills it. See [[project-548-generated-testing]].

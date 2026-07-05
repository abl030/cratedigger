---
name: feedback_review_loop_at_orchestrator
description: "In multi-agent refactor workflows, the review-until-clean loop belongs to the orchestrator, not the sub-agent"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: ca46505c-0e1c-4544-9921-b676ae760375
---

When orchestrating implementer sub-agents, keep the "review → fix → re-review until a pass is clean" iteration at the ORCHESTRATOR level. Give each implementer sub ONE self-review instruction (its single built-in pre-commit review): run it, fix findings, report. Do NOT tell the sub to "iterate/re-review until clean" — it will spawn nested review-of-review agents and keep spawning even after a pass already came back clean (observed on the #468 agent, 2026-07-04: it launched a second reviewer on its own diff after the first was clean).

**Why:** The sub can't tell when to stop; the orchestrator can. The gate is: implementer self-reviews once → orchestrator runs an independent review → if findings, sub fixes → orchestrator re-reviews the fix delta → repeat until an orchestrator pass is clean → merge. The value of the loop (it caught 3 real bugs on #466/PR #503 that the built-in review missed) is real, but the loop is the orchestrator's to run.

**How to apply:** In sub-agent prompts write "run your built-in Opus pre-commit review, fix everything it finds, then report" — never "iterate until clean." Reviews run on opus, implementer main-loops on sonnet ([[feedback_opus_for_reviews]]). Don't needlessly double-review when a pass is clean ([[feedback_opus_for_reviews]]).

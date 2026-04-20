---
name: TDD feedback
description: User requires strict TDD — tests first, then implementation, verify at each step
type: feedback
---

Always use green/green or red/green TDD. Write tests FIRST that capture the expected behavior, then implement to make them pass.

**Why:** User explicitly requested "remember to do TDD at all times to catch regressions. write your tests first." Multiple times across sessions.
**How to apply:** For every new dataclass, function, or refactoring step: (1) write failing tests, (2) implement, (3) verify full suite passes before moving on. Never skip the test-first step.

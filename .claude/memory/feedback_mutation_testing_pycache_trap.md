---
name: mutation-testing-pycache-trap
description: Same-second mutate/test/revert cycles defeat Python's mtime-based __pycache__ invalidation and produce FALSE "mutant survived" results — always use PYTHONDONTWRITEBYTECODE=1
metadata:
  type: feedback
---

Planted-mutant evidence is the house standard for accepting a regression pin
(`code-quality.md`). It has a trap that silently inverts the result.

**Python's `__pycache__` invalidation is mtime-based at one-second granularity.**
A rapid mutate -> test -> revert cycle completing inside the same wall-clock second
can leave stale bytecode in place, so the test runs against the *unmutated* code
and reports **GREEN — mutant survived**. That is a false negative in the most
damaging direction: it looks like a missing test, so a reviewer demands a pin that
already exists, or an author "fixes" a non-problem.

Bit at least three agents during the 2026-08-20 #1214 series. One implementer
reported two false survivors on its first pass and caught them only by re-running
more slowly.

**How to apply.** Every mutation run:

```
PYTHONDONTWRITEBYTECODE=1   # or python3 -B
```

plus clear `__pycache__` between cycles. Brief it explicitly to any agent doing
mutation work, and treat an unexpected survivor as suspect until re-run this way.

Two related traps from the same series:

- **`git archive HEAD | tar -x` produces a non-repo**, and ~11 tests in this suite
  need a real repository — they fail at baseline and invalidate the whole mutant
  harness. `git init` the extract and re-baseline to green **before** planting
  anything, and include a no-op control mutant to prove the harness works.
- **Writing a file into a directory bumps that directory's mtime** (new dirent).
  This silently defeated three "stale mtime" fixtures once a reaper started
  walking recursively — the fixture's own setup refreshed what it was pretending
  was stale.

See [[correction-rounds-mint-false-claims]] and [[1214-daily-gate-enospc]].

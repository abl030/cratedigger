---
name: feedback-ride-ruff-defaults
description: Keep Ruff's wide default rule set; when an upstream Ruff bump flags code, fix the code, never loosen ruff.toml
metadata:
  type: feedback
---

ruff.toml rides Ruff's ~420-rule default set (SIM, RUF, PERF, PL*, UP, ...) via `extend-select`. The operator adopted it deliberately at the big Ruff upgrade and wants to "ride the Ruff best practice wave". When a nixpkgs bump tightens a rule and the daily unstable gate rejects the lock (e.g. #1447, SIM102 in 0.17.4), fix the flagged sites.

**Why:** Operator chose stricter lint for code quality and accepts occasional drift-fix work (stated 2026-11-09).
**How to apply:** Never propose an explicit `select`, pinning Ruff, or a repo-wide ignore as the "meta" fix for lint drift. A narrow `# noqa` with a reason is fine only if the original structure is deliberate.

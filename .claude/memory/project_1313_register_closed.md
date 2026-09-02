---
name: 1313-register-closed
description: "#1313 architecture register CLOSED 2026-09-02 — 22 PRs live-verified; triage-then-fan-out residual pattern; operator questions on ratchet convergence + log-only pins outstanding"
metadata:
  type: project
---

Issue #1313 (2026-09-01 architecture register, successor to [[1278-register-closed]]) closed 2026-09-02 with everything shipped and live-verified: 15 register-item PRs (#1314 JobLane, #1316 WebRuntime, #1319 JS harness, #1323 HAVE-evidence, #1324 context split, #1325 fake package split, #1326 __test__ bags, #1327 flake fix, #1329 selection table, #1330 import_one adapter, #1331 quality trim, #1332 tag-sync, #1333 phase parsers, #1334 preview_snapshot, #1336 AttemptTally) plus 7 residual-sweep batch PRs (#1337-#1339, #1341, #1343-#1345). Deployed as cratedigger b54a8b30 / nixosconfig df79ee23, cycle invocation b3382b81 verified. Register-sized leftovers: #1346 (JS suite depth), #1347 (preview_ mirror columns), #1348 (cycle counters); mutmut residuals consolidated on #1321.

**Why:** the orchestration pattern that worked: hands-off one-item briefs to opus agents in harness-created worktrees (isolation: "worktree"; EnterWorktree fails from subagents), two at a time with disjoint file territories, then a 73-residual sweep run as triage-first (one agent classifies and batches) followed by paired batch implementation — one agent doing the whole sweep was correctly judged too big. Recurring round defect: nearly every agent shipped false claims caught only by the next independent read, exactly per [[correction-rounds-mint-false-claims]].

**How to apply:** two operator questions remain OPEN (quoted verbatim in #1313's closing disposition comment): (1) does the typing ratchet's "ten below baseline" convergence rule apply to pure decreases, and (2) when does a log-only branch earn a pin (49/60 dismissed #1321 survivors are logging-only). Answer them in .claude/rules/code-quality.md before the next register. Also: mutmut 3.7.0 silently skips decorated definitions (~9% of production functions, docs/mutation-testing.md) — relevant to every #1321 batch.

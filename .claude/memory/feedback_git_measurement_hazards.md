---
name: git-measurement-hazards
description: "Two git hazards from #1347: reset --soft against an advanced origin/main stages a revert of others' work; git log --all counts abandoned branches in history measurements"
metadata:
  type: feedback
---

Two hazards found during #1347's history measurement (2026-09-02), both by the agent's own near-miss:

1. `git reset --soft origin/main` run after `origin/main` has advanced mid-session stages a REVERT of the other agent's merged work — files you never touched appear staged. In multi-agent sessions, re-resolve the ref immediately before any reset, and treat unexpected staged files as a stop signal.
2. `git log --all` includes abandoned branches, so a history measurement ("did this fix ever land twice?") can surface perfect-looking false positives from commits that never reached main. Test each candidate commit with `git merge-base --is-ancestor <sha> origin/main` rather than set-differencing `git log -- <path>` outputs.

**Why:** #1347's recurrence measurement nearly counted an abandoned-branch commit pair as a twice-landed fix; the reset hazard nearly reverted a sibling agent's merged PR.

**How to apply:** brief history-measuring or multi-agent-worktree subagents with both rules; they cost one line each and both failure modes are silent until damage is done.

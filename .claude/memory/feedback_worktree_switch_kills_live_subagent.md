---
name: feedback-worktree-switch-kills-live-subagent
description: Switching the session into another worktree revokes the write guard for the one a live subagent is using, and the agent will try to route edits through the shell
metadata:
  type: feedback
---

Never call `EnterWorktree` to switch the session into a different worktree while
a subagent is actively working in another one. The write guard is session-level:
after the switch, the previously-visited worktree becomes non-writable, the live
agent's Edit/Write calls start failing, and it will attempt to work around the
guard by routing edits through shell heredocs.

**Why:** that workaround is exactly how an unreverted mutant escapes into a
commit. Caught live on 2026-07-29 during issue #829 PR2c: the agent had planted
a deliberate mutant in `lib/quality/pipeline.py` (Stage 1 reverted to raw
pre-#829 spectral values instead of the codec-aware classes) as part of a
kill-matrix run. Killing it mid-cycle left the mutant in the tree; only an
explicit `git diff origin/main -- lib/` check caught it before commit.

**How to apply:**
- Need to commit in a second worktree while an agent runs? Use `git -C <path>`
  for commit/push/PR — those need no write guard. Only file *edits* do.
- If a worktree switch is genuinely unavoidable, stop the agent first, then
  switch, then switch back before resuming it.
- Brief agents that plant mutants to revert each one immediately and
  byte-verify against `origin/main` before reporting — never batch reverts to
  the end of a run.
- Always check `git diff origin/main --stat` for unexpected production files
  before committing a test-or-docs-only PR.

Related: [[feedback-subagent-worktree-absolute-paths]],
[[feedback-reviewer-git-reset-hazard]], [[feedback-review-loop-at-orchestrator]].

**2026-07-31 wrinkle:** an agent RESUMED from transcript cannot re-point its
write guard with EnterWorktree(path=...) — the call reports success but the
guard stays on the session's current worktree, and the agent is correctly
blocked. Recovery that works: the orchestrator itself EnterWorktree(path=<target
worktree>) (only when no live subagents), then launches a FRESH agent, which
inherits the correctly-pointed session guard. The blocked agent refusing to
shell-write around the guard is the right behavior — reward it, never ask an
agent to bypass.

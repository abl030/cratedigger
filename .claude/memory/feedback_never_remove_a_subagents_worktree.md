---
name: feedback_never_remove_a_subagents_worktree
description: A background subagent holds no live process between turns — an empty, process-free worktree is NOT evidence the agent is dead
metadata:
  type: feedback
---

Never delete a subagent's isolation worktree to "clean up", and never infer a
subagent is dead from OS evidence. A background agent between turns has **no
running process and no open file handle**, so `ps`, `pgrep`, and a scan of
`/proc/*/cwd` all come back empty for a perfectly healthy agent. A clean
`git status` in its worktree means only that it is still in its research phase.

**Why:** on issue #1093 the host restarted mid-run. Both worktrees were clean
at `main` with zero commits and no process held either one, so I concluded both
agents had died with the host and removed the worktrees and branches. Both were
alive. They resumed, found their working directories gone, and correctly
refused to shell-write around the revoked guard — each returning a
research-phase report instead of an implementation. The operator had to tell me
"no, your agents are actively working". Cost: two full research phases
(~500k subagent tokens) and a restart of the whole implementation leg.

**How to apply:** the authoritative liveness signal is the harness — the task
notification, or `SendMessage` to the agent's ID. Ask it, don't autopsy it. If a
worktree looks abandoned, leave it: a stale empty worktree costs nothing, and
the harness auto-cleans unchanged ones. Only remove a worktree after that
agent's completion notification has actually arrived. Related:
[[feedback_worktree_switch_kills_live_subagent]] (the same guard revoked from
the other direction) and [[feedback_agent_offscript_is_not_malfunction]] (ask
before undoing an artifact you did not create).

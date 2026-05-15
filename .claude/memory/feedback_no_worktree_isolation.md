---
name: no-worktree-isolation
description: User prohibits worktree isolation for sub-agents in cratedigger; agents work in the repo serially
metadata: 
  node_type: memory
  type: feedback
  originSessionId: fb515e37-9250-4130-87b6-19dae079ad4a
---

Do not use `isolation: "worktree"` when dispatching sub-agents for cratedigger work. The user considers it an anti-pattern that doesn't work in practice.

**Why:** User-stated preference, asserted during ce-work execution for the preview-never-decides refactor (2026-05-16). They consider worktree isolation broken in the harness/repo combination — likely a combination of slow path activations, broken Nix dev shells across worktrees, virtiofs/shared-storage assumptions, or git-internals overhead that doesn't survive the cratedigger ce-work flow.

**How to apply:** When ce-work would otherwise route to "Parallel subagents with worktree isolation," instead use **serial sub-agents in the shared working directory**. Dispatch one sub-agent at a time, review the diff, run tests, commit, then dispatch the next. Worktree isolation is OFF the menu even when the parallel-safety check passes — fall back to serial regardless.

Related: [[user_profile]] (the user is a senior dev who has tried both modes and judged worktree isolation worse).

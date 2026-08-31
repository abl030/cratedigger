---
name: worktree-isolated-git-boundaries
description: A worktree-isolated session (and its subagents) cannot run git against sibling worktrees, the shared checkout, or other repos — plan reviews and deploys around it
metadata:
  type: feedback
---

The harness refuses any git command from a worktree-isolated session that targets outside its own worktree — `git -C <elsewhere>`, compound commands it can't verify, and other repos (`~/nixosconfig`). Subagents inherit the pin: a mutant runner assigned a sibling worktree could edit files there by absolute path but not run `git status/checkout` in it. After ExitWorktree, a background session's file-edit tools are blocked EVERYWHERE (even the out-of-repo memory dir) until EnterWorktree again — shell heredocs still work for out-of-repo files.

**Why:** discovered on #1277/#1279 — the mutant runner had to prove restoration by `cp -a` snapshot + sha256 against `git show <commit>:<path>`; the deploy runbook was blocked entirely until ExitWorktree.

**How to apply:** (1) Give a mutant-runner subagent either the session's own committed-clean worktree (when no other agent is concurrently editing) or brief it up front to use snapshot+content-hash restoration proof. (2) Run the deploy skill from the shared checkout — ExitWorktree (remove, after verifying the branch is pushed+merged) before starting the runbook. (3) Reader subagents should write probe scripts to /tmp or the job tmp dir, never bare filenames (LSP picks up strays as diagnostics noise).

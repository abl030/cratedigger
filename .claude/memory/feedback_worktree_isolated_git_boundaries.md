---
name: worktree-isolated-git-boundaries
description: A worktree-isolated session (and its subagents) cannot run git against sibling worktrees, the shared checkout, or other repos — plan reviews and deploys around it
metadata:
  type: feedback
---

The harness refuses any git command from a worktree-isolated session that targets outside its own worktree — `git -C <elsewhere>`, compound commands it can't verify, and other repos (`~/nixosconfig`). Subagents inherit the pin: a mutant runner assigned a sibling worktree could edit files there by absolute path but not run `git status/checkout` in it. After ExitWorktree, a background session's file-edit tools are blocked EVERYWHERE (even the out-of-repo memory dir) until EnterWorktree again — shell heredocs still work for out-of-repo files.

**Why:** discovered on #1277/#1279 — the mutant runner had to prove restoration by `cp -a` snapshot + sha256 against `git show <commit>:<path>`; the deploy runbook was blocked entirely until ExitWorktree.

**How to apply:** (1) A mutant-runner subagent works on a `git archive <commit> | tar -x -C "$CLAUDE_JOB_DIR/tmp/<agent-name>/..."` snapshot plus a `-pristine` twin, never a live worktree, and proves each restore with `diff -rq` (the `orchestrate-issue` "Running agents" rules, since #1394). (2) Run `scripts/deploy.sh` from the shared checkout — ExitWorktree (remove, after verifying the branch is pushed+merged) first, because it drives git in `~/nixosconfig`. (3) Reader subagents write probe scripts under their own job tmp subdirectory, never `/tmp` and never bare filenames (LSP picks up strays as diagnostics noise). (4) Confirmed on #1278 items 2+3: a review subagent that runs `git worktree add` succeeds at CREATING the tree but cannot `cd`/`git -C` into it, so its edits silently land in the spawning session's worktree — one runner's first mutant landed in the implementer's live tree this way; the snapshot brief in (1) is what works, verified with `pwd` plus checking where the first edit actually landed before running anything destructive. (Since 2026-09-09, `Agent(isolation: "worktree")` measurably gives a subagent its own worktree, see [[review-mutants-target-the-changed-expressions]]; the snapshot rule stays because it needs no such guarantee.)

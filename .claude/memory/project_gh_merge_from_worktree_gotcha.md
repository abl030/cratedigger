---
name: project-gh-merge-from-worktree-gotcha
description: gh pr merge --delete-branch exits 1 from a .claude/worktrees worktree but the remote merge still succeeds
metadata: 
  node_type: memory
  type: project
  originSessionId: 3ab3d13f-0f47-49fb-a70a-cab192c3145b
---

`gh pr merge <n> --merge --delete-branch` run from inside a `.claude/worktrees/*`
worktree exits 1 with `fatal: 'main' is already used by worktree at
/home/abl030/cratedigger` — but this is only the LOCAL post-merge step (gh trying
to check out `main` to fast-forward it, which fails because the primary worktree
holds `main`). **The remote merge and branch-delete still happen.**

**Why:** background jobs always work in a worktree; the primary checkout keeps
`main` checked out, so gh can't switch the worktree to `main` after merging.

**How to apply:** don't treat the exit-1 as a merge failure. Verify the real
outcome via the API: `gh pr view <n> --json state,mergedAt,mergeCommit` (expect
`state: MERGED`) and `gh issue view <n> --json state` (auto-closed via "Closes
#n"). Then clean up the remote branch if it lingered: `git ls-remote --heads
origin <branch>` → `git push origin --delete <branch>`. Related:
[[project_repo_walkers_exclude_claude_worktrees]].

---
name: feedback-subagent-worktree-absolute-paths
description: isolation:"worktree" does not stop a subagent editing the shared checkout via absolute paths — every brief must name the worktree and forbid the shared path
metadata:
  type: feedback
---

Spawning a subagent with `isolation: "worktree"` sets its cwd to an isolated
worktree but does NOT prevent it writing to `/home/abl030/cratedigger` if it
constructs absolute paths. On 2026-07-27 the #829 Phase 5 PR1 implementer did
exactly that — its edits landed in the shared checkout, and it only noticed
mid-task, then copied the files into the worktree and restored the shared tree.

**Why:** the operator's live working copy sits at `/home/abl030/cratedigger`
on `main` and may hold uncommitted work. A subagent writing or restoring there
can destroy it, and "I restored it byte-identical" is only true for the files
it knew it had touched.

**How to apply:** every implementer and reviewer brief states, verbatim:
- the exact worktree path to work in, and that it is the ONLY writable tree;
- never touch `/home/abl030/cratedigger` (the operator's live checkout);
- never run `git reset` / `git checkout -- .` / `git stash` / `git clean`
  anywhere;
- never touch `/mnt/virtio/Music/calibration-tmp/` (live calibration data).

After any subagent reports, the orchestrator verifies the shared checkout
independently before reviewing the work: `git status --porcelain` (expect
empty), the branch, `git stash list` unchanged, and `git reflog` free of
resets.

Related: [[feedback-reviewer-git-reset-hazard]], [[feedback-review-loop-at-orchestrator]]

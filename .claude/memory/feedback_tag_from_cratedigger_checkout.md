---
name: tag-from-cratedigger-checkout
description: Deploy tags must be cut from a cratedigger checkout — never in the same command chain as nixosconfig cleanup
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 2c7df303-3035-430d-9e3a-df8c2e5bd67b
---

Three times in one session (2026-07-10) the deploy tag failed with `fatal: bad object type` / `Failed to resolve ref` because the command chain started with `cd ~/nixosconfig && git worktree remove ...` and the `git tag` at the end ran inside nixosconfig, where the cratedigger commit doesn't exist.

**Why:** `cd` persists for the rest of a compound Bash command; the tag targets a cratedigger SHA.

**How to apply:** run the nixosconfig deploy-worktree cleanup and the cratedigger `git tag`/`git push origin <tag>` as SEPARATE Bash calls, with the tag call issued from the cratedigger worktree (the shell resets cwd there between calls). Related: [[deploy-via-master-worktree]].

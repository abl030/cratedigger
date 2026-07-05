---
name: feedback_reviewer_git_reset_hazard
description: Review/verify subagents must stay in the PR worktree — a reviewer's git reset --hard in the shared checkout wiped uncommitted work
metadata:
  type: feedback
---

When orchestrating parallel worktree-isolated agents, review/verify subagents that run git ops (`git checkout <sha>`, `git reset --hard`) in the SHARED main checkout (`/home/abl030/cratedigger`) can silently destroy the operator's uncommitted working-tree edits. During the 2026-07-04 refactor batch (#479/#481/#495/#496/#497), a PR #518 reviewer ran `git reset --hard` in the main checkout instead of its worktree and wiped an uncommitted `MEMORY.md` edit plus a linter's uncommitted `lib/quality.py`→`lib/quality/pipeline.py` doc-path updates in the always-loaded rule files — unrecoverable, because unstaged changes never enter git objects.

**Why:** a reviewer only needs read access to an already-pushed PR diff; it has no reason to mutate the shared checkout, but left unconstrained it defaults to `git checkout`/`git reset --hard` in its cwd, which is the shared repo unless told otherwise.

**How to apply:**
- Every review/verify subagent prompt MUST say: "run ALL git ops inside the PR's own worktree/branch checkout; NEVER `git reset --hard` (or checkout) in the main `/home/abl030/cratedigger` checkout."
- Prefer having reviewers diff via `gh pr diff`/`git show <sha>` (no checkout mutation) over checking out branches locally.
- Related resume pattern: background implementer agents can hit the session limit mid-task (resets at a wall-clock time, e.g. 9pm Australia/Perth). Resume via SendMessage to the agentId — the agent's worktree retains its uncommitted work and its conversation context survives, so it finishes rather than re-doing the carve.

Links [[feedback_review_loop_at_orchestrator]] [[feedback_opus_for_reviews]].

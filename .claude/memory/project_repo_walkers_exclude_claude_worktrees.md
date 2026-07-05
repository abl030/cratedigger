---
name: repo-walkers-exclude-claude-worktrees
description: Any repo-root tree-walking check must exclude .claude/worktrees or it fails/slows in the shared checkout
metadata: 
  node_type: memory
  type: project
  originSessionId: 03dd5709-5c48-45aa-8e20-26fc4f8cf2c0
---

The shared cratedigger checkout at `/home/abl030/cratedigger` accumulates
agent git worktrees under `.claude/worktrees/` — nested checkouts of the repo
(≈34 dirs / ~11k `.py` files at times). Any check that walks the whole repo
from `REPO_ROOT` and does NOT prune `.claude` will descend into every stale
worktree.

**Why:** each worktree is a full copy of the tree, so a REPO_ROOT walker sees
duplicate copies of every file at `.claude/worktrees/<name>/...`. Consequences
seen: pyright turned into a multi-minute crawl (#520); `tests/test_beets_album_op.py`
(`beet`-argv allowlist audit) FAILED because worktree copies of allowlisted
files have worktree-relative paths not in `ALLOWED_FILES` (#543); the
`is`-on-enum lint silently scanned thousands of worktree files every run.

**Agents running inside their own worktree are unaffected** — a fresh worktree
checkout has no nested `.claude/worktrees/`. The problem is specific to the
shared checkout root (where the orchestrator runs the full suite).

**Rule when adding/reviewing a repo-root walker:**
- pyright: `.claude/worktrees` is in `pyrightconfig.json` `exclude` (#520).
- `os.walk(REPO_ROOT)` audits: put `.claude` in the dir-basename skip-set.
  Fixed instances: `tests/test_beets_album_op.py::IGNORE_DIRS`,
  `tests/test_lint_no_is_on_enum.py::SKIP_DIRS` (both #543).
- Walkers scoped to specific subdirs (`lib`/`harness`/`scripts`/`web`/`tests`)
  are inherently safe — worktrees live under `.claude/`, never under those.

Surfaced during the #512/#521/#536/#537 cleanup batch (2026-07-05): a
green-together full-suite run in the shared checkout tripped the beet-argv
audit even though all four PRs were clean in fresh worktrees. Related:
[[project-501-510-refactor-batch]].

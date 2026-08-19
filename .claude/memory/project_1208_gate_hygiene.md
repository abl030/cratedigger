---
name: 1208-gate-hygiene
description: "#1208 CLOSED 2026-08-20: pyright blowup = $PWD-planted .nixpkgs-src symlinks (shellHook now root-anchors); explicit pyright exclude lists SUPPRESS the built-in defaults; worktree hygiene = policy line + sweep, no machinery"
metadata:
  type: project
---

2026-08-20: issue #1208 CLOSED — PRs #1212 (headroom test env-floor design) and #1213 (shellHook root-anchor + pyright excludes) merged; items 1+4 declined as machinery by operator ruling (quoted verbatim on the issue).

Durable gotchas:

- **An explicit `exclude` list in a pyright config SUPPRESSES pyright's built-in defaults** (`**/node_modules`, `**/__pycache__`, `**/.*`, `autoExcludeVenv` — applied only when `exclude` is empty). Our configs silently walked `.ruff_cache`'s ~47k entries on every run for months. Both configs now carry `**/.*` + `**/node_modules` explicitly; keep them when editing excludes.
- **The dev shellHook used to plant `.nixpkgs-src`/`.pyright-venv` GC roots at `$PWD`** — subdir shell entries left nixpkgs-source symlinks inside the repo, and pyright analyzed the whole nixpkgs Python corpus (the real cause of the 2026-08-19 gate OOM cascade; the worktree pile was a bystander). Fixed: `nix/shell.nix` anchors via `git rev-parse --show-toplevel`. If a shared-checkout gate ever blows up again, `find . -name '.nixpkgs-src' -not -path './.nixpkgs-src'` first.
- **Worktree hygiene is policy, not machinery** (operator ruling on #1208): CLAUDE.md's standing rule is clean+merged trees sweepable via `git worktree remove` WITHOUT `--force` (the tool refuses dirty trees). The 2026-08-20 one-time authorized sweep removed 55 worktrees.
- Nix `result*` symlinks are excluded from pyright (`**/result`, `**/result-*`) and unanchored in .gitignore; a planted 666M result link reproduced the blowup class before the exclude.

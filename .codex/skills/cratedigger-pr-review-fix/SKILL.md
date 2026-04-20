---
name: cratedigger-pr-review-fix
description: Review, fix, verify, and publish GitHub pull requests for the Cratedigger repository. Use when working in this repo and the user asks to review a PR, inspect the current PR branch, address findings, push the fixes to the PR branch, and leave a concise PR comment summarizing the changes and verification.
---

# Cratedigger PR Review Fix

## Overview

Run the full Cratedigger PR workflow end-to-end: load repo context, inspect the PR with GitHub plus the local checkout, review for bugs and regressions, implement fixes on the PR branch, verify with repo-required checks, push, and comment on the PR.

Prefer GitHub connector data for PR metadata, patch text, and comments. Prefer local `git` and the checked-out repo for file context, edits, tests, and pushing.

## Workflow

### 1. Load repo context first

Always begin with the repo rules before forming conclusions.

1. Run `hostname`.
2. Read `CLAUDE.md`.
3. Read `.claude/rules/code-quality.md`.
4. Read `.claude/rules/scope.md`.
5. Read `.claude/rules/nix-shell.md` for Python or test changes.
6. Read path-scoped rules for any touched files:
   - `.claude/rules/pipeline-db.md` for `lib/pipeline_db.py` or `scripts/pipeline_cli.py`
   - `.claude/rules/harness.md` for `harness/**`, `lib/beets.py`, or `lib/quality.py`
   - `.claude/rules/web.md` for `web/**`
   - `.claude/rules/deploy.md` when deployment or migration work is in scope

Do not review or patch the PR until that context is loaded.

### 2. Resolve PR context

1. Confirm the repo remote with `git remote -v`.
2. Check local branch state with `git status --short --branch`.
3. Resolve the PR from the user request, URL, or current branch.
4. Fetch PR metadata and the patch with the GitHub connector.
5. Read the touched files in full local context, not only the patch hunks.

If the current checkout is not already on the PR branch and the worktree is clean, fetch the branch non-destructively and switch to it. If the worktree is dirty and switching would be risky, use a separate worktree or stop and ask.

### 3. Review like a code reviewer first

Review for:

- correctness bugs
- behavioral regressions
- missing tests
- unfinished wiring
- rule violations from `CLAUDE.md` or `.claude/rules/`

Default standard:

- findings first
- highest severity first
- use exact file and line references
- keep summaries brief

If the user asked for a review-only pass, stop after findings. If the user asked to fix the PR, or the request is ambiguous but action-oriented, continue through implementation.

### 4. Fix on the PR branch

1. Patch only the files needed for the findings.
2. Use `apply_patch` for manual edits.
3. Do not revert unrelated user changes.
4. Keep one logical change per commit.
5. When fixing a bug, also fix the structural cause if the repo rules make that the correct scope.

For Cratedigger specifically:

- use typed dataclasses, not dict bridges
- keep decision logic in `lib/quality.py` when the behavior is a pure decision
- keep pipeline logging complete and typed
- ensure new code is actually wired into production paths

### 5. Verify with repo-required checks

For Python and tests, always use `nix-shell --run`.

Minimum verification after a focused PR fix:

1. run targeted unit tests for the touched behavior
2. run `pyright` on every touched Python file

Use the full suite when the change is broad, cross-cutting, or risky:

```bash
nix-shell --run "bash scripts/run_tests.sh"
```

If you use the full suite, read `/tmp/cratedigger-test-output.txt` instead of rerunning just to inspect output.

Do not claim verification you did not run.

### 6. Push the fix

After verification:

1. inspect `git diff --stat`
2. stage only the intended files
3. commit with a specific non-interactive message
4. push to the PR branch with `git push origin HEAD`

Do not amend unless explicitly requested.

### 7. Comment on the PR

After the push succeeds, add a top-level PR comment with:

- what you fixed
- why it mattered
- what verification you ran
- any residual risk or follow-up

Keep the comment concise. Prefer a short human summary over a changelog.

Comment shape:

```text
Addressed two review issues on this branch:
- <bug or regression fixed>
- <test or wiring gap fixed>

Verification:
- <command>
- <command>

Residual risk:
- <only if needed>
```

Use the GitHub connector comment action. Use `gh` only if the connector cannot perform the needed PR operation.

## Command reminders

Useful local commands:

```bash
hostname
git remote -v
git status --short --branch
git diff --stat origin/main...HEAD
nix-shell --run "python3 -m unittest tests.<module> -v"
nix-shell --run "pyright <files>"
```

Useful GitHub actions:

- fetch PR metadata
- fetch PR patch
- list changed files when patch navigation is noisy
- add a top-level PR comment after push

## Done criteria

This workflow is complete only when all of these are true:

1. `CLAUDE.md` and relevant rules were read first
2. the PR was reviewed against both patch and full-file context
3. actionable findings were fixed on the PR branch
4. verification was run and recorded accurately
5. the branch was pushed
6. a PR comment was posted

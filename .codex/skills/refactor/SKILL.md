---
name: refactor
description: Codex-first Cratedigger refactor workflow. Use when Codex is driving a structural refactor and should clear cheap breadth with narrow, invariant-scoped Codex reviews before spending a Claude convergence round.
---

# Codex Refactor

This is the Codex-local refactor workflow for Cratedigger. Keep the shared
refactor contract, but do not use broad generic Codex branch review as the
first cheap pass.

## Base Contract

1. Read `../../../.claude/commands/refactor.md`.
2. Treat sections `1-6` and `9-12` there as mandatory.
3. Replace sections `7-8` with the review loop below.
4. When committing from Codex, also follow `../../CODEX.md`.
5. Do not edit the shared Claude workflow unless the user asked for a shared
   workflow change.

## Review Loop

### 1. Lock the seam before coding

Before the first edit, write down:

- the invariant in one sentence
- the seam owner
- the surface matrix: every route, CLI, script, job, or helper that must land
  on the seam in this batch
- the stale helpers, raw writes, or fallback branches that must disappear
- the contract tests that will pin the seam

If the invariant or seam owner is fuzzy, stop and scope down. Claude is
expensive; do not spend a Claude round discovering your inventory.

### 2. Use Codex for cheap breadth

Codex is for finding stragglers, unwired callers, missing tests, and obvious
semantic drift one seam at a time. Do not start with a broad no-prompt
`codex exec review --base main` scan just because the CLI makes it easy.

Instead, run `1-3` focused reviews, each on one seam only. Good buckets:

- seam semantics and side effects
- surface-matrix migration and remaining raw callers
- live-shape drift and missing regression tests

Keep each pass on named files and explicit failure modes. Ask for:

- concrete regressions only
- file:line references
- a counterexample or failing execution path
- missing regression tests

Ignore style, naming, and speculative architecture advice.

### 3. Prompt shape

Keep the review prompt explicit and auditable in a file.

Current Codex CLI note: on current builds, `codex exec review` rejects custom
prompts even though `--help` still shows an optional `[PROMPT]`. For focused
seam reviews, use `codex exec` with an explicit adversarial-review prompt.
Keep `codex exec review` only for optional broad no-prompt scans.

```bash
cat >/tmp/codex-review-prompt.txt <<'EOF'
You are an adversarial code reviewer.

Read CLAUDE.md and directly relevant rule files if needed.
Review the current diff, but only for:
- <file 1>
- <file 2>
- <file 3>

What the change claims: <one sentence invariant>.
Out of scope: <what later commits still plan to do>.

Focus on:
- correctness bugs
- behavior drift in the seam
- missing side effects or bookkeeping
- unwired callers or raw bypasses
- live-data or wire-shape edge cases
- missing regression tests

Give only concrete regressions with file:line references and a counterexample.
Ignore style, naming, and broad redesign advice.
Do not edit code.
EOF

# If your current worktree is already on the branch and clean enough to review,
# you can run the prompt there directly. If not, materialize the branch diff in
# a detached scratch worktree so the review sees only the intended batch.
git diff main...HEAD > /tmp/codex-review.patch
git worktree add --detach /tmp/codex-review-main main
git -C /tmp/codex-review-main apply /tmp/codex-review.patch

rm -f /tmp/codex-review.txt
(
  cd /tmp/codex-review-main
  prompt=$(cat /tmp/codex-review-prompt.txt)
  nix-shell --run "codex exec --full-auto -o /tmp/codex-review.txt \"$prompt\""
)
cat /tmp/codex-review.txt
```

For broad refactors, prefer multiple small prompts over one whole-branch
review. Name the seam in the prompt and in the output filename.

### 4. Clear Codex before Claude

Do not spend a Claude partner-review round until all of these are true:

- the surface matrix is complete
- the invariant grep gate is clean
- focused Codex seam reviews are clean or already fixed
- targeted tests pass
- `pyright` is clean on the touched files
- the review target is one exact snapshot, not a dirty worktree mixing
  committed `HEAD` with extra local fixes

Claude should be finding semantic residue, not grep-level misses.

If Claude is unavailable and the user still wants convergence, keep using the
focused Codex loop and say explicitly that the branch lacks a final partner
review verdict.

### 5. Re-review discipline

After a substantive Claude finding:

- fix the issue
- add a pinning test or write the equivalence argument in the review-fix commit
  body
- rerun only the relevant focused Codex review first
- rerun Claude only if the finding changed behavior, ownership, or seam shape

Do not rerun the full cheap-review pack unless the seam ownership changed.

### 6. Stop-and-refactor trigger

If two substantive review rounds keep hitting the same seam:

- stop patching leaves
- rewrite the invariant in one sentence
- centralize ownership or open a follow-up issue
- test the seam directly
- resume reviews only after the seam story is clean

If the remaining dispute is semantics with no concrete counterexample,
document the equivalence and stop.

### 7. Long-running review commands

When running Codex or Claude review commands:

- launch them in their own long-lived PTY session
- use `nix-shell --run` for Codex review
- for focused custom-prompt Codex reviews, use `codex exec` with an explicit
  prompt file; do not rely on `codex exec review` accepting prompts
- freeze the review target before launch; review exactly one filesystem state
- if the branch tip is what you want reviewed, use a clean worktree with no
  extra local edits
- if the local uncommitted fixes are what you want reviewed, materialize that
  exact tree in a detached scratch worktree or temporary review branch first
- do not run Claude or Codex review from a dirty worktree that still exposes
  both committed `HEAD` and newer uncommitted fixes through file reads
- staging alone is not enough when the reviewer can still `sed`/`cat` the dirty
  tree; either clean the tree or review from a scratch copy of the intended
  snapshot
- mixed-state review is pure token waste: the reviewer can burn a full round
  re-reporting `HEAD` findings that are already fixed locally, easily wasting
  another ~100k tokens for no new signal
- if the current worktree is noisy or cannot safely switch branches, materialize
  the branch diff in a detached scratch worktree before running the prompt
- treat quiet review sessions as expensive remote jobs, not interactive REPLs
- do not poll output files or kill a quiet run
- a Claude or Codex review can sit silent for many minutes and then write only
  at the end; silence is normal and not a failure signal
- interrupting a quiet run can throw away roughly a six-figure token bill of
  already-burned context and force you to pay for the whole round again
- unless the user explicitly wants to abandon the review, do not cancel it just
  because it looks idle
- do unrelated local work until the session exits
- read the output once at the end

## Done Criteria

The skill is complete when:

1. the surface matrix existed before the first feature commit
2. cheap focused Codex reviews ran before Claude
3. every real finding got a test or an explicit equivalence argument
4. Claude rounds were spent on semantic issues, not mechanical stragglers
5. the review-fix audit trail still matches the shared refactor workflow

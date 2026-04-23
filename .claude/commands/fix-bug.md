---
name: fix-bug
description: Fix a Cratedigger bug end-to-end with root-cause analysis, structural remediation, RED/GREEN tests, and mandatory partner-engine review so either Claude or Codex can drive the work.
---

# Fix Bug Pipeline

End-to-end bug fix: confirm the bug, find the structural cause, write RED tests, fix the seam, run a self-review gate, run partner-engine review, and present the finished PR for approval.

**Usage**: `/fix-bug <description of the problem>`

The argument `$ARGUMENTS` is the user-visible bug report or broken behavior.

## Roles

- **Primary driver**: whichever engine is executing the workflow right now.
- **Partner reviewer**: the other engine. This review is mandatory.
- **Independent reviewer**: an optional same-engine delegated reviewer if the current platform supports it and the bug looks wider than one leaf site.

## Workflow

### 1. Load repo context first

Read:

1. `CLAUDE.md`
2. `.claude/rules/code-quality.md`
3. `.claude/rules/scope.md`
4. Any path-scoped rules for the touched area

Do not start patching before the rules are loaded.

### 2. Debug and confirm the bug

Investigate the exact code path that produces the bad behavior:

- inspect live data if applicable
- read the source that implements the behavior
- trace the real call path
- identify the failing assumption, function, and line

Write a brief private summary for yourself: what is broken, where it breaks, and why.

### 3. Look for the structural cause

If the driver platform supports delegated review and the bug smells broader than one site, run one fresh reviewer with minimal context:

- tell it which files or functions to inspect
- tell it the symptom, not your diagnosis
- ask it to find other instances of the same pattern and point out refactoring targets

Whether you use a delegated reviewer or not, the goal is the same: fix the structure that allowed the bug, not only the visible output.

Also build a short bug-class surface list:

- every sibling path that asks the same broken question
- every old helper or fallback chain that could still answer it differently
- every ingress contract test that should exist once the fix lands

### 4. Write RED tests first

Add failing tests that lock in:

- the exact scenario that triggered the bug
- every other site that shares the same broken pattern
- the boundary behavior that would have caught the bug earlier, if relevant

Confirm the tests fail before fixing code.

### 5. Fix the bug as a structural change

Implement the smallest refactor that makes the bug class harder to repeat:

- extract shared helpers when duplicated logic caused the drift
- centralize the invariant when sibling paths disagree
- update contracts when frontend or API callers depend on the same shape

Then run:

- focused tests for the touched behavior
- `pyright` on every touched file
- broader verification when the change crosses multiple layers

### 6. Run a same-engine pre-review, then the mandatory partner-engine review

Before the partner engine reviews the branch, force one disjoint pass from the current engine. Use the strongest option the current platform supports:

1. a native branch-review command
2. a fresh same-engine delegated reviewer with minimal context
3. a bug-class grep gate over the old helper names, raw fallback chains, and sibling ingress paths

This does **not** replace the partner review. Its job is to catch obvious stragglers before the other engine spends a round on them.

If the current engine is Codex, use the native review command first:

```bash
rm -f /tmp/codex-self-review.txt
nix-shell --run "codex exec review --base main -o /tmp/codex-self-review.txt"
cat /tmp/codex-self-review.txt
```

Notes:

- In this repo, launch Codex review from `nix-shell --run` so any Python or
  test commands the reviewer executes inherit the dev-shell dependencies
  (`msgspec`, `psycopg2`, `music-tag`, etc.) instead of failing in plain shell.
- Do not pass a positional prompt with `--base`.
- If you are launching this from an outer tool wrapper, run it in its own
  long-lived session and then leave it alone.
- Do **not** poll with `pgrep`, `tail`, repeated `cat`, or status chatter
  while the review is running.
- Do **not** interrupt a quiet review just because the output file is still
  empty. Codex often writes only at the end.
- When the session exits, read `/tmp/codex-self-review.txt` and continue from
  the completed review.

Run the partner review on the branch only after that pre-review and grep gate are clean.

#### If Claude is the primary driver, use Codex as partner reviewer

```bash
rm -f /tmp/codex-review.txt
nix-shell --run "codex exec review --base main -o /tmp/codex-review.txt"
cat /tmp/codex-review.txt
```

Notes:

- In this repo, launch Codex review from `nix-shell --run` so the reviewer can
  execute Python or tests with the same dependency set the branch verification
  uses, instead of plain-shell import failures.
- Do not pass a positional prompt with `--base`.
- If you are launching this from an outer tool wrapper, run it in its own
  long-lived session and then leave it alone.
- Do **not** poll with `pgrep`, `tail`, repeated `cat`, or status chatter
  while the review is running.
- Do **not** interrupt a quiet review just because the output file is still
  empty. Codex often writes only at the end.
- When the session exits, read `/tmp/codex-review.txt` and continue from the
  completed review.

#### If Codex is the primary driver, use Claude as partner reviewer

```bash
rm -f /tmp/claude-review.txt
cat <<'EOF' | claude -p --model opus \
  --permission-mode dontAsk \
  --allowedTools "Bash(git status --short --branch),Bash(git diff main...HEAD),Bash(git show --stat --summary HEAD),Bash(rg *),Bash(sed *),Bash(cat /tmp/*),Bash(nix-shell --run *)" \
  > /tmp/claude-review.txt
You are an adversarial code reviewer.

Read CLAUDE.md and any directly relevant .claude/rules/*.md files if needed.
Review the current branch against main.

Focus on:
- correctness bugs
- missing tests
- type or route-contract drift
- unfinished wiring
- duplicated or diverging invariants

Use file:line references where possible.
Order findings by severity.
Do not edit code.
EOF
cat /tmp/claude-review.txt
```

Use stdin for the prompt. In this environment, the reliable headless shape is
the normal `claude -p` auth path with a narrow allowlist that still includes
`nix-shell --run ...` and `/tmp` reads, so the reviewer can inspect real
verification output without falling out of the repo environment.

Notes:

- keep normal `claude -p` auth resolution; do **not** add `--bare` unless auth
  comes from `ANTHROPIC_API_KEY` or explicit `--settings`
- do **not** use `bypassPermissions` / `--dangerously-skip-permissions` for
  review, because that defeats the constrained allowlist
- if `/tmp/claude-review.txt` stays empty while the process is still running,
  Claude may still be working; it often writes once at completion

Fix every real finding, add the missing regression coverage, and rerun review if the branch changed materially.

### 7. Stop patching leaves after round 2

If two real review rounds keep finding adjacent problems around the same invariant, stop and ask:

> Are these leaves of the same missing abstraction?

Signals:

- round 2 is "apply the same fix to one more site"
- sibling functions disagree on the same exact question
- the second fix partially undoes or relaxes the first
- multiple files answer the same business question in inconsistent ways

When that happens:

1. write down the invariant
2. grep every site that asks it
3. extract one seam that owns it
4. move callers to the seam
5. test the seam directly
6. resume partner review

Hard cap: six real review rounds. After that, document the remaining work as follow-up scope instead of hiding it inside one bug PR.

### 8. Create the PR and present it for approval

When the branch is clean:

1. create the feature branch if you have not already
2. commit the fix with a message that names the bug and the structural remedy
3. push and open a PR
4. present the result to the user with:
   - one-line bug summary
   - what changed structurally
   - what tests were added
   - the partner-engine review verdict
   - the PR URL

Wait for approval before merging unless the user explicitly asked you to continue through merge and deploy.

## Rules

- Do not ask for confirmation between internal steps unless the bug turns out not to be a bug, cannot be reproduced, or requires multi-repo design work.
- The partner-engine review is required, even for "small" fixes.
- Follow `.claude/rules/scope.md`: the refactor that caused the bug is part of the fix.
- Follow `.claude/rules/code-quality.md`: RED/GREEN tests, typed boundaries, `pyright` clean, no parallel code paths, finish what you start.

---
name: fix-bug
description: Fix a Cratedigger bug end-to-end with root-cause analysis, structural remediation, RED/GREEN tests, and mandatory partner-engine review so either Claude or Codex can drive the work.
---

# Fix Bug Pipeline

End-to-end bug fix: confirm the bug, find the structural cause, write RED tests, fix the seam, run partner-engine review, and present the finished PR for approval.

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

### 6. Run the mandatory partner-engine review

Run the partner review on the branch before presenting the fix.

#### If Claude is the primary driver, use Codex as partner reviewer

```bash
rm -f /tmp/codex-review.txt
codex exec review --base main -o /tmp/codex-review.txt
while pgrep -f "codex-raw exec review" > /dev/null; do sleep 30; done
cat /tmp/codex-review.txt
```

Do not pass a positional prompt with `--base`.

#### If Codex is the primary driver, use Claude as partner reviewer

```bash
rm -f /tmp/claude-review.txt
cat <<'EOF' | claude -p --model opus \
  --allowed-tools 'Bash(git status --short --branch)' \
                  'Bash(git diff main...HEAD)' \
                  'Bash(git show --stat --summary HEAD)' \
                  'Bash(rg *)' \
                  'Bash(sed *)' \
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

Use stdin for the prompt. In this environment that is the reliable `claude -p` path when `--allowed-tools` is present.

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

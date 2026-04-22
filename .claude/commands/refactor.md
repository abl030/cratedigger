---
name: refactor
description: Drive a structural refactor in Cratedigger with an explicit invariant, live probes, staged commits, RED/GREEN verification, and mandatory partner-engine review so either Claude or Codex can lead the work.
---

# Refactor Pipeline

Large structural refactor: invariant -> inventory -> live probe -> staged commits -> self-review gate -> partner review -> PR -> merge -> deploy -> reflect.

**Usage**: `/refactor <description of the refactor>`

The argument `$ARGUMENTS` is the user-visible structural change. Scope should be "extract abstraction X from N drifting call sites" or "push contract Y through path Z". If you cannot state the invariant in one sentence, stop and scope down.

## Roles

- **Primary driver**: whichever engine is executing the workflow right now.
- **Partner reviewer**: the other engine. This review is mandatory.
- **Independent reviewer**: an optional same-engine delegated reviewer if the current platform supports it and the task merits it. Use minimal context only. Never replace the partner review with this.

## Workflow

### 1. Load repo context first

Read the project rules before making structural decisions:

1. `CLAUDE.md`
2. `.claude/rules/code-quality.md`
3. `.claude/rules/scope.md`
4. Any path-scoped rules for the touched area

If the refactor touches DB schemas, subprocess output, HTTP payloads, or deployment behavior, read the matching rule files before planning commits.

### 2. Define the invariant

Write down, in one sentence, what the refactor enforces. Examples:

- "Every `beet remove` and `beet move` operation routes through one typed wrapper."
- "Every library row returned to the frontend conforms to one shared contract."
- "Every exact release identity decision goes through one helper."

If you cannot do this cleanly, you are not holding a refactor yet.

### 3. Inventory every site and probe the live shape

Grep every call site, type, route, SQL query, column, or JS consumer that participates in the invariant. The ad hoc count is always wrong.

For wire-boundary, DB-coupled, or subprocess-crossing refactors, probe the real shape before editing:

- table schemas and FK behavior
- representative JSON payloads
- subprocess stdout
- live rows that already exist in production data
- every reader of the fields you plan to reshape

Declared types lie. Production readers lie less.

### 4. Plan 2-5 feature commits and a surface matrix

Before coding, list the surfaces that must end up on the seam in this batch:

- every route, CLI path, job, script, or frontend flow that asks the invariant question
- every old helper, accessor, raw branch, or fallback chain that the seam should replace
- every ingress contract test you need once the seam exists

Partner review should not be the first place you discover an obvious unwired surface. Do this grep-and-list work now.

Each feature commit must:

- stand alone
- keep tests green
- keep `pyright` clean
- deliver one sentence worth of structural value

Do not pre-plan the review-fix commits. They are part of the audit trail.

### 5. Run the per-commit cycle

For each feature commit:

1. Write RED tests first.
2. Implement the structural change.
3. Run focused tests for the touched modules.
4. Run `pyright` on every touched file.
5. Commit with a message that states what structural change this commit delivers.

Every real review finding later in the cycle gets either:

- a pinning regression test, or
- a documented equivalence argument in the review-fix commit

Do not silently drop findings.

### 6. Run a same-engine pre-review before partner review

Before the partner engine reviews the branch, force one disjoint pass from the current engine. Use the strongest option the current platform supports:

1. a native branch-review command
2. a fresh same-engine delegated reviewer with minimal context
3. the invariant grep gate below, if the platform lacks review support

This pre-review does **not** replace the partner review. Its job is to catch obvious stragglers before the other engine spends a round on them.

#### Native same-engine review

If the current engine has a native review command, run it before the partner review.

For Codex, use:

```bash
rm -f /tmp/codex-self-review.txt
codex exec review --base main -o /tmp/codex-self-review.txt
while pgrep -f "codex-raw exec review" > /dev/null; do sleep 30; done
cat /tmp/codex-self-review.txt
```

Notes:

- Do not pass a positional prompt with `--base`.
- Treat every real finding as pre-review work. Do **not** count this as satisfying the partner review.

#### Delegated same-engine review

If there is no native review command but the platform supports delegated review, run one fresh reviewer with minimal context:

- point it at the commit or diff
- describe the invariant and the claimed scope
- ask for correctness bugs, missing tests, unwired paths, stale contracts, and duplicated logic
- do not feed it your conclusions

This pass is for disjoint signal, not agreement.

#### Invariant grep gate

Before partner review, build one grep pack from the invariant and clear the obvious stragglers yourself. Search for:

- old helper names or deprecated module imports being replaced by the seam
- raw source or ID-shape branches (`isdigit`, `detect_release_source`, direct source-string comparisons, hand-rolled fallback chains)
- direct DB, JSON, or route-level field reads the seam should supersede
- frontend state reads or writes that bypass the new helper, key normalizer, or store seam

Every remaining match must be:

- migrated to the seam
- deleted
- or explicitly justified in code or the follow-up note

### 7. Run the mandatory partner-engine review

The partner engine should review the branch only after the same-engine pre-review and grep gate are clean for the current batch. Review after each meaningful commit or review-fix batch, and again once at the end.

#### If Claude is the primary driver, use Codex as partner reviewer

```bash
rm -f /tmp/codex-review.txt
codex exec review --base main -o /tmp/codex-review.txt
while pgrep -f "codex-raw exec review" > /dev/null; do sleep 30; done
cat /tmp/codex-review.txt
```

Notes:

- Do not pass a positional prompt with `--base`.
- Treat every real finding as a fixable issue, not as commentary.
- If Codex is also available as the current engine's native pre-review, run that first and only then ask Claude or Codex to do the partner pass.

#### If Codex is the primary driver, use Claude as partner reviewer

Use stdin for the prompt. In this environment, the reliable headless shape is:

- keep normal `claude -p` auth resolution (do **not** add `--bare` unless auth comes from `ANTHROPIC_API_KEY` or explicit `--settings`)
- add `--permission-mode dontAsk` so an out-of-policy tool request fails instead of hanging on an unseen prompt
- keep the allowlist narrow to the read-only Bash commands needed for review

```bash
rm -f /tmp/claude-review.txt
cat <<'EOF' | claude -p --model opus \
  --permission-mode dontAsk \
  --allowedTools "Bash(git status --short --branch),Bash(git diff main...HEAD),Bash(git show --stat --summary HEAD),Bash(rg *),Bash(sed *)" \
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

Notes:

- `--bare` is great for scripted runs, but it skips stored login/keychain state; in this repo's current setup that can fail with `Not logged in`.
- Do **not** swap in `bypassPermissions` / `--dangerously-skip-permissions` here. Per Claude's docs, bypass mode ignores the allowlist and approves every tool, which defeats the constrained-review setup.
- If the review is still running after several minutes and `/tmp/claude-review.txt` is still empty, that is not by itself a bug — Claude often writes only once the full review completes.
- If Codex is the current engine, the normal sequence is Codex self-review first, then Claude partner review. Do not make Claude spend a round finding grep-level stragglers.

For commit-scoped review, tighten the allowed diff command and prompt to `HEAD~1..HEAD`. For final branch review, keep `main...HEAD`.

### 8. Stop and refactor when reviews keep circling the same seam

If two review rounds on the branch keep finding real issues around the same invariant, do not keep patching leaves.

Do this before the next round:

1. Write down the invariant the reviews keep circling.
2. Grep every site that asks that question.
3. Extract one typed seam that owns the invariant.
4. Migrate callers to the seam.
5. Test around the seam, not around each leaf.
6. Resume reviews.

If you reach six real review rounds on the same branch, stop. Document the remaining issues as follow-up work instead of pretending the scope is still stable.

### 9. Run one final branch-wide pass

After every feature commit and review-fix commit has landed:

- rerun the same-engine pre-review or grep gate on the full branch
- run the mandatory partner-engine review on the full branch
- converge on zero unfixed correctness findings or explicitly documented follow-ups

### 10. Open the PR, merge, deploy, verify

1. Open a PR with the acceptance checklist.
2. Record the review rounds and what each review-fix commit addressed.
3. Merge with rebase.
4. Deploy.
5. Verify the deployed code contains a unique signature from the refactor.

Use the repo's deploy workflow rather than improvising one.

### 11. Reflect

Once the change is live, write a short note for the next refactor:

- what the same-engine reviewer caught that the partner engine did not
- what the partner engine caught that the same-engine reviewer did not
- whether the stop-and-refactor trigger fired
- what grep or surface-matrix item should have happened earlier
- what live probe should have happened earlier

Keep it short and specific.

## Rules

- Run the workflow autonomously. Do not ask for confirmation between internal steps unless the live system contradicts the task.
- The partner-engine review is required. Do not skip it because the diff looks small.
- Minimal context for independent reviewers is a hard rule. Do not preload them with your diagnosis.
- Every structural change must satisfy `.claude/rules/code-quality.md`: RED/GREEN tests, typed boundaries, `pyright` clean, no parallel code paths.
- Rebase merge only. Squash drops the review-fix audit trail that makes this workflow useful.

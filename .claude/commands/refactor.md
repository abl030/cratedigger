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

- Read each hit to confirm it is a real instance, not a lookalike.
- Write down the list before touching code.
- If the grep spans more than three files and more than two call shapes, run
  one minimal-context explorer or delegated search pass to hunt the pattern,
  not to validate your conclusion.

For wire-boundary, DB-coupled, or subprocess-crossing refactors, probe the real shape before editing:

- table schemas and FK behavior
- representative JSON payloads
- subprocess stdout
- live rows that already exist in production data
- every reader of the fields you plan to reshape

Concrete probes that have paid for themselves repeatedly in this repo:

- SQLite column types: `sqlite3 path/to/db ".schema <table>"`
- Postgres column types and constraints: `psql -c "\d+ <table>"`
- FK delete behavior: `grep -n 'ON DELETE' migrations/`
- external API response keys: `curl $URL | jq 'keys'`
- subprocess stdout: run the actual subprocess with representative input and
  capture the real output shape
- JSONB blobs: `SELECT jsonb_pretty(<col>) FROM <table> LIMIT 3`
- production readers of the shape: grep every caller that indexes or
  destructures the fields you are reshaping, for example
  `grep -rn "req\['beets_distance'\]" scripts/ lib/`

Declared types lie. Production readers lie less.

Do not skip this step for refactors that cross a process, DB, or wire boundary.

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

For each feature commit. Reviews are cheap; default to running them, not
skipping them because "the next commit touches the same area anyway."

1. Write RED tests first.
2. Implement the structural change.
3. Run focused tests for the touched modules.
4. Run `pyright` on every touched file.
5. Commit with a message that states what structural change this commit delivers.

For review-fix commits, preserve the review round in the subject instead of
collapsing everything into a generic follow-up. Preferred patterns:

- `review(r3 p2): document equivalence + pin observed prod shapes (codex)`
- `review(r2 adversarial): stale docs + pre-48914ca mbid pin test`
- `review(final holistic): delete dead helper + fix stale refs`

The subject should name the actual finding or invariant closed, not just say
`address review`.

Every real review finding later in the cycle gets either:

- a pinning regression test, or
- a documented equivalence argument in the review-fix commit

Do not silently drop findings. The review-fix body is part of the audit trail
and should be as detailed as needed to explain:

- who found the issue and in which round
- what real breakage, drift, or risk they identified
- why the fix or documented-equivalence argument is correct
- what verification ran after the change

If the right answer is "the stricter contract is already correct for every
observed production shape", say that explicitly in the body instead of adding a
hedge that weakens the invariant.

Keep agent-specific attribution trailers in the engine-local rules rather than
hardcoding them into this shared workflow.

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
nix-shell --run "codex exec review --base main -o /tmp/codex-self-review.txt"
cat /tmp/codex-self-review.txt
```

Notes:

- In this repo, launch Codex review from `nix-shell --run` so any Python or
  test commands the reviewer executes inherit the dev-shell dependencies
  (`msgspec`, `psycopg2`, `music-tag`, etc.) instead of failing in plain shell
  with environment noise.
- Do not pass a positional prompt with `--base`.
- If you are launching this from an outer tool wrapper, run it in its own long-lived session and then leave it alone.
- Do **not** poll with `pgrep`, `tail`, repeated `cat`, or status chatter while the review is running.
- Do **not** interrupt a quiet review just because the output file is still empty. Codex often writes only at the end.
- When the session exits, read `/tmp/codex-self-review.txt` and continue from the completed review.
- Treat every real finding as pre-review work. Do **not** count this as satisfying the partner review.

#### Delegated same-engine review

If there is no native review command, or the change is broad enough that a
second disjoint pass is worth it, run one adversarial same-engine reviewer with
minimal context:

- point it at the commit or diff
- describe the invariant and the claimed scope
- ask for correctness bugs, missing tests, unwired paths, stale contracts, and duplicated logic
- do not feed it your conclusions

This pass is for disjoint signal, not agreement.

When the platform supports it, prefer a concrete adversarial-review prompt over
an informal "take a look". A good generic shape is:

```text
You are an adversarial code reviewer.

Read CLAUDE.md and directly relevant rule files if needed.
Review the current commit or branch diff.

What the change claims: <one sentence>.
Out of scope: <what later commits still plan to do>.

Focus on:
- correctness bugs
- missing tests
- wire-boundary / live-data shape drift
- unfinished wiring
- stale docs or docstrings
- duplicated or diverging invariants

Use file:line references where possible.
Order findings as CRITICAL, HIGH, MEDIUM, LOW, NIT.
Do not edit code.
```

Triage the result explicitly:

- fix every CRITICAL and HIGH before moving on
- fix MEDIUM inline unless there is a clear reason not to
- either fix LOW findings or mention them in the review-fix audit trail
- every real finding gets a pinning regression test or a documented
  equivalence argument

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
nix-shell --run "codex exec review --base main -o /tmp/codex-review.txt"
cat /tmp/codex-review.txt
```

Notes:

- In this repo, launch Codex review from `nix-shell --run` so the reviewer can
  execute Python or tests with the same dependency set the branch verification
  uses, instead of plain-shell import failures.
- Do not pass a positional prompt with `--base`.
- If you are launching this from an outer tool wrapper, run it in its own long-lived session and then leave it alone.
- Do **not** poll with `pgrep`, `tail`, repeated `cat`, or status chatter while the review is running.
- Do **not** interrupt a quiet review just because the output file is still empty. Codex often writes only at the end.
- When the session exits, read `/tmp/codex-review.txt` and continue from the completed review.
- Treat every real finding as a fixable issue, not as commentary.
- If Codex is also available as the current engine's native pre-review, run that first and only then ask Claude or Codex to do the partner pass.

#### If Codex is the primary driver, use Claude as partner reviewer

Use stdin for the prompt. In this environment, the reliable headless shape is:

- keep normal `claude -p` auth resolution (do **not** add `--bare` unless auth comes from `ANTHROPIC_API_KEY` or explicit `--settings`)
- add `--permission-mode dontAsk` so an out-of-policy tool request fails instead of hanging on an unseen prompt
- keep the allowlist narrow, but include `nix-shell --run ...` and `/tmp` reads so the reviewer can inspect real verification output without falling out of the repo environment

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

Notes:

- `--bare` is great for scripted runs, but it skips stored login/keychain state; in this repo's current setup that can fail with `Not logged in`.
- Do **not** swap in `bypassPermissions` / `--dangerously-skip-permissions` here. Per Claude's docs, bypass mode ignores the allowlist and approves every tool, which defeats the constrained-review setup.
- Do **not** give the reviewer edit authority in this pass. If you want the partner engine to patch findings itself, do that as a separate worker step after the review, not inside the review command.
- If you are launching this from an outer tool wrapper, run it in its own long-lived session and then leave it alone.
- Do **not** poll `/tmp/claude-review.txt`, tail logs, or narrate the empty file while the review is running.
- Do **not** interrupt a quiet review just because the output file is still empty. Claude often writes only once the full review completes.
- When the session exits, read `/tmp/claude-review.txt` and continue from the completed review.
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
- run one holistic adversarial review over `git diff main..HEAD` and ask:
  - do the feature commits fit together cleanly
  - is anything still unwired
  - is any helper, config, docstring, or comment now stale
  - does the acceptance checklist still match what shipped
- run the mandatory partner-engine review on the full branch
- converge on zero unfixed correctness findings or explicitly documented follow-ups

If a final branch-wide review round is still finding real bugs at round 6, stop
and document follow-up work instead of pretending the branch is converged.

### 10. Open the PR, merge, deploy, verify

1. Open a PR with the acceptance checklist.
2. Post a PR comment listing every review round, what each review-fix commit
   addressed, and the final convergence verdict for the branch.
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

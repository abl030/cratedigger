# Refactor Pipeline

Large structural refactor: invariant → inventory → probe-live → staged commits with cheap review gates at every step → PR → merge → deploy → reflect.

**Usage**: `/refactor <description of the refactor>`

The argument `$ARGUMENTS` is what the user wants restructured. Scope tends to be "extract abstraction X from N drifting callsites" or "plumb type Y through path Z" — structural, correctness-motivated, multi-commit.

Sibling skills: `/fix-bug` for single-site corrections (same review machinery, narrower scope); `/deploy` for step 7; `.claude/rules/scope.md` for the "clean as you go" philosophy.

## Why this skill exists

PR #136 (BeetsAlbumOp + release lock + sibling propagation) shipped clean after an in-process adversarial reviewer caught architecture issues and Codex caught runtime/type bugs across 5 rounds. The two reviewers find **disjoint classes of bugs** — the agent reasons about "does this look right?"; Codex reasons about "does this actually work against the declared types?". Running both, liberally, at every commit is cheap compared to a bad merge.

## Pipeline

### 1. Define the invariant

Write down, in one sentence, what the refactor enforces. Examples:
- "Every `beet remove`/`beet move` subprocess op routes through one typed wrapper."
- "Every wire-boundary type validates its fields via `msgspec` at the decoder."
- "Every dispatch outcome propagates back to the caller that triggered it."

If you can't write it in one sentence, you don't have a refactor — you have a redesign. Stop and scope down.

### 2. Inventory every site

Grep for every callsite, type, function, or DB column that touches the invariant. The ad-hoc count is always wrong.

- Read each hit to confirm it's really an instance (not a lookalike).
- Write down the list before touching code.
- If the grep spans > 3 files and > 2 call shapes, spawn an Explore agent with **minimal context** — brief it on the pattern to hunt, not your conclusion.

### 3. Probe the live system

**Non-optional for wire-boundary / DB-coupled / subprocess-crossing refactors.** Declared types lie; what ships lies less. Examples of what to probe:

- SQLite column types: `ssh <host> 'sqlite3 path/to/db ".schema <table>"'`
- Postgres column types + constraints: `ssh <host> 'psql -c "\d+ <table>"'`
- Postgres foreign-key behaviour: `grep -n 'ON DELETE' migrations/` — a fake mirroring a delete needs to cascade, and this is often missed because it's not in the column signature.
- External API response shapes: `curl $URL | jq 'keys'`
- Subprocess stdout: run the actual subprocess with representative input, capture the output shape.
- JSONB blobs: query `SELECT jsonb_pretty(<col>) FROM <table> LIMIT 3`.
- **Production readers of the data shape**: `grep` every caller that indexes the columns the refactor produces (e.g. `grep -rn "req\['beets_distance'\]" scripts/ lib/`). Fakes and wire-boundary types that omit a DB-defaulted column will pass every typed-assertion test but raise `KeyError` the moment a real reader exercises the path. Issue #140's partial-row-shape bug (codex's final pass) would have shown up here in 10 seconds.

Every missed wire-boundary bug in PR #136 (the `discogs_albumid INTEGER` one) could have been caught with a 30-second probe here. **Do not skip this step** for any refactor that touches data crossing a process/type boundary.

### 4. Plan the commits

Split into 2-5 feature commits. Each must:
- Stand alone (suite passes, pyright clean, app boots).
- Land on top of a known-safe baseline (main, or the previous feature + its review fixes).
- Carry a 1-sentence "delivers X" summary.

Do NOT plan the review-fix commits — those emerge from the reviews and preserve the audit trail on a rebase merge.

Make a feature branch. Track the commit plan with `TaskCreate`, one task per feature commit + one task per review round.

### 5. Per-commit review cycle

**For each feature commit**, run this sequence. Reviews are cheap — default to running them, not skipping.

#### 5a. Implement + test (TDD per `.claude/rules/code-quality.md`)

- Write RED tests first, then fix.
- `nix-shell --run "python3 -m unittest tests.<module> -v"` for the affected modules.
- `nix-shell --run "pyright <touched files>"` — must be 0 errors.
- Commit with a clear message stating what structural change this delivers.

#### 5b. Adversarial in-process review

Spawn an Opus agent with minimal context:

```
Subagent: general-purpose, model: opus
Prompt: "You are an adversarial code reviewer. Find correctness bugs, test-coverage
gaps, behavior drift. Be harsh. Don't congratulate the author.

Read CLAUDE.md for orientation. The commit to review is HEAD (title: '<commit msg>').
Parent is <sha>.

What the commit claims: <1-sentence summary>. Out of scope: <other feature commits
still to come>.

Investigate:
- git diff HEAD~1
- gh issue view <N>
- Read the new module(s), the migrated callsites, the new tests

Look for: <point at the specific invariant, edge cases, test coverage gaps,
wire-boundary type issues, backwards compat with old JSONB rows, dead parameters
or fields, stale docstrings>. Use file:line citations. Order findings CRITICAL >
HIGH > MEDIUM > LOW > NIT with concrete recommendations. Under 1200 words.
Do NOT fix — only report."
```

- Fix every CRITICAL and HIGH before moving on.
- Fix MEDIUMs inline (they're cheap).
- Note LOWs in the commit message; fix the ones that clarify the next reviewer.
- **Every finding gets a pinning regression test OR documented equivalence** in the review-fix commit. Never silently drop.

Commit fixes as `review(commit N): <short summary>`. Multiple review-fix commits per feature commit are fine.

#### 5c. Codex review

```bash
rm -f /tmp/codex-review.txt
codex exec review --base main -o /tmp/codex-review.txt
```

**Argument parsing quirk** (same as `/fix-bug`): `codex exec review` rejects a positional `[PROMPT]` argument when `--base <BRANCH>` is used. Don't pass a prompt with `--base` — codex picks up project review config automatically.

Wait for the real process exit (not the file becoming non-empty — codex writes partial output mid-run):

```bash
# Monitor until codex-raw process truly exits:
while pgrep -f "codex-raw exec review" > /dev/null; do sleep 30; done
```

Read `/tmp/codex-review.txt`. Expect:
- 0-2 findings per commit, usually P1/P2 correctness.
- Often runtime/type bugs the in-process agent missed (e.g. SQLite column type vs declared dataclass type).

Each finding gets a commit: pinning regression test + fix. Push. Re-run codex. Repeat until clean.

#### 5d. Stop-and-refactor trigger

**If two review rounds across this branch both flag findings on the same invariant or same pair of collaborators** (not style nits — genuine correctness findings), stop patching leaves. This rule is adapted from `/fix-bug`'s round-2 rule but widened to *any* review round (in-process or codex), not just codex-specific.

Signals:
- Round N reveals a case round N-1 didn't cover; fix is "apply the same pattern to one more call site."
- Two sibling functions disagree on the same invariant (ID dispatch, presence check, cleanup contract).
- Your fix in round N contradicts your fix in round N-1 (over-tightened, now relaxing).

When you see them, do this before the next round:

1. **Write down the invariant the reviews keep circling.** One sentence.
2. **Grep every site that asks that question.** There will be more than you found in step 2.
3. **Extract one typed seam** — a function/method/type that owns the invariant. Every site routes through it.
4. **Migrate call sites; test around the seam, not around each migrated site.**
5. **Resume reviews.**

The PR #136 R3 refactor (`DispatchOutcome.deferred` plumbing) is the canonical example: three findings (R2 P1 + R3 P2 + R3 P3) were all leaves of *"contention = deferred retry; leave everything resumable."* Extracting the seam fixed all three AND set up R4 for a clean catch the next round.

Hard cap: **6 review rounds per commit**. At round 6, document remaining findings as follow-up issues and proceed.

### 6. Final review pass

After every feature commit has passed its own cycle:

**6a. Holistic adversarial review.** Spawn an agent with the full branch diff (`git diff main..HEAD`). Ask:
- Do the feature commits fit together?
- Anything unwired (new dataclass constructed but never produced, new config option nobody sets)?
- Dead code or stale docs/comments?
- Does the issue's acceptance checklist actually match what shipped?

**6b. Final codex pass.** Run codex on the final branch. **Convergence = codex returns no findings.** If R6 is still finding real bugs, call it: ship with documented follow-ups, don't loop into round 7.

### 7. Open PR, merge, deploy

- `gh pr create` with acceptance checklist from the issue.
- Post a PR comment listing every review round (in-process + codex), each finding's fix commit sha, and the final codex verdict. This is the audit trail for a rebase-merged refactor; losing it to a squash is the reason squash is disabled.
- `gh pr merge <N> --rebase`.
- Run `/deploy` (skill). Verify deployed code has a unique signature from the refactor (grep `/nix/store/*/lib/*.py` on the target host).

### 8. Reflect

Once deployed and verified live, write a short reflection. Target audience is the next refactor, not the user. Cover:

- **What the in-process agent caught that codex didn't.** Usually architecture, type system, docstring lies.
- **What codex caught that the agent didn't.** Usually runtime, declared-vs-actual type drift, cross-module data shapes.
- **Did stop-and-refactor trigger?** On which invariant? How many rounds did the seam save?
- **What should you probe earlier next time?** (Step 3's miss, almost always.)

One paragraph each, max. Be specific; don't perform.

## Rules

- Run the full pipeline autonomously. Do NOT ask for confirmation between steps.
- Stop and ask if: you can't write the invariant in one sentence, or step 3 reveals the live system contradicts the task description.
- The in-process agent in step 5b must be truly independent — **minimal context, don't lead it to your conclusion**. Specifically don't paste your analysis into the prompt.
- Every review finding gets a pinning test OR documented equivalence in the commit that addresses it. Never silently drop a finding.
- Rebase-merge only (`--rebase`). Squash loses the review-fix audit trail that made this pipeline valuable.
- **Reviews are cheap. Run them.** If you're unsure whether a change is risky, run a codex round — 2 minutes vs the cost of a bad merge. Between commit N and N+1 if tempted to skip review because "the next commit changes the same area anyway" — run it anyway, the findings sharpen the next commit's scope.
- If codex finds a runtime bug that the in-process agent missed (or vice versa), note it in the reflection. Those gaps inform when to spawn which review in future.
- Follow all `.claude/rules/code-quality.md` requirements: typed dataclasses, TDD, no parallel code paths, finish what you start, wire-boundary types use `msgspec.convert` at the decoder.

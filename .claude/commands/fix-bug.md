# Fix Bug Pipeline

End-to-end bug fix: debug, confirm, refactor according to code rules, RED/GREEN TDD, PR, Codex review, present for approval.

**Usage**: `/fix-bug <description of the problem>`

The argument `$ARGUMENTS` is the user's description of the bug or the problematic behavior.

## Pipeline

### 1. Debug and confirm the bug

Investigate the problem described in: `$ARGUMENTS`

- Query live data if applicable (pipeline-cli show/query, journalctl, etc.)
- Read the relevant source code
- Trace the exact code path that produces the wrong behavior
- Identify the root cause — which line, which function, which assumption

Present a brief confirmation to yourself (do not ask the user yet): what the bug is, where it is, and why it happens.

### 2. Structural analysis (fresh sub-agent)

Spawn an Explore sub-agent with **minimal context** — do NOT give it your analysis. Brief it only on:
- Which file(s) and function(s) to audit
- What class of bug to look for (the symptom, not the cause)
- Ask it to find ALL instances of the same pattern and suggest refactoring targets

The sub-agent should read `CLAUDE.md`, `.claude/rules/code-quality.md`, and `.claude/rules/scope.md` to understand the project's refactoring philosophy: "Every bug fix is a refactoring opportunity. If a bug was caused by duplication, inconsistency, or missing abstractions — fix the structure, not just the output."

The sub-agent reports back its independent findings. You synthesize both analyses.

### 3. Write RED tests

Write failing tests that lock in the correct behavior for every bug site identified:
- Use existing test patterns and shared infrastructure (see code-quality.md "New Work Checklist")
- Cover the exact live scenario that triggered the bug
- Cover every other instance of the same pattern the sub-agent found
- Tests must FAIL against the current code — run them and confirm RED

### 4. Fix (GREEN)

Implement the fix as a refactor per scope.md — fix the structure, not just the output:
- Extract shared helpers to eliminate the duplication that caused the bug
- Make sure the same class of bug cannot recur
- Run the new tests and confirm GREEN
- Run `pyright` on all touched files — must be 0 errors
- Run full test suite — must be 0 regressions

### 5. Create PR

- Create a feature branch (e.g. `fix/descriptive-name`)
- Commit with a clear message explaining the bug and the structural fix
- Push and create a PR with summary + test plan

### 6. Codex review

Invoke Codex's built-in code review on the PR branch:

```bash
codex exec review --base main -o /tmp/codex-review.txt "Review this PR using the soularr-pr-review-fix skill. Review only — do not push fixes. Focus on: correctness bugs, test gaps, rule violations from CLAUDE.md and .claude/rules/, unfinished wiring."
```

Read `/tmp/codex-review.txt` and parse the findings:
- If Codex found real issues: fix them, re-run tests, add a commit, push
- If Codex found only style nits or false positives: note them but proceed
- Re-run Codex if you made substantial changes (limit: 2 review rounds)

### 7. Present for approval

Present the finished PR to the user with:
- One-line summary of the bug
- What the fix actually changed (structural, not just symptom)
- Test coverage added
- Codex review verdict (clean / nits noted / issues fixed)
- PR URL

Wait for the user to approve before merging.

## Rules

- Do NOT ask the user for confirmation between steps — run the full pipeline autonomously
- DO stop and ask if: the bug turns out to be a design decision (not a bug), or the fix would require changes to multiple repos, or you can't reproduce the problem
- The sub-agent in step 2 must be truly independent — don't lead it to your conclusion
- Follow all code-quality.md rules: typed dataclasses, TDD, no parallel code paths, finish what you start

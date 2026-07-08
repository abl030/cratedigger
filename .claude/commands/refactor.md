# Refactor Pipeline

Structural refactor: invariant → pin + fuzz → inventory → staged commits →
adversarial review rounds → PR → merge → deploy → reflect.

**Usage**: `/refactor <description>`

State the invariant in ONE sentence. If you can't, scope down.

## Test doctrine — single source of truth

`.claude/rules/code-quality.md` § "Testing — Red/Green TDD" and
`docs/generated-testing.md` own the test rules. This skill adds NO test
vocabulary of its own. The non-negotiable core: **an invariant ships as a
PAIR — one deterministic pin AND one generated (Hypothesis) property, in
the same PR.** Defining an invariant and only pinning it is 90% of the
race and then sitting down.

## Workflow

1. **Load context**: CLAUDE.md, code-quality.md, scope.md, path-scoped rules.
2. **Invariant** (one sentence) — this is what the pin, the property, the
   grep gate, and every review round audit against.
3. **Inventory + live probe**: grep every participating site (delegate a
   minimal-context explorer for breadth); probe real shapes (schemas, live
   rows, subprocess output, on-disk layout). Declared types lie.
4. **Plan 2–5 staged commits**, each standalone: suite-relevant tests
   green, full-repo pyright 0. RED first (pin + property), then implement,
   then GREEN — record RED evidence in the commit body.
5. **Invariant grep gate** before any review: old helpers, raw
   reconstructions, bypasses — every hit migrated, deleted, or justified.
6. **Adversarial review rounds** — reviewer is an **Opus agent** with
   minimal context (do not feed it your conclusions; findings ordered by
   severity; no edit authority). Codex (`nix-shell --run "codex exec
   review --base main -o /tmp/codex-review.txt"`) is an optional second
   engine for high-risk branches — historically strong at deploy-window
   state and filesystem-limit boundaries. Always review ONE clean
   committed snapshot; never a dirty tree. Don't poll or interrupt a
   quiet review.
7. **Every real finding** → a review-fix commit `review(rN <engine>): <finding>`
   containing either a pinning test + property or a documented-equivalence
   argument (scope.md's forward-only rule beats compat shims). Rounds
   continue until a clean round; at 6 real rounds, stop and file follow-ups.
8. **Mutation-qualify the property**: revert the fix's single point (or
   plant an equivalent mutant) and show the generated property kills it.
9. **PR** with the review audit trail; **merge commit** (per CLAUDE.md —
   never rebase/squash); deploy via `/deploy`; verify a unique signature
   in the deployed store; live-verify behavior.
10. **Reflect**: two or three lines — what reviews caught, what the
    inventory missed, which probe should have run earlier.

## Rules

- Autonomous end-to-end; stop only if the live system contradicts the task.
- Implementer subagents get the PAIR requirement verbatim in their brief —
  never offer a deterministic-only alternative.
- Reviews are cheap relative to shipped bugs: default to another round.

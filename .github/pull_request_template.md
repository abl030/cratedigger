## Summary

<!-- What changed and why. Keep issue references non-closing until deploy/live proof, unless this PR is docs-only. -->

## Fault injection

One sentence (a short list if the change touches an eight-clause checker or similar — per-clause evidence does not fit in one sentence): what mutant(s) you tried against this diff and what happened — name the mutant and the test, not just "planted a mutant, confirmed RED". This used to be an exhaustive per-diff-site table; the table is where confabulation happened (PR #1209's matrix claimed "RED at the property, not just a pin" for the exact property later proved agree-by-construction), so it is gone in favor of an honest account. Whether to run fault injection at all, and how much, is the "when in doubt" judgment call from `.claude/rules/code-quality.md` § "Testing — Red/Green TDD" — this section is where you report it, not a demand that every PR contain a mutant story; "N/A" is a fine answer for a change fault injection doesn't bear on, UNLESS one of the unconditional obligations below applies to this PR. What stays unconditional regardless of what this section says: the regression-pin rule ("the pin must fail on the defect and pass on the fix, proven with an actual planted mutant, not asserted from reading the pin"); the adapter rule ("Each named adapter must be exercised by the invariant's deterministic pin and generated property ... and must have at least one known-bad mutant at that adapter"); and Standing scope — a PR **adding or changing a checker clause** audits that checker's clauses as part of the change and records, per clause, the named world and the killed mutant here (not "N/A"). A diff with mutable Python production surface also reports its mutmut breadth-pass tally (mutants run / killed / survived) and each survivor dismissed as equivalent with its one-line rationale (`docs/mutation-testing.md`, issue #1317) — the tally is machine evidence, the dismissals are audited claims.

## Reviewer

Independent review is two agents with disjoint jobs (`.claude/rules/code-quality.md` § "Pre-Commit Review Gate"): a READER that only reads, thinks, and prods production code (claim re-derivation, namespace cross-reference, composition seams, auditing the author's mutmut survivor dismissals — no mutants), and a MUTANT RUNNER that plants at least two mutants per new or changed test in its own isolated worktree, aimed at that test's named subject — not only at sites the author's sentence already covers, and including tests added to kill mutmut survivors — reporting each KILLED/SURVIVED with actual command evidence. A mutant that cites a scheduling change for a kill-both-phases test is the #1155 shape; a prose RED without output is the #1209 shape. One agent may do both only on a small, low-risk diff.

## Risk

<!-- Blast radius. What this does not change. -->

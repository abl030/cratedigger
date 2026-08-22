## Summary

<!-- What changed and why. Keep issue references non-closing until deploy/live proof, unless this PR is docs-only. -->

## Fault injection

One sentence (a short list if the change touches an eight-clause checker or similar — per-clause evidence does not fit in one sentence): what mutant(s) you tried against this diff and what happened — name the mutant and the test, not just "planted a mutant, confirmed RED". This used to be an exhaustive per-diff-site table; the table is where confabulation happened (PR #1209's matrix claimed "RED at the property, not just a pin" for the exact property later proved agree-by-construction), so it is gone in favor of an honest account. Whether to run fault injection at all, and how much, is the "when in doubt" judgment call from `.claude/rules/code-quality.md` § "Testing — Red/Green TDD" — this section is where you report it, not a demand that every PR contain a mutant story; "N/A" is a fine answer for a change fault injection doesn't bear on, UNLESS one of the three unconditional obligations below applies to this PR. What stays unconditional regardless of what this section says: the regression-pin rule ("the pin must fail on the defect and pass on the fix, proven with an actual planted mutant, not asserted from reading the pin"); the adapter rule ("Each named adapter must be exercised by the invariant's deterministic pin and generated property ... and must have at least one known-bad mutant at that adapter"); and Standing scope — a PR **adding or changing a checker clause** audits that checker's clauses as part of the change and records, per clause, the named world and the killed mutant here (not "N/A").

## Reviewer

Plant at least two mutants per new or changed test yourself, aimed at that test's named subject — not only at sites the author's sentence already covers. A mutant that cites a scheduling change for a kill-both-phases test is the #1155 shape.

## Risk

<!-- Blast radius. What this does not change. -->

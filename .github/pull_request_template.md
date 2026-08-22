## Summary

<!-- What changed and why. Keep issue references non-closing until deploy/live proof, unless this PR is docs-only. -->

## Fault injection

One sentence: what mutant(s) you tried against this diff and what happened — name the mutant and the test, not just "planted a mutant, confirmed RED". This used to be an exhaustive per-diff-site table; the table is where confabulation happened (PR #1209's matrix claimed "RED at the property, not just a pin" for the exact property later proved agree-by-construction), so it is gone in favor of an honest account. The regression-pin rule ("accepted only with planted-mutant evidence in both directions") and the adapter rule ("each named adapter must have at least one known-bad mutant") stay unconditional regardless of what this sentence says — `.claude/rules/code-quality.md` § "Testing — Red/Green TDD".

## Reviewer

Plant at least two mutants per new or changed test yourself, aimed at that test's named subject — not only at sites the author's sentence already covers. A mutant that cites a scheduling change for a kill-both-phases test is the #1155 shape.

## Risk

<!-- Blast radius. What this does not change. -->

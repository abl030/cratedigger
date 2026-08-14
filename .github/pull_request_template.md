## Summary

<!-- What changed and why. Keep issue references non-closing until deploy/live proof, unless this PR is docs-only. -->

## Kill matrix (per diff site, not per PR)

Every new or changed assertion, checker clause, and constant gets its own row. A mutant that kills row A does not qualify row B. "Planted a mutant, confirmed RED" is not a matrix. A row that cannot name a one-line production mutation is the finding.

| Site (assertion / clause / constant) | One-line production mutant | Test that must go RED | Result |
| --- | --- | --- | --- |
| | | | |

## Reviewer

Plant at least two mutants yourself at sites the matrix names. Do not only check the author's table.

## Risk

<!-- Blast radius. What this does not change. -->

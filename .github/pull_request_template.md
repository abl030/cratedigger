## Summary

<!-- What changed and why. Keep issue references non-closing until deploy/live proof, unless this PR is docs-only. -->

## Kill matrix (per diff site, not per PR)

Every new or changed assertion, checker clause, and constant gets its own row. A mutant that kills row A does not qualify row B. "Planted a mutant, confirmed RED" is not a matrix. A row that cannot name a one-line production mutation is the finding.

| Site (assertion / clause / constant) | One-line production mutant | Test that must go RED | Result |
| --- | --- | --- | --- |
| | | | |

## Reviewer

Plant at least two mutants yourself. Aim at the named subject of each new or changed test, not only at sites the author's table already lists. A matrix that cites a scheduling mutant for a kill-both-phases test is the #1155 shape.

## Risk

<!-- Blast radius. What this does not change. -->

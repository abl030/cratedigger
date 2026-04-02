# Scope — Clean As You Go

- Every bug fix is a refactoring opportunity. Don't just patch the symptom.
- If a bug was caused by duplication, inconsistency, or missing abstractions — fix the structure, not just the output.
- Add contract tests and shared code so the same class of bug can't recur.
- Don't ask permission to refactor when fixing a bug. The refactor IS the fix.
- One logical change per commit still applies — but "fix + refactor that caused it" is one logical change.

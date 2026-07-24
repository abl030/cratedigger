# Scope — Clean As You Go

- Every bug fix is a refactoring opportunity. Don't just patch the symptom.
- If a bug was caused by duplication, inconsistency, or missing abstractions — fix the structure, not just the output.
- Add contract tests and shared code so the same class of bug can't recur.
- Don't ask permission to refactor when fixing a bug. The refactor IS the fix.
- One logical change per commit still applies — but "fix + refactor that caused it" is one logical change.

## Single-operator, no backwards-compat

Each Cratedigger deployment has one trusted operator. The module is still
distributed and other installations exist, so defaults, examples, and security
boundaries must not rely on this homelab's paths or identities. The one-operator
rule removes multi-tenant authorization and backwards-compatibility machinery;
it does not turn a co-resident service or another installation into trusted
authority. Treat that as a hard rule, not a casual observation — it shapes a
class of decisions:

- **Backfills are not product code.** Anything that has to walk the existing data once after a schema change is an operator/agent-driven one-shot. The agent runs it during the deploy window (live `python3 -c '...'`, heredoc'd Python on doc2, raw SQL via `pipeline-cli query --write --confirm WRITE -`, whatever fits) and then throws it away. Pure-SQL data work belongs in a numbered migration (run-once by the migrator). Network-dependent data work that the migrator can't express belongs in the agent's working memory or the deploy doc as a reference shape — never in a committed `scripts/backfill_*.py`. See the search-plan iter2 PR1 cleanup (commits in the 2026-05-25 series) for the canonical instance.
- **Forward-only data assumptions.** After a deploy completes, the code assumes the new column/row exists with the expected values. No defensive `if old_shape_exists: …` branches, no fallback paths for "what if the migration didn't run," no `getattr(row, 'new_field', sensible_default)`. The migration ran. The data is there. Move on.
- **No deprecated-but-kept helpers for phantom future readers.** When a follow-up PR makes a previous PR's helper obsolete, **delete the helper in the follow-up**. Don't leave it behind with a `# DEPRECATED: …` comment "for migration safety" or "in case someone is still using it." Nobody else is using it. The accumulation of deprecated cruft across iterations is itself a product bug.
- **No retry-window / idempotency machinery for one-shot operations.** If something runs exactly once (a backfill, a data fix, a recovery script), it doesn't need 30d-vs-1d retry windows, advisory locks for concurrent invocations, dry-run flags, or batched commits for resumability. Those affordances exist for long-lived operational tooling that genuinely runs multiple times. A one-shot just runs.
- **The agent IS the operator for ops purposes.** When a deploy needs to walk data through external network calls (MB / Discogs lookups), the right answer is "the agent SSHes in and runs a transient Python invocation during the controlled window," not "we commit a script and a systemd unit and tests to maintain it forever." The agent has Read / Bash / file-write tools; the operator has the agent. That stack is the one-shot deployment surface.

The pattern this rule kills: every iteration accumulates more scripts, more deprecated helpers, more "in case we need to" infrastructure, until the repo carries 5x the operational machinery of the actual product. The single-operator invariant says: stop. The repo carries product code. Operational one-shots live in chat, heredocs, and the deploy runbook — not on disk forever.

---
name: single-operator-no-backfill-scripts
description: "Cratedigger is a single-operator system. Backfills/one-shots are agent-driven during deploys, NOT committed scripts. No compatibility shims, no deprecated-but-kept helpers, no retry-window machinery for one-shot operations."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 112affdc-9033-495e-b8ec-0799417bfaf8
---

Cratedigger has exactly one user: the operator. There is no "other people's installs" to worry about. The agent IS the operator for ops purposes.

**The rule:** When the conversation considers committing a script for one-time data work, a `# DEPRECATED` helper, retry-window machinery for one-shot operations, or defensive `if old_shape: …` fallbacks for "what if the migration didn't run" — push back. The agent runs the one-shot. The repo carries product code only.

**Why:** Before this rule was codified (commit `9126c6d`, CLAUDE.md § archivist frame + `.claude/rules/scope.md` § single-operator), every iteration accumulated more operational machinery: May 19 left `scripts/backfill_release_group_year.py`; PR #370's U3 added `scripts/backfill_field_resolutions.py` with retry-window tables, advisory locks, batched commits, and 1,300 LOC of tests — for an operation that runs once per deploy. ~2,800 LOC of code-we-don't-want-to-maintain. The user's frustration ("we keep having this conversation") triggered the explicit codification and the deletion of all backfill scripts in commit `2ba7797`.

**How to apply:**
- Schema migrations land via `cratedigger-db-migrate.service` (numbered SQL files). Pure-SQL data work goes in the same migration sequence (see migration 033 for `is_va_compilation` + `one_track_structural`).
- Network-dependent data work (MB/Discogs lookups during deploys) runs as a transient `python3 -c '...'` or heredoc'd Python invocation that the agent generates during a controlled window when the 3 DB-mutating services are stopped. The shape lives in the deploy runbook for reproducibility — never as a committed `scripts/backfill_*.py`.
- After a deploy completes, the code assumes the new state. No defensive `if column_exists: …` branches.
- When a follow-up PR makes a previous PR's helper obsolete, DELETE the helper in the follow-up. Don't leave it behind.

Related: [[scope-clean-as-you-go]], [[archivist-frame]] (CLAUDE.md invariants).

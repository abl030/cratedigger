---
name: project-430-fakedb-migration-done
description: Issue
metadata: 
  node_type: memory
  type: project
  originSessionId: 67e95325-7ad6-4fe8-932e-a5ab7533f2ae
---

Issue #430 closed 2026-06-12: all `tests/web` contract tests migrated from the MagicMock harness to per-test bare `FakePipelineDB` (`_FakeDbWebServerCase`, `DB_FACTORY` hook for typed failure-injecting subclasses). `WEB_HARNESS_MOCK_BASELINE` in `tests/_mock_audit_scanner.py` is permanently empty — it bans `mock_db` references in tests/web and `_pipeline_db_test_harness` anywhere.

**Why:** patterns that compounded and are worth reusing on future multi-PR refactors:
- Exact-match ratchet baseline (growth fails AND un-shrunk baseline fails) made the migration self-enforcing across 5 PRs.
- `TestDashboardFakeParity` (real-PG vs fake shape comparison on identical seeds) converts fake-mirror drift from review archaeology into mechanical failures — the adversarial reviewer found 6 birth-drift divergences in my hand-written mirror; the parity gate would have caught them all.
- Adversarial same-engine pre-review caught structural gameability (ratchet evasion via aliasing, partial-wipe pins); Codex partner rounds caught production-SQL semantics drift (dense vs sparse series, errors-bucket FILTER, cycle-row ordering). Run both; they find disjoint things.

**How to apply:** for the next big migration, land the ratchet FIRST (baseline = current counts), shrink per PR; pair any hand-written fake mirror of SQL with a real-PG shape-parity test in the same PR.

Follow-ups + the full session playbook (review-engine division of labour, fake-validator findings, sys.path landmines, setUp-collision gotchas) are in **issue #445** — read it before starting the beets-side mock migration.

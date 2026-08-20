---
name: 1156-suite-perf-complete
description: "#1156 all 7 items shipped 2026-08-20 (PRs #1218/#1223/#1224/#1225): four measured per-target wins, ZERO aggregate change — test_nix_module::remainder is ~97% of the phase wall time (follow-up #1226)"
metadata:
  type: project
---

2026-08-20: issue #1156 fully dispositioned. Items 2/3/6 in PR #1218; items 1, 4+5,
7 in PRs #1225, #1224, #1223. Combined `main` `aee0290c` re-gated green (6/6
phases, 11,146 tests, 464 targets). No deploy — all test-infra/dev-shell/docs.

**The headline lesson: per-target wins do not move suite wall time unless they
are ON the critical path.** Four measured wins produced *zero* aggregate change
(123.5s before, 126.9s after — same within noise), because
`tests.test_nix_module::remainder` at **123.0s** is ~97% of a 126.9s python
phase. Everything else finishes inside its shadow. Always check the pole
(`SLOW:` lines in the bundle's `python.log`) BEFORE optimising anything, and say
plainly when a real win won't show up in the headline. Follow-up: **#1226**.

Measured wins, each independently reproduced by review on a quiet box:

- **Item 1** (shard `webAuthMatrix`, 39 worlds → 20/19, each its own
  `HOTSPOT_ISOLATED_METHODS` singleton): module critical path **−20% to −29%**
  under controlled paired load. The feared 2→3 concurrent-heavy-eval regression
  did NOT occur at 22 or 30 busy processes; `_rest` was unharmed. Pole moved to
  `remainder`, exactly as the pre-existing docstring predicted.
- **Item 4** (shared `_shim.py` + `import _shim` stub so bytecode is
  `__pycache__`d): `test_deploy_pin_generated` **53.70s → 49.12s (−8.5%)**.
- **Item 7** (`TRUNCATE`→`DELETE` on test resets): isolated reset **2.18×**,
  `test_pipeline_db` **13.13s → 9.74s (−26%)**.
- **Item 5**: measured within noise; shipped as consistency work, labelled so.

Durable technical findings:

- **`TRUNCATE` never fires row-level DELETE triggers; `DELETE` does.** Migration
  066 installs three `DEFERRABLE INITIALLY DEFERRED` constraint triggers, so
  `delete_all_rows` MUST run inside one transaction — not "for atomicity" but for
  correctness. With autocommit on and the same table order it raises
  `CheckViolation` and leaves rows behind. Use `db._atomic()` (production's own
  helper) — a hand-rolled autocommit flip also skips `_ensure_conn()` and breaks
  on a closed connection.
- **`ON DELETE RESTRICT` fires at end-of-statement even when the FK is
  `DEFERRABLE INITIALLY DEFERRED`** — only the existence check defers, not the
  delete-time action. Forces `album_requests` before `import_jobs`.
- **`DELETE` + `autovacuum=off` leaves dead tuples where `TRUNCATE` reclaimed**
  (+1.90 MiB vs +0.15 MiB over 6,000 resets) but removes catalog bloat an order
  of magnitude larger (+15.14 → +0.00 MiB): total growth **5.6× smaller**, and
  sub-linear (page-level opportunistic pruning). Crossover exists at ~3,200 rows
  per reset, far above the real ~15.
- **Reverting beats shipping on assertion.** Two changes were reverted after
  mechanical tracing showed they could not do what they claimed: a Discogs
  `handler_exit_delay` (couldn't move `max_active`) and a `sys.path.insert` in
  the new stubs (only mattered under `PYTHONSAFEPATH`, absent from the repo, and
  strictly less correct for symlinks).

See [[concurrent-agent-measurement-hazards]] and [[1214-daily-gate-enospc]].

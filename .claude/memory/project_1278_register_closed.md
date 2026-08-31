---
name: 1278-register-closed
description: "2026-08-31: issue #1278 (architecture-review debt register) CLOSED complete; comment-residual sweep PR #1311 live-verified; candidate 1 continues as #1312"
metadata:
  type: project
---

Issue #1278 closed 2026-08-31 with everything shipped: strong candidates 2-9, all six "worth exploring" items, both speculative/hygiene items, and the comment residuals (this session's PR #1311, deployed via nixosconfig `27f8b18f` pinning `20b55c53`, cycle `fc74d033` verified from the exact store). **Strong candidate 1 (owned-key slskd module) was NOT shipped** — PRs #1280/#1281 fixed specific ownership bugs only; the full extraction continues as issue #1312, which also carries the slskd sweeps' exception-contract residual and the `_match_transfer_all_history` docstring sweep.

Lessons from the sweep's review round (two-role split, working as designed for the fifth series running):

- **A generated-checker clause over a fake must scope its claim to the fake's world space.** V6's first draft claimed production verdict-totality after a write; production legitimately returns `not_found`/`beets_unavailable` post-write in worlds the fake cannot produce — the reader drove the real service over such a world and showed the clause accusing correct code. Keep enforcement, scope the CLAIM, and state the widening obligation for future strategies.
- **A kwarg-deletion sweep is finished by whole-tree Pyright, not by grep**: the call-expression regex missed alias-bound sites (`_build_candidate = staticmethod(...)`) and two extra files; deleting the param made every missed site fail loudly.
- **Value-parity seeders must populate every payload cohort** or halves compare vacuously (the first world left `superseded_and_legacy` empty by construction and `zero_find_cycles` pinned at 1, hiding any-vs-count and superseded-branch defects — both mutant-proven after enrichment).
- Three residual claims on the register were stale when written (the `record_unfindable_run_metrics` CHECK-mirror gap had been closed since 2026-08-12) — verify each residual against current main before building it.

Pending operator decisions recorded in the close-out comment (never taken by agents): request 8954 Bad Rip re-run; the two preserved preview-lane divergences; absorbing `lib/beets_delete.py`'s era probe into `CAPABILITIES`.

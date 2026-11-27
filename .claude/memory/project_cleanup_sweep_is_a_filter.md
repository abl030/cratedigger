---
name: project_cleanup_sweep_is_a_filter
description: The global "Cleanup Wrong Matches" sweep is a filter that leaves only possible imports — its kept_would_import rows are the point, not a bug
metadata:
  type: project
---

The global **Cleanup Wrong Matches** button (`cleanup_all_wrong_matches`, `pipeline-cli wrong-match-triage`) deliberately runs the evidence reducer on every visible row. It deletes proven non-imports so that what is left is the operator's review list of *possible imports*. `kept_would_import` survivors (~25% of evaluated rows in Nov 2026) are the intended output.

It is NOT gated by the `rejection_scenario_is_delete_eligible` allowlist. That allowlist governs only the automatic post-rejection lane (issue #1468, resolved 2026-11-28 by fixing the docs).

**Why:** In #1468 I misread the button as "delete all" and proposed making it delete unconditionally. The user corrected me: it works as designed.

**How to apply:** Don't conflate it with the unconditional operator deletes (per-release Delete All, Converge's unmatched rows — see [[project_converge_operator_authority]]). Those bypass the classifier. The Cleanup sweep must keep using it.

---
name: project-1278-wx4-wrong-match-queue-view
description: "#1278 wx4 shipped 2026-08-31: Wrong Matches queue projection extracted to web/wrong_match_queue_view.py, live-verified"
metadata:
  type: project
---

2026-08-31: "Worth exploring" item 4 of #1278 SHIPPED and live-verified. PR #1304
(merge commit `aca9773d`), nixosconfig pin `6d1830d7`, cycle `d2ce17ec` verified
from the new source store; live `/api/wrong-matches` served by the extracted
module (empty queue cross-checked genuine — the one on-disk folder is
unreferenced, triage's category).

- `web/wrong_match_queue_view.py::build_wrong_match_groups` is the projection's
  non-HTTP interface: narrow `WrongMatchQueueDB` Protocol (get_wrong_matches,
  list_active_import_jobs, get_download_history_batch) + injected
  `check_beets_library_detail`/`compute_library_rank` (route supplies them from
  `web.server` at request time — that module stays the single patch seam).
- Equivalence instrument (3rd use in series): seeded-world byte-differential —
  render the full route function in a base-tree `git archive` vs working tree
  over identical fake worlds, volatile timestamps normalized. Session one-shot,
  never committed (scope.md).
- Review: two-role split, 40 runner mutants; the ONE survivor was the diff's only
  changed payload expression (`_serialize_import_job` inlined to
  `job.to_json_dict()`) — see [[review-mutants-target-the-changed-expressions]].
- `tests/web/test_<stem>.py` naming makes a non-route `web/` module's direct
  tests probe-derivable by targeted selection; the EXACT_PATH_NEIGHBOURS entry
  then only needs to add the route contract tests (+ MASKABLE_ENTRY_PINS pin,
  since web/ is unpoliced).

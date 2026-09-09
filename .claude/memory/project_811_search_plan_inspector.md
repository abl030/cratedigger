---
name: project-811-search-plan-inspector
description: Issue #811 shipped 2026-09-09 (PR #1400, deployed 5b7c6966) — search-plan detail page rewrite + download_log.search_log_id link; lessons on the fingerprint-guarded stamp, the acceptance-outcome window, and load-starved gates
metadata:
  type: project
---

Issue #811 shipped and was live-verified on 2026-09-09: PR #1400 merged at 5b7c6966, deployed via `scripts/deploy.sh` (nixosconfig 5c784062), verified through `pipeline-cli search-plan show 986` on doc2 and the live page at music.ablz.au.

**Why:** the operator wanted the quality override visible on the search-plan detail view and approved a real column linking downloads to the search that found them ("do it properly, it adds good tooling for later too. linking searches to imports"). Summary panel on Recents rows stays unchanged by decision.

**How to apply:**
- The link is `download_log.search_log_id` (migration 086). The grab is claimed BEFORE the search row exists, so `record_consumed_search_attempt` stamps the id onto `active_download_state` inside its own transaction, guarded on `status='downloading'` plus an exact `attempt_fingerprint` match. Never pair by time.
- `IMPORT_ACCEPTANCE_OUTCOMES` (success, force_import, local_import, manual_import) is the "was this request ever imported" vocabulary; `WRITABLE_IMPORT_ACCEPTANCE_OUTCOMES` is the terminal guard's narrower set. Do not widen a refusal guard with a retired outcome. A window keyed on `outcome='success'` alone mis-windowed 17.6% of live requests.
- Production `allowed_filetypes` is a 13-tier ladder (flac 24/192 ... wav), not the `flac,mp3` default; the dev server shows the default.
- A guard with two conjuncts needs a world where each conjunct decides alone (the `processing` row with state+fingerprint retained was the missing world twice).
- Gates on doc1 starve at load 25+: `test_mb_artist_pagination_generated`'s fan-out observation and #1392's daily-flake timing tests fail on scheduling, pass in isolation. Launch the gate while another session's suite holds the admission lock so it queues behind it.
- A branch cut before a migration lands on main collides on the version number; the gate's migrator test catches it. Renumber, do not resequence main.

Related: [[project-1355-partial-run]], [[feedback-live-means-verified-deploy]].

**Live verification lesson (2026-09-09, #1405):** the search→download link shipped inert. `lib/quality/download_state.py::_copy_download_state` rebuilt `ActiveDownloadState` field by field on every poll reducer pass and carried `attempt_fingerprint` by hand (a comment, not a test) but not the new `search_log_id`, so the first poll after claim dropped it. Both ends were tested; the writer in the middle was not. Any new identity field on a persisted struct owes a pin on EVERY copy site, and copies should be `msgspec.structs.replace`, never field-by-field. Watch the first real row after deploy, not just the schema and the page.

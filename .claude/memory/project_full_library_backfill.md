---
name: project-full-library-backfill
description: On 2026-06-04 the whole beets library was ingested into the pipeline as wanted requests for upgrade; explains the ~4700 wanted cohort
metadata: 
  node_type: memory
  type: project
  originSessionId: e9668fa5-6583-4b44-8c39-ca73609609e3
---

On 2026-06-04 the entire beets library (8320 albums) was backfilled into the pipeline DB. Before: only 4307 distinct release IDs were tracked (3590 imported). The 4056 non-lossless missing albums (mostly legacy MP3, plus AAC) were added as **`wanted` source=request** rows via the canonical `pipeline-cli add <mbid>` path looped on doc2 (idempotent — each `add` short-circuits on `get_request_by_release_id`, so safe to re-run). Result: **8299/8320 (99.7%) coverage**; the only 21 still-missing are deliberately-excluded already-lossless albums (no upgrade headroom worth the search load).

**Why wanted, not imported:** `wanted` = "on-disk copy hasn't met the quality bar, keep hunting for an upgrade"; `imported` = "bar met." So the whole lossy back-catalogue now rides the never-stops-searching upgrade loop. The anti-downgrade guard is real and verified: `load_current_evidence_for_action` (lib/import_evidence.py) measures the on-disk copy at decision time and **fails closed** on error, so a wanted row with NULL evidence can never let a worse candidate replace a better on-disk file. NULL `current_*`/evidence on these rows is fine — backfilled from on-disk at decision time.

Every added row got an active search plan (`generate_for_new_request`, generator `search-plan/2026-05-25-1`); 0 plan-less/unsearchable. The operator accepted the tier-0 "stampede" (all 4056 land with attempt-counters=0, dominating `get_wanted_searchable` ordering for ~1–2 days). Within minutes the loop began converting (imports +21, 61 concurrent downloads). One-shot, not committed (see [[feedback_single_operator_no_backfill_scripts]]).

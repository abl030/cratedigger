---
name: feedback-discogs-mirror-indexes
description: "When adding an ad-hoc index to the discogs mirror DB, plain CREATE INDEX is acceptable — no need for CONCURRENTLY or to wait for the build"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 006fd00b-7a40-43b0-b09c-d48f70500b4a
---

When adding a new index to the live discogs mirror PostgreSQL (on doc2, nspawn container at 192.168.100.13), plain `CREATE INDEX` is acceptable — no need to use `CREATE INDEX CONCURRENTLY` and no need to babysit the build. Fire it off and move on.

**Why:** The discogs UI (`discogs.ablz.au` + the cratedigger Browse-Discogs path) tolerates a few minutes of table lock during a one-off index add. The DB is read-only at runtime (the importer is a monthly oneshot); a `CREATE INDEX` that blocks readers for ~20 minutes is preferable to the ceremony around `CONCURRENTLY` (and `CONCURRENTLY` is slower and can fail partway through).

**How to apply:** When the workflow is "add an index to `schema.rs` for future reimports, and also create it now on the live DB so the new query is fast today" — for the now-on-live-DB step, run plain `CREATE INDEX … ON … (…)` via psql and don't wait. Don't run `EXPLAIN ANALYZE` afterwards to verify unless the user asks; just trust that the next query path will pick it up.

Worked example: 2026-05-18, adding `idx_release_track_artist_artist_id` for the new `/api/artists/{id}/appearances` endpoint. I used `CREATE INDEX CONCURRENTLY` (took 22s); user noted in retrospect that the ceremony wasn't necessary — just fire `CREATE INDEX` next time.

Related: [[project-cratedigger-rename]] for the broader mirror infra context.

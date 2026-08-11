-- 076_drop_canonical_release_id.sql -- retire migration 074's columns (#1059)
--
-- NUMBERED 076, NOT 075, AND THE GAP IS DELIBERATE.  Version 75 is already
-- recorded in this deployment's `schema_migrations` (applied 2026-08-08): it
-- was `075_replaced_from_status.sql`, shipped by PR #1073 and then reverted
-- out of the repository by PR #1074 while remaining applied in the database.
-- `lib/migrator.py` selects work by pure set difference (`shipped - applied`),
-- so a file numbered 075 here would be seen as already applied and SILENTLY
-- SKIPPED -- these columns would never drop, and `assert_schema_current`
-- would report a healthy schema while doing it.  Never renumber this down.
--
-- Migration 074 added `canonical_release_id` / `canonical_resolved_at` for a
-- dual-identity runtime design: every request would carry BOTH the merged-away
-- acquisition id and the survivor, and a daily reconciliation sweep would keep
-- the second column fresh while twelve read paths resolved over the union.
-- That design was reverted (PR #1074).  The merge is now followed at the ONE
-- seam where it announces itself -- import-time `mbid_not_found` -- which
-- retags the installed Beets album onto the survivor and then rekeys
-- `mb_release_id` in place.  A request has exactly one release id again, so
-- these two columns are dead weight plus two CHECK constraints and an index
-- that can only ever describe a state nothing writes.
--
-- 074 was applied in production on 2026-08-07 and stays in history untouched.
--
-- DEPLOY HAZARD -- this migration must run behind
-- `scripts/cratedigger_deploy_hold.py`.  Dropping columns NARROWS the
-- `AlbumRequestRow` msgspec boundary: `msgspec.convert` ignores extra keys but
-- raises on a MISSING required field, so a mid-flight pre-#1059 cycle would
-- raise on its next `SELECT * FROM album_requests` read the moment the columns
-- disappear.  Time it like any other destructive schema change
-- (`.claude/rules/pipeline-db.md` -- "Deploy-window semantics").

-- Dropping the columns would cascade to both of these, but naming them makes
-- the removal auditable against 074 rather than implicit.
DROP INDEX idx_album_requests_canonical_release_id;

ALTER TABLE album_requests
    DROP CONSTRAINT album_requests_canonical_requires_observation,
    DROP CONSTRAINT album_requests_canonical_is_not_acquisition,
    DROP COLUMN canonical_release_id,
    DROP COLUMN canonical_resolved_at;

-- 026_album_requests_release_group_year.sql
--
-- U3 of the search-plan-entropy plan: persist the release-group's first
-- release year alongside the per-release ``year`` already on
-- ``album_requests``. R9 (data layer).
--
-- ``year`` reflects the specific pressing the operator queued (e.g. a
-- 2008 Kid A reissue). The release-group's first-release year (e.g.
-- 2000 for the original Kid A) is what most Soulseek folder names use,
-- so the generator wants both values: it emits a year-suffixed query
-- per distinct year. When the two values match, the conditional slot
-- is dropped to avoid the kind of low-entropy slot repetition U5 kills
-- elsewhere.
--
-- Nullable: many requests will not have a populated
-- ``mb_release_group_id`` (legacy rows pre-dating the column, Discogs-
-- only rows, manual/imported rows). The generator skips the
-- release-group-year slot when this column is NULL.
--
-- Backfill is handled by ``scripts/backfill_release_group_year.py`` —
-- run as part of the U3 deploy. Forward-only ALTER here; the script
-- fills the column from the local MB mirror in idempotent batches.

ALTER TABLE album_requests
    ADD COLUMN release_group_year INTEGER NULL;

-- 009_curator_ban_outcome.sql - record curator-marked bad-rip events
--
-- The "Bad rip" button (issue #188) is treated as just another download_log
-- event so it surfaces uniformly on the recents tab, the pipeline tab's
-- "last:" verdict line, and the per-row download history. Add the outcome
-- to the CHECK constraint; the route writes the row via log_download with
-- outcome='curator_ban'.

ALTER TABLE download_log DROP CONSTRAINT IF EXISTS download_log_outcome_check;
ALTER TABLE download_log ADD CONSTRAINT download_log_outcome_check
    CHECK (outcome IN ('success', 'rejected', 'failed', 'timeout',
                       'force_import', 'manual_import', 'curator_ban'));

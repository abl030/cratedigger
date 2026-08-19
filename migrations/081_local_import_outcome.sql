-- 081_local_import_outcome.sql
--
-- PR3 of issue #1176 (import-local lane): the lane that actually writes
-- job_type='local_import' (migration 080) and source='local' (migration
-- 080) now exists. It needs its own download_log.outcome value the same
-- way force-import got 'force_import' — a successful local import is a
-- distinct, denormalized outcome, not a bare 'success' (which would erase
-- the provenance distinction the Recents tab and every ('success',
-- 'force_import')-style "was this imported" filter already key on).
--
-- Widens download_log_outcome_check (latest definition: migration 054) to
-- admit 'local_import' alongside the existing taxonomy. This is the ONLY
-- outcome-side change PR3 needs: job_type='local_import' and
-- source='local' are both already admitted (migration 080).

ALTER TABLE download_log DROP CONSTRAINT IF EXISTS download_log_outcome_check;
ALTER TABLE download_log ADD CONSTRAINT download_log_outcome_check
    CHECK (outcome IN ('success', 'rejected', 'failed', 'timeout',
                       'force_import', 'manual_import', 'curator_ban',
                       'measurement_failed', 'user_offline',
                       'have_analysis_error',
                       'youtube_running', 'youtube_success', 'youtube_failed',
                       'local_import'));

-- Widen download_log_outcome_check to include 'have_analysis_error'.
--
-- Installed-HAVE analysis failures are attempt-local environment failures,
-- not quality verdicts.  They receive their own audit outcome so ordinary
-- retry bookkeeping can proceed without reusing a quality outcome.

ALTER TABLE download_log DROP CONSTRAINT IF EXISTS download_log_outcome_check;
ALTER TABLE download_log ADD CONSTRAINT download_log_outcome_check
    CHECK (outcome IN ('success', 'rejected', 'failed', 'timeout',
                       'force_import', 'manual_import', 'curator_ban',
                       'measurement_failed', 'user_offline',
                       'have_analysis_error',
                       'youtube_running', 'youtube_success', 'youtube_failed'));

-- Widen download_log_outcome_check to include 'user_offline'.
--
-- lib/enqueue.py writes outcome='user_offline' for verified peer-offline
-- enqueue rejections (the pooyork incident handling), but the CHECK
-- constraint was never widened — the write raised CheckViolation the
-- moment the path fired (prod has zero user_offline rows). Caught on
-- 2026-07-02 when FakePipelineDB.log_download started mirroring this
-- constraint (test-fidelity Rule A) after the #146 phase-3 grace escape
-- shipped the same class of bug with outcome='error'.

ALTER TABLE download_log DROP CONSTRAINT IF EXISTS download_log_outcome_check;
ALTER TABLE download_log ADD CONSTRAINT download_log_outcome_check
    CHECK (outcome IN ('success', 'rejected', 'failed', 'timeout',
                       'force_import', 'manual_import', 'curator_ban',
                       'measurement_failed', 'user_offline',
                       'youtube_running', 'youtube_success', 'youtube_failed'));

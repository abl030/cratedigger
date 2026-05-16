-- 020_recover_stuck_preview_uncertain_jobs.sql - one-time recovery sweep
--
-- After the preview-never-decides refactor (U1-U6), no production code path
-- writes ``preview_status = 'uncertain'``. The preview worker now emits
-- exactly two terminal outputs per claimed job: ``evidence_ready`` (importer
-- decides on facts) and ``measurement_failed`` (self-healing finalize fires).
--
-- This migration is the one-time sweep that flips every existing
-- ``import_jobs`` row stuck in ``preview_status = 'uncertain'`` back to
-- ``preview_status = 'waiting'`` so the preview worker re-claims them on its
-- next tick. Under the new contract:
--   * Rows where the underlying audio was a #251 stderr false-positive now
--     re-measure clean; importer accepts; beets imports.
--   * Rows with genuine spectral rejections re-measure their facts cleanly;
--     importer rejects; self-healing finalize routes the request → wanted;
--     search resumes on the next poll tick.
--   * Rows with genuinely corrupt audio (rc!=0) mark ``audio_corrupt=true``;
--     importer rejects; self-healing finalize fires; request → wanted.
--
-- Idempotent: the WHERE clause guards against re-application — once a row
-- has flipped to ``waiting`` (or moved through the lifecycle into another
-- terminal), this migration touches nothing on a second run.
--
-- Precedent: ``migrations/018_neutral_import_job_preview_ready.sql:45-67``
-- is the recovery-sweep pattern this file mirrors.

UPDATE import_jobs
SET preview_status = 'waiting',
    preview_result = NULL,
    preview_message = 'Recovered by preview-never-decides refactor (020)',
    preview_error = NULL,
    preview_worker_id = NULL,
    preview_started_at = NULL,
    preview_heartbeat_at = NULL,
    preview_completed_at = NULL,
    importable_at = NULL,
    updated_at = NOW()
WHERE status = 'queued'
  AND preview_status = 'uncertain';

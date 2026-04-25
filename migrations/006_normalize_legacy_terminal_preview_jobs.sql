-- 006_normalize_legacy_terminal_preview_jobs.sql - clean legacy queue display
--
-- Before async previews existed, completed/failed import jobs had no preview
-- lifecycle. Migration 004 added preview_status with a temporary default of
-- `waiting`, which made old terminal history look like live preview backlog in
-- Recents. Terminal jobs already ran the importer path, so mark their preview
-- stage as effectively importable while preserving the import result itself.

UPDATE import_jobs
SET preview_status = 'would_import',
    preview_message = CASE
        WHEN preview_message IS NULL
          OR preview_message = 'Preview gate disabled'
          THEN 'Queued before async preview gate'
        ELSE preview_message
    END,
    preview_completed_at = COALESCE(preview_completed_at, completed_at, updated_at, NOW()),
    importable_at = COALESCE(importable_at, created_at, updated_at, NOW())
WHERE status IN ('completed', 'failed')
  AND preview_status = 'waiting'
  AND preview_attempts = 0
  AND preview_result IS NULL
  AND preview_started_at IS NULL
  AND preview_heartbeat_at IS NULL;

-- 018_neutral_import_job_preview_ready.sql - make preview readiness neutral
--
-- Async preview no longer authorizes imports with a stored `would_import`
-- verdict. It stores durable evidence and marks the job ready for the final
-- action-time evidence check.

ALTER TABLE import_jobs
    ALTER COLUMN preview_status SET DEFAULT 'would_import',
    ALTER COLUMN preview_message SET DEFAULT 'Preview gate disabled',
    ALTER COLUMN preview_completed_at SET DEFAULT NOW(),
    ALTER COLUMN importable_at SET DEFAULT NOW();

ALTER TABLE import_jobs
    DROP CONSTRAINT import_jobs_preview_status_check;

ALTER TABLE import_jobs
    ADD CONSTRAINT import_jobs_preview_status_check
        CHECK (
            preview_status IN (
                'waiting',
                'running',
                'evidence_ready',
                'would_import',
                'confident_reject',
                'uncertain',
                'error'
            )
        );

DROP INDEX idx_import_jobs_importable_claim;

CREATE INDEX idx_import_jobs_importable_claim
    ON import_jobs(importable_at, created_at, id)
    WHERE status = 'queued'
      AND preview_status IN ('evidence_ready', 'would_import');

CREATE INDEX idx_import_jobs_disabled_automation_preview_requeue
    ON import_jobs(created_at, id)
    WHERE status = 'queued'
      AND job_type = 'automation_import'
      AND preview_status IN ('evidence_ready', 'would_import')
      AND preview_message = 'Preview gate disabled'
      AND preview_result IS NULL;

UPDATE import_jobs AS job
SET preview_status = 'waiting',
    preview_result = NULL,
    preview_message = NULL,
    preview_error = NULL,
    preview_worker_id = NULL,
    preview_started_at = NULL,
    preview_heartbeat_at = NULL,
    preview_completed_at = NULL,
    importable_at = NULL,
    updated_at = NOW()
WHERE job.status = 'queued'
  AND job.preview_status IN ('would_import', 'evidence_ready')
  AND (
      job.preview_message IS DISTINCT FROM 'Preview gate disabled'
      OR job.preview_result IS NOT NULL
  )
  AND NOT EXISTS (
      SELECT 1
      FROM album_quality_evidence AS evidence
      WHERE evidence.owner_type = 'import_job_candidate'
        AND evidence.owner_id = job.id
  );

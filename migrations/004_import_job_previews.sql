-- 004_import_job_previews.sql - async import preview gate
--
-- Preview state lives on import_jobs because preview is a readiness stage for
-- the same queued import work drained by the serial beets importer.

ALTER TABLE import_jobs
    ADD COLUMN IF NOT EXISTS preview_status TEXT NOT NULL DEFAULT 'waiting'
        CHECK (
            preview_status IN (
                'waiting',
                'running',
                'would_import',
                'confident_reject',
                'uncertain',
                'error'
            )
        ),
    ADD COLUMN IF NOT EXISTS preview_result JSONB,
    ADD COLUMN IF NOT EXISTS preview_message TEXT,
    ADD COLUMN IF NOT EXISTS preview_error TEXT,
    ADD COLUMN IF NOT EXISTS preview_attempts INTEGER NOT NULL DEFAULT 0
        CHECK (preview_attempts >= 0),
    ADD COLUMN IF NOT EXISTS preview_worker_id TEXT,
    ADD COLUMN IF NOT EXISTS preview_started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS preview_heartbeat_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS preview_completed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS importable_at TIMESTAMPTZ;

-- Jobs already queued before this migration were created under the pre-preview
-- contract. Let them drain instead of waiting for a preview worker that did not
-- exist when they were enqueued.
UPDATE import_jobs
SET preview_status = 'would_import',
    preview_message = COALESCE(preview_message, 'Queued before async preview gate'),
    preview_completed_at = COALESCE(preview_completed_at, updated_at, NOW()),
    importable_at = COALESCE(importable_at, created_at, NOW())
WHERE status IN ('queued', 'running')
  AND preview_status = 'waiting'
  AND preview_attempts = 0
  AND preview_result IS NULL;

CREATE INDEX IF NOT EXISTS idx_import_jobs_preview_claim
    ON import_jobs(preview_status, created_at, id)
    WHERE status = 'queued'
      AND preview_status = 'waiting';

CREATE INDEX IF NOT EXISTS idx_import_jobs_importable_claim
    ON import_jobs(status, preview_status, importable_at, created_at, id)
    WHERE status = 'queued'
      AND preview_status = 'would_import';

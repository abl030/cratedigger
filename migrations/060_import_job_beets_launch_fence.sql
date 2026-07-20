-- 060_import_job_beets_launch_fence.sql
--
-- One import_jobs row is the durable identity of one Beets mutation.  The
-- launch fields are written atomically immediately before import_one.py is
-- allowed to run.  A running row with no marker is safe to retry after a
-- worker crash; a marked row is not.

ALTER TABLE import_jobs
    ADD COLUMN expected_request_status TEXT,
    ADD COLUMN beets_launch_authorized_at TIMESTAMPTZ,
    ADD COLUMN beets_launch_release_id TEXT,
    ADD COLUMN beets_launch_source_path TEXT,
    ADD COLUMN beets_launch_request_status TEXT,
    ADD COLUMN beets_launch_snapshot_fingerprint TEXT;

-- Existing queued jobs were prepared against the request state visible at
-- deployment. New enqueues capture this value in the same INSERT that creates
-- the job, so a later status change makes launch authorization fail closed.
UPDATE import_jobs AS job
SET expected_request_status = request.status
FROM album_requests AS request
WHERE request.id = job.request_id;

ALTER TABLE import_jobs
    DROP CONSTRAINT IF EXISTS import_jobs_status_check;

ALTER TABLE import_jobs
    ADD CONSTRAINT import_jobs_status_check CHECK (
        status IN (
            'queued',
            'running',
            'recovery_required',
            'completed',
            'failed'
        )
    );

-- A deploy must not reinterpret a pre-fence running row as proof that Beets
-- never started.  Conservatively stop it for operator recovery.  Deployment
-- stops the old importer before this migration, so no legacy worker can race
-- this conversion and launch afterward.
UPDATE import_jobs AS job
SET status = 'recovery_required',
    beets_launch_authorized_at = COALESCE(job.started_at, NOW()),
    beets_launch_release_id = request.mb_release_id,
    beets_launch_source_path = CASE job.job_type
        WHEN 'automation_import'
            THEN request.active_download_state->>'current_path'
        WHEN 'force_import'
            THEN job.payload->>'failed_path'
        WHEN 'youtube_import'
            THEN job.payload->>'staged_path'
        ELSE NULL
    END,
    beets_launch_request_status = request.status,
    beets_launch_snapshot_fingerprint = (
        SELECT evidence.snapshot_fingerprint
        FROM album_quality_evidence AS evidence
        WHERE evidence.id = job.candidate_evidence_id
    ),
    worker_id = NULL,
    heartbeat_at = NULL,
    message = 'Recovery required: job was running before the Beets launch fence was deployed',
    error = 'Automatic replay refused because the legacy worker may have reached Beets',
    updated_at = NOW()
FROM album_requests AS request
WHERE job.status = 'running'
  AND request.id = job.request_id;

-- A malformed/legacy running row may have no surviving request.  It is still
-- ambiguous: absence of request context is not proof that Beets never ran.
UPDATE import_jobs
SET status = 'recovery_required',
    beets_launch_authorized_at = COALESCE(started_at, NOW()),
    worker_id = NULL,
    heartbeat_at = NULL,
    message = 'Recovery required: legacy running job has no launch authority snapshot',
    error = 'Automatic replay refused because the legacy worker may have reached Beets',
    updated_at = NOW()
WHERE status = 'running';

DROP INDEX IF EXISTS idx_import_jobs_active_dedupe;
CREATE UNIQUE INDEX idx_import_jobs_active_dedupe
    ON import_jobs(dedupe_key)
    WHERE dedupe_key IS NOT NULL
      AND status IN ('queued', 'running', 'recovery_required');

DROP INDEX IF EXISTS one_active_youtube_import_per_request;
CREATE UNIQUE INDEX one_active_youtube_import_per_request
    ON import_jobs (request_id)
    WHERE job_type = 'youtube_import'
      AND status IN ('queued', 'running', 'recovery_required');

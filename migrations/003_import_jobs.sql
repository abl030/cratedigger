-- 003_import_jobs.sql — shared importer queue
--
-- Durable queue used by web, CLI, automation, and the importer service to
-- funnel beets-mutating import work through one serial owner.

CREATE TABLE IF NOT EXISTS import_jobs (
    id SERIAL PRIMARY KEY,
    job_type TEXT NOT NULL CHECK (
        job_type IN ('force_import', 'manual_import', 'automation_import')
    ),
    status TEXT NOT NULL DEFAULT 'queued' CHECK (
        status IN ('queued', 'running', 'completed', 'failed')
    ),
    request_id INTEGER REFERENCES album_requests(id) ON DELETE SET NULL,
    dedupe_key TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    result JSONB,
    message TEXT,
    error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    worker_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_import_jobs_active_dedupe
    ON import_jobs(dedupe_key)
    WHERE dedupe_key IS NOT NULL
      AND status IN ('queued', 'running');

CREATE INDEX IF NOT EXISTS idx_import_jobs_claim
    ON import_jobs(status, created_at, id)
    WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS idx_import_jobs_request_recent
    ON import_jobs(request_id, updated_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_import_jobs_recent
    ON import_jobs(updated_at DESC, id DESC);

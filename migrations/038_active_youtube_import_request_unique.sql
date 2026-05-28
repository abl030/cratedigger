-- 038_active_youtube_import_request_unique.sql
--
-- Once yt-dlp has staged a rescue and the worker has created the
-- ``youtube_import`` handoff, the active guard must remain scoped to the
-- album request, not to the selected YouTube browse id. Otherwise a second
-- click for the same request but a different browse id can enqueue a second
-- active import job against the same target request.

CREATE UNIQUE INDEX one_active_youtube_import_per_request
    ON import_jobs (request_id)
    WHERE job_type = 'youtube_import'
      AND status IN ('queued', 'running');

-- 080_local_import_vocabulary.sql
--
-- PR1 of issue #1176 (import-local lane). There is currently no way to say
-- "here is a request ID, here is a folder already on disk, import it" — every
-- import entry point is bound to how the files arrived (a slskd download, a
-- rejection that wrote a ``failed_path``, or a YouTube rescue). This
-- migration adds ONLY the DB vocabulary a future ``import-local`` lane needs;
-- it makes nothing reachable. PR2 (module option + path authority) and PR3
-- (the lane itself, which will write these values) land separately.
--
--   1. ``download_log_source_check`` is widened to admit ``'local'``
--      alongside the existing ``'slskd'`` / ``'youtube'`` discriminators
--      (migration 037). A local import has no slskd transfer and no YT
--      worker behind it, so it needs its own ``source`` value the same way
--      YouTube rescues got one.
--
--   2. ``import_jobs_job_type_check`` is widened to admit ``'local_import'``
--      and simultaneously drops ``'manual_import'``. ``manual_import`` was
--      the job_type for an HTTP endpoint removed as security finding
--      CD-SEC-03 (``docs/security-audit-2026-07-12.md``); ``import_jobs``
--      carries ZERO rows with ``job_type = 'manual_import'`` (verified
--      2026-08-19), so retiring it loses no history. This repo is
--      single-operator, forward-only, no compat shims (``.claude/rules/
--      scope.md``) — there is no reason to keep vocabulary for a job_type no
--      writer has produced since the endpoint that wrote it was deleted.
--      ``download_log_outcome_check`` is DELIBERATELY left untouched: the
--      ``manual_import`` OUTCOME (as opposed to job_type) is still carried
--      by 7 live ``download_log`` audit rows from April 2026 and remains
--      valid vocabulary for that column — this migration only retires the
--      job_type value, never the outcome value.
--
--   3. Partial unique index ``one_active_local_import_per_request`` mirrors
--      ``one_active_youtube_import_per_request`` (migration 060): at most
--      one active (``queued``/``running``/``recovery_required``)
--      ``local_import`` job per request at a time. A local import has no
--      originating ``download_log`` row to dedupe against (unlike
--      ``force_import``/``youtube_import``, which key their dedupe string on
--      a download_log id) — the request itself is the only natural grain,
--      so the same per-request exclusivity the YouTube lane gets at the DB
--      layer applies here too.

ALTER TABLE download_log DROP CONSTRAINT IF EXISTS download_log_source_check;
ALTER TABLE download_log ADD CONSTRAINT download_log_source_check
    CHECK (source IN ('slskd', 'youtube', 'local'));

ALTER TABLE import_jobs DROP CONSTRAINT IF EXISTS import_jobs_job_type_check;
ALTER TABLE import_jobs ADD CONSTRAINT import_jobs_job_type_check
    CHECK (job_type IN (
        'force_import', 'automation_import', 'youtube_import', 'local_import'
    ));

DROP INDEX IF EXISTS one_active_local_import_per_request;
CREATE UNIQUE INDEX one_active_local_import_per_request
    ON import_jobs (request_id)
    WHERE job_type = 'local_import'
      AND status IN ('queued', 'running', 'recovery_required');

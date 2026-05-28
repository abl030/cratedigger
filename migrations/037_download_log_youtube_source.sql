-- 037_download_log_youtube_source.sql
--
-- U1 of the YouTube rescue ingest API (see
-- ``docs/plans/2026-05-28-001-feat-youtube-rescue-ingest-api-plan.md``).
--
-- Extends ``download_log`` so it can double as the queue + audit trail for
-- YT rescue submissions alongside its existing slskd role:
--
--   1. ``source`` discriminator column distinguishes sourcing channels
--      (``'slskd'`` default, ``'youtube'`` for new rows). The DEFAULT
--      backfills every pre-037 row to ``'slskd'`` in this single ALTER
--      (per the single-operator no-backfill-script rule in CLAUDE.md).
--
--   2. ``youtube_metadata`` JSONB carries the YT-specific audit payload
--      (typed ``YoutubeIngestMetadata: msgspec.Struct`` at the read seam).
--      Nullable — only populated for ``source='youtube'`` rows.
--
--   3. Partial unique index ``one_youtube_running_per_request`` enforces
--      R4 idempotency at the DB layer: at most one ``youtube_running`` row
--      per ``request_id`` at any time. Once the row transitions to
--      ``youtube_success`` or ``youtube_failed`` the index admits the next
--      submission. Application-level pre-insert checks would have a race
--      window between read and insert; a partial unique index is atomic.
--
--   4. Three new outcome values widen the ``download_log_outcome_check``
--      CHECK constraint: ``youtube_running`` (in-flight), ``youtube_success``
--      (worker completed), ``youtube_failed`` (any failure). The plan's
--      "no broader retrofit" guidance applies to slskd-side outcomes; the
--      three YT outcomes have to land in the constraint or the partial
--      unique index test (and every YT INSERT) would be unreachable.
--      Mirrors the pattern from ``019_preview_evidence_facts.sql:31-35``.

ALTER TABLE download_log
    ADD COLUMN source TEXT NOT NULL DEFAULT 'slskd'
        CHECK (source IN ('slskd', 'youtube')),
    ADD COLUMN youtube_metadata JSONB;

ALTER TABLE download_log DROP CONSTRAINT IF EXISTS download_log_outcome_check;
ALTER TABLE download_log ADD CONSTRAINT download_log_outcome_check
    CHECK (outcome IN ('success', 'rejected', 'failed', 'timeout',
                       'force_import', 'manual_import', 'curator_ban',
                       'measurement_failed',
                       'youtube_running', 'youtube_success', 'youtube_failed'));

CREATE UNIQUE INDEX one_youtube_running_per_request
    ON download_log (request_id)
    WHERE source = 'youtube' AND outcome = 'youtube_running';

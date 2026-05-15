-- 019_preview_evidence_facts.sql - new evidence facts for preview never decides
--
-- Adds the typed columns that the new pure measurement helper must persist
-- to ``album_quality_evidence`` and ``album_quality_evidence_files`` so the
-- pure decision function (``preimport_decide``) can read facts, not strings.
-- Also widens two CHECK constraints so the new ``measurement_failed`` terminal
-- can flow through ``download_log.outcome`` and ``import_jobs.preview_status``.
--
-- Defaults are conservative and chosen so the downstream Struct fields stay
-- non-Optional (per the U3 plan). Legacy evidence rows decoded into the
-- extended Struct shape pick up these defaults; the decision function reads
-- them as facts (e.g. ``folder_layout='flat'`` + ``audio_file_count=0`` is the
-- explicit "empty inventory" signal that AE4 requires).

ALTER TABLE album_quality_evidence
    ADD COLUMN audio_corrupt BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN folder_layout TEXT NOT NULL DEFAULT 'flat'
        CHECK (folder_layout IN ('flat', 'nested')),
    ADD COLUMN audio_file_count INTEGER NOT NULL DEFAULT 0
        CHECK (audio_file_count >= 0),
    ADD COLUMN filetype_band TEXT NOT NULL DEFAULT '',
    ADD COLUMN matched_bad_audio_hash_id BIGINT
        REFERENCES bad_audio_hashes(id) ON DELETE SET NULL,
    ADD COLUMN matched_bad_audio_hash_path TEXT;

ALTER TABLE album_quality_evidence_files
    ADD COLUMN decode_ok BOOLEAN NOT NULL DEFAULT TRUE;

-- Extend download_log.outcome CHECK with the new 'measurement_failed' terminal.
-- Mirrors migrations/009_curator_ban_outcome.sql:11-12.
ALTER TABLE download_log DROP CONSTRAINT IF EXISTS download_log_outcome_check;
ALTER TABLE download_log ADD CONSTRAINT download_log_outcome_check
    CHECK (outcome IN ('success', 'rejected', 'failed', 'timeout',
                       'force_import', 'manual_import', 'curator_ban',
                       'measurement_failed'));

-- Extend import_jobs.preview_status CHECK with the new 'measurement_failed'
-- value. Mirrors migrations/018_neutral_import_job_preview_ready.sql:13-28.
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
                'measurement_failed',
                'error'
            )
        );

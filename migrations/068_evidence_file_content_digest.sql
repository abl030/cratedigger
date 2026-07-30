-- 068_evidence_file_content_digest.sql
--
-- Evidence snapshot reuse must prove exact bytes, not only path and size.
-- Historical evidence cannot be safely backfilled from database state: its
-- recorded source path may have changed since measurement. Leave those rows
-- NULL so the action-time snapshot guard rejects reuse and the normal preview
-- path rebuilds evidence from the current bytes.

ALTER TABLE album_quality_evidence_files
    ADD COLUMN content_sha256 TEXT
        CHECK (
            content_sha256 IS NULL
            OR content_sha256 ~ '^[0-9a-f]{64}$'
        );

COMMENT ON COLUMN album_quality_evidence_files.content_sha256 IS
    'SHA-256 of the exact file bytes; NULL historical rows are never reusable';

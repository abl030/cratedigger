-- 085_drop_evidence_file_content_digest.sql
--
-- Migration 068 added album_quality_evidence_files.content_sha256 to prove
-- exact bytes before an evidence snapshot could be reused. The action-time
-- snapshot guard it was meant to feed was never built: no Python reads or
-- writes the column, and 068 itself deliberately backfilled nothing. The
-- whole live population is 60 non-null values against 338,834 NULLs
-- (measured 2026-09-09); no committed code path can produce them. Every
-- reader of this table names its columns explicitly
-- (lib/pipeline_db/evidence.py, scripts/decision_differential.py), so
-- nothing selects it even implicitly.
--
-- Forward-only removal, issue #1378 item 4. A future byte-level reuse guard
-- adds its own column together with the writer that fills it.

ALTER TABLE album_quality_evidence_files
    DROP COLUMN content_sha256;

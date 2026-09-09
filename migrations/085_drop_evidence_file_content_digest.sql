-- 085_drop_evidence_file_content_digest.sql
--
-- Migration 068 added album_quality_evidence_files.content_sha256 to prove
-- exact bytes before an evidence snapshot could be reused. Nothing in the
-- current tree reads or writes it. The 60 non-null values against 338,834
-- NULLs (measured 2026-09-09) are residue of the reverted "Reuse exact HAVE
-- evidence during preview" work, the only revision that ever wrote the
-- column: they cover 6 evidence rows measured 07:32-07:58 on 2026-07-31
-- (+0800), spanning that revision's commit (5e05e5e7, 07:17) and continuing
-- past its revert (13ed134f, 07:50) by the usual deploy lag -- 48 of the 60
-- were written after the revert was authored.
--
-- No read site of this table selects the column even implicitly: every one
-- names its columns, and there is no SELECT * against the table anywhere.
-- The production readers are lib/pipeline_db/evidence.py and
-- lib/pipeline_db/download_log.py; scripts/decision_differential.py reads it
-- too, and its own fail-closed schema contract never listed the column.
--
-- Forward-only removal, issue #1378 item 4. A future byte-level reuse guard
-- adds its own column together with the writer that fills it.

ALTER TABLE album_quality_evidence_files
    DROP COLUMN content_sha256;

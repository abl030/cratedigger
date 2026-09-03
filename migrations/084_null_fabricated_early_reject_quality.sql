-- 084_null_fabricated_early_reject_quality.sql - stop persisting fabricated
-- MP3/0-kbps quality on early-reject candidate evidence (issue #1355 item 2)
--
-- lib/quality_evidence.py::evidence_from_measurement used to fabricate
-- format='MP3' and min/avg/median_bitrate_kbps=0 on a candidate the
-- harness never measured (audio_corrupt / bad_audio_hash / nested_layout /
-- empty_fileset), purely to satisfy AlbumQualityEvidence.
-- policy_incomplete_reasons()'s pre-#1355 readiness gate. That gate no
-- longer requires a quality measurement on a row already carrying one of
-- those facts, so the producer now leaves the fields honestly NULL going
-- forward (schema unchanged: these columns were already nullable). This
-- migration corrects the rows the old code already wrote.
--
-- A genuine ffprobe measurement can never read exactly 0 kbps, so
-- min_bitrate_kbps = avg_bitrate_kbps = median_bitrate_kbps = 0 on a row
-- carrying one of the four reject facts is unambiguous evidence of this
-- fabrication, not a coincidence. lineage_version < 4 predates the reject
-- facts entirely (migration 019 added them as columns with DDL defaults);
-- a pre-019 row shows those defaults rather than an observed fact, so it
-- is excluded rather than rewritten.
--
-- Measured on doc2, 2026-09-03: exactly 28 rows match the bitrate
-- condition below, all candidate evidence (linked from download_log or
-- import_jobs, never album_requests.current_evidence_id), all
-- audio_corrupt = TRUE — the only one of the four facts with a live row
-- today. Zero rows match the format condition: that fabrication only
-- fires on empty_fileset, and no empty_fileset row exists at
-- lineage_version >= 4 in the live table. Both UPDATEs are kept for a
-- complete fix; the second is a documented no-op against today's data.

UPDATE album_quality_evidence
SET min_bitrate_kbps = NULL,
    avg_bitrate_kbps = NULL,
    median_bitrate_kbps = NULL
WHERE lineage_version >= 4
  AND min_bitrate_kbps = 0
  AND avg_bitrate_kbps = 0
  AND median_bitrate_kbps = 0
  AND (
    audio_corrupt = TRUE
    OR matched_bad_audio_hash_id IS NOT NULL
    OR folder_layout = 'nested'
    OR audio_file_count = 0
  );

UPDATE album_quality_evidence
SET format = NULL,
    storage_format = NULL
WHERE lineage_version >= 4
  AND audio_file_count = 0
  AND format IS NOT NULL;

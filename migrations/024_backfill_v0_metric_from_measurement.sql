-- 024_backfill_v0_metric_from_measurement.sql
--
-- Before this migration, album_quality_evidence.v0_* fields were only
-- populated when the candidate came from a comparable lossless container.
-- Non-lossless candidates (MP3 V0, CBR 320, Opus, ...) had v0_metric=NULL,
-- which made every audit row and UI download-history entry blank in the
-- "V0 probe" column.
--
-- The producer (lib/quality_evidence.py::evidence_from_import_result) now
-- falls back to ``neutral_v0_metric_from_measurement`` when no
-- lossless-source probe exists. New rows are populated correctly. This
-- migration backfills the legacy NULL rows from the same source
-- (``download_log.import_result``'s ``new_measurement.{min,avg,median}_bitrate_kbps``)
-- so the audit/UI surface looks uniform across history.
--
-- Forward-only: this is a one-shot data fix. The producer change makes
-- future writes unnecessary, but we intentionally do not add a runtime
-- compat layer for legacy NULLs — backfill once, then both sides assume
-- one shape.

WITH evidence_measurements AS (
    SELECT
        dl.candidate_evidence_id                                                AS evidence_id,
        ((dl.import_result -> 'new_measurement') ->> 'min_bitrate_kbps')::int   AS min_kbps,
        ((dl.import_result -> 'new_measurement') ->> 'avg_bitrate_kbps')::int   AS avg_kbps,
        ((dl.import_result -> 'new_measurement') ->> 'median_bitrate_kbps')::int AS median_kbps,
        ROW_NUMBER() OVER (
            PARTITION BY dl.candidate_evidence_id
            ORDER BY dl.id DESC
        ) AS rn
    FROM download_log dl
    WHERE dl.candidate_evidence_id IS NOT NULL
      AND dl.import_result IS NOT NULL
      AND dl.import_result -> 'new_measurement' IS NOT NULL
)
UPDATE album_quality_evidence e
SET v0_min_bitrate_kbps    = m.min_kbps,
    v0_avg_bitrate_kbps    = m.avg_kbps,
    v0_median_bitrate_kbps = m.median_kbps,
    v0_source_lineage      = 'native_lossy_research',
    v0_source_provenance   = 'new_measurement_fallback',
    updated_at             = NOW()
FROM evidence_measurements m
WHERE e.id = m.evidence_id
  AND m.rn = 1
  -- Only touch rows that have NO v0_metric at all. The shape CHECK
  -- constraint (album_quality_evidence_v0_metric_shape) forbids partial
  -- writes, and rows that already have a probe were correct under the
  -- old policy too.
  AND e.v0_min_bitrate_kbps    IS NULL
  AND e.v0_avg_bitrate_kbps    IS NULL
  AND e.v0_median_bitrate_kbps IS NULL
  AND e.v0_source_lineage      IS NULL
  AND e.v0_source_provenance   IS NULL
  AND e.v0_proof_provenance    IS NULL
  -- At least one bitrate measurement must be present, per the same shape
  -- CHECK constraint.
  AND (m.min_kbps IS NOT NULL OR m.avg_kbps IS NOT NULL OR m.median_kbps IS NOT NULL);

-- Backfill download_log.v0_probe_* columns directly. The read path's
-- evidence overlay (lib/pipeline_db.py::_overlay_evidence_onto_download_log_row)
-- already fills v0_probe_kind + v0_probe_avg_bitrate from the evidence
-- join, but min/median are not overlaid — and a probe-less rejected row
-- whose evidence_id is NULL is the more common shape we want to fill.
-- This UPDATE pulls directly from the row's own JSONB so it works for
-- rows that never had a candidate_evidence_id at all (pre-rekey audit
-- history).
UPDATE download_log dl
SET v0_probe_kind            = 'native_lossy_research_v0',
    v0_probe_min_bitrate     = ((dl.import_result -> 'new_measurement') ->> 'min_bitrate_kbps')::int,
    v0_probe_avg_bitrate     = ((dl.import_result -> 'new_measurement') ->> 'avg_bitrate_kbps')::int,
    v0_probe_median_bitrate  = ((dl.import_result -> 'new_measurement') ->> 'median_bitrate_kbps')::int
WHERE dl.v0_probe_kind IS NULL
  AND dl.import_result IS NOT NULL
  AND dl.import_result -> 'new_measurement' IS NOT NULL
  AND (
        ((dl.import_result -> 'new_measurement') ->> 'min_bitrate_kbps')    IS NOT NULL
     OR ((dl.import_result -> 'new_measurement') ->> 'avg_bitrate_kbps')    IS NOT NULL
     OR ((dl.import_result -> 'new_measurement') ->> 'median_bitrate_kbps') IS NOT NULL
  );

-- Force/manual import audit rows retain the exact validation row they came
-- from. The self-FK is the durable lineage boundary: readers join this row
-- for the original beets distance instead of guessing by request/time.
ALTER TABLE download_log
    ADD COLUMN source_download_log_id BIGINT
        REFERENCES download_log(id);

-- Preview-owned neutral V0 research is attempted at most once per immutable
-- evidence snapshot, including when the research probe fails or returns no
-- metric. Policy code does not consume this marker.
ALTER TABLE album_quality_evidence
    ADD COLUMN on_disk_v0_research_attempted BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE download_log
    ADD CONSTRAINT download_log_source_not_self
        CHECK (source_download_log_id IS NULL OR source_download_log_id <> id);

CREATE INDEX idx_download_log_source_download_log_id
    ON download_log(source_download_log_id)
    WHERE source_download_log_id IS NOT NULL;

-- Recover only historical force-import lineage that the old terminal bundle
-- already proves exactly. PostgreSQL's transaction-stable NOW() made the
-- terminal download_log.created_at and import_jobs.completed_at identical.
-- The payload and recorded wrong-match dismissal must independently name
-- the same existing source row on the same request. Window counts reject any
-- output/job/source ambiguity instead of selecting a nearby row.
WITH exact_force_candidates AS (
    SELECT
        output.id AS output_id,
        job.id AS job_id,
        source.id AS source_id,
        COUNT(*) OVER (PARTITION BY output.id) AS output_candidate_count,
        COUNT(*) OVER (PARTITION BY job.id) AS job_candidate_count,
        COUNT(*) OVER (PARTITION BY source.id) AS source_candidate_count
    FROM download_log output
    JOIN import_jobs job
      ON job.request_id = output.request_id
     AND job.job_type = 'force_import'
     AND job.status = 'completed'
     AND job.completed_at = output.created_at
    JOIN download_log source
      ON source.request_id = output.request_id
     AND source.id = CASE
         WHEN job.payload ->> 'download_log_id' ~ '^[0-9]+$'
         THEN (job.payload ->> 'download_log_id')::BIGINT
         ELSE NULL
     END
    WHERE output.outcome = 'force_import'
      AND output.source_download_log_id IS NULL
      AND CASE
          WHEN job.result #>> '{wrong_match_dismissal,download_log_id}'
              ~ '^[0-9]+$'
          THEN (
              job.result #>> '{wrong_match_dismissal,download_log_id}'
          )::BIGINT
          ELSE NULL
      END = source.id
), unique_force_candidates AS (
    SELECT output_id, source_id
    FROM exact_force_candidates
    WHERE output_candidate_count = 1
      AND job_candidate_count = 1
      AND source_candidate_count = 1
)
UPDATE download_log output
SET source_download_log_id = candidate.source_id
FROM unique_force_candidates candidate
WHERE output.id = candidate.output_id;

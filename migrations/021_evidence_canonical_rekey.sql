-- 021_evidence_canonical_rekey.sql — rekey album_quality_evidence by content
--
-- Re-key ``album_quality_evidence`` from ``(owner_type, owner_id)`` to
-- ``(mb_release_id, snapshot_fingerprint)`` and add addressing FK columns
-- on ``import_jobs``, ``download_log``, and ``album_requests``.
--
-- The fingerprint is computed in-SQL from each row's existing
-- ``album_quality_evidence_files`` records using the canonical formula
-- documented in ``lib/quality_evidence.py::snapshot_fingerprint``:
--
--   SHA-256 over the JSON text "[" + comma-join over files (sorted by
--   relative_path) of "[relative_path,size_bytes,extension,container,codec]"
--   + "]", where each JSON value uses PG's compact ``to_jsonb()::text``
--   encoding (no whitespace), NULL codec renders as the JSON literal ``null``.
--
-- This single atomic transaction:
--   1. Enables pgcrypto for ``digest()``.
--   2. Adds the new columns (nullable initially so backfill can populate).
--   3. Backfills ``mb_release_id`` and ``source_path`` per old owner_type.
--   4. Backfills ``snapshot_fingerprint`` from the files table.
--   5. Deletes orphan rows that resolve to NULL mb_release_id (logged).
--   6. Dedupes rows that collapse under (mb_release_id, snapshot_fingerprint)
--      keeping the most recent ``measured_at``.
--   7. Backfills the three addressing FKs (with cross-walk for download_log).
--   8. Tightens constraints + drops the old owner_type/owner_id keying.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1) Add new columns (nullable for now)
ALTER TABLE album_quality_evidence
    ADD COLUMN mb_release_id TEXT,
    ADD COLUMN snapshot_fingerprint TEXT,
    ADD COLUMN source_path TEXT;

ALTER TABLE import_jobs
    ADD COLUMN candidate_evidence_id BIGINT
        REFERENCES album_quality_evidence(id) ON DELETE SET NULL;

ALTER TABLE download_log
    ADD COLUMN candidate_evidence_id BIGINT
        REFERENCES album_quality_evidence(id) ON DELETE SET NULL;

ALTER TABLE album_requests
    ADD COLUMN current_evidence_id BIGINT
        REFERENCES album_quality_evidence(id) ON DELETE SET NULL;

-- 2) Backfill mb_release_id + source_path per old owner_type by JOIN.
--    import_job_candidate: mb_release_id from the request the job points at;
--                          source_path from payload->>'failed_path'.
UPDATE album_quality_evidence AS e
SET mb_release_id = ar.mb_release_id,
    source_path = COALESCE(j.payload->>'failed_path', '')
FROM import_jobs AS j
LEFT JOIN album_requests AS ar ON ar.id = j.request_id
WHERE e.owner_type = 'import_job_candidate'
  AND e.owner_id = j.id;

--    download_log_candidate: mb_release_id from request; source_path from
--                            download_log.staged_path.
UPDATE album_quality_evidence AS e
SET mb_release_id = ar.mb_release_id,
    source_path = COALESCE(dl.staged_path, '')
FROM download_log AS dl
LEFT JOIN album_requests AS ar ON ar.id = dl.request_id
WHERE e.owner_type = 'download_log_candidate'
  AND e.owner_id = dl.id;

--    request_current: mb_release_id direct; source_path from imported_path
--                     (the beets library path), or empty when not set.
UPDATE album_quality_evidence AS e
SET mb_release_id = ar.mb_release_id,
    source_path = COALESCE(ar.imported_path, '')
FROM album_requests AS ar
WHERE e.owner_type = 'request_current'
  AND e.owner_id = ar.id;

-- 3) Backfill snapshot_fingerprint from the files table. Build the canonical
--    JSON text inline using to_jsonb() per value (compact, no whitespace),
--    NULL codec rendered as the literal 'null' to match Python's
--    json.dumps(None) output.
UPDATE album_quality_evidence AS e
SET snapshot_fingerprint = encode(digest(payload.json_text, 'sha256'), 'hex')
FROM (
    SELECT
        f.evidence_id,
        '[' || string_agg(
            '[' || to_jsonb(f.relative_path)::text || ','
                 || to_jsonb(f.size_bytes)::text || ','
                 || to_jsonb(f.extension)::text || ','
                 || to_jsonb(f.container)::text || ','
                 || COALESCE(to_jsonb(f.codec)::text, 'null')
                 || ']',
            ',' ORDER BY f.relative_path
        ) || ']' AS json_text
    FROM album_quality_evidence_files AS f
    GROUP BY f.evidence_id
) AS payload
WHERE e.id = payload.evidence_id;

--    Rows with no files row collapse to the empty-inventory fingerprint:
--    sha256("[]") = 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945
UPDATE album_quality_evidence AS e
SET snapshot_fingerprint = encode(digest('[]', 'sha256'), 'hex')
WHERE snapshot_fingerprint IS NULL;

-- 4) Drop orphans: rows whose old owner does not resolve to a release.
--    These are legacy rows that never had a release to address. Logged in
--    a NOTICE so the deploy log captures the count.
DO $$
DECLARE
    orphan_count INT;
BEGIN
    SELECT COUNT(*) INTO orphan_count
    FROM album_quality_evidence
    WHERE mb_release_id IS NULL OR length(mb_release_id) = 0;
    IF orphan_count > 0 THEN
        RAISE NOTICE 'evidence-rekey: deleting % orphan evidence row(s) with no resolvable mb_release_id', orphan_count;
    END IF;
END
$$;

DELETE FROM album_quality_evidence
WHERE mb_release_id IS NULL OR length(mb_release_id) = 0;

-- 5) Dedupe: collapse rows that share (mb_release_id, snapshot_fingerprint).
--    Keep the most recent measured_at. Remember the mapping so addressing-FK
--    backfill below points old owner ids at the survivor's id.
CREATE TEMPORARY TABLE _evidence_dedupe AS
SELECT
    e.id AS old_id,
    e.owner_type,
    e.owner_id,
    FIRST_VALUE(e.id) OVER (
        PARTITION BY e.mb_release_id, e.snapshot_fingerprint
        ORDER BY e.measured_at DESC, e.id DESC
    ) AS survivor_id
FROM album_quality_evidence AS e;

-- Delete losers; their addressing references will be rewritten to the
-- survivor in step 6.
DELETE FROM album_quality_evidence AS e
USING _evidence_dedupe AS d
WHERE e.id = d.old_id
  AND d.old_id <> d.survivor_id;

-- 6) Backfill addressing FKs from the old owner_type/owner_id pairs,
--    routed through the dedupe table so dropped duplicates redirect to
--    their surviving canonical row.
UPDATE import_jobs AS j
SET candidate_evidence_id = d.survivor_id
FROM _evidence_dedupe AS d
WHERE d.owner_type = 'import_job_candidate'
  AND d.owner_id = j.id;

UPDATE download_log AS dl
SET candidate_evidence_id = d.survivor_id
FROM _evidence_dedupe AS d
WHERE d.owner_type = 'download_log_candidate'
  AND d.owner_id = dl.id;

UPDATE album_requests AS ar
SET current_evidence_id = d.survivor_id
FROM _evidence_dedupe AS d
WHERE d.owner_type = 'request_current'
  AND d.owner_id = ar.id;

-- 6b) Cross-walk: download_log rows with no direct ownership get the FK
--     from their sibling import_job (most recent with non-NULL FK for
--     the same request).
UPDATE download_log AS dl
SET candidate_evidence_id = sibling.candidate_evidence_id
FROM (
    SELECT DISTINCT ON (j.request_id)
        j.request_id,
        j.candidate_evidence_id
    FROM import_jobs AS j
    WHERE j.candidate_evidence_id IS NOT NULL
    ORDER BY j.request_id, j.created_at DESC, j.id DESC
) AS sibling
WHERE dl.candidate_evidence_id IS NULL
  AND dl.request_id = sibling.request_id;

DROP TABLE _evidence_dedupe;

-- 7) Tighten constraints, then drop the old keying.
ALTER TABLE album_quality_evidence
    ALTER COLUMN mb_release_id SET NOT NULL,
    ALTER COLUMN snapshot_fingerprint SET NOT NULL,
    ALTER COLUMN source_path SET NOT NULL,
    ADD CONSTRAINT album_quality_evidence_mb_release_id_nonempty
        CHECK (length(mb_release_id) > 0);

DROP INDEX idx_album_quality_evidence_owner;

ALTER TABLE album_quality_evidence
    DROP CONSTRAINT album_quality_evidence_one_per_owner;

ALTER TABLE album_quality_evidence
    DROP COLUMN owner_type,
    DROP COLUMN owner_id;

ALTER TABLE album_quality_evidence
    ADD CONSTRAINT album_quality_evidence_release_fingerprint_unique
        UNIQUE (mb_release_id, snapshot_fingerprint);

CREATE INDEX idx_album_quality_evidence_release
    ON album_quality_evidence(mb_release_id);

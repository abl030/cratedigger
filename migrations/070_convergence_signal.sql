-- 070_convergence_signal.sql -- direct candidate attribution and derivation
--
-- Migration 021 deliberately cross-walked legacy download_log rows to a
-- sibling import job's evidence.  Those links are useful for historical
-- rendering but cannot prove that the evidence describes this attempt's
-- bytes.  Convergence admits only positively attributed terminal links.

ALTER TABLE download_log
    ADD COLUMN candidate_evidence_direct BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN candidate_contributor_usernames TEXT[];

COMMENT ON COLUMN download_log.candidate_evidence_direct IS
    'True only when the terminal writer positively linked this exact attempt to its producing import job evidence. Migration-021 sibling cross-walks remain false.';

COMMENT ON COLUMN download_log.candidate_contributor_usernames IS
    'Normalized structured Soulseek contributor identities for this exact attempt. Ambiguous legacy comma-joined display text is excluded, never split.';

-- Keep the one-time attribution join bounded to already-linked rows. These
-- small partial indexes also support later forensic checks of the positive
-- provenance bit without scanning the multi-million-row history table.
CREATE INDEX idx_download_log_candidate_evidence_attribution
    ON download_log (
        request_id, candidate_evidence_id, created_at, id
    )
    WHERE candidate_evidence_id IS NOT NULL;

CREATE INDEX idx_import_jobs_candidate_evidence_attribution
    ON import_jobs (
        request_id, candidate_evidence_id, completed_at, id
    )
    WHERE candidate_evidence_id IS NOT NULL
      AND completed_at IS NOT NULL;

-- The pre-070 field is presentation text, not a machine-readable peer set.
-- Preserve only unambiguous single-value historical display text. Any comma
-- may be a username character or a join separator, so those rows remain NULL.
-- This under-counts history but cannot manufacture extra peers.
UPDATE download_log
SET candidate_contributor_usernames = ARRAY[
    LOWER(BTRIM(soulseek_username))
]
WHERE source = 'slskd'
  AND NULLIF(BTRIM(soulseek_username), '') IS NOT NULL
  AND POSITION(',' IN soulseek_username) = 0;

-- Historical terminal rows can be admitted only when transaction-stable
-- timestamps, request identity, and candidate evidence identify one exact
-- job/log pair in both directions.  Ambiguity stays false (fail closed).
WITH candidates AS (
    SELECT
        dl.id AS download_log_id,
        job.id AS import_job_id,
        COUNT(*) OVER (PARTITION BY dl.id) AS jobs_for_log,
        COUNT(*) OVER (PARTITION BY job.id) AS logs_for_job
    FROM download_log dl
    JOIN import_jobs job
      ON job.request_id = dl.request_id
     AND job.candidate_evidence_id = dl.candidate_evidence_id
     AND job.completed_at = dl.created_at
    WHERE dl.candidate_evidence_id IS NOT NULL
      AND job.completed_at IS NOT NULL
), unique_candidates AS (
    SELECT candidate.download_log_id
    FROM candidates candidate
    JOIN download_log dl ON dl.id = candidate.download_log_id
    WHERE candidate.jobs_for_log = 1
      AND candidate.logs_for_job = 1
      AND CARDINALITY(dl.candidate_contributor_usernames) > 0
)
UPDATE download_log dl
SET candidate_evidence_direct = TRUE
FROM unique_candidates candidate
WHERE dl.id = candidate.download_log_id;

ALTER TABLE download_log
    ADD CONSTRAINT download_log_candidate_evidence_direct_requires_link
    CHECK (
        candidate_evidence_direct IS FALSE
        OR (
            candidate_evidence_id IS NOT NULL
            AND COALESCE(
                CARDINALITY(candidate_contributor_usernames), 0
            ) > 0
        )
    );

-- Every convergence read is request-local.  The partial index excludes
-- world errors, non-exact/high-distance candidates, legacy cross-walks, and
-- unrelated sources before joining evidence.  INCLUDE covers the remaining
-- attempt facts used by the derivation.
CREATE INDEX idx_download_log_convergence_candidates
    ON download_log (request_id, created_at DESC, id DESC)
    INCLUDE (
        candidate_evidence_id, candidate_contributor_usernames, filetype
    )
    WHERE candidate_evidence_direct IS TRUE
      AND source = 'slskd'
      AND outcome IN ('success', 'rejected')
      AND beets_scenario = 'strong_match'
      AND beets_distance <= 0.15;

-- One request-local, read-only derivation is shared by list, detail, and the
-- operator stop CAS.  It returns no row unless the current holding is the
-- canonical provisional lossless-source world and at least five qualifying
-- candidate observations from five atomic Soulseek peers agree within the
-- newest consecutive 500-Hz band.
CREATE FUNCTION derive_request_convergence_signal(target_request_id BIGINT)
RETURNS TABLE (
    request_id BIGINT,
    authority_current_evidence_id BIGINT,
    observation_count INTEGER,
    distinct_peer_count INTEGER,
    distinct_candidate_snapshot_count INTEGER,
    distinct_codec_count INTEGER,
    cliff_hz INTEGER,
    raw_cliff_min_hz INTEGER,
    raw_cliff_max_hz INTEGER,
    cliff_spread_hz INTEGER,
    latest_qualifying_log_id BIGINT,
    first_observed_at TIMESTAMPTZ,
    latest_observed_at TIMESTAMPTZ,
    signal_token TEXT
)
LANGUAGE SQL
STABLE
PARALLEL SAFE
ROWS 1
AS $function$
WITH request_authority AS MATERIALIZED (
    SELECT
        request.id,
        current_evidence.id AS current_evidence_id
    FROM album_requests request
    JOIN album_quality_evidence current_evidence
      ON current_evidence.id = request.current_evidence_id
    WHERE request.id = target_request_id
      AND request.status <> 'replaced'
      AND current_evidence.v0_subject = 'source'
      AND current_evidence.verified_lossless IS FALSE
), eligible AS MATERIALIZED (
    SELECT
        dl.request_id,
        request.current_evidence_id,
        dl.id AS log_id,
        dl.candidate_contributor_usernames AS contributor_usernames,
        dl.candidate_evidence_id,
        evidence.snapshot_fingerprint,
        COALESCE(
            NULLIF(LOWER(BTRIM(evidence.codec)), ''),
            NULLIF(LOWER(BTRIM(evidence.format)), ''),
            'unknown'
        ) AS codec,
        evidence.cliff_hz AS raw_cliff_hz,
        CASE
            WHEN evidence.cliff_hz IS NULL THEN NULL
            ELSE (ROUND(evidence.cliff_hz / 500.0) * 500)::INTEGER
        END AS cliff_bin_hz,
        dl.created_at AS observed_at
    FROM request_authority request
    JOIN download_log dl
      ON dl.request_id = request.id
     AND dl.candidate_evidence_direct IS TRUE
     AND dl.source = 'slskd'
     AND dl.outcome IN ('success', 'rejected')
     AND CARDINALITY(dl.candidate_contributor_usernames) > 0
     AND dl.beets_scenario = 'strong_match'
     AND dl.beets_distance <= 0.15
    JOIN album_quality_evidence evidence
      ON evidence.id = dl.candidate_evidence_id
    WHERE evidence.verified_lossless IS FALSE
      AND evidence.codec_family = 'lossless'
      AND evidence.spectral_subject = 'source'
      AND evidence.spectral_measurement_version = 2
), ordered AS (
    SELECT
        eligible.*,
        ROW_NUMBER() OVER (
            ORDER BY observed_at DESC, log_id DESC
        ) AS reverse_ordinal,
        FIRST_VALUE(cliff_bin_hz) OVER (
            ORDER BY observed_at DESC, log_id DESC
        ) AS latest_cliff_bin_hz
    FROM eligible
), bounded AS (
    SELECT
        ordered.*,
        MIN(reverse_ordinal) FILTER (
            WHERE cliff_bin_hz IS DISTINCT FROM latest_cliff_bin_hz
        ) OVER () AS first_break_ordinal
    FROM ordered
), current_run AS MATERIALIZED (
    SELECT *
    FROM bounded
    WHERE latest_cliff_bin_hz IS NOT NULL
      AND reverse_ordinal < COALESCE(first_break_ordinal, 2147483647)
), attempt_summary AS (
    SELECT
        MIN(request_id) AS request_id,
        MIN(current_evidence_id) AS current_evidence_id,
        COUNT(*)::INTEGER AS observation_count,
        COUNT(DISTINCT snapshot_fingerprint)::INTEGER
            AS distinct_candidate_snapshot_count,
        COUNT(DISTINCT codec)::INTEGER AS distinct_codec_count,
        MIN(latest_cliff_bin_hz)::INTEGER AS cliff_hz,
        MIN(raw_cliff_hz)::INTEGER AS raw_cliff_min_hz,
        MAX(raw_cliff_hz)::INTEGER AS raw_cliff_max_hz,
        (MAX(raw_cliff_hz) - MIN(raw_cliff_hz))::INTEGER AS cliff_spread_hz,
        (ARRAY_AGG(log_id ORDER BY observed_at DESC, log_id DESC))[1]
            AS latest_qualifying_log_id,
        MIN(observed_at) AS first_observed_at,
        MAX(observed_at) AS latest_observed_at,
        STRING_AGG(
            JSONB_BUILD_ARRAY(
                log_id,
                candidate_evidence_id,
                contributor_usernames,
                snapshot_fingerprint,
                codec,
                raw_cliff_hz,
                observed_at
            )::TEXT,
            CHR(30)
            ORDER BY observed_at DESC, log_id DESC
        ) AS qualifying_identity
    FROM current_run
), peer_summary AS (
    SELECT COUNT(DISTINCT LOWER(BTRIM(peer.username)))::INTEGER
        AS distinct_peer_count
    FROM current_run attempt
    CROSS JOIN LATERAL UNNEST(
        attempt.contributor_usernames
    ) AS peer(username)
    WHERE NULLIF(BTRIM(peer.username), '') IS NOT NULL
), signal_facts AS (
    SELECT attempt.*, peer.distinct_peer_count
    FROM attempt_summary attempt
    CROSS JOIN peer_summary peer
    WHERE attempt.observation_count >= 5
      AND peer.distinct_peer_count >= 5
)
SELECT
    facts.request_id,
    facts.current_evidence_id AS authority_current_evidence_id,
    facts.observation_count,
    facts.distinct_peer_count,
    facts.distinct_candidate_snapshot_count,
    facts.distinct_codec_count,
    facts.cliff_hz,
    facts.raw_cliff_min_hz,
    facts.raw_cliff_max_hz,
    facts.cliff_spread_hz,
    facts.latest_qualifying_log_id,
    facts.first_observed_at,
    facts.latest_observed_at,
    ENCODE(DIGEST(
        JSONB_BUILD_OBJECT(
            'request_id', facts.request_id,
            'current_evidence_id', facts.current_evidence_id,
            'observation_count', facts.observation_count,
            'distinct_peer_count', facts.distinct_peer_count,
            'distinct_candidate_snapshot_count',
                facts.distinct_candidate_snapshot_count,
            'distinct_codec_count', facts.distinct_codec_count,
            'cliff_hz', facts.cliff_hz,
            'raw_cliff_min_hz', facts.raw_cliff_min_hz,
            'raw_cliff_max_hz', facts.raw_cliff_max_hz,
            'cliff_spread_hz', facts.cliff_spread_hz,
            'latest_qualifying_log_id', facts.latest_qualifying_log_id,
            'first_observed_at', facts.first_observed_at,
            'latest_observed_at', facts.latest_observed_at,
            'qualifying_identity', facts.qualifying_identity
        )::TEXT,
        'sha256'
    ), 'hex') AS signal_token
FROM signal_facts facts;
$function$;

COMMENT ON FUNCTION derive_request_convergence_signal(BIGINT) IS
    'Derived, provisional search-convergence observation for one exact request. Never proof and never an automatic policy decision.';

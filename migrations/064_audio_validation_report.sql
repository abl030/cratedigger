-- Persist the exact bounded audio-integrity report that produced each
-- content-addressed evidence row. Historical rows cannot prove that the new
-- strict decoder policy ran, so only existing explicit corruption becomes a
-- legacy failure; every other row is honestly legacy_unrecorded.

ALTER TABLE album_quality_evidence
    ADD COLUMN audio_validation JSONB;

UPDATE album_quality_evidence
SET audio_validation = CASE
    WHEN audio_corrupt THEN jsonb_build_object(
        'policy_id', 'pre-audio-integrity-v2',
        'tool', 'legacy',
        'tool_version', '',
        'outcome', 'legacy_failure',
        'files_checked', 0,
        'files_failed', 1,
        'diagnostics', jsonb_build_array(jsonb_build_object(
            'relative_path', '',
            'category', 'legacy_failure',
            'return_code', NULL,
            -- Four bytes per Unicode scalar is the PostgreSQL UTF-8 maximum,
            -- so 512 characters cannot exceed the runtime's 2 KiB byte cap.
            'stderr_excerpt', LEFT(COALESCE(audio_error, ''), 512),
            'stderr_bytes', OCTET_LENGTH(COALESCE(audio_error, '')),
            'stderr_sha256', '',
            'stderr_truncated',
                OCTET_LENGTH(COALESCE(audio_error, ''))
                    > OCTET_LENGTH(LEFT(COALESCE(audio_error, ''), 512))
        )),
        'omitted_diagnostics', 0
    )
    ELSE jsonb_build_object(
        'policy_id', 'pre-audio-integrity-v2',
        'tool', 'legacy',
        'tool_version', '',
        'outcome', 'legacy_unrecorded',
        'files_checked', 0,
        'files_failed', 0,
        'diagnostics', '[]'::jsonb,
        'omitted_diagnostics', 0
    )
END;

CREATE FUNCTION audio_validation_report_is_valid(
    report JSONB,
    corrupt BOOLEAN
) RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    report_outcome TEXT;
    diagnostic JSONB;
    diagnostic_count INTEGER;
BEGIN
    IF report IS NULL OR corrupt IS NULL
       OR jsonb_typeof(report) <> 'object'
       OR NOT report ?& ARRAY[
            'policy_id',
            'tool',
            'tool_version',
            'outcome',
            'files_checked',
            'files_failed',
            'diagnostics',
            'omitted_diagnostics'
       ]
       OR jsonb_typeof(report->'policy_id') <> 'string'
       OR jsonb_typeof(report->'tool') <> 'string'
       OR jsonb_typeof(report->'tool_version') <> 'string'
       OR jsonb_typeof(report->'outcome') <> 'string'
       OR jsonb_typeof(report->'files_checked') <> 'number'
       OR jsonb_typeof(report->'files_failed') <> 'number'
       OR jsonb_typeof(report->'diagnostics') <> 'array'
       OR jsonb_typeof(report->'omitted_diagnostics') <> 'number'
    THEN
        RETURN FALSE;
    END IF;

    -- JSON numbers include decimals and exponents. The Python contract is
    -- integer-only and all three counters are non-negative.
    IF report->>'files_checked' !~ '^(0|[1-9][0-9]*)$'
       OR report->>'files_failed' !~ '^(0|[1-9][0-9]*)$'
       OR report->>'omitted_diagnostics' !~ '^(0|[1-9][0-9]*)$'
    THEN
        RETURN FALSE;
    END IF;

    report_outcome := report->>'outcome';
    diagnostic_count := jsonb_array_length(report->'diagnostics');
    IF report_outcome NOT IN (
        'passed',
        'audio_corrupt',
        'skipped',
        'legacy_failure',
        'legacy_unrecorded'
    ) OR diagnostic_count > 16
    THEN
        RETURN FALSE;
    END IF;

    FOR diagnostic IN
        SELECT value FROM jsonb_array_elements(report->'diagnostics')
    LOOP
        IF jsonb_typeof(diagnostic) <> 'object'
           OR NOT diagnostic ?& ARRAY[
                'relative_path',
                'category',
                'return_code',
                'stderr_excerpt',
                'stderr_bytes',
                'stderr_sha256',
                'stderr_truncated'
           ]
           OR jsonb_typeof(diagnostic->'relative_path') <> 'string'
           OR jsonb_typeof(diagnostic->'category') <> 'string'
           OR (
                jsonb_typeof(diagnostic->'return_code') <> 'null'
                AND (
                    jsonb_typeof(diagnostic->'return_code') <> 'number'
                    OR diagnostic->>'return_code'
                        !~ '^-?(0|[1-9][0-9]*)$'
                )
           )
           OR jsonb_typeof(diagnostic->'stderr_excerpt') <> 'string'
           OR jsonb_typeof(diagnostic->'stderr_bytes') <> 'number'
           OR diagnostic->>'stderr_bytes' !~ '^(0|[1-9][0-9]*)$'
           OR jsonb_typeof(diagnostic->'stderr_sha256') <> 'string'
           OR jsonb_typeof(diagnostic->'stderr_truncated') <> 'boolean'
           OR octet_length(diagnostic->>'stderr_excerpt') > 2048
        THEN
            RETURN FALSE;
        END IF;
    END LOOP;

    IF corrupt <> (
        report_outcome IN ('audio_corrupt', 'legacy_failure')
    ) THEN
        RETURN FALSE;
    END IF;

    IF report_outcome = 'passed' THEN
        RETURN (report->>'files_failed')::NUMERIC = 0
            AND diagnostic_count = 0
            AND (report->>'omitted_diagnostics')::NUMERIC = 0;
    ELSIF report_outcome IN ('skipped', 'legacy_unrecorded') THEN
        RETURN (report->>'files_checked')::NUMERIC = 0
            AND (report->>'files_failed')::NUMERIC = 0
            AND diagnostic_count = 0
            AND (report->>'omitted_diagnostics')::NUMERIC = 0;
    ELSIF report_outcome = 'audio_corrupt' THEN
        IF (report->>'files_failed')::NUMERIC = 0
           OR (report->>'files_failed')::NUMERIC
                > (report->>'files_checked')::NUMERIC
           OR (report->>'files_failed')::NUMERIC
                <> diagnostic_count
                    + (report->>'omitted_diagnostics')::NUMERIC
        THEN
            RETURN FALSE;
        END IF;
        RETURN NOT EXISTS (
            SELECT 1
            FROM jsonb_array_elements(report->'diagnostics') AS item(value)
            WHERE value->>'category' NOT IN (
                'decode_error',
                'ffmpeg_failed_unclassified',
                'decode_timeout'
            )
        );
    END IF;

    -- The only remaining accepted outcome is the exact legacy-failure
    -- sentinel generated by the backfill above.
    RETURN report_outcome = 'legacy_failure'
        AND (report->>'files_checked')::NUMERIC = 0
        AND (report->>'files_failed')::NUMERIC = 1
        AND diagnostic_count = 1
        AND (report->>'omitted_diagnostics')::NUMERIC = 0
        AND report->'diagnostics'->0->>'category' = 'legacy_failure';
END;
$$;

ALTER TABLE album_quality_evidence
    ALTER COLUMN audio_validation SET NOT NULL,
    ADD CONSTRAINT album_quality_evidence_audio_validation_shape_check
    CHECK (audio_validation_report_is_valid(
        audio_validation,
        audio_corrupt
    ));

COMMENT ON COLUMN album_quality_evidence.audio_validation IS
    'Bounded typed audio-only validation audit; legacy_unrecorded is not a pass';

COMMENT ON FUNCTION audio_validation_report_is_valid(JSONB, BOOLEAN) IS
    'Enforces the typed bounded album audio-validation JSONB contract';

CREATE INDEX download_log_measurement_failed_staged_path_idx
    ON download_log(staged_path)
    WHERE outcome = 'measurement_failed'
      AND staged_path IS NOT NULL
      AND BTRIM(staged_path) <> '';

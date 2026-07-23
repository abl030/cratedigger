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

ALTER TABLE album_quality_evidence
    ALTER COLUMN audio_validation SET NOT NULL,
    ADD CONSTRAINT album_quality_evidence_audio_validation_shape_check
    CHECK (
        jsonb_typeof(audio_validation) = 'object'
        AND audio_validation ?& ARRAY[
            'policy_id',
            'tool',
            'tool_version',
            'outcome',
            'files_checked',
            'files_failed',
            'diagnostics',
            'omitted_diagnostics'
        ]
        AND audio_validation->>'outcome' IN (
            'passed',
            'audio_corrupt',
            'skipped',
            'legacy_failure',
            'legacy_unrecorded'
        )
        AND jsonb_typeof(audio_validation->'files_checked') = 'number'
        AND jsonb_typeof(audio_validation->'files_failed') = 'number'
        AND jsonb_typeof(audio_validation->'diagnostics') = 'array'
        AND jsonb_array_length(audio_validation->'diagnostics') <= 16
        AND jsonb_typeof(audio_validation->'omitted_diagnostics') = 'number'
        AND audio_corrupt = (
            audio_validation->>'outcome' IN (
                'audio_corrupt',
                'legacy_failure'
            )
        )
    );

COMMENT ON COLUMN album_quality_evidence.audio_validation IS
    'Bounded typed audio-only validation audit; legacy_unrecorded is not a pass';

CREATE INDEX download_log_measurement_failed_staged_path_idx
    ON download_log(staged_path)
    WHERE outcome = 'measurement_failed'
      AND staged_path IS NOT NULL
      AND BTRIM(staged_path) <> '';

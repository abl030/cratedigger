-- 070_cd_rip_bit_verification.sql - issue #962 exact CD rip provenance
--
-- Positive-only structured evidence. NULL means unavailable, malformed,
-- partial, mismatched, or not run; those states remain deliberately
-- indistinguishable to policy and are never persisted as a penalty.

CREATE FUNCTION cd_rip_unsigned_integer_array_is_valid(
    values_json JSONB,
    minimum_value NUMERIC,
    maximum_value NUMERIC
) RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    item JSONB;
    item_value NUMERIC;
BEGIN
    IF values_json IS NULL OR jsonb_typeof(values_json) <> 'array' THEN
        RETURN FALSE;
    END IF;
    FOR item IN SELECT value FROM jsonb_array_elements(values_json)
    LOOP
        IF jsonb_typeof(item) <> 'number'
           OR item#>>'{}' !~ '^(0|[1-9][0-9]*)$'
        THEN
            RETURN FALSE;
        END IF;
        item_value := (item#>>'{}')::numeric;
        IF item_value < minimum_value OR item_value > maximum_value THEN
            RETURN FALSE;
        END IF;
    END LOOP;
    RETURN TRUE;
END;
$$;

CREATE FUNCTION cd_rip_toc_is_valid(
    offsets JSONB,
    leadout JSONB
) RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    item JSONB;
    item_value NUMERIC;
    previous_value NUMERIC := -1;
    leadout_value NUMERIC;
BEGIN
    IF offsets IS NULL OR jsonb_typeof(offsets) <> 'array'
       OR jsonb_array_length(offsets) NOT BETWEEN 1 AND 99
       OR leadout IS NULL OR jsonb_typeof(leadout) <> 'number'
       OR leadout#>>'{}' !~ '^(0|[1-9][0-9]*)$'
    THEN
        RETURN FALSE;
    END IF;
    FOR item IN SELECT value FROM jsonb_array_elements(offsets)
    LOOP
        IF jsonb_typeof(item) <> 'number'
           OR item#>>'{}' !~ '^(0|[1-9][0-9]*)$'
        THEN
            RETURN FALSE;
        END IF;
        item_value := (item#>>'{}')::numeric;
        IF item_value <= previous_value OR item_value > 4294967295 THEN
            RETURN FALSE;
        END IF;
        previous_value := item_value;
    END LOOP;
    leadout_value := (leadout#>>'{}')::numeric;
    RETURN previous_value >= 0
       AND leadout_value > previous_value
       AND leadout_value <= 4294967295;
END;
$$;

CREATE FUNCTION cd_rip_ctdb_toc_is_valid(
    response_toc JSONB,
    response_shift JSONB,
    submitted_offsets JSONB,
    submitted_leadout JSONB
) RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    item_index INTEGER;
    response_value NUMERIC;
    shift_value NUMERIC;
    expected_value NUMERIC;
BEGIN
    IF response_toc IS NULL OR jsonb_typeof(response_toc) <> 'array'
       OR response_shift IS NULL OR jsonb_typeof(response_shift) <> 'number'
       OR response_shift#>>'{}' !~ '^(0|[1-9][0-9]*)$'
       OR submitted_offsets IS NULL
       OR jsonb_typeof(submitted_offsets) <> 'array'
       OR jsonb_array_length(submitted_offsets) NOT BETWEEN 1 AND 99
       OR submitted_leadout IS NULL
       OR jsonb_typeof(submitted_leadout) <> 'number'
       OR submitted_leadout#>>'{}' !~ '^(0|[1-9][0-9]*)$'
       OR jsonb_array_length(response_toc)
            <> jsonb_array_length(submitted_offsets) + 1
    THEN
        RETURN FALSE;
    END IF;
    shift_value := (response_shift#>>'{}')::numeric;
    IF shift_value > 4294967295 THEN
        RETURN FALSE;
    END IF;
    FOR item_index IN 0..jsonb_array_length(response_toc) - 1
    LOOP
        IF jsonb_typeof(response_toc->item_index) <> 'number'
           OR response_toc->>item_index !~ '^(0|[1-9][0-9]*)$'
        THEN
            RETURN FALSE;
        END IF;
        response_value := (response_toc->>item_index)::numeric;
        IF response_value > 4294967295 THEN
            RETURN FALSE;
        END IF;
        IF item_index < jsonb_array_length(submitted_offsets) THEN
            IF jsonb_typeof(submitted_offsets->item_index) <> 'number'
               OR submitted_offsets->>item_index
                    !~ '^(0|[1-9][0-9]*)$'
            THEN
                RETURN FALSE;
            END IF;
            expected_value := (submitted_offsets->>item_index)::numeric;
        ELSE
            expected_value := (submitted_leadout#>>'{}')::numeric;
        END IF;
        IF response_value - shift_value <> expected_value THEN
            RETURN FALSE;
        END IF;
    END LOOP;
    RETURN TRUE;
END;
$$;

ALTER TABLE album_quality_evidence
    ADD COLUMN cd_rip_verification JSONB,
    ADD CONSTRAINT album_quality_evidence_cd_rip_shape CHECK (
        CASE
        WHEN cd_rip_verification IS NULL THEN TRUE
        WHEN jsonb_typeof(cd_rip_verification) <> 'object' THEN FALSE
        WHEN jsonb_typeof(cd_rip_verification->'toc') <> 'object' THEN FALSE
        WHEN jsonb_typeof(cd_rip_verification#>'{toc,track_offsets_sectors}')
             <> 'array' THEN FALSE
        ELSE ((
            cd_rip_verification->>'algorithm' = 'cd-rip-bit-verifier-v1'
            AND cd_rip_verification->>'provenance' IN ('measured', 'carried')
            AND cd_rip_verification->>'source_format' IN ('flac', 'alac')
            AND cd_rip_toc_is_valid(
                cd_rip_verification#>'{toc,track_offsets_sectors}',
                cd_rip_verification#>'{toc,leadout_sector}'
            )
            AND cd_rip_verification#>>'{toc,track_offsets_sectors,0}' = '0'
            AND NULLIF(cd_rip_verification#>>'{toc,accuraterip_id}', '')
                IS NOT NULL
            AND NULLIF(cd_rip_verification#>>'{toc,musicbrainz_disc_id}', '')
                IS NOT NULL
            AND (
                jsonb_typeof(cd_rip_verification->'accuraterip') = 'object'
                OR jsonb_typeof(cd_rip_verification->'ctdb') = 'object'
            )
            AND CASE
                WHEN jsonb_typeof(cd_rip_verification->'accuraterip')
                     = 'object'
                THEN (
                    cd_rip_verification#>>'{accuraterip,provider}'
                        = 'accuraterip'
                    AND cd_rip_verification#>>'{accuraterip,url}'
                        ~ '^https://[^[:space:]]+$'
                    AND cd_rip_verification#>>'{accuraterip,checksum_version}'
                        IN ('arv1', 'arv2')
                    AND (cd_rip_verification#>>
                            '{accuraterip,read_offset_samples}')
                        ~ '^-?[0-9]+$'
                    AND (cd_rip_verification#>>
                            '{accuraterip,read_offset_samples}')::numeric
                        BETWEEN -5000 AND 5000
                    AND jsonb_typeof(cd_rip_verification#>
                            '{accuraterip,track_confidences}') = 'array'
                    AND jsonb_array_length(cd_rip_verification#>
                            '{accuraterip,track_confidences}')
                        = jsonb_array_length(cd_rip_verification#>
                            '{toc,track_offsets_sectors}')
                    AND cd_rip_unsigned_integer_array_is_valid(
                        cd_rip_verification#>
                            '{accuraterip,track_confidences}',
                        1,
                        255
                    )
                    AND jsonb_typeof(cd_rip_verification#>
                            '{accuraterip,track_checksums}') = 'array'
                    AND jsonb_array_length(cd_rip_verification#>
                            '{accuraterip,track_checksums}')
                        = jsonb_array_length(cd_rip_verification#>
                            '{toc,track_offsets_sectors}')
                    AND cd_rip_unsigned_integer_array_is_valid(
                        cd_rip_verification#>'{accuraterip,track_checksums}',
                        0,
                        4294967295
                    )
                    AND cd_rip_verification#>>'{accuraterip,response_sha256}'
                        ~ '^[0-9a-f]{64}$'
                ) IS TRUE
                ELSE cd_rip_verification->'accuraterip' IS NULL
                     OR jsonb_typeof(cd_rip_verification->'accuraterip')
                        = 'null'
            END
            AND CASE
                WHEN jsonb_typeof(cd_rip_verification->'ctdb') = 'object'
                THEN (
                    cd_rip_verification#>>'{ctdb,provider}' = 'ctdb'
                    AND cd_rip_verification#>>'{ctdb,url}'
                        ~ '^https://[^[:space:]]+$'
                    AND NULLIF(cd_rip_verification#>>'{ctdb,entry_id}', '')
                        IS NOT NULL
                    AND (cd_rip_verification#>>'{ctdb,confidence}')
                        ~ '^[0-9]+$'
                    AND (cd_rip_verification#>>'{ctdb,confidence}')::numeric > 0
                    AND (cd_rip_verification#>>'{ctdb,crc32}')
                        ~ '^[0-9]+$'
                    AND (cd_rip_verification#>>'{ctdb,crc32}')::numeric
                        BETWEEN 0 AND 4294967295
                    AND cd_rip_verification#>>'{ctdb,stride_samples}' = '5880'
                    AND jsonb_typeof(cd_rip_verification#>
                            '{ctdb,response_toc_sectors}') = 'array'
                    AND cd_rip_ctdb_toc_is_valid(
                        cd_rip_verification#>'{ctdb,response_toc_sectors}',
                        cd_rip_verification#>
                            '{ctdb,response_toc_shift_sectors}',
                        cd_rip_verification#>'{toc,track_offsets_sectors}',
                        cd_rip_verification#>'{toc,leadout_sector}'
                    )
                    AND cd_rip_verification#>>'{ctdb,response_sha256}'
                        ~ '^[0-9a-f]{64}$'
                ) IS TRUE
                ELSE cd_rip_verification->'ctdb' IS NULL
                     OR jsonb_typeof(cd_rip_verification->'ctdb') = 'null'
            END
        ) IS TRUE)
        END
    ),
    ADD CONSTRAINT album_quality_evidence_cd_rip_proof_pair CHECK (
        (cd_rip_verification IS NULL
         AND verified_lossless_classifier IS DISTINCT FROM
             'cd_rip_bit_verified_v1')
        OR (
            cd_rip_verification IS NOT NULL
            AND verified_lossless IS TRUE
            AND verified_lossless_classifier = 'cd_rip_bit_verified_v1'
            AND verified_lossless_provenance =
                cd_rip_verification->>'provenance'
            AND LOWER(verified_lossless_source) =
                cd_rip_verification->>'source_format'
        )
    );

COMMENT ON COLUMN album_quality_evidence.cd_rip_verification IS
    'Positive-only exact CD bit evidence: TOC/provider identities and either an all-track same-offset AccurateRip ARv1/ARv2 match or an exact CTDB whole-disc CRC. Carried rows still describe the downloaded source, not derivative bytes.';

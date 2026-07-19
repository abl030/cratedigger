-- 057_lossless_lineage_spectral_subject_check.sql
--
-- R19: an installed derivative of a lossless source must retain the source
-- spectral fact (or no spectral fact), never a scan of the installed bytes.
-- The three row-local lineage signals mirror
-- lib.import_preview.preserve_existing_source_spectral().  Legacy v1/v3 rows
-- remain rebuild-on-touch data and are deliberately outside this v4 contract.

ALTER TABLE album_quality_evidence
    ADD CONSTRAINT album_quality_evidence_lossless_lineage_spectral_subject
        CHECK (
            lineage_version < 4
            OR NOT (
                spectral_subject IS NOT DISTINCT FROM 'installed'
                AND (
                    v0_subject IS NOT DISTINCT FROM 'source'
                    OR verified_lossless IS TRUE
                    OR LOWER(COALESCE(was_converted_from, ''))
                        IN ('flac', 'alac', 'wav')
                )
            )
        );

COMMENT ON CONSTRAINT
    album_quality_evidence_lossless_lineage_spectral_subject
    ON album_quality_evidence IS
    'v4 lossless-lineage rows cannot describe installed spectral bytes';

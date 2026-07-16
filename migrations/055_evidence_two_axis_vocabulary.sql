-- 055_evidence_two_axis_vocabulary.sql
--
-- Quality facts use one uniform vocabulary:
--   subject    = installed | source
--   provenance = measured  | carried
--
-- Historical v1/v3 rows are mapped best-effort but remain version-gated so
-- an unknown legacy value can be rebuilt on touch instead of blocking deploy.

ALTER TABLE album_quality_evidence
    DROP CONSTRAINT album_quality_evidence_v0_metric_shape,
    DROP CONSTRAINT album_quality_evidence_verified_proof_shape,
    DROP CONSTRAINT album_quality_evidence_v0_source_lineage_check,
    DROP CONSTRAINT album_quality_evidence_lineage_version_check;

ALTER TABLE album_quality_evidence
    RENAME COLUMN v0_source_lineage TO v0_subject;

ALTER TABLE album_quality_evidence
    RENAME COLUMN v0_source_provenance TO v0_provenance;

ALTER TABLE album_quality_evidence
    RENAME COLUMN verified_lossless_proof_origin
    TO verified_lossless_provenance;

ALTER TABLE album_quality_evidence
    DROP COLUMN v0_proof_provenance,
    ADD COLUMN spectral_subject TEXT,
    ADD COLUMN spectral_provenance TEXT;

UPDATE album_quality_evidence
SET v0_subject = CASE v0_subject
        WHEN 'lossless_source' THEN 'source'
        WHEN 'native_lossy_research' THEN 'installed'
        WHEN 'on_disk_research' THEN 'installed'
        WHEN 'unknown_v0_source' THEN 'installed'
        ELSE v0_subject
    END,
    v0_provenance = CASE v0_provenance
        WHEN 'lossless_source_v0' THEN 'measured'
        WHEN 'native_lossy_research_v0' THEN 'measured'
        WHEN 'on_disk_research_v0' THEN 'measured'
        WHEN 'album_requests.current_lossless_source_v0_probe' THEN 'carried'
        WHEN 'new_measurement_fallback' THEN 'measured'
        ELSE v0_provenance
    END,
    verified_lossless_provenance = CASE verified_lossless_provenance
        WHEN 'import_result' THEN 'measured'
        WHEN 'legacy_request_seed' THEN 'carried'
        ELSE verified_lossless_provenance
    END,
    -- Existing spectral facts describe the row's installed snapshot. New
    -- writers decide between installed/measured and source/carried explicitly.
    spectral_subject = CASE
        WHEN spectral_grade IS NOT NULL THEN 'installed'
        ELSE spectral_subject
    END,
    spectral_provenance = CASE
        WHEN spectral_grade IS NOT NULL THEN 'measured'
        ELSE spectral_provenance
    END
WHERE v0_subject IN (
        'lossless_source',
        'native_lossy_research',
        'on_disk_research',
        'unknown_v0_source'
    )
    OR v0_provenance IN (
        'lossless_source_v0',
        'native_lossy_research_v0',
        'on_disk_research_v0',
        'album_requests.current_lossless_source_v0_probe',
        'new_measurement_fallback'
    )
    OR verified_lossless_provenance IN (
        'import_result',
        'legacy_request_seed'
    )
    OR spectral_grade IS NOT NULL;

ALTER TABLE album_quality_evidence
    ALTER COLUMN lineage_version SET DEFAULT 4;

ALTER TABLE album_quality_evidence
    ADD CONSTRAINT album_quality_evidence_lineage_version_check
        CHECK (lineage_version IN (1, 3, 4)),
    ADD CONSTRAINT album_quality_evidence_v0_subject_domain
        CHECK (
            lineage_version < 4
            OR v0_subject IS NULL
            OR v0_subject IN ('installed', 'source')
        ),
    ADD CONSTRAINT album_quality_evidence_v0_provenance_domain
        CHECK (
            lineage_version < 4
            OR v0_provenance IS NULL
            OR v0_provenance IN ('measured', 'carried')
        ),
    ADD CONSTRAINT album_quality_evidence_spectral_subject_domain
        CHECK (
            lineage_version < 4
            OR spectral_subject IS NULL
            OR spectral_subject IN ('installed', 'source')
        ),
    ADD CONSTRAINT album_quality_evidence_spectral_provenance_domain
        CHECK (
            lineage_version < 4
            OR spectral_provenance IS NULL
            OR spectral_provenance IN ('measured', 'carried')
        ),
    ADD CONSTRAINT album_quality_evidence_verified_provenance_domain
        CHECK (
            lineage_version < 4
            OR verified_lossless_provenance IS NULL
            OR verified_lossless_provenance IN ('measured', 'carried')
        ),
    ADD CONSTRAINT album_quality_evidence_v0_metric_shape
        CHECK (
            lineage_version < 4
            OR (
                v0_min_bitrate_kbps IS NULL
                AND v0_avg_bitrate_kbps IS NULL
                AND v0_median_bitrate_kbps IS NULL
                AND v0_subject IS NULL
                AND v0_provenance IS NULL
            )
            OR (
                v0_subject IS NOT NULL
                AND v0_provenance IS NOT NULL
                AND (
                    v0_min_bitrate_kbps IS NOT NULL
                    OR v0_avg_bitrate_kbps IS NOT NULL
                    OR v0_median_bitrate_kbps IS NOT NULL
                )
            )
        ),
    ADD CONSTRAINT album_quality_evidence_v0_cross_product
        CHECK (
            lineage_version < 4
            OR v0_subject IS DISTINCT FROM 'installed'
            OR v0_provenance = 'measured'
        ),
    ADD CONSTRAINT album_quality_evidence_spectral_shape
        CHECK (
            lineage_version < 4
            OR (
                spectral_grade IS NULL
                AND spectral_bitrate_kbps IS NULL
                AND spectral_subject IS NULL
                AND spectral_provenance IS NULL
            )
            OR (
                spectral_grade IS NOT NULL
                AND spectral_subject IS NOT NULL
                AND spectral_provenance IS NOT NULL
            )
        ),
    ADD CONSTRAINT album_quality_evidence_spectral_cross_product
        CHECK (
            lineage_version < 4
            OR spectral_subject IS DISTINCT FROM 'installed'
            OR spectral_provenance = 'measured'
        ),
    ADD CONSTRAINT album_quality_evidence_verified_proof_shape
        CHECK (
            lineage_version < 4
            OR (
                verified_lossless = TRUE
                AND verified_lossless_provenance IS NOT NULL
                AND verified_lossless_source IS NOT NULL
                AND verified_lossless_classifier IS NOT NULL
            )
            OR (
                verified_lossless = FALSE
                AND verified_lossless_provenance IS NULL
                AND verified_lossless_source IS NULL
                AND verified_lossless_classifier IS NULL
                AND verified_lossless_detail IS NULL
            )
        );

COMMENT ON COLUMN album_quality_evidence.lineage_version IS
    '1=historical ambiguous projection, 3=separate lineage, 4=two-axis evidence vocabulary';

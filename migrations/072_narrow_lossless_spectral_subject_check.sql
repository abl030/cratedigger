-- 072_narrow_lossless_spectral_subject_check.sql
--
-- Source V0/proof records describe historical provenance; they do not prove
-- that the installed bytes are an irreplaceable derivative.  Keep only the
-- conversion marker at the SQL boundary.  The application additionally
-- verifies the exact current media manifest before preserving source spectral
-- across analyzer generations.

ALTER TABLE album_quality_evidence
    DROP CONSTRAINT album_quality_evidence_lossless_lineage_spectral_subject;

ALTER TABLE album_quality_evidence
    ADD CONSTRAINT album_quality_evidence_lossless_lineage_spectral_subject
        CHECK (
            lineage_version < 4
            OR spectral_subject IS DISTINCT FROM 'installed'
            OR COALESCE(LOWER(was_converted_from), '')
                NOT IN ('flac', 'alac', 'wav')
        );

COMMENT ON CONSTRAINT album_quality_evidence_lossless_lineage_spectral_subject
    ON album_quality_evidence IS
    'v4 installed spectral is excluded only while a lossless conversion marker remains';

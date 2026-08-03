-- Conversion lineage is a durable fact about the installed output, whereas
-- spectral_subject records which bytes a particular spectral measurement saw.
-- A fresh HAVE scan must be able to record installed spectral facts without
-- erasing FLAC -> ALAC/Opus/Vorbis lineage. R19's source-spectral reuse rule
-- is enforced by the exact manifest-aware application predicate, not SQL.
ALTER TABLE album_quality_evidence
    DROP CONSTRAINT album_quality_evidence_lossless_lineage_spectral_subject;

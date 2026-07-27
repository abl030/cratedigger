-- 065_spectral_capture_facts.sql - issue #829 Phase 5 PR1 capture
--
-- Adds four measured facts to album_quality_evidence, captured alongside the
-- existing spectral_grade/spectral_bitrate_kbps tuple by the same
-- measurement pass. This migration is capture-only: nothing in the
-- production decider reads these columns yet (PR2/PR3 own that).
--
-- Forward-only, no backfill (.claude/rules/scope.md): legacy rows keep all
-- four columns NULL and retain their old semantics behind the version stamp.

ALTER TABLE album_quality_evidence
    ADD COLUMN cliff_hz INTEGER CHECK (cliff_hz IS NULL OR cliff_hz >= 0),
    ADD COLUMN codec_family TEXT CHECK (
        codec_family IS NULL OR codec_family IN (
            'mp3', 'aac', 'opus', 'vorbis', 'lossless', 'other'
        )
    ),
    ADD COLUMN ultrasonic_deficit_db DOUBLE PRECISION,
    ADD COLUMN spectral_measurement_version SMALLINT;

COMMENT ON COLUMN album_quality_evidence.cliff_hz IS
    'Raw in-window spectral cliff frequency (Hz) from detect_cliff() -- the primitive spectral_bitrate_kbps buckets. NULL when no cliff was detected or the row predates this capture (issue #829 Phase 5 PR1).';

COMMENT ON COLUMN album_quality_evidence.codec_family IS
    'Normalised codec family at measurement time: mp3/aac/opus/vorbis/lossless/other. NULL for legacy rows measured before this capture shipped.';

COMMENT ON COLUMN album_quality_evidence.ultrasonic_deficit_db IS
    'Level-invariant ultrasonic deficit: ref_db(1-4kHz) minus mean(20.5-22kHz extension slices), averaged across tracks. Consumed by the PR3 proof leg; not read by any decision yet.';

COMMENT ON COLUMN album_quality_evidence.spectral_measurement_version IS
    '2 for rows measured by the issue #829 Phase 5 PR1+ spectral_check code. NULL for legacy rows measured before this capture shipped (forward-only, no backfill).';
